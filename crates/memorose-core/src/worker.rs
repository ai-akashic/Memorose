use crate::llm::{EmbedInput, EmbedPart, LLMClient};
use crate::MemoroseEngine;
use anyhow::Result;
use memorose_common::{
    config::AppConfig, tokenizer::count_tokens, Asset, Event, EventContent, GraphEdge, MemoryUnit,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use tokio::time::Duration;

type PackedGroupKey = (String, uuid::Uuid, Option<String>);

#[derive(Debug, Clone)]
struct PackedEventGroup {
    key: PackedGroupKey,
    seq_no: u64,
    events: Vec<Event>,
}

struct ProducedBatch {
    key: PackedGroupKey,
    seq_no: u64,
    event_ids: Vec<uuid::Uuid>,
    user_id: String,
    stream_id: uuid::Uuid,
    summary: String,
    valid_at: Option<String>,
    assets: Vec<Asset>,
    metadata: serde_json::Value,
    embed_input: Option<EmbedInput>,
}

struct RunningFlagGuard {
    flag: Arc<AtomicBool>,
}

impl RunningFlagGuard {
    fn try_acquire(flag: Arc<AtomicBool>) -> Option<Self> {
        flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| Self { flag })
    }
}

impl Drop for RunningFlagGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct BackgroundWorker {
    engine: MemoroseEngine,
    llm_client: Option<Arc<dyn LLMClient>>,
    config: memorose_common::config::WorkerConfig,
    last_decay: Arc<tokio::sync::Mutex<std::time::Instant>>,
    last_compaction: Arc<tokio::sync::Mutex<std::time::Instant>>,
    last_consolidation: Arc<tokio::sync::Mutex<std::time::Instant>>,
    last_insight: Arc<tokio::sync::Mutex<std::time::Instant>>,
    last_community: Arc<tokio::sync::Mutex<std::time::Instant>>,
    consolidation_running: Arc<AtomicBool>,
    materialization_running: Arc<AtomicBool>,
    insight_running: Arc<AtomicBool>,
    raft: Option<crate::raft::MemoroseRaft>,
}

impl BackgroundWorker {
    fn should_process_reflection_marker(&self, marker: &crate::engine::ReflectionMarker) -> bool {
        if marker.first_event_at_ts <= 0 {
            return true;
        }

        if marker.pending_units >= self.config.insight_min_pending_l1.max(1) {
            return true;
        }

        if marker.pending_tokens >= self.config.insight_min_pending_tokens.max(1) {
            return true;
        }

        let max_delay_ms = self
            .config
            .insight_max_delay_ms
            .max(self.config.tick_interval_ms);
        let age_ms = chrono::Utc::now()
            .timestamp_millis()
            .saturating_sub(marker.first_event_at_ts);
        age_ms as u64 >= max_delay_ms
    }

    fn packed_event_key(event: &Event) -> PackedGroupKey {
        let is_agent = event.metadata.get("role").and_then(|v| v.as_str()) == Some("assistant")
            || event.metadata.get("agent_id").is_some();
        let agent_id = if is_agent {
            event
                .metadata
                .get("agent_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or(Some("default_agent".to_string()))
        } else {
            None
        };
        (event.user_id.clone(), event.stream_id, agent_id)
    }

    fn estimate_event_pack_tokens(event: &Event) -> usize {
        let content_tokens = match &event.content {
            EventContent::Text(text) => count_tokens(text),
            EventContent::Json(value) => count_tokens(&value.to_string()),
            EventContent::Image(url) | EventContent::Audio(url) | EventContent::Video(url) => {
                count_tokens(url) + 12
            }
        };
        content_tokens + 4
    }

    fn pack_events_for_consolidation(&self, events: Vec<Event>) -> Vec<PackedEventGroup> {
        let mut packed_batches = Vec::new();
        let mut current_batch = Vec::new();
        let mut current_key: Option<PackedGroupKey> = None;
        let mut current_tokens = 0usize;
        let mut next_seq_by_key: HashMap<PackedGroupKey, u64> = HashMap::new();
        let target_tokens = self.config.consolidation_target_tokens.max(1);
        let max_events_per_pack = self.config.consolidation_max_events_per_pack.max(1);

        for event in events {
            let key = Self::packed_event_key(&event);
            let event_tokens = Self::estimate_event_pack_tokens(&event).max(1);
            let should_flush = Some(&key) != current_key.as_ref()
                || current_batch.len() >= max_events_per_pack
                || (!current_batch.is_empty() && current_tokens + event_tokens > target_tokens);

            if should_flush {
                if !current_batch.is_empty() {
                    let flushed_key = current_key
                        .as_ref()
                        .expect("current_key must exist when flushing a non-empty batch")
                        .clone();
                    let seq_no = next_seq_by_key
                        .entry(flushed_key.clone())
                        .and_modify(|seq| *seq += 1)
                        .or_insert(0);
                    packed_batches.push(PackedEventGroup {
                        key: flushed_key,
                        seq_no: *seq_no,
                        events: std::mem::take(&mut current_batch),
                    });
                }
                current_key = Some(key);
                current_tokens = 0;
            }

            current_tokens += event_tokens;
            current_batch.push(event);
        }

        if !current_batch.is_empty() {
            let flushed_key = current_key
                .as_ref()
                .expect("current_key must exist when flushing a non-empty batch")
                .clone();
            let seq_no = next_seq_by_key
                .entry(flushed_key.clone())
                .and_modify(|seq| *seq += 1)
                .or_insert(0);
            packed_batches.push(PackedEventGroup {
                key: flushed_key,
                seq_no: *seq_no,
                events: current_batch,
            });
        }

        packed_batches
    }

    fn schedule_packed_groups_fairly(
        &self,
        packed_batches: Vec<PackedEventGroup>,
    ) -> Vec<PackedEventGroup> {
        let mut by_key: HashMap<PackedGroupKey, VecDeque<PackedEventGroup>> = HashMap::new();
        let mut active_keys = VecDeque::new();

        for group in packed_batches {
            let key = group.key.clone();
            let queue = by_key.entry(key.clone()).or_default();
            if queue.is_empty() {
                active_keys.push_back(key);
            }
            queue.push_back(group);
        }

        let mut scheduled = Vec::new();
        while let Some(key) = active_keys.pop_front() {
            let mut should_requeue = false;
            if let Some(queue) = by_key.get_mut(&key) {
                if let Some(group) = queue.pop_front() {
                    scheduled.push(group);
                }
                should_requeue = !queue.is_empty();
            }

            if should_requeue {
                active_keys.push_back(key);
            } else {
                by_key.remove(&key);
            }
        }

        scheduled
    }

    fn limit_scheduled_groups_by_event_budget(
        &self,
        scheduled_batches: Vec<PackedEventGroup>,
        event_budget: usize,
    ) -> Vec<PackedEventGroup> {
        let event_budget = event_budget.max(1);
        let mut selected = Vec::new();
        let mut selected_events = 0usize;

        for group in scheduled_batches {
            let group_events = group.events.len().max(1);
            if selected.is_empty() || selected_events + group_events <= event_budget {
                selected_events += group_events;
                selected.push(group);
            } else {
                break;
            }
        }

        selected
    }

    fn normalize_asset_storage_key(asset_type: &str, storage_key: &str) -> String {
        let trimmed = storage_key.trim();
        if trimmed.starts_with("http://")
            || trimmed.starts_with("https://")
            || trimmed.starts_with("s3://")
            || trimmed.starts_with("local://")
            || trimmed.starts_with("inline://")
        {
            return trimmed.to_string();
        }

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        asset_type.hash(&mut hasher);
        trimmed.hash(&mut hasher);
        format!("inline://{}/{:016x}", asset_type, hasher.finish())
    }

    fn build_asset(
        storage_key: String,
        original_name: &str,
        asset_type: &str,
        description: Option<String>,
    ) -> Asset {
        Asset {
            storage_key: Self::normalize_asset_storage_key(asset_type, &storage_key),
            original_name: original_name.to_string(),
            asset_type: asset_type.to_string(),
            description,
            metadata: std::collections::HashMap::new(),
        }
    }

    async fn hydrate_extracted_facts(&self, unit: &mut MemoryUnit) {
        if unit.level != 1 || unit.memory_type != memorose_common::MemoryType::Factual {
            return;
        }

        let mut stored_facts = Vec::new();

        if let Some(client) = self.llm_client.clone() {
            let arbitrator = crate::arbitrator::Arbitrator::with_client(client);
            match arbitrator.extract_memory_facts(unit).await {
                Ok(facts) => {
                    stored_facts.extend(
                        facts
                            .into_iter()
                            .filter_map(crate::fact_extraction::stored_fact_from_extracted_fact),
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        "Memory fact extraction during worker hydration failed for {}: {:?}",
                        unit.id,
                        error
                    );
                }
            }
        }

        if stored_facts.is_empty() {
            stored_facts.extend(
                crate::fact_extraction::detect_memory_facts(unit)
                    .iter()
                    .map(crate::fact_extraction::stored_fact_from_descriptor),
            );
        }

        let mut deduped = Vec::new();
        let mut seen = HashSet::new();
        for fact in stored_facts {
            let key = format!(
                "{}|{}|{}|{}|{}",
                fact.subject,
                fact.subject_ref.as_deref().unwrap_or(""),
                fact.attribute,
                fact.canonical_value
                    .as_deref()
                    .unwrap_or(fact.value.as_str()),
                fact.change_type
            );
            if seen.insert(key) {
                deduped.push(fact);
            }
        }

        unit.extracted_facts = deduped;
    }

    pub fn new(engine: MemoroseEngine) -> Self {
        let config = AppConfig::load().unwrap_or_else(|e| {
            tracing::warn!("Failed to load config ({}), using defaults", e);
            AppConfig::default()
        });
        Self::with_config(engine, config)
    }

    pub fn with_config(engine: MemoroseEngine, config: AppConfig) -> Self {
        let llm_client = crate::llm::create_llm_client(&config.llm);

        if llm_client.is_none() {
            tracing::warn!("BackgroundWorker starting without API Key. Summary and Insight features will be disabled/degraded.");
        }

        let now = std::time::Instant::now();
        Self {
            engine,
            llm_client,
            config: config.worker,
            last_decay: Arc::new(tokio::sync::Mutex::new(now)),
            last_compaction: Arc::new(tokio::sync::Mutex::new(now)),
            last_consolidation: Arc::new(tokio::sync::Mutex::new(now)),
            last_insight: Arc::new(tokio::sync::Mutex::new(now)),
            last_community: Arc::new(tokio::sync::Mutex::new(now)),
            consolidation_running: Arc::new(AtomicBool::new(false)),
            materialization_running: Arc::new(AtomicBool::new(false)),
            insight_running: Arc::new(AtomicBool::new(false)),
            raft: None,
        }
    }

    pub fn set_raft(&mut self, raft: crate::raft::MemoroseRaft) {
        self.raft = Some(raft);
    }

    pub async fn is_leader(&self) -> bool {
        if let Some(raft) = &self.raft {
            let metrics = raft.metrics().borrow().clone();
            metrics.current_leader == Some(metrics.id)
        } else {
            true
        }
    }

    pub async fn run(&self) {
        let tick_ms = self.config.tick_interval_ms.max(10);
        let consolidation_interval_ms = self
            .config
            .consolidation_interval_ms
            .max(self.config.tick_interval_ms);
        let insight_interval_ms = self
            .config
            .insight_interval_ms
            .max(self.config.tick_interval_ms);
        tracing::info!(
            "Background Worker started (maintenance={}ms, consolidation={}ms, materialization={}ms, insight={}ms).",
            tick_ms,
            consolidation_interval_ms,
            tick_ms,
            insight_interval_ms
        );

        let mut loop_tasks = tokio::task::JoinSet::new();

        let consolidation_worker = self.clone();
        loop_tasks.spawn(async move {
            consolidation_worker.run_consolidation_loop().await;
            "consolidation"
        });

        let materialization_worker = self.clone();
        loop_tasks.spawn(async move {
            materialization_worker.run_materialization_loop().await;
            "materialization"
        });

        if self.llm_client.is_some() {
            let insight_worker = self.clone();
            loop_tasks.spawn(async move {
                insight_worker.run_insight_loop().await;
                "insight"
            });
        }

        let mut interval = tokio::time::interval(Duration::from_millis(tick_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if !self.is_leader().await {
                        continue;
                    }

                    if let Err(e) = self.run_decay_cycle().await {
                        tracing::error!("Decay cycle failed: {:?}", e);
                    }

                    if let Err(e) = self.run_l3_task_cycle().await {
                        tracing::error!("L3 Task cycle failed: {:?}", e);
                    }

                    if let Err(e) = self.run_compaction_cycle().await {
                        tracing::error!("Compaction cycle failed: {:?}", e);
                    }

                    if self.llm_client.is_some() {
                        if let Err(e) = self.run_community_cycle().await {
                            tracing::error!("Community cycle failed: {:?}", e);
                        }
                    }
                }
                Some(result) = loop_tasks.join_next() => {
                    match result {
                        Ok(loop_name) => {
                            tracing::warn!("{} loop exited unexpectedly; restarting", loop_name);
                        }
                        Err(error) => {
                            tracing::error!("Background loop task failed: {:?}", error);
                        }
                    }

                    loop_tasks.abort_all();
                    while loop_tasks.join_next().await.is_some() {}

                    let should_restart_insight = self.llm_client.is_some();
                    let consolidation_worker = self.clone();
                    loop_tasks.spawn(async move {
                        consolidation_worker.run_consolidation_loop().await;
                        "consolidation"
                    });

                    let materialization_worker = self.clone();
                    loop_tasks.spawn(async move {
                        materialization_worker.run_materialization_loop().await;
                        "materialization"
                    });

                    if should_restart_insight {
                        let insight_worker = self.clone();
                        loop_tasks.spawn(async move {
                            insight_worker.run_insight_loop().await;
                            "insight"
                        });
                    }
                }
            }
        }
    }

    async fn run_consolidation_loop(self) {
        let tick_ms = self.config.tick_interval_ms.max(10);
        tracing::info!(
            "Consolidation loop started (poll={}ms, run_interval={}ms).",
            tick_ms,
            self.config
                .consolidation_interval_ms
                .max(self.config.tick_interval_ms)
        );

        let mut interval = tokio::time::interval(Duration::from_millis(tick_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            interval.tick().await;

            if !self.is_leader().await {
                continue;
            }

            let Some(_running_guard) =
                RunningFlagGuard::try_acquire(self.consolidation_running.clone())
            else {
                tracing::debug!("Consolidation loop is still busy; skipping this tick.");
                continue;
            };

            if let Err(error) = self.run_consolidation_cycle().await {
                tracing::error!("Consolidation loop failed: {:?}", error);
            }
        }
    }

    async fn run_materialization_loop(self) {
        let tick_ms = self.config.tick_interval_ms.max(10);
        tracing::info!("Materialization loop started (poll={}ms).", tick_ms);

        let mut interval = tokio::time::interval(Duration::from_millis(tick_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            interval.tick().await;

            if !self.is_leader().await {
                continue;
            }

            let Some(_running_guard) =
                RunningFlagGuard::try_acquire(self.materialization_running.clone())
            else {
                tracing::debug!("Materialization loop is still busy; skipping this tick.");
                continue;
            };

            if let Err(error) = self.run_materialization_cycle().await {
                tracing::error!("Materialization loop failed: {:?}", error);
            }
        }
    }

    async fn run_insight_loop(self) {
        if self.llm_client.is_none() {
            tracing::info!("Insight loop disabled: no LLM client configured.");
            return;
        }

        let tick_ms = self.config.tick_interval_ms.max(10);
        tracing::info!(
            "Insight loop started (poll={}ms, run_interval={}ms).",
            tick_ms,
            self.config
                .insight_interval_ms
                .max(self.config.tick_interval_ms)
        );

        let mut interval = tokio::time::interval(Duration::from_millis(tick_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            interval.tick().await;

            if !self.is_leader().await {
                continue;
            }

            let Some(_running_guard) = RunningFlagGuard::try_acquire(self.insight_running.clone())
            else {
                tracing::debug!("Insight loop is still busy; skipping this tick.");
                continue;
            };

            if let Err(error) = self.run_insight_cycle().await {
                tracing::error!("Insight loop failed: {:?}", error);
            }
        }
    }

    async fn run_compaction_cycle(&self) -> Result<()> {
        let compaction_interval = Duration::from_secs(self.config.compaction_interval_secs.max(1));
        let should_compact = {
            let last = self.last_compaction.lock().await;
            last.elapsed() > compaction_interval
        };

        if should_compact {
            tracing::info!("Running LanceDB compaction...");
            self.engine.compact_vector_store().await?;
            let mut last = self.last_compaction.lock().await;
            *last = std::time::Instant::now();
        }
        Ok(())
    }

    async fn run_l3_task_cycle(&self) -> Result<()> {
        // Find all users
        let kv = self.engine.kv();
        let results = kv.scan(b"u:")?;
        let mut users = std::collections::HashSet::new();
        for (k, _) in results {
            let key_str = String::from_utf8_lossy(&k);
            let parts: Vec<&str> = key_str.split(':').collect();
            if parts.len() >= 2 {
                users.insert(parts[1].to_string());
            }
        }

        for user_id in users {
            let ready_tasks = match self.engine.get_ready_l3_tasks(&user_id).await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!("Failed to fetch ready tasks for user {}: {}", user_id, e);
                    continue;
                }
            };

            for mut task in ready_tasks {
                tracing::info!("Auto-processing L3 task: {}", task.title);
                task.status = memorose_common::TaskStatus::InProgress;
                let _ = self.engine.store_l3_task(&task).await;

                let mut summary = String::from(
                    "Task automatically completed by backend worker without LLM interaction.",
                );

                // If we have an LLM client, generate an insight for the milestone
                if let Some(client) = &self.llm_client {
                    let prompt = format!("You are an autonomous agent executing a planned milestone.\nTask: {}\nDescription: {}\nPlease provide a brief, professional summary of the simulated execution of this task.", task.title, task.description);
                    if let Ok(res) = client.generate(&prompt).await {
                        summary = res.data.clone();
                    }
                }

                task.status = memorose_common::TaskStatus::Completed;
                task.progress = 1.0;
                task.result_summary = Some(summary.clone());
                task.updated_at = chrono::Utc::now();
                let _ = self.engine.store_l3_task(&task).await;

                // Sediment the completed milestone as an event in L0
                let event = memorose_common::Event::new(
                    task.org_id.clone(),
                    user_id.clone(),
                    task.agent_id
                        .clone()
                        .or_else(|| Some("system_worker".to_string())),
                    uuid::Uuid::new_v4(),
                    memorose_common::EventContent::Text(format!(
                        "Completed Milestone '{}': {}",
                        task.title, summary
                    )),
                );
                let _ = self.engine.ingest_event(event).await;
            }
        }
        Ok(())
    }

    async fn run_decay_cycle(&self) -> Result<()> {
        if !self.config.forgetting_enabled {
            return Ok(());
        }

        let decay_interval = Duration::from_secs(self.config.decay_interval_secs.max(1));
        let should_decay = {
            let last = self.last_decay.lock().await;
            last.elapsed() > decay_interval
        };

        if should_decay {
            tracing::info!("Running memory decay and pruning...");

            // Scan active_user markers to find users needing decay
            let skv = self.engine.system_kv();
            let active_pairs =
                tokio::task::spawn_blocking(move || skv.scan(b"active_user:")).await??;

            for (key, _) in active_pairs {
                let key_str = String::from_utf8(key)?;
                if let Some(user_id) = key_str.strip_prefix("active_user:") {
                    self.engine
                        .decay_importance(user_id, self.config.decay_factor)
                        .await?;

                    let pruned = self
                        .engine
                        .prune_memories(user_id, self.config.prune_threshold)
                        .await?;
                    if pruned > 0 {
                        tracing::info!(
                            "Pruned {} low-importance memories for user {}",
                            pruned,
                            user_id
                        );
                    }
                }
            }

            let mut last = self.last_decay.lock().await;
            *last = std::time::Instant::now();
        }
        Ok(())
    }

    fn parse_metadata_embedding(metadata: &serde_json::Value) -> Option<Option<Vec<f32>>> {
        metadata
            .get("embedding")
            .and_then(|v| v.as_array())
            .map(|values| {
                values
                    .iter()
                    .map(|v| v.as_f64().map(|f| f as f32))
                    .collect::<Option<Vec<f32>>>()
                    .filter(|vec| !vec.is_empty())
            })
    }

    fn pending_input_from_embed_input(
        input: EmbedInput,
    ) -> crate::engine::PendingMaterializationInput {
        match input {
            EmbedInput::Text(text) => crate::engine::PendingMaterializationInput::Text(text),
            EmbedInput::Multimodal { parts } => crate::engine::PendingMaterializationInput::Multimodal {
                parts: parts
                    .into_iter()
                    .map(|part| match part {
                        EmbedPart::Text(text) => {
                            crate::engine::PendingMaterializationPart::Text { text }
                        }
                        EmbedPart::InlineData { mime_type, data } => {
                            crate::engine::PendingMaterializationPart::InlineData {
                                mime_type,
                                data,
                            }
                        }
                    })
                    .collect(),
            },
        }
    }

    fn embed_input_from_pending_input(
        input: crate::engine::PendingMaterializationInput,
    ) -> EmbedInput {
        match input {
            crate::engine::PendingMaterializationInput::Text(text) => EmbedInput::Text(text),
            crate::engine::PendingMaterializationInput::Multimodal { parts } => {
                EmbedInput::Multimodal {
                    parts: parts
                        .into_iter()
                        .map(|part| match part {
                            crate::engine::PendingMaterializationPart::Text { text } => {
                                EmbedPart::Text(text)
                            }
                            crate::engine::PendingMaterializationPart::InlineData {
                                mime_type,
                                data,
                            } => EmbedPart::InlineData { mime_type, data },
                        })
                        .collect(),
                }
            }
        }
    }

    async fn publish_materialization_job(
        &self,
        mut job: crate::engine::PendingMaterializationJob,
    ) -> Result<bool> {
        if let Some(existing) = self
            .engine
            .get_memory_unit_including_forgotten(&job.unit.user_id, job.unit.id)?
        {
            if existing.visible
                && existing.materialization_state == memorose_common::MaterializationState::Published
            {
                self.run_post_publish_hooks_once(&existing, &job.post_publish_edges)
                    .await?;
                self.engine.delete_materialization_job(&job)?;
                return Ok(true);
            }
        }

        job.unit.visible = true;
        job.unit.materialization_state = memorose_common::MaterializationState::Published;
        job.unit.materialized_at = Some(chrono::Utc::now());

        self.engine
            .publish_materialized_memory_unit(&job.unit)
            .await?;

        self.run_post_publish_hooks_once(&job.unit, &job.post_publish_edges)
            .await?;
        self.engine.delete_materialization_job(&job)?;
        Ok(true)
    }

    async fn run_post_publish_hooks_once(
        &self,
        unit: &MemoryUnit,
        staged_edges: &[GraphEdge],
    ) -> Result<()> {
        if self.engine.materialization_post_publish_applied(unit.id)? {
            return Ok(());
        }

        self.run_post_publish_hooks(std::slice::from_ref(unit), staged_edges)
            .await?;
        self.engine
            .mark_materialization_post_publish_applied(unit.id)?;
        Ok(())
    }

    async fn run_post_publish_hooks(
        &self,
        units: &[MemoryUnit],
        staged_edges: &[GraphEdge],
    ) -> Result<()> {
        for unit in units {
            self.engine
                .run_published_memory_unit_side_effects(unit)
                .await?;
        }

        for edge in staged_edges {
            self.engine.graph().add_edge(edge).await?;
        }

        let mut l1_increase_by_user: HashMap<String, usize> = HashMap::new();
        for unit in units {
            if unit.level == 1 {
                *l1_increase_by_user.entry(unit.user_id.clone()).or_insert(0) += 1;
            }
        }

        let community_step = self.config.community_trigger_l1_step.max(1);
        for (user_id, delta) in l1_increase_by_user {
            if let Ok((before, after)) = self.engine.bump_l1_count_and_get_range(&user_id, delta).await {
                if before / community_step < after / community_step && after >= community_step {
                    let _ = self.engine.set_needs_community(&user_id);
                }
            }
        }

        if self.engine.task_reflection {
            for unit in units {
                if let Some(ref meta) = unit.task_metadata {
                    if meta.status == memorose_common::TaskStatus::Completed {
                        if let Ok(incoming) = self
                            .engine
                            .graph()
                            .get_incoming_edges(&unit.user_id, unit.id)
                            .await
                        {
                            for edge in incoming {
                                if edge.relation == memorose_common::RelationType::IsSubTaskOf {
                                    let _ = self
                                        .update_parent_progress(&unit.user_id, edge.source_id)
                                        .await;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn run_materialization_cycle(&self) -> Result<bool> {
        let limit = self.config.consolidation_store_batch_size.max(1);
        let mut jobs = self.engine.fetch_due_materialization_jobs(limit)?;
        if jobs.is_empty() {
            return Ok(false);
        }

        let mut any_published = false;
        let mut ready_jobs = Vec::new();
        let mut jobs_needing_embedding = Vec::new();

        for job in jobs.drain(..) {
            if let Some(existing) = self
                .engine
                .get_memory_unit_including_forgotten(&job.unit.user_id, job.unit.id)?
            {
                if existing.visible
                    && existing.materialization_state
                        == memorose_common::MaterializationState::Published
                {
                    ready_jobs.push(job);
                    continue;
                }
            }

            if job.unit.embedding.is_some() {
                ready_jobs.push(job);
            } else {
                jobs_needing_embedding.push(job);
            }
        }

        for mut job in ready_jobs {
            match self.publish_materialization_job(job.clone()).await {
                Ok(published) => any_published |= published,
                Err(error) => {
                    let error_message = format!("Materialization publish failed: {:?}", error);
                    if job.attempts >= self.config.consolidation_max_retries {
                        self.engine.fail_materialization_job(&mut job, &error_message)?;
                    } else {
                        self.engine
                            .reschedule_materialization_job(&mut job, &error_message)?;
                    }
                }
            }
        }

        if jobs_needing_embedding.is_empty() {
            return Ok(any_published);
        }

        let Some(client) = self.llm_client.as_ref() else {
            let max_retries = self.config.consolidation_max_retries;
            for mut job in jobs_needing_embedding {
                let error = "No LLM client available for materialization embedding";
                if job.attempts >= max_retries {
                    self.engine.fail_materialization_job(&mut job, error)?;
                } else {
                    self.engine.reschedule_materialization_job(&mut job, error)?;
                }
            }
            return Ok(any_published);
        };

        let inputs_to_embed = jobs_needing_embedding
            .iter()
            .map(|job| {
                job.embed_input
                    .clone()
                    .map(Self::embed_input_from_pending_input)
                    .unwrap_or_else(|| EmbedInput::Text(job.unit.content.clone()))
            })
            .collect::<Vec<_>>();

        match client.embed_content_batch(inputs_to_embed).await {
            Ok(response) if response.data.len() == jobs_needing_embedding.len() => {
                for (mut job, embedding) in jobs_needing_embedding
                    .into_iter()
                    .zip(response.data.into_iter())
                {
                    if embedding.is_empty() {
                        let error = "Materialization embedding returned empty vector";
                        if job.attempts >= self.config.consolidation_max_retries {
                            self.engine.fail_materialization_job(&mut job, error)?;
                        } else {
                            self.engine.reschedule_materialization_job(&mut job, error)?;
                        }
                        continue;
                    }

                    job.unit.embedding = Some(embedding);
                    match self.publish_materialization_job(job.clone()).await {
                        Ok(published) => any_published |= published,
                        Err(error) => {
                            let error_message =
                                format!("Materialization publish after embedding failed: {:?}", error);
                            if job.attempts >= self.config.consolidation_max_retries {
                                self.engine.fail_materialization_job(&mut job, &error_message)?;
                            } else {
                                self.engine
                                    .reschedule_materialization_job(&mut job, &error_message)?;
                            }
                        }
                    }
                }
            }
            Ok(response) => {
                let max_retries = self.config.consolidation_max_retries;
                let error = format!(
                    "Materialization embedding size mismatch: expected={}, got={}",
                    jobs_needing_embedding.len(),
                    response.data.len()
                );
                for mut job in jobs_needing_embedding {
                    if job.attempts >= max_retries {
                        self.engine.fail_materialization_job(&mut job, &error)?;
                    } else {
                        self.engine.reschedule_materialization_job(&mut job, &error)?;
                    }
                }
            }
            Err(error) => {
                let max_retries = self.config.consolidation_max_retries;
                let error_message = format!("Materialization embedding failed: {:?}", error);
                for mut job in jobs_needing_embedding {
                    if job.attempts >= max_retries {
                        self.engine.fail_materialization_job(&mut job, &error_message)?;
                    } else {
                        self.engine
                            .reschedule_materialization_job(&mut job, &error_message)?;
                    }
                }
            }
        }

        Ok(any_published)
    }

    /// Generates a semantic fingerprint by stripping numbers, punctuation, and converting to lowercase.
    /// This allows us to catch highly similar structural logs (e.g. "Tool failed at 12:01" vs "Tool failed at 12:02").
    fn generate_semantic_fingerprint(text: &str) -> u64 {
        let normalized: String = text
            .chars()
            .filter(|c| c.is_alphabetic() || c.is_whitespace())
            .map(|c| c.to_ascii_lowercase())
            .collect();

        // Reduce multiple whitespaces to single space for stable hashing
        let collapsed: String = normalized.split_whitespace().collect::<Vec<_>>().join(" ");

        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        collapsed.hash(&mut hasher);
        hasher.finish()
    }

    async fn extract_text_and_embed_input(
        event: &memorose_common::Event,
        llm: Option<&dyn crate::llm::LLMClient>,
    ) -> (String, EmbedInput, Vec<Asset>) {
        match &event.content {
            memorose_common::EventContent::Text(t) => {
                (t.clone(), EmbedInput::Text(t.clone()), vec![])
            }
            memorose_common::EventContent::Image(url) => {
                // For native multimodal embedding, try to fetch bytes for inline embedding
                let text_description = if let Some(client) = llm {
                    client
                        .describe_image(url)
                        .await
                        .map(|r| r.data)
                        .unwrap_or_else(|_| format!("Image at {}", url))
                } else {
                    format!("Image at {}", url)
                };

                let embed_input = if url.starts_with("http") {
                    // URL-based: fetch the bytes for inline embedding
                    match reqwest::get(url).await {
                        Ok(resp) => {
                            let mime = resp
                                .headers()
                                .get("content-type")
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("image/jpeg")
                                .to_string();
                            match resp.bytes().await {
                                Ok(bytes) => EmbedInput::Multimodal {
                                    parts: vec![EmbedPart::InlineData {
                                        mime_type: mime,
                                        data: base64::Engine::encode(
                                            &base64::engine::general_purpose::STANDARD,
                                            &bytes,
                                        ),
                                    }],
                                },
                                Err(_) => EmbedInput::Text(text_description.clone()),
                            }
                        }
                        Err(_) => EmbedInput::Text(text_description.clone()),
                    }
                } else {
                    // Already base64
                    EmbedInput::Multimodal {
                        parts: vec![EmbedPart::InlineData {
                            mime_type: "image/jpeg".to_string(),
                            data: url.clone(),
                        }],
                    }
                };

                (
                    text_description.clone(),
                    embed_input,
                    vec![Self::build_asset(
                        url.clone(),
                        "image",
                        "image",
                        Some(text_description),
                    )],
                )
            }
            memorose_common::EventContent::Audio(url) => {
                let text_description = if let Some(client) = llm {
                    client
                        .transcribe(url)
                        .await
                        .map(|r| r.data)
                        .unwrap_or_else(|_| format!("Audio at {}", url))
                } else {
                    format!("Audio at {}", url)
                };

                let embed_input = if url.starts_with("http") {
                    match reqwest::get(url).await {
                        Ok(resp) => {
                            let mime = resp
                                .headers()
                                .get("content-type")
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("audio/mp3")
                                .to_string();
                            match resp.bytes().await {
                                Ok(bytes) => EmbedInput::Multimodal {
                                    parts: vec![EmbedPart::InlineData {
                                        mime_type: mime,
                                        data: base64::Engine::encode(
                                            &base64::engine::general_purpose::STANDARD,
                                            &bytes,
                                        ),
                                    }],
                                },
                                Err(_) => EmbedInput::Text(text_description.clone()),
                            }
                        }
                        Err(_) => EmbedInput::Text(text_description.clone()),
                    }
                } else {
                    EmbedInput::Multimodal {
                        parts: vec![EmbedPart::InlineData {
                            mime_type: "audio/mp3".to_string(),
                            data: url.clone(),
                        }],
                    }
                };

                (
                    text_description.clone(),
                    embed_input,
                    vec![Self::build_asset(
                        url.clone(),
                        "audio",
                        "audio",
                        Some(text_description),
                    )],
                )
            }
            memorose_common::EventContent::Video(url) => {
                let text_description = if let Some(client) = llm {
                    client
                        .describe_video(url)
                        .await
                        .map(|r| r.data)
                        .unwrap_or_else(|_| format!("Video at {}", url))
                } else {
                    format!("Video at {}", url)
                };

                let embed_input = if url.starts_with("http") {
                    match reqwest::get(url).await {
                        Ok(resp) => {
                            let mime = resp
                                .headers()
                                .get("content-type")
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("video/mp4")
                                .to_string();
                            match resp.bytes().await {
                                Ok(bytes) => EmbedInput::Multimodal {
                                    parts: vec![EmbedPart::InlineData {
                                        mime_type: mime,
                                        data: base64::Engine::encode(
                                            &base64::engine::general_purpose::STANDARD,
                                            &bytes,
                                        ),
                                    }],
                                },
                                Err(_) => EmbedInput::Text(text_description.clone()),
                            }
                        }
                        Err(_) => EmbedInput::Text(text_description.clone()),
                    }
                } else {
                    EmbedInput::Multimodal {
                        parts: vec![EmbedPart::InlineData {
                            mime_type: "video/mp4".to_string(),
                            data: url.clone(),
                        }],
                    }
                };

                (
                    text_description.clone(),
                    embed_input,
                    vec![Self::build_asset(
                        url.clone(),
                        "video",
                        "video",
                        Some(text_description),
                    )],
                )
            }
            memorose_common::EventContent::Json(val) => {
                let text = val.to_string();
                (text.clone(), EmbedInput::Text(text), vec![])
            }
        }
    }

    async fn run_consolidation_cycle(&self) -> Result<bool> {
        let consolidation_interval = Duration::from_millis(
            self.config
                .consolidation_interval_ms
                .max(self.config.tick_interval_ms),
        );
        let should_run = {
            let last = self.last_consolidation.lock().await;
            last.elapsed() > consolidation_interval
        };
        if !should_run {
            return Ok(false);
        }

        let batch_size = self.config.consolidation_batch_size.max(1);
        let fetch_limit =
            batch_size.saturating_mul(self.config.consolidation_fetch_multiplier.max(1));
        let events = self
            .engine
            .fetch_pending_events_limited(fetch_limit)
            .await?;
        if events.is_empty() {
            return Ok(false);
        }

        // 1. Filter valid events
        let max_retries = self.config.consolidation_max_retries;
        let mut valid_events = Vec::new();
        let mut failed_events = Vec::new();

        for event in events {
            let retry_count = self
                .engine
                .get_retry_count(&event.id.to_string())
                .await
                .unwrap_or(0);
            if retry_count >= max_retries {
                tracing::warn!(
                    "Event {} exceeded max retries ({}/{}), moving to failed queue",
                    event.id,
                    retry_count,
                    max_retries
                );
                failed_events.push(event);
            } else {
                valid_events.push(event);
            }
        }

        // Mark failed
        for event in failed_events {
            if let Err(e) = self
                .engine
                .mark_event_failed(
                    &event.id.to_string(),
                    &format!("Exceeded max retries ({})", max_retries),
                )
                .await
            {
                tracing::error!("Failed to mark event {} as failed: {:?}", event.id, e);
            }
        }

        if valid_events.is_empty() {
            return Ok(false);
        }

        valid_events.sort_by_key(|event| {
            (
                Self::packed_event_key(event),
                event.transaction_time,
            )
        });

        // 1.5 Batching / Prompt Packing with overfetch + fair selection
        let pending_valid_count = valid_events.len();
        let packed_batches = self.pack_events_for_consolidation(valid_events);
        let scheduled_batches = self.schedule_packed_groups_fairly(packed_batches);
        let scheduled_batches =
            self.limit_scheduled_groups_by_event_budget(scheduled_batches, batch_size);
        let distinct_keys = scheduled_batches
            .iter()
            .map(|group| group.key.clone())
            .collect::<HashSet<_>>()
            .len();
        let selected_event_count = scheduled_batches
            .iter()
            .map(|group| group.events.len())
            .sum::<usize>();
        let all_fetched_ids: Vec<String> = scheduled_batches
            .iter()
            .flat_map(|group| group.events.iter().map(|event| event.id.to_string()))
            .collect();

        if scheduled_batches.is_empty() {
            return Ok(false);
        }

        tracing::info!(
            "Consolidating {} packed event groups via pipeline (selected_events={}, deferred_events={}, keys={}, concurrency={}, fetch_limit={}, target_tokens={}, max_events_per_pack={}, store_batch_size={})...",
            scheduled_batches.len(),
            selected_event_count,
            pending_valid_count.saturating_sub(selected_event_count),
            distinct_keys,
            self.config.llm_concurrency,
            fetch_limit,
            self.config.consolidation_target_tokens,
            self.config.consolidation_max_events_per_pack,
            self.config.consolidation_store_batch_size
        );

        // 2. Pipeline: Producer (Compress) -> Channel -> Consumer (Embed & Store)
        let (tx, mut rx) = mpsc::channel(self.config.llm_concurrency * 2);
        let llm_client_clone = self.llm_client.clone();
        let concurrency_limit = self.config.llm_concurrency;
        let engine_clone = self.engine.clone();

        // Spawn Producer — keep the handle so we can detect panics after the consumer drains.
        let producer_handle = tokio::spawn(async move {
            let mut join_set = tokio::task::JoinSet::new();

            for packed_group in scheduled_batches {
                let PackedEventGroup {
                    key,
                    seq_no,
                    events,
                } = packed_group;

                if events.is_empty() {
                    continue;
                }
                let llm = llm_client_clone.clone();
                let engine = engine_clone.clone();

                // Limit concurrency
                if join_set.len() >= concurrency_limit {
                    if let Some(res) = join_set.join_next().await {
                        match res {
                            Ok(data) => {
                                if tx.send(data).await.is_err() {
                                    tracing::error!("Compression pipeline: consumer channel closed; dropping batch");
                                }
                            }
                            Err(e) => tracing::error!("Compression task panicked: {:?}", e),
                        }
                    }
                }

                join_set.spawn(async move {
                    let mut events_iter = events.into_iter();
                    let first_event = events_iter
                        .next()
                        .expect("packed group must contain at least one event");
                    let (first_text, first_embed_input, mut assets) =
                        Self::extract_text_and_embed_input(&first_event, llm.as_deref()).await;
                    let mut combined_text = format!("Message 1: {}", first_text);
                    let embed_input = if first_embed_input.has_multimodal_parts() {
                        Some(first_embed_input)
                    } else {
                        None
                    };

                    let metadata = first_event.metadata.clone();
                    let user_id = first_event.user_id.clone();
                    let stream_id = first_event.stream_id;
                    let is_agent =
                        metadata.get("role").and_then(|v| v.as_str()) == Some("assistant")
                            || metadata.get("agent_id").is_some();
                    let mut event_ids = vec![first_event.id];

                    for (index, evt) in events_iter.enumerate() {
                        let (evt_text, _evt_embed_input, evt_assets) =
                            Self::extract_text_and_embed_input(&evt, llm.as_deref()).await;
                        combined_text.push_str(&format!("\nMessage {}: {}", index + 2, evt_text));
                        event_ids.push(evt.id);
                        assets.extend(evt_assets);
                    }

                    // Semantic Deduplication Check
                    let fingerprint = Self::generate_semantic_fingerprint(&combined_text);
                    let dedup_key = format!("dedup:{}:{}", user_id, fingerprint);

                    let is_duplicate = if let Ok(Some(last_seen_bytes)) = engine.system_kv().get(dedup_key.as_bytes()) {
                        if let Some(last_seen) = String::from_utf8(last_seen_bytes).ok().and_then(|s| s.parse::<i64>().ok()) {
                            let now = chrono::Utc::now().timestamp();
                            // Deduplicate if seen within the last 1 hour (3600 seconds).
                            // Use saturating_sub so clock-skew or a future stored timestamp
                            // never causes underflow (which would bypass deduplication).
                            now.saturating_sub(last_seen) < 3600
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    let (summary, valid_at) = if is_duplicate {
                        tracing::debug!("Semantic deduplication triggered for fingerprint {}. Skipping LLM compression.", fingerprint);
                        // Update timestamp for LRU-like rolling window
                        let _ = engine.system_kv().put(dedup_key.as_bytes(), chrono::Utc::now().timestamp().to_string().as_bytes());
                        (combined_text, None)
                    } else {
                        // Compression
                        let (compressed, valid) = match llm.as_ref() {
                            Some(client) => match client.compress(&combined_text, is_agent).await {
                                Ok(out) => (out.data.content, out.data.valid_at),
                                Err(e) => {
                                    tracing::warn!("Packed compression failed for {}: {:?}", event_ids[0], e);
                                    (combined_text, None)
                                }
                            },
                            None => (combined_text, None),
                        };

                        // Save fingerprint
                        let _ = engine.system_kv().put(dedup_key.as_bytes(), chrono::Utc::now().timestamp().to_string().as_bytes());

                        (compressed, valid)
                    };

                    ProducedBatch {
                        key,
                        seq_no,
                        event_ids,
                        user_id,
                        stream_id,
                        summary,
                        valid_at,
                        assets,
                        metadata,
                        embed_input,
                    }
                });
            }

            // Drain remaining
            while let Some(res) = join_set.join_next().await {
                match res {
                    Ok(data) => {
                        if tx.send(data).await.is_err() {
                            tracing::error!(
                                "Compression pipeline: consumer channel closed; dropping batch"
                            );
                        }
                    }
                    Err(e) => tracing::error!("Compression task panicked: {:?}", e),
                }
            }
        });

        // 3. Consumer Loop (Embed & Store)
        let mut buffer = Vec::new();
        let mini_batch_size = self.config.consolidation_store_batch_size.max(1);
        let mut processed_ids = std::collections::HashSet::new();
        let mut any_processed = false;
        let mut next_commit_seq_by_key: HashMap<PackedGroupKey, u64> = HashMap::new();
        let mut pending_by_key: HashMap<PackedGroupKey, HashMap<u64, ProducedBatch>> =
            HashMap::new();

        while let Some(item) = rx.recv().await {
            let key = item.key.clone();
            let pending = pending_by_key.entry(key.clone()).or_default();
            pending.insert(item.seq_no, item);

            let next_seq = next_commit_seq_by_key.entry(key.clone()).or_insert(0);
            while let Some(ready) = pending.remove(next_seq) {
                buffer.push((
                    ready.event_ids,
                    ready.user_id,
                    ready.stream_id,
                    ready.summary,
                    ready.valid_at,
                    ready.assets,
                    ready.metadata,
                    ready.embed_input,
                ));
                *next_seq += 1;
            }
            if pending.is_empty() {
                pending_by_key.remove(&key);
            }
            if buffer.len() >= mini_batch_size {
                let batch: Vec<_> = buffer.drain(..).collect();
                match self.process_pipeline_batch(batch).await {
                    Ok(ids) => {
                        processed_ids.extend(ids);
                        any_processed = true;
                    }
                    Err(error) => {
                        tracing::error!("Consolidation pipeline batch failed: {:?}", error);
                    }
                }
            }
        }

        // Process remaining
        if !buffer.is_empty() {
            match self.process_pipeline_batch(buffer).await {
                Ok(ids) => {
                    processed_ids.extend(ids);
                    any_processed = true;
                }
                Err(error) => {
                    tracing::error!("Consolidation pipeline tail batch failed: {:?}", error);
                }
            }
        }

        // Wait for the producer to finish.  The consumer loop above only exits once the
        // sender side of the channel is dropped (i.e., the producer task completed), so
        // this await should return immediately.  We still check the result to surface panics.
        if let Err(e) = producer_handle.await {
            tracing::error!("Consolidation producer task panicked: {:?}", e);
        }

        // 4. Retry Logic: Check which IDs were NOT processed
        for id in all_fetched_ids {
            // If ID is not in processed_ids and not in failed_events (already handled), increment retry
            // Note: failed_events logic handled above. We only care about valid_events that failed in pipeline.
            // But 'id' here includes all initial fetch.
            // Simplified: Try to increment retry for anything that wasn't successfully marked processed.
            // Mark_event_processed deletes the pending key, so increment_retry_count_if_pending works safely.
            if !processed_ids.contains(&id) {
                // If it's already deleted (e.g. marked failed), this does nothing.
                let _ = self.engine.increment_retry_count_if_pending(&id).await;
            }
        }

        *self.last_consolidation.lock().await = std::time::Instant::now();
        Ok(any_processed)
    }

    /// Helper for pipeline batch processing
    async fn reconcile_staged_units_before_store(
        &self,
        units: &mut Vec<MemoryUnit>,
    ) -> Result<Vec<GraphEdge>> {
        let mut removed_ids = HashSet::new();
        let mut staged_edges = Vec::new();
        let mut edge_keys = HashSet::new();

        for index in 0..units.len() {
            let unit = units[index].clone();
            if removed_ids.contains(&unit.id)
                || unit.level != 1
                || unit.memory_type != memorose_common::MemoryType::Factual
                || !matches!(
                    unit.domain,
                    memorose_common::MemoryDomain::Agent | memorose_common::MemoryDomain::User
                )
            {
                continue;
            }

            let context = units[..index]
                .iter()
                .filter(|candidate| {
                    !removed_ids.contains(&candidate.id)
                        && candidate.user_id == unit.user_id
                        && candidate.level == 1
                        && candidate.memory_type == memorose_common::MemoryType::Factual
                        && matches!(
                            candidate.domain,
                            memorose_common::MemoryDomain::Agent
                                | memorose_common::MemoryDomain::User
                        )
                })
                .cloned()
                .collect::<Vec<_>>();

            if context.is_empty() {
                continue;
            }

            let actions = self
                .engine
                .detect_memory_correction_actions(&unit, &context)
                .await?;

            for action in actions {
                if action.target_id == unit.id || removed_ids.contains(&action.target_id) {
                    continue;
                }

                let Some(target_unit) = units[..index]
                    .iter()
                    .find(|candidate| candidate.id == action.target_id)
                else {
                    continue;
                };

                let decision = self
                    .engine
                    .validate_memory_correction_relation(
                        &unit,
                        target_unit,
                        action.kind,
                        action.confidence,
                    )
                    .await;

                match decision {
                    crate::engine::ValidatedCorrectionDecision::Tombstone { relation } => {
                        removed_ids.insert(action.target_id);
                        let _ = self.engine.record_rac_decision_with_review(
                            &crate::engine::RacDecisionRecord {
                                created_at: chrono::Utc::now(),
                                stage: "staged_pre_store".into(),
                                user_id: unit.user_id.clone(),
                                org_id: unit.org_id.clone(),
                                source_unit_id: unit.id,
                                target_unit_id: Some(action.target_id),
                                action: format!("{:?}", action.kind).to_ascii_lowercase(),
                                confidence: action.confidence,
                                effect: crate::engine::RacDecisionEffect::Tombstone,
                                relation: Some(format!("{:?}", relation).to_ascii_lowercase()),
                                reason: action.reason.clone(),
                                guard_reason: None,
                            },
                        );
                    }
                    crate::engine::ValidatedCorrectionDecision::RelationOnly {
                        relation,
                        guard_reason,
                    } => {
                        let relation_name = format!("{:?}", relation).to_ascii_lowercase();
                        let edge_key = (
                            unit.user_id.clone(),
                            unit.id,
                            action.target_id,
                            relation_name.clone(),
                        );
                        if edge_keys.insert(edge_key) {
                            staged_edges.push(GraphEdge::new(
                                unit.user_id.clone(),
                                unit.id,
                                action.target_id,
                                relation.clone(),
                                action.confidence,
                            ));
                        }
                        let _ = self.engine.record_rac_decision_with_review(
                            &crate::engine::RacDecisionRecord {
                                created_at: chrono::Utc::now(),
                                stage: "staged_pre_store".into(),
                                user_id: unit.user_id.clone(),
                                org_id: unit.org_id.clone(),
                                source_unit_id: unit.id,
                                target_unit_id: Some(action.target_id),
                                action: format!("{:?}", action.kind).to_ascii_lowercase(),
                                confidence: action.confidence,
                                effect: crate::engine::RacDecisionEffect::RelationOnly,
                                relation: Some(relation_name),
                                reason: action.reason.clone(),
                                guard_reason,
                            },
                        );
                    }
                    crate::engine::ValidatedCorrectionDecision::Skip {
                        effect,
                        guard_reason,
                    } => {
                        let _ = self.engine.record_rac_decision_with_review(
                            &crate::engine::RacDecisionRecord {
                                created_at: chrono::Utc::now(),
                                stage: "staged_pre_store".into(),
                                user_id: unit.user_id.clone(),
                                org_id: unit.org_id.clone(),
                                source_unit_id: unit.id,
                                target_unit_id: Some(action.target_id),
                                action: format!("{:?}", action.kind).to_ascii_lowercase(),
                                confidence: action.confidence,
                                effect,
                                relation: None,
                                reason: action.reason.clone(),
                                guard_reason: Some(guard_reason),
                            },
                        );
                    }
                }
            }
        }

        units.retain(|unit| !removed_ids.contains(&unit.id));

        let surviving_ids = units.iter().map(|unit| unit.id).collect::<HashSet<_>>();
        staged_edges.retain(|edge| {
            surviving_ids.contains(&edge.source_id) && surviving_ids.contains(&edge.target_id)
        });

        Ok(staged_edges)
    }

    async fn process_pipeline_batch(
        &self,
        batch: Vec<(
            Vec<uuid::Uuid>,
            String,
            uuid::Uuid,
            String,
            Option<String>,
            Vec<Asset>,
            serde_json::Value,
            Option<EmbedInput>,
        )>,
    ) -> Result<Vec<String>> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        let mut staged_units = Vec::new();
        let mut processed_ids = Vec::new();

        for (
            _idx,
            (event_ids, user_id, stream_id, summary, valid_at, assets, metadata, embed_input),
        ) in batch.into_iter().enumerate()
        {
            let embedding = match Self::parse_metadata_embedding(&metadata) {
                Some(Some(vec)) => Some(vec),
                Some(None) | None => None,
            };

            let is_agent = metadata.get("role").and_then(|v| v.as_str()) == Some("assistant")
                || metadata.get("agent_id").is_some();

            let agent_id = metadata
                .get("agent_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let memory_type = if is_agent {
                memorose_common::MemoryType::Procedural
            } else {
                memorose_common::MemoryType::Factual
            };

            let mut unit = MemoryUnit::new(
                None,
                user_id,
                agent_id,
                stream_id,
                memory_type,
                summary,
                embedding,
            );
            unit.valid_time = valid_at.and_then(|s| {
                chrono::DateTime::parse_from_rfc3339(&s)
                    .ok()
                    .map(|d| d.with_timezone(&chrono::Utc))
            });
            unit.assets = assets;

            // Link to all source events
            for evt_id in &event_ids {
                unit.references.push(*evt_id);
            }

            // Task Metadata Logic
            if let Some(level) = metadata.get("target_level").and_then(|v| v.as_u64()) {
                unit.level = level as u8;
                if let Some(pid_str) = metadata.get("parent_id").and_then(|v| v.as_str()) {
                    if let Ok(pid) = uuid::Uuid::parse_str(pid_str) {
                        unit.references.push(pid);
                    }
                }
                if level >= 1 {
                    let status = match metadata.get("task_status").and_then(|v| v.as_str()) {
                        Some("Completed") => memorose_common::TaskStatus::Completed,
                        Some("Active") => memorose_common::TaskStatus::InProgress,
                        Some("Failed") => memorose_common::TaskStatus::Failed(
                            "Metadata indicated failure".to_string(),
                        ),
                        _ => memorose_common::TaskStatus::Pending,
                    };
                    unit.task_metadata = Some(memorose_common::TaskMetadata {
                        status,
                        progress: metadata
                            .get("task_progress")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(0.0) as f32,
                    });
                }
            }

            self.hydrate_extracted_facts(&mut unit).await;
            let pending_input = if unit.embedding.is_some() {
                None
            } else {
                Some(Self::pending_input_from_embed_input(
                    embed_input.unwrap_or_else(|| EmbedInput::Text(unit.content.clone())),
                ))
            };
            staged_units.push((unit, pending_input));
            for evt_id in event_ids {
                processed_ids.push(evt_id.to_string());
            }
        }

        if !staged_units.is_empty() {
            let mut pending_input_by_unit = staged_units
                .iter()
                .map(|(unit, pending_input)| (unit.id, pending_input.clone()))
                .collect::<HashMap<_, _>>();
            let mut units_to_stage = staged_units
                .into_iter()
                .map(|(unit, _)| unit)
                .collect::<Vec<_>>();
            let staged_edges = self
                .reconcile_staged_units_before_store(&mut units_to_stage)
                .await?;

            for unit in &units_to_stage {
                if unit.level == 1
                    && unit.memory_type == memorose_common::MemoryType::Factual
                    && matches!(
                        unit.domain,
                        memorose_common::MemoryDomain::Agent | memorose_common::MemoryDomain::User
                    )
                {
                    if let Err(error) = self.engine.reconcile_conflicting_memory_unit(unit).await {
                        tracing::error!(
                            "Pre-store memory reconciliation failed for unit {}: {:?}",
                            unit.id,
                            error
                        );
                    }
                }
            }

            let mut edges_by_source = HashMap::<uuid::Uuid, Vec<GraphEdge>>::new();
            for edge in staged_edges {
                edges_by_source.entry(edge.source_id).or_default().push(edge);
            }

            let jobs = units_to_stage
                .into_iter()
                .map(|unit| {
                    let unit_id = unit.id;
                    crate::engine::PendingMaterializationJob::new(
                        unit,
                        edges_by_source.remove(&unit_id).unwrap_or_default(),
                        pending_input_by_unit.remove(&unit_id).flatten(),
                    )
                })
                .collect::<Vec<_>>();
            self.engine.enqueue_materialization_jobs(jobs)?;
        }

        // Mark processed
        for eid in &processed_ids {
            self.engine.mark_event_processed(eid).await?;
        }

        Ok(processed_ids)
    }

    async fn run_community_cycle(&self) -> Result<()> {
        let community_interval = Duration::from_millis(
            self.config
                .community_interval_ms
                .max(self.config.tick_interval_ms),
        );
        let should_run = {
            let last = self.last_community.lock().await;
            last.elapsed() > community_interval
        };
        if !should_run {
            return Ok(());
        }

        let user_ids = self.engine.get_pending_communities()?;
        if user_ids.is_empty() {
            return Ok(());
        }

        let max_users = self.config.community_max_users_per_cycle.max(1);
        let min_members = self.config.community_min_members.max(1);
        let max_groups = self.config.community_max_groups_per_user.max(1);

        tracing::info!(
            "Running L2 Graph Community Detection for up to {} users (queued={})...",
            max_users,
            user_ids.len()
        );

        for user_id in user_ids.into_iter().take(max_users) {
            match self
                .engine
                .process_communities_with_limits(&user_id, min_members, max_groups)
                .await
            {
                Ok(created) => {
                    tracing::debug!(
                        "Community processing finished for user {} (created_l2={})",
                        user_id,
                        created
                    );
                    self.engine.clear_community_marker(&user_id)?;
                }
                Err(e) => {
                    tracing::warn!("Community processing failed for user {}: {:?}", user_id, e);
                }
            }
        }
        *self.last_community.lock().await = std::time::Instant::now();
        Ok(())
    }

    async fn run_insight_cycle(&self) -> Result<()> {
        if self.llm_client.is_none() {
            return Ok(());
        }

        let insight_interval = Duration::from_millis(
            self.config
                .insight_interval_ms
                .max(self.config.tick_interval_ms),
        );
        let should_run = {
            let last = self.last_insight.lock().await;
            last.elapsed() > insight_interval
        };
        if !should_run {
            return Ok(());
        }

        let engine = self.engine.clone();

        // Marker-driven: only process users with needs_reflect markers
        let pending_markers = engine.get_pending_reflection_markers()?;
        if pending_markers.is_empty() {
            return Ok(());
        }

        for (user_id, marker) in pending_markers {
            if !self.should_process_reflection_marker(&marker) {
                continue;
            }
            let mut remaining_marker = marker.clone();
            let max_batches = self.config.insight_max_batches_per_cycle.max(1);

            for batch_no in 0..max_batches {
                if remaining_marker.pending_units == 0 {
                    engine.clear_reflection_marker(&user_id)?;
                    break;
                }

                let reflection_limit = remaining_marker
                    .pending_units
                    .min(self.config.insight_max_l1_per_batch.max(1));
                tracing::info!(
                    "Running user-window reflection (user={}, batch={}, pending_units={}, pending_tokens={}, first_tx_micros={}, limit={}, token_budget={})",
                    user_id,
                    batch_no + 1,
                    remaining_marker.pending_units,
                    remaining_marker.pending_tokens,
                    remaining_marker.first_event_tx_micros,
                    reflection_limit,
                    self.config.insight_batch_target_tokens
                );

                match engine
                    .reflect_on_user_window_batch(
                        &user_id,
                        (remaining_marker.first_event_tx_micros > 0)
                            .then_some(remaining_marker.first_event_tx_micros),
                        remaining_marker.first_event_id.as_deref(),
                        reflection_limit,
                        self.config.insight_batch_target_tokens,
                    )
                    .await
                {
                    Ok(outcome) if outcome.consumed_units == 0 => {
                        engine.clear_reflection_marker(&user_id)?;
                        break;
                    }
                    Ok(outcome) => {
                        tracing::debug!(
                            "User-window reflection batch completed for user {} with {} topics from {} units",
                            user_id,
                            outcome.created_topics,
                            outcome.consumed_units
                        );
                        engine.consume_reflection_marker_batch(
                            &user_id,
                            outcome.consumed_units,
                            outcome.consumed_tokens,
                            outcome.next_first_event_tx_micros,
                            outcome.next_first_event_id.clone(),
                        )?;

                        remaining_marker.pending_units = remaining_marker
                            .pending_units
                            .saturating_sub(outcome.consumed_units);
                        remaining_marker.pending_tokens = remaining_marker
                            .pending_tokens
                            .saturating_sub(outcome.consumed_tokens);
                        remaining_marker.first_event_tx_micros =
                            outcome.next_first_event_tx_micros.unwrap_or_default();
                        remaining_marker.first_event_id = outcome.next_first_event_id;

                        if remaining_marker.pending_units == 0
                            || remaining_marker.first_event_tx_micros <= 0
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "User-window reflection failed for user {}: {:?}",
                            user_id,
                            e
                        );
                        break;
                    }
                }
            }
        }

        *self.last_insight.lock().await = std::time::Instant::now();
        Ok(())
    }

    pub async fn update_parent_progress(&self, user_id: &str, parent_id: uuid::Uuid) -> Result<()> {
        // Atomic locking per task to prevent race conditions during Read-Modify-Write.
        let lock = {
            self.engine
                .task_locks
                .entry(parent_id)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .value()
                .clone()
        };

        {
            let _guard = lock.lock().await;

            let incoming = self
                .engine
                .graph()
                .get_incoming_edges(user_id, parent_id)
                .await?;

            let mut total = 0;
            let mut completed = 0;

            for edge in incoming {
                if edge.relation == memorose_common::RelationType::IsSubTaskOf {
                    total += 1;
                    if let Some(child) =
                        self.engine.get_memory_unit(user_id, edge.source_id).await?
                    {
                        if let Some(ref meta) = child.task_metadata {
                            if meta.status == memorose_common::TaskStatus::Completed {
                                completed += 1;
                            }
                        }
                    }
                }
            }

            if total > 0 {
                let progress = completed as f32 / total as f32;
                if let Some(mut parent) = self.engine.get_memory_unit(user_id, parent_id).await? {
                    let mut meta =
                        parent
                            .task_metadata
                            .clone()
                            .unwrap_or(memorose_common::TaskMetadata {
                                status: memorose_common::TaskStatus::InProgress,
                                progress: 0.0,
                            });

                    if (meta.progress - progress).abs() > 0.001 {
                        meta.progress = progress;
                        if progress >= 1.0 {
                            meta.status = memorose_common::TaskStatus::Completed;
                        }
                        parent.task_metadata = Some(meta);
                        self.engine.store_memory_unit(parent).await?;
                    }
                }
            }
            // _guard dropped here
        }

        // Remove the DashMap entry when no other tasks are contending for it,
        // preventing unbounded growth over long-running sessions.
        if Arc::strong_count(&lock) == 1 {
            self.engine.task_locks.remove(&parent_id);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::CompressionOutput;
    use async_trait::async_trait;
    use chrono::Utc;
    use memorose_common::{Event, EventContent, L3Task, MemoryType, TaskStatus};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;
    use uuid::Uuid;

    const TEST_USER: &str = "test_user";

    struct MockLLM {
        fail_compress: bool,
        generate_response: Option<String>,
    }

    struct ContextAwareCorrectionLLM;

    struct BatchFallbackLLM {
        single_embed_calls: AtomicUsize,
    }

    struct FactErrorLLM;

    struct AssetErrorLLM;

    struct ConfidenceCorrectionLLM {
        confidence: f32,
    }

    struct TopicLLM;
    struct OutOfOrderCompressionLLM;
    struct ConcurrentAssetLLM {
        active_describes: AtomicUsize,
        max_describes: AtomicUsize,
    }

    #[async_trait]
    impl crate::llm::LLMClient for MockLLM {
        async fn generate(&self, _prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: self.generate_response.clone().unwrap_or_default(),
                usage: Default::default(),
            })
        }
        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 384],
                usage: Default::default(),
            })
        }
        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<CompressionOutput>> {
            if self.fail_compress {
                return Err(anyhow::anyhow!("LLM Error"));
            }
            Ok(crate::llm::LLMResponse {
                data: CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: Default::default(),
            })
        }
        async fn summarize_group(
            &self,
            _texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "summary".into(),
                usage: Default::default(),
            })
        }
        async fn describe_image(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "image".into(),
                usage: Default::default(),
            })
        }
        async fn describe_video(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "video".into(),
                usage: Default::default(),
            })
        }
        async fn transcribe(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "audio".into(),
                usage: Default::default(),
            })
        }
    }

    #[async_trait]
    impl crate::llm::LLMClient for ContextAwareCorrectionLLM {
        async fn generate(&self, prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            let first_target_id = prompt
                .lines()
                .find_map(|line| {
                    line.strip_prefix("ID: ")
                        .and_then(|value| Uuid::parse_str(value.trim()).ok())
                })
                .map(|id| id.to_string());

            let data = if prompt.contains("Content: I now live in Beijing")
                || prompt.contains("Content: Maintenant j'habite à Lyon")
                || prompt.contains("Content: btw, I moved from Shanghai to Beijing lol")
            {
                first_target_id
                    .map(|target_id| {
                        format!(
                            r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Residence updated","confidence":0.97}}]"#,
                            target_id
                        )
                    })
                    .unwrap_or_else(|| "[]".into())
            } else {
                "[]".into()
            };

            Ok(crate::llm::LLMResponse {
                data,
                usage: Default::default(),
            })
        }

        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 384],
                usage: Default::default(),
            })
        }

        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: Default::default(),
            })
        }

        async fn summarize_group(
            &self,
            texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: texts.join("\n"),
                usage: Default::default(),
            })
        }

        async fn describe_image(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "image".into(),
                usage: Default::default(),
            })
        }

        async fn describe_video(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "video".into(),
                usage: Default::default(),
            })
        }

        async fn transcribe(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "audio".into(),
                usage: Default::default(),
            })
        }
    }

    #[async_trait]
    impl crate::llm::LLMClient for OutOfOrderCompressionLLM {
        async fn generate(&self, prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            let first_target_id = prompt
                .lines()
                .find_map(|line| {
                    line.strip_prefix("ID: ")
                        .and_then(|value| Uuid::parse_str(value.trim()).ok())
                })
                .map(|id| id.to_string());

            let data = if prompt.contains("Content: Message 1: I now live in Beijing")
                || prompt.contains("Content: I now live in Beijing")
            {
                first_target_id
                    .map(|target_id| {
                        format!(
                            r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Residence updated","confidence":0.97}}]"#,
                            target_id
                        )
                    })
                    .unwrap_or_else(|| "[]".into())
            } else {
                "[]".into()
            };

            Ok(crate::llm::LLMResponse {
                data,
                usage: Default::default(),
            })
        }

        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 384],
                usage: Default::default(),
            })
        }

        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<CompressionOutput>> {
            if text.contains("I live in Shanghai") {
                tokio::time::sleep(Duration::from_millis(80)).await;
            } else {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            Ok(crate::llm::LLMResponse {
                data: CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: Default::default(),
            })
        }

        async fn summarize_group(
            &self,
            texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: texts.join("\n"),
                usage: Default::default(),
            })
        }

        async fn describe_image(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "image".into(),
                usage: Default::default(),
            })
        }

        async fn describe_video(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "video".into(),
                usage: Default::default(),
            })
        }

        async fn transcribe(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "audio".into(),
                usage: Default::default(),
            })
        }
    }

    #[async_trait]
    impl crate::llm::LLMClient for ConcurrentAssetLLM {
        async fn generate(&self, _prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: String::new(),
                usage: Default::default(),
            })
        }

        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 384],
                usage: Default::default(),
            })
        }

        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: Default::default(),
            })
        }

        async fn summarize_group(
            &self,
            texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: texts.join("\n"),
                usage: Default::default(),
            })
        }

        async fn describe_image(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            let current = self.active_describes.fetch_add(1, Ordering::SeqCst) + 1;
            let mut observed = self.max_describes.load(Ordering::SeqCst);
            while current > observed {
                match self.max_describes.compare_exchange(
                    observed,
                    current,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(actual) => observed = actual,
                }
            }

            tokio::time::sleep(Duration::from_millis(40)).await;
            self.active_describes.fetch_sub(1, Ordering::SeqCst);

            Ok(crate::llm::LLMResponse {
                data: "image".into(),
                usage: Default::default(),
            })
        }

        async fn describe_video(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "video".into(),
                usage: Default::default(),
            })
        }

        async fn transcribe(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "audio".into(),
                usage: Default::default(),
            })
        }
    }

    #[async_trait]
    impl crate::llm::LLMClient for BatchFallbackLLM {
        async fn generate(&self, _prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: String::new(),
                usage: Default::default(),
            })
        }

        async fn embed(&self, text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            self.single_embed_calls.fetch_add(1, Ordering::SeqCst);
            Ok(crate::llm::LLMResponse {
                data: vec![text.len() as f32],
                usage: Default::default(),
            })
        }

        async fn embed_content_batch(
            &self,
            _inputs: Vec<EmbedInput>,
        ) -> Result<crate::llm::LLMResponse<Vec<Vec<f32>>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![vec![999.0]],
                usage: Default::default(),
            })
        }

        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: Default::default(),
            })
        }

        async fn summarize_group(
            &self,
            _texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "summary".into(),
                usage: Default::default(),
            })
        }

        async fn describe_image(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "image".into(),
                usage: Default::default(),
            })
        }

        async fn describe_video(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "video".into(),
                usage: Default::default(),
            })
        }

        async fn transcribe(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "audio".into(),
                usage: Default::default(),
            })
        }
    }

    #[async_trait]
    impl crate::llm::LLMClient for FactErrorLLM {
        async fn generate(&self, _prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            Err(anyhow::anyhow!("fact extraction failed"))
        }

        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 384],
                usage: Default::default(),
            })
        }

        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: Default::default(),
            })
        }

        async fn summarize_group(
            &self,
            _texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "summary".into(),
                usage: Default::default(),
            })
        }

        async fn describe_image(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "image".into(),
                usage: Default::default(),
            })
        }

        async fn describe_video(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "video".into(),
                usage: Default::default(),
            })
        }

        async fn transcribe(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "audio".into(),
                usage: Default::default(),
            })
        }
    }

    #[async_trait]
    impl crate::llm::LLMClient for AssetErrorLLM {
        async fn generate(&self, _prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: String::new(),
                usage: Default::default(),
            })
        }

        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 384],
                usage: Default::default(),
            })
        }

        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: Default::default(),
            })
        }

        async fn summarize_group(
            &self,
            _texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "summary".into(),
                usage: Default::default(),
            })
        }

        async fn describe_image(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Err(anyhow::anyhow!("image description failed"))
        }

        async fn describe_video(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Err(anyhow::anyhow!("video description failed"))
        }

        async fn transcribe(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Err(anyhow::anyhow!("audio transcription failed"))
        }
    }

    #[async_trait]
    impl crate::llm::LLMClient for ConfidenceCorrectionLLM {
        async fn generate(&self, prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            let first_target_id = prompt
                .lines()
                .find_map(|line| {
                    line.strip_prefix("ID: ")
                        .and_then(|value| Uuid::parse_str(value.trim()).ok())
                })
                .map(|id| id.to_string());

            let data = if prompt.contains("Content: I now live in Beijing") {
                first_target_id
                    .map(|target_id| {
                        format!(
                            r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Residence updated","confidence":{}}}]"#,
                            target_id, self.confidence
                        )
                    })
                    .unwrap_or_else(|| "[]".into())
            } else {
                "[]".into()
            };

            Ok(crate::llm::LLMResponse {
                data,
                usage: Default::default(),
            })
        }

        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 384],
                usage: Default::default(),
            })
        }

        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: Default::default(),
            })
        }

        async fn summarize_group(
            &self,
            texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: texts.join("\n"),
                usage: Default::default(),
            })
        }

        async fn describe_image(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "image".into(),
                usage: Default::default(),
            })
        }

        async fn describe_video(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "video".into(),
                usage: Default::default(),
            })
        }

        async fn transcribe(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "audio".into(),
                usage: Default::default(),
            })
        }
    }

    #[async_trait]
    impl crate::llm::LLMClient for TopicLLM {
        async fn generate(&self, prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            let ids = prompt
                .lines()
                .filter_map(|line| {
                    line.strip_prefix("ID: ")
                        .and_then(|value| Uuid::parse_str(value.trim()).ok())
                })
                .map(|id| format!(r#""{}""#, id))
                .collect::<Vec<_>>()
                .join(",");

            Ok(crate::llm::LLMResponse {
                data: format!(r#"[{{"summary":"Topic summary","source_ids":[{}]}}]"#, ids),
                usage: Default::default(),
            })
        }

        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 3],
                usage: Default::default(),
            })
        }

        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: Default::default(),
            })
        }

        async fn summarize_group(
            &self,
            texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: texts.join("\n"),
                usage: Default::default(),
            })
        }

        async fn describe_image(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "image".into(),
                usage: Default::default(),
            })
        }

        async fn describe_video(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "video".into(),
                usage: Default::default(),
            })
        }

        async fn transcribe(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "audio".into(),
                usage: Default::default(),
            })
        }
    }

    #[tokio::test]
    async fn test_consolidation_with_llm_failure() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: true,
            generate_response: None,
        }));
        *worker.last_consolidation.lock().await =
            std::time::Instant::now() - Duration::from_secs(1);

        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("Hello".into()),
        );
        engine.ingest_event_directly(event.clone()).await?;

        let processed = worker.run_consolidation_cycle().await?;
        assert!(processed);

        let pending = engine.fetch_pending_events().await?;
        assert!(
            pending.is_empty(),
            "Event should be marked processed even if LLM failed (fallback mode)"
        );

        Ok(())
    }

    #[test]
    fn test_normalize_asset_storage_key_and_build_asset() {
        let http = BackgroundWorker::normalize_asset_storage_key("image", "https://a/b.png");
        assert_eq!(http, "https://a/b.png");

        let generated = BackgroundWorker::normalize_asset_storage_key("image", " raw-inline ");
        assert!(generated.starts_with("inline://image/"));

        let asset = BackgroundWorker::build_asset(
            " raw-inline ".into(),
            "photo.png",
            "image",
            Some("preview".into()),
        );
        assert_eq!(asset.original_name, "photo.png");
        assert_eq!(asset.asset_type, "image");
        assert_eq!(asset.description.as_deref(), Some("preview"));
        assert!(asset.storage_key.starts_with("inline://image/"));
    }

    #[test]
    fn test_parse_metadata_embedding_and_semantic_fingerprint() {
        let metadata = serde_json::json!({"embedding":[1.0, 2.5, 3.25]});
        assert_eq!(
            BackgroundWorker::parse_metadata_embedding(&metadata),
            Some(Some(vec![1.0, 2.5, 3.25]))
        );

        let empty = serde_json::json!({"embedding":[]});
        assert_eq!(
            BackgroundWorker::parse_metadata_embedding(&empty),
            Some(None)
        );

        let invalid = serde_json::json!({"embedding":[1.0, "oops"]});
        assert_eq!(
            BackgroundWorker::parse_metadata_embedding(&invalid),
            Some(None)
        );

        let fingerprint_a =
            BackgroundWorker::generate_semantic_fingerprint("Tool failed at 12:01!!!");
        let fingerprint_b = BackgroundWorker::generate_semantic_fingerprint("tool failed at 12:02");
        assert_eq!(fingerprint_a, fingerprint_b);
    }

    #[tokio::test]
    async fn test_extract_text_and_embed_input_for_text_json_and_inline_media() -> Result<()> {
        let stream_id = Uuid::new_v4();
        let llm = MockLLM {
            fail_compress: false,
            generate_response: None,
        };

        let text_event = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Text("Hello".into()),
        );
        let (text, text_input, text_assets) =
            BackgroundWorker::extract_text_and_embed_input(&text_event, Some(&llm)).await;
        assert_eq!(text, "Hello");
        assert!(matches!(text_input, EmbedInput::Text(ref v) if v == "Hello"));
        assert!(text_assets.is_empty());

        let json_event = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Json(serde_json::json!({"kind":"demo"})),
        );
        let (json_text, json_input, json_assets) =
            BackgroundWorker::extract_text_and_embed_input(&json_event, Some(&llm)).await;
        assert!(json_text.contains("\"kind\":\"demo\""));
        assert!(matches!(json_input, EmbedInput::Text(_)));
        assert!(json_assets.is_empty());

        let image_event = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Image("YmFzZTY0aW1hZ2U=".into()),
        );
        let (image_text, image_input, image_assets) =
            BackgroundWorker::extract_text_and_embed_input(&image_event, Some(&llm)).await;
        assert_eq!(image_text, "image");
        assert!(matches!(image_input, EmbedInput::Multimodal { .. }));
        assert_eq!(image_assets.len(), 1);

        let audio_event = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Audio("YmFzZTY0YXVkaW8=".into()),
        );
        let (_, audio_input, audio_assets) =
            BackgroundWorker::extract_text_and_embed_input(&audio_event, Some(&llm)).await;
        assert!(matches!(audio_input, EmbedInput::Multimodal { .. }));
        assert_eq!(audio_assets.len(), 1);

        let video_event = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Video("YmFzZTY0dmlkZW8=".into()),
        );
        let (_, video_input, video_assets) =
            BackgroundWorker::extract_text_and_embed_input(&video_event, Some(&llm)).await;
        assert!(matches!(video_input, EmbedInput::Multimodal { .. }));
        assert_eq!(video_assets.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_extract_text_and_embed_input_without_llm_uses_plain_fallbacks() -> Result<()> {
        let stream_id = Uuid::new_v4();

        let image_event = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Image("ZmFrZS1pbWFnZQ==".into()),
        );
        let (image_text, _, _) =
            BackgroundWorker::extract_text_and_embed_input(&image_event, None).await;
        assert_eq!(image_text, "Image at ZmFrZS1pbWFnZQ==");

        let audio_event = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Audio("ZmFrZS1hdWRpbw==".into()),
        );
        let (audio_text, _, _) =
            BackgroundWorker::extract_text_and_embed_input(&audio_event, None).await;
        assert_eq!(audio_text, "Audio at ZmFrZS1hdWRpbw==");

        let video_event = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Video("ZmFrZS12aWRlbw==".into()),
        );
        let (video_text, _, _) =
            BackgroundWorker::extract_text_and_embed_input(&video_event, None).await;
        assert_eq!(video_text, "Video at ZmFrZS12aWRlbw==");

        Ok(())
    }

    #[tokio::test]
    async fn test_extract_text_and_embed_input_falls_back_when_media_llm_calls_fail() -> Result<()>
    {
        let stream_id = Uuid::new_v4();
        let llm = AssetErrorLLM;

        let image_event = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Image("aW1hZ2UtYnl0ZXM=".into()),
        );
        let (image_text, image_input, image_assets) =
            BackgroundWorker::extract_text_and_embed_input(&image_event, Some(&llm)).await;
        assert_eq!(image_text, "Image at aW1hZ2UtYnl0ZXM=");
        assert!(matches!(image_input, EmbedInput::Multimodal { .. }));
        assert!(image_assets[0].storage_key.starts_with("inline://image/"));

        let audio_event = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Audio("YXVkaW8tYnl0ZXM=".into()),
        );
        let (audio_text, audio_input, audio_assets) =
            BackgroundWorker::extract_text_and_embed_input(&audio_event, Some(&llm)).await;
        assert_eq!(audio_text, "Audio at YXVkaW8tYnl0ZXM=");
        assert!(matches!(audio_input, EmbedInput::Multimodal { .. }));
        assert!(audio_assets[0].storage_key.starts_with("inline://audio/"));

        let video_event = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Video("dmlkZW8tYnl0ZXM=".into()),
        );
        let (video_text, video_input, video_assets) =
            BackgroundWorker::extract_text_and_embed_input(&video_event, Some(&llm)).await;
        assert_eq!(video_text, "Video at dmlkZW8tYnl0ZXM=");
        assert!(matches!(video_input, EmbedInput::Multimodal { .. }));
        assert!(video_assets[0].storage_key.starts_with("inline://video/"));

        Ok(())
    }

    #[tokio::test]
    async fn test_hydrate_extracted_facts_skips_non_l1_and_falls_back_on_llm_error() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut worker = BackgroundWorker::new(engine);
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: None,
        }));

        let mut non_l1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            MemoryType::Factual,
            "I live in Beijing".into(),
            None,
        );
        non_l1.level = 2;
        worker.hydrate_extracted_facts(&mut non_l1).await;
        assert!(non_l1.extracted_facts.is_empty());

        let mut factual = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            MemoryType::Factual,
            "I now live in Beijing".into(),
            None,
        );
        worker.hydrate_extracted_facts(&mut factual).await;
        assert!(factual
            .extracted_facts
            .iter()
            .any(|fact| fact.attribute == "residence"
                && fact.canonical_value.as_deref() == Some("beijing")));

        Ok(())
    }

    #[tokio::test]
    async fn test_hydrate_extracted_facts_logs_llm_error_and_uses_rule_fallback() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut worker = BackgroundWorker::new(engine);
        worker.llm_client = Some(Arc::new(FactErrorLLM));

        let mut factual = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            MemoryType::Factual,
            "I now live in Beijing".into(),
            None,
        );
        worker.hydrate_extracted_facts(&mut factual).await;

        assert!(factual
            .extracted_facts
            .iter()
            .any(|fact| fact.attribute == "residence"
                && fact.canonical_value.as_deref() == Some("beijing")));

        Ok(())
    }

    #[tokio::test]
    async fn test_run_decay_cycle_decays_and_prunes_active_users() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            MemoryType::Factual,
            "Low importance memory".into(),
            None,
        );
        unit.importance = 0.15;
        let unit_id = unit.id;
        engine.store_memory_unit(unit).await?;
        engine
            .system_kv()
            .put(format!("active_user:{TEST_USER}").as_bytes(), b"1")?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.config.forgetting_enabled = true;
        worker.config.decay_interval_secs = 1;
        worker.config.decay_factor = 0.5;
        worker.config.prune_threshold = 0.1;
        *worker.last_decay.lock().await = std::time::Instant::now() - Duration::from_secs(2);

        worker.run_decay_cycle().await?;

        assert!(engine.get_memory_unit(TEST_USER, unit_id).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_run_decay_cycle_skips_when_forgetting_disabled() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            MemoryType::Factual,
            "Low importance memory retained by default".into(),
            None,
        );
        unit.importance = 0.15;
        let unit_id = unit.id;
        engine.store_memory_unit(unit).await?;
        engine
            .system_kv()
            .put(format!("active_user:{TEST_USER}").as_bytes(), b"1")?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.config.forgetting_enabled = false;
        worker.config.decay_interval_secs = 1;
        worker.config.decay_factor = 0.5;
        worker.config.prune_threshold = 0.1;
        *worker.last_decay.lock().await = std::time::Instant::now() - Duration::from_secs(2);

        worker.run_decay_cycle().await?;

        let retained = engine
            .get_memory_unit(TEST_USER, unit_id)
            .await?
            .expect("memory should be retained when forgetting is disabled");
        assert_eq!(retained.importance, 0.15);
        Ok(())
    }

    #[tokio::test]
    async fn test_run_l3_task_cycle_completes_ready_tasks_and_emits_event() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine
            .store_memory_unit(MemoryUnit::new(
                None,
                TEST_USER.into(),
                None,
                stream_id,
                MemoryType::Factual,
                "seed user entry".into(),
                None,
            ))
            .await?;

        let mut completed_dep = L3Task::new(
            None,
            TEST_USER.into(),
            None,
            "Dependency".into(),
            "already finished".into(),
        );
        completed_dep.status = TaskStatus::Completed;
        completed_dep.progress = 1.0;
        engine.store_l3_task(&completed_dep).await?;

        let mut ready = L3Task::new(
            None,
            TEST_USER.into(),
            None,
            "Ready task".into(),
            "should auto complete".into(),
        );
        ready.dependencies = vec![completed_dep.task_id];
        engine.store_l3_task(&ready).await?;

        let mut blocked = L3Task::new(
            None,
            TEST_USER.into(),
            None,
            "Blocked task".into(),
            "missing dep".into(),
        );
        blocked.dependencies = vec![Uuid::new_v4()];
        engine.store_l3_task(&blocked).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: Some("LLM summary".into()),
        }));

        worker.run_l3_task_cycle().await?;

        let ready_after = engine
            .get_l3_task(TEST_USER, ready.task_id)
            .await?
            .expect("ready task should exist");
        assert_eq!(ready_after.status, TaskStatus::Completed);
        assert_eq!(ready_after.progress, 1.0);
        assert_eq!(ready_after.result_summary.as_deref(), Some("LLM summary"));

        let blocked_after = engine
            .get_l3_task(TEST_USER, blocked.task_id)
            .await?
            .expect("blocked task should exist");
        assert_eq!(blocked_after.status, TaskStatus::Pending);

        let pending_events = engine.fetch_pending_events().await?;
        assert_eq!(pending_events.len(), 1);
        assert_eq!(pending_events[0].agent_id.as_deref(), Some("system_worker"));
        assert!(matches!(
            &pending_events[0].content,
            EventContent::Text(content)
                if content.contains("Completed Milestone 'Ready task': LLM summary")
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_consolidation_cycle_returns_false_when_no_pending_events() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine);
        worker.config.consolidation_interval_ms = 1; worker.config.tick_interval_ms = 1;
        *worker.last_consolidation.lock().await =
            std::time::Instant::now() - Duration::from_secs(1);

        assert!(!worker.run_consolidation_cycle().await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_consolidation_cycle_skips_when_interval_not_elapsed() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine);
        let processed = worker.run_consolidation_cycle().await?;

        assert!(!processed);
        Ok(())
    }

    #[tokio::test]
    async fn test_consolidation_cycle_marks_exhausted_retries_failed() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let worker = BackgroundWorker::new(engine.clone());
        *worker.last_consolidation.lock().await =
            std::time::Instant::now() - Duration::from_secs(1);

        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("Retry me".into()),
        );
        let event_id = event.id.to_string();
        engine.ingest_event_directly(event).await?;

        for _ in 0..worker.config.consolidation_max_retries {
            engine.increment_retry_count_if_pending(&event_id).await?;
        }

        let processed = worker.run_consolidation_cycle().await?;
        assert!(!processed);
        assert_eq!(engine.fetch_pending_events().await?.len(), 0);
        assert_eq!(engine.get_retry_count(&event_id).await?, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_consolidation_cycle_success() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: None,
        }));
        *worker.last_consolidation.lock().await =
            std::time::Instant::now() - Duration::from_secs(1);

        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("Success".into()),
        );
        engine.ingest_event_directly(event.clone()).await?;

        worker.run_consolidation_cycle().await?;

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 1);
        assert_eq!(l1s[0].content, "Message 1: Success");

        Ok(())
    }

    #[tokio::test]
    async fn test_consolidation_respects_stream_boundaries() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: None,
        }));
        *worker.last_consolidation.lock().await =
            std::time::Instant::now() - Duration::from_secs(1);

        let stream_a = Uuid::new_v4();
        let stream_b = Uuid::new_v4();

        let event_a = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_a,
            EventContent::Text("First stream event".into()),
        );
        let event_b = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_b,
            EventContent::Text("Second stream event".into()),
        );

        engine.ingest_event_directly(event_a).await?;
        engine.ingest_event_directly(event_b).await?;

        worker.run_consolidation_cycle().await?;

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 2);
        assert!(l1s.iter().any(|u| u.stream_id == stream_a));
        assert!(l1s.iter().any(|u| u.stream_id == stream_b));

        Ok(())
    }

    #[tokio::test]
    async fn test_consolidation_token_aware_packing_can_merge_more_than_ten_events() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: None,
        }));
        worker.config.consolidation_interval_ms = 1; worker.config.tick_interval_ms = 1;
        worker.config.consolidation_batch_size = 32;
        worker.config.consolidation_target_tokens = 10_000;
        worker.config.consolidation_max_events_per_pack = 32;
        *worker.last_consolidation.lock().await =
            std::time::Instant::now() - Duration::from_secs(1);

        let stream_id = Uuid::new_v4();
        for index in 0..12 {
            let event = Event::new(
                None,
                TEST_USER.into(),
                None,
                stream_id,
                EventContent::Text(format!("Short event {index}")),
            );
            engine.ingest_event_directly(event).await?;
        }

        worker.run_consolidation_cycle().await?;

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 1);
        assert_eq!(l1s[0].stream_id, stream_id);
        assert!(l1s[0].content.contains("Message 12: Short event 11"));

        Ok(())
    }

    #[tokio::test]
    async fn test_consolidation_cycle_preserves_same_stream_commit_order_when_compression_finishes_out_of_order(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let llm = Arc::new(OutOfOrderCompressionLLM);
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(llm.clone()));

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(llm);
        worker.config.consolidation_interval_ms = 1; worker.config.tick_interval_ms = 1;
        worker.config.llm_concurrency = 2;
        worker.config.consolidation_batch_size = 8;
        worker.config.consolidation_target_tokens = 4;
        worker.config.consolidation_max_events_per_pack = 1;
        *worker.last_consolidation.lock().await =
            std::time::Instant::now() - Duration::from_secs(1);

        let stream_id = Uuid::new_v4();
        engine
            .ingest_event_directly(Event::new(
                None,
                TEST_USER.into(),
                None,
                stream_id,
                EventContent::Text("I live in Shanghai".into()),
            ))
            .await?;
        engine
            .ingest_event_directly(Event::new(
                None,
                TEST_USER.into(),
                None,
                stream_id,
                EventContent::Text("I now live in Beijing".into()),
            ))
            .await?;

        assert!(worker.run_consolidation_cycle().await?);

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 1);
        assert_eq!(l1s[0].content, "Message 1: I now live in Beijing");

        let recent = engine.list_recent_rac_decisions(8)?;
        assert!(recent.iter().any(|record| {
            record.stage == "staged_pre_store"
                && matches!(
                    record.effect,
                    crate::engine::RacDecisionEffect::Tombstone
                        | crate::engine::RacDecisionEffect::RelationOnly
                )
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_consolidation_cycle_overfetches_and_avoids_hot_stream_monopoly() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: None,
        }));
        worker.config.consolidation_interval_ms = 1; worker.config.tick_interval_ms = 1;
        worker.config.consolidation_batch_size = 2;
        worker.config.consolidation_fetch_multiplier = 3;
        worker.config.consolidation_target_tokens = 4;
        worker.config.consolidation_max_events_per_pack = 1;
        *worker.last_consolidation.lock().await =
            std::time::Instant::now() - Duration::from_secs(1);

        let now = chrono::Utc::now();
        let stream_a = Uuid::new_v4();
        let stream_b = Uuid::new_v4();

        let mut a1 = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_a,
            EventContent::Text("A1".into()),
        );
        a1.transaction_time = now;

        let mut a2 = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_a,
            EventContent::Text("A2".into()),
        );
        a2.transaction_time = now + chrono::Duration::milliseconds(1);

        let mut b1 = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_b,
            EventContent::Text("B1".into()),
        );
        b1.transaction_time = now + chrono::Duration::milliseconds(2);

        engine.ingest_event_directly(a1).await?;
        engine.ingest_event_directly(a2).await?;
        engine.ingest_event_directly(b1).await?;

        assert!(worker.run_consolidation_cycle().await?);

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 2);
        assert!(l1s.iter().any(|unit| unit.stream_id == stream_a));
        assert!(l1s.iter().any(|unit| unit.stream_id == stream_b));

        let pending = engine.fetch_pending_events().await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].stream_id, stream_a);

        Ok(())
    }

    #[tokio::test]
    async fn test_consolidation_cycle_parallelizes_multimodal_preprocessing() -> Result<()> {
        let temp_dir = tempdir()?;
        let llm = Arc::new(ConcurrentAssetLLM {
            active_describes: AtomicUsize::new(0),
            max_describes: AtomicUsize::new(0),
        });
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(llm.clone());
        worker.config.consolidation_interval_ms = 1; worker.config.tick_interval_ms = 1;
        worker.config.llm_concurrency = 2;
        worker.config.consolidation_batch_size = 2;
        worker.config.consolidation_fetch_multiplier = 1;
        worker.config.consolidation_target_tokens = 4;
        worker.config.consolidation_max_events_per_pack = 1;
        *worker.last_consolidation.lock().await =
            std::time::Instant::now() - Duration::from_secs(1);

        engine
            .ingest_event_directly(Event::new(
                None,
                TEST_USER.into(),
                None,
                Uuid::new_v4(),
                EventContent::Image("inline://image/a".into()),
            ))
            .await?;
        engine
            .ingest_event_directly(Event::new(
                None,
                TEST_USER.into(),
                None,
                Uuid::new_v4(),
                EventContent::Image("inline://image/b".into()),
            ))
            .await?;

        assert!(worker.run_consolidation_cycle().await?);
        assert!(llm.max_describes.load(Ordering::SeqCst) >= 2);

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 2);

        Ok(())
    }

    #[test]
    fn test_pack_events_for_consolidation_respects_token_budget() {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let temp_dir = tempdir().expect("tempdir");
        let engine = rt
            .block_on(MemoroseEngine::new_with_default_threshold(
                temp_dir.path(),
                1000,
                true,
                true,
            ))
            .expect("engine");

        let mut worker = BackgroundWorker::new(engine);
        worker.config.consolidation_target_tokens = 6;
        worker.config.consolidation_max_events_per_pack = 32;

        let stream_id = Uuid::new_v4();
        let events = vec![
            Event::new(
                None,
                TEST_USER.into(),
                None,
                stream_id,
                EventContent::Text("alpha beta gamma delta".into()),
            ),
            Event::new(
                None,
                TEST_USER.into(),
                None,
                stream_id,
                EventContent::Text("epsilon zeta eta theta".into()),
            ),
        ];

        let packed = worker.pack_events_for_consolidation(events);
        assert_eq!(packed.len(), 2);
        assert_eq!(packed[0].events.len(), 1);
        assert_eq!(packed[1].events.len(), 1);
    }

    #[test]
    fn test_schedule_packed_groups_fairly_round_robins_keys_while_preserving_per_key_order() {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let temp_dir = tempdir().expect("tempdir");
        let engine = rt
            .block_on(MemoroseEngine::new_with_default_threshold(
                temp_dir.path(),
                1000,
                true,
                true,
            ))
            .expect("engine");
        let mut worker = BackgroundWorker::new(engine);

        let stream_a = Uuid::new_v4();
        let stream_b = Uuid::new_v4();
        let stream_c = Uuid::new_v4();

        let scheduled = worker.schedule_packed_groups_fairly(vec![
            PackedEventGroup {
                key: (TEST_USER.into(), stream_a, None),
                seq_no: 0,
                events: Vec::new(),
            },
            PackedEventGroup {
                key: (TEST_USER.into(), stream_a, None),
                seq_no: 1,
                events: Vec::new(),
            },
            PackedEventGroup {
                key: (TEST_USER.into(), stream_a, None),
                seq_no: 2,
                events: Vec::new(),
            },
            PackedEventGroup {
                key: (TEST_USER.into(), stream_b, None),
                seq_no: 0,
                events: Vec::new(),
            },
            PackedEventGroup {
                key: (TEST_USER.into(), stream_b, None),
                seq_no: 1,
                events: Vec::new(),
            },
            PackedEventGroup {
                key: (TEST_USER.into(), stream_c, None),
                seq_no: 0,
                events: Vec::new(),
            },
        ]);

        let order: Vec<(Uuid, u64)> = scheduled
            .into_iter()
            .map(|group| (group.key.1, group.seq_no))
            .collect();

        assert_eq!(
            order,
            vec![
                (stream_a, 0),
                (stream_b, 0),
                (stream_c, 0),
                (stream_a, 1),
                (stream_b, 1),
                (stream_a, 2),
            ]
        );
    }

    #[test]
    fn test_limit_scheduled_groups_by_event_budget_keeps_prefix_without_splitting_groups() {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let temp_dir = tempdir().expect("tempdir");
        let engine = rt
            .block_on(MemoroseEngine::new_with_default_threshold(
                temp_dir.path(),
                1000,
                true,
                true,
            ))
            .expect("engine");
        let mut worker = BackgroundWorker::new(engine);

        let stream_a = Uuid::new_v4();
        let stream_b = Uuid::new_v4();
        let stream_c = Uuid::new_v4();
        let mk_group = |stream_id: Uuid, seq_no: u64, count: usize| PackedEventGroup {
            key: (TEST_USER.into(), stream_id, None),
            seq_no,
            events: (0..count)
                .map(|_| {
                    Event::new(
                        None,
                        TEST_USER.into(),
                        None,
                        stream_id,
                        EventContent::Text("x".into()),
                    )
                })
                .collect(),
        };

        let limited = worker.limit_scheduled_groups_by_event_budget(
            vec![
                mk_group(stream_a, 0, 1),
                mk_group(stream_b, 0, 2),
                mk_group(stream_c, 0, 1),
            ],
            3,
        );

        let summary: Vec<(Uuid, usize)> = limited
            .into_iter()
            .map(|group| (group.key.1, group.events.len()))
            .collect();
        assert_eq!(summary, vec![(stream_a, 1), (stream_b, 2)]);
    }

    #[tokio::test]
    async fn test_process_pipeline_batch_reconciles_before_store() -> Result<()> {
        let temp_dir = tempdir()?;
        let stream_id = Uuid::new_v4();

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Home base: Shanghai".into(),
            None,
        );
        let old_id = old_unit.id;

        let correction_llm = Arc::new(MockLLM {
            fail_compress: false,
            generate_response: Some(format!(
                r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Residence updated","confidence":0.96}}]"#,
                old_id
            )),
        });

        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(
                correction_llm.clone(),
            ));
        engine.store_memory_unit(old_unit).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(correction_llm);

        let processed = worker
            .process_pipeline_batch(vec![(
                vec![Uuid::new_v4()],
                TEST_USER.into(),
                stream_id,
                "I now live in Beijing".into(),
                None,
                Vec::new(),
                serde_json::json!({}),
                None,
            )])
            .await?;

        assert_eq!(processed.len(), 1);
        assert!(engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine.get_memory_unit(TEST_USER, old_id).await?.is_none());

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert!(l1s
            .iter()
            .any(|unit| unit.content == "I now live in Beijing"));

        Ok(())
    }

    #[tokio::test]
    async fn test_process_pipeline_batch_reconciles_staged_units_before_store() -> Result<()> {
        let temp_dir = tempdir()?;
        let stream_id = Uuid::new_v4();

        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(Arc::new(
                ContextAwareCorrectionLLM,
            )));

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: None,
        }));

        let processed = worker
            .process_pipeline_batch(vec![
                (
                    vec![Uuid::new_v4()],
                    TEST_USER.into(),
                    stream_id,
                    "I live in Shanghai".into(),
                    None,
                    Vec::new(),
                    serde_json::json!({}),
                    None,
                ),
                (
                    vec![Uuid::new_v4()],
                    TEST_USER.into(),
                    stream_id,
                    "I now live in Beijing".into(),
                    None,
                    Vec::new(),
                    serde_json::json!({}),
                    None,
                ),
            ])
            .await?;

        assert_eq!(processed.len(), 2);

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 1);
        assert_eq!(l1s[0].content, "I now live in Beijing");

        Ok(())
    }

    #[tokio::test]
    async fn test_process_pipeline_batch_reconciles_french_staged_units_before_store() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let stream_id = Uuid::new_v4();

        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(Arc::new(
                ContextAwareCorrectionLLM,
            )));

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: None,
        }));

        let processed = worker
            .process_pipeline_batch(vec![
                (
                    vec![Uuid::new_v4()],
                    TEST_USER.into(),
                    stream_id,
                    "J'habite à Paris".into(),
                    None,
                    Vec::new(),
                    serde_json::json!({}),
                    None,
                ),
                (
                    vec![Uuid::new_v4()],
                    TEST_USER.into(),
                    stream_id,
                    "Maintenant j'habite à Lyon".into(),
                    None,
                    Vec::new(),
                    serde_json::json!({}),
                    None,
                ),
            ])
            .await?;

        assert_eq!(processed.len(), 2);

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 1);
        assert_eq!(l1s[0].content, "Maintenant j'habite à Lyon");

        Ok(())
    }

    #[tokio::test]
    async fn test_process_pipeline_batch_reconciles_noisy_staged_units_before_store() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let stream_id = Uuid::new_v4();

        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(Arc::new(
                ContextAwareCorrectionLLM,
            )));

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: None,
        }));

        let processed = worker
            .process_pipeline_batch(vec![
                (
                    vec![Uuid::new_v4()],
                    TEST_USER.into(),
                    stream_id,
                    "I live in Shanghai".into(),
                    None,
                    Vec::new(),
                    serde_json::json!({}),
                    None,
                ),
                (
                    vec![Uuid::new_v4()],
                    TEST_USER.into(),
                    stream_id,
                    "btw, I moved from Shanghai to Beijing lol".into(),
                    None,
                    Vec::new(),
                    serde_json::json!({}),
                    None,
                ),
            ])
            .await?;

        assert_eq!(processed.len(), 2);

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 1);
        assert_eq!(l1s[0].content, "btw, I moved from Shanghai to Beijing lol");

        Ok(())
    }

    #[tokio::test]
    async fn test_process_pipeline_batch_persists_extracted_facts() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: Some(
                r#"{"facts":[{"subject":"user","subject_ref":"user:self","attribute":"residence","value":"Beijing","change_type":"update","temporal_status":"current","polarity":"positive","evidence_span":"I now live in Beijing","confidence":0.93}]}"#
                    .into(),
            ),
        }));

        let processed = worker
            .process_pipeline_batch(vec![(
                vec![Uuid::new_v4()],
                TEST_USER.into(),
                stream_id,
                "I now live in Beijing".into(),
                None,
                Vec::new(),
                serde_json::json!({}),
                None,
            )])
            .await?;

        assert_eq!(processed.len(), 1);

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 1);
        assert_eq!(l1s[0].extracted_facts.len(), 1);
        assert_eq!(
            l1s[0].extracted_facts[0].subject_ref.as_deref(),
            Some("user:self")
        );
        assert_eq!(
            l1s[0].extracted_facts[0].canonical_value.as_deref(),
            Some("beijing")
        );
        assert_eq!(
            l1s[0].extracted_facts[0].evidence_span.as_deref(),
            Some("I now live in Beijing")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_process_pipeline_batch_falls_back_to_multi_rule_fact_extraction() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: None,
        }));

        let processed = worker
            .process_pipeline_batch(vec![(
                vec![Uuid::new_v4()],
                TEST_USER.into(),
                stream_id,
                "I now live in Beijing and my email is dylan@example.com".into(),
                None,
                Vec::new(),
                serde_json::json!({}),
                None,
            )])
            .await?;

        assert_eq!(processed.len(), 1);

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 1);
        assert_eq!(l1s[0].extracted_facts.len(), 2);
        assert!(l1s[0]
            .extracted_facts
            .iter()
            .any(|fact| fact.attribute == "residence"
                && fact.canonical_value.as_deref() == Some("beijing")));
        assert!(l1s[0]
            .extracted_facts
            .iter()
            .any(|fact| fact.attribute == "contact"
                && fact.canonical_value.as_deref() == Some("dylan@example.com")));

        Ok(())
    }

    #[tokio::test]
    async fn test_process_pipeline_batch_uses_metadata_embedding_and_task_metadata() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let parent_id = Uuid::new_v4();
        let stream_id = Uuid::new_v4();

        let worker = BackgroundWorker::new(engine.clone());
        let processed = worker
            .process_pipeline_batch(vec![(
                vec![Uuid::new_v4()],
                TEST_USER.into(),
                stream_id,
                "Task progress update".into(),
                Some("2026-04-06T10:20:30Z".into()),
                Vec::new(),
                serde_json::json!({
                    "embedding": [0.25, 0.5, 0.75],
                    "target_level": 1,
                    "parent_id": parent_id.to_string(),
                    "task_status": "Completed",
                    "task_progress": 0.6
                }),
                None,
            )])
            .await?;

        assert_eq!(processed.len(), 1);
        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 1);
        assert_eq!(l1s[0].embedding.as_deref(), Some(&[0.25, 0.5, 0.75][..]));
        assert_eq!(
            l1s[0].valid_time.map(|dt| dt.to_rfc3339()),
            Some("2026-04-06T10:20:30+00:00".into())
        );
        assert!(l1s[0].references.contains(&parent_id));
        assert_eq!(
            l1s[0].task_metadata.as_ref().map(|meta| meta.progress),
            Some(0.6)
        );
        assert_eq!(
            l1s[0]
                .task_metadata
                .as_ref()
                .map(|meta| meta.status.clone()),
            Some(TaskStatus::Completed)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_process_pipeline_batch_falls_back_to_individual_embed_content_calls() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();
        let llm = Arc::new(BatchFallbackLLM {
            single_embed_calls: AtomicUsize::new(0),
        });

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(llm.clone());

        let processed = worker
            .process_pipeline_batch(vec![
                (
                    vec![Uuid::new_v4()],
                    TEST_USER.into(),
                    stream_id,
                    "alpha".into(),
                    None,
                    Vec::new(),
                    serde_json::json!({}),
                    None,
                ),
                (
                    vec![Uuid::new_v4()],
                    TEST_USER.into(),
                    stream_id,
                    "beta beta".into(),
                    None,
                    Vec::new(),
                    serde_json::json!({}),
                    None,
                ),
            ])
            .await?;

        assert_eq!(processed.len(), 2);
        assert_eq!(llm.single_embed_calls.load(Ordering::SeqCst), 2);
        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 2);
        assert!(l1s
            .iter()
            .any(|unit| unit.content == "alpha" && unit.embedding.as_deref() == Some(&[5.0][..])));
        assert!(l1s.iter().any(
            |unit| unit.content == "beta beta" && unit.embedding.as_deref() == Some(&[9.0][..])
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_process_pipeline_batch_sets_reflect_and_community_markers() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: None,
        }));
        worker.config.community_trigger_l1_step = 1;

        let processed = worker
            .process_pipeline_batch(vec![(
                vec![Uuid::new_v4()],
                TEST_USER.into(),
                stream_id,
                "hello marker".into(),
                None,
                Vec::new(),
                serde_json::json!({}),
                None,
            )])
            .await?;

        assert_eq!(processed.len(), 1);
        assert_eq!(
            engine.get_pending_reflections()?,
            vec![TEST_USER.to_string()]
        );
        assert_eq!(
            engine.get_pending_communities()?,
            vec![TEST_USER.to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_process_pipeline_batch_stages_relation_only_edge_for_medium_confidence_obsolete(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let stream_id = Uuid::new_v4();
        let correction_llm = Arc::new(ConfidenceCorrectionLLM { confidence: 0.78 });

        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(
                correction_llm.clone(),
            ));

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(correction_llm);

        let processed = worker
            .process_pipeline_batch(vec![
                (
                    vec![Uuid::new_v4()],
                    TEST_USER.into(),
                    stream_id,
                    "I live in Shanghai".into(),
                    None,
                    Vec::new(),
                    serde_json::json!({}),
                    None,
                ),
                (
                    vec![Uuid::new_v4()],
                    TEST_USER.into(),
                    stream_id,
                    "I now live in Beijing".into(),
                    None,
                    Vec::new(),
                    serde_json::json!({}),
                    None,
                ),
            ])
            .await?;

        assert_eq!(processed.len(), 2);

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 2);
        let shanghai = l1s
            .iter()
            .find(|unit| unit.content == "I live in Shanghai")
            .expect("old unit should exist");
        let beijing = l1s
            .iter()
            .find(|unit| unit.content == "I now live in Beijing")
            .expect("new unit should exist");

        let outgoing = engine
            .graph()
            .get_outgoing_edges(TEST_USER, beijing.id)
            .await?;
        assert!(outgoing.iter().any(|edge| edge.target_id == shanghai.id));
        let recent = engine
            .list_recent_rac_decisions(8)?
            .into_iter()
            .find(|record| record.stage == "staged_pre_store")
            .expect("expected staged relation-only decision");
        assert_eq!(
            recent.effect,
            crate::engine::RacDecisionEffect::RelationOnly
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_process_pipeline_batch_skips_low_confidence_obsolete_action() -> Result<()> {
        let temp_dir = tempdir()?;
        let stream_id = Uuid::new_v4();
        let correction_llm = Arc::new(ConfidenceCorrectionLLM { confidence: 0.4 });

        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(
                correction_llm.clone(),
            ));

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(correction_llm);

        worker
            .process_pipeline_batch(vec![
                (
                    vec![Uuid::new_v4()],
                    TEST_USER.into(),
                    stream_id,
                    "I live in Shanghai".into(),
                    None,
                    Vec::new(),
                    serde_json::json!({}),
                    None,
                ),
                (
                    vec![Uuid::new_v4()],
                    TEST_USER.into(),
                    stream_id,
                    "I now live in Beijing".into(),
                    None,
                    Vec::new(),
                    serde_json::json!({}),
                    None,
                ),
            ])
            .await?;

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 2);
        let shanghai = l1s
            .iter()
            .find(|unit| unit.content == "I live in Shanghai")
            .expect("old unit should exist");
        let beijing = l1s
            .iter()
            .find(|unit| unit.content == "I now live in Beijing")
            .expect("new unit should exist");

        let outgoing = engine
            .graph()
            .get_outgoing_edges(TEST_USER, beijing.id)
            .await?;
        assert!(outgoing.iter().all(|edge| edge.target_id != shanghai.id
            || !matches!(edge.relation, memorose_common::RelationType::EvolvedTo)));
        let recent = engine
            .list_recent_rac_decisions(8)?
            .into_iter()
            .find(|record| record.stage == "staged_pre_store")
            .expect("expected staged skip decision");
        assert_ne!(recent.effect, crate::engine::RacDecisionEffect::Tombstone);
        assert_ne!(
            recent.effect,
            crate::engine::RacDecisionEffect::RelationOnly
        );
        assert_eq!(
            recent.guard_reason.as_deref(),
            Some("obsolete_low_confidence")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_consolidation_cycle_uses_semantic_dedup_for_multi_event_batch() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: None,
        }));
        worker.config.consolidation_interval_ms = 1; worker.config.tick_interval_ms = 1;
        *worker.last_consolidation.lock().await =
            std::time::Instant::now() - Duration::from_secs(1);

        let stream_id = Uuid::new_v4();
        let event_1 = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Text("First duplicate message".into()),
        );
        let event_2 = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Text("Second duplicate message".into()),
        );
        engine.ingest_event_directly(event_1).await?;
        engine.ingest_event_directly(event_2).await?;

        let combined = "Message 1: First duplicate message\nMessage 2: Second duplicate message";
        let fingerprint = BackgroundWorker::generate_semantic_fingerprint(combined);
        engine.system_kv().put(
            format!("dedup:{TEST_USER}:{fingerprint}").as_bytes(),
            chrono::Utc::now().timestamp().to_string().as_bytes(),
        )?;

        assert!(worker.run_consolidation_cycle().await?);

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 1);
        assert_eq!(l1s[0].content, combined);

        Ok(())
    }

    #[tokio::test]
    async fn test_consolidation_cycle_groups_assistant_events_as_procedural_memory() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: None,
        }));
        worker.config.consolidation_interval_ms = 1; worker.config.tick_interval_ms = 1;
        *worker.last_consolidation.lock().await =
            std::time::Instant::now() - Duration::from_secs(1);

        let mut event = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("Assistant reply".into()),
        );
        event.metadata = serde_json::json!({"role":"assistant"});
        engine.ingest_event_directly(event).await?;

        assert!(worker.run_consolidation_cycle().await?);

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 1);
        assert_eq!(l1s[0].memory_type, MemoryType::Procedural);
        assert_eq!(l1s[0].content, "Message 1: Assistant reply");

        Ok(())
    }

    #[tokio::test]
    async fn test_is_leader_without_raft_returns_true() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let worker = BackgroundWorker::with_config(engine, AppConfig::default());

        assert!(worker.is_leader().await);
        Ok(())
    }

    #[tokio::test]
    async fn test_run_compaction_cycle_skips_when_interval_not_elapsed() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let mut worker = BackgroundWorker::with_config(engine, AppConfig::default());
        worker.config.compaction_interval_secs = 60;

        let before = *worker.last_compaction.lock().await;
        worker.run_compaction_cycle().await?;
        let after = *worker.last_compaction.lock().await;
        assert_eq!(before, after);
        Ok(())
    }

    #[tokio::test]
    async fn test_run_compaction_cycle_updates_timestamp_when_elapsed() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let mut worker = BackgroundWorker::with_config(engine, AppConfig::default());
        worker.config.compaction_interval_secs = 1;

        *worker.last_compaction.lock().await = std::time::Instant::now() - Duration::from_secs(2);
        let before = *worker.last_compaction.lock().await;
        worker.run_compaction_cycle().await?;
        let after = *worker.last_compaction.lock().await;
        assert!(after > before);
        Ok(())
    }

    #[tokio::test]
    async fn test_run_l3_task_cycle_without_llm_uses_default_summary() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        engine
            .store_memory_unit(MemoryUnit::new(
                None,
                TEST_USER.into(),
                None,
                Uuid::new_v4(),
                MemoryType::Factual,
                "seed user entry".into(),
                None,
            ))
            .await?;

        let ready = L3Task::new(
            None,
            TEST_USER.into(),
            None,
            "Ready task".into(),
            "default summary".into(),
        );
        engine.store_l3_task(&ready).await?;

        let worker = BackgroundWorker::with_config(engine.clone(), AppConfig::default());
        worker.run_l3_task_cycle().await?;

        let ready_after = engine
            .get_l3_task(TEST_USER, ready.task_id)
            .await?
            .expect("ready task should exist");
        assert_eq!(ready_after.status, TaskStatus::Completed);
        assert_eq!(
            ready_after.result_summary.as_deref(),
            Some("Task automatically completed by backend worker without LLM interaction.")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_run_insight_cycle_without_llm_returns_early() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let worker = BackgroundWorker::with_config(engine.clone(), AppConfig::default());

        engine.set_needs_reflect(TEST_USER)?;
        worker.run_insight_cycle().await?;

        assert_eq!(
            engine.get_pending_reflections()?,
            vec![TEST_USER.to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_run_insight_cycle_clears_marker_when_no_recent_streams() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM {
            fail_compress: false,
            generate_response: None,
        }));
        worker.config.insight_interval_ms = 1;
        worker.config.insight_min_pending_l1 = 1;
        *worker.last_insight.lock().await = std::time::Instant::now() - Duration::from_secs(1);

        engine.set_needs_reflect(TEST_USER)?;
        worker.run_insight_cycle().await?;

        assert!(engine.get_pending_reflections()?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_run_insight_cycle_generates_topics_and_clears_marker() -> Result<()> {
        let temp_dir = tempdir()?;
        let llm = Arc::new(TopicLLM);
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(llm.clone()));

        let stream_id = Uuid::new_v4();
        engine
            .store_memory_unit(MemoryUnit::new(
                None,
                TEST_USER.into(),
                None,
                stream_id,
                MemoryType::Factual,
                "I live in Beijing".into(),
                None,
            ))
            .await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(llm);
        worker.config.insight_interval_ms = 1;
        worker.config.insight_min_pending_l1 = 1;
        *worker.last_insight.lock().await = std::time::Instant::now() - Duration::from_secs(1);

        engine.set_needs_reflect(TEST_USER)?;
        worker.run_insight_cycle().await?;

        assert!(engine.get_pending_reflections()?.is_empty());

        let prefix = format!("u:{}:unit:", TEST_USER);
        let kv = engine.kv();
        let all_units = kv.scan(prefix.as_bytes())?;
        let topics = all_units
            .into_iter()
            .filter_map(|(_, value)| serde_json::from_slice::<MemoryUnit>(&value).ok())
            .filter(|unit| unit.level == 2 && unit.content == "Topic summary")
            .collect::<Vec<_>>();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].embedding.as_deref(), Some(&[0.0, 0.0, 0.0][..]));

        Ok(())
    }

    #[tokio::test]
    async fn test_run_insight_cycle_only_reflects_incremental_l1_window() -> Result<()> {
        let temp_dir = tempdir()?;
        let llm = Arc::new(TopicLLM);
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(llm.clone()));

        let first_stream_id = Uuid::new_v4();
        let second_stream_id = Uuid::new_v4();

        let mut first = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            first_stream_id,
            MemoryType::Factual,
            "first delta".into(),
            None,
        );
        first.transaction_time = Utc::now() - chrono::Duration::minutes(5);
        let first_id = first.id;
        engine.store_memory_units(vec![first]).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(llm.clone());
        worker.config.insight_interval_ms = 1;
        worker.config.insight_min_pending_l1 = 1;
        *worker.last_insight.lock().await = std::time::Instant::now() - Duration::from_secs(1);

        worker.run_insight_cycle().await?;

        let mut second = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            second_stream_id,
            MemoryType::Factual,
            "second delta".into(),
            None,
        );
        second.transaction_time = Utc::now();
        let second_id = second.id;
        engine.store_memory_units(vec![second]).await?;
        *worker.last_insight.lock().await = std::time::Instant::now() - Duration::from_secs(1);

        worker.run_insight_cycle().await?;

        let prefix = format!("u:{}:unit:", TEST_USER);
        let kv = engine.kv();
        let all_units = kv.scan(prefix.as_bytes())?;
        let topics = all_units
            .into_iter()
            .filter_map(|(_, value)| serde_json::from_slice::<MemoryUnit>(&value).ok())
            .filter(|unit| unit.level == 2 && unit.content == "Topic summary")
            .collect::<Vec<_>>();
        assert_eq!(topics.len(), 2);

        let topic_refs = topics
            .iter()
            .map(|unit| {
                let mut refs = unit.references.clone();
                refs.sort();
                refs
            })
            .collect::<Vec<_>>();

        assert!(topic_refs.iter().any(|refs| refs == &vec![first_id]));
        assert!(topic_refs.iter().any(|refs| refs == &vec![second_id]));

        Ok(())
    }

    #[tokio::test]
    async fn test_run_insight_cycle_drains_backlog_across_multiple_batches() -> Result<()> {
        let temp_dir = tempdir()?;
        let llm = Arc::new(TopicLLM);
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(llm.clone()));

        let stream_id = Uuid::new_v4();
        let now = Utc::now();
        let mut units = Vec::new();
        for offset in 0..3 {
            let mut unit = MemoryUnit::new(
                None,
                TEST_USER.into(),
                None,
                stream_id,
                MemoryType::Factual,
                format!("delta {}", offset),
                None,
            );
            unit.transaction_time = now + chrono::Duration::microseconds(offset as i64);
            units.push(unit);
        }
        engine.store_memory_units(units).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(llm);
        worker.config.insight_interval_ms = 1;
        worker.config.insight_min_pending_l1 = 1;
        worker.config.insight_max_l1_per_batch = 1;
        worker.config.insight_max_batches_per_cycle = 2;
        worker.config.insight_batch_target_tokens = usize::MAX / 4;
        *worker.last_insight.lock().await = std::time::Instant::now() - Duration::from_secs(1);

        worker.run_insight_cycle().await?;

        let markers = engine.get_pending_reflection_markers()?;
        let marker = markers
            .into_iter()
            .find(|(user_id, _)| user_id == TEST_USER)
            .map(|(_, marker)| marker)
            .expect("marker should remain for unfinished backlog");
        assert_eq!(marker.pending_units, 1);

        *worker.last_insight.lock().await = std::time::Instant::now() - Duration::from_secs(1);
        worker.run_insight_cycle().await?;
        assert!(engine.get_pending_reflections()?.is_empty());

        let prefix = format!("u:{}:unit:", TEST_USER);
        let kv = engine.kv();
        let all_units = kv.scan(prefix.as_bytes())?;
        let topics = all_units
            .into_iter()
            .filter_map(|(_, value)| serde_json::from_slice::<MemoryUnit>(&value).ok())
            .filter(|unit| unit.level == 2 && unit.content == "Topic summary")
            .collect::<Vec<_>>();
        assert_eq!(topics.len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_run_insight_cycle_defers_small_pending_reflection() -> Result<()> {
        let temp_dir = tempdir()?;
        let llm = Arc::new(TopicLLM);
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(llm.clone()));

        let stream_id = Uuid::new_v4();
        engine
            .store_memory_unit(MemoryUnit::new(
                None,
                TEST_USER.into(),
                None,
                stream_id,
                MemoryType::Factual,
                "small delta".into(),
                None,
            ))
            .await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(llm);
        worker.config.insight_interval_ms = 1;
        worker.config.insight_min_pending_l1 = 2;
        worker.config.insight_min_pending_tokens = usize::MAX / 2;
        worker.config.insight_max_delay_ms = u64::MAX / 2;
        *worker.last_insight.lock().await = std::time::Instant::now() - Duration::from_secs(1);

        worker.run_insight_cycle().await?;

        assert_eq!(
            engine.get_pending_reflections()?,
            vec![TEST_USER.to_string()]
        );
        let prefix = format!("u:{}:unit:", TEST_USER);
        let kv = engine.kv();
        let all_units = kv.scan(prefix.as_bytes())?;
        let topics = all_units
            .into_iter()
            .filter_map(|(_, value)| serde_json::from_slice::<MemoryUnit>(&value).ok())
            .filter(|unit| unit.level == 2 && unit.content == "Topic summary")
            .collect::<Vec<_>>();
        assert!(topics.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_run_community_cycle_clears_marker_when_no_edges_exist() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.config.community_interval_ms = 1;
        *worker.last_community.lock().await = std::time::Instant::now() - Duration::from_secs(1);

        engine.set_needs_community(TEST_USER)?;
        worker.run_community_cycle().await?;

        assert!(engine.get_pending_communities()?.is_empty());
        Ok(())
    }

    #[test]
    fn test_packed_event_key_and_estimate_tokens() {
        let mut event = Event::new(
            None,
            "user1".into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("Hello".into()),
        );
        event.stream_id = Uuid::new_v4();

        // Key logic
        let key = BackgroundWorker::packed_event_key(&event);
        assert_eq!(key.0, "user1");
        assert_eq!(key.2, None);

        event.metadata = serde_json::json!({"role": "assistant"});
        let key_agent = BackgroundWorker::packed_event_key(&event);
        assert_eq!(key_agent.2, Some("default_agent".to_string()));

        // Token logic
        let tokens_text = BackgroundWorker::estimate_event_pack_tokens(&event);
        assert!(tokens_text > 4);

        let event_json = Event::new(
            None,
            "user1".into(),
            None,
            Uuid::new_v4(),
            EventContent::Json(serde_json::json!({ "a": 1 })),
        );
        let tokens_json = BackgroundWorker::estimate_event_pack_tokens(&event_json);
        assert!(tokens_json > 4);

        let event_img = Event::new(
            None,
            "user1".into(),
            None,
            Uuid::new_v4(),
            EventContent::Image("http://test".into()),
        );
        let tokens_img = BackgroundWorker::estimate_event_pack_tokens(&event_img);
        assert!(tokens_img > 16);
    }

    #[tokio::test]
    async fn test_worker_lifecycle_methods() {
        let temp = tempfile::tempdir().unwrap();
        let engine = MemoroseEngine::new(temp.path(), 1000, false, false, 0.5, 128)
            .await
            .unwrap();
        let mut worker = BackgroundWorker::new(engine);

        // is_leader without raft
        assert!(worker.is_leader().await);

        // If we want to test set_raft we need a MemoroseRaft object, which might be hard to mock.
        // We'll skip set_raft for now.
    }
}
