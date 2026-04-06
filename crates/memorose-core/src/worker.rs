use crate::llm::{EmbedInput, EmbedPart, LLMClient};
use crate::MemoroseEngine;
use anyhow::Result;
use memorose_common::{config::AppConfig, Asset, GraphEdge, MemoryUnit};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;

pub struct BackgroundWorker {
    engine: MemoroseEngine,
    llm_client: Option<Arc<dyn LLMClient>>,
    config: memorose_common::config::WorkerConfig,
    last_decay: tokio::sync::Mutex<std::time::Instant>,
    last_compaction: tokio::sync::Mutex<std::time::Instant>,
    last_consolidation: tokio::sync::Mutex<std::time::Instant>,
    last_insight: tokio::sync::Mutex<std::time::Instant>,
    last_community: tokio::sync::Mutex<std::time::Instant>,
    raft: Option<crate::raft::MemoroseRaft>,
}

impl BackgroundWorker {
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
            last_decay: tokio::sync::Mutex::new(now),
            last_compaction: tokio::sync::Mutex::new(now),
            last_consolidation: tokio::sync::Mutex::new(now),
            last_insight: tokio::sync::Mutex::new(now),
            last_community: tokio::sync::Mutex::new(now),
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
        tracing::info!("Background Worker started (Loop interval: {}ms).", tick_ms);
        let mut interval = tokio::time::interval(Duration::from_millis(tick_ms));

        loop {
            interval.tick().await;

            if !self.is_leader().await {
                continue;
            }

            // Each cycle tracks its own last-run timestamp independently,
            // preventing the thundering-herd that occurred when a shared
            // tick counter was reset to zero.

            if let Err(e) = self.run_decay_cycle().await {
                tracing::error!("Decay cycle failed: {:?}", e);
            }

            if let Err(e) = self.run_l3_task_cycle().await {
                tracing::error!("L3 Task cycle failed: {:?}", e);
            }

            if let Err(e) = self.run_consolidation_cycle().await {
                tracing::error!("Consolidation cycle failed: {:?}", e);
            }

            if let Err(e) = self.run_compaction_cycle().await {
                tracing::error!("Compaction cycle failed: {:?}", e);
            }

            if self.llm_client.is_some() {
                if let Err(e) = self.run_insight_cycle().await {
                    tracing::error!("Insight cycle failed: {:?}", e);
                }

                if let Err(e) = self.run_community_cycle().await {
                    tracing::error!("Community cycle failed: {:?}", e);
                }
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
        let events = self.engine.fetch_pending_events_limited(batch_size).await?;
        if events.is_empty() {
            return Ok(false);
        }

        // Track all fetched IDs for fallback retry handling
        let all_fetched_ids: Vec<String> = events.iter().map(|e| e.id.to_string()).collect();

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

        valid_events.sort_by(|a, b| a.transaction_time.cmp(&b.transaction_time));

        // 1.5 Batching / Prompt Packing (Group contiguous events)
        let mut packed_batches: Vec<Vec<memorose_common::Event>> = Vec::new();
        let mut current_batch: Vec<memorose_common::Event> = Vec::new();
        let mut current_key: Option<(String, uuid::Uuid, Option<String>)> = None;

        for event in valid_events {
            let is_agent = event.metadata.get("role").and_then(|v| v.as_str()) == Some("assistant")
                || event.metadata.get("agent_id").is_some();
            let agent_id = if is_agent {
                event
                    .metadata
                    .get("agent_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or(Some("default_agent".to_string())) // Fallback to group all raw assistant messages together
            } else {
                None
            };

            let key = (event.user_id.clone(), event.stream_id, agent_id);

            if Some(&key) != current_key.as_ref() || current_batch.len() >= 10 {
                // Max 10 events per packed prompt
                if !current_batch.is_empty() {
                    packed_batches.push(std::mem::take(&mut current_batch));
                }
                current_key = Some(key);
            }
            current_batch.push(event);
        }
        if !current_batch.is_empty() {
            packed_batches.push(current_batch);
        }

        tracing::info!(
            "Consolidating {} packed event groups via pipeline (concurrency={})...",
            packed_batches.len(),
            self.config.llm_concurrency
        );

        // 2. Pipeline: Producer (Compress) -> Channel -> Consumer (Embed & Store)
        let (tx, mut rx) = mpsc::channel(self.config.llm_concurrency * 2);
        let llm_client_clone = self.llm_client.clone();
        let concurrency_limit = self.config.llm_concurrency;
        let engine_clone = self.engine.clone();

        // Spawn Producer — keep the handle so we can detect panics after the consumer drains.
        let producer_handle = tokio::spawn(async move {
            let mut join_set = tokio::task::JoinSet::new();

            for mut events in packed_batches {
                if events.is_empty() {
                    continue;
                }
                let llm = llm_client_clone.clone();
                let engine = engine_clone.clone();

                // For a packed batch, we will extract the common identifiers from the first event
                let first_event = events.remove(0);
                let (first_text, first_embed_input, mut assets) =
                    Self::extract_text_and_embed_input(&first_event, llm.as_deref()).await;
                let mut combined_text = format!("Message 1: {}", first_text);
                // Track the first multimodal embed input for the batch
                let embed_input = if first_embed_input.has_multimodal_parts() {
                    Some(first_embed_input)
                } else {
                    None
                };

                // Metadata logic (merge simple fields or keep first)
                let metadata = first_event.metadata.clone();
                let user_id = first_event.user_id.clone();
                let stream_id = first_event.stream_id;
                let is_agent = metadata.get("role").and_then(|v| v.as_str()) == Some("assistant")
                    || metadata.get("agent_id").is_some();

                // We keep all event IDs to mark them as processed later
                let mut event_ids = vec![first_event.id];

                // Append the rest
                for (i, evt) in events.into_iter().enumerate() {
                    let (evt_text, _evt_embed_input, evt_assets) =
                        Self::extract_text_and_embed_input(&evt, llm.as_deref()).await;
                    combined_text.push_str(&format!("\nMessage {}: {}", i + 2, evt_text));
                    event_ids.push(evt.id);
                    assets.extend(evt_assets);
                }

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

                    (event_ids, user_id, stream_id, summary, valid_at, assets, metadata, embed_input)
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
        let mini_batch_size = 20;
        let mut processed_ids = std::collections::HashSet::new();
        let mut any_processed = false;

        while let Some(item) = rx.recv().await {
            buffer.push(item);
            if buffer.len() >= mini_batch_size {
                let batch: Vec<_> = buffer.drain(..).collect();
                if let Ok(ids) = self.process_pipeline_batch(batch).await {
                    processed_ids.extend(ids);
                    any_processed = true;
                }
            }
        }

        // Process remaining
        if !buffer.is_empty() {
            if let Ok(ids) = self.process_pipeline_batch(buffer).await {
                processed_ids.extend(ids);
                any_processed = true;
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
                        let _ =
                            self.engine
                                .record_rac_decision_with_review(&crate::engine::RacDecisionRecord {
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
                                });
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
                        let _ =
                            self.engine
                                .record_rac_decision_with_review(&crate::engine::RacDecisionRecord {
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
                                });
                    }
                    crate::engine::ValidatedCorrectionDecision::Skip {
                        effect,
                        guard_reason,
                    } => {
                        let _ =
                            self.engine
                                .record_rac_decision_with_review(&crate::engine::RacDecisionRecord {
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
                                });
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

        // Phase 2: Batch Embed
        let mut inputs_to_embed = Vec::new();
        let mut needs_embedding = Vec::new();

        for (idx, (event_ids, _, _, summary, _, _, metadata, embed_input)) in
            batch.iter().enumerate()
        {
            match Self::parse_metadata_embedding(metadata) {
                Some(Some(_)) => continue,
                Some(None) | None => {}
            }
            if summary.trim().is_empty() {
                tracing::warn!("Skipping empty summary for packed events {:?}", event_ids);
                continue;
            }
            // Use multimodal embed input if available, otherwise fall back to text
            let input = embed_input
                .clone()
                .unwrap_or_else(|| EmbedInput::Text(summary.clone()));
            inputs_to_embed.push(input);
            needs_embedding.push(idx);
        }

        let embeddings = if !inputs_to_embed.is_empty() {
            if let Some(client) = self.llm_client.as_ref() {
                match client.embed_content_batch(inputs_to_embed.clone()).await {
                    Ok(embs) if embs.data.len() == needs_embedding.len() => embs.data,
                    _ => {
                        // Fallback to individual embed_content calls
                        let mut embs = Vec::with_capacity(inputs_to_embed.len());
                        for input in inputs_to_embed {
                            embs.push(
                                client
                                    .embed_content(input)
                                    .await
                                    .map(|r| r.data)
                                    .unwrap_or_default(),
                            );
                        }
                        embs
                    }
                }
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        let mut embeddings_by_idx = HashMap::new();
        for (i, emb) in needs_embedding.into_iter().zip(embeddings.into_iter()) {
            if !emb.is_empty() {
                embeddings_by_idx.insert(i, emb);
            }
        }

        // Phase 3: Store
        let mut units_to_store = Vec::new();
        let mut processed_ids = Vec::new();

        for (
            idx,
            (event_ids, user_id, stream_id, summary, valid_at, assets, metadata, _embed_input),
        ) in batch.into_iter().enumerate()
        {
            let embedding = match Self::parse_metadata_embedding(&metadata) {
                Some(Some(vec)) => Some(vec),
                Some(None) | None => embeddings_by_idx.remove(&idx),
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
            units_to_store.push(unit);
            for evt_id in event_ids {
                processed_ids.push(evt_id.to_string());
            }
        }

        if !units_to_store.is_empty() {
            let staged_edges = self
                .reconcile_staged_units_before_store(&mut units_to_store)
                .await?;

            for unit in &units_to_store {
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

            self.engine
                .store_memory_units_without_reconciliation(units_to_store.clone())
                .await?;

            for edge in staged_edges {
                self.engine.graph().add_edge(&edge).await?;
            }

            // Post-storage hooks (Reflection markers, etc.)
            let mut l1_increase_by_user: HashMap<String, usize> = HashMap::new();
            for unit in &units_to_store {
                let _ = self.engine.set_needs_reflect(&unit.user_id);
                if unit.level == 1 {
                    *l1_increase_by_user.entry(unit.user_id.clone()).or_insert(0) += 1;
                }
            }

            // Community Trigger
            let community_step = self.config.community_trigger_l1_step.max(1);
            for (user_id, delta) in l1_increase_by_user {
                if let Ok((before, after)) = self
                    .engine
                    .bump_l1_count_and_get_range(&user_id, delta)
                    .await
                {
                    if before / community_step < after / community_step && after >= community_step {
                        let _ = self.engine.set_needs_community(&user_id);
                    }
                }
            }

            // Task Reflection
            if self.engine.task_reflection {
                // ... simplified task reflection trigger ...
                for unit in &units_to_store {
                    if let Some(ref meta) = unit.task_metadata {
                        if meta.status == memorose_common::TaskStatus::Completed {
                            // This part is complex to clone inside loop, maybe skip for pipeline simplification or re-implement
                            // For now, let's keep it minimal or trigger async?
                            // Re-implementing simplified version:
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
        let user_ids = engine.get_pending_reflections()?;
        if user_ids.is_empty() {
            return Ok(());
        }

        for user_id in user_ids {
            // Fetch recent L1s for this user to find active streams
            let recent_l1s = match engine
                .fetch_recent_l1_units(&user_id, self.config.insight_recent_l1_limit.max(1))
                .await
            {
                Ok(l1s) => l1s,
                Err(e) => {
                    tracing::warn!(
                        "Failed to load recent L1 units for reflection (user {}): {:?}",
                        user_id,
                        e
                    );
                    continue;
                }
            };

            // Extract unique stream IDs
            let mut unique_streams = std::collections::HashSet::new();
            for unit in recent_l1s {
                unique_streams.insert(unit.stream_id);
            }

            if !unique_streams.is_empty() {
                tracing::info!(
                    "Found {} active streams for reflection (user {})",
                    unique_streams.len(),
                    user_id
                );
                let mut all_succeeded = true;
                for stream_id in unique_streams {
                    match engine.reflect_on_session(&user_id, stream_id).await {
                        Ok(_) => {
                            tracing::debug!(
                                "Reflection completed for stream {} (user {})",
                                stream_id,
                                user_id
                            );
                        }
                        Err(e) => {
                            all_succeeded = false;
                            tracing::warn!(
                                "Reflection failed for stream {} (user {}): {:?}",
                                stream_id,
                                user_id,
                                e
                            );
                        }
                    }
                }
                if all_succeeded {
                    engine.clear_reflection_marker(&user_id)?;
                }
            } else {
                engine.clear_reflection_marker(&user_id)?;
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

            let data = if prompt.contains("Content: I now live in Beijing") {
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
        assert_eq!(BackgroundWorker::parse_metadata_embedding(&empty), Some(None));

        let invalid = serde_json::json!({"embedding":[1.0, "oops"]});
        assert_eq!(BackgroundWorker::parse_metadata_embedding(&invalid), Some(None));

        let fingerprint_a =
            BackgroundWorker::generate_semantic_fingerprint("Tool failed at 12:01!!!");
        let fingerprint_b =
            BackgroundWorker::generate_semantic_fingerprint("tool failed at 12:02");
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
    async fn test_extract_text_and_embed_input_falls_back_when_media_llm_calls_fail() -> Result<()> {
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
        worker.config.decay_interval_secs = 1;
        worker.config.decay_factor = 0.5;
        worker.config.prune_threshold = 0.1;
        *worker.last_decay.lock().await = std::time::Instant::now() - Duration::from_secs(2);

        worker.run_decay_cycle().await?;

        assert!(engine.get_memory_unit(TEST_USER, unit_id).await?.is_none());
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
        worker.config.consolidation_interval_ms = 1;
        *worker.last_consolidation.lock().await = std::time::Instant::now() - Duration::from_secs(1);

        assert!(!worker.run_consolidation_cycle().await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_consolidation_cycle_skips_when_interval_not_elapsed() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let worker = BackgroundWorker::new(engine);
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
        assert_eq!(l1s[0].valid_time.map(|dt| dt.to_rfc3339()), Some("2026-04-06T10:20:30+00:00".into()));
        assert!(l1s[0].references.contains(&parent_id));
        assert_eq!(l1s[0].task_metadata.as_ref().map(|meta| meta.progress), Some(0.6));
        assert_eq!(
            l1s[0].task_metadata.as_ref().map(|meta| meta.status.clone()),
            Some(TaskStatus::Completed)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_process_pipeline_batch_falls_back_to_individual_embed_content_calls() -> Result<()> {
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
        assert!(l1s.iter().any(|unit| unit.content == "alpha" && unit.embedding.as_deref() == Some(&[5.0][..])));
        assert!(l1s.iter().any(|unit| unit.content == "beta beta" && unit.embedding.as_deref() == Some(&[9.0][..])));

        Ok(())
    }

    #[tokio::test]
    async fn test_process_pipeline_batch_sets_reflect_and_community_markers() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut worker = BackgroundWorker::new(engine.clone());
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
        assert_eq!(engine.get_pending_reflections()?, vec![TEST_USER.to_string()]);
        assert_eq!(engine.get_pending_communities()?, vec![TEST_USER.to_string()]);
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

        let outgoing = engine.graph().get_outgoing_edges(TEST_USER, beijing.id).await?;
        assert!(outgoing.iter().any(|edge| edge.target_id == shanghai.id));
        let recent = engine
            .list_recent_rac_decisions(8)?
            .into_iter()
            .find(|record| record.stage == "staged_pre_store")
            .expect("expected staged relation-only decision");
        assert_eq!(recent.effect, crate::engine::RacDecisionEffect::RelationOnly);

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

        let outgoing = engine.graph().get_outgoing_edges(TEST_USER, beijing.id).await?;
        assert!(outgoing
            .iter()
            .all(|edge| edge.target_id != shanghai.id
                || !matches!(edge.relation, memorose_common::RelationType::EvolvedTo)));
        let recent = engine
            .list_recent_rac_decisions(8)?
            .into_iter()
            .find(|record| record.stage == "staged_pre_store")
            .expect("expected staged skip decision");
        assert_ne!(recent.effect, crate::engine::RacDecisionEffect::Tombstone);
        assert_ne!(recent.effect, crate::engine::RacDecisionEffect::RelationOnly);
        assert_eq!(recent.guard_reason.as_deref(), Some("obsolete_low_confidence"));

        Ok(())
    }

    #[tokio::test]
    async fn test_consolidation_cycle_uses_semantic_dedup_for_multi_event_batch() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.config.consolidation_interval_ms = 1;
        *worker.last_consolidation.lock().await = std::time::Instant::now() - Duration::from_secs(1);

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
        worker.config.consolidation_interval_ms = 1;
        *worker.last_consolidation.lock().await = std::time::Instant::now() - Duration::from_secs(1);

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

        assert_eq!(engine.get_pending_reflections()?, vec![TEST_USER.to_string()]);
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
}
