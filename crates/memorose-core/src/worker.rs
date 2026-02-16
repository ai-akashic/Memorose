use crate::MemoroseEngine;
use crate::llm::LLMClient;
use crate::llm::gemini::GeminiClient;
use memorose_common::{MemoryUnit, config::AppConfig, Asset};
use tokio::time::Duration;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

pub struct BackgroundWorker {
    engine: MemoroseEngine,
    llm_client: Option<Arc<dyn LLMClient>>,
    config: memorose_common::config::WorkerConfig,
    last_decay: std::sync::Mutex<std::time::Instant>,
    last_compaction: std::sync::Mutex<std::time::Instant>,
    raft: Option<crate::raft::MemoroseRaft>,
}

impl BackgroundWorker {
    pub fn new(engine: MemoroseEngine) -> Self {
        let config = AppConfig::load().unwrap_or_else(|_| {
            tracing::warn!("Failed to load config, using defaults");
            AppConfig::load().unwrap() // Should not fail with defaults
        });
        Self::with_config(engine, config)
    }

    pub fn with_config(engine: MemoroseEngine, config: AppConfig) -> Self {
        let llm_client: Option<Arc<dyn LLMClient>> = if let Some(api_key) = config.get_active_key() {
            let masked_key = if api_key.len() > 5 { format!("{}***", &api_key[..5]) } else { "INVALID".to_string() };
            tracing::info!("Worker Initializing. Config Key: {}, Model: {}, Embed: {}",
                masked_key, config.get_model_name(), config.get_embedding_model_name());

            let client = GeminiClient::new(
                api_key.clone(),
                config.get_model_name(),
                config.get_embedding_model_name(),
            );
            Some(Arc::new(client))
        } else {
            tracing::warn!("BackgroundWorker starting without API Key. Summary and Insight features will be disabled/degraded.");
            None
        };

        let now = std::time::Instant::now();
        Self {
            engine,
            llm_client,
            config: config.worker,
            last_decay: std::sync::Mutex::new(now),
            last_compaction: std::sync::Mutex::new(now),
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
        let mut tick_count: u64 = 0;

        loop {
            interval.tick().await;

            if !self.is_leader().await {
                continue;
            }

            tick_count += tick_ms;

            // 1. Forgetting (Decay) - scan active_user markers
            let decay_interval_ms = self.config.decay_interval_secs.max(1) * 1000;
            if tick_count % decay_interval_ms == 0 {
                if let Err(e) = self.run_decay_cycle().await {
                    tracing::error!("Decay cycle failed: {:?}", e);
                }
            }

            // 2. L0 -> L1 (Consolidation)
            let consolidation_interval_ms = self.config.consolidation_interval_ms.max(tick_ms);
            if tick_count % consolidation_interval_ms == 0 {
                if let Err(e) = self.run_consolidation_cycle().await {
                    tracing::error!("Consolidation cycle failed: {:?}", e);
                }
            }

            // 3. Compaction
            let compaction_interval_ms = self.config.compaction_interval_secs.max(1) * 1000;
            if tick_count % compaction_interval_ms == 0 {
                if let Err(e) = self.run_compaction_cycle().await {
                    tracing::error!("Compaction cycle failed: {:?}", e);
                }
            }

            // 4. Cognitive Cycles (Requires LLM) - marker-driven
            if self.llm_client.is_some() {
                // Insight Cycle (Reflection) - driven by needs_reflect markers
                let insight_interval_ms = self.config.insight_interval_ms.max(tick_ms);
                if tick_count % insight_interval_ms == 0 {
                    if let Err(e) = self.run_insight_cycle().await {
                        tracing::error!("Insight cycle failed: {:?}", e);
                    }
                }

                // Community Cycle (L2) - driven by needs_community markers
                let community_interval_ms = self.config.community_interval_ms.max(tick_ms);
                if tick_count % community_interval_ms == 0 {
                    if let Err(e) = self.run_community_cycle().await {
                        tracing::error!("Community cycle failed: {:?}", e);
                    }
                }
            }

            // Reset tick_count periodically to avoid overflow
            if tick_count > 86400 * 1000 * 7 { // Reset every week
                tick_count = 0;
            }
        }
    }

    async fn run_compaction_cycle(&self) -> Result<()> {
        let compaction_interval = Duration::from_secs(self.config.compaction_interval_secs.max(1));
        let should_compact = {
            let last = self.last_compaction.lock().unwrap();
            last.elapsed() > compaction_interval
        };

        if should_compact {
            tracing::info!("Running LanceDB compaction...");
            self.engine.compact_vector_store().await?;
            let mut last = self.last_compaction.lock().unwrap();
            *last = std::time::Instant::now();
        }
        Ok(())
    }

    async fn run_decay_cycle(&self) -> Result<()> {
        let decay_interval = Duration::from_secs(self.config.decay_interval_secs.max(1));
        let should_decay = {
            let last = self.last_decay.lock().unwrap();
            last.elapsed() > decay_interval
        };

        if should_decay {
            tracing::info!("Running memory decay and pruning...");

            // Scan active_user markers to find users needing decay
            let skv = self.engine.system_kv();
            let active_pairs = tokio::task::spawn_blocking(move || {
                skv.scan(b"active_user:")
            }).await??;

            for (key, _) in active_pairs {
                let key_str = String::from_utf8(key)?;
                if let Some(user_id) = key_str.strip_prefix("active_user:") {
                    self.engine.decay_importance(user_id, self.config.decay_factor).await?;

                    let pruned = self.engine.prune_memories(user_id, self.config.prune_threshold).await?;
                    if pruned > 0 {
                        tracing::info!("Pruned {} low-importance memories for user {}", pruned, user_id);
                    }
                }
            }

            let mut last = self.last_decay.lock().unwrap();
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

    async fn run_consolidation_cycle(&self) -> Result<bool> {
        let batch_size = self.config.consolidation_batch_size.max(1);
        let events = self.engine.fetch_pending_events_limited(batch_size).await?;
        if events.is_empty() {
            return Ok(false);
        }

        // 记录正在处理的 event IDs
        let processing_event_ids: Vec<String> = events.iter()
            .map(|e| e.id.to_string())
            .collect();

        let result: Result<bool> = async {
            // 过滤掉超过最大重试次数的 events
            let max_retries = self.config.consolidation_max_retries;
            let mut valid_events = Vec::new();
            let mut failed_events = Vec::new();

            for event in events {
                let retry_count = self.engine.get_retry_count(&event.id.to_string()).await.unwrap_or(0);
                if retry_count >= max_retries {
                    tracing::warn!(
                        "Event {} exceeded max retries ({}/{}), moving to failed queue",
                        event.id, retry_count, max_retries
                    );
                    failed_events.push(event);
                } else {
                    valid_events.push(event);
                }
            }

            // 标记失败的 events
            for event in failed_events {
                if let Err(e) = self.engine.mark_event_failed(
                    &event.id.to_string(),
                    &format!("Exceeded max retries ({})", max_retries)
                ).await {
                    tracing::error!("Failed to mark event {} as failed: {:?}", event.id, e);
                }
            }

            if valid_events.is_empty() {
                return Ok(false);
            }

            tracing::info!(
                "Consolidating {} events with batch embedding (batch_size={})...",
                valid_events.len(),
                batch_size
            );

            // Phase 1: Process content and compress in parallel
            let mut join_set = tokio::task::JoinSet::new();
            let llm_client = self.llm_client.clone();
            let mut compressed_data = Vec::new();

            for event in valid_events {
                let llm_clone = llm_client.clone();
                let event_id = event.id;
                let user_id = event.user_id.clone();
                let app_id = event.app_id.clone();
                let stream_id = event.stream_id;
                let content = event.content.clone();
                let metadata = event.metadata.clone();

                // Limit concurrency
                if join_set.len() >= self.config.llm_concurrency {
                    if let Some(res) = join_set.join_next().await {
                        match res {
                            Ok(data) => compressed_data.push(data),
                            Err(e) => tracing::error!("Failed to process consolidation task: {:?}", e),
                        }
                    }
                }

                join_set.spawn(async move {
                    // 1. Process Content & Extract Assets
                    let (text_to_process, assets) = match content {
                        memorose_common::EventContent::Text(t) => (t, vec![]),
                        memorose_common::EventContent::Image(url) => {
                            let description = if let Some(client) = llm_clone.as_ref() {
                                client.describe_image(&url).await.unwrap_or_else(|e| {
                                    tracing::warn!("Vision processing failed for {}: {}", event_id, e);
                                    format!("Image asset at {}", url)
                                })
                            } else {
                                format!("Image asset at {}", url)
                            };

                            let asset = Asset {
                                storage_key: url.clone(),
                                original_name: "image".to_string(),
                                asset_type: "image".to_string(),
                                metadata: HashMap::new(),
                            };
                            (description, vec![asset])
                        },
                        memorose_common::EventContent::Audio(url) => {
                            let transcript = if let Some(client) = llm_clone.as_ref() {
                                client.transcribe(&url).await.unwrap_or_else(|e| {
                                    tracing::warn!("STT processing failed for {}: {}", event_id, e);
                                    format!("Audio asset at {}", url)
                                })
                            } else {
                                format!("Audio asset at {}", url)
                            };

                            let asset = Asset {
                                storage_key: url.clone(),
                                original_name: "audio".to_string(),
                                asset_type: "audio".to_string(),
                                metadata: HashMap::new(),
                            };
                            (transcript, vec![asset])
                        },
                        memorose_common::EventContent::Json(val) => (val.to_string(), vec![]),
                        memorose_common::EventContent::Video(url) => {
                            let description = if let Some(client) = llm_clone.as_ref() {
                                client.describe_video(&url).await.unwrap_or_else(|e| {
                                    tracing::warn!("Video processing failed for {}: {}", event_id, e);
                                    format!("Video asset at {}", url)
                                })
                            } else {
                                format!("Video asset at {}", url)
                            };

                            let asset = Asset {
                                storage_key: url.clone(),
                                original_name: "video".to_string(),
                                asset_type: "video".to_string(),
                                metadata: HashMap::new(),
                            };
                            (description, vec![asset])
                        }
                    };

                    // 2. Compression (Text summarization)
                    let (summary, valid_at) = match llm_clone.as_ref() {
                        Some(client) => match client.compress(&text_to_process).await {
                            Ok(out) => (out.content, out.valid_at),
                            Err(e) => {
                                tracing::warn!("Compression failed for {}: {:?}", event_id, e);
                                (text_to_process, None)
                            }
                        },
                        None => (text_to_process, None),
                    };

                    // Return data for batching
                    (event_id, user_id, app_id, stream_id, summary, valid_at, assets, metadata)
                });
            }

            // Collect all compressed results
            while let Some(res) = join_set.join_next().await {
                match res {
                    Ok(data) => compressed_data.push(data),
                    Err(e) => tracing::error!("Failed to process consolidation task: {:?}", e),
                }
            }

            if compressed_data.is_empty() {
                return Ok(false);
            }

            // Phase 2: Batch embed all summaries
            let mut texts_to_embed = Vec::new();
            let mut needs_embedding = Vec::new();

            for (idx, (event_id, _, _, _, summary, _, _, metadata)) in compressed_data.iter().enumerate() {
                // Use metadata embedding when valid; re-embed when invalid or missing.
                match Self::parse_metadata_embedding(metadata) {
                    Some(Some(_)) => continue,
                    Some(None) => {
                        tracing::warn!(
                            "Invalid embedding metadata for event {}, falling back to model embedding",
                            event_id
                        );
                    }
                    None => {}
                }

                // Skip empty summaries to avoid Gemini API "empty text content" errors
                if summary.trim().is_empty() {
                    tracing::warn!(
                        "Skipping empty summary for event {} (will store without embedding)",
                        event_id
                    );
                    continue;
                }

                texts_to_embed.push(summary.clone());
                needs_embedding.push(idx);
            }

            let embeddings = if !texts_to_embed.is_empty() {
                if let Some(client) = llm_client.as_ref() {
                    tracing::info!("Batch embedding {} texts...", texts_to_embed.len());
                    let mut fallback_to_single = false;

                    let batch_embeddings = match client.embed_batch(texts_to_embed.clone()).await {
                        Ok(embs) => {
                            if embs.len() == needs_embedding.len() {
                                Some(embs)
                            } else {
                                tracing::error!(
                                    "Batch embedding count mismatch: requested={}, received={}",
                                    needs_embedding.len(),
                                    embs.len()
                                );
                                fallback_to_single = true;
                                None
                            }
                        }
                        Err(e) => {
                            tracing::error!("Batch embedding failed: {:?}", e);
                            fallback_to_single = true;
                            None
                        }
                    };

                    if let Some(embs) = batch_embeddings {
                        embs
                    } else if fallback_to_single {
                        tracing::info!(
                            "Falling back to individual embedding for {} texts",
                            texts_to_embed.len()
                        );
                        let mut embs = Vec::with_capacity(texts_to_embed.len());
                        for text in texts_to_embed {
                            match client.embed(&text).await {
                                Ok(embedding) => embs.push(embedding),
                                Err(e) => {
                                    tracing::warn!("Fallback embed failed: {:?}", e);
                                    embs.push(vec![]);
                                }
                            }
                        }
                        embs
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            } else {
                vec![]
            };

            if embeddings.len() != needs_embedding.len() {
                tracing::warn!(
                    "Embedding result count mismatch after fallback: expected={}, got={}",
                    needs_embedding.len(),
                    embeddings.len()
                );
            }

            let mut embeddings_by_idx = HashMap::with_capacity(needs_embedding.len());
            for (event_idx, embedding) in needs_embedding.into_iter().zip(embeddings.into_iter()) {
                if !embedding.is_empty() {
                    embeddings_by_idx.insert(event_idx, embedding);
                }
            }

            // Phase 3: Create memory units with embeddings
            let mut units_to_store = Vec::new();
            let mut processed_event_ids = Vec::new();

            for (idx, (event_id, user_id, app_id, stream_id, summary, valid_at, assets, metadata)) in compressed_data.into_iter().enumerate() {
                // Get embedding (either from metadata or from batch result)
                let embedding = match Self::parse_metadata_embedding(&metadata) {
                    Some(Some(vec)) => Some(vec),
                    Some(None) | None => embeddings_by_idx.remove(&idx),
                };

                // Create Unit
                let mut unit = MemoryUnit::new(user_id, app_id, stream_id, summary, embedding);
                unit.valid_time = valid_at.and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok().map(|d| d.with_timezone(&chrono::Utc)));
                unit.assets = assets;

                // Task-specific Logic
                if let Some(level) = metadata.get("target_level").and_then(|v| v.as_u64()) {
                    unit.level = level as u8;

                    if let Some(parent_id_str) = metadata.get("parent_id").and_then(|v| v.as_str()) {
                        if let Ok(parent_id) = uuid::Uuid::parse_str(parent_id_str) {
                            unit.references.push(parent_id);
                        }
                    }

                    if level >= 1 {
                        let status = match metadata.get("task_status").and_then(|v| v.as_str()) {
                            Some("Completed") => memorose_common::TaskStatus::Completed,
                            Some("Active") => memorose_common::TaskStatus::Active,
                            Some("Failed") => memorose_common::TaskStatus::Failed,
                            Some("Blocked") => memorose_common::TaskStatus::Blocked,
                            _ => memorose_common::TaskStatus::Pending,
                        };

                        unit.task_metadata = Some(memorose_common::TaskMetadata {
                            status,
                            progress: metadata.get("task_progress").and_then(|v| v.as_f64()).unwrap_or(0.0) as f32,
                        });
                    }
                }

                units_to_store.push(unit);
                processed_event_ids.push(event_id.to_string());
            }

            // Store all units
            tracing::info!("Storing {} memory units...", units_to_store.len());

            if !units_to_store.is_empty() {
                self.engine.store_memory_units(units_to_store.clone()).await?;

                // Set reflection markers and optionally trigger community detection.
                let mut user_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
                let mut l1_increase_by_user: HashMap<String, usize> = HashMap::new();
                for unit in &units_to_store {
                    user_ids.insert(unit.user_id.clone());
                    if unit.level == 1 {
                        *l1_increase_by_user.entry(unit.user_id.clone()).or_insert(0) += 1;
                    }
                }
                for user_id in user_ids {
                    if let Err(e) = self.engine.set_needs_reflect(&user_id) {
                        tracing::warn!("Failed to set reflection marker for user {}: {:?}", user_id, e);
                    }
                }

                // 累积触发：新增 L1 跨过配置阈值的倍数时触发社区检测
                let community_step = self.config.community_trigger_l1_step.max(1);
                for (user_id, delta) in l1_increase_by_user {
                    match self.engine.bump_l1_count_and_get_range(&user_id, delta).await {
                        Ok((before, after)) if before / community_step < after / community_step && after >= community_step => {
                            if let Err(e) = self.engine.set_needs_community(&user_id) {
                                tracing::warn!("Failed to set community marker for user {}: {:?}", user_id, e);
                            } else {
                                tracing::info!(
                                    "Triggered community detection for user {} (L1 count: {} -> {}, step={})",
                                    user_id,
                                    before,
                                    after,
                                    community_step
                                );
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!("Failed to update L1 count for user {}: {:?}", user_id, e);
                        }
                    }
                }

                // Real-time Task Reflection (Bottom-up)
                if self.engine.task_reflection {
                    let mut parents_to_update: std::collections::HashMap<uuid::Uuid, String> = std::collections::HashMap::new();
                    for unit in &units_to_store {
                        if let Some(ref meta) = unit.task_metadata {
                            if meta.status == memorose_common::TaskStatus::Completed {
                                if let Ok(incoming) = self.engine.graph().get_incoming_edges(&unit.user_id, unit.id).await {
                                    for edge in incoming {
                                        if edge.relation == memorose_common::RelationType::IsSubTaskOf {
                                            parents_to_update.insert(edge.source_id, unit.user_id.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }

                    for (parent_id, user_id) in parents_to_update {
                        let _ = self.update_parent_progress(&user_id, parent_id).await;
                    }
                }
            }

            for event_id in processed_event_ids {
                self.engine.mark_event_processed(&event_id).await?;
            }
            Ok(true)
        }.await;

        if result.is_err() {
            for event_id in &processing_event_ids {
                match self.engine.increment_retry_count_if_pending(event_id).await {
                    Ok(Some(_)) => {}
                    Ok(None) => {}
                    Err(e) => {
                        tracing::error!("Failed to increment retry count for {}: {:?}", event_id, e);
                    }
                }
            }
        }

        result
    }

    async fn run_community_cycle(&self) -> Result<()> {
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
                }
                Err(e) => {
                    tracing::warn!("Community processing failed for user {}: {:?}", user_id, e);
                }
            }

            self.engine.clear_community_marker(&user_id)?;
        }
        Ok(())
    }

    async fn run_insight_cycle(&self) -> Result<()> {
        if self.llm_client.is_none() {
            return Ok(())
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
                Err(_) => {
                    engine.clear_reflection_marker(&user_id)?;
                    continue;
                }
            };

            // Extract unique stream IDs
            let mut unique_streams = std::collections::HashSet::new();
            for unit in recent_l1s {
                unique_streams.insert(unit.stream_id);
            }

            if !unique_streams.is_empty() {
                tracing::info!("Found {} active streams for reflection (user {})", unique_streams.len(), user_id);
                for stream_id in unique_streams {
                    match engine.reflect_on_session(&user_id, stream_id).await {
                        Ok(_) => {
                            tracing::debug!("Reflection completed for stream {} (user {})", stream_id, user_id);
                        }
                        Err(e) => {
                            tracing::warn!("Reflection failed for stream {} (user {}): {:?}", stream_id, user_id, e);
                        }
                    }
                }
            }

            engine.clear_reflection_marker(&user_id)?;
        }

        Ok(())
    }

    pub async fn update_parent_progress(&self, user_id: &str, parent_id: uuid::Uuid) -> Result<()> {
        // Atomic locking per task to prevent race conditions during Read-Modify-Write
        let lock = {
            self.engine.task_locks.entry(parent_id).or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))).value().clone()
        };

        let _guard = lock.lock().await;

        let incoming = self.engine.graph().get_incoming_edges(user_id, parent_id).await?;

        let mut total = 0;
        let mut completed = 0;

        for edge in incoming {
            if edge.relation == memorose_common::RelationType::IsSubTaskOf {
                total += 1;
                if let Some(child) = self.engine.get_memory_unit(user_id, edge.source_id).await? {
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
                 let mut meta = parent.task_metadata.clone().unwrap_or(memorose_common::TaskMetadata {
                     status: memorose_common::TaskStatus::Active,
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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::llm::CompressionOutput;
    use async_trait::async_trait;
    use memorose_common::{Event, EventContent};
    use uuid::Uuid;

    const TEST_USER: &str = "test_user";
    const TEST_APP: &str = "test_app";

    struct MockLLM {
        fail_compress: bool,
    }

    #[async_trait]
    impl crate::llm::LLMClient for MockLLM {
        async fn generate(&self, _prompt: &str) -> Result<String> { Ok("mock".into()) }
        async fn embed(&self, _text: &str) -> Result<Vec<f32>> { Ok(vec![0.0; 384]) }
        async fn compress(&self, text: &str) -> Result<CompressionOutput> {
            if self.fail_compress {
                return Err(anyhow::anyhow!("LLM Error"));
            }
            Ok(CompressionOutput { content: text.to_string(), valid_at: None })
        }
        async fn summarize_group(&self, _texts: Vec<String>) -> Result<String> { Ok("summary".into()) }
        async fn describe_image(&self, _url: &str) -> Result<String> { Ok("image".into()) }
        async fn describe_video(&self, _url: &str) -> Result<String> { Ok("video".into()) }
        async fn transcribe(&self, _url: &str) -> Result<String> { Ok("audio".into()) }
    }

    #[tokio::test]
    async fn test_consolidation_with_llm_failure() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM { fail_compress: true }));

        let event = Event::new(TEST_USER.into(), TEST_APP.into(), Uuid::new_v4(), EventContent::Text("Hello".into()));
        engine.ingest_event_directly(event.clone()).await?;

        let processed = worker.run_consolidation_cycle().await?;
        assert!(processed);

        let pending = engine.fetch_pending_events().await?;
        assert!(pending.is_empty(), "Event should be marked processed even if LLM failed (fallback mode)");

        Ok(())
    }

    #[tokio::test]
    async fn test_consolidation_cycle_success() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM { fail_compress: false }));

        let event = Event::new(TEST_USER.into(), TEST_APP.into(), Uuid::new_v4(), EventContent::Text("Success".into()));
        engine.ingest_event_directly(event.clone()).await?;

        worker.run_consolidation_cycle().await?;

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 1);
        assert_eq!(l1s[0].content, "Success");

        Ok(())
    }
}
