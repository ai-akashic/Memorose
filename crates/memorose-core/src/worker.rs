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
        tracing::info!("Background Worker started.");
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut tick_count: u64 = 0;

        loop {
            interval.tick().await;

            if !self.is_leader().await {
                continue;
            }

            tick_count += 1;

            // 1. Forgetting (Decay) - scan active_user markers
            let decay_interval = Duration::from_secs(60);
            if tick_count % decay_interval.as_secs() == 0 {
                if let Err(e) = self.run_decay_cycle().await {
                    tracing::error!("Decay cycle failed: {:?}", e);
                }
            }

            // 2. L0 -> L1 (Consolidation)
            let consolidation_interval = Duration::from_millis(self.config.consolidation_interval_ms);
            if tick_count % consolidation_interval.as_secs().max(1) == 0 {
                if let Err(e) = self.run_consolidation_cycle().await {
                    Err::<bool, _>(e).map_err(|e| tracing::error!("Consolidation cycle failed: {:?}", e)).ok();
                }
            }

            // 3. Compaction - Every 1 hour (3600 ticks)
            if tick_count % 3600 == 0 {
                if let Err(e) = self.run_compaction_cycle().await {
                    tracing::error!("Compaction cycle failed: {:?}", e);
                }
            }

            // 4. Cognitive Cycles (Requires LLM) - marker-driven
            if self.llm_client.is_some() {
                // Insight Cycle (Reflection) - driven by needs_reflect markers
                let insight_interval = Duration::from_millis(self.config.insight_interval_ms);
                if tick_count % insight_interval.as_secs().max(1) == 0 {
                    if let Err(e) = self.run_insight_cycle().await {
                        tracing::error!("Insight cycle failed: {:?}", e);
                    }
                }

                // Community Cycle (L2) - driven by needs_community markers
                let community_interval = Duration::from_millis(self.config.community_interval_ms);
                if tick_count % community_interval.as_secs().max(1) == 0 {
                    if let Err(e) = self.run_community_cycle().await {
                        tracing::error!("Community cycle failed: {:?}", e);
                    }
                }
            }
        }
    }

    async fn run_compaction_cycle(&self) -> Result<()> {
        let should_compact = {
            let last = self.last_compaction.lock().unwrap();
            last.elapsed() > Duration::from_secs(3600)
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
        let should_decay = {
            let last = self.last_decay.lock().unwrap();
            last.elapsed() > Duration::from_secs(60)
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
                    self.engine.decay_importance(user_id, 0.9).await?;

                    let pruned = self.engine.prune_memories(user_id, 0.1).await?;
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

    async fn run_consolidation_cycle(&self) -> Result<bool> {
        let events = self.engine.fetch_pending_events().await?;
        if events.is_empty() {
            return Ok(false);
        }

        tracing::info!("Consolidating {} events concurrently...", events.len());
        let mut units_to_store = Vec::new();
        let mut processed_event_ids = Vec::new();

        let mut join_set = tokio::task::JoinSet::new();
        let llm_client = self.llm_client.clone();

        for event in events {
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
                    if let Ok(Some((unit, eid))) = res {
                        units_to_store.push(unit);
                        processed_event_ids.push(eid);
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

                // 3. Embedding
                let embedding = if let Some(ref client) = llm_clone {
                    client.embed(&summary).await.ok()
                } else {
                    None
                };

                // 4. Create Unit with user_id and app_id
                let mut unit = MemoryUnit::new(user_id, app_id, stream_id, summary, embedding);
                unit.valid_time = valid_at.and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok().map(|d| d.with_timezone(&chrono::Utc)));
                unit.assets = assets;

                // 5. Task-specific Logic
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

                Some((unit, event_id.to_string()))
            });
        }

        // Collect remaining
        while let Some(res) = join_set.join_next().await {
            if let Ok(Some((unit, eid))) = res {
                units_to_store.push(unit);
                processed_event_ids.push(eid);
            }
        }

        if !units_to_store.is_empty() {
            self.engine.store_memory_units(units_to_store.clone()).await?;

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
    }

    async fn run_community_cycle(&self) -> Result<()> {
        let user_ids = self.engine.get_pending_communities()?;
        if user_ids.is_empty() {
            return Ok(());
        }

        tracing::info!("Running L2 Graph Community Detection for {} users...", user_ids.len());
        for user_id in user_ids {
            if let Err(e) = self.engine.process_communities(&user_id).await {
                tracing::warn!("Community processing failed for user {}: {:?}", user_id, e);
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
            let recent_l1s = match engine.fetch_recent_l1_units(&user_id, 20).await {
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
                    if let Err(e) = engine.reflect_on_session(&user_id, stream_id).await {
                        tracing::warn!("Reflection failed for stream {} (user {}): {:?}", stream_id, user_id, e);
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
        let engine = MemoroseEngine::new(temp_dir.path(), 1000, true, true).await?;

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
        let engine = MemoroseEngine::new(temp_dir.path(), 1000, true, true).await?;

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
