use crate::MemoroseEngine;
use crate::llm::LLMClient;
use crate::llm::gemini::GeminiClient;
use memorose_common::{MemoryUnit, config::AppConfig, Asset};
use tokio::time::Duration;
use tokio::sync::mpsc;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

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
        let llm_client = crate::llm::create_llm_client(&config.llm);
        
        if llm_client.is_none() {
            tracing::warn!("BackgroundWorker starting without API Key. Summary and Insight features will be disabled/degraded.");
        }

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

    /// Generates a semantic fingerprint by stripping numbers, punctuation, and converting to lowercase.
    /// This allows us to catch highly similar structural logs (e.g. "Tool failed at 12:01" vs "Tool failed at 12:02").
    fn generate_semantic_fingerprint(text: &str) -> u64 {
        let normalized: String = text.chars()
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

    async fn extract_text_from_event(event: &memorose_common::Event, llm: Option<&dyn crate::llm::LLMClient>) -> String {
        match &event.content {
            memorose_common::EventContent::Text(t) => t.clone(),
            memorose_common::EventContent::Image(url) => {
                if let Some(client) = llm {
                    client.describe_image(url).await.map(|r| r.data).unwrap_or_else(|e| format!("Image at {}", url))
                } else {
                    format!("Image at {}", url)
                }
            }
            memorose_common::EventContent::Audio(url) => {
                if let Some(client) = llm {
                    client.transcribe(url).await.map(|r| r.data).unwrap_or_else(|e| format!("Audio at {}", url))
                } else {
                    format!("Audio at {}", url)
                }
            }
            memorose_common::EventContent::Video(url) => {
                if let Some(client) = llm {
                    client.describe_video(url).await.map(|r| r.data).unwrap_or_else(|e| format!("Video at {}", url))
                } else {
                    format!("Video at {}", url)
                }
            }
            memorose_common::EventContent::Json(val) => val.to_string(),
        }
    }

    fn extract_assets_from_event(event: &memorose_common::Event) -> Vec<memorose_common::Asset> {
        match &event.content {
            memorose_common::EventContent::Text(_) | memorose_common::EventContent::Json(_) => vec![],
            memorose_common::EventContent::Image(url) => vec![memorose_common::Asset {
                storage_key: url.clone(),
                original_name: "image".to_string(),
                asset_type: "image".to_string(),
                metadata: std::collections::HashMap::new(),
            }],
            memorose_common::EventContent::Audio(url) => vec![memorose_common::Asset {
                storage_key: url.clone(),
                original_name: "audio".to_string(),
                asset_type: "audio".to_string(),
                metadata: std::collections::HashMap::new(),
            }],
            memorose_common::EventContent::Video(url) => vec![memorose_common::Asset {
                storage_key: url.clone(),
                original_name: "video".to_string(),
                asset_type: "video".to_string(),
                metadata: std::collections::HashMap::new(),
            }],
        }
    }

    async fn run_consolidation_cycle(&self) -> Result<bool> {
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

        // Mark failed
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

        // 1.5 Batching / Prompt Packing (Group contiguous events)
        let mut packed_batches: Vec<Vec<memorose_common::Event>> = Vec::new();
        let mut current_batch: Vec<memorose_common::Event> = Vec::new();
        let mut current_key: Option<(String, Option<String>)> = None;

        for event in valid_events {
            let is_agent = event.metadata.get("role").and_then(|v| v.as_str()) == Some("assistant") 
                || event.metadata.get("agent_id").is_some();
            let agent_id = if is_agent {
                event.metadata.get("agent_id").and_then(|v| v.as_str()).map(|s| s.to_string())
                    .or(Some("default_agent".to_string())) // Fallback to group all raw assistant messages together
            } else {
                None
            };
            
            let key = (event.user_id.clone(), agent_id);
            
            if Some(&key) != current_key.as_ref() || current_batch.len() >= 10 { // Max 10 events per packed prompt
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

        // Spawn Producer
        tokio::spawn(async move {
            let mut join_set = tokio::task::JoinSet::new();
            
            for mut events in packed_batches {
                if events.is_empty() { continue; }
                let llm = llm_client_clone.clone();
                let engine = engine_clone.clone();
                
                // For a packed batch, we will extract the common identifiers from the first event
                let first_event = events.remove(0);
                let mut combined_text = format!("Message 1: {}", Self::extract_text_from_event(&first_event, llm.as_deref()).await);
                
                // Metadata logic (merge simple fields or keep first)
                let metadata = first_event.metadata.clone();
                let user_id = first_event.user_id.clone();
                let app_id = first_event.app_id.clone();
                let stream_id = first_event.stream_id;
                let is_agent = metadata.get("role").and_then(|v| v.as_str()) == Some("assistant") 
                    || metadata.get("agent_id").is_some();
                    
                // We keep all event IDs to mark them as processed later
                let mut event_ids = vec![first_event.id];
                
                let mut assets = Self::extract_assets_from_event(&first_event);

                // Append the rest
                for (i, evt) in events.into_iter().enumerate() {
                    combined_text.push_str(&format!("\nMessage {}: {}", i + 2, Self::extract_text_from_event(&evt, llm.as_deref()).await));
                    event_ids.push(evt.id);
                    assets.extend(Self::extract_assets_from_event(&evt));
                }

                // Limit concurrency
                if join_set.len() >= concurrency_limit {
                    if let Some(res) = join_set.join_next().await {
                         match res {
                             Ok(data) => { let _ = tx.send(data).await; }
                             Err(e) => tracing::error!("Compression task panicked: {:?}", e),
                         }
                    }
                }

                join_set.spawn(async move {
                    // Semantic Deduplication Check
                    let fingerprint = Self::generate_semantic_fingerprint(&combined_text);
                    let dedup_key = format!("dedup:{}:{}", user_id, fingerprint);
                    
                    let is_duplicate = if let Ok(Some(last_seen_bytes)) = engine.system_kv().get(dedup_key.as_bytes()) {
                        if let Some(last_seen) = String::from_utf8(last_seen_bytes).ok().and_then(|s| s.parse::<u64>().ok()) {
                            let now = chrono::Utc::now().timestamp() as u64;
                            // Deduplicate if seen within the last 1 hour (3600 seconds)
                            now - last_seen < 3600
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

                    (event_ids, user_id, app_id, stream_id, summary, valid_at, assets, metadata)
                });
            }

            // Drain remaining
            while let Some(res) = join_set.join_next().await {
                match res {
                    Ok(data) => { let _ = tx.send(data).await; }
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

        Ok(any_processed)
    }

    /// Helper for pipeline batch processing
    async fn process_pipeline_batch(
        &self, 
        batch: Vec<(Vec<uuid::Uuid>, String, String, uuid::Uuid, String, Option<String>, Vec<Asset>, serde_json::Value)>
    ) -> Result<Vec<String>> {
        if batch.is_empty() { return Ok(Vec::new()); }

        // Phase 2: Batch Embed
        let mut texts_to_embed = Vec::new();
        let mut needs_embedding = Vec::new();

        for (idx, (event_ids, _, _, _, summary, _, _, metadata)) in batch.iter().enumerate() {
            match Self::parse_metadata_embedding(metadata) {
                Some(Some(_)) => continue,
                Some(None) | None => {}
            }
            if summary.trim().is_empty() {
                tracing::warn!("Skipping empty summary for packed events {:?}", event_ids);
                continue;
            }
            texts_to_embed.push(summary.clone());
            needs_embedding.push(idx);
        }

        let embeddings = if !texts_to_embed.is_empty() {
            if let Some(client) = self.llm_client.as_ref() {
                // ... same embedding logic ...
                match client.embed_batch(texts_to_embed.clone()).await {
                    Ok(embs) if embs.data.len() == needs_embedding.len() => embs.data,
                    _ => {
                        // Fallback
                        let mut embs = Vec::with_capacity(texts_to_embed.len());
                        for text in texts_to_embed {
                            embs.push(client.embed(&text).await.map(|r| r.data).unwrap_or_default());
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

        for (idx, (event_ids, user_id, app_id, stream_id, summary, valid_at, assets, metadata)) in batch.into_iter().enumerate() {
            let embedding = match Self::parse_metadata_embedding(&metadata) {
                Some(Some(vec)) => Some(vec),
                Some(None) | None => embeddings_by_idx.remove(&idx),
            };

            let is_agent = metadata.get("role").and_then(|v| v.as_str()) == Some("assistant") 
                || metadata.get("agent_id").is_some();
            
            let agent_id = metadata.get("agent_id").and_then(|v| v.as_str()).map(|s| s.to_string());
            
            let memory_type = if is_agent {
                memorose_common::MemoryType::Procedural
            } else {
                memorose_common::MemoryType::Factual
            };

            let mut unit = MemoryUnit::new(
                user_id, 
                agent_id,
                app_id, 
                stream_id, 
                memory_type,
                summary, 
                embedding
            );
            unit.valid_time = valid_at.and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok().map(|d| d.with_timezone(&chrono::Utc)));
            unit.assets = assets;

            // Link to all source events
            for evt_id in &event_ids {
                unit.references.push(*evt_id);
            }

            // Task Metadata Logic
            if let Some(level) = metadata.get("target_level").and_then(|v| v.as_u64()) {
                unit.level = level as u8;
                if let Some(pid_str) = metadata.get("parent_id").and_then(|v| v.as_str()) {
                    if let Ok(pid) = uuid::Uuid::parse_str(pid_str) { unit.references.push(pid); }
                }
                if level >= 1 {
                    let status = match metadata.get("task_status").and_then(|v| v.as_str()) {
                        Some("Completed") => memorose_common::TaskStatus::Completed,
                        Some("Active") => memorose_common::TaskStatus::Active,
                        Some("Failed") => memorose_common::TaskStatus::Failed,
                        _ => memorose_common::TaskStatus::Pending,
                    };
                    unit.task_metadata = Some(memorose_common::TaskMetadata {
                        status,
                        progress: metadata.get("task_progress").and_then(|v| v.as_f64()).unwrap_or(0.0) as f32,
                    });
                }
            }

            units_to_store.push(unit);
            for evt_id in event_ids {
                processed_ids.push(evt_id.to_string());
            }
        }

        if !units_to_store.is_empty() {
            self.engine.store_memory_units(units_to_store.clone()).await?;

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
                 if let Ok((before, after)) = self.engine.bump_l1_count_and_get_range(&user_id, delta).await {
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
                             if let Ok(incoming) = self.engine.graph().get_incoming_edges(&unit.user_id, unit.id).await {
                                 for edge in incoming {
                                     if edge.relation == memorose_common::RelationType::IsSubTaskOf {
                                         let _ = self.update_parent_progress(&unit.user_id, edge.source_id).await;
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
        async fn generate(&self, _prompt: &str) -> Result<crate::llm::LLMResponse<String>> { Ok(crate::llm::LLMResponse::default()) }
        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> { Ok(crate::llm::LLMResponse { data: vec![0.0; 384], usage: Default::default() }) }
        async fn compress(&self, text: &str, _is_agent: bool) -> Result<crate::llm::LLMResponse<CompressionOutput>> {
            if self.fail_compress {
                return Err(anyhow::anyhow!("LLM Error"));
            }
            Ok(crate::llm::LLMResponse { data: CompressionOutput { content: text.to_string(), valid_at: None }, usage: Default::default() })
        }
        async fn summarize_group(&self, _texts: Vec<String>) -> Result<crate::llm::LLMResponse<String>> { Ok(crate::llm::LLMResponse { data: "summary".into(), usage: Default::default() }) }
        async fn describe_image(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> { Ok(crate::llm::LLMResponse { data: "image".into(), usage: Default::default() }) }
        async fn describe_video(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> { Ok(crate::llm::LLMResponse { data: "video".into(), usage: Default::default() }) }
        async fn transcribe(&self, _url: &str) -> Result<crate::llm::LLMResponse<String>> { Ok(crate::llm::LLMResponse { data: "audio".into(), usage: Default::default() }) }
    }

    #[tokio::test]
    async fn test_consolidation_with_llm_failure() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut worker = BackgroundWorker::new(engine.clone());
        worker.llm_client = Some(Arc::new(MockLLM { fail_compress: true }));

        let event = Event::new(TEST_USER.into(), None, TEST_APP.into(), Uuid::new_v4(), EventContent::Text("Hello".into()));
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

        let event = Event::new(TEST_USER.into(), None, TEST_APP.into(), Uuid::new_v4(), EventContent::Text("Success".into()));
        engine.ingest_event_directly(event.clone()).await?;

        worker.run_consolidation_cycle().await?;

        let l1s = engine.fetch_recent_l1_units(TEST_USER, 10).await?;
        assert_eq!(l1s.len(), 1);
        assert_eq!(l1s[0].content, "Message 1: Success");

        Ok(())
    }
}
