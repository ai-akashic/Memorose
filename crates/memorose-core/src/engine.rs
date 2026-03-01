use std::path::PathBuf;
use crate::storage::kv::KvStore;
use crate::storage::system_kv::SystemKvStore;
use crate::storage::vector::VectorStore;
use crate::storage::index::TextIndex;
use crate::storage::graph::GraphStore;
use crate::arbitrator::Arbitrator;
use crate::reranker::Reranker;
use memorose_common::{Event, MemoryUnit, GraphEdge, RelationType, TimeRange};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;
use flate2::Compression;
use dashmap::DashMap;
use tokio::sync::Mutex;
use lancedb::connect;

#[derive(Clone)]
pub struct MemoroseEngine {
    _kv: KvStore,
    vector: VectorStore,
    index: TextIndex,
    graph: GraphStore,
    arbitrator: Arbitrator,
    reranker: std::sync::Arc<dyn Reranker>,
    _root_path: PathBuf,
    _commit_interval_ms: u64,
    pub auto_planner: bool,
    pub task_reflection: bool,
    pub task_locks: Arc<DashMap<Uuid, Arc<Mutex<()>>>>,
    pub auto_link_similarity_threshold: f32,
    // New: Query optimization components
    query_cache: Arc<crate::graph::QueryCache>,
    batch_executor: Arc<crate::graph::BatchExecutor>,
}

impl MemoroseEngine {
    fn build_time_filter(&self, range: Option<TimeRange>) -> Option<String> {
        let range = range?;
        let mut conditions = Vec::new();

        if let Some(start) = range.start {
            conditions.push(format!("valid_time >= {}", start.timestamp_micros()));
        }
        if let Some(end) = range.end {
            conditions.push(format!("valid_time <= {}", end.timestamp_micros()));
        }

        if conditions.is_empty() {
            None
        } else {
            Some(conditions.join(" AND "))
        }
    }

    pub fn build_user_filter(&self, user_id: &str, app_id: Option<&str>, extra: Option<String>) -> Option<String> {
        fn escape_sql_string(s: &str) -> String {
            s.replace('\'', "''")
        }
        let mut conditions = vec![format!("user_id = '{}'", escape_sql_string(user_id))];
        if let Some(aid) = app_id {
            conditions.push(format!("app_id = '{}'", escape_sql_string(aid)));
        }
        if let Some(e) = extra {
            conditions.push(e);
        }
        Some(conditions.join(" AND "))
    }
}

fn cosine_similarity(v1: &[f32], v2: &[f32]) -> f32 {
    let dot_product: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
    let magnitude_v1: f32 = v1.iter().map(|v| v * v).sum::<f32>().sqrt();
    let magnitude_v2: f32 = v2.iter().map(|v| v * v).sum::<f32>().sqrt();
    if magnitude_v1 < f32::EPSILON || magnitude_v2 < f32::EPSILON {
        return 0.0;
    }
    (dot_product / (magnitude_v1 * magnitude_v2)).clamp(-1.0, 1.0)
}

impl MemoroseEngine {
    pub async fn new_with_default_threshold(
        path: impl Into<PathBuf>,
        commit_interval_ms: u64,
        auto_planner: bool,
        task_reflection: bool,
    ) -> Result<Self> {
        let dim = memorose_common::config::AppConfig::load().ok().map(|c| c.llm.embedding_dim).unwrap_or(768);
        Self::new(
            path,
            commit_interval_ms,
            auto_planner,
            task_reflection,
            memorose_common::config::DEFAULT_AUTO_LINK_SIMILARITY_THRESHOLD,
            dim,
        ).await
    }

    pub async fn new(
        path: impl Into<PathBuf>,
        commit_interval_ms: u64,
        auto_planner: bool,
        task_reflection: bool,
        auto_link_similarity_threshold: f32,
        embedding_dim: i32,
    ) -> Result<Self> {
        let root_path = path.into();
        std::fs::create_dir_all(&root_path)?;
        let root_path = root_path.canonicalize()?;

        let kv_path = root_path.join("rocksdb");
        let kv = tokio::task::spawn_blocking(move || {
            KvStore::open(kv_path)
        }).await??;

        let vector_path = root_path.join("lancedb");
        let vector_uri = vector_path.to_str().unwrap().to_string();
        let vector = VectorStore::new(&vector_uri, embedding_dim).await?;

        let db = Arc::new(connect(&vector_uri).execute().await?);
        let graph = GraphStore::new(db).await?;

        let index_path = root_path.join("tantivy");
        let index = tokio::task::spawn_blocking(move || {
            TextIndex::new(index_path, commit_interval_ms)
        }).await??;

        let arbitrator = Arbitrator::new();
        let reranker: Arc<dyn crate::reranker::Reranker> = if let Ok(config) = memorose_common::config::AppConfig::load() {
            if config.reranker.r#type == memorose_common::config::RerankerType::Http && config.reranker.endpoint.is_some() {
                Arc::new(crate::reranker::HttpReranker::new(config.reranker.endpoint.unwrap()))
            } else {
                Arc::new(crate::reranker::WeightedReranker::new())
            }
        } else {
            Arc::new(crate::reranker::WeightedReranker::new())
        };

        // 初始化查询优化组件
        let query_cache = Arc::new(crate::graph::QueryCache::new(crate::graph::CacheConfig {
            ttl: std::time::Duration::from_secs(300),  // 5 分钟 TTL
            max_entries: 5000,
            enabled: true,
        }));
        let batch_executor = Arc::new(crate::graph::BatchExecutor::new(graph.clone()));

        Ok(Self {
            _kv: kv,
            vector,
            index,
            graph,
            arbitrator,
            reranker,
            _root_path: root_path,
            _commit_interval_ms: commit_interval_ms,
            auto_planner,
            task_reflection,
            task_locks: Arc::new(DashMap::new()),
            auto_link_similarity_threshold,
            query_cache,
            batch_executor,
        })
    }

    pub fn with_reranker(mut self, reranker: std::sync::Arc<dyn Reranker>) -> Self {
        self.reranker = reranker;
        self
    }

    pub async fn ingest_event(&self, event: Event) -> Result<()> {
        self.ingest_event_directly(event).await
    }

    pub async fn ingest_event_directly(&self, event: Event) -> Result<()> {
        // Validate event content is not empty
        let is_empty = match &event.content {
            memorose_common::EventContent::Text(text) => text.trim().is_empty(),
            memorose_common::EventContent::Image(url) => url.trim().is_empty(),
            memorose_common::EventContent::Audio(url) => url.trim().is_empty(),
            memorose_common::EventContent::Video(url) => url.trim().is_empty(),
            memorose_common::EventContent::Json(val) => {
                val.is_null() || (val.is_string() && val.as_str().unwrap_or("").trim().is_empty())
            }
        };

        if is_empty {
            return Err(anyhow::anyhow!(
                "Rejected empty event: event_id={}, user_id={}, content type={:?}",
                event.id,
                event.user_id,
                std::mem::discriminant(&event.content)
            ));
        }

        let event_id = event.id.to_string();
        let user_id = event.user_id.clone();
        let app_id = event.app_id.clone();

        // Store event under user prefix
        let key = format!("u:{}:event:{}", user_id, event_id);
        let val = serde_json::to_vec(&event)?;
        self._kv.put(key.as_bytes(), &val)?;

        // Global pending queue with user_id/app_id in value
        let pending_key = format!("pending:{}", event_id);
        let pending_val = serde_json::to_vec(&serde_json::json!({
            "user_id": user_id,
            "app_id": app_id
        }))?;
        self.system_kv().put(pending_key.as_bytes(), &pending_val)?;

        // Set active_user marker
        let active_key = format!("active_user:{}", user_id);
        self.system_kv().put(active_key.as_bytes(), &[])?;

        Ok(())
    }

    pub fn kv(&self) -> KvStore {
        self._kv.clone()
    }

    pub fn system_kv(&self) -> SystemKvStore {
        SystemKvStore::new(self._kv.clone())
    }

    pub fn root_path(&self) -> PathBuf {
        self._root_path.clone()
    }

    pub fn commit_interval_ms(&self) -> u64 {
        self._commit_interval_ms
    }

    pub fn auto_planner(&self) -> bool {
        self.auto_planner
    }

    pub fn task_reflection(&self) -> bool {
        self.task_reflection
    }

    pub async fn compact_vector_store(&self) -> Result<()> {
        self.vector.compact_files("memories").await?;
        Ok(())
    }

    pub fn graph(&self) -> &GraphStore {
        &self.graph
    }

    fn derive_l2_app_id(units: &[MemoryUnit]) -> String {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for unit in units {
            if !unit.app_id.is_empty() {
                *counts.entry(unit.app_id.clone()).or_insert(0) += 1;
            }
        }

        let mut best_app = String::new();
        let mut best_count = 0usize;
        for (app_id, count) in counts {
            if count > best_count || (count == best_count && (best_app.is_empty() || app_id < best_app)) {
                best_app = app_id;
                best_count = count;
            }
        }

        if best_count > 0 {
            best_app
        } else {
            units
                .first()
                .map(|u| u.app_id.clone())
                .unwrap_or_default()
        }
    }

    pub async fn fetch_pending_events(&self) -> Result<Vec<Event>> {
        self.fetch_pending_events_limited(usize::MAX).await
    }

    /// Count pending events without deserialising their bodies — much cheaper than
    /// `fetch_pending_events().len()` for systems with many pending events.
    pub async fn count_pending_events(&self) -> Result<usize> {
        let skv = self.system_kv();
        tokio::task::spawn_blocking(move || skv.count_prefix(b"pending:")).await?
    }

    pub async fn fetch_pending_events_limited(&self, limit: usize) -> Result<Vec<Event>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let skv = self.system_kv();
        let pending_pairs = tokio::task::spawn_blocking(move || {
            skv.scan(b"pending:")
        }).await??;

        let mut events = Vec::new();
        for (key, val) in pending_pairs {
            if events.len() >= limit {
                break;
            }

            let key_str = String::from_utf8(key)?;
            let parts: Vec<&str> = key_str.split(':').collect();
            if parts.len() == 2 {
                let event_id = parts[1];
                // Parse user_id from the pending value
                let user_id = if !val.is_empty() {
                    if let Ok(info) = serde_json::from_slice::<serde_json::Value>(&val) {
                        info["user_id"].as_str().unwrap_or("_legacy").to_string()
                    } else {
                        "_legacy".to_string()
                    }
                } else {
                    "_legacy".to_string()
                };
                if let Some(event) = self.get_event(&user_id, event_id).await? {
                    events.push(event);
                }
            }
        }
        Ok(events)
    }

    pub async fn mark_event_processed(&self, id: &str) -> Result<()> {
        let key = format!("pending:{}", id);
        self.system_kv().delete(key.as_bytes())?;
        // 同时删除重试计数
        let retry_key = format!("retry_count:{}", id);
        self.system_kv().delete(retry_key.as_bytes())?;
        Ok(())
    }

    pub async fn get_retry_count(&self, id: &str) -> Result<u32> {
        let key = format!("retry_count:{}", id);
        match self.system_kv().get(key.as_bytes())? {
            Some(bytes) => {
                let count = u32::from_le_bytes(bytes.try_into().unwrap_or([0, 0, 0, 0]));
                Ok(count)
            }
            None => Ok(0),
        }
    }

    pub async fn increment_retry_count(&self, id: &str) -> Result<u32> {
        let key = format!("retry_count:{}", id);
        let current = self.get_retry_count(id).await?;
        let new_count = current + 1;
        self.system_kv().put(key.as_bytes(), &new_count.to_le_bytes())?;
        Ok(new_count)
    }

    pub async fn increment_retry_count_if_pending(&self, id: &str) -> Result<Option<u32>> {
        let pending_key = format!("pending:{}", id);
        if self.system_kv().get(pending_key.as_bytes())?.is_none() {
            return Ok(None);
        }
        let count = self.increment_retry_count(id).await?;
        Ok(Some(count))
    }

    pub async fn mark_event_failed(&self, id: &str, error: &str) -> Result<()> {
        // 从 pending 队列移除
        let pending_key = format!("pending:{}", id);
        self.system_kv().delete(pending_key.as_bytes())?;

        // 移到失败队列
        let retry_count = self.get_retry_count(id).await?;
        let failed_key = format!("failed:{}", id);
        let failed_info = serde_json::json!({
            "error": error,
            "failed_at": chrono::Utc::now().to_rfc3339(),
            "retry_count": retry_count
        });
        self.system_kv().put(failed_key.as_bytes(), &serde_json::to_vec(&failed_info)?)?;

        // 清理重试计数，避免失败事件残留状态。
        let retry_key = format!("retry_count:{}", id);
        self.system_kv().delete(retry_key.as_bytes())?;

        Ok(())
    }

    pub async fn get_event(&self, user_id: &str, id: &str) -> Result<Option<Event>> {
        let key = format!("u:{}:event:{}", user_id, id);
        let val = self._kv.get(key.as_bytes())?;
        match val {
            Some(bytes) => {
                let event: Event = serde_json::from_slice(&bytes)?;
                Ok(Some(event))
            }
            None => Ok(None),
        }
    }

    pub async fn delete_event(&self, user_id: &str, id: &str) -> Result<()> {
        let key = format!("u:{}:event:{}", user_id, id);
        self._kv.delete(key.as_bytes())?;
        Ok(())
    }

    // ── Marker Methods ──────────────────────────────────────────────

    pub fn set_needs_reflect(&self, user_id: &str) -> Result<()> {
        let key = format!("needs_reflect:{}", user_id);
        let ts = chrono::Utc::now().timestamp().to_string();
        self.system_kv().put(key.as_bytes(), ts.as_bytes())
    }

    pub fn set_needs_community(&self, user_id: &str) -> Result<()> {
        let key = format!("needs_community:{}", user_id);
        let ts = chrono::Utc::now().timestamp().to_string();
        self.system_kv().put(key.as_bytes(), ts.as_bytes())
    }

    pub fn get_pending_reflections(&self) -> Result<Vec<String>> {
        let pairs = self.system_kv().scan(b"needs_reflect:")?;
        let mut user_ids = Vec::new();
        for (key, _) in pairs {
            let key_str = String::from_utf8(key)?;
            if let Some(uid) = key_str.strip_prefix("needs_reflect:") {
                user_ids.push(uid.to_string());
            }
        }
        Ok(user_ids)
    }

    pub fn clear_reflection_marker(&self, user_id: &str) -> Result<()> {
        let key = format!("needs_reflect:{}", user_id);
        self.system_kv().delete(key.as_bytes())
    }

    pub fn get_pending_communities(&self) -> Result<Vec<String>> {
        let pairs = self.system_kv().scan(b"needs_community:")?;
        let mut user_ids = Vec::new();
        for (key, _) in pairs {
            let key_str = String::from_utf8(key)?;
            if let Some(uid) = key_str.strip_prefix("needs_community:") {
                user_ids.push(uid.to_string());
            }
        }
        Ok(user_ids)
    }

    pub fn clear_community_marker(&self, user_id: &str) -> Result<()> {
        let key = format!("needs_community:{}", user_id);
        self.system_kv().delete(key.as_bytes())
    }

    // ── Reflection ──────────────────────────────────────────────────

    /// Prospective Reflection: Summarize recent L1 memories into L2 Topic memories.
    pub async fn reflect_on_session(&self, user_id: &str, stream_id: uuid::Uuid) -> Result<()> {
        let recent_l1 = self.fetch_recent_l1_units(user_id, 20).await?;
        let session_units: Vec<MemoryUnit> = recent_l1.into_iter()
            .filter(|u| u.stream_id == stream_id)
            .collect();

        if session_units.is_empty() {
            return Ok(());
        }

        let topic_units = self.arbitrator.extract_topics(user_id, &session_units[0].app_id, stream_id, session_units.clone()).await?;

        if topic_units.is_empty() {
            return Ok(());
        }

        let mut units_to_store = Vec::new();
        for mut unit in topic_units {
            if let Some(client) = self.arbitrator.get_llm_client() {
                if let Ok(embedding) = client.embed(&unit.content).await {
                    unit.embedding = Some(embedding.data);
                }
            }
            units_to_store.push(unit);
        }

        self.store_memory_units(units_to_store.clone()).await?;

        for topic in units_to_store {
            for source_id in topic.references {
                let edge = GraphEdge::new(topic.user_id.clone(), topic.id, source_id, RelationType::DerivedFrom, 1.0);
                self.graph.add_edge(&edge).await?;
            }
        }

        Ok(())
    }

    /// Retrospective Reflection: Apply feedback to the reranker and reinforce graph associations.
    pub async fn apply_reranker_feedback(&self, user_id: &str, cited_ids: Vec<String>, retrieved_ids: Vec<String>) -> Result<()> {
        self.reranker.apply_feedback(&self._kv, cited_ids.clone(), retrieved_ids).await?;

        if cited_ids.len() >= 2 {
            self.reinforce_associations(user_id, cited_ids).await?;
        }

        Ok(())
    }

    /// Internal method to increase edge weights between memories that were useful together.
    async fn reinforce_associations(&self, user_id: &str, cited_ids: Vec<String>) -> Result<()> {
        let uid = user_id.to_string();

        let uuids: Vec<Uuid> = cited_ids.iter()
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect();

        if uuids.len() < 2 {
            return Ok(());
        }

        for i in 0..uuids.len() {
            for j in (i + 1)..uuids.len() {
                let id_a = uuids[i];
                let id_b = uuids[j];

                self.graph.reinforce_edge(&uid, id_a, id_b, 0.1).await?;
                self.graph.reinforce_edge(&uid, id_b, id_a, 0.1).await?;
            }
        }

        Ok(())
    }

    pub async fn export_snapshot(&self, output_path: PathBuf) -> Result<()> {
        let engine = self.clone();
        tokio::task::spawn_blocking(move || {
            tracing::info!("Exporting snapshot to {:?}", output_path);

            engine.index.commit().map_err(|e| anyhow::anyhow!("Tantivy commit failed: {}", e))?;
            engine._kv.flush().map_err(|e| anyhow::anyhow!("RocksDB flush failed: {}", e))?;

            if let Some(parent) = output_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| anyhow::anyhow!("Failed to create parent dir {:?}: {}", parent, e))?;
            }

            let file = std::fs::File::create(&output_path).map_err(|e| anyhow::anyhow!("Failed to create output file {:?}: {}", output_path, e))?;
            let enc = GzEncoder::new(file, Compression::default());
            let mut tar = tar::Builder::new(enc);

            let root = &engine._root_path;
            tracing::info!("Root path for snapshot: {:?}", root);

            if root.join("rocksdb").exists() {
                tracing::info!("Adding rocksdb to tar...");
                engine.append_dir_to_tar(&mut tar, root, "rocksdb")?;
            }
            if root.join("lancedb").exists() {
                tracing::info!("Adding lancedb to tar...");
                engine.append_dir_to_tar(&mut tar, root, "lancedb")?;
            }
            if root.join("tantivy").exists() {
                tracing::info!("Adding tantivy to tar...");
                engine.append_dir_to_tar(&mut tar, root, "tantivy")?;
            }

            tar.finish().map_err(|e| anyhow::anyhow!("Tar finish failed: {}", e))?;
            Ok(())
        }).await?
    }

    fn append_dir_to_tar<W: std::io::Write>(&self, tar: &mut tar::Builder<W>, root: &PathBuf, dir_name: &str) -> Result<()> {
        let dir_path = root.join(dir_name);
        for entry in walkdir::WalkDir::new(&dir_path) {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    if e.io_error().map(|ioe| ioe.kind() == std::io::ErrorKind::NotFound).unwrap_or(false) {
                        continue;
                    }
                    return Err(anyhow::anyhow!("Failed to walk dir {:?}: {}", dir_path, e));
                }
            };

            let path = entry.path();
            if path.is_file() {
                let rel_path = path.strip_prefix(root)?;
                let mut file = match std::fs::File::open(path) {
                    Ok(f) => f,
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::NotFound {
                            continue;
                        }
                        return Err(anyhow::anyhow!("Failed to open file {:?}: {}", path, e));
                    }
                };
                tar.append_file(rel_path, &mut file)?;
            }
        }
        Ok(())
    }

    pub async fn restore_from_snapshot(snapshot_path: PathBuf, target_dir: PathBuf) -> Result<()> {
        tracing::info!("Restoring snapshot from {:?} to {:?}", snapshot_path, target_dir);

        if target_dir.exists() {
            std::fs::remove_dir_all(&target_dir)?;
        }
        std::fs::create_dir_all(&target_dir)?;

        let file = std::fs::File::open(&snapshot_path)?;
        let dec = GzDecoder::new(file);
        let mut archive = tar::Archive::new(dec);

        archive.unpack(&target_dir)?;

        Ok(())
    }

    // ── Memory Storage ──────────────────────────────────────────────

    /// L1: Store processed MemoryUnit (Vector + KV + Text)
    pub async fn store_memory_unit(&self, unit: MemoryUnit) -> Result<()> {
        self.store_memory_unit_with_depth(unit, 0).await
    }

    async fn store_memory_unit_with_depth(&self, unit: MemoryUnit, depth: usize) -> Result<()> {
        let is_goal = unit.level == 3;
        let unit_id = unit.id;
        let user_id = unit.user_id.clone();
        let app_id = unit.app_id.clone();
        let stream_id = unit.stream_id;
        let content = unit.content.clone();
        let references = unit.references.clone();

        self.store_memory_units(vec![unit]).await?;

        // Handle Explicit Linking (Task Hierarchy)
        if !references.is_empty() {
            for parent_id in references {
                let edge = GraphEdge::new(user_id.clone(), unit_id, parent_id, RelationType::IsSubTaskOf, 1.0);
                self.graph.add_edge(&edge).await?;
            }
        }

        // Handle Auto-Planning for L3 Goals
        if is_goal && self.auto_planner && depth < 5 {
            // Write a "pending" marker so callers can observe the in-flight planning state.
            // The task clears it to "done" or "failed" when it finishes.
            let planning_key = format!("planning:{}", unit_id);
            let _ = self.system_kv().put(planning_key.as_bytes(), b"pending");

            let engine = self.clone();
            let uid = user_id.clone();
            let aid = app_id.clone();
            let cnt = content.clone();
            tokio::spawn(async move {
                let key = format!("planning:{}", unit_id);
                match engine.auto_plan_goal(uid, aid, stream_id, unit_id, cnt, depth + 1).await {
                    Ok(()) => {
                        let _ = engine.system_kv().put(key.as_bytes(), b"done");
                    }
                    Err(e) => {
                        tracing::error!("Auto-planning failed for goal {}: {:?}", unit_id, e);
                        let _ = engine.system_kv().put(key.as_bytes(), b"failed");
                    }
                }
            });
        }
        Ok(())
    }

    pub fn auto_plan_goal(&self, user_id: String, app_id: String, stream_id: Uuid, goal_id: Uuid, goal_content: String, depth: usize) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            tracing::info!("Auto-planning goal {} (depth {})", goal_id, depth);

            let milestones = self.arbitrator.decompose_goal(&user_id, &app_id, stream_id, &goal_content).await?;

            if milestones.is_empty() {
                return Ok(());
            }

            for ms in milestones.clone() {
                 self.store_memory_unit_with_depth(ms, depth).await?;
            }

            for ms in milestones {
                let edge = GraphEdge::new(ms.user_id.clone(), ms.id, goal_id, RelationType::IsSubTaskOf, 1.0);
                self.graph.add_edge(&edge).await?;
            }

            Ok(())
        })
    }

    pub async fn store_memory_units(&self, units: Vec<MemoryUnit>) -> Result<()> {
        if units.is_empty() {
            return Ok(());
        }

        // 1. Store Metadata in KV (user-prefixed keys + global index)
        let kv = self._kv.clone();
        let skv = self.system_kv();
        let mut kv_batch = Vec::new();
        let mut marker_user_ids = HashSet::new();
        for unit in &units {
            let key = format!("u:{}:unit:{}", unit.user_id, unit.id);
            let val = serde_json::to_vec(unit)?;
            kv_batch.push((key, val));

            // Global index for dashboard lookups
            let idx_key = format!("idx:unit:{}", unit.id);
            kv_batch.push((idx_key, unit.user_id.as_bytes().to_vec()));

            marker_user_ids.insert(unit.user_id.clone());
        }

        let skv_clone = skv.clone();
        let marker_uids: Vec<String> = marker_user_ids.into_iter().collect();
        tokio::task::spawn_blocking(move || {
            for (k, v) in kv_batch {
                kv.put(k.as_bytes(), &v)?;
            }
            // Set reflection markers for each user that got new units
            for uid in &marker_uids {
                let reflect_key = format!("needs_reflect:{}", uid);
                let ts = chrono::Utc::now().timestamp().to_string();
                skv_clone.put(reflect_key.as_bytes(), ts.as_bytes())?;
            }
            Ok::<(), anyhow::Error>(())
        }).await??;

        // Maintain L1 secondary index for efficient fetch_recent_l1_units.
        // Key: "l1_idx:{user_id}:{id}" -> timestamp_micros as little-endian bytes (fast sort, no JSON).
        // The user_id prefix is critical: without it the global scan mixes all users' L1 units.
        let l1_units: Vec<(String, String, i64)> = units.iter()
            .filter(|u| u.level == 1)
            .map(|u| (u.user_id.clone(), u.id.to_string(), u.transaction_time.timestamp_micros()))
            .collect();
        if !l1_units.is_empty() {
            let kv_l1 = self._kv.clone();
            tokio::task::spawn_blocking(move || {
                for (uid, id, ts_micros) in &l1_units {
                    let key = format!("l1_idx:{}:{}", uid, id);
                    kv_l1.put(key.as_bytes(), &ts_micros.to_le_bytes())?;
                }
                Ok::<(), anyhow::Error>(())
            }).await??;
        }

        // 2. Store Vector in Lance (single "memories" table)
        let units_with_embeddings: Vec<MemoryUnit> = units.iter()
            .filter(|u| u.embedding.is_some())
            .cloned()
            .collect();

        if !units_with_embeddings.is_empty() {
            self.vector.ensure_table("memories").await?;
            self.vector.add("memories", units_with_embeddings).await?;
        }

        // 3. Index Text in Tantivy
        // Optimization: Removed immediate commit/reload to improve write throughput.
        // We rely on the background commit loop (configured via commit_interval_ms) for eventual consistency.
        let index = self.index.clone();
        let units_for_index = units.clone();
        tokio::task::spawn_blocking(move || {
            for unit in &units_for_index {
                index.index_unit(unit)?;
            }
            Ok::<(), anyhow::Error>(())
        }).await??;

        // 4. Automatic Semantic Linking (Parallelized)
        let mut join_set = tokio::task::JoinSet::new();
        for unit in units {
            let engine = self.clone();
            join_set.spawn(async move {
                if let Err(e) = engine.auto_link_memory(&unit).await {
                    tracing::error!("Auto-linking failed for unit {}: {:?}", unit.id, e);
                }
                if let Err(e) = engine.semantic_link_memory(&unit).await {
                    tracing::error!("Semantic linking failed for unit {}: {:?}", unit.id, e);
                }
            });
        }

        while let Some(res) = join_set.join_next().await {
            if let Err(e) = res {
                tracing::error!("Parallel linking task panicked: {:?}", e);
            }
        }

        Ok(())
    }

    async fn auto_link_memory(&self, unit: &MemoryUnit) -> Result<()> {
        if let Some(ref embedding) = unit.embedding {
            let filter = self.build_user_filter(&unit.user_id, None, None);
            let similar = self.search_similar(&unit.user_id, None, embedding, 5, filter).await?;

            for (peer, score) in similar {
                if peer.id != unit.id && score > self.auto_link_similarity_threshold {  // 使用配置值
                    let edge = GraphEdge::new(unit.user_id.clone(), unit.id, peer.id, RelationType::RelatedTo, score);
                    self.graph.add_edge(&edge).await?;

                    // Set community marker since graph changed
                    self.set_needs_community(&unit.user_id)?;
                }
            }
        }
        Ok(())
    }

    async fn semantic_link_memory(&self, unit: &MemoryUnit) -> Result<()> {
        let context = self.fetch_recent_l1_units(&unit.user_id, 5).await?;

        let context: Vec<MemoryUnit> = context.into_iter()
            .filter(|u| u.id != unit.id)
            .collect();

        if context.is_empty() { return Ok(()); }

        let edges = self.arbitrator.analyze_relations(unit, &context).await?;

        if !edges.is_empty() {
             for edge in edges {
                 self.graph.add_edge(&edge).await?;
             }
        }
        Ok(())
    }

    // ── Search ──────────────────────────────────────────────────────

    pub async fn search_similar(&self, user_id: &str, _app_id: Option<&str>, vector: &[f32], limit: usize, filter: Option<String>) -> Result<Vec<(MemoryUnit, f32)>> {
        let results = match self.vector.search("memories", vector, limit, filter).await {
            Ok(res) => res,
            Err(_) => return Ok(Vec::new()),
        };
        self.fetch_units_with_scores(user_id, results).await
    }

    /// Perform a BFS graph traversal to expand context from seed memories.
    async fn expand_subgraph(&self, user_id: &str, seeds: Vec<(MemoryUnit, f32)>, depth: usize) -> Result<Vec<(MemoryUnit, f32)>> {
        if depth == 0 || seeds.is_empty() {
            return Ok(seeds);
        }

        let mut results: HashMap<String, (MemoryUnit, f32)> = seeds.iter()
            .map(|(u, s)| (u.id.to_string(), (u.clone(), *s)))
            .collect();

        let mut frontier: Vec<String> = seeds.iter().map(|(u, _)| u.id.to_string()).collect();
        let mut visited: HashSet<String> = frontier.iter().cloned().collect();

        for _d in 0..depth {
            if frontier.is_empty() { break; }

            // Guard against unbounded expansion
            if results.len() > 500 {
                tracing::warn!("Graph expansion hit limit of 500 nodes, stopping early.");
                break;
            }

            if frontier.len() > 10 {
                frontier.truncate(10);
            }

            let mut next_frontier = HashSet::new();
            
            // 优化：使用 BatchExecutor 批量查询
            let node_ids: Vec<Uuid> = frontier.iter()
                .filter_map(|id_str| Uuid::parse_str(id_str).ok())
                .collect();

            if node_ids.is_empty() {
                break;
            }

            // 批量查询出边和入边
            let (out_map_res, in_map_res) = tokio::join!(
                self.batch_executor.batch_get_outgoing_edges(user_id, &node_ids),
                self.batch_executor.batch_get_incoming_edges(user_id, &node_ids)
            );

            let out_map = out_map_res?;
            let in_map = in_map_res?;

            let mut edges_to_process = Vec::new();

            for node_id in &node_ids {
                if let Some(edges) = out_map.get(node_id) {
                    edges_to_process.extend(edges.iter().cloned());
                }
                if let Some(edges) = in_map.get(node_id) {
                    edges_to_process.extend(edges.iter().cloned());
                }
            }

            let mut neighbor_ids_to_fetch = HashSet::new();

            for edge in edges_to_process {
                let is_outgoing = visited.contains(&edge.source_id.to_string());
                let neighbor_id = if is_outgoing { edge.target_id } else { edge.source_id };
                let neighbor_str = neighbor_id.to_string();

                if visited.contains(&neighbor_str) { continue; }

                let is_relevant = match edge.relation {
                    RelationType::DerivedFrom | RelationType::EvolvedTo => true,
                    RelationType::RelatedTo if edge.weight > self.auto_link_similarity_threshold => true,
                    _ => false,
                };

                if is_relevant {
                    neighbor_ids_to_fetch.insert(neighbor_str.clone());
                    next_frontier.insert(neighbor_str);
                }
            }

            let ids_list: Vec<String> = neighbor_ids_to_fetch.into_iter().collect();
            if !ids_list.is_empty() {
                let units = self.fetch_units(user_id, ids_list).await?;
                for unit in units {
                    let score = 0.8_f32.powi((_d + 1) as i32) * 0.8;

                    let unit_id_str = unit.id.to_string();
                    results.insert(unit_id_str.clone(), (unit, score));
                    visited.insert(unit_id_str);
                }
            }

            frontier = next_frontier.into_iter().collect();
        }

        Ok(results.into_values().collect())
    }

    /// Perform hybrid search combining vector similarity and full-text search using Reciprocal Rank Fusion (RRF).
    pub async fn search_procedural(&self, user_id: &str, app_id: Option<&str>, agent_id: Option<&str>, query_text: &str, vector: &[f32], limit: usize) -> Result<Vec<(MemoryUnit, f32)>> {
        let mut extra_filter = "memory_type = 'procedural'".to_string();
        if let Some(aid) = agent_id {
            extra_filter.push_str(&format!(" AND agent_id = '{}'", aid.replace('\'', "''")));
        }
        
        let vec_filter = self.build_user_filter(user_id, app_id, Some(extra_filter));
        let vector_future = self.vector.search("memories", vector, limit * 2, vec_filter);
        
        // Skip Tantivy full-text for procedural, vector is better for behavior trajectories, or we can use it
        // Let's stick to vector-only for now, to ensure tight behavioral trajectory matches.
        let vector_hits = match vector_future.await {
            Ok(hits) => hits,
            Err(e) => {
                if e.to_string().contains("not found") {
                    Vec::new()
                } else {
                    return Err(e);
                }
            }
        };

        if vector_hits.is_empty() {
            return Ok(Vec::new());
        }

        let candidates_to_fetch: Vec<String> = vector_hits.iter().map(|(id, _)| id.clone()).collect();
        let mut units: Vec<MemoryUnit> = self.fetch_units(user_id, candidates_to_fetch).await?;
        
        // Ensure strictly procedural
        units.retain(|u| u.memory_type == memorose_common::MemoryType::Procedural);

        let mut seeds = Vec::new();
        for unit in units {
            let score = vector_hits.iter()
                .find(|(id, _)| *id == unit.id.to_string())
                .map(|(_, s)| *s)
                .unwrap_or(0.0);
            seeds.push((unit, score));
        }

        // We can do chronological trajectory tracking here in the future
        // For now, rerank and return
        let final_results = self.reranker.rerank(query_text, &self._kv, seeds).await?;
        
        Ok(final_results.into_iter().take(limit).collect())
    }

    pub async fn search_hybrid(&self, user_id: &str, app_id: Option<&str>, query_text: &str, vector: &[f32], limit: usize, enable_arbitration: bool, min_score: Option<f32>, graph_depth: usize, valid_time: Option<TimeRange>, transaction_time: Option<TimeRange>) -> Result<Vec<(MemoryUnit, f32)>> {
        let time_filter = self.build_time_filter(valid_time.clone());
        let vec_filter = self.build_user_filter(user_id, app_id, time_filter);

        let vector_future = self.vector.search("memories", vector, limit * 2, vec_filter);

        let index = self.index.clone();
        let q_text = query_text.to_string();
        let vt = valid_time.clone();
        let tt = transaction_time.clone();
        let uid = Some(user_id.to_string());
        let aid = app_id.map(|s| s.to_string());
        let text_future = tokio::task::spawn_blocking(move || {
            // Ensure reader sees latest committed segments before searching
            index.reload().ok();
            index.search_bitemporal(&q_text, limit * 2, vt, tt, uid.as_deref(), aid.as_deref())
        });

        let (vector_results, text_results) = tokio::join!(vector_future, text_future);

        let vector_hits = match vector_results {
            Ok(hits) => hits,
            Err(e) => {
                let msg = e.to_string().to_lowercase();
                // "Table 'memories' not found" is expected on a fresh node with no ingested data.
                // Require both a table-related term AND "not found" to avoid swallowing real errors.
                if (msg.contains("table") || msg.contains("no such")) && msg.contains("not found") {
                    Vec::new()
                } else {
                    return Err(e);
                }
            }
        };

        let text_hits = text_results??;

        // RRF Fusion on IDs
        let k = 60.0;
        let mut rrf_scores: HashMap<String, f32> = HashMap::new();

        for (rank, (id, _sim_score)) in vector_hits.into_iter().enumerate() {
            *rrf_scores.entry(id).or_default() += 1.0 / (k + rank as f32);
        }

        for (rank, id) in text_hits.into_iter().enumerate() {
            *rrf_scores.entry(id).or_default() += 1.0 / (k + rank as f32);
        }

        // Normalize RRF scores to [0, 1] range so they are compatible with reranker weights
        let max_rrf = rrf_scores.values().cloned().fold(0.0_f32, f32::max);
        if max_rrf > 0.0 {
            for score in rrf_scores.values_mut() {
                *score /= max_rrf;
            }
        }

        let mut sorted_ids: Vec<(String, f32)> = rrf_scores.into_iter().collect();
        sorted_ids.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let candidates_to_fetch: Vec<String> = sorted_ids.iter().take(limit * 3).map(|(id, _)| id.clone()).collect();
        let units: Vec<MemoryUnit> = self.fetch_units(user_id, candidates_to_fetch).await?;

        let mut seeds = Vec::new();
        for unit in units {
            let score = sorted_ids.iter()
                .find(|(id, _)| *id == unit.id.to_string())
                .map(|(_, s)| *s)
                .unwrap_or(0.0);
            seeds.push((unit, score));
        }

        // Graph Expansion (BFS)
        let mut expanded_units = self.expand_subgraph(user_id, seeds, graph_depth).await?;

        expanded_units.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Time and Importance Reranking
        let final_results = self.reranker.rerank(query_text, &self._kv, expanded_units).await?;

        // Default threshold lowered: RRF scores are now normalized to [0,1], and the
        // reranker adds importance (0.2) + recency (0.1) components, so a reasonable
        // cutoff is ~0.3 to keep relevant results while filtering noise.
        let threshold = min_score.unwrap_or(0.3);
        let mut final_results: Vec<_> = final_results.into_iter()
            .filter(|(_, score)| *score >= threshold)
            .collect();

        if final_results.is_empty() {
            return Ok(Vec::new());
        }

        // Semantic Dedup — O(N²·D); cap input so cost stays bounded even after subgraph expansion.
        let dedup_cap = (limit * 4).max(20);
        if final_results.len() > dedup_cap {
            final_results.truncate(dedup_cap);
        }
        let mut deduped_results: Vec<(MemoryUnit, f32)> = Vec::new();
        for (unit, score) in final_results {
            let mut is_duplicate = false;
            for (existing_unit, _) in &deduped_results {
                if let (Some(v1), Some(v2)) = (&unit.embedding, &existing_unit.embedding) {
                    if cosine_similarity(v1, v2) > 0.92 {
                        is_duplicate = true;
                        break;
                    }
                }
            }
            if !is_duplicate {
                deduped_results.push((unit, score));
            }
        }
        final_results = deduped_results;

        let mut results_for_arbitration = final_results;
        if results_for_arbitration.len() > limit * 2 {
             results_for_arbitration.truncate(limit * 2);
        }

        // Heuristic Arbitration Trigger
        let mut should_arbitrate = false;
        if enable_arbitration && results_for_arbitration.len() >= 2 {
            let top1_score = results_for_arbitration[0].1;
            let top2_score = results_for_arbitration[1].1;

            if (top1_score - top2_score).abs() < 0.25 {
                should_arbitrate = true;
            } else {
                tracing::info!("Skipping arbitration due to high confidence in Top 1 (Score gap: {:.2})", (top1_score - top2_score).abs());
            }
        }

        if should_arbitrate {
            tracing::info!("Executing LLM Arbitration for {} candidates...", results_for_arbitration.len());
            let units_to_arbitrate: Vec<MemoryUnit> = results_for_arbitration.iter().map(|(u, _)| u.clone()).collect();
            let arbitrated = self.arbitrator.arbitrate(units_to_arbitrate, Some(query_text)).await?;

            let mut arbitrated_results = Vec::new();
            for unit in arbitrated {
                if let Some((_, score)) = results_for_arbitration.iter().find(|(u, _)| u.id == unit.id) {
                    arbitrated_results.push((unit, *score));
                }
            }
            Ok(arbitrated_results)
        } else {
            Ok(results_for_arbitration)
        }
    }

    pub async fn search_text(&self, user_id: &str, app_id: Option<&str>, query: &str, limit: usize, enable_arbitration: bool, time_range: Option<TimeRange>) -> Result<Vec<MemoryUnit>> {
        let index = self.index.clone();
        tokio::task::spawn_blocking(move || {
            index.reload().ok();
        }).await?;

        let index = self.index.clone();
        let q = query.to_string();
        let tr = time_range.clone();
        let uid = Some(user_id.to_string());
        let aid = app_id.map(|s| s.to_string());
        let ids = tokio::task::spawn_blocking(move || {
            index.search(&q, limit, tr, uid.as_deref(), aid.as_deref())
        }).await??;

        let units = self.fetch_units(user_id, ids).await?;

        if enable_arbitration {
            self.arbitrator.arbitrate(units, Some(query)).await
        } else {
            Ok(units)
        }
    }

    /// Search and then consolidate the results into a single narrative.
    pub async fn search_consolidated(&self, user_id: &str, app_id: Option<&str>, query: &str, limit: usize) -> Result<String> {
        let units = self.search_text(user_id, app_id, query, limit, false, None).await?;
        self.arbitrator.consolidate(units).await
    }

    // ── Memory Retrieval ────────────────────────────────────────────

    pub async fn get_memory_unit(&self, user_id: &str, id: Uuid) -> Result<Option<MemoryUnit>> {
        let key = format!("u:{}:unit:{}", user_id, id);
        let val = self._kv.get(key.as_bytes())?;
        match val {
            Some(bytes) => {
                let unit: MemoryUnit = serde_json::from_slice(&bytes)?;
                Ok(Some(unit))
            }
            None => Ok(None),
        }
    }

    /// Get a memory unit using the global index (for dashboard)
    pub async fn get_memory_unit_by_index(&self, id: Uuid) -> Result<Option<MemoryUnit>> {
        let idx_key = format!("idx:unit:{}", id);
        if let Some(uid_bytes) = self._kv.get(idx_key.as_bytes())? {
            let user_id = String::from_utf8(uid_bytes)?;
            self.get_memory_unit(&user_id, id).await
        } else {
            Ok(None)
        }
    }

    // ── Forgetting ──────────────────────────────────────────────────

    /// Apply importance decay to memories for a specific user.
    /// Updates only the KV store — does NOT re-index into LanceDB/Tantivy
    /// or trigger auto-linking/LLM calls.
    pub async fn decay_importance(&self, user_id: &str, factor: f32) -> Result<()> {
        let prefix = format!("u:{}:unit:", user_id);
        let kv = self._kv.clone();
        let prefix_bytes = prefix.into_bytes();

        let pairs = tokio::task::spawn_blocking(move || kv.scan(&prefix_bytes)).await??;

        let kv = self._kv.clone();
        tokio::task::spawn_blocking(move || {
            for (key, val) in pairs {
                if let Ok(mut unit) = serde_json::from_slice::<MemoryUnit>(&val) {
                    unit.importance *= factor;
                    if let Ok(new_val) = serde_json::to_vec(&unit) {
                        kv.put(&key, &new_val)?;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }).await??;

        Ok(())
    }

    /// Remove memories with importance below the threshold for a specific user.
    /// Deletes from KV, LanceDB vector store, and Tantivy text index.
    pub async fn prune_memories(&self, user_id: &str, threshold: f32) -> Result<usize> {
        let prefix = format!("u:{}:unit:", user_id);
        let kv = self._kv.clone();

        let prefix_bytes = prefix.into_bytes();
        let pairs = tokio::task::spawn_blocking({
            let kv = kv.clone();
            move || kv.scan(&prefix_bytes)
        }).await??;

        // Collect units to prune first, then delete from all stores.
        let mut to_prune: Vec<(Vec<u8>, MemoryUnit)> = Vec::new();
        for (key, val) in pairs {
            if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(&val) {
                if unit.importance < threshold {
                    to_prune.push((key, unit));
                }
            }
        }

        let count = to_prune.len();
        if count == 0 {
            return Ok(0);
        }

        // 1. Delete from KV + L1 secondary index
        let kv_clone = kv.clone();
        let keys_and_levels: Vec<(Vec<u8>, String, u8)> = to_prune
            .iter()
            .map(|(k, u)| (k.clone(), u.id.to_string(), u.level))
            .collect();
        let user_id_owned = user_id.to_string();
        tokio::task::spawn_blocking(move || {
            for (key, id, level) in &keys_and_levels {
                kv_clone.delete(key)?;
                if *level == 1 {
                    let l1_key = format!("l1_idx:{}:{}", user_id_owned, id);
                    kv_clone.delete(l1_key.as_bytes()).ok();
                }
            }
            Ok::<(), anyhow::Error>(())
        }).await??;

        // 2. Delete from LanceDB vector store
        for (_, unit) in &to_prune {
            if let Err(e) = self.vector.delete_by_id("memories", &unit.id.to_string()).await {
                tracing::warn!("Failed to delete unit {} from vector store during pruning: {:?}", unit.id, e);
            }
        }

        // 3. Delete from Tantivy text index
        let index = self.index.clone();
        let ids: Vec<String> = to_prune.iter().map(|(_, u)| u.id.to_string()).collect();
        tokio::task::spawn_blocking(move || {
            for id in &ids {
                index.delete_unit(id)?;
            }
            Ok::<(), anyhow::Error>(())
        }).await??;

        Ok(count)
    }

    // ── Community Detection ─────────────────────────────────────────

    /// Graph-driven L2 Generation for a specific user.
    pub async fn process_communities(&self, user_id: &str) -> Result<()> {
        self.process_communities_with_limits(user_id, 3, usize::MAX).await?;
        Ok(())
    }

    /// Graph-driven L2 generation with configurable thresholds/limits.
    /// Returns number of L2 units created in this run.
    pub async fn process_communities_with_limits(
        &self,
        user_id: &str,
        min_members: usize,
        max_groups: usize,
    ) -> Result<usize> {
        let edges = self.graph.get_all_edges_for_user(user_id).await?;

        if edges.is_empty() {
            return Ok(0);
        }

        let communities = tokio::task::spawn_blocking(move || {
            crate::community::CommunityDetector::detect_communities(&edges)
        }).await?;

        let mut community_groups: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
        for (node_id, community_id) in communities {
            community_groups.entry(community_id).or_default().push(node_id);
        }

        let min_members = min_members.max(1);
        let mut created = 0usize;

        for (_comm_id, members) in community_groups {
            if created >= max_groups {
                break;
            }

            if members.len() < min_members {
                continue;
            }

            let member_ids: Vec<String> = members.iter().map(|id| id.to_string()).collect();
            let units = self.fetch_units(user_id, member_ids.clone()).await?;

            if units.is_empty() {
                continue;
            }

            let texts: Vec<String> = units.iter().map(|u| u.content.clone()).collect();

            let insight = self.arbitrator.summarize_community(texts).await?;

            let mut l2_unit = MemoryUnit::new(
                user_id.to_string(),
                None,
                Self::derive_l2_app_id(&units),
                Uuid::new_v4(),
                memorose_common::MemoryType::Factual,
                insight.summary,
                None
            );
            l2_unit.level = 2;
            l2_unit.keywords.push(insight.name.clone());
            l2_unit.keywords.extend(insight.keywords);
            l2_unit.references = members.clone();

            if let Some(client) = self.arbitrator.get_llm_client() {
                if let Ok(emb) = client.embed(&l2_unit.content).await {
                    l2_unit.embedding = Some(emb.data);
                }
            }

            self.store_memory_unit(l2_unit.clone()).await?;

            let l2_id = l2_unit.id;
            let uid2 = user_id.to_string();
            for member_id in members {
                let edge = GraphEdge::new(uid2.clone(), l2_id, member_id, RelationType::DerivedFrom, 1.0);
                self.graph.add_edge(&edge).await?;
            }

            created += 1;
            tracing::info!("Created L2 Insight '{}' from {} members for user {}", insight.name, units.len(), user_id);
        }

        Ok(created)
    }

    /// 增强版社区检测（支持多种算法）
    ///
    /// 使用 Louvain、加权 LPA 等高级算法，并提供模块度评估
    pub async fn detect_communities_enhanced(
        &self,
        user_id: &str,
        config: crate::community::DetectionConfig,
    ) -> Result<crate::community::CommunityResult> {
        use crate::community::{BatchCommunityDetector, EnhancedCommunityDetector};

        // 获取用户的所有节点
        let edges = self.graph.get_all_edges_for_user(user_id).await?;

        if edges.is_empty() {
            return Ok(crate::community::CommunityResult {
                node_to_community: HashMap::new(),
                community_to_nodes: HashMap::new(),
                modularity: 0.0,
                num_communities: 0,
            });
        }

        // 提取所有节点
        let mut all_nodes: HashSet<Uuid> = HashSet::new();
        for edge in &edges {
            all_nodes.insert(edge.source_id);
            all_nodes.insert(edge.target_id);
        }
        let node_ids: Vec<Uuid> = all_nodes.into_iter().collect();

        tracing::info!(
            "Starting enhanced community detection for user {} with {} nodes, {} edges",
            user_id,
            node_ids.len(),
            edges.len()
        );

        // 对于大图，使用批量优化版本
        if node_ids.len() > 1000 {
            let batch_detector = BatchCommunityDetector::new(
                self.graph.clone(),
                config,
            );
            batch_detector.detect_communities_for_user(user_id, &node_ids).await
        } else {
            // 小图直接使用增强检测器
            let detector = EnhancedCommunityDetector::new(config);
            tokio::task::spawn_blocking(move || {
                detector.detect(&edges)
            }).await?
        }
    }

    /// 两阶段社区检测（先快速粗分，再精细优化）
    ///
    /// 适合超大图（> 10000 节点）
    pub async fn detect_communities_two_phase(
        &self,
        user_id: &str,
        config: crate::community::DetectionConfig,
    ) -> Result<crate::community::CommunityResult> {
        use crate::community::BatchCommunityDetector;

        let edges = self.graph.get_all_edges_for_user(user_id).await?;

        if edges.is_empty() {
            return Ok(crate::community::CommunityResult {
                node_to_community: HashMap::new(),
                community_to_nodes: HashMap::new(),
                modularity: 0.0,
                num_communities: 0,
            });
        }

        let mut all_nodes: HashSet<Uuid> = HashSet::new();
        for edge in &edges {
            all_nodes.insert(edge.source_id);
            all_nodes.insert(edge.target_id);
        }
        let node_ids: Vec<Uuid> = all_nodes.into_iter().collect();

        let batch_detector = BatchCommunityDetector::new(
            self.graph.clone(),
            config,
        );

        batch_detector.two_phase_detection(user_id, &node_ids).await
    }

    /// 处理社区并生成 L2 摘要（使用增强算法）
    pub async fn process_communities_enhanced(
        &self,
        user_id: &str,
        config: crate::community::DetectionConfig,
    ) -> Result<()> {
        let result = self.detect_communities_enhanced(user_id, config).await?;

        tracing::info!(
            "Detected {} communities with modularity {:.4} for user {}",
            result.num_communities,
            result.modularity,
            user_id
        );

        // 为每个社区生成 L2 摘要
        for (_comm_id, members) in result.community_to_nodes {
            let member_ids: Vec<String> = members.iter().map(|id| id.to_string()).collect();
            let units = self.fetch_units(user_id, member_ids.clone()).await?;

            if units.is_empty() {
                continue;
            }

            let texts: Vec<String> = units.iter().map(|u| u.content.clone()).collect();
            let insight = self.arbitrator.summarize_community(texts).await?;

            let mut l2_unit = MemoryUnit::new(
                user_id.to_string(),
                None,
                Self::derive_l2_app_id(&units),
                Uuid::new_v4(),
                memorose_common::MemoryType::Factual,
                insight.summary,
                None
            );
            l2_unit.level = 2;
            l2_unit.keywords.push(insight.name.clone());
            l2_unit.keywords.extend(insight.keywords);
            l2_unit.references = members.clone();

            if let Some(client) = self.arbitrator.get_llm_client() {
                if let Ok(emb) = client.embed(&l2_unit.content).await {
                    l2_unit.embedding = Some(emb.data);
                }
            }

            self.store_memory_unit(l2_unit.clone()).await?;

            let l2_id = l2_unit.id;
            let uid2 = user_id.to_string();
            for member_id in members {
                let edge = GraphEdge::new(uid2.clone(), l2_id, member_id, RelationType::DerivedFrom, 1.0);
                self.graph.add_edge(&edge).await?;
            }

            tracing::info!(
                "Created L2 Insight '{}' from {} members for user {}",
                insight.name,
                units.len(),
                user_id
            );
        }

        Ok(())
    }

    // ── Fetch Helpers ───────────────────────────────────────────────

    /// Fetch the latest L1 memory units for a specific user.
    /// Uses the "l1_idx:{user_id}:{id}" secondary index to avoid loading all units into memory.
    pub async fn fetch_recent_l1_units(&self, user_id: &str, limit: usize) -> Result<Vec<MemoryUnit>> {
        let prefix = format!("u:{}:unit:", user_id);
        let store = self._kv.clone();
        let prefix_bytes = prefix.into_bytes();

        // Scan the compact L1 index (values are 8-byte timestamps, much cheaper than full units).
        // The prefix is user-scoped so we only read this user's entries.
        let l1_index_prefix = format!("l1_idx:{}:", user_id).into_bytes();
        let strip_prefix = format!("l1_idx:{}:", user_id);
        let index_pairs = tokio::task::spawn_blocking({
            let store = store.clone();
            move || store.scan(&l1_index_prefix)
        }).await??;

        if index_pairs.is_empty() {
            // Fallback for nodes that pre-date the L1 index: scan full units.
            return self.fetch_recent_l1_units_fallback(prefix_bytes, limit).await;
        }

        // Sort by timestamp descending, take top `limit` IDs.
        let mut id_ts: Vec<(String, i64)> = index_pairs
            .into_iter()
            .filter_map(|(k, v)| {
                let key_str = String::from_utf8(k).ok()?;
                let id = key_str.strip_prefix(&strip_prefix)?.to_string();
                let ts = i64::from_le_bytes(v.as_slice().try_into().ok()?);
                Some((id, ts))
            })
            .collect();

        id_ts.sort_by(|a, b| b.1.cmp(&a.1));
        id_ts.truncate(limit);

        // Multi-get the actual units by their KV keys.
        let keys: Vec<String> = id_ts.iter()
            .map(|(id, _)| format!("u:{}:unit:{}", user_id, id))
            .collect();

        let values = tokio::task::spawn_blocking({
            let store = store.clone();
            let key_refs_owned: Vec<Vec<u8>> = keys.iter().map(|k| k.as_bytes().to_vec()).collect();
            move || store.multi_get(&key_refs_owned.iter().map(|k| k.as_slice()).collect::<Vec<_>>())
        }).await??;

        let results: Vec<MemoryUnit> = values.into_iter()
            .filter_map(|v| v.and_then(|bytes| serde_json::from_slice(&bytes).ok()))
            .collect();

        Ok(results)
    }

    async fn fetch_recent_l1_units_fallback(&self, prefix_bytes: Vec<u8>, limit: usize) -> Result<Vec<MemoryUnit>> {
        let store = self._kv.clone();
        let pairs = tokio::task::spawn_blocking(move || store.scan(&prefix_bytes)).await??;
        let mut results: Vec<MemoryUnit> = pairs
            .into_iter()
            .filter_map(|(_, val)| serde_json::from_slice::<MemoryUnit>(&val).ok())
            .filter(|u| u.level == 1)
            .collect();
        results.sort_by(|a, b| b.transaction_time.cmp(&a.transaction_time));
        results.truncate(limit);
        Ok(results)
    }

    /// Count the total number of L1 memory units for a specific user.
    pub async fn count_l1_units(&self, user_id: &str) -> Result<usize> {
        let prefix = format!("u:{}:unit:", user_id);
        let store = self._kv.clone();
        let prefix_bytes = prefix.into_bytes();

        // Try the L1 index first (only counts IDs, much cheaper).
        // Prefix is user-scoped so this returns only this user's L1 count.
        let l1_index_prefix = format!("l1_idx:{}:", user_id).into_bytes();
        let index_pairs = tokio::task::spawn_blocking({
            let store = store.clone();
            move || store.scan(&l1_index_prefix)
        }).await??;

        if !index_pairs.is_empty() {
            return Ok(index_pairs.len());
        }

        // Fallback: scan all units and count level-1 ones.
        let count = tokio::task::spawn_blocking(move || {
            let pairs = store.scan(&prefix_bytes)?;
            let count = pairs.into_iter()
                .filter_map(|(_, val)| serde_json::from_slice::<MemoryUnit>(&val).ok())
                .filter(|u| u.level == 1)
                .count();
            Ok::<usize, anyhow::Error>(count)
        }).await??;

        Ok(count)
    }

    /// Track cumulative L1 growth and return the count range crossed by this update.
    pub async fn bump_l1_count_and_get_range(&self, user_id: &str, delta: usize) -> Result<(usize, usize)> {
        if delta == 0 {
            let current = self.current_l1_count(user_id).await?;
            return Ok((current, current));
        }

        let key = format!("l1_count:{}", user_id);
        if let Some(bytes) = self.system_kv().get(key.as_bytes())? {
            let current = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])) as usize;
            let updated = current.saturating_add(delta);
            self.system_kv().put(key.as_bytes(), &(updated as u64).to_le_bytes())?;
            return Ok((current, updated));
        }

        // Lazy init from real storage count so legacy data doesn't lose trigger state.
        let current_after_store = self.count_l1_units(user_id).await?;
        let current_before_store = current_after_store.saturating_sub(delta);
        self.system_kv()
            .put(key.as_bytes(), &(current_after_store as u64).to_le_bytes())?;
        Ok((current_before_store, current_after_store))
    }

    async fn current_l1_count(&self, user_id: &str) -> Result<usize> {
        let key = format!("l1_count:{}", user_id);
        if let Some(bytes) = self.system_kv().get(key.as_bytes())? {
            return Ok(u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])) as usize);
        }

        let current = self.count_l1_units(user_id).await?;
        self.system_kv()
            .put(key.as_bytes(), &(current as u64).to_le_bytes())?;
        Ok(current)
    }

    pub async fn fetch_units_with_scores(&self, user_id: &str, results: Vec<(String, f32)>) -> Result<Vec<(MemoryUnit, f32)>> {
        if results.is_empty() { return Ok(Vec::new()); }

        let keys: Vec<String> = results.iter().map(|(id, _)| format!("u:{}:unit:{}", user_id, id)).collect();
        let store = self._kv.clone();

        let db_results = tokio::task::spawn_blocking(move || {
            let key_bytes: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
            store.multi_get(&key_bytes)
        }).await??;

        let mut final_results = Vec::new();
        for (i, res) in db_results.into_iter().enumerate() {
            if let Some(bytes) = res {
                if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(&bytes) {
                    final_results.push((unit, results[i].1));
                }
            }
        }
        Ok(final_results)
    }

    pub async fn fetch_units(&self, user_id: &str, ids: Vec<String>) -> Result<Vec<MemoryUnit>> {
        if ids.is_empty() { return Ok(Vec::new()); }

        let keys: Vec<String> = ids.iter().map(|id| format!("u:{}:unit:{}", user_id, id)).collect();
        let store = self._kv.clone();

        let results = tokio::task::spawn_blocking(move || {
            let key_bytes: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
            store.multi_get(&key_bytes)
        }).await??;

        let mut units = Vec::new();
        for res in results {
            if let Some(bytes) = res {
                if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(&bytes) {
                    units.push(unit);
                }
            }
        }
        Ok(units)
    }

    // ── 图查询优化 API ──────────────────────────────────────────────────

    /// 批量查询多个节点的邻居（使用批量优化）
    pub async fn batch_get_neighbors(
        &self,
        user_id: &str,
        node_ids: &[Uuid],
    ) -> Result<HashMap<Uuid, Vec<GraphEdge>>> {
        self.batch_executor.batch_get_outgoing_edges(user_id, node_ids).await
    }

    /// 带缓存的邻居查询（用于热点查询）
    pub async fn get_neighbors_cached(
        &self,
        user_id: &str,
        node_id: Uuid,
    ) -> Result<Vec<GraphEdge>> {
        use crate::graph::CacheKey;

        let cache_key = CacheKey::OneHopNeighbors {
            user_id: user_id.to_string(),
            node_id,
            direction: crate::graph::cache::Direction::Outgoing,
        };

        // 尝试从缓存获取
        if let Some(cached) = self.query_cache.get_edges(&cache_key).await {
            return Ok(cached);
        }

        // 缓存未命中，查询数据库
        let edges = self.graph.get_outgoing_edges(user_id, node_id).await?;

        // 写入缓存
        self.query_cache.put_edges(cache_key, edges.clone()).await;

        Ok(edges)
    }

    /// 多跳遍历（使用批量优化）
    pub async fn multi_hop_traverse(
        &self,
        user_id: &str,
        start_nodes: Vec<Uuid>,
        max_hops: usize,
        min_weight: Option<f32>,
    ) -> Result<Vec<Uuid>> {
        self.batch_executor.batch_multi_hop_traverse(
            user_id,
            start_nodes,
            max_hops,
            min_weight,
        ).await
    }

    /// 失效用户的查询缓存（在写入边时调用）
    pub async fn invalidate_query_cache(&self, user_id: &str) {
        self.query_cache.invalidate_user(user_id).await;
    }

    /// 获取缓存统计信息
    pub async fn query_cache_stats(&self) -> crate::graph::cache::CacheStats {
        self.query_cache.stats().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use uuid::Uuid;
    use memorose_common::EventContent;
    use chrono::Utc;

    const TEST_USER: &str = "test_user";
    const TEST_APP: &str = "test_app";

    #[tokio::test]
    async fn test_engine_integration() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        // 1. Test L0 Ingestion
        let stream_id = Uuid::new_v4();
        let event = Event::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, EventContent::Text("L0 Test".to_string()));
        engine.ingest_event(event.clone()).await?;

        let pending = engine.fetch_pending_events().await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, event.id);

        let retrieved_event = engine.get_event(TEST_USER, &event.id.to_string()).await?;
        assert!(retrieved_event.is_some());
        assert_eq!(retrieved_event.unwrap().id, event.id);

        // Mark processed
        engine.mark_event_processed(&event.id.to_string()).await?;
        let pending_after = engine.fetch_pending_events().await?;
        assert!(pending_after.is_empty());

        // 2. Test L1 Storage & Retrieval
        let mut embedding = vec![0.0; 384];
        embedding[10] = 1.0;
        let unit = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "L1 Insight".to_string(), Some(embedding.clone()));

        engine.store_memory_unit(unit.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        // Search by Vector
        let filter = engine.build_user_filter(TEST_USER, None, None);
        let similar = engine.search_similar(TEST_USER, None, &embedding, 1, filter).await?;
        assert_eq!(similar.len(), 1);
        assert_eq!(similar[0].0.id, unit.id);

        // Search by Text
        let text_hits = engine.search_text(TEST_USER, None, "Insight", 1, true, None).await?;
        assert_eq!(text_hits.len(), 1);
        assert_eq!(text_hits[0].id, unit.id);

        // 3. Test Forgetting Mechanism
        let mut weak_unit = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "Weak Memory".to_string(), None);
        weak_unit.importance = 0.15;
        engine.store_memory_unit(weak_unit.clone()).await?;

        // Decay: 0.15 * 0.5 = 0.075
        engine.decay_importance(TEST_USER, 0.5).await?;

        // Prune memories below 0.1
        let pruned_count = engine.prune_memories(TEST_USER, 0.1).await?;
        assert!(pruned_count >= 1);

        // Verify it's gone
        let search_gone = engine.search_text(TEST_USER, None, "Weak", 1, true, None).await?;
        assert!(search_gone.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_auto_linking() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        // 1. Store first memory
        let mut emb1 = vec![0.0; 384];
        emb1[0] = 1.0;
        let unit1 = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "Apple is a fruit".to_string(), Some(emb1));
        engine.store_memory_unit(unit1.clone()).await?;

        // 2. Store second similar memory
        let mut emb2 = vec![0.0; 384];
        emb2[0] = 0.99;
        let unit2 = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "Apples are sweet".to_string(), Some(emb2));
        engine.store_memory_unit(unit2.clone()).await?;

        // Verify graph edge exists from unit2 to unit1
        let edges = engine.graph().get_outgoing_edges(TEST_USER, unit2.id).await?;
        assert!(!edges.is_empty(), "Edge should be automatically created");
        assert_eq!(edges[0].target_id, unit1.id);

        Ok(())
    }

    #[tokio::test]
    async fn test_conflict_arbitration() -> Result<()> {
        if std::env::var("GOOGLE_API_KEY").is_err() && std::env::var("OPENAI_API_KEY").is_err() {
            return Ok(());
        }

        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut emb1 = vec![0.0; 384]; emb1[0] = 1.0;
        let mut unit1 = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "I love cats".to_string(), Some(emb1.clone()));
        unit1.transaction_time = Utc::now() - chrono::Duration::days(1);
        engine.store_memory_unit(unit1.clone()).await?;

        let mut emb2 = vec![0.0; 384]; emb2[0] = 0.95;
        let unit2 = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "I hate cats now".to_string(), Some(emb2.clone()));
        engine.store_memory_unit(unit2.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let results = engine.search_text(TEST_USER, None, "cats", 10, true, None).await?;

        println!("Arbitration results: {:?}", results.iter().map(|u| &u.content).collect::<Vec<_>>());

        Ok(())
    }

    #[tokio::test]
    async fn test_community_flow() -> Result<()> {
        let has_google = std::env::var("GOOGLE_API_KEY").map(|s| !s.is_empty()).unwrap_or(false);
        let has_openai = std::env::var("OPENAI_API_KEY").map(|s| !s.is_empty()).unwrap_or(false);
        if !has_google && !has_openai {
            return Ok(());
        }

        let temp_dir = tempdir()?;
        let engine = match MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await {
            Ok(e) => e,
            Err(_) => return Ok(()), // skip if backend fails to initialize
        };
        let stream_id = Uuid::new_v4();

        let mut emb1 = vec![0.0; 768]; emb1[0] = 1.0;
        let u1 = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "Rust is memory safe".to_string(), Some(emb1.clone()));
        engine.store_memory_unit(u1.clone()).await?;

        let mut emb2 = vec![0.0; 768]; emb2[0] = 0.95;
        let u2 = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "The borrow checker prevents data races".to_string(), Some(emb2.clone()));
        engine.store_memory_unit(u2.clone()).await?;

        let mut emb3 = vec![0.0; 768]; emb3[0] = 0.90;
        let u3 = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "Ownership is key to Rust".to_string(), Some(emb3.clone()));
        engine.store_memory_unit(u3.clone()).await?;

        let _ = engine.process_communities(TEST_USER).await;

        let prefix = format!("u:{}:unit:", TEST_USER);
        let kv = engine._kv.clone();
        let prefix_bytes = prefix.into_bytes();
        let all_units: Vec<(Vec<u8>, Vec<u8>)> = tokio::task::spawn_blocking(move || {
            kv.scan(&prefix_bytes)
        }).await??;

        let l2_units: Vec<MemoryUnit> = all_units.into_iter()
            .filter_map(|(_, v): (Vec<u8>, Vec<u8>)| serde_json::from_slice::<MemoryUnit>(&v).ok())
            .filter(|u| u.level == 2)
            .collect();

        if !l2_units.is_empty() {
            let l2 = &l2_units[0];
            println!("Generated L2: {} - {}", l2.keywords.first().unwrap_or(&"No Name".to_string()), l2.content);

            assert!(l2.references.len() >= 3);
            assert!(!l2.keywords.is_empty(), "L2 unit should have keywords (at least title)");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_feedback_loop() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let u1 = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "Memory A".into(), None);
        let u2 = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "Memory B".into(), None);
        engine.store_memory_unit(u1.clone()).await?;
        engine.store_memory_unit(u2.clone()).await?;

        engine.apply_reranker_feedback(TEST_USER, vec![u1.id.to_string(), u2.id.to_string()], vec![]).await?;

        let edges = engine.graph().get_outgoing_edges(TEST_USER, u1.id).await?;
        let edge = edges.iter().find(|e| e.target_id == u2.id).expect("Edge should be created by reinforcement");
        assert!((edge.weight - 0.1).abs() < 0.001);

        engine.apply_reranker_feedback(TEST_USER, vec![u1.id.to_string(), u2.id.to_string()], vec![]).await?;
        let edges_updated = engine.graph().get_outgoing_edges(TEST_USER, u1.id).await?;
        let edge_updated = edges_updated.iter().find(|e| e.target_id == u2.id).unwrap();
        assert!((edge_updated.weight - 0.2).abs() < 0.001);

        Ok(())
    }

    #[tokio::test]
    async fn test_temporal_text_search() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut u1 = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "Memorose started in 2020".into(), None);
        u1.valid_time = Some(chrono::TimeZone::with_ymd_and_hms(&Utc, 2020, 1, 1, 0, 0, 0).unwrap());
        engine.store_memory_unit(u1.clone()).await?;

        let mut u2 = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "Memorose is advanced in 2026".into(), None);
        u2.valid_time = Some(chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 1, 1, 0, 0, 0).unwrap());
        engine.store_memory_unit(u2.clone()).await?;

        engine.index.commit()?;
        engine.index.reload()?;

        let range = TimeRange {
            start: Some(chrono::TimeZone::with_ymd_and_hms(&Utc, 2025, 1, 1, 0, 0, 0).unwrap()),
            end: Some(chrono::TimeZone::with_ymd_and_hms(&Utc, 2027, 1, 1, 0, 0, 0).unwrap()),
        };

        let hits = engine.search_text(TEST_USER, None, "Memorose", 10, false, Some(range)).await?;

        assert_eq!(hits.len(), 1, "Should only return 1 hit due to time filtering");
        assert_eq!(hits[0].id, u2.id);

        Ok(())
    }

    #[tokio::test]
    async fn test_search_filters() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut u1 = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "Highly relevant".into(), Some(vec![1.0; 768]));
        u1.importance = 1.0;
        let mut u2 = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "Less relevant".into(), Some(vec![0.5; 768]));
        u2.importance = 0.5;

        engine.store_memory_units(vec![u1.clone(), u2.clone()]).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let results = engine.search_hybrid(
            TEST_USER,
            None,
            "relevant",
            &vec![1.0; 768],
            10,
            false,
            Some(0.3),
            0,
            None,
            None
        ).await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.id, u1.id);

        Ok(())
    }

    struct MockReranker;
    #[async_trait::async_trait]
    impl crate::reranker::Reranker for MockReranker {
        async fn rerank(&self, _query: &str, _store: &KvStore, _candidates: Vec<(MemoryUnit, f32)>) -> Result<Vec<(MemoryUnit, f32)>> {
            Ok(vec![])
        }
        async fn apply_feedback(&self, _store: &KvStore, _c: Vec<String>, _r: Vec<String>) -> Result<()> { Ok(()) }
    }

    #[tokio::test]
    async fn test_custom_reranker() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?
            .with_reranker(std::sync::Arc::new(MockReranker));

        let u1 = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), Uuid::new_v4(), memorose_common::MemoryType::Factual, "Test".into(), Some(vec![1.0; 768]));
        engine.store_memory_unit(u1).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let results = engine.search_hybrid(TEST_USER, None, "Test", &vec![1.0; 768], 10, false, None, 0, None, None).await?;
        assert!(results.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrency_progress_update() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        // 1. Create parent L2
        let mut parent = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, "Parent Task".into(), None);
        parent.level = 2;
        parent.task_metadata = Some(memorose_common::TaskMetadata {
            status: memorose_common::TaskStatus::Active,
            progress: 0.0,
        });
        let parent_id = parent.id;
        engine.store_memory_unit(parent).await?;

        // 2. Create 10 children L1s and link them
        for i in 0..10 {
            let mut child = MemoryUnit::new(TEST_USER.into(), None, TEST_APP.into(), stream_id, memorose_common::MemoryType::Factual, format!("Child {}", i), None);
            child.level = 1;
            child.task_metadata = Some(memorose_common::TaskMetadata {
                status: memorose_common::TaskStatus::Completed,
                progress: 1.0,
            });
            child.references.push(parent_id);
            engine.store_memory_unit(child).await?;
        }

        // 3. Simulate concurrent updates using the worker logic
        use crate::worker::BackgroundWorker;
        let worker = std::sync::Arc::new(BackgroundWorker::new(engine.clone()));
        let mut handles = Vec::new();

        for _ in 0..20 {
            let worker_clone = worker.clone();
            let pid = parent_id;
            handles.push(tokio::spawn(async move {
                worker_clone.update_parent_progress(TEST_USER, pid).await
            }));
        }

        for h in handles {
            h.await.unwrap().expect("Concurrent update failed");
        }

        // 4. Verify final progress
        let updated_parent = engine.get_memory_unit(TEST_USER, parent_id).await?.unwrap();
        let meta = updated_parent.task_metadata.unwrap();

        assert!((meta.progress - 1.0).abs() < 0.001);
        assert_eq!(meta.status, memorose_common::TaskStatus::Completed);

        Ok(())
    }

    #[tokio::test]
    async fn test_user_isolation() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        // Store memory for user A
        let unit_a = MemoryUnit::new("user_a".into(), None, "app1".into(), stream_id, memorose_common::MemoryType::Factual, "Secret of user A".into(), None);
        engine.store_memory_unit(unit_a.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        // Store memory for user B
        let unit_b = MemoryUnit::new("user_b".into(), None, "app1".into(), stream_id, memorose_common::MemoryType::Factual, "Secret of user B".into(), None);
        engine.store_memory_unit(unit_b.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        // User A should only see their own data
        let results_a = engine.search_text("user_a", None, "Secret", 10, false, None).await?;
        assert_eq!(results_a.len(), 1);
        assert_eq!(results_a[0].user_id, "user_a");

        // User B should only see their own data
        let results_b = engine.search_text("user_b", None, "Secret", 10, false, None).await?;
        assert_eq!(results_b.len(), 1);
        assert_eq!(results_b[0].user_id, "user_b");

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_event_failed_clears_retry_state() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let event = Event::new(
            TEST_USER.into(),
            None,
            TEST_APP.into(),
            Uuid::new_v4(),
            EventContent::Text("retry me".into()),
        );
        let event_id = event.id.to_string();
        engine.ingest_event_directly(event).await?;

        assert_eq!(engine.increment_retry_count_if_pending(&event_id).await?, Some(1));
        assert_eq!(engine.get_retry_count(&event_id).await?, 1);

        engine.mark_event_failed(&event_id, "simulated failure").await?;

        assert_eq!(engine.get_retry_count(&event_id).await?, 0);
        assert_eq!(engine.increment_retry_count_if_pending(&event_id).await?, None);
        assert!(engine.fetch_pending_events().await?.is_empty());
        let failed_key = format!("failed:{}", event_id);
        assert!(engine.system_kv().get(failed_key.as_bytes())?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_bump_l1_count_tracks_threshold_crossing() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        for i in 0..4 {
            let unit = MemoryUnit::new(
                TEST_USER.into(),
                None,
                TEST_APP.into(),
                stream_id,
                memorose_common::MemoryType::Factual,
                format!("base {}", i),
                None,
            );
            engine.store_memory_unit(unit).await?;
        }

        for i in 0..2 {
            let unit = MemoryUnit::new(
                TEST_USER.into(),
                None,
                TEST_APP.into(),
                stream_id,
                memorose_common::MemoryType::Factual,
                format!("delta {}", i),
                None,
            );
            engine.store_memory_unit(unit).await?;
        }

        let (before, after) = engine.bump_l1_count_and_get_range(TEST_USER, 2).await?;
        assert_eq!((before, after), (4, 6));
        assert!(before / 5 < after / 5);

        let unit = MemoryUnit::new(
            TEST_USER.into(),
            None,
            TEST_APP.into(),
            stream_id,
            memorose_common::MemoryType::Factual,
            "delta 2".into(),
            None,
        );
        engine.store_memory_unit(unit).await?;
        let (before, after) = engine.bump_l1_count_and_get_range(TEST_USER, 1).await?;
        assert_eq!((before, after), (6, 7));
        assert!(!(before / 5 < after / 5));

        Ok(())
    }
}
