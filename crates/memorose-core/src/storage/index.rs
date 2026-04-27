use anyhow::Result;
use memorose_common::{MemoryUnit, TimeRange};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tantivy::schema::*;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy};

#[derive(Debug, Clone)]
pub struct TextIndexConfig {
    pub commit_min_interval_ms: u64,
    pub commit_max_interval_ms: u64,
    pub commit_docs_threshold: usize,
    pub commit_bytes_threshold: u64,
    pub recent_overlay_enabled: bool,
    pub recent_overlay_ttl_secs: u64,
    pub recent_overlay_per_user_max_docs: usize,
    pub recent_overlay_per_user_max_bytes: usize,
    pub recent_overlay_global_max_bytes: usize,
    pub recent_overlay_query_limit: usize,
}

impl TextIndexConfig {
    pub fn legacy(interval_ms: u64) -> Self {
        let interval_ms = interval_ms.max(1);
        Self {
            commit_min_interval_ms: interval_ms,
            commit_max_interval_ms: interval_ms,
            commit_docs_threshold: usize::MAX,
            commit_bytes_threshold: u64::MAX,
            recent_overlay_enabled: true,
            recent_overlay_ttl_secs: 120,
            recent_overlay_per_user_max_docs: 1000,
            recent_overlay_per_user_max_bytes: 8_388_608,
            recent_overlay_global_max_bytes: 134_217_728,
            recent_overlay_query_limit: 200,
        }
    }

    pub fn from_storage_config(storage: &memorose_common::config::StorageConfig) -> Self {
        let legacy_interval = storage.index_commit_interval_ms.max(1);
        let commit_min_interval_ms = storage.index_commit_min_interval_ms.max(1);
        let commit_max_interval_ms = storage
            .index_commit_max_interval_ms
            .max(commit_min_interval_ms)
            .max(legacy_interval);
        Self {
            commit_min_interval_ms,
            commit_max_interval_ms,
            commit_docs_threshold: storage.index_commit_docs_threshold.max(1),
            commit_bytes_threshold: storage.index_commit_bytes_threshold.max(1),
            recent_overlay_enabled: storage.recent_overlay_enabled,
            recent_overlay_ttl_secs: storage.recent_overlay_ttl_secs.max(1),
            recent_overlay_per_user_max_docs: storage.recent_overlay_per_user_max_docs.max(1),
            recent_overlay_per_user_max_bytes: storage.recent_overlay_per_user_max_bytes.max(1),
            recent_overlay_global_max_bytes: storage.recent_overlay_global_max_bytes.max(1),
            recent_overlay_query_limit: storage.recent_overlay_query_limit.max(1),
        }
    }

    fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.commit_min_interval_ms.min(1000).max(1))
    }

    fn commit_min_interval(&self) -> Duration {
        Duration::from_millis(self.commit_min_interval_ms.max(1))
    }

    fn commit_max_interval(&self) -> Duration {
        Duration::from_millis(
            self.commit_max_interval_ms
                .max(self.commit_min_interval_ms)
                .max(1),
        )
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct TextIndexMetricSnapshot {
    pub dirty_docs: usize,
    pub dirty_bytes: u64,
    pub commit_seq: u64,
    pub commit_total: usize,
    pub commit_skipped_busy_total: usize,
    pub overlay_docs: usize,
    pub overlay_bytes: usize,
    pub overlay_evicted_total: usize,
    pub overlay_hit_total: usize,
    pub overlay_miss_total: usize,
    pub overlay_merge_total: usize,
    pub commit_latency_total_ms: u64,
}

impl TextIndexMetricSnapshot {
    pub fn merge(&mut self, other: &Self) {
        self.dirty_docs += other.dirty_docs;
        self.dirty_bytes += other.dirty_bytes;
        self.commit_seq = self.commit_seq.max(other.commit_seq);
        self.commit_total += other.commit_total;
        self.commit_skipped_busy_total += other.commit_skipped_busy_total;
        self.overlay_docs += other.overlay_docs;
        self.overlay_bytes += other.overlay_bytes;
        self.overlay_evicted_total += other.overlay_evicted_total;
        self.overlay_hit_total += other.overlay_hit_total;
        self.overlay_miss_total += other.overlay_miss_total;
        self.overlay_merge_total += other.overlay_merge_total;
        self.commit_latency_total_ms += other.commit_latency_total_ms;
    }
}

#[derive(Default)]
struct TextIndexRuntimeMetrics {
    commit_total: AtomicUsize,
    commit_skipped_busy_total: AtomicUsize,
    overlay_evicted_total: AtomicUsize,
    overlay_hit_total: AtomicUsize,
    overlay_miss_total: AtomicUsize,
    overlay_merge_total: AtomicUsize,
    commit_latency_total_ms: AtomicU64,
}

struct PendingCommitState {
    dirty_docs: usize,
    dirty_bytes: u64,
    last_commit_at: Instant,
    current_commit_seq: u64,
}

impl PendingCommitState {
    fn new() -> Self {
        Self {
            dirty_docs: 0,
            dirty_bytes: 0,
            last_commit_at: Instant::now(),
            current_commit_seq: 0,
        }
    }
}

#[derive(Clone)]
struct RecentDoc {
    id: String,
    org_id: Option<String>,
    user_id: String,
    agent_id: Option<String>,
    domain: String,
    content: String,
    transaction_time_micros: i64,
    valid_time_micros: Option<i64>,
    inserted_at: Instant,
    estimated_bytes: usize,
    commit_seq: u64,
}

#[derive(Default)]
struct UserRecentBuffer {
    docs: VecDeque<RecentDoc>,
    total_bytes: usize,
}

struct RecentOverlay {
    enabled: bool,
    ttl: Duration,
    per_user_max_docs: usize,
    per_user_max_bytes: usize,
    global_max_bytes: usize,
    query_limit: usize,
    users: HashMap<String, UserRecentBuffer>,
    total_bytes: usize,
}

impl RecentOverlay {
    fn new(config: &TextIndexConfig) -> Self {
        Self {
            enabled: config.recent_overlay_enabled,
            ttl: Duration::from_secs(config.recent_overlay_ttl_secs.max(1)),
            per_user_max_docs: config.recent_overlay_per_user_max_docs.max(1),
            per_user_max_bytes: config.recent_overlay_per_user_max_bytes.max(1),
            global_max_bytes: config.recent_overlay_global_max_bytes.max(1),
            query_limit: config.recent_overlay_query_limit.max(1),
            users: HashMap::new(),
            total_bytes: 0,
        }
    }

    fn insert(&mut self, doc: RecentDoc) -> usize {
        if !self.enabled {
            return 0;
        }

        self.prune_expired();
        self.remove_id(&doc.id);

        let user_buffer = self.users.entry(doc.user_id.clone()).or_default();
        user_buffer.total_bytes += doc.estimated_bytes;
        user_buffer.docs.push_back(doc.clone());
        self.total_bytes += doc.estimated_bytes;

        let mut evicted = self.enforce_user_limits(&doc.user_id);
        evicted += self.enforce_global_limit();
        evicted
    }

    fn remove_id(&mut self, id: &str) {
        if !self.enabled {
            return;
        }

        let mut empty_users = Vec::new();
        for (user_id, buffer) in self.users.iter_mut() {
            let removed_bytes: usize = buffer
                .docs
                .iter()
                .filter(|doc| doc.id == id)
                .map(|doc| doc.estimated_bytes)
                .sum();
            if removed_bytes == 0 {
                continue;
            }
            buffer.docs.retain(|doc| doc.id != id);
            buffer.total_bytes = buffer.total_bytes.saturating_sub(removed_bytes);
            self.total_bytes = self.total_bytes.saturating_sub(removed_bytes);
            if buffer.docs.is_empty() {
                empty_users.push(user_id.clone());
            }
        }
        for user_id in empty_users {
            self.users.remove(&user_id);
        }
    }

    fn prune_expired(&mut self) -> usize {
        if !self.enabled {
            return 0;
        }

        let now = Instant::now();
        let ttl = self.ttl;
        self.retain_docs(|doc| now.duration_since(doc.inserted_at) <= ttl)
    }

    fn cleanup_committed(&mut self, committed_seq: u64) -> usize {
        if !self.enabled {
            return 0;
        }

        self.retain_docs(|doc| doc.commit_seq > committed_seq)
    }

    fn search(
        &mut self,
        query_str: &str,
        limit: usize,
        valid_time: Option<TimeRange>,
        transaction_time: Option<TimeRange>,
        org_id: Option<&str>,
        user_id: Option<&str>,
        agent_id: Option<&str>,
        domain: Option<&str>,
    ) -> Vec<String> {
        if !self.enabled {
            return Vec::new();
        }

        self.prune_expired();

        let terms = tokenize_query_terms(query_str);
        let scan_budget = self.query_limit.max(limit).max(1);
        let mut matches = Vec::new();

        let selected_users: Vec<String> = if let Some(user_id) = user_id {
            vec![user_id.to_string()]
        } else {
            self.users.keys().cloned().collect()
        };

        for selected_user in selected_users {
            let Some(buffer) = self.users.get(&selected_user) else {
                continue;
            };

            for doc in buffer.docs.iter().rev().take(scan_budget) {
                if !matches_filters(
                    doc,
                    org_id,
                    user_id,
                    agent_id,
                    domain,
                    valid_time.as_ref(),
                    transaction_time.as_ref(),
                ) {
                    continue;
                }

                let score = overlay_match_score(&doc.content, &terms);
                if score == 0 {
                    continue;
                }

                matches.push((score, doc.transaction_time_micros, doc.id.clone()));
            }
        }

        matches.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));

        let mut seen = HashSet::new();
        matches
            .into_iter()
            .filter_map(|(_, _, id)| {
                if seen.insert(id.clone()) {
                    Some(id)
                } else {
                    None
                }
            })
            .take(limit)
            .collect()
    }

    fn retain_docs<F>(&mut self, mut keep: F) -> usize
    where
        F: FnMut(&RecentDoc) -> bool,
    {
        let mut rebuilt = HashMap::new();
        let mut total_bytes = 0usize;
        let mut evicted = 0usize;

        for (user_id, buffer) in std::mem::take(&mut self.users) {
            let mut docs = VecDeque::new();
            let mut user_bytes = 0usize;
            for doc in buffer.docs {
                if keep(&doc) {
                    user_bytes += doc.estimated_bytes;
                    docs.push_back(doc);
                } else {
                    evicted += 1;
                }
            }
            if !docs.is_empty() {
                total_bytes += user_bytes;
                rebuilt.insert(
                    user_id,
                    UserRecentBuffer {
                        docs,
                        total_bytes: user_bytes,
                    },
                );
            }
        }

        self.users = rebuilt;
        self.total_bytes = total_bytes;
        evicted
    }

    fn enforce_user_limits(&mut self, user_id: &str) -> usize {
        let Some(buffer) = self.users.get_mut(user_id) else {
            return 0;
        };
        let mut evicted = 0usize;

        while buffer.docs.len() > self.per_user_max_docs
            || buffer.total_bytes > self.per_user_max_bytes
        {
            if let Some(doc) = buffer.docs.pop_front() {
                buffer.total_bytes = buffer.total_bytes.saturating_sub(doc.estimated_bytes);
                self.total_bytes = self.total_bytes.saturating_sub(doc.estimated_bytes);
                evicted += 1;
            } else {
                break;
            }
        }

        if buffer.docs.is_empty() {
            self.users.remove(user_id);
        }
        evicted
    }

    fn enforce_global_limit(&mut self) -> usize {
        let mut evicted = 0usize;
        while self.total_bytes > self.global_max_bytes {
            let oldest_user = self
                .users
                .iter()
                .filter_map(|(user_id, buffer)| {
                    buffer
                        .docs
                        .front()
                        .map(|doc| (user_id.clone(), doc.inserted_at))
                })
                .min_by_key(|(_, inserted_at)| *inserted_at)
                .map(|(user_id, _)| user_id);

            let Some(user_id) = oldest_user else {
                break;
            };

            let mut remove_user = false;
            if let Some(buffer) = self.users.get_mut(&user_id) {
                if let Some(doc) = buffer.docs.pop_front() {
                    buffer.total_bytes = buffer.total_bytes.saturating_sub(doc.estimated_bytes);
                    self.total_bytes = self.total_bytes.saturating_sub(doc.estimated_bytes);
                    evicted += 1;
                }
                remove_user = buffer.docs.is_empty();
            }

            if remove_user {
                self.users.remove(&user_id);
            }
        }
        evicted
    }

    fn snapshot_usage(&self) -> (usize, usize) {
        let docs = self.users.values().map(|buffer| buffer.docs.len()).sum();
        (docs, self.total_bytes)
    }
}

#[derive(Clone)]
pub struct TextIndex {
    index: Index,
    writer: Arc<Mutex<IndexWriter>>,
    reader: IndexReader,
    config: TextIndexConfig,
    commit_state: Arc<Mutex<PendingCommitState>>,
    overlay: Arc<Mutex<RecentOverlay>>,
    metrics: Arc<TextIndexRuntimeMetrics>,
    _shutdown: Arc<tokio::sync::Notify>,
    _commit_task: Arc<tokio::task::JoinHandle<()>>,
}

impl Drop for TextIndex {
    fn drop(&mut self) {
        if Arc::strong_count(&self._shutdown) == 2 {
            self._shutdown.notify_one();
        }
    }
}

impl TextIndex {
    pub fn new<P: AsRef<Path>>(path: P, interval_ms: u64) -> Result<Self> {
        Self::with_config(path, TextIndexConfig::legacy(interval_ms))
    }

    pub fn with_config<P: AsRef<Path>>(path: P, config: TextIndexConfig) -> Result<Self> {
        let index_path = path.as_ref();
        std::fs::create_dir_all(index_path)?;

        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("id", STRING | STORED);
        schema_builder.add_text_field("org_id", STRING | STORED);
        schema_builder.add_text_field("user_id", STRING | STORED);
        schema_builder.add_text_field("agent_id", STRING | STORED);
        schema_builder.add_text_field("domain", STRING | STORED);
        schema_builder.add_text_field("namespace_key", STRING | STORED);
        schema_builder.add_text_field("content", TEXT | STORED);
        schema_builder.add_text_field("stream_id", STRING);
        schema_builder.add_u64_field("level", INDEXED | STORED);
        schema_builder.add_i64_field("transaction_time", INDEXED | STORED | FAST);
        schema_builder.add_i64_field("valid_time", INDEXED | STORED | FAST);
        let schema = schema_builder.build();

        let index = match Index::open_or_create(
            tantivy::directory::MmapDirectory::open(index_path)?,
            schema.clone(),
        ) {
            Ok(idx) => idx,
            Err(e) => {
                tracing::warn!("Tantivy schema incompatible, recreating index: {}", e);
                std::fs::remove_dir_all(index_path)?;
                std::fs::create_dir_all(index_path)?;
                Index::open_or_create(
                    tantivy::directory::MmapDirectory::open(index_path)?,
                    schema.clone(),
                )?
            }
        };

        let writer = index.writer(50_000_000)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        let writer_arc = Arc::new(Mutex::new(writer));
        let reader_clone = reader.clone();
        let writer_clone = writer_arc.clone();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let shutdown_clone = shutdown.clone();
        let commit_state = Arc::new(Mutex::new(PendingCommitState::new()));
        let commit_state_clone = commit_state.clone();
        let overlay = Arc::new(Mutex::new(RecentOverlay::new(&config)));
        let overlay_clone = overlay.clone();
        let metrics = Arc::new(TextIndexRuntimeMetrics::default());
        let metrics_clone = metrics.clone();
        let commit_config = config.clone();

        let commit_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(commit_config.poll_interval());
            interval.tick().await;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Ok(mut overlay) = overlay_clone.lock() {
                            let evicted = overlay.prune_expired();
                            if evicted > 0 {
                                metrics_clone
                                    .overlay_evicted_total
                                    .fetch_add(evicted, Ordering::Relaxed);
                            }
                        }

                        let should_commit = match commit_state_clone.lock() {
                            Ok(state) => {
                                let elapsed = state.last_commit_at.elapsed();
                                state.dirty_docs > 0
                                    && elapsed >= commit_config.commit_min_interval()
                                    && (state.dirty_docs >= commit_config.commit_docs_threshold
                                        || state.dirty_bytes >= commit_config.commit_bytes_threshold
                                        || elapsed >= commit_config.commit_max_interval())
                            }
                            Err(e) => {
                                tracing::warn!("TextIndex commit state mutex was poisoned; recovering");
                                let state = e.into_inner();
                                let elapsed = state.last_commit_at.elapsed();
                                state.dirty_docs > 0
                                    && elapsed >= commit_config.commit_min_interval()
                                    && (state.dirty_docs >= commit_config.commit_docs_threshold
                                        || state.dirty_bytes >= commit_config.commit_bytes_threshold
                                        || elapsed >= commit_config.commit_max_interval())
                            }
                        };

                        if !should_commit {
                            continue;
                        }

                        let commit_result = match writer_clone.try_lock() {
                            Ok(mut writer) => {
                                let started_at = Instant::now();
                                let result = writer.commit();
                                if result.is_ok() {
                                    metrics_clone.commit_total.fetch_add(1, Ordering::Relaxed);
                                    metrics_clone.commit_latency_total_ms.fetch_add(
                                        started_at.elapsed().as_millis() as u64,
                                        Ordering::Relaxed,
                                    );
                                }
                                result
                            }
                            Err(std::sync::TryLockError::WouldBlock) => {
                                tracing::debug!("Skipping background commit (lock busy)");
                                metrics_clone
                                    .commit_skipped_busy_total
                                    .fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            Err(std::sync::TryLockError::Poisoned(e)) => {
                                tracing::warn!("TextIndex writer mutex was poisoned; recovering");
                                let mut writer = e.into_inner();
                                let started_at = Instant::now();
                                let result = writer.commit();
                                if result.is_ok() {
                                    metrics_clone.commit_total.fetch_add(1, Ordering::Relaxed);
                                    metrics_clone.commit_latency_total_ms.fetch_add(
                                        started_at.elapsed().as_millis() as u64,
                                        Ordering::Relaxed,
                                    );
                                }
                                result
                            }
                        };

                        match commit_result {
                            Ok(_) => {
                                if let Err(e) = reader_clone.reload() {
                                    tracing::error!("Background reload failed: {:?}", e);
                                }

                                let committed_seq = match commit_state_clone.lock() {
                                    Ok(mut state) => {
                                        state.dirty_docs = 0;
                                        state.dirty_bytes = 0;
                                        state.last_commit_at = Instant::now();
                                        state.current_commit_seq += 1;
                                        state.current_commit_seq
                                    }
                                    Err(e) => {
                                        tracing::warn!("TextIndex commit state mutex was poisoned after commit; recovering");
                                        let mut state = e.into_inner();
                                        state.dirty_docs = 0;
                                        state.dirty_bytes = 0;
                                        state.last_commit_at = Instant::now();
                                        state.current_commit_seq += 1;
                                        state.current_commit_seq
                                    }
                                };

                                if let Ok(mut overlay) = overlay_clone.lock() {
                                    let evicted = overlay.cleanup_committed(committed_seq);
                                    if evicted > 0 {
                                        metrics_clone
                                            .overlay_evicted_total
                                            .fetch_add(evicted, Ordering::Relaxed);
                                    }
                                }
                            }
                            Err(e) => tracing::error!("Background commit failed: {:?}", e),
                        }
                    }
                    _ = shutdown_clone.notified() => {
                        tracing::debug!("TextIndex background commit task stopping");
                        break;
                    }
                }
            }
        });

        Ok(Self {
            index,
            writer: writer_arc,
            reader,
            config,
            commit_state,
            overlay,
            metrics,
            _shutdown: shutdown,
            _commit_task: Arc::new(commit_task),
        })
    }

    pub fn index_unit(&self, unit: &MemoryUnit) -> Result<()> {
        let schema = self.index.schema();
        let id_field = schema.get_field("id").unwrap();
        let org_id_field = schema.get_field("org_id").unwrap();
        let user_id_field = schema.get_field("user_id").unwrap();
        let agent_id_field = schema.get_field("agent_id").unwrap();
        let domain_field = schema.get_field("domain").unwrap();
        let namespace_key_field = schema.get_field("namespace_key").unwrap();
        let content_field = schema.get_field("content").unwrap();
        let stream_field = schema.get_field("stream_id").unwrap();
        let level_field = schema.get_field("level").unwrap();
        let tx_time_field = schema.get_field("transaction_time").unwrap();
        let valid_time_field = schema.get_field("valid_time").unwrap();

        let mut doc = tantivy::TantivyDocument::default();
        doc.add_text(id_field, &unit.id.to_string());
        doc.add_text(org_id_field, unit.org_id.as_deref().unwrap_or(""));
        doc.add_text(user_id_field, &unit.user_id);
        doc.add_text(agent_id_field, unit.agent_id.as_deref().unwrap_or(""));
        doc.add_text(domain_field, unit.domain.as_str());
        doc.add_text(namespace_key_field, &unit.namespace_key);
        doc.add_text(content_field, &unit.content);
        doc.add_text(stream_field, &unit.stream_id.to_string());
        doc.add_u64(level_field, unit.level as u64);
        doc.add_i64(tx_time_field, unit.transaction_time.timestamp_micros());
        if let Some(vt) = unit.valid_time {
            doc.add_i64(valid_time_field, vt.timestamp_micros());
        }

        let writer = self.writer.lock().unwrap_or_else(|e| {
            tracing::warn!("TextIndex writer mutex was poisoned; recovering");
            e.into_inner()
        });
        writer.add_document(doc)?;
        drop(writer);

        let estimated_bytes = estimate_doc_bytes(unit);
        let commit_seq = {
            let mut state = self.commit_state.lock().unwrap_or_else(|e| {
                tracing::warn!("TextIndex commit state mutex was poisoned; recovering");
                e.into_inner()
            });
            state.dirty_docs += 1;
            state.dirty_bytes = state.dirty_bytes.saturating_add(estimated_bytes as u64);
            state.current_commit_seq + 1
        };

        let recent_doc = RecentDoc {
            id: unit.id.to_string(),
            org_id: unit.org_id.clone(),
            user_id: unit.user_id.clone(),
            agent_id: unit.agent_id.clone(),
            domain: unit.domain.as_str().to_string(),
            content: unit.content.clone(),
            transaction_time_micros: unit.transaction_time.timestamp_micros(),
            valid_time_micros: unit.valid_time.map(|t| t.timestamp_micros()),
            inserted_at: Instant::now(),
            estimated_bytes,
            commit_seq,
        };
        let mut overlay = self.overlay.lock().unwrap_or_else(|e| {
            tracing::warn!("TextIndex overlay mutex was poisoned; recovering");
            e.into_inner()
        });
        let evicted = overlay.insert(recent_doc);
        if evicted > 0 {
            self.metrics
                .overlay_evicted_total
                .fetch_add(evicted, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn delete_unit(&self, id: &str) -> Result<()> {
        let schema = self.index.schema();
        let id_field = schema.get_field("id").unwrap();
        let term = tantivy::Term::from_field_text(id_field, id);
        let writer = self.writer.lock().unwrap_or_else(|e| {
            tracing::warn!("TextIndex writer mutex was poisoned; recovering");
            e.into_inner()
        });
        writer.delete_term(term);
        drop(writer);

        {
            let mut state = self.commit_state.lock().unwrap_or_else(|e| {
                tracing::warn!("TextIndex commit state mutex was poisoned; recovering");
                e.into_inner()
            });
            state.dirty_docs += 1;
        }

        let mut overlay = self.overlay.lock().unwrap_or_else(|e| {
            tracing::warn!("TextIndex overlay mutex was poisoned; recovering");
            e.into_inner()
        });
        overlay.remove_id(id);
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let had_dirty = {
            let state = self.commit_state.lock().unwrap_or_else(|e| {
                tracing::warn!("TextIndex commit state mutex was poisoned; recovering");
                e.into_inner()
            });
            state.dirty_docs > 0
        };

        let mut writer = self.writer.lock().unwrap_or_else(|e| {
            tracing::warn!("TextIndex writer mutex was poisoned; recovering");
            e.into_inner()
        });
        let started_at = Instant::now();
        writer.commit()?;
        self.metrics.commit_total.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .commit_latency_total_ms
            .fetch_add(started_at.elapsed().as_millis() as u64, Ordering::Relaxed);
        drop(writer);

        if had_dirty {
            let committed_seq = {
                let mut state = self.commit_state.lock().unwrap_or_else(|e| {
                    tracing::warn!("TextIndex commit state mutex was poisoned; recovering");
                    e.into_inner()
                });
                state.dirty_docs = 0;
                state.dirty_bytes = 0;
                state.last_commit_at = Instant::now();
                state.current_commit_seq += 1;
                state.current_commit_seq
            };

            let mut overlay = self.overlay.lock().unwrap_or_else(|e| {
                tracing::warn!("TextIndex overlay mutex was poisoned; recovering");
                e.into_inner()
            });
            let evicted = overlay.cleanup_committed(committed_seq);
            if evicted > 0 {
                self.metrics
                    .overlay_evicted_total
                    .fetch_add(evicted, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    pub fn reload(&self) -> Result<()> {
        self.reader.reload()?;
        Ok(())
    }

    pub fn search(
        &self,
        query_str: &str,
        limit: usize,
        time_range: Option<TimeRange>,
        org_id: Option<&str>,
        user_id: Option<&str>,
    ) -> Result<Vec<String>> {
        self.search_bitemporal(
            query_str, limit, time_range, None, org_id, user_id, None, None,
        )
    }

    pub fn search_bitemporal(
        &self,
        query_str: &str,
        limit: usize,
        valid_time: Option<TimeRange>,
        transaction_time: Option<TimeRange>,
        org_id: Option<&str>,
        user_id: Option<&str>,
        agent_id: Option<&str>,
        domain: Option<&str>,
    ) -> Result<Vec<String>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let searcher = self.reader.searcher();
        let schema = self.index.schema();
        let content_field = schema.get_field("content").unwrap();
        let id_field = schema.get_field("id").unwrap();

        let query_parser = tantivy::query::QueryParser::for_index(&self.index, vec![content_field]);
        let base_query = match query_parser.parse_query(query_str) {
            Ok(q) => q,
            Err(_) => {
                let sanitized = sanitize_query(query_str);
                query_parser
                    .parse_query(&sanitized)
                    .unwrap_or_else(|_| Box::new(tantivy::query::AllQuery))
            }
        };

        let mut sub_queries: Vec<(tantivy::query::Occur, Box<dyn tantivy::query::Query>)> =
            vec![(tantivy::query::Occur::Must, base_query)];

        if let Some(oid) = org_id {
            let org_id_field = schema.get_field("org_id").unwrap();
            let term = tantivy::Term::from_field_text(org_id_field, oid);
            let term_query =
                tantivy::query::TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);
            sub_queries.push((tantivy::query::Occur::Must, Box::new(term_query)));
        }

        if let Some(uid) = user_id {
            let user_id_field = schema.get_field("user_id").unwrap();
            let term = tantivy::Term::from_field_text(user_id_field, uid);
            let term_query =
                tantivy::query::TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);
            sub_queries.push((tantivy::query::Occur::Must, Box::new(term_query)));
        }

        if let Some(agid) = agent_id {
            let agent_id_field = schema.get_field("agent_id").unwrap();
            let term = tantivy::Term::from_field_text(agent_id_field, agid);
            let term_query =
                tantivy::query::TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);
            sub_queries.push((tantivy::query::Occur::Must, Box::new(term_query)));
        }

        if let Some(domain) = domain {
            let domain_field = schema.get_field("domain").unwrap();
            let term = tantivy::Term::from_field_text(domain_field, domain);
            let term_query =
                tantivy::query::TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);
            sub_queries.push((tantivy::query::Occur::Must, Box::new(term_query)));
        }

        if let Some(range) = valid_time.clone() {
            if range.start.is_some() || range.end.is_some() {
                let valid_time_field = schema.get_field("valid_time").unwrap();
                let time_query = i64_range_query(valid_time_field, &range);
                sub_queries.push((tantivy::query::Occur::Must, Box::new(time_query)));
            }
        }

        if let Some(range) = transaction_time.clone() {
            if range.start.is_some() || range.end.is_some() {
                let transaction_time_field = schema.get_field("transaction_time").unwrap();
                let time_query = i64_range_query(transaction_time_field, &range);
                sub_queries.push((tantivy::query::Occur::Must, Box::new(time_query)));
            }
        }

        let combined_query = tantivy::query::BooleanQuery::new(sub_queries);
        let top_docs_collector =
            tantivy::collector::TopDocs::with_limit(limit.max(1)).order_by_score();
        let top_docs = searcher.search(&combined_query, &top_docs_collector)?;

        let mut tantivy_results = Vec::new();
        let mut seen = HashSet::new();
        for (_score, doc_address) in top_docs {
            let retrieved_doc: tantivy::TantivyDocument = searcher.doc(doc_address)?;
            if let Some(val) = retrieved_doc.get_first(id_field) {
                if let Some(s) = val.as_str() {
                    let id = s.to_string();
                    if seen.insert(id.clone()) {
                        tantivy_results.push(id);
                    }
                }
            }
        }

        let overlay_results = if self.config.recent_overlay_enabled {
            let overlay_results = {
                let mut overlay = self.overlay.lock().unwrap_or_else(|e| {
                    tracing::warn!(
                        "TextIndex overlay mutex was poisoned during search; recovering"
                    );
                    e.into_inner()
                });
                overlay.search(
                    query_str,
                    limit.max(1),
                    valid_time,
                    transaction_time,
                    org_id,
                    user_id,
                    agent_id,
                    domain,
                )
            };

            if overlay_results.is_empty() {
                self.metrics
                    .overlay_miss_total
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.metrics
                    .overlay_hit_total
                    .fetch_add(1, Ordering::Relaxed);
            }

            overlay_results
        } else {
            Vec::new()
        };

        let mut results = Vec::with_capacity(limit);
        let mut seen = HashSet::new();

        for id in overlay_results {
            if seen.insert(id.clone()) {
                self.metrics
                    .overlay_merge_total
                    .fetch_add(1, Ordering::Relaxed);
                results.push(id);
                if results.len() >= limit {
                    return Ok(results);
                }
            }
        }

        for id in tantivy_results {
            if seen.insert(id.clone()) {
                results.push(id);
                if results.len() >= limit {
                    break;
                }
            }
        }

        Ok(results)
    }

    pub fn metrics_snapshot(&self) -> TextIndexMetricSnapshot {
        let (dirty_docs, dirty_bytes, commit_seq) = {
            let state = self.commit_state.lock().unwrap_or_else(|e| {
                tracing::warn!(
                    "TextIndex commit state mutex was poisoned during snapshot; recovering"
                );
                e.into_inner()
            });
            (
                state.dirty_docs,
                state.dirty_bytes,
                state.current_commit_seq,
            )
        };
        let (overlay_docs, overlay_bytes) = {
            let overlay = self.overlay.lock().unwrap_or_else(|e| {
                tracing::warn!("TextIndex overlay mutex was poisoned during snapshot; recovering");
                e.into_inner()
            });
            overlay.snapshot_usage()
        };

        TextIndexMetricSnapshot {
            dirty_docs,
            dirty_bytes,
            commit_seq,
            commit_total: self.metrics.commit_total.load(Ordering::Relaxed),
            commit_skipped_busy_total: self
                .metrics
                .commit_skipped_busy_total
                .load(Ordering::Relaxed),
            overlay_docs,
            overlay_bytes,
            overlay_evicted_total: self.metrics.overlay_evicted_total.load(Ordering::Relaxed),
            overlay_hit_total: self.metrics.overlay_hit_total.load(Ordering::Relaxed),
            overlay_miss_total: self.metrics.overlay_miss_total.load(Ordering::Relaxed),
            overlay_merge_total: self.metrics.overlay_merge_total.load(Ordering::Relaxed),
            commit_latency_total_ms: self.metrics.commit_latency_total_ms.load(Ordering::Relaxed),
        }
    }
}

fn estimate_doc_bytes(unit: &MemoryUnit) -> usize {
    unit.content.len()
        + unit.user_id.len()
        + unit.org_id.as_deref().map_or(0, str::len)
        + unit.agent_id.as_deref().map_or(0, str::len)
        + unit.domain.as_str().len()
        + 256
}

fn sanitize_query(query_str: &str) -> String {
    query_str
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect()
}

fn tokenize_query_terms(query_str: &str) -> Vec<String> {
    sanitize_query(query_str)
        .split_whitespace()
        .map(|term| term.to_lowercase())
        .filter(|term| !term.is_empty())
        .collect()
}

fn i64_range_query(field: Field, range: &TimeRange) -> tantivy::query::RangeQuery {
    let lower = range
        .start
        .map(|time| {
            Bound::Included(tantivy::Term::from_field_i64(
                field,
                time.timestamp_micros(),
            ))
        })
        .unwrap_or(Bound::Unbounded);
    let upper = range
        .end
        .map(|time| {
            Bound::Included(tantivy::Term::from_field_i64(
                field,
                time.timestamp_micros(),
            ))
        })
        .unwrap_or(Bound::Unbounded);
    tantivy::query::RangeQuery::new(lower, upper)
}

fn overlay_match_score(content: &str, terms: &[String]) -> usize {
    if terms.is_empty() {
        return 1;
    }

    let lowered = content.to_lowercase();
    terms.iter().filter(|term| lowered.contains(*term)).count()
}

fn matches_filters(
    doc: &RecentDoc,
    org_id: Option<&str>,
    user_id: Option<&str>,
    agent_id: Option<&str>,
    domain: Option<&str>,
    valid_time: Option<&TimeRange>,
    transaction_time: Option<&TimeRange>,
) -> bool {
    if let Some(org_id) = org_id {
        if doc.org_id.as_deref() != Some(org_id) {
            return false;
        }
    }

    if let Some(user_id) = user_id {
        if doc.user_id != user_id {
            return false;
        }
    }

    if let Some(agent_id) = agent_id {
        if doc.agent_id.as_deref() != Some(agent_id) {
            return false;
        }
    }

    if let Some(domain) = domain {
        if doc.domain != domain {
            return false;
        }
    }

    if let Some(range) = valid_time {
        if !time_range_matches(doc.valid_time_micros, range) {
            return false;
        }
    }

    if let Some(range) = transaction_time {
        if !time_range_matches(Some(doc.transaction_time_micros), range) {
            return false;
        }
    }

    true
}

fn time_range_matches(value: Option<i64>, range: &TimeRange) -> bool {
    let Some(value) = value else {
        return false;
    };

    if let Some(start) = range.start {
        if value < start.timestamp_micros() {
            return false;
        }
    }

    if let Some(end) = range.end {
        if value > end.timestamp_micros() {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use memorose_common::{MemoryDomain, TimeRange};
    use tempfile::tempdir;
    use uuid::Uuid;

    #[test]
    fn test_text_index() -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let temp_dir = tempdir()?;
            let index = TextIndex::new(temp_dir.path(), 1000)?;

            let stream_id = Uuid::new_v4();
            let unit = MemoryUnit::new(
                None,
                "u1".into(),
                None,
                stream_id,
                memorose_common::MemoryType::Factual,
                "The quick brown fox jumps".to_string(),
                None,
            );

            index.index_unit(&unit)?;
            index.commit()?;
            index.reload()?;

            let results = index.search("fox", 10, None, None, None)?;
            assert!(!results.is_empty());
            assert_eq!(results[0], unit.id.to_string());
            Ok(())
        })
    }

    #[test]
    fn test_text_index_recent_overlay_is_merged_even_when_tantivy_limit_is_full() -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let temp_dir = tempdir()?;
            let index = TextIndex::new(temp_dir.path(), 60_000)?;
            let stream_id = Uuid::new_v4();

            let committed = MemoryUnit::new(
                None,
                "u1".into(),
                None,
                stream_id,
                memorose_common::MemoryType::Factual,
                "shared phrase old".to_string(),
                None,
            );
            index.index_unit(&committed)?;
            index.commit()?;
            index.reload()?;

            let mut recent = MemoryUnit::new(
                None,
                "u1".into(),
                None,
                stream_id,
                memorose_common::MemoryType::Factual,
                "shared phrase new".to_string(),
                None,
            );
            recent.transaction_time = Utc::now() + chrono::Duration::seconds(1);
            index.index_unit(&recent)?;

            let results = index.search("shared phrase", 1, None, None, Some("u1"))?;
            assert_eq!(results, vec![recent.id.to_string()]);
            Ok(())
        })
    }

    #[test]
    fn test_text_index_bitemporal_filters_delete_and_sanitize_query() -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let temp_dir = tempdir()?;
            let index = TextIndex::new(temp_dir.path(), 1000)?;
            let stream_id = Uuid::new_v4();

            let mut matching = MemoryUnit::new(
                Some("org_a".into()),
                "u1".into(),
                Some("agent_a".into()),
                stream_id,
                memorose_common::MemoryType::Factual,
                "Beijing cleanup playbook".to_string(),
                None,
            );
            matching.domain = MemoryDomain::Agent;
            matching.transaction_time = Utc.with_ymd_and_hms(2026, 4, 6, 10, 0, 0).unwrap();
            matching.valid_time = Some(Utc.with_ymd_and_hms(2026, 4, 7, 10, 0, 0).unwrap());

            let mut other = MemoryUnit::new(
                Some("org_b".into()),
                "u2".into(),
                Some("agent_b".into()),
                stream_id,
                memorose_common::MemoryType::Factual,
                "Beijing cleanup playbook".to_string(),
                None,
            );
            other.domain = MemoryDomain::User;
            other.transaction_time = Utc.with_ymd_and_hms(2026, 4, 8, 10, 0, 0).unwrap();
            other.valid_time = Some(Utc.with_ymd_and_hms(2026, 4, 9, 10, 0, 0).unwrap());

            index.index_unit(&matching)?;
            index.index_unit(&other)?;
            index.commit()?;
            index.reload()?;

            let filtered = index.search_bitemporal(
                "Beijing cleanup",
                10,
                Some(TimeRange {
                    start: Some(Utc.with_ymd_and_hms(2026, 4, 7, 0, 0, 0).unwrap()),
                    end: Some(Utc.with_ymd_and_hms(2026, 4, 7, 23, 59, 59).unwrap()),
                }),
                Some(TimeRange {
                    start: Some(Utc.with_ymd_and_hms(2026, 4, 6, 0, 0, 0).unwrap()),
                    end: Some(Utc.with_ymd_and_hms(2026, 4, 6, 23, 59, 59).unwrap()),
                }),
                Some("org_a"),
                Some("u1"),
                Some("agent_a"),
                Some("agent"),
            )?;
            assert_eq!(filtered, vec![matching.id.to_string()]);

            let sanitized = index.search("Beijing:[", 10, None, None, None)?;
            assert_eq!(sanitized.len(), 2);

            index.delete_unit(&matching.id.to_string())?;
            index.commit()?;
            index.reload()?;
            let after_delete = index.search("Beijing cleanup", 10, None, None, None)?;
            assert_eq!(after_delete, vec![other.id.to_string()]);

            Ok(())
        })
    }

    #[test]
    fn test_text_index_background_commit_makes_documents_searchable() -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let temp_dir = tempdir()?;
            let index = TextIndex::new(temp_dir.path(), 10)?;
            let unit = MemoryUnit::new(
                None,
                "u_bg".into(),
                None,
                Uuid::new_v4(),
                memorose_common::MemoryType::Factual,
                "background committed record".to_string(),
                None,
            );

            index.index_unit(&unit)?;
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

            let results = index.search("background committed", 10, None, None, None)?;
            assert!(results.contains(&unit.id.to_string()));
            Ok(())
        })
    }

    #[test]
    fn test_text_index_overlay_returns_uncommitted_documents() -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let temp_dir = tempdir()?;
            let config = TextIndexConfig {
                commit_min_interval_ms: 5_000,
                commit_max_interval_ms: 5_000,
                commit_docs_threshold: 10_000,
                commit_bytes_threshold: 1_000_000_000,
                recent_overlay_enabled: true,
                recent_overlay_ttl_secs: 120,
                recent_overlay_per_user_max_docs: 1000,
                recent_overlay_per_user_max_bytes: 8_388_608,
                recent_overlay_global_max_bytes: 134_217_728,
                recent_overlay_query_limit: 200,
            };
            let index = TextIndex::with_config(temp_dir.path(), config)?;

            let unit = MemoryUnit::new(
                Some("org_overlay".into()),
                "overlay_user".into(),
                Some("overlay_agent".into()),
                Uuid::new_v4(),
                memorose_common::MemoryType::Factual,
                "freshly inserted searchable memory".into(),
                None,
            );

            index.index_unit(&unit)?;

            let results = index.search_bitemporal(
                "freshly inserted",
                10,
                None,
                None,
                Some("org_overlay"),
                Some("overlay_user"),
                Some("overlay_agent"),
                None,
            )?;

            assert_eq!(results, vec![unit.id.to_string()]);
            Ok(())
        })
    }

    #[test]
    fn test_text_index_metrics_snapshot_tracks_overlay_and_commit() -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let temp_dir = tempdir()?;
            let config = TextIndexConfig {
                commit_min_interval_ms: 5_000,
                commit_max_interval_ms: 5_000,
                commit_docs_threshold: 10_000,
                commit_bytes_threshold: 1_000_000_000,
                recent_overlay_enabled: true,
                recent_overlay_ttl_secs: 120,
                recent_overlay_per_user_max_docs: 1000,
                recent_overlay_per_user_max_bytes: 8_388_608,
                recent_overlay_global_max_bytes: 134_217_728,
                recent_overlay_query_limit: 200,
            };
            let index = TextIndex::with_config(temp_dir.path(), config)?;

            let unit = MemoryUnit::new(
                Some("org_metrics".into()),
                "metrics_user".into(),
                Some("metrics_agent".into()),
                Uuid::new_v4(),
                memorose_common::MemoryType::Factual,
                "metrics visible before commit".into(),
                None,
            );

            index.index_unit(&unit)?;
            let before = index.metrics_snapshot();
            assert_eq!(before.dirty_docs, 1);
            assert_eq!(before.overlay_docs, 1);

            let results = index.search("metrics visible", 10, None, None, None)?;
            assert_eq!(results, vec![unit.id.to_string()]);

            let after_search = index.metrics_snapshot();
            assert_eq!(after_search.overlay_hit_total, 1);
            assert_eq!(after_search.overlay_merge_total, 1);

            index.commit()?;
            let after_commit = index.metrics_snapshot();
            assert_eq!(after_commit.dirty_docs, 0);
            assert_eq!(after_commit.commit_total, 1);
            assert_eq!(after_commit.overlay_docs, 0);
            Ok(())
        })
    }
}
