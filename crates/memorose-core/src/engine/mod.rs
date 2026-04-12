pub mod types;
pub(crate) mod helpers;
mod ingest;
mod reflection;
mod snapshot;
mod memory_crud;
mod task;
mod organization;
mod correction;
mod search;
mod forgetting;
mod community;
mod query_cache;

#[cfg(test)]
mod tests;

// Re-export public types
pub use types::{
    OrganizationAutomationCounterSnapshot, OrganizationKnowledgeContributionEntry,
    OrganizationKnowledgeContributionRecord, OrganizationKnowledgeContributionStatus,
    OrganizationKnowledgeDetailRecord, OrganizationKnowledgeMembershipEntry,
    OrganizationKnowledgeMembershipRecord, OrganizationKnowledgeRecord,
    OrganizationKnowledgeSearchHit, PendingMaterializationInput, PendingMaterializationJob,
    PendingMaterializationJobStatus, PendingMaterializationPart, PlannedMemoryCorrectionAction,
    RacDecisionEffect, RacDecisionRecord, RacMetricHistoryPoint, RacMetricSnapshot,
    RacReviewRecord, RacReviewStatus, ReflectionBatchOutcome, ReflectionMarker, SharedSearchHit,
};
pub(crate) use types::ValidatedCorrectionDecision;

use crate::arbitrator::Arbitrator;
use crate::reranker::Reranker;
use crate::storage::graph::GraphStore;
use crate::storage::index::TextIndex;
use crate::storage::kv::KvStore;
use crate::storage::system_kv::SystemKvStore;
use crate::storage::vector::VectorStore;
use anyhow::Result;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone)]
pub struct MemoroseEngine {
    pub(crate) kv_store: KvStore,
    pub(crate) vector: VectorStore,
    pub(crate) index: TextIndex,
    pub(crate) graph: GraphStore,
    pub(crate) arbitrator: Arbitrator,
    pub(crate) reranker: std::sync::Arc<dyn Reranker>,
    pub(crate) root_path: PathBuf,
    pub(crate) commit_interval_ms: u64,
    pub(crate) storage_config: memorose_common::config::StorageConfig,
    pub auto_planner: bool,
    pub task_reflection: bool,
    pub task_locks: Arc<DashMap<Uuid, Arc<Mutex<()>>>>,
    pub auto_link_similarity_threshold: f32,
    // New: Query optimization components
    pub(crate) query_cache: Arc<crate::graph::QueryCache>,
    pub(crate) batch_executor: Arc<crate::graph::BatchExecutor>,
}

impl MemoroseEngine {
    pub async fn new_with_default_threshold(
        path: impl Into<PathBuf>,
        commit_interval_ms: u64,
        auto_planner: bool,
        task_reflection: bool,
    ) -> Result<Self> {
        let app_config = memorose_common::config::AppConfig::load().ok();
        let dim = app_config
            .as_ref()
            .map(|c| c.llm.embedding_dim)
            .unwrap_or(768);
        let mut storage_config = app_config.map(|c| c.storage).unwrap_or_default();
        storage_config.index_commit_interval_ms = commit_interval_ms;
        storage_config.index_commit_min_interval_ms = commit_interval_ms.max(1);
        storage_config.index_commit_max_interval_ms = commit_interval_ms.max(1);
        Self::new_with_storage_config(
            path,
            storage_config,
            auto_planner,
            task_reflection,
            memorose_common::config::DEFAULT_AUTO_LINK_SIMILARITY_THRESHOLD,
            dim,
        )
        .await
    }

    pub async fn new(
        path: impl Into<PathBuf>,
        commit_interval_ms: u64,
        auto_planner: bool,
        task_reflection: bool,
        auto_link_similarity_threshold: f32,
        embedding_dim: i32,
    ) -> Result<Self> {
        let mut storage_config = memorose_common::config::StorageConfig::default();
        storage_config.index_commit_interval_ms = commit_interval_ms;
        storage_config.index_commit_min_interval_ms = commit_interval_ms.max(1);
        storage_config.index_commit_max_interval_ms = commit_interval_ms.max(1);
        Self::new_with_storage_config(
            path,
            storage_config,
            auto_planner,
            task_reflection,
            auto_link_similarity_threshold,
            embedding_dim,
        )
        .await
    }

    pub async fn new_with_storage_config(
        path: impl Into<PathBuf>,
        storage_config: memorose_common::config::StorageConfig,
        auto_planner: bool,
        task_reflection: bool,
        auto_link_similarity_threshold: f32,
        embedding_dim: i32,
    ) -> Result<Self> {
        use crate::storage::index::TextIndexConfig;
        use lancedb::connect;

        let app_config = memorose_common::config::AppConfig::load().ok();
        let root_path = path.into();
        std::fs::create_dir_all(&root_path)?;
        let root_path = root_path.canonicalize()?;

        let kv_path = root_path.join("rocksdb");
        let kv = tokio::task::spawn_blocking(move || KvStore::open(kv_path)).await??;

        let vector_path = root_path.join("lancedb");
        let vector_uri = vector_path.to_str().unwrap().to_string();
        let vector = VectorStore::new(&vector_uri, embedding_dim).await?;

        let db = Arc::new(connect(&vector_uri).execute().await?);
        let graph = GraphStore::new(db).await?;

        let index_path = root_path.join("tantivy");
        let index_config = TextIndexConfig::from_storage_config(&storage_config);
        let index =
            tokio::task::spawn_blocking(move || TextIndex::with_config(index_path, index_config))
                .await??;

        let arbitrator = Arbitrator::new();
        let reranker: Arc<dyn crate::reranker::Reranker> =
            if let Some(config) = app_config.as_ref() {
                if config.reranker.r#type == memorose_common::config::RerankerType::Http
                    && config.reranker.endpoint.is_some()
                {
                    Arc::new(crate::reranker::HttpReranker::new(
                        config.reranker.endpoint.clone().unwrap(),
                    ))
                } else {
                    Arc::new(crate::reranker::WeightedReranker::new())
                }
            } else {
                Arc::new(crate::reranker::WeightedReranker::new())
            };

        // Initialize query optimization components
        let query_cache = Arc::new(crate::graph::QueryCache::new(crate::graph::CacheConfig {
            ttl: std::time::Duration::from_secs(300),
            max_entries: 5000,
            enabled: true,
        }));
        let batch_executor = Arc::new(crate::graph::BatchExecutor::new(graph.clone()));

        let engine = Self {
            kv_store: kv,
            vector,
            index,
            graph,
            arbitrator,
            reranker,
            root_path,
            commit_interval_ms: storage_config.index_commit_max_interval_ms,
            storage_config,
            auto_planner,
            task_reflection,
            task_locks: Arc::new(DashMap::new()),
            auto_link_similarity_threshold,
            query_cache,
            batch_executor,
        };

        let reconciliation = engine.reconcile_organization_storage().await?;
        if reconciliation.removed_persisted_views > 0
            || reconciliation.reconciled_records > 0
            || reconciliation.removed_records > 0
            || reconciliation.removed_stale_source_relations > 0
        {
            tracing::info!(
                removed_persisted_views = reconciliation.removed_persisted_views,
                reconciled_records = reconciliation.reconciled_records,
                removed_records = reconciliation.removed_records,
                removed_stale_source_relations = reconciliation.removed_stale_source_relations,
                "Reconciled organization knowledge storage during startup"
            );
        }

        Ok(engine)
    }

    pub fn with_reranker(mut self, reranker: std::sync::Arc<dyn Reranker>) -> Self {
        self.reranker = reranker;
        self
    }

    pub fn with_arbitrator(mut self, arbitrator: Arbitrator) -> Self {
        self.arbitrator = arbitrator;
        self
    }

    pub fn kv(&self) -> KvStore {
        self.kv_store.clone()
    }

    pub fn system_kv(&self) -> SystemKvStore {
        SystemKvStore::new(self.kv_store.clone())
    }

    pub fn root_path(&self) -> PathBuf {
        self.root_path.clone()
    }

    pub fn commit_interval_ms(&self) -> u64 {
        self.commit_interval_ms
    }

    pub fn storage_config(&self) -> &memorose_common::config::StorageConfig {
        &self.storage_config
    }

    pub fn auto_planner(&self) -> bool {
        self.auto_planner
    }

    pub fn task_reflection(&self) -> bool {
        self.task_reflection
    }

    pub fn graph(&self) -> &GraphStore {
        &self.graph
    }

    pub async fn compact_vector_store(&self) -> Result<()> {
        self.vector.compact_files("memories").await?;
        Ok(())
    }

    pub fn get_org_share_policy(
        &self,
        user_id: &str,
        org_id: &str,
    ) -> Result<memorose_common::SharePolicy> {
        let key = Self::org_share_policy_key(user_id, org_id);
        match self.system_kv().get(key.as_bytes())? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).unwrap_or_default()),
            None => Ok(memorose_common::SharePolicy::default()),
        }
    }

    pub fn set_org_share_policy(
        &self,
        user_id: &str,
        org_id: &str,
        policy: &memorose_common::SharePolicy,
    ) -> Result<()> {
        let policy = Self::normalize_share_policy(
            policy.clone(),
            memorose_common::ShareTarget::Organization,
        );
        let key = Self::org_share_policy_key(user_id, org_id);
        self.system_kv()
            .put(key.as_bytes(), &serde_json::to_vec(&policy)?)
    }

    pub fn get_org_backfill_status(
        &self,
        user_id: &str,
        org_id: &str,
    ) -> Result<Option<serde_json::Value>> {
        let key = Self::backfill_status_key(
            &memorose_common::MemoryDomain::Organization,
            user_id,
            org_id,
        );
        match self.system_kv().get(key.as_bytes())? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            None => Ok(None),
        }
    }

    pub async fn disable_org_contribution(
        &self,
        user_id: &str,
        org_id: &str,
    ) -> Result<usize> {
        let knowledge_records = self
            .find_org_knowledge_records_for_contributor(user_id, org_id)
            .await?;
        let mut removed_contributions = 0;

        for (record, source_ids_to_remove) in knowledge_records {
            if source_ids_to_remove.is_empty() {
                continue;
            }

            for source_id in &source_ids_to_remove {
                self.revoke_organization_contribution(record.id, *source_id)?;
            }

            let record_source_ids = self.resolve_organization_record_source_ids(&record).await?;
            let remaining_source_ids: Vec<Uuid> = record_source_ids
                .iter()
                .copied()
                .filter(|source_id| !source_ids_to_remove.contains(source_id))
                .collect();
            let remaining_sources = self
                .load_organization_source_units(&remaining_source_ids)
                .await?;

            if remaining_sources.is_empty() {
                self.delete_organization_knowledge_records(vec![record.clone()])
                    .await?;
            } else {
                let topic = Self::select_organization_topic(&remaining_sources);

                if let Some(topic) = topic {
                    let rebuilt_record = self
                        .compose_organization_knowledge_record(
                            org_id,
                            &remaining_sources,
                            Some(&record),
                            &topic,
                        )
                        .ok_or_else(|| {
                            anyhow::anyhow!("failed to rebuild organization knowledge")
                        })?;
                    let rebuilt_unit =
                        Self::materialize_organization_read_view(&rebuilt_record);
                    let unit_id = rebuilt_record.id;
                    let topic_relations = Self::organization_topic_relations(
                        org_id,
                        unit_id,
                        &topic,
                        rebuilt_record.updated_at,
                    );
                    let previous_relation_keys = self
                        .list_organization_relations_for_knowledge(record.id)
                        .await?
                        .into_iter()
                        .map(|relation| Self::organization_relation_key(&relation))
                        .collect::<Vec<_>>();
                    let candidate_contribution_records =
                        Self::organization_candidate_contribution_records(
                            org_id,
                            unit_id,
                            &remaining_sources,
                            rebuilt_record.updated_at,
                        );
                    let topic_relation_keys = topic_relations
                        .iter()
                        .map(Self::organization_relation_key)
                        .collect::<Vec<_>>();

                    let memberships = self
                        .publish_organization_knowledge(
                            rebuilt_record,
                            rebuilt_unit,
                            candidate_contribution_records,
                            topic_relations,
                            types::OrganizationPublicationKind::Rebuild,
                        )
                        .await?;
                    let membership_keys = memberships
                        .iter()
                        .map(|membership| {
                            Self::organization_membership_source_key(membership.source_id)
                        })
                        .collect::<Vec<_>>();
                    for stale_relation_key in previous_relation_keys {
                        if !membership_keys.contains(&stale_relation_key)
                            && !topic_relation_keys.contains(&stale_relation_key)
                        {
                            self.delete_organization_relation_by_primary_key(
                                &stale_relation_key,
                            )
                            .ok();
                        }
                    }
                } else {
                    self.delete_organization_knowledge_records(vec![record.clone()])
                        .await?;
                }
            }

            for source_id in source_ids_to_remove {
                self.delete_organization_membership(source_id).ok();
                removed_contributions += 1;
            }
        }

        Ok(removed_contributions)
    }

    fn org_share_policy_key(user_id: &str, org_id: &str) -> String {
        format!("share_policy:user:{}:org:{}", user_id, org_id)
    }

    fn backfill_status_key(
        domain: &memorose_common::MemoryDomain,
        user_id: &str,
        scope_id: &str,
    ) -> String {
        format!("share_backfill:{}:{}:{}", domain.as_str(), user_id, scope_id)
    }
}
