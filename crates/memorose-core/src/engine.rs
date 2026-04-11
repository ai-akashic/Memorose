use crate::arbitrator::{
    Arbitrator, ExtractedMemoryFact, MemoryCorrectionAction, MemoryCorrectionKind,
};
use crate::fact_extraction::{self, MemoryFactChangeType, MemoryFactDescriptor};
use crate::reranker::Reranker;
use crate::storage::graph::GraphStore;
use crate::storage::index::{TextIndex, TextIndexConfig, TextIndexMetricSnapshot};
use crate::storage::kv::KvStore;
use crate::storage::system_kv::SystemKvStore;
use crate::storage::vector::VectorStore;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Timelike, Utc};
use dashmap::DashMap;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use lancedb::connect;
use memorose_common::{
    tokenizer::count_tokens, Event, ForgettingTombstone, GraphEdge, MemoryDomain, MemoryType,
    MaterializationState, MemoryUnit, RelationType, SharePolicy, ShareTarget, TimeRange,
};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

const OBSOLETE_ACTION_MIN_CONFIDENCE: f32 = 0.85;
const OBSOLETE_ACTION_RELATION_ONLY_MIN_CONFIDENCE: f32 = 0.70;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ReflectionMarker {
    pub first_event_at_ts: i64,
    pub last_event_at_ts: i64,
    pub pending_units: usize,
    pub pending_tokens: usize,
    #[serde(default)]
    pub first_event_tx_micros: i64,
    #[serde(default)]
    pub last_event_tx_micros: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_event_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReflectionBatchOutcome {
    pub created_topics: usize,
    pub consumed_units: usize,
    pub consumed_tokens: usize,
    pub next_first_event_tx_micros: Option<i64>,
    pub next_first_event_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PendingMaterializationJobStatus {
    Pending,
    RetryScheduled,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingMaterializationPart {
    Text { text: String },
    InlineData { mime_type: String, data: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingMaterializationInput {
    Text(String),
    Multimodal { parts: Vec<PendingMaterializationPart> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingMaterializationJob {
    pub job_id: Uuid,
    pub unit: MemoryUnit,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub post_publish_edges: Vec<GraphEdge>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embed_input: Option<PendingMaterializationInput>,
    pub status: PendingMaterializationJobStatus,
    #[serde(default)]
    pub attempts: u32,
    pub next_attempt_at_micros: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl PendingMaterializationJob {
    pub fn new(
        mut unit: MemoryUnit,
        post_publish_edges: Vec<GraphEdge>,
        embed_input: Option<PendingMaterializationInput>,
    ) -> Self {
        let now = Utc::now();
        unit.visible = false;
        unit.materialization_state = MaterializationState::Pending;
        unit.materialized_at = None;
        Self {
            job_id: Uuid::new_v4(),
            unit,
            post_publish_edges,
            embed_input,
            status: PendingMaterializationJobStatus::Pending,
            attempts: 0,
            next_attempt_at_micros: now.timestamp_micros(),
            last_error: None,
            created_at: now,
            updated_at: now,
        }
    }
}

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
    storage_config: memorose_common::config::StorageConfig,
    pub auto_planner: bool,
    pub task_reflection: bool,
    pub task_locks: Arc<DashMap<Uuid, Arc<Mutex<()>>>>,
    pub auto_link_similarity_threshold: f32,
    // New: Query optimization components
    query_cache: Arc<crate::graph::QueryCache>,
    batch_executor: Arc<crate::graph::BatchExecutor>,
}

#[derive(Clone)]
struct OrganizationProjectionTopic {
    label: String,
    alias_keys: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrganizationKnowledgeRecord {
    pub id: Uuid,
    pub org_id: String,
    pub topic_label: String,
    pub topic_alias_keys: Vec<String>,
    pub memory_type: MemoryType,
    pub content: String,
    pub embedding: Option<Vec<f32>>,
    pub keywords: Vec<String>,
    pub importance: f32,
    pub valid_time: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum OrganizationKnowledgeRelationKind {
    Source { source_id: Uuid },
    TopicAlias { topic_key: String },
}

#[derive(Clone, Serialize, Deserialize)]
struct OrganizationKnowledgeRelationRecord {
    org_id: String,
    knowledge_id: Uuid,
    relation: OrganizationKnowledgeRelationKind,
    updated_at: DateTime<Utc>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrganizationKnowledgeMembershipRecord {
    pub org_id: String,
    pub knowledge_id: Uuid,
    pub source_id: Uuid,
    pub contributor_user_id: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrganizationKnowledgeContributionStatus {
    Candidate,
    Active,
    Revoked,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrganizationKnowledgeApprovalMode {
    Auto,
}

impl Default for OrganizationKnowledgeContributionStatus {
    fn default() -> Self {
        Self::Active
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrganizationKnowledgeContributionRecord {
    pub org_id: String,
    pub knowledge_id: Uuid,
    pub source_id: Uuid,
    pub contributor_user_id: String,
    #[serde(default)]
    pub status: OrganizationKnowledgeContributionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activated_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_mode: Option<OrganizationKnowledgeApprovalMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_by: Option<String>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrganizationKnowledgeMembershipEntry {
    pub membership: OrganizationKnowledgeMembershipRecord,
    pub source_unit: MemoryUnit,
    pub contribution: Option<OrganizationKnowledgeContributionRecord>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrganizationKnowledgeContributionEntry {
    pub contribution: OrganizationKnowledgeContributionRecord,
    pub source_unit: Option<MemoryUnit>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrganizationKnowledgeDetailRecord {
    pub record: OrganizationKnowledgeRecord,
    pub read_view: MemoryUnit,
    pub memberships: Vec<OrganizationKnowledgeMembershipEntry>,
    pub contributions: Vec<OrganizationKnowledgeContributionEntry>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrganizationKnowledgeSearchHit {
    pub knowledge_id: Uuid,
    pub org_id: String,
    pub unit: MemoryUnit,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SharedSearchHit {
    NativeMemory {
        unit: MemoryUnit,
    },
    OrganizationKnowledge {
        knowledge: OrganizationKnowledgeSearchHit,
    },
}

impl SharedSearchHit {
    pub fn native(unit: MemoryUnit) -> Self {
        Self::NativeMemory { unit }
    }

    fn organization_knowledge(record: &OrganizationKnowledgeRecord, unit: MemoryUnit) -> Self {
        Self::OrganizationKnowledge {
            knowledge: OrganizationKnowledgeSearchHit {
                knowledge_id: record.id,
                org_id: record.org_id.clone(),
                unit,
            },
        }
    }

    pub fn memory_unit(&self) -> &MemoryUnit {
        match self {
            Self::NativeMemory { unit } => unit,
            Self::OrganizationKnowledge { knowledge } => &knowledge.unit,
        }
    }

    pub fn into_memory_unit(self) -> MemoryUnit {
        match self {
            Self::NativeMemory { unit } => unit,
            Self::OrganizationKnowledge { knowledge } => knowledge.unit,
        }
    }
}

impl Deref for SharedSearchHit {
    type Target = MemoryUnit;

    fn deref(&self) -> &Self::Target {
        self.memory_unit()
    }
}

impl DerefMut for SharedSearchHit {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::NativeMemory { unit } => unit,
            Self::OrganizationKnowledge { knowledge } => &mut knowledge.unit,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrganizationAutomationCounterSnapshot {
    pub org_id: String,
    pub auto_approved_total: usize,
    pub auto_publish_total: usize,
    pub rebuild_total: usize,
    pub revoke_total: usize,
    pub merged_publication_total: usize,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct RacMetricSnapshot {
    pub fact_extraction_attempt_total: usize,
    pub fact_extraction_success_total: usize,
    pub correction_action_obsolete_total: usize,
    pub correction_action_contradicts_total: usize,
    pub correction_action_reaffirm_total: usize,
    pub correction_action_ignore_total: usize,
    pub tombstone_total: usize,
}

impl RacMetricSnapshot {
    pub fn merge(&mut self, other: &Self) {
        self.fact_extraction_attempt_total += other.fact_extraction_attempt_total;
        self.fact_extraction_success_total += other.fact_extraction_success_total;
        self.correction_action_obsolete_total += other.correction_action_obsolete_total;
        self.correction_action_contradicts_total += other.correction_action_contradicts_total;
        self.correction_action_reaffirm_total += other.correction_action_reaffirm_total;
        self.correction_action_ignore_total += other.correction_action_ignore_total;
        self.tombstone_total += other.tombstone_total;
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct RacMetricHistoryPoint {
    pub bucket_start: String,
    pub fact_extraction_attempt_total: usize,
    pub fact_extraction_success_total: usize,
    pub correction_action_obsolete_total: usize,
    pub correction_action_contradicts_total: usize,
    pub correction_action_reaffirm_total: usize,
    pub correction_action_ignore_total: usize,
    pub tombstone_total: usize,
}

impl RacMetricHistoryPoint {
    pub fn merge(&mut self, other: &Self) {
        self.fact_extraction_attempt_total += other.fact_extraction_attempt_total;
        self.fact_extraction_success_total += other.fact_extraction_success_total;
        self.correction_action_obsolete_total += other.correction_action_obsolete_total;
        self.correction_action_contradicts_total += other.correction_action_contradicts_total;
        self.correction_action_reaffirm_total += other.correction_action_reaffirm_total;
        self.correction_action_ignore_total += other.correction_action_ignore_total;
        self.tombstone_total += other.tombstone_total;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RacDecisionEffect {
    Tombstone,
    RelationOnly,
    Noop,
    Rejected,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RacDecisionRecord {
    pub created_at: DateTime<Utc>,
    pub stage: String,
    pub user_id: String,
    pub org_id: Option<String>,
    pub source_unit_id: Uuid,
    pub target_unit_id: Option<Uuid>,
    pub action: String,
    pub confidence: f32,
    pub effect: RacDecisionEffect,
    pub relation: Option<String>,
    pub reason: String,
    pub guard_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RacReviewStatus {
    Pending,
    Approved,
    Rejected,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RacReviewRecord {
    pub review_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub stage: String,
    pub user_id: String,
    pub org_id: Option<String>,
    pub source_unit_id: Uuid,
    pub target_unit_id: Uuid,
    pub action: String,
    pub confidence: f32,
    pub relation: Option<String>,
    pub reason: String,
    pub guard_reason: Option<String>,
    pub status: RacReviewStatus,
    pub reviewer: Option<String>,
    pub reviewer_note: Option<String>,
}

pub(crate) enum ValidatedCorrectionDecision {
    Tombstone {
        relation: RelationType,
    },
    RelationOnly {
        relation: RelationType,
        guard_reason: Option<String>,
    },
    Skip {
        effect: RacDecisionEffect,
        guard_reason: String,
    },
}

#[derive(Clone, Debug)]
pub struct PlannedMemoryCorrectionAction {
    pub target_id: Uuid,
    pub kind: MemoryCorrectionKind,
    pub confidence: f32,
    pub reason: String,
    pub effect: RacDecisionEffect,
    pub relation: Option<RelationType>,
    pub guard_reason: Option<String>,
}

#[derive(Clone, Copy)]
enum OrganizationPublicationKind {
    New,
    Rebuild,
}

struct OrganizationKnowledgeMutation {
    topic_relations: Vec<OrganizationKnowledgeRelationRecord>,
    candidate_contribution_records: Vec<OrganizationKnowledgeContributionRecord>,
    stale_relation_keys: Vec<String>,
    obsolete_records: Vec<OrganizationKnowledgeRecord>,
    record: OrganizationKnowledgeRecord,
    unit: MemoryUnit,
}

struct OrganizationKnowledgeSnapshot {
    record: OrganizationKnowledgeRecord,
    read_view: MemoryUnit,
    membership_sources: Vec<(OrganizationKnowledgeMembershipRecord, MemoryUnit)>,
    contributions: Vec<OrganizationKnowledgeContributionRecord>,
}

#[derive(Default)]
struct OrganizationStorageReconciliationStats {
    removed_persisted_views: usize,
    reconciled_records: usize,
    removed_records: usize,
    removed_stale_source_relations: usize,
}

impl MemoroseEngine {
    async fn build_organization_knowledge_detail_record_from_snapshot(
        &self,
        snapshot: OrganizationKnowledgeSnapshot,
    ) -> OrganizationKnowledgeDetailRecord {
        let OrganizationKnowledgeSnapshot {
            record,
            read_view,
            membership_sources,
            mut contributions,
        } = snapshot;
        contributions.sort_by_key(Self::organization_contribution_sort_key);
        let contribution_records_by_source = contributions
            .iter()
            .map(|contribution| (contribution.source_id, contribution.clone()))
            .collect::<HashMap<_, _>>();
        let mut membership_entries = membership_sources
            .into_iter()
            .map(
                |(membership, source_unit)| OrganizationKnowledgeMembershipEntry {
                    contribution: contribution_records_by_source
                        .get(&membership.source_id)
                        .cloned(),
                    membership,
                    source_unit,
                },
            )
            .collect::<Vec<_>>();
        membership_entries.sort_by(|left, right| {
            let left_activated_at = left
                .contribution
                .as_ref()
                .and_then(|contribution| contribution.activated_at);
            let right_activated_at = right
                .contribution
                .as_ref()
                .and_then(|contribution| contribution.activated_at);
            right_activated_at
                .cmp(&left_activated_at)
                .then_with(|| right.membership.updated_at.cmp(&left.membership.updated_at))
                .then_with(|| left.membership.source_id.cmp(&right.membership.source_id))
        });
        let mut contribution_entries = Vec::with_capacity(contributions.len());
        for contribution in contributions {
            let source_unit = if let Some(entry) = membership_entries
                .iter()
                .find(|entry| entry.membership.source_id == contribution.source_id)
            {
                Some(entry.source_unit.clone())
            } else {
                self.get_native_memory_unit_by_index(contribution.source_id)
                    .await
                    .ok()
                    .flatten()
            };
            contribution_entries.push(OrganizationKnowledgeContributionEntry {
                contribution,
                source_unit,
            });
        }

        OrganizationKnowledgeDetailRecord {
            record,
            read_view,
            memberships: membership_entries,
            contributions: contribution_entries,
        }
    }

    async fn load_organization_knowledge_snapshot(
        &self,
        id: Uuid,
    ) -> Result<Option<OrganizationKnowledgeSnapshot>> {
        let Some(record) = self.load_organization_knowledge(id)? else {
            return Ok(None);
        };
        let read_view = self
            .materialize_organization_read_view_for_record(&record)
            .await?;
        let membership_sources = self.load_organization_membership_sources(id).await?;
        let contributions = self.list_organization_contributions(id).await?;
        Ok(Some(OrganizationKnowledgeSnapshot {
            record,
            read_view,
            membership_sources,
            contributions,
        }))
    }

    async fn list_organization_knowledge_snapshots(
        &self,
        org_id_filter: Option<&str>,
    ) -> Result<Vec<OrganizationKnowledgeSnapshot>> {
        let mut snapshots = Vec::new();
        let mut records = self
            .list_organization_knowledge_records(org_id_filter, None)
            .await?;
        records.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.id.cmp(&right.id))
        });

        for record in records {
            if let Some(snapshot) = self.load_organization_knowledge_snapshot(record.id).await? {
                snapshots.push(snapshot);
            }
        }

        Ok(snapshots)
    }

    fn is_local_domain(domain: &MemoryDomain) -> bool {
        matches!(domain, MemoryDomain::Agent | MemoryDomain::User)
    }

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

    pub fn build_user_filter(&self, user_id: &str, extra: Option<String>) -> Option<String> {
        fn escape_sql_string(s: &str) -> String {
            s.replace('\'', "''")
        }
        let mut conditions = vec![format!("user_id = '{}'", escape_sql_string(user_id))];
        if let Some(e) = extra {
            conditions.push(e);
        }
        Some(conditions.join(" AND "))
    }

    fn build_global_filter(
        &self,
        domain: MemoryDomain,
        org_id: Option<&str>,
        agent_id: Option<&str>,
        extra: Option<String>,
    ) -> Option<String> {
        fn escape_sql_string(s: &str) -> String {
            s.replace('\'', "''")
        }

        let mut conditions = vec![format!("domain = '{}'", domain.as_str())];
        if let Some(oid) = org_id {
            conditions.push(format!("org_id = '{}'", escape_sql_string(oid)));
        }
        if let Some(agid) = agent_id {
            conditions.push(format!("agent_id = '{}'", escape_sql_string(agid)));
        }
        if let Some(e) = extra {
            conditions.push(e);
        }
        Some(conditions.join(" AND "))
    }

    fn org_share_policy_key(user_id: &str, org_id: &str) -> String {
        format!("share_policy:user:{}:org:{}", user_id, org_id)
    }

    fn organization_knowledge_key(id: Uuid) -> String {
        format!("organization_knowledge:{}", id)
    }

    fn organization_source_relation_key(source_id: Uuid) -> String {
        format!("organization_knowledge_relation:source:{}", source_id)
    }

    fn organization_topic_relation_key(org_id: &str, topic_key: &str) -> String {
        format!(
            "organization_knowledge_relation:topic:{}:{}",
            org_id, topic_key
        )
    }

    fn organization_knowledge_contribution_key(knowledge_id: Uuid, source_id: Uuid) -> String {
        format!(
            "organization_knowledge_contribution:{}:{}",
            knowledge_id, source_id
        )
    }

    fn organization_knowledge_contribution_prefix(knowledge_id: Uuid) -> String {
        format!("organization_knowledge_contribution:{}:", knowledge_id)
    }

    fn organization_membership_source_key(source_id: Uuid) -> String {
        format!("organization_knowledge_membership:source:{}", source_id)
    }

    fn organization_membership_by_knowledge_prefix(knowledge_id: Uuid) -> String {
        format!(
            "organization_knowledge_membership_by_knowledge:{}:",
            knowledge_id
        )
    }

    fn organization_membership_by_knowledge_key(
        membership: &OrganizationKnowledgeMembershipRecord,
    ) -> String {
        format!(
            "{}{}",
            Self::organization_membership_by_knowledge_prefix(membership.knowledge_id),
            membership.source_id
        )
    }

    fn organization_knowledge_relation_index_prefix(knowledge_id: Uuid) -> String {
        format!(
            "organization_knowledge_relation_by_knowledge:{}:",
            knowledge_id
        )
    }

    fn organization_knowledge_relation_index_key(
        record: &OrganizationKnowledgeRelationRecord,
    ) -> String {
        match &record.relation {
            OrganizationKnowledgeRelationKind::Source { source_id } => format!(
                "{}source:{}",
                Self::organization_knowledge_relation_index_prefix(record.knowledge_id),
                source_id
            ),
            OrganizationKnowledgeRelationKind::TopicAlias { topic_key } => format!(
                "{}topic:{}",
                Self::organization_knowledge_relation_index_prefix(record.knowledge_id),
                topic_key
            ),
        }
    }

    fn organization_relation_key(record: &OrganizationKnowledgeRelationRecord) -> String {
        match &record.relation {
            OrganizationKnowledgeRelationKind::Source { source_id } => {
                Self::organization_source_relation_key(*source_id)
            }
            OrganizationKnowledgeRelationKind::TopicAlias { topic_key } => {
                Self::organization_topic_relation_key(&record.org_id, topic_key)
            }
        }
    }

    fn organization_read_view_owner(org_id: &str) -> String {
        format!("__organization__:{}", org_id)
    }

    fn normalize_whitespace(text: &str) -> String {
        text.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn normalize_memory_keywords(keywords: &[String], limit: usize) -> Vec<String> {
        fact_extraction::normalize_memory_keywords(keywords, limit)
    }

    fn neutralize_first_person_language(text: &str) -> String {
        text.split_whitespace()
            .map(|token| {
                let prefix_len = token
                    .find(|c: char| c.is_alphanumeric())
                    .unwrap_or(token.len());
                let suffix_start = token
                    .rfind(|c: char| c.is_alphanumeric())
                    .map(|idx| idx + 1)
                    .unwrap_or(prefix_len);
                let prefix = &token[..prefix_len];
                let core = &token[prefix_len..suffix_start];
                let suffix = &token[suffix_start..];

                let replacement = match core.to_ascii_lowercase().as_str() {
                    "i" | "me" => Some("the contributor"),
                    "my" | "mine" => Some("the contributor's"),
                    "we" | "us" => Some("the organization"),
                    "our" | "ours" => Some("the organization's"),
                    _ => None,
                };

                match replacement {
                    Some(value) => format!("{}{}{}", prefix, value, suffix),
                    None => token.to_string(),
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn normalize_organization_keywords(source: &MemoryUnit) -> Vec<String> {
        Self::normalize_memory_keywords(&source.keywords, 8)
    }

    fn build_organization_topic_key(label: &str) -> String {
        let mut key = String::new();
        let mut needs_separator = false;

        for ch in label.chars() {
            if ch.is_ascii_alphanumeric() {
                if needs_separator && !key.is_empty() {
                    key.push('-');
                }
                key.push(ch.to_ascii_lowercase());
                needs_separator = false;
            } else if !key.is_empty() {
                needs_separator = true;
            }
        }

        key
    }

    fn fallback_organization_topic_label(text: &str) -> Option<String> {
        let normalized = Self::normalize_whitespace(text);
        if normalized.is_empty() {
            return None;
        }

        let label = normalized
            .split_whitespace()
            .take(6)
            .collect::<Vec<_>>()
            .join(" ");
        if label.is_empty() {
            None
        } else {
            Some(label)
        }
    }

    fn organization_topic_candidates_from_keywords_and_content(
        keywords: &[String],
        content: &str,
    ) -> Vec<(String, String)> {
        let mut seen = HashSet::new();
        let mut candidates = Vec::new();

        for label in keywords {
            let key = Self::build_organization_topic_key(label);
            if !key.is_empty() && seen.insert(key.clone()) {
                candidates.push((label.clone(), key));
            }
        }

        if candidates.is_empty() {
            if let Some(label) = Self::fallback_organization_topic_label(content) {
                let key = Self::build_organization_topic_key(&label);
                if !key.is_empty() && seen.insert(key.clone()) {
                    candidates.push((label, key));
                }
            }
        }

        candidates
    }

    fn organization_source_topic_candidates(source: &MemoryUnit) -> Vec<(String, String)> {
        let keywords = Self::normalize_organization_keywords(source);
        Self::organization_topic_candidates_from_keywords_and_content(&keywords, &source.content)
    }

    fn select_organization_topic_from_candidates(
        candidate_groups: &[Vec<(String, String)>],
    ) -> Option<OrganizationProjectionTopic> {
        let mut total_counts: HashMap<String, usize> = HashMap::new();
        let mut primary_counts: HashMap<String, usize> = HashMap::new();
        let mut labels_by_key: HashMap<String, String> = HashMap::new();
        let mut alias_order = Vec::new();

        for group in candidate_groups {
            if group.is_empty() {
                continue;
            }

            let mut seen_in_group = HashSet::new();
            for (index, (label, key)) in group.iter().enumerate() {
                if !seen_in_group.insert(key.clone()) {
                    continue;
                }

                *total_counts.entry(key.clone()).or_insert(0) += 1;
                if index == 0 {
                    *primary_counts.entry(key.clone()).or_insert(0) += 1;
                }

                labels_by_key
                    .entry(key.clone())
                    .and_modify(|existing| {
                        if label.len() < existing.len() {
                            *existing = label.clone();
                        }
                    })
                    .or_insert_with(|| label.clone());
                alias_order.push(key.clone());
            }
        }

        let mut alias_keys = alias_order
            .into_iter()
            .filter(|key| total_counts.contains_key(key))
            .collect::<Vec<_>>();
        alias_keys.sort_by(|left, right| {
            total_counts
                .get(right)
                .copied()
                .unwrap_or_default()
                .cmp(&total_counts.get(left).copied().unwrap_or_default())
                .then_with(|| {
                    primary_counts
                        .get(right)
                        .copied()
                        .unwrap_or_default()
                        .cmp(&primary_counts.get(left).copied().unwrap_or_default())
                })
                .then_with(|| {
                    labels_by_key
                        .get(left)
                        .map(|label| label.len())
                        .unwrap_or(usize::MAX)
                        .cmp(
                            &labels_by_key
                                .get(right)
                                .map(|label| label.len())
                                .unwrap_or(usize::MAX),
                        )
                })
                .then_with(|| left.cmp(right))
        });
        alias_keys.dedup();

        let key = alias_keys.first()?.clone();
        let label = labels_by_key.get(&key)?.clone();

        Some(OrganizationProjectionTopic { label, alias_keys })
    }

    fn select_organization_topic(sources: &[MemoryUnit]) -> Option<OrganizationProjectionTopic> {
        let candidate_groups = sources
            .iter()
            .map(Self::organization_source_topic_candidates)
            .filter(|group| !group.is_empty())
            .collect::<Vec<_>>();
        Self::select_organization_topic_from_candidates(&candidate_groups)
    }

    fn merge_organization_keywords(primary_label: &str, sources: &[MemoryUnit]) -> Vec<String> {
        let mut merged = Vec::new();
        let mut seen = HashSet::new();

        let primary = Self::normalize_whitespace(primary_label).trim().to_string();
        if !primary.is_empty() && seen.insert(primary.to_ascii_lowercase()) {
            merged.push(primary);
        }

        for source in sources {
            for keyword in Self::normalize_organization_keywords(source) {
                let dedupe_key = keyword.to_ascii_lowercase();
                if seen.insert(dedupe_key) {
                    merged.push(keyword);
                }
                if merged.len() >= 8 {
                    return merged;
                }
            }
        }

        merged
    }

    fn merge_organization_embedding(
        sources: &[MemoryUnit],
        representative: &MemoryUnit,
    ) -> Option<Vec<f32>> {
        let embeddings: Vec<&Vec<f32>> = sources
            .iter()
            .filter_map(|source| source.embedding.as_ref())
            .collect();
        if embeddings.is_empty() {
            return representative.embedding.clone();
        }

        let dim = embeddings[0].len();
        if embeddings.iter().any(|embedding| embedding.len() != dim) {
            return representative.embedding.clone();
        }

        let mut merged = vec![0.0; dim];
        for embedding in embeddings {
            for (index, value) in embedding.iter().enumerate() {
                merged[index] += *value;
            }
        }
        for value in &mut merged {
            *value /= sources
                .iter()
                .filter(|source| source.embedding.is_some())
                .count() as f32;
        }

        Some(merged)
    }

    fn compose_organization_knowledge_record(
        &self,
        org_id: &str,
        sources: &[MemoryUnit],
        existing: Option<&OrganizationKnowledgeRecord>,
        topic: &OrganizationProjectionTopic,
    ) -> Option<OrganizationKnowledgeRecord> {
        if sources.is_empty() {
            return None;
        }

        let mut sorted_sources = sources.to_vec();
        sorted_sources.sort_by(|left, right| {
            right
                .importance
                .partial_cmp(&left.importance)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right.valid_time.cmp(&left.valid_time))
                .then_with(|| right.transaction_time.cmp(&left.transaction_time))
                .then_with(|| left.id.cmp(&right.id))
        });

        let representative = sorted_sources.first()?;
        let keywords = Self::merge_organization_keywords(&topic.label, &sorted_sources);
        let content = Self::build_organization_knowledge_content(representative, &keywords);
        let embedding = Self::merge_organization_embedding(&sorted_sources, representative);
        let now = Utc::now();

        Some(OrganizationKnowledgeRecord {
            id: existing
                .map(|record| record.id)
                .unwrap_or_else(Uuid::new_v4),
            org_id: org_id.to_string(),
            topic_label: topic.label.clone(),
            topic_alias_keys: topic.alias_keys.clone(),
            memory_type: representative.memory_type.clone(),
            content,
            embedding,
            keywords,
            importance: sorted_sources
                .iter()
                .map(|source| source.importance)
                .fold(0.0, f32::max),
            valid_time: sorted_sources
                .iter()
                .filter_map(|source| source.valid_time)
                .max(),
            created_at: existing.map(|record| record.created_at).unwrap_or(now),
            updated_at: now,
        })
    }

    fn materialize_organization_read_view(record: &OrganizationKnowledgeRecord) -> MemoryUnit {
        let mut read_view = MemoryUnit::new_with_domain(
            Some(record.org_id.clone()),
            Self::organization_read_view_owner(&record.org_id),
            None,
            Uuid::nil(),
            record.memory_type.clone(),
            MemoryDomain::Organization,
            record.content.clone(),
            record.embedding.clone(),
        );
        read_view.id = record.id;
        read_view.keywords = record.keywords.clone();
        read_view.importance = record.importance;
        read_view.level = 2;
        read_view.stream_id = Uuid::nil();
        read_view.transaction_time = record.updated_at;
        read_view.last_accessed_at = record.updated_at;
        read_view.valid_time = record.valid_time;
        read_view.references.clear();
        read_view.assets.clear();
        read_view
    }

    fn organization_memberships_from_contributions(
        contributions: &[OrganizationKnowledgeContributionRecord],
    ) -> Vec<OrganizationKnowledgeMembershipRecord> {
        contributions
            .iter()
            .filter(|contribution| {
                matches!(
                    contribution.status,
                    OrganizationKnowledgeContributionStatus::Active
                )
            })
            .map(|contribution| OrganizationKnowledgeMembershipRecord {
                org_id: contribution.org_id.clone(),
                knowledge_id: contribution.knowledge_id,
                source_id: contribution.source_id,
                contributor_user_id: contribution.contributor_user_id.clone(),
                updated_at: contribution.updated_at,
            })
            .collect()
    }

    fn organization_topic_relations(
        org_id: &str,
        knowledge_id: Uuid,
        topic: &OrganizationProjectionTopic,
        updated_at: DateTime<Utc>,
    ) -> Vec<OrganizationKnowledgeRelationRecord> {
        topic
            .alias_keys
            .iter()
            .map(|topic_key| OrganizationKnowledgeRelationRecord {
                org_id: org_id.to_string(),
                knowledge_id,
                relation: OrganizationKnowledgeRelationKind::TopicAlias {
                    topic_key: topic_key.clone(),
                },
                updated_at,
            })
            .collect()
    }

    fn organization_candidate_contribution_records(
        org_id: &str,
        knowledge_id: Uuid,
        sources: &[MemoryUnit],
        candidate_at: DateTime<Utc>,
    ) -> Vec<OrganizationKnowledgeContributionRecord> {
        sources
            .iter()
            .map(|source| OrganizationKnowledgeContributionRecord {
                org_id: org_id.to_string(),
                knowledge_id,
                source_id: source.id,
                contributor_user_id: source.user_id.clone(),
                status: OrganizationKnowledgeContributionStatus::Candidate,
                candidate_at: Some(candidate_at),
                activated_at: None,
                approval_mode: None,
                approved_by: None,
                updated_at: candidate_at,
                revoked_at: None,
            })
            .collect()
    }

    fn activate_organization_contribution_records(
        candidates: &[OrganizationKnowledgeContributionRecord],
        activated_at: DateTime<Utc>,
    ) -> Vec<OrganizationKnowledgeContributionRecord> {
        candidates
            .iter()
            .map(|candidate| {
                let mut active = candidate.clone();
                active.status = OrganizationKnowledgeContributionStatus::Active;
                active.candidate_at = active.candidate_at.or(Some(activated_at));
                active.activated_at = Some(activated_at);
                active.approval_mode = Some(OrganizationKnowledgeApprovalMode::Auto);
                active.approved_by = Some("system:auto_publish".to_string());
                active.updated_at = activated_at;
                active.revoked_at = None;
                active
            })
            .collect()
    }

    fn build_organization_knowledge_content(source: &MemoryUnit, keywords: &[String]) -> String {
        let summary =
            Self::neutralize_first_person_language(&Self::normalize_whitespace(&source.content));
        if let Some(title) = keywords.first() {
            let summary_lower = summary.to_ascii_lowercase();
            let title_lower = title.to_ascii_lowercase();
            if summary_lower.starts_with(&title_lower) {
                summary
            } else {
                format!("{}: {}", title, summary)
            }
        } else {
            summary
        }
    }

    fn matches_valid_time_filter(
        valid_time: Option<DateTime<Utc>>,
        range: Option<&TimeRange>,
    ) -> bool {
        let Some(range) = range else {
            return true;
        };
        let Some(valid_time) = valid_time else {
            return false;
        };
        if let Some(start) = range.start {
            if valid_time < start {
                return false;
            }
        }
        if let Some(end) = range.end {
            if valid_time > end {
                return false;
            }
        }
        true
    }

    fn tokenize_search_text(text: &str) -> Vec<String> {
        fact_extraction::tokenize_search_text(text)
    }

    fn detect_memory_fact(unit: &MemoryUnit) -> Option<MemoryFactDescriptor> {
        fact_extraction::detect_memory_fact(unit)
    }

    fn fact_change_supports_obsolete(change_type: MemoryFactChangeType) -> bool {
        fact_extraction::fact_change_supports_obsolete(change_type)
    }

    fn fact_change_supports_contradiction(change_type: MemoryFactChangeType) -> bool {
        fact_extraction::fact_change_supports_contradiction(change_type)
    }

    fn build_memory_correction_focus_terms_with_fact(
        unit: &MemoryUnit,
        fact: Option<&MemoryFactDescriptor>,
    ) -> Vec<String> {
        fact_extraction::build_memory_correction_focus_terms_with_fact(unit, fact)
    }

    fn keyword_overlap_score(query_text: &str, content: &str, keywords: &[String]) -> f32 {
        fact_extraction::keyword_overlap_score(query_text, content, keywords)
    }

    fn memory_correction_candidate_score(
        unit: &MemoryUnit,
        candidate: &MemoryUnit,
        focus_terms: &[String],
        query_fact: Option<&MemoryFactDescriptor>,
    ) -> f32 {
        fact_extraction::memory_correction_candidate_score(unit, candidate, focus_terms, query_fact)
    }

    fn subject_keys_compatible(left: &str, right: &str) -> bool {
        fact_extraction::subject_keys_compatible(left, right)
    }

    fn memory_unit_token_cost(unit: &MemoryUnit) -> usize {
        let mut total = count_tokens(&unit.content);
        if !unit.keywords.is_empty() {
            total += count_tokens(&unit.keywords.join(" "));
        }
        for asset in &unit.assets {
            total += count_tokens(&asset.original_name);
            total += count_tokens(&asset.asset_type);
            if let Some(description) = asset.description.as_deref() {
                total += count_tokens(description);
            }
            if asset.storage_key.starts_with("http://") || asset.storage_key.starts_with("https://")
            {
                total += count_tokens(&asset.storage_key);
            }
        }
        total.max(1)
    }

    fn truncate_scored_results_to_token_budget<T>(
        results: Vec<(T, f32)>,
        token_budget: Option<usize>,
        mut token_cost: impl FnMut(&T) -> usize,
    ) -> Vec<(T, f32)> {
        let Some(token_budget) = token_budget.filter(|budget| *budget > 0) else {
            return results;
        };

        let mut used = 0usize;
        let mut budgeted = Vec::new();
        for (item, score) in results {
            let item_tokens = token_cost(&item);
            if used.saturating_add(item_tokens) > token_budget {
                continue;
            }
            used += item_tokens;
            budgeted.push((item, score));
        }
        budgeted
    }

    fn apply_token_budget_to_scored_memory_units(
        results: Vec<(MemoryUnit, f32)>,
        token_budget: Option<usize>,
    ) -> Vec<(MemoryUnit, f32)> {
        Self::truncate_scored_results_to_token_budget(results, token_budget, |unit| {
            Self::memory_unit_token_cost(unit)
        })
    }

    fn apply_token_budget_to_scored_shared_hits(
        results: Vec<(SharedSearchHit, f32)>,
        token_budget: Option<usize>,
    ) -> Vec<(SharedSearchHit, f32)> {
        Self::truncate_scored_results_to_token_budget(results, token_budget, |hit| {
            Self::memory_unit_token_cost(hit.memory_unit())
        })
    }

    fn descriptor_from_extracted_fact(fact: ExtractedMemoryFact) -> Option<MemoryFactDescriptor> {
        fact_extraction::descriptor_from_extracted_fact(fact)
    }

    fn push_unique_memory_terms(
        terms: &mut Vec<String>,
        seen: &mut HashSet<String>,
        values: impl IntoIterator<Item = String>,
    ) {
        for value in values {
            let normalized = value.trim();
            if normalized.is_empty() {
                continue;
            }
            let key = normalized.to_ascii_lowercase();
            if seen.insert(key) {
                terms.push(normalized.to_string());
            }
        }
    }

    fn memory_fact_descriptor_key(descriptor: &MemoryFactDescriptor) -> String {
        format!(
            "{:?}|{}|{:?}|{}|{:?}",
            descriptor.subject,
            descriptor.subject_key,
            descriptor.attribute,
            descriptor.value_payload.comparison_key(),
            descriptor.change_type
        )
    }

    fn compatible_fact_pair_score(
        left: &MemoryFactDescriptor,
        right: &MemoryFactDescriptor,
    ) -> Option<i32> {
        if left.subject != right.subject
            || !Self::subject_keys_compatible(&left.subject_key, &right.subject_key)
            || left.attribute != right.attribute
        {
            return None;
        }

        let exact_subject_key_bonus = if left.subject_key == right.subject_key {
            20
        } else {
            0
        };
        let value_kind_bonus = if left.value_kind == right.value_kind {
            5
        } else {
            0
        };
        let same_value_bonus =
            if left.value_payload.comparison_key() == right.value_payload.comparison_key() {
                2
            } else {
                0
            };

        Some(
            exact_subject_key_bonus
                + value_kind_bonus
                + same_value_bonus
                + left.confidence as i32
                + right.confidence as i32,
        )
    }

    fn build_memory_correction_focus_terms(
        unit: &MemoryUnit,
        facts: &[MemoryFactDescriptor],
    ) -> Vec<String> {
        let mut terms = Vec::new();
        let mut seen = HashSet::new();

        Self::push_unique_memory_terms(
            &mut terms,
            &mut seen,
            Self::build_memory_correction_focus_terms_with_fact(unit, None),
        );

        for fact in facts {
            Self::push_unique_memory_terms(
                &mut terms,
                &mut seen,
                Self::build_memory_correction_focus_terms_with_fact(unit, Some(fact)),
            );
        }

        terms.truncate(12);
        terms
    }

    fn build_memory_correction_search_queries(
        unit: &MemoryUnit,
        facts: &[MemoryFactDescriptor],
        focus_terms: &[String],
    ) -> Vec<String> {
        let mut queries = Vec::new();
        let mut seen = HashSet::new();

        Self::push_unique_memory_terms(
            &mut queries,
            &mut seen,
            std::iter::once(unit.content.clone()),
        );

        for fact in facts {
            Self::push_unique_memory_terms(
                &mut queries,
                &mut seen,
                fact.attribute
                    .search_phrases()
                    .iter()
                    .map(|phrase| (*phrase).to_string()),
            );
            Self::push_unique_memory_terms(
                &mut queries,
                &mut seen,
                Self::tokenize_search_text(&fact.value)
                    .into_iter()
                    .filter(|token| fact_extraction::is_memory_correction_focus_token(token)),
            );
            Self::push_unique_memory_terms(
                &mut queries,
                &mut seen,
                Self::tokenize_search_text(&fact.canonical_value)
                    .into_iter()
                    .filter(|token| fact_extraction::is_memory_correction_focus_token(token)),
            );
            Self::push_unique_memory_terms(
                &mut queries,
                &mut seen,
                Self::tokenize_search_text(fact.value_payload.comparison_key())
                    .into_iter()
                    .filter(|token| fact_extraction::is_memory_correction_focus_token(token)),
            );
        }

        Self::push_unique_memory_terms(
            &mut queries,
            &mut seen,
            focus_terms.iter().take(6).cloned(),
        );

        queries
    }

    fn memory_correction_candidate_score_for_facts(
        unit: &MemoryUnit,
        candidate: &MemoryUnit,
        focus_terms: &[String],
        query_facts: &[MemoryFactDescriptor],
    ) -> f32 {
        if query_facts.is_empty() {
            return Self::memory_correction_candidate_score(unit, candidate, focus_terms, None);
        }

        query_facts
            .iter()
            .map(|fact| {
                Self::memory_correction_candidate_score(unit, candidate, focus_terms, Some(fact))
            })
            .fold(0.0, f32::max)
    }

    async fn resolve_memory_fact_descriptors(
        &self,
        unit: &MemoryUnit,
    ) -> Vec<MemoryFactDescriptor> {
        let rule_facts = fact_extraction::detect_memory_facts(unit);
        let mut descriptors = Vec::new();
        let mut seen = HashSet::new();

        for descriptor in unit
            .extracted_facts
            .iter()
            .filter_map(fact_extraction::descriptor_from_stored_fact)
        {
            let key = Self::memory_fact_descriptor_key(&descriptor);
            if seen.insert(key) {
                descriptors.push(descriptor);
            }
        }

        if descriptors.is_empty() {
            let _ = self.increment_rac_metric_counter("fact_extraction_attempt_total", 1);
            let extracted = match self.arbitrator.extract_memory_facts(unit).await {
                Ok(facts) => facts,
                Err(error) => {
                    tracing::warn!(
                        "Memory fact extraction fallback failed for {}: {:?}",
                        unit.id,
                        error
                    );
                    Vec::new()
                }
            };

            for descriptor in extracted
                .into_iter()
                .filter_map(Self::descriptor_from_extracted_fact)
            {
                let key = Self::memory_fact_descriptor_key(&descriptor);
                if seen.insert(key) {
                    descriptors.push(descriptor);
                }
            }
        }

        for rule_fact in rule_facts {
            let key = Self::memory_fact_descriptor_key(&rule_fact);
            if descriptors.is_empty() || seen.insert(key) {
                descriptors.push(rule_fact);
            }
        }

        if !descriptors.is_empty() {
            let _ = self.increment_rac_metric_counter("fact_extraction_success_total", 1);
        }

        descriptors
    }

    pub async fn hydrate_memory_unit_extracted_facts(&self, unit: &mut MemoryUnit) {
        if unit.level != 1 || unit.memory_type != memorose_common::MemoryType::Factual {
            return;
        }

        let mut stored_facts = Vec::new();

        match self.arbitrator.extract_memory_facts(unit).await {
            Ok(facts) => {
                stored_facts.extend(
                    facts
                        .into_iter()
                        .filter_map(crate::fact_extraction::stored_fact_from_extracted_fact),
                );
            }
            Err(error) => {
                tracing::warn!(
                    "Memory fact extraction during engine hydration failed for {}: {:?}",
                    unit.id,
                    error
                );
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

    pub async fn plan_memory_correction_actions(
        &self,
        unit: &MemoryUnit,
        limit: usize,
    ) -> Result<Vec<PlannedMemoryCorrectionAction>> {
        let context = self
            .fetch_memory_correction_candidates(unit, limit.max(1))
            .await?;
        if context.is_empty() {
            return Ok(Vec::new());
        }

        let actions = self
            .detect_memory_correction_actions(unit, &context)
            .await?;
        let mut planned = Vec::new();

        for action in actions {
            let Some(target_unit) =
                self.get_memory_unit_including_forgotten(&unit.user_id, action.target_id)?
            else {
                planned.push(PlannedMemoryCorrectionAction {
                    target_id: action.target_id,
                    kind: action.kind,
                    confidence: action.confidence,
                    reason: action.reason,
                    effect: RacDecisionEffect::Rejected,
                    relation: None,
                    guard_reason: Some("target_missing".into()),
                });
                continue;
            };

            let (effect, relation, guard_reason) = match self
                .validate_memory_correction_relation(
                    unit,
                    &target_unit,
                    action.kind,
                    action.confidence,
                )
                .await
            {
                ValidatedCorrectionDecision::Tombstone { relation } => {
                    (RacDecisionEffect::Tombstone, Some(relation), None)
                }
                ValidatedCorrectionDecision::RelationOnly {
                    relation,
                    guard_reason,
                } => (
                    RacDecisionEffect::RelationOnly,
                    Some(relation),
                    guard_reason,
                ),
                ValidatedCorrectionDecision::Skip {
                    effect,
                    guard_reason,
                } => (effect, None, Some(guard_reason)),
            };

            planned.push(PlannedMemoryCorrectionAction {
                target_id: action.target_id,
                kind: action.kind,
                confidence: action.confidence,
                reason: action.reason,
                effect,
                relation,
                guard_reason,
            });
        }

        Ok(planned)
    }

    async fn resolve_fact_descriptors_compatible(
        &self,
        unit: &MemoryUnit,
        candidate: &MemoryUnit,
    ) -> Option<(MemoryFactDescriptor, MemoryFactDescriptor)> {
        let left_descriptors = self.resolve_memory_fact_descriptors(unit).await;
        let right_descriptors = self.resolve_memory_fact_descriptors(candidate).await;

        left_descriptors
            .iter()
            .flat_map(|left| {
                right_descriptors.iter().filter_map(|right| {
                    Self::compatible_fact_pair_score(left, right)
                        .map(|score| (score, left.clone(), right.clone()))
                })
            })
            .max_by_key(|(score, _, _)| *score)
            .map(|(_, left, right)| (left, right))
    }

    fn organization_similarity_score(
        record: &OrganizationKnowledgeRecord,
        query_text: &str,
        vector: &[f32],
    ) -> f32 {
        let lexical = Self::keyword_overlap_score(query_text, &record.content, &record.keywords);
        let semantic = record
            .embedding
            .as_ref()
            .map(|embedding| cosine_similarity(embedding, vector).max(0.0))
            .unwrap_or(0.0);

        match (semantic > 0.0, lexical > 0.0) {
            (true, true) => semantic * 0.7 + lexical * 0.3,
            (true, false) => semantic,
            (false, true) => lexical,
            (false, false) => 0.0,
        }
    }

    fn backfill_status_key(domain: &MemoryDomain, user_id: &str, scope_id: &str) -> String {
        format!(
            "share_backfill:{}:{}:{}",
            domain.as_str(),
            user_id,
            scope_id
        )
    }

    fn organization_metric_counter_key(org_id: &str, metric: &str) -> String {
        format!("organization_metric:{}:{}", org_id, metric)
    }

    fn rac_metric_counter_key(metric: &str) -> String {
        format!("rac_metric:{}", metric)
    }

    fn rac_metric_bucket_counter_key(metric: &str, bucket_start: &str) -> String {
        format!("rac_metric_bucket:{}:{}", bucket_start, metric)
    }

    fn rac_decision_key(created_at: DateTime<Utc>, source_unit_id: Uuid, nonce: Uuid) -> String {
        format!(
            "rac_decision:{:020}:{}:{}",
            created_at.timestamp_micros(),
            source_unit_id,
            nonce
        )
    }

    fn rac_review_key(review_id: &str) -> String {
        format!("rac_review:{}", review_id)
    }

    fn normalize_share_policy(mut policy: SharePolicy, target: ShareTarget) -> SharePolicy {
        policy.targets = vec![target];
        policy
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
    fn rac_metric_bucket_start(now: DateTime<Utc>) -> DateTime<Utc> {
        now.with_minute(0)
            .and_then(|dt| dt.with_second(0))
            .and_then(|dt| dt.with_nanosecond(0))
            .unwrap_or(now)
    }

    async fn materialize_organization_read_view_for_record(
        &self,
        record: &OrganizationKnowledgeRecord,
    ) -> Result<MemoryUnit> {
        Ok(Self::materialize_organization_read_view(record))
    }

    fn increment_organization_metric_counter(
        &self,
        org_id: &str,
        metric: &str,
        delta: usize,
    ) -> Result<()> {
        if delta == 0 {
            return Ok(());
        }

        let key = Self::organization_metric_counter_key(org_id, metric);
        let current = self
            .system_kv()
            .get(key.as_bytes())?
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])) as usize)
            .unwrap_or(0);
        self.system_kv()
            .put(key.as_bytes(), &((current + delta) as u64).to_le_bytes())?;
        Ok(())
    }

    fn get_organization_metric_counter(&self, org_id: &str, metric: &str) -> Result<usize> {
        let key = Self::organization_metric_counter_key(org_id, metric);
        Ok(self
            .system_kv()
            .get(key.as_bytes())?
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])) as usize)
            .unwrap_or(0))
    }

    fn increment_rac_metric_counter(&self, metric: &str, delta: usize) -> Result<()> {
        if delta == 0 {
            return Ok(());
        }

        let key = Self::rac_metric_counter_key(metric);
        let current = self
            .system_kv()
            .get(key.as_bytes())?
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])) as usize)
            .unwrap_or(0);
        self.system_kv()
            .put(key.as_bytes(), &((current + delta) as u64).to_le_bytes())?;

        let bucket_start = Self::rac_metric_bucket_start(Utc::now())
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let bucket_key = Self::rac_metric_bucket_counter_key(metric, &bucket_start);
        let bucket_current = self
            .system_kv()
            .get(bucket_key.as_bytes())?
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])) as usize)
            .unwrap_or(0);
        self.system_kv().put(
            bucket_key.as_bytes(),
            &((bucket_current + delta) as u64).to_le_bytes(),
        )?;
        Ok(())
    }

    fn get_rac_metric_counter(&self, metric: &str) -> Result<usize> {
        let key = Self::rac_metric_counter_key(metric);
        Ok(self
            .system_kv()
            .get(key.as_bytes())?
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])) as usize)
            .unwrap_or(0))
    }

    pub(crate) fn record_rac_decision(&self, record: &RacDecisionRecord) -> Result<()> {
        let key = Self::rac_decision_key(record.created_at, record.source_unit_id, Uuid::new_v4());
        self.system_kv()
            .put(key.as_bytes(), &serde_json::to_vec(record)?)
    }

    fn should_enqueue_rac_review(record: &RacDecisionRecord) -> bool {
        record.effect == RacDecisionEffect::RelationOnly
            && record.action == "obsolete"
            && record.guard_reason.as_deref() == Some("obsolete_relation_only_due_to_confidence")
            && record.target_unit_id.is_some()
    }

    pub(crate) fn enqueue_rac_review_for_decision(
        &self,
        record: &RacDecisionRecord,
    ) -> Result<Option<RacReviewRecord>> {
        if !Self::should_enqueue_rac_review(record) {
            return Ok(None);
        }

        let review = RacReviewRecord {
            review_id: Uuid::new_v4().to_string(),
            created_at: record.created_at,
            updated_at: record.created_at,
            stage: record.stage.clone(),
            user_id: record.user_id.clone(),
            org_id: record.org_id.clone(),
            source_unit_id: record.source_unit_id,
            target_unit_id: record.target_unit_id.expect("target checked above"),
            action: record.action.clone(),
            confidence: record.confidence,
            relation: record.relation.clone(),
            reason: record.reason.clone(),
            guard_reason: record.guard_reason.clone(),
            status: RacReviewStatus::Pending,
            reviewer: None,
            reviewer_note: None,
        };
        self.system_kv().put(
            Self::rac_review_key(&review.review_id).as_bytes(),
            &serde_json::to_vec(&review)?,
        )?;
        Ok(Some(review))
    }

    pub(crate) fn record_rac_decision_with_review(
        &self,
        record: &RacDecisionRecord,
    ) -> Result<Option<RacReviewRecord>> {
        self.record_rac_decision(record)?;
        self.enqueue_rac_review_for_decision(record)
    }

    fn organization_contribution_sort_key(
        contribution: &OrganizationKnowledgeContributionRecord,
    ) -> (u8, std::cmp::Reverse<DateTime<Utc>>, Uuid) {
        let status_rank = match contribution.status {
            OrganizationKnowledgeContributionStatus::Active => 0,
            OrganizationKnowledgeContributionStatus::Candidate => 1,
            OrganizationKnowledgeContributionStatus::Revoked => 2,
        };

        (
            status_rank,
            std::cmp::Reverse(contribution.updated_at),
            contribution.source_id,
        )
    }

    pub async fn get_organization_knowledge_detail_record(
        &self,
        id: Uuid,
    ) -> Result<Option<OrganizationKnowledgeDetailRecord>> {
        let Some(snapshot) = self.load_organization_knowledge_snapshot(id).await? else {
            return Ok(None);
        };
        Ok(Some(
            self.build_organization_knowledge_detail_record_from_snapshot(snapshot)
                .await,
        ))
    }

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
            if let Ok(config) = memorose_common::config::AppConfig::load() {
                if config.reranker.r#type == memorose_common::config::RerankerType::Http
                    && config.reranker.endpoint.is_some()
                {
                    Arc::new(crate::reranker::HttpReranker::new(
                        config.reranker.endpoint.unwrap(),
                    ))
                } else {
                    Arc::new(crate::reranker::WeightedReranker::new())
                }
            } else {
                Arc::new(crate::reranker::WeightedReranker::new())
            };

        // 初始化查询优化组件
        let query_cache = Arc::new(crate::graph::QueryCache::new(crate::graph::CacheConfig {
            ttl: std::time::Duration::from_secs(300), // 5 分钟 TTL
            max_entries: 5000,
            enabled: true,
        }));
        let batch_executor = Arc::new(crate::graph::BatchExecutor::new(graph.clone()));

        let engine = Self {
            _kv: kv,
            vector,
            index,
            graph,
            arbitrator,
            reranker,
            _root_path: root_path,
            _commit_interval_ms: storage_config.index_commit_max_interval_ms,
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

    pub async fn ingest_event(&self, event: Event) -> Result<()> {
        self.ingest_event_directly(event).await
    }

    pub async fn ingest_event_directly(&self, event: Event) -> Result<()> {
        self.ingest_events_directly(vec![event]).await
    }

    fn validate_event_not_empty(event: &Event) -> Result<()> {
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
        Ok(())
    }

    pub async fn ingest_events_directly(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut batch = rocksdb::WriteBatch::default();
        for event in &events {
            Self::validate_event_not_empty(event)?;

            let event_id = event.id.to_string();
            let user_id = event.user_id.clone();
            let key = format!("u:{}:event:{}", user_id, event_id);
            let val = serde_json::to_vec(event)?;
            batch.put(key.as_bytes(), &val);

            let pending_key = format!("pending:{}", event_id);
            let pending_val = serde_json::to_vec(&serde_json::json!({
                "user_id": user_id
            }))?;
            batch.put(pending_key.as_bytes(), &pending_val);

            let active_key = format!("active_user:{}", event.user_id);
            batch.put(active_key.as_bytes(), []);
        }

        self._kv.write_batch(batch)?;
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

    pub fn storage_config(&self) -> &memorose_common::config::StorageConfig {
        &self.storage_config
    }

    pub fn auto_planner(&self) -> bool {
        self.auto_planner
    }

    pub fn task_reflection(&self) -> bool {
        self.task_reflection
    }

    pub fn get_org_share_policy(&self, user_id: &str, org_id: &str) -> Result<SharePolicy> {
        let key = Self::org_share_policy_key(user_id, org_id);
        match self.system_kv().get(key.as_bytes())? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).unwrap_or_default()),
            None => Ok(SharePolicy::default()),
        }
    }

    pub fn set_org_share_policy(
        &self,
        user_id: &str,
        org_id: &str,
        policy: &SharePolicy,
    ) -> Result<()> {
        let policy = Self::normalize_share_policy(policy.clone(), ShareTarget::Organization);
        let key = Self::org_share_policy_key(user_id, org_id);
        self.system_kv()
            .put(key.as_bytes(), &serde_json::to_vec(&policy)?)
    }

    pub fn get_org_backfill_status(
        &self,
        user_id: &str,
        org_id: &str,
    ) -> Result<Option<serde_json::Value>> {
        let key = Self::backfill_status_key(&MemoryDomain::Organization, user_id, org_id);
        match self.system_kv().get(key.as_bytes())? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            None => Ok(None),
        }
    }

    pub async fn disable_org_contribution(&self, user_id: &str, org_id: &str) -> Result<usize> {
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
                    let rebuilt_unit = Self::materialize_organization_read_view(&rebuilt_record);
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
                            OrganizationPublicationKind::Rebuild,
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
                            self.delete_organization_relation_by_primary_key(&stale_relation_key)
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

    pub async fn compact_vector_store(&self) -> Result<()> {
        self.vector.compact_files("memories").await?;
        Ok(())
    }

    pub fn graph(&self) -> &GraphStore {
        &self.graph
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
        let pending_pairs = tokio::task::spawn_blocking(move || skv.scan(b"pending:")).await??;

        let mut events = Vec::new();
        let mut invalid_pending_entries = Vec::new();
        for (key, val) in pending_pairs {
            if events.len() >= limit {
                break;
            }

            let key_str = String::from_utf8(key)?;
            let parts: Vec<&str> = key_str.split(':').collect();
            if parts.len() == 2 {
                let event_id = parts[1];
                // Parse user_id from the pending value.
                // If the metadata is malformed or the original event body is gone,
                // move the pending marker into the failed queue so it doesn't block
                // consolidation forever.
                let user_id = if !val.is_empty() {
                    if let Ok(info) = serde_json::from_slice::<serde_json::Value>(&val) {
                        match info["user_id"].as_str() {
                            Some(user_id) if !user_id.is_empty() => user_id.to_string(),
                            _ => {
                                invalid_pending_entries.push((
                                    event_id.to_string(),
                                    "Pending metadata missing user_id".to_string(),
                                ));
                                continue;
                            }
                        }
                    } else {
                        invalid_pending_entries.push((
                            event_id.to_string(),
                            "Malformed pending metadata".to_string(),
                        ));
                        continue;
                    }
                } else {
                    invalid_pending_entries.push((
                        event_id.to_string(),
                        "Pending metadata missing user_id".to_string(),
                    ));
                    continue;
                };
                if let Some(event) = self.get_event(&user_id, event_id).await? {
                    events.push(event);
                } else {
                    invalid_pending_entries.push((
                        event_id.to_string(),
                        format!("Pending entry missing source event for user {}", user_id),
                    ));
                }
            }
        }

        for (event_id, reason) in invalid_pending_entries {
            if let Err(err) = self.mark_event_failed(&event_id, &reason).await {
                tracing::warn!(
                    "Failed to move invalid pending entry {} to failed queue: {:?}",
                    event_id,
                    err
                );
            }
        }

        events.sort_by(|a, b| a.transaction_time.cmp(&b.transaction_time));
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
        self.system_kv()
            .put(key.as_bytes(), &new_count.to_le_bytes())?;
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
        self.system_kv()
            .put(failed_key.as_bytes(), &serde_json::to_vec(&failed_info)?)?;

        // 清理重试计数，避免失败事件残留状态。
        let retry_key = format!("retry_count:{}", id);
        self.system_kv().delete(retry_key.as_bytes())?;

        Ok(())
    }

    fn get_event_raw(&self, user_id: &str, id: &str) -> Result<Option<Event>> {
        let key = format!("u:{}:event:{}", user_id, id);
        let val = self._kv.get(key.as_bytes())?;
        match val {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    pub async fn get_event(&self, user_id: &str, id: &str) -> Result<Option<Event>> {
        if self.is_event_forgotten(user_id, id)? {
            return Ok(None);
        }
        self.get_event_raw(user_id, id)
    }

    pub async fn delete_event(&self, user_id: &str, id: &str) -> Result<()> {
        let key = format!("u:{}:event:{}", user_id, id);
        let pending_key = format!("pending:{}", id);
        let retry_key = format!("retry_count:{}", id);
        let failed_key = format!("failed:{}", id);
        let forgotten_key = Self::forgotten_event_key(user_id, id);

        let mut batch = rocksdb::WriteBatch::default();
        batch.delete(key.as_bytes());
        batch.delete(pending_key.as_bytes());
        batch.delete(retry_key.as_bytes());
        batch.delete(failed_key.as_bytes());
        batch.delete(forgotten_key.as_bytes());

        self._kv.write_batch(batch)?;
        Ok(())
    }

    // ── Marker Methods ──────────────────────────────────────────────

    pub fn set_needs_reflect(&self, user_id: &str) -> Result<()> {
        self.bump_reflection_marker(user_id, 1, 0)
    }

    pub fn bump_reflection_marker(
        &self,
        user_id: &str,
        pending_units_delta: usize,
        pending_tokens_delta: usize,
    ) -> Result<()> {
        self.bump_reflection_marker_with_window(
            user_id,
            pending_units_delta,
            pending_tokens_delta,
            None,
            None,
            None,
        )
    }

    pub fn bump_reflection_marker_with_window(
        &self,
        user_id: &str,
        pending_units_delta: usize,
        pending_tokens_delta: usize,
        first_event_tx_micros: Option<i64>,
        last_event_tx_micros: Option<i64>,
        first_event_id: Option<String>,
    ) -> Result<()> {
        let key = format!("needs_reflect:{}", user_id);
        let now = chrono::Utc::now().timestamp_millis();
        let next = match self.system_kv().get(key.as_bytes())? {
            Some(raw) => {
                let mut marker =
                    serde_json::from_slice::<ReflectionMarker>(&raw).unwrap_or_else(|_| {
                        ReflectionMarker {
                            first_event_at_ts: now,
                            last_event_at_ts: now,
                            pending_units: 0,
                            pending_tokens: 0,
                            first_event_tx_micros: 0,
                            last_event_tx_micros: 0,
                            first_event_id: None,
                        }
                    });
                if marker.first_event_at_ts <= 0 {
                    marker.first_event_at_ts = now;
                }
                marker.last_event_at_ts = now;
                marker.pending_units = marker.pending_units.saturating_add(pending_units_delta);
                marker.pending_tokens = marker.pending_tokens.saturating_add(pending_tokens_delta);
                if let Some(first_tx) = first_event_tx_micros {
                    let should_update_cursor = marker.first_event_tx_micros <= 0
                        || first_tx < marker.first_event_tx_micros
                        || (first_tx == marker.first_event_tx_micros
                            && first_event_id
                                .as_ref()
                                .zip(marker.first_event_id.as_ref())
                                .map(|(new_id, old_id)| new_id < old_id)
                                .unwrap_or(marker.first_event_id.is_none()));
                    if should_update_cursor {
                        marker.first_event_tx_micros = first_tx;
                        marker.first_event_id = first_event_id.clone();
                    }
                }
                if let Some(last_tx) = last_event_tx_micros {
                    marker.last_event_tx_micros = marker.last_event_tx_micros.max(last_tx);
                }
                marker
            }
            None => ReflectionMarker {
                first_event_at_ts: now,
                last_event_at_ts: now,
                pending_units: pending_units_delta,
                pending_tokens: pending_tokens_delta,
                first_event_tx_micros: first_event_tx_micros.unwrap_or_default(),
                last_event_tx_micros: last_event_tx_micros.unwrap_or_default(),
                first_event_id,
            },
        };
        self.system_kv()
            .put(key.as_bytes(), &serde_json::to_vec(&next)?)
    }

    pub fn consume_reflection_marker_batch(
        &self,
        user_id: &str,
        consumed_units: usize,
        consumed_tokens: usize,
        next_first_event_tx_micros: Option<i64>,
        next_first_event_id: Option<String>,
    ) -> Result<()> {
        let key = format!("needs_reflect:{}", user_id);
        let Some(raw) = self.system_kv().get(key.as_bytes())? else {
            return Ok(());
        };
        let mut marker = serde_json::from_slice::<ReflectionMarker>(&raw).unwrap_or_default();
        marker.pending_units = marker.pending_units.saturating_sub(consumed_units);
        marker.pending_tokens = marker.pending_tokens.saturating_sub(consumed_tokens);

        if marker.pending_units == 0 || next_first_event_tx_micros.is_none() {
            return self.clear_reflection_marker(user_id);
        }

        marker.first_event_tx_micros = next_first_event_tx_micros.unwrap_or_default();
        marker.first_event_id = next_first_event_id;
        self.system_kv()
            .put(key.as_bytes(), &serde_json::to_vec(&marker)?)
    }

    pub fn set_needs_community(&self, user_id: &str) -> Result<()> {
        let key = format!("needs_community:{}", user_id);
        let ts = chrono::Utc::now().timestamp().to_string();
        self.system_kv().put(key.as_bytes(), ts.as_bytes())
    }

    pub fn get_pending_reflections(&self) -> Result<Vec<String>> {
        Ok(self
            .get_pending_reflection_markers()?
            .into_iter()
            .map(|(user_id, _)| user_id)
            .collect())
    }

    pub fn get_pending_reflection_markers(&self) -> Result<Vec<(String, ReflectionMarker)>> {
        let pairs = self.system_kv().scan(b"needs_reflect:")?;
        let mut markers = Vec::new();
        for (key, value) in pairs {
            let key_str = String::from_utf8(key)?;
            if let Some(uid) = key_str.strip_prefix("needs_reflect:") {
                let marker = serde_json::from_slice::<ReflectionMarker>(&value).unwrap_or_default();
                markers.push((uid.to_string(), marker));
            }
        }
        Ok(markers)
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

    async fn populate_missing_embeddings(&self, units: &mut [MemoryUnit]) {
        let Some(client) = self.arbitrator.get_llm_client() else {
            return;
        };

        let mut texts = Vec::new();
        let mut indices = Vec::new();
        for (index, unit) in units.iter().enumerate() {
            if unit.embedding.is_none() && !unit.content.trim().is_empty() {
                indices.push(index);
                texts.push(unit.content.clone());
            }
        }

        if texts.is_empty() {
            return;
        }

        let embeddings = match client.embed_batch(texts.clone()).await {
            Ok(response) if response.data.len() == indices.len() => response.data,
            _ => {
                let mut fallback = Vec::with_capacity(texts.len());
                for text in texts {
                    fallback.push(
                        client
                            .embed(&text)
                            .await
                            .map(|response| response.data)
                            .unwrap_or_default(),
                    );
                }
                fallback
            }
        };

        for (index, embedding) in indices.into_iter().zip(embeddings.into_iter()) {
            if !embedding.is_empty() {
                units[index].embedding = Some(embedding);
            }
        }
    }

    async fn reflect_on_units(
        &self,
        user_id: &str,
        stream_id: uuid::Uuid,
        source_units: Vec<MemoryUnit>,
    ) -> Result<usize> {
        if source_units.is_empty() {
            return Ok(0);
        }

        let topic_units = self
            .arbitrator
            .extract_topics(user_id, stream_id, source_units)
            .await?;

        if topic_units.is_empty() {
            return Ok(0);
        }

        let topic_count = topic_units.len();
        let jobs = topic_units
            .into_iter()
            .map(|topic| {
                let post_publish_edges = topic
                    .references
                    .iter()
                    .map(|source_id| {
                        GraphEdge::new(
                            topic.user_id.clone(),
                            topic.id,
                            *source_id,
                            RelationType::DerivedFrom,
                            1.0,
                        )
                    })
                    .collect::<Vec<_>>();
                PendingMaterializationJob::new(topic, post_publish_edges, None)
            })
            .collect::<Vec<_>>();
        self.enqueue_materialization_jobs(jobs)?;

        Ok(topic_count)
    }

    async fn fetch_l1_units_for_reflection_batch(
        &self,
        user_id: &str,
        min_transaction_time_micros: Option<i64>,
        min_event_id: Option<&str>,
        max_units: usize,
        max_tokens: usize,
    ) -> Result<(Vec<MemoryUnit>, usize, Option<(i64, String)>)> {
        if max_units == 0 {
            return Ok((Vec::new(), 0, None));
        }

        let store = self._kv.clone();
        let l1_index_prefix = format!("l1_idx:{}:", user_id).into_bytes();
        let strip_prefix = format!("l1_idx:{}:", user_id);
        let index_pairs = tokio::task::spawn_blocking({
            let store = store.clone();
            move || store.scan(&l1_index_prefix)
        })
        .await??;

        let cursor = min_transaction_time_micros.zip(min_event_id.map(str::to_string));
        if index_pairs.is_empty() {
            let prefix = format!("u:{}:unit:", user_id).into_bytes();
            let pairs = tokio::task::spawn_blocking({
                let store = store.clone();
                move || store.scan(&prefix)
            })
            .await??;

            let mut ordered_units: Vec<(String, i64, MemoryUnit)> = pairs
                .into_iter()
                .filter_map(|(_, value)| serde_json::from_slice::<MemoryUnit>(&value).ok())
                .filter(|unit| unit.level == 1 && Self::is_local_domain(&unit.domain))
                .filter_map(|unit| {
                    let id = unit.id.to_string();
                    let ts = unit.transaction_time.timestamp_micros();
                    let include = match &cursor {
                        Some((cursor_ts, cursor_id)) => {
                            ts > *cursor_ts || (ts == *cursor_ts && id >= *cursor_id)
                        }
                        None => true,
                    };
                    include.then_some((id, ts, unit))
                })
                .collect();
            ordered_units.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
            return Self::pack_reflection_batch_units(ordered_units, max_units, max_tokens);
        }

        let mut id_ts: Vec<(String, i64)> = index_pairs
            .into_iter()
            .filter_map(|(key, value)| {
                let key_str = String::from_utf8(key).ok()?;
                let id = key_str.strip_prefix(&strip_prefix)?.to_string();
                let ts = i64::from_le_bytes(value.as_slice().try_into().ok()?);
                let include = match &cursor {
                    Some((cursor_ts, cursor_id)) => {
                        ts > *cursor_ts || (ts == *cursor_ts && id >= *cursor_id)
                    }
                    None => true,
                };
                include.then_some((id, ts))
            })
            .collect();

        if id_ts.is_empty() {
            return Ok((Vec::new(), 0, None));
        }

        id_ts.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        if id_ts.len() > max_units.saturating_add(1) {
            id_ts.truncate(max_units.saturating_add(1));
        }

        let keys: Vec<String> = id_ts
            .iter()
            .map(|(id, _)| format!("u:{}:unit:{}", user_id, id))
            .collect();
        let values = tokio::task::spawn_blocking({
            let store = store.clone();
            let key_refs_owned: Vec<Vec<u8>> = keys.iter().map(|k| k.as_bytes().to_vec()).collect();
            move || {
                store.multi_get(
                    &key_refs_owned
                        .iter()
                        .map(|k| k.as_slice())
                        .collect::<Vec<_>>(),
                )
            }
        })
        .await??;

        let ordered_units: Vec<(String, i64, MemoryUnit)> = id_ts
            .into_iter()
            .zip(values.into_iter())
            .filter_map(|((id, ts), value)| {
                let unit =
                    value.and_then(|bytes| serde_json::from_slice::<MemoryUnit>(&bytes).ok())?;
                (unit.level == 1 && Self::is_local_domain(&unit.domain)).then_some((id, ts, unit))
            })
            .collect();
        Self::pack_reflection_batch_units(ordered_units, max_units, max_tokens)
    }

    fn pack_reflection_batch_units(
        mut ordered_units: Vec<(String, i64, MemoryUnit)>,
        max_units: usize,
        max_tokens: usize,
    ) -> Result<(Vec<MemoryUnit>, usize, Option<(i64, String)>)> {
        ordered_units.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

        let mut batch = Vec::new();
        let mut consumed_tokens = 0usize;
        let mut next_cursor = None;

        for (id, ts, unit) in ordered_units {
            let unit_tokens = count_tokens(&unit.content).max(1);
            let would_exceed_tokens = !batch.is_empty()
                && consumed_tokens.saturating_add(unit_tokens) > max_tokens.max(1);
            let would_exceed_units = batch.len() >= max_units;

            if would_exceed_tokens || would_exceed_units {
                next_cursor = Some((ts, id));
                break;
            }

            consumed_tokens = consumed_tokens.saturating_add(unit_tokens);
            batch.push(unit);
        }

        Ok((batch, consumed_tokens, next_cursor))
    }

    /// Prospective Reflection: Summarize recent L1 memories into L2 Topic memories.
    pub async fn reflect_on_session(&self, user_id: &str, stream_id: uuid::Uuid) -> Result<()> {
        let recent_l1 = self.fetch_recent_l1_units(user_id, 20).await?;
        let session_units: Vec<MemoryUnit> = recent_l1
            .into_iter()
            .filter(|u| u.stream_id == stream_id)
            .collect();

        self.reflect_on_units(user_id, stream_id, session_units)
            .await
            .map(|_| ())
    }

    /// User-window reflection: summarize a bounded recent L1 window, even across streams.
    pub async fn reflect_on_user_window(&self, user_id: &str, limit: usize) -> Result<usize> {
        self.reflect_on_user_window_since(user_id, None, limit)
            .await
    }

    /// User-window reflection with an optional lower transaction-time bound.
    pub async fn reflect_on_user_window_since(
        &self,
        user_id: &str,
        min_transaction_time_micros: Option<i64>,
        limit: usize,
    ) -> Result<usize> {
        let recent_l1 = match min_transaction_time_micros.filter(|ts| *ts > 0) {
            Some(min_ts) => {
                self.fetch_recent_l1_units_since(user_id, min_ts, limit.max(1))
                    .await?
            }
            None => self.fetch_recent_l1_units(user_id, limit.max(1)).await?,
        };
        let Some(stream_id) = recent_l1
            .iter()
            .max_by_key(|unit| unit.transaction_time)
            .map(|unit| unit.stream_id)
        else {
            return Ok(0);
        };

        self.reflect_on_units(user_id, stream_id, recent_l1).await
    }

    pub async fn reflect_on_user_window_batch(
        &self,
        user_id: &str,
        min_transaction_time_micros: Option<i64>,
        min_event_id: Option<&str>,
        max_units: usize,
        max_tokens: usize,
    ) -> Result<ReflectionBatchOutcome> {
        let (source_units, consumed_tokens, next_cursor) = self
            .fetch_l1_units_for_reflection_batch(
                user_id,
                min_transaction_time_micros,
                min_event_id,
                max_units.max(1),
                max_tokens.max(1),
            )
            .await?;

        let consumed_units = source_units.len();
        let Some(stream_id) = source_units
            .iter()
            .max_by_key(|unit| unit.transaction_time)
            .map(|unit| unit.stream_id)
        else {
            return Ok(ReflectionBatchOutcome::default());
        };

        let created_topics = self
            .reflect_on_units(user_id, stream_id, source_units)
            .await?;
        Ok(ReflectionBatchOutcome {
            created_topics,
            consumed_units,
            consumed_tokens,
            next_first_event_tx_micros: next_cursor.as_ref().map(|(ts, _)| *ts),
            next_first_event_id: next_cursor.map(|(_, id)| id),
        })
    }

    /// Retrospective Reflection: Apply feedback to the reranker and reinforce graph associations.
    pub async fn apply_reranker_feedback(
        &self,
        user_id: &str,
        cited_ids: Vec<String>,
        retrieved_ids: Vec<String>,
    ) -> Result<()> {
        self.reranker
            .apply_feedback(&self._kv, cited_ids.clone(), retrieved_ids)
            .await?;

        if cited_ids.len() >= 2 {
            self.reinforce_associations(user_id, cited_ids).await?;
        }

        Ok(())
    }

    /// Internal method to increase edge weights between memories that were useful together.
    async fn reinforce_associations(&self, user_id: &str, cited_ids: Vec<String>) -> Result<()> {
        let uid = user_id.to_string();

        let uuids: Vec<Uuid> = cited_ids
            .iter()
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

            engine
                .index
                .commit()
                .map_err(|e| anyhow::anyhow!("Tantivy commit failed: {}", e))?;
            engine
                ._kv
                .flush()
                .map_err(|e| anyhow::anyhow!("RocksDB flush failed: {}", e))?;

            if let Some(parent) = output_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    anyhow::anyhow!("Failed to create parent dir {:?}: {}", parent, e)
                })?;
            }

            let file = std::fs::File::create(&output_path).map_err(|e| {
                anyhow::anyhow!("Failed to create output file {:?}: {}", output_path, e)
            })?;
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

            tar.finish()
                .map_err(|e| anyhow::anyhow!("Tar finish failed: {}", e))?;
            Ok(())
        })
        .await?
    }

    fn append_dir_to_tar<W: std::io::Write>(
        &self,
        tar: &mut tar::Builder<W>,
        root: &PathBuf,
        dir_name: &str,
    ) -> Result<()> {
        let dir_path = root.join(dir_name);
        for entry in walkdir::WalkDir::new(&dir_path) {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    if e.io_error()
                        .map(|ioe| ioe.kind() == std::io::ErrorKind::NotFound)
                        .unwrap_or(false)
                    {
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
        tracing::info!(
            "Restoring snapshot from {:?} to {:?}",
            snapshot_path,
            target_dir
        );

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
        let org_id = unit.org_id.clone();
        let agent_id = unit.agent_id.clone();
        let stream_id = unit.stream_id;
        let content = unit.content.clone();
        let references = unit.references.clone();

        self.store_memory_units(vec![unit]).await?;

        // Handle Explicit Linking (Task Hierarchy)
        if !references.is_empty() {
            for parent_id in references {
                let edge = GraphEdge::new(
                    user_id.clone(),
                    unit_id,
                    parent_id,
                    RelationType::IsSubTaskOf,
                    1.0,
                );
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
            let cnt = content.clone();
            let org = org_id.clone();
            let agent = agent_id.clone();
            tokio::spawn(async move {
                let key = format!("planning:{}", unit_id);
                match engine
                    .auto_plan_goal(org, uid, agent, stream_id, unit_id, cnt, depth + 1)
                    .await
                {
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

    pub fn auto_plan_goal(
        &self,
        org_id: Option<String>,
        user_id: String,
        agent_id: Option<String>,
        stream_id: Uuid,
        goal_id: Uuid,
        goal_content: String,
        depth: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            tracing::info!("Auto-planning goal {} (depth {})", goal_id, depth);

            let milestones = self
                .arbitrator
                .decompose_goal(
                    org_id.as_deref(),
                    &user_id,
                    agent_id.as_deref(),
                    stream_id,
                    &goal_content,
                )
                .await?;

            if milestones.is_empty() {
                return Ok(());
            }

            let mut updated_milestones = Vec::new();
            for mut ms in milestones {
                ms.parent_id = Some(goal_id);
                self.store_l3_task(&ms).await?;
                updated_milestones.push(ms);
            }

            for ms in updated_milestones {
                let edge = GraphEdge::new(
                    ms.user_id.clone(),
                    ms.task_id,
                    goal_id,
                    RelationType::IsSubTaskOf,
                    1.0,
                );
                self.graph.add_edge(&edge).await?;
            }

            Ok(())
        })
    }

    pub async fn store_l3_task(&self, task: &memorose_common::L3Task) -> Result<()> {
        let key = format!("l3:task:{}:{}", task.user_id, task.task_id);
        let val = serde_json::to_vec(task)?;
        self._kv.put(key.as_bytes(), &val)?;
        Ok(())
    }

    pub async fn get_l3_task(
        &self,
        user_id: &str,
        task_id: Uuid,
    ) -> Result<Option<memorose_common::L3Task>> {
        let key = format!("l3:task:{}:{}", user_id, task_id);
        if let Some(val) = self._kv.get(key.as_bytes())? {
            let task: memorose_common::L3Task = serde_json::from_slice(&val)?;
            Ok(Some(task))
        } else {
            Ok(None)
        }
    }

    pub async fn list_l3_tasks(&self, user_id: &str) -> Result<Vec<memorose_common::L3Task>> {
        let prefix = format!("l3:task:{}:", user_id);
        let results = self._kv.scan(prefix.as_bytes())?;
        let mut tasks = Vec::new();
        for (_, val) in results {
            if let Ok(task) = serde_json::from_slice::<memorose_common::L3Task>(&val) {
                tasks.push(task);
            }
        }
        Ok(tasks)
    }

    /// Agent Action Driver: Get tasks that are Pending and have all dependencies Completed.
    pub async fn get_ready_l3_tasks(&self, user_id: &str) -> Result<Vec<memorose_common::L3Task>> {
        let all_tasks = self.list_l3_tasks(user_id).await?;

        // Build a map of task_id -> status for quick dependency checking
        let status_map: std::collections::HashMap<Uuid, memorose_common::TaskStatus> = all_tasks
            .iter()
            .map(|t| (t.task_id, t.status.clone()))
            .collect();

        let mut ready_tasks = Vec::new();
        for task in all_tasks {
            if task.status == memorose_common::TaskStatus::Pending {
                let mut all_deps_completed = true;
                for dep_id in &task.dependencies {
                    if let Some(dep_status) = status_map.get(dep_id) {
                        if *dep_status != memorose_common::TaskStatus::Completed {
                            all_deps_completed = false;
                            break;
                        }
                    } else {
                        // If a dependency is missing, we consider it blocked/not completed
                        all_deps_completed = false;
                        break;
                    }
                }

                if all_deps_completed {
                    ready_tasks.push(task);
                }
            }
        }
        Ok(ready_tasks)
    }

    pub fn schedule_share_backfill(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        domain: MemoryDomain,
    ) -> Result<()> {
        let scope_id = match domain {
            MemoryDomain::Organization => org_id.unwrap_or("_global").to_string(),
            _ => return Ok(()),
        };

        let status_key = Self::backfill_status_key(&domain, user_id, &scope_id);
        let pending = serde_json::json!({
            "status": "pending",
            "scheduled_at": chrono::Utc::now().to_rfc3339(),
            "org_id": org_id,
            "domain": domain.as_str()
        });
        self.system_kv()
            .put(status_key.as_bytes(), &serde_json::to_vec(&pending)?)?;

        let engine = self.clone();
        let user_id = user_id.to_string();
        let org_id = org_id.map(|value| value.to_string());
        tokio::spawn(async move {
            let result = engine
                .run_share_backfill(&user_id, org_id.as_deref(), domain.clone())
                .await;

            let payload = match result {
                Ok(projected) => serde_json::json!({
                    "status": "done",
                    "finished_at": chrono::Utc::now().to_rfc3339(),
                    "projected": projected,
                    "org_id": org_id,
                    "domain": domain.as_str()
                }),
                Err(error) => serde_json::json!({
                    "status": "failed",
                    "finished_at": chrono::Utc::now().to_rfc3339(),
                    "error": error.to_string(),
                    "org_id": org_id,
                    "domain": domain.as_str()
                }),
            };

            if let Err(error) = engine.system_kv().put(
                status_key.as_bytes(),
                &serde_json::to_vec(&payload).unwrap_or_default(),
            ) {
                tracing::warn!("Failed to update share backfill status: {:?}", error);
            }
        });

        Ok(())
    }

    async fn run_share_backfill(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        domain: MemoryDomain,
    ) -> Result<usize> {
        let prefix = format!("u:{}:unit:", user_id);
        let store = self._kv.clone();
        let prefix_bytes = prefix.into_bytes();
        let pairs = tokio::task::spawn_blocking(move || store.scan(&prefix_bytes)).await??;

        let native_units: Vec<MemoryUnit> = pairs
            .into_iter()
            .filter_map(|(_, val)| serde_json::from_slice::<MemoryUnit>(&val).ok())
            .filter(|unit| Self::is_local_domain(&unit.domain))
            .filter(|unit| unit.level <= 2)
            .filter(|unit| match domain {
                MemoryDomain::Organization => unit.org_id.as_deref() == org_id,
                _ => false,
            })
            .collect();

        let published = self
            .publish_native_shared_knowledge_for_domain(&native_units, Some(domain))
            .await?;
        Ok(published)
    }

    fn should_publish_to_organization(source: &MemoryUnit) -> bool {
        source.domain == MemoryDomain::User
            && source.level == 2
            && !source.content.trim().is_empty()
            && source.content != "LLM not available"
            && source.content != "No memories provided."
    }

    async fn load_organization_source_units(&self, source_ids: &[Uuid]) -> Result<Vec<MemoryUnit>> {
        let mut sources = Vec::new();

        for source_id in source_ids {
            let Some(source) = self.get_native_memory_unit_by_index(*source_id).await? else {
                continue;
            };
            if Self::should_publish_to_organization(&source) {
                sources.push(source);
            }
        }

        Ok(sources)
    }

    fn load_organization_knowledge(&self, id: Uuid) -> Result<Option<OrganizationKnowledgeRecord>> {
        let key = Self::organization_knowledge_key(id);
        match self.system_kv().get(key.as_bytes())? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            None => Ok(None),
        }
    }

    fn store_organization_knowledge(&self, record: &OrganizationKnowledgeRecord) -> Result<()> {
        let key = Self::organization_knowledge_key(record.id);
        self.system_kv()
            .put(key.as_bytes(), &serde_json::to_vec(record)?)
    }

    fn load_organization_membership(
        &self,
        source_id: Uuid,
    ) -> Result<Option<OrganizationKnowledgeMembershipRecord>> {
        let key = Self::organization_membership_source_key(source_id);
        match self.system_kv().get(key.as_bytes())? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            None => Ok(None),
        }
    }

    fn load_organization_topic_relation(
        &self,
        org_id: &str,
        topic_key: &str,
    ) -> Result<Option<OrganizationKnowledgeRelationRecord>> {
        let key = Self::organization_topic_relation_key(org_id, topic_key);
        match self.system_kv().get(key.as_bytes())? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            None => Ok(None),
        }
    }

    fn store_organization_relation(
        &self,
        relation: &OrganizationKnowledgeRelationRecord,
    ) -> Result<()> {
        let primary_key = Self::organization_relation_key(relation);
        let index_key = Self::organization_knowledge_relation_index_key(relation);
        let value = serde_json::to_vec(relation)?;
        self.system_kv().put(primary_key.as_bytes(), &value)?;
        self.system_kv().put(index_key.as_bytes(), &value)
    }

    fn store_organization_relations(
        &self,
        relations: &[OrganizationKnowledgeRelationRecord],
    ) -> Result<()> {
        for relation in relations {
            self.store_organization_relation(relation)?;
        }
        Ok(())
    }

    fn store_organization_membership(
        &self,
        membership: &OrganizationKnowledgeMembershipRecord,
    ) -> Result<()> {
        let primary_key = Self::organization_membership_source_key(membership.source_id);
        let index_key = Self::organization_membership_by_knowledge_key(membership);
        let value = serde_json::to_vec(membership)?;
        self.system_kv().put(primary_key.as_bytes(), &value)?;
        self.system_kv().put(index_key.as_bytes(), &value)
    }

    fn store_organization_memberships(
        &self,
        memberships: &[OrganizationKnowledgeMembershipRecord],
    ) -> Result<()> {
        for membership in memberships {
            self.store_organization_membership(membership)?;
        }
        Ok(())
    }

    fn delete_organization_membership(&self, source_id: Uuid) -> Result<()> {
        if let Some(bytes) = self
            .system_kv()
            .get(Self::organization_membership_source_key(source_id).as_bytes())?
        {
            if let Ok(membership) =
                serde_json::from_slice::<OrganizationKnowledgeMembershipRecord>(&bytes)
            {
                let index_key = Self::organization_membership_by_knowledge_key(&membership);
                self.system_kv().delete(index_key.as_bytes()).ok();
            }
        }
        self.system_kv()
            .delete(Self::organization_membership_source_key(source_id).as_bytes())
            .ok();
        Ok(())
    }

    fn delete_organization_relation_by_primary_key(&self, primary_key: &str) -> Result<()> {
        if let Some(bytes) = self.system_kv().get(primary_key.as_bytes())? {
            if let Ok(relation) =
                serde_json::from_slice::<OrganizationKnowledgeRelationRecord>(&bytes)
            {
                let index_key = Self::organization_knowledge_relation_index_key(&relation);
                self.system_kv().delete(index_key.as_bytes()).ok();
            }
        }
        self.system_kv().delete(primary_key.as_bytes()).ok();
        Ok(())
    }

    fn delete_organization_membership_or_relation_by_key(&self, key: &str) -> Result<()> {
        if key.starts_with("organization_knowledge_membership:source:") {
            let source_id = key
                .rsplit(':')
                .next()
                .and_then(|value| Uuid::parse_str(value).ok());
            if let Some(source_id) = source_id {
                self.delete_organization_membership(source_id)?;
            } else {
                self.system_kv().delete(key.as_bytes()).ok();
            }
            return Ok(());
        }

        self.delete_organization_relation_by_primary_key(key)
    }

    fn store_organization_contribution(
        &self,
        contribution: &OrganizationKnowledgeContributionRecord,
    ) -> Result<()> {
        let key = Self::organization_knowledge_contribution_key(
            contribution.knowledge_id,
            contribution.source_id,
        );
        self.system_kv()
            .put(key.as_bytes(), &serde_json::to_vec(contribution)?)
    }

    fn store_organization_contributions(
        &self,
        contributions: &[OrganizationKnowledgeContributionRecord],
    ) -> Result<()> {
        for contribution in contributions {
            self.store_organization_contribution(contribution)?;
        }
        Ok(())
    }

    fn submit_organization_contribution_candidates(
        &self,
        candidates: &[OrganizationKnowledgeContributionRecord],
    ) -> Result<()> {
        self.store_organization_contributions(candidates)
    }

    fn approve_organization_contribution_candidates(
        &self,
        candidates: &[OrganizationKnowledgeContributionRecord],
        activated_at: DateTime<Utc>,
    ) -> Result<Vec<OrganizationKnowledgeContributionRecord>> {
        let approved = Self::activate_organization_contribution_records(candidates, activated_at);
        self.store_organization_contributions(&approved)?;
        if let Some(first) = approved.first() {
            self.increment_organization_metric_counter(
                &first.org_id,
                "auto_approved_total",
                approved.len(),
            )?;
        }
        Ok(approved)
    }

    async fn list_organization_contributions(
        &self,
        knowledge_id: Uuid,
    ) -> Result<Vec<OrganizationKnowledgeContributionRecord>> {
        let prefix = Self::organization_knowledge_contribution_prefix(knowledge_id);
        let system_kv = self.system_kv();
        let pairs =
            tokio::task::spawn_blocking(move || system_kv.scan(prefix.as_bytes())).await??;

        Ok(pairs
            .into_iter()
            .filter_map(|(_, val)| {
                serde_json::from_slice::<OrganizationKnowledgeContributionRecord>(&val).ok()
            })
            .collect())
    }

    async fn list_organization_memberships(
        &self,
        knowledge_id: Uuid,
    ) -> Result<Vec<OrganizationKnowledgeMembershipRecord>> {
        let prefix = Self::organization_membership_by_knowledge_prefix(knowledge_id);
        let system_kv = self.system_kv();
        let pairs =
            tokio::task::spawn_blocking(move || system_kv.scan(prefix.as_bytes())).await??;

        let mut memberships = pairs
            .into_iter()
            .filter_map(|(_, val)| {
                serde_json::from_slice::<OrganizationKnowledgeMembershipRecord>(&val).ok()
            })
            .collect::<Vec<_>>();
        memberships.sort_by(|left, right| left.source_id.cmp(&right.source_id));
        Ok(memberships)
    }

    async fn resolve_organization_record_source_ids(
        &self,
        record: &OrganizationKnowledgeRecord,
    ) -> Result<Vec<Uuid>> {
        let mut source_ids = self
            .list_organization_memberships(record.id)
            .await?
            .into_iter()
            .map(|membership| membership.source_id)
            .collect::<Vec<_>>();
        source_ids.sort();
        source_ids.dedup();
        Ok(source_ids)
    }

    async fn delete_organization_contributions(&self, knowledge_id: Uuid) -> Result<()> {
        let prefix = Self::organization_knowledge_contribution_prefix(knowledge_id);
        let system_kv = self.system_kv();
        let pairs =
            tokio::task::spawn_blocking(move || system_kv.scan(prefix.as_bytes())).await??;
        for (key, _) in pairs {
            self.system_kv().delete(&key).ok();
        }
        Ok(())
    }

    async fn delete_organization_memberships(&self, knowledge_id: Uuid) -> Result<()> {
        for membership in self.list_organization_memberships(knowledge_id).await? {
            self.delete_organization_membership(membership.source_id)?;
        }
        Ok(())
    }

    async fn load_organization_membership_sources(
        &self,
        knowledge_id: Uuid,
    ) -> Result<Vec<(OrganizationKnowledgeMembershipRecord, MemoryUnit)>> {
        let mut sources = Vec::new();
        for membership in self.list_organization_memberships(knowledge_id).await? {
            let Some(source_unit) = self
                .get_native_memory_unit_by_index(membership.source_id)
                .await?
            else {
                continue;
            };
            sources.push((membership, source_unit));
        }
        Ok(sources)
    }

    fn load_organization_contribution(
        &self,
        knowledge_id: Uuid,
        source_id: Uuid,
    ) -> Result<Option<OrganizationKnowledgeContributionRecord>> {
        let key = Self::organization_knowledge_contribution_key(knowledge_id, source_id);
        match self.system_kv().get(key.as_bytes())? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            None => Ok(None),
        }
    }

    fn revoke_organization_contribution(&self, knowledge_id: Uuid, source_id: Uuid) -> Result<()> {
        let Some(mut contribution) =
            self.load_organization_contribution(knowledge_id, source_id)?
        else {
            return Ok(());
        };

        contribution.status = OrganizationKnowledgeContributionStatus::Revoked;
        contribution.updated_at = Utc::now();
        contribution.revoked_at = Some(contribution.updated_at);
        self.store_organization_contribution(&contribution)?;
        self.increment_organization_metric_counter(&contribution.org_id, "revoke_total", 1)
    }

    async fn list_organization_relations_for_knowledge(
        &self,
        knowledge_id: Uuid,
    ) -> Result<Vec<OrganizationKnowledgeRelationRecord>> {
        let prefix = Self::organization_knowledge_relation_index_prefix(knowledge_id);
        let system_kv = self.system_kv();
        let indexed_pairs =
            tokio::task::spawn_blocking(move || system_kv.scan(prefix.as_bytes())).await??;

        let mut indexed_relations = indexed_pairs
            .into_iter()
            .filter_map(|(_, val)| {
                serde_json::from_slice::<OrganizationKnowledgeRelationRecord>(&val).ok()
            })
            .filter(|relation| {
                matches!(
                    relation.relation,
                    OrganizationKnowledgeRelationKind::TopicAlias { .. }
                )
            })
            .collect::<Vec<_>>();
        if !indexed_relations.is_empty() {
            indexed_relations.sort_by(|left, right| {
                Self::organization_relation_key(left).cmp(&Self::organization_relation_key(right))
            });
            return Ok(indexed_relations);
        }

        let system_kv = self.system_kv();
        let pairs = tokio::task::spawn_blocking(move || {
            system_kv.scan(b"organization_knowledge_relation:")
        })
        .await??;

        let mut relations = pairs
            .into_iter()
            .filter_map(|(_, val)| {
                serde_json::from_slice::<OrganizationKnowledgeRelationRecord>(&val).ok()
            })
            .filter(|relation| relation.knowledge_id == knowledge_id)
            .filter(|relation| {
                matches!(
                    relation.relation,
                    OrganizationKnowledgeRelationKind::TopicAlias { .. }
                )
            })
            .collect::<Vec<_>>();
        relations.sort_by(|left, right| {
            Self::organization_relation_key(left).cmp(&Self::organization_relation_key(right))
        });
        Ok(relations)
    }

    async fn cleanup_stale_organization_source_relations(&self) -> Result<usize> {
        let system_kv = self.system_kv();
        let primary_pairs = tokio::task::spawn_blocking(move || {
            system_kv.scan(b"organization_knowledge_relation:source:")
        })
        .await??;
        let system_kv = self.system_kv();
        let indexed_pairs = tokio::task::spawn_blocking(move || {
            system_kv.scan(b"organization_knowledge_relation_by_knowledge:")
        })
        .await??;

        let mut removed = 0usize;
        for (key, _) in primary_pairs {
            self.system_kv().delete(&key).ok();
            removed += 1;
        }
        for (key, value) in indexed_pairs {
            let is_stale_source_index = String::from_utf8_lossy(&key).contains(":source:");
            let is_source_relation =
                serde_json::from_slice::<OrganizationKnowledgeRelationRecord>(&value)
                    .ok()
                    .map(|relation| {
                        matches!(
                            relation.relation,
                            OrganizationKnowledgeRelationKind::Source { .. }
                        )
                    })
                    .unwrap_or(false);
            if is_stale_source_index || is_source_relation {
                self.system_kv().delete(&key).ok();
                removed += 1;
            }
        }

        Ok(removed)
    }

    fn select_retained_organization_knowledge(
        existing_records: &[OrganizationKnowledgeRecord],
    ) -> Option<OrganizationKnowledgeRecord> {
        let mut records = existing_records.to_vec();
        records.sort_by(|left, right| {
            right
                .importance
                .partial_cmp(&left.importance)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.created_at.cmp(&right.created_at))
                .then_with(|| left.id.cmp(&right.id))
        });
        records.into_iter().next()
    }

    async fn build_organization_knowledge_mutation(
        &self,
        source: &MemoryUnit,
        target_domain: MemoryDomain,
    ) -> Result<Option<OrganizationKnowledgeMutation>> {
        let mut stale_relation_keys = Vec::new();

        let mutation = match target_domain {
            MemoryDomain::Organization => {
                if !Self::should_publish_to_organization(source) {
                    None
                } else {
                    let Some(org_id) = source.org_id.as_deref() else {
                        return Ok(None);
                    };
                    if let Some(existing_membership) =
                        self.load_organization_membership(source.id)?
                    {
                        if existing_membership.org_id == org_id
                            && self
                                .load_organization_knowledge(existing_membership.knowledge_id)?
                                .is_some()
                        {
                            return Ok(None);
                        }
                        stale_relation_keys
                            .push(Self::organization_membership_source_key(source.id));
                    }

                    let source_topic_candidates =
                        Self::organization_source_topic_candidates(source);
                    if source_topic_candidates.is_empty() {
                        return Ok(None);
                    }

                    let mut existing_records_by_id = HashMap::new();
                    for (_, topic_key) in &source_topic_candidates {
                        if let Some(existing_relation) =
                            self.load_organization_topic_relation(org_id, topic_key)?
                        {
                            if let Some(existing_record) =
                                self.load_organization_knowledge(existing_relation.knowledge_id)?
                            {
                                if existing_record.org_id == org_id {
                                    existing_records_by_id
                                        .entry(existing_record.id)
                                        .or_insert(existing_record);
                                } else {
                                    stale_relation_keys
                                        .push(Self::organization_relation_key(&existing_relation));
                                }
                            } else {
                                stale_relation_keys
                                    .push(Self::organization_relation_key(&existing_relation));
                            }
                        }
                    }

                    let existing_records = existing_records_by_id
                        .into_values()
                        .collect::<Vec<OrganizationKnowledgeRecord>>();
                    let mut source_ids = Vec::new();
                    for record in &existing_records {
                        source_ids
                            .extend(self.resolve_organization_record_source_ids(record).await?);
                    }
                    source_ids.sort();
                    source_ids.dedup();
                    if !source_ids.contains(&source.id) {
                        source_ids.push(source.id);
                    }

                    let sources = self.load_organization_source_units(&source_ids).await?;
                    let Some(topic) = Self::select_organization_topic(&sources) else {
                        return Ok(None);
                    };
                    let retained_record =
                        Self::select_retained_organization_knowledge(&existing_records);
                    let obsolete_records = existing_records
                        .iter()
                        .filter(|record| {
                            Some(record.id) != retained_record.as_ref().map(|record| record.id)
                        })
                        .cloned()
                        .collect::<Vec<_>>();

                    let Some(record) = self.compose_organization_knowledge_record(
                        org_id,
                        &sources,
                        retained_record.as_ref(),
                        &topic,
                    ) else {
                        return Ok(None);
                    };
                    let unit = Self::materialize_organization_read_view(&record);

                    let candidate_contribution_records =
                        Self::organization_candidate_contribution_records(
                            org_id,
                            record.id,
                            &sources,
                            record.updated_at,
                        );
                    let approved_contribution_records =
                        Self::activate_organization_contribution_records(
                            &candidate_contribution_records,
                            record.updated_at,
                        );
                    let memberships = Self::organization_memberships_from_contributions(
                        &approved_contribution_records,
                    );
                    let topic_relations = Self::organization_topic_relations(
                        org_id,
                        record.id,
                        &topic,
                        record.updated_at,
                    );

                    let mut previous_relation_keys = Vec::new();
                    for existing_record in &existing_records {
                        previous_relation_keys.extend(
                            self.list_organization_relations_for_knowledge(existing_record.id)
                                .await?
                                .into_iter()
                                .map(|relation| Self::organization_relation_key(&relation)),
                        );
                    }
                    let membership_keys = memberships
                        .iter()
                        .map(|membership| {
                            Self::organization_membership_source_key(membership.source_id)
                        })
                        .collect::<Vec<_>>();
                    let topic_relation_keys = topic_relations
                        .iter()
                        .map(Self::organization_relation_key)
                        .collect::<Vec<_>>();
                    for stale_relation_key in previous_relation_keys {
                        if !membership_keys.contains(&stale_relation_key)
                            && !topic_relation_keys.contains(&stale_relation_key)
                        {
                            stale_relation_keys.push(stale_relation_key);
                        }
                    }

                    Some(OrganizationKnowledgeMutation {
                        topic_relations,
                        candidate_contribution_records,
                        stale_relation_keys,
                        obsolete_records,
                        record,
                        unit,
                    })
                }
            }
            _ => None,
        };

        Ok(mutation)
    }

    async fn find_org_knowledge_records_for_contributor(
        &self,
        user_id: &str,
        org_id: &str,
    ) -> Result<Vec<(OrganizationKnowledgeRecord, Vec<Uuid>)>> {
        let prefix = format!("u:{}:unit:", user_id);
        let store = self._kv.clone();
        let prefix_bytes = prefix.into_bytes();
        let pairs = tokio::task::spawn_blocking(move || store.scan(&prefix_bytes)).await??;

        let source_units: Vec<MemoryUnit> = pairs
            .into_iter()
            .filter_map(|(_, val)| serde_json::from_slice::<MemoryUnit>(&val).ok())
            .collect();

        let mut knowledge_by_id: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
        for source_unit in source_units {
            if source_unit.org_id.as_deref() != Some(org_id)
                || !Self::should_publish_to_organization(&source_unit)
            {
                continue;
            }

            let Some(membership) = self.load_organization_membership(source_unit.id)? else {
                continue;
            };
            if membership.org_id != org_id {
                continue;
            }
            knowledge_by_id
                .entry(membership.knowledge_id)
                .or_default()
                .push(source_unit.id);
        }

        let mut knowledge_records = Vec::new();
        for (knowledge_id, source_ids) in knowledge_by_id {
            let Some(record) = self.load_organization_knowledge(knowledge_id)? else {
                continue;
            };
            if record.org_id == org_id {
                knowledge_records.push((record, source_ids));
            }
        }

        Ok(knowledge_records)
    }

    async fn delete_organization_knowledge_records(
        &self,
        records: Vec<OrganizationKnowledgeRecord>,
    ) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }

        for record in &records {
            self.system_kv()
                .delete(Self::organization_knowledge_key(record.id).as_bytes())
                .ok();
            for relation in self
                .list_organization_relations_for_knowledge(record.id)
                .await?
            {
                self.delete_organization_relation_by_primary_key(&Self::organization_relation_key(
                    &relation,
                ))
                .ok();
            }
            self.delete_organization_memberships(record.id).await?;
        }

        for record in &records {
            self.delete_organization_contributions(record.id).await?;
            let unit = Self::materialize_organization_read_view(record);
            self.delete_materialized_organization_view_storage(&unit)
                .await?;
        }

        Ok(records.len())
    }

    async fn list_persisted_organization_read_view_units(
        &self,
    ) -> Result<Vec<(Vec<u8>, MemoryUnit)>> {
        let kv = self._kv.clone();
        let pairs = tokio::task::spawn_blocking(move || kv.scan(b"u:")).await??;

        Ok(pairs
            .into_iter()
            .filter(|(key, _)| key.windows(6).any(|window| window == b":unit:"))
            .filter_map(|(key, val)| {
                serde_json::from_slice::<MemoryUnit>(&val)
                    .ok()
                    .map(|unit| (key, unit))
            })
            .filter(|(_, unit)| unit.domain == MemoryDomain::Organization)
            .collect())
    }

    async fn delete_memory_unit_storage_by_key(
        &self,
        unit_key: Vec<u8>,
        unit_id: Uuid,
    ) -> Result<()> {
        let kv = self._kv.clone();
        let index_key = format!("idx:unit:{}", unit_id);
        let hooks_key = Self::materialization_post_publish_key(unit_id);

        tokio::task::spawn_blocking(move || {
            kv.delete(&unit_key)?;
            kv.delete(index_key.as_bytes()).ok();
            kv.delete(hooks_key.as_bytes()).ok();
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        if let Err(error) = self
            .vector
            .delete_by_id("memories", &unit_id.to_string())
            .await
        {
            tracing::warn!(
                "Failed to delete materialized unit {} from vector store: {:?}",
                unit_id,
                error
            );
        }

        let index = self.index.clone();
        let id = unit_id.to_string();
        tokio::task::spawn_blocking(move || {
            index.delete_unit(&id)?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        Ok(())
    }

    async fn delete_materialized_organization_view_storage(&self, unit: &MemoryUnit) -> Result<()> {
        let unit_key = format!("u:{}:unit:{}", unit.user_id, unit.id).into_bytes();
        self.delete_memory_unit_storage_by_key(unit_key, unit.id)
            .await
    }

    async fn upsert_organization_knowledge(
        &self,
        record: OrganizationKnowledgeRecord,
        unit: MemoryUnit,
    ) -> Result<()> {
        self.store_organization_knowledge(&record)?;
        self.delete_materialized_organization_view_storage(&unit)
            .await
    }

    async fn publish_organization_knowledge(
        &self,
        record: OrganizationKnowledgeRecord,
        unit: MemoryUnit,
        candidate_contribution_records: Vec<OrganizationKnowledgeContributionRecord>,
        topic_relations: Vec<OrganizationKnowledgeRelationRecord>,
        publication_kind: OrganizationPublicationKind,
    ) -> Result<Vec<OrganizationKnowledgeMembershipRecord>> {
        let knowledge_id = record.id;
        let activated_at = record.updated_at;
        let org_id = record.org_id.clone();
        let existing_revoked_contributions = self
            .list_organization_contributions(knowledge_id)
            .await?
            .into_iter()
            .filter(|contribution| {
                matches!(
                    contribution.status,
                    OrganizationKnowledgeContributionStatus::Revoked
                )
            })
            .collect::<Vec<_>>();

        self.delete_organization_contributions(knowledge_id).await?;
        self.submit_organization_contribution_candidates(&candidate_contribution_records)?;
        let approved_contribution_records = self.approve_organization_contribution_candidates(
            &candidate_contribution_records,
            activated_at,
        )?;
        let active_source_ids = approved_contribution_records
            .iter()
            .map(|contribution| contribution.source_id)
            .collect::<HashSet<_>>();
        let retained_revoked_contributions = existing_revoked_contributions
            .into_iter()
            .filter(|contribution| !active_source_ids.contains(&contribution.source_id))
            .collect::<Vec<_>>();
        if !retained_revoked_contributions.is_empty() {
            self.store_organization_contributions(&retained_revoked_contributions)?;
        }
        let memberships =
            Self::organization_memberships_from_contributions(&approved_contribution_records);

        self.upsert_organization_knowledge(record, unit).await?;
        self.delete_organization_memberships(knowledge_id).await?;
        self.store_organization_memberships(&memberships)?;
        self.store_organization_relations(&topic_relations)?;
        self.increment_organization_metric_counter(&org_id, "auto_publish_total", 1)?;
        if matches!(publication_kind, OrganizationPublicationKind::Rebuild) {
            self.increment_organization_metric_counter(&org_id, "rebuild_total", 1)?;
        }
        if candidate_contribution_records.len() > 1 {
            self.increment_organization_metric_counter(&org_id, "merged_publication_total", 1)?;
        }

        Ok(memberships)
    }

    async fn load_reconciled_organization_source_units(
        &self,
        org_id: &str,
        source_ids: &[Uuid],
    ) -> Result<Vec<MemoryUnit>> {
        let mut sources = Vec::new();

        for source_id in source_ids {
            let Some(source) = self.get_native_memory_unit_by_index(*source_id).await? else {
                continue;
            };
            if source.org_id.as_deref() != Some(org_id)
                || !Self::should_publish_to_organization(&source)
            {
                continue;
            }

            let policy = self.get_org_share_policy(&source.user_id, org_id)?;
            if policy.contribute {
                sources.push(source);
            }
        }

        Ok(sources)
    }

    fn organization_record_matches_reconciled_state(
        existing: &OrganizationKnowledgeRecord,
        reconciled: &OrganizationKnowledgeRecord,
    ) -> bool {
        existing.id == reconciled.id
            && existing.org_id == reconciled.org_id
            && existing.topic_label == reconciled.topic_label
            && existing.topic_alias_keys == reconciled.topic_alias_keys
            && existing.memory_type == reconciled.memory_type
            && existing.content == reconciled.content
            && existing.embedding == reconciled.embedding
            && existing.keywords == reconciled.keywords
            && existing.importance == reconciled.importance
            && existing.valid_time == reconciled.valid_time
            && existing.created_at == reconciled.created_at
    }

    fn reconcile_active_organization_contributions(
        org_id: &str,
        knowledge_id: Uuid,
        sources: &[MemoryUnit],
        existing_contributions: &[OrganizationKnowledgeContributionRecord],
        reconciled_at: DateTime<Utc>,
        keep_existing_timestamps: bool,
    ) -> Vec<OrganizationKnowledgeContributionRecord> {
        let mut existing_by_source: HashMap<Uuid, OrganizationKnowledgeContributionRecord> =
            HashMap::new();
        for contribution in existing_contributions {
            if matches!(
                contribution.status,
                OrganizationKnowledgeContributionStatus::Revoked
            ) {
                continue;
            }
            existing_by_source
                .entry(contribution.source_id)
                .or_insert_with(|| contribution.clone());
        }

        sources
            .iter()
            .map(|source| {
                if let Some(existing) = existing_by_source.get(&source.id) {
                    let mut active = existing.clone();
                    active.org_id = org_id.to_string();
                    active.knowledge_id = knowledge_id;
                    active.source_id = source.id;
                    active.contributor_user_id = source.user_id.clone();
                    active.status = OrganizationKnowledgeContributionStatus::Active;
                    active.candidate_at = active.candidate_at.or(Some(reconciled_at));
                    active.activated_at = active
                        .activated_at
                        .or(active.candidate_at)
                        .or(Some(reconciled_at));
                    active.approval_mode = Some(OrganizationKnowledgeApprovalMode::Auto);
                    active.approved_by = Some("system:auto_publish".to_string());
                    if !keep_existing_timestamps
                        || !matches!(
                            existing.status,
                            OrganizationKnowledgeContributionStatus::Active
                        )
                    {
                        active.updated_at = reconciled_at;
                    }
                    active.revoked_at = None;
                    active
                } else {
                    let candidate = OrganizationKnowledgeContributionRecord {
                        org_id: org_id.to_string(),
                        knowledge_id,
                        source_id: source.id,
                        contributor_user_id: source.user_id.clone(),
                        status: OrganizationKnowledgeContributionStatus::Candidate,
                        candidate_at: Some(reconciled_at),
                        activated_at: None,
                        approval_mode: None,
                        approved_by: None,
                        updated_at: reconciled_at,
                        revoked_at: None,
                    };
                    Self::activate_organization_contribution_records(&[candidate], reconciled_at)
                        .into_iter()
                        .next()
                        .expect("expected active contribution")
                }
            })
            .collect()
    }

    async fn reconcile_organization_record(
        &self,
        record: OrganizationKnowledgeRecord,
    ) -> Result<bool> {
        let existing_contributions = self.list_organization_contributions(record.id).await?;
        let active_source_ids = existing_contributions
            .iter()
            .filter(|contribution| {
                matches!(
                    contribution.status,
                    OrganizationKnowledgeContributionStatus::Active
                        | OrganizationKnowledgeContributionStatus::Candidate
                )
            })
            .map(|contribution| contribution.source_id)
            .collect::<Vec<_>>();

        let sources = self
            .load_reconciled_organization_source_units(&record.org_id, &active_source_ids)
            .await?;

        if sources.is_empty() {
            self.delete_organization_knowledge_records(vec![record])
                .await?;
            return Ok(false);
        }

        let Some(topic) = Self::select_organization_topic(&sources) else {
            self.delete_organization_knowledge_records(vec![record])
                .await?;
            return Ok(false);
        };

        let mut reconciled_record = self
            .compose_organization_knowledge_record(&record.org_id, &sources, Some(&record), &topic)
            .ok_or_else(|| anyhow::anyhow!("failed to reconcile organization knowledge"))?;
        let record_unchanged =
            Self::organization_record_matches_reconciled_state(&record, &reconciled_record);
        if record_unchanged {
            reconciled_record.updated_at = record.updated_at;
        }

        let active_contributions = Self::reconcile_active_organization_contributions(
            &record.org_id,
            record.id,
            &sources,
            &existing_contributions,
            reconciled_record.updated_at,
            record_unchanged,
        );
        let active_source_ids = active_contributions
            .iter()
            .map(|contribution| contribution.source_id)
            .collect::<HashSet<_>>();
        let mut contributions_to_store = active_contributions;
        contributions_to_store.extend(existing_contributions.into_iter().filter(|contribution| {
            matches!(
                contribution.status,
                OrganizationKnowledgeContributionStatus::Revoked
            ) && !active_source_ids.contains(&contribution.source_id)
        }));

        let memberships =
            Self::organization_memberships_from_contributions(&contributions_to_store);
        let topic_relations = Self::organization_topic_relations(
            &record.org_id,
            record.id,
            &topic,
            reconciled_record.updated_at,
        );
        let previous_relation_keys = self
            .list_organization_relations_for_knowledge(record.id)
            .await?
            .into_iter()
            .map(|relation| Self::organization_relation_key(&relation))
            .collect::<Vec<_>>();
        let retained_relation_keys = topic_relations
            .iter()
            .map(Self::organization_relation_key)
            .collect::<HashSet<_>>();

        self.store_organization_knowledge(&reconciled_record)?;
        self.delete_organization_contributions(record.id).await?;
        self.delete_organization_memberships(record.id).await?;
        self.store_organization_contributions(&contributions_to_store)?;
        self.store_organization_memberships(&memberships)?;
        self.store_organization_relations(&topic_relations)?;
        for relation_key in previous_relation_keys {
            if !retained_relation_keys.contains(&relation_key) {
                self.delete_organization_relation_by_primary_key(&relation_key)
                    .ok();
            }
        }

        let read_view = Self::materialize_organization_read_view(&reconciled_record);
        self.delete_materialized_organization_view_storage(&read_view)
            .await?;

        Ok(true)
    }

    async fn reconcile_organization_storage(
        &self,
    ) -> Result<OrganizationStorageReconciliationStats> {
        let mut stats = OrganizationStorageReconciliationStats::default();
        stats.removed_stale_source_relations =
            self.cleanup_stale_organization_source_relations().await?;

        for (unit_key, unit) in self.list_persisted_organization_read_view_units().await? {
            self.delete_memory_unit_storage_by_key(unit_key, unit.id)
                .await?;
            stats.removed_persisted_views += 1;
        }

        for record in self.list_organization_knowledge_records(None, None).await? {
            if self.reconcile_organization_record(record).await? {
                stats.reconciled_records += 1;
            } else {
                stats.removed_records += 1;
            }
        }

        Ok(stats)
    }

    async fn publish_native_shared_knowledge(&self, units: &[MemoryUnit]) -> Result<usize> {
        self.publish_native_shared_knowledge_for_domain(units, None)
            .await
    }

    async fn publish_native_shared_knowledge_for_domain(
        &self,
        units: &[MemoryUnit],
        only_domain: Option<MemoryDomain>,
    ) -> Result<usize> {
        let mut published_count = 0;

        for unit in units {
            if unit.domain != MemoryDomain::User || unit.level != 2 {
                continue;
            }

            if let Some(org_id) = unit.org_id.as_deref() {
                let org_policy = self.get_org_share_policy(&unit.user_id, org_id)?;
                if org_policy.contribute
                    && only_domain
                        .as_ref()
                        .map(|domain| domain == &MemoryDomain::Organization)
                        .unwrap_or(true)
                {
                    if let Some(mutation) = self
                        .build_organization_knowledge_mutation(unit, MemoryDomain::Organization)
                        .await?
                    {
                        if !mutation.obsolete_records.is_empty() {
                            self.delete_organization_knowledge_records(mutation.obsolete_records)
                                .await?;
                        }
                        self.publish_organization_knowledge(
                            mutation.record,
                            mutation.unit,
                            mutation.candidate_contribution_records,
                            mutation.topic_relations,
                            OrganizationPublicationKind::New,
                        )
                        .await?;
                        for stale_relation_key in mutation.stale_relation_keys {
                            self.delete_organization_membership_or_relation_by_key(
                                &stale_relation_key,
                            )
                            .ok();
                        }
                        published_count += 1;
                    }
                }
            }
        }

        Ok(published_count)
    }

    async fn write_materialized_search_storage(&self, unit: &MemoryUnit) -> Result<()> {
        if unit.embedding.is_some() {
            self.vector.ensure_table("memories").await?;
            self.vector
                .delete_by_id("memories", &unit.id.to_string())
                .await?;
            self.vector.add("memories", vec![unit.clone()]).await?;
        }

        let index = self.index.clone();
        let unit_for_index = unit.clone();
        let id = unit.id.to_string();
        tokio::task::spawn_blocking(move || {
            index.delete_unit(&id)?;
            index.index_unit(&unit_for_index)?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        Ok(())
    }

    async fn write_published_memory_unit_metadata(&self, unit: &MemoryUnit) -> Result<()> {
        let kv = self._kv.clone();
        let unit_to_store = unit.clone();
        tokio::task::spawn_blocking(move || {
            let mut batch = rocksdb::WriteBatch::default();
            let key = format!("u:{}:unit:{}", unit_to_store.user_id, unit_to_store.id);
            let idx_key = format!("idx:unit:{}", unit_to_store.id);
            batch.put(key.as_bytes(), &serde_json::to_vec(&unit_to_store)?);
            batch.put(idx_key.as_bytes(), unit_to_store.user_id.as_bytes());

            if unit_to_store.level == 1 && Self::is_local_domain(&unit_to_store.domain) {
                let l1_key = format!("l1_idx:{}:{}", unit_to_store.user_id, unit_to_store.id);
                batch.put(
                    l1_key.as_bytes(),
                    unit_to_store.transaction_time.timestamp_micros().to_le_bytes(),
                );
            }

            kv.write_batch(batch)?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        if unit.level == 1 && Self::is_local_domain(&unit.domain) {
            let tx_micros = unit.transaction_time.timestamp_micros();
            self.bump_reflection_marker_with_window(
                &unit.user_id,
                1,
                count_tokens(&unit.content),
                Some(tx_micros),
                Some(tx_micros),
                Some(unit.id.to_string()),
            )?;
        }

        Ok(())
    }

    pub(crate) async fn publish_materialized_memory_unit(&self, unit: &MemoryUnit) -> Result<()> {
        Self::validate_materialized_units(std::slice::from_ref(unit))?;
        self.write_materialized_search_storage(unit).await?;
        self.write_published_memory_unit_metadata(unit).await
    }

    pub(crate) async fn run_published_memory_unit_side_effects(
        &self,
        unit: &MemoryUnit,
    ) -> Result<()> {
        if !Self::is_local_domain(&unit.domain) || !self.is_visible_memory_unit(unit)? {
            return Ok(());
        }

        if let Err(error) = self.auto_link_memory(unit).await {
            tracing::error!("Auto-linking failed for unit {}: {:?}", unit.id, error);
        }
        if let Err(error) = self.semantic_link_memory(unit).await {
            tracing::error!("Semantic linking failed for unit {}: {:?}", unit.id, error);
        }

        self.publish_native_shared_knowledge(std::slice::from_ref(unit))
            .await?;
        Ok(())
    }

    pub async fn store_memory_units(&self, units: Vec<MemoryUnit>) -> Result<()> {
        self.store_memory_units_internal(units, true).await
    }

    async fn store_memory_units_internal(
        &self,
        units: Vec<MemoryUnit>,
        run_reconciliation: bool,
    ) -> Result<()> {
        if units.is_empty() {
            return Ok(());
        }

        Self::validate_materialized_units(&units)?;

        // 1. Store Metadata in KV (user-prefixed keys + global index)
        let kv = self._kv.clone();
        let mut kv_batch = rocksdb::WriteBatch::default();
        let mut reflection_deltas: HashMap<String, (usize, usize, i64, i64, String)> =
            HashMap::new();
        for unit in &units {
            let key = format!("u:{}:unit:{}", unit.user_id, unit.id);
            let val = serde_json::to_vec(unit)?;
            kv_batch.put(key.as_bytes(), &val);

            // Global index for dashboard lookups
            let idx_key = format!("idx:unit:{}", unit.id);
            kv_batch.put(idx_key.as_bytes(), unit.user_id.as_bytes());

            if unit.level == 1 && Self::is_local_domain(&unit.domain) {
                let tx_micros = unit.transaction_time.timestamp_micros();
                let entry = reflection_deltas.entry(unit.user_id.clone()).or_insert((
                    0,
                    0,
                    tx_micros,
                    tx_micros,
                    unit.id.to_string(),
                ));
                entry.0 = entry.0.saturating_add(1);
                entry.1 = entry.1.saturating_add(count_tokens(&unit.content));
                if tx_micros < entry.2 || (tx_micros == entry.2 && unit.id.to_string() < entry.4) {
                    entry.2 = tx_micros;
                    entry.4 = unit.id.to_string();
                }
                entry.3 = entry.3.max(tx_micros);
            }
        }

        tokio::task::spawn_blocking(move || {
            kv.write_batch(kv_batch)?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        for (
            user_id,
            (pending_units, pending_tokens, first_tx_micros, last_tx_micros, first_event_id),
        ) in reflection_deltas
        {
            self.bump_reflection_marker_with_window(
                &user_id,
                pending_units,
                pending_tokens,
                Some(first_tx_micros),
                Some(last_tx_micros),
                Some(first_event_id),
            )?;
        }

        // Maintain L1 secondary index for efficient fetch_recent_l1_units.
        // Key: "l1_idx:{user_id}:{id}" -> timestamp_micros as little-endian bytes (fast sort, no JSON).
        // The user_id prefix is critical: without it the global scan mixes all users' L1 units.
        let l1_units: Vec<(String, String, i64)> = units
            .iter()
            .filter(|u| u.level == 1 && Self::is_local_domain(&u.domain))
            .map(|u| {
                (
                    u.user_id.clone(),
                    u.id.to_string(),
                    u.transaction_time.timestamp_micros(),
                )
            })
            .collect();
        if !l1_units.is_empty() {
            let kv_l1 = self._kv.clone();
            tokio::task::spawn_blocking(move || {
                let mut batch = rocksdb::WriteBatch::default();
                for (uid, id, ts_micros) in &l1_units {
                    let key = format!("l1_idx:{}:{}", uid, id);
                    batch.put(key.as_bytes(), ts_micros.to_le_bytes());
                }
                kv_l1.write_batch(batch)?;
                Ok::<(), anyhow::Error>(())
            })
            .await??;
        }

        // 2. Store Vector in Lance (single "memories" table)
        let units_with_embeddings: Vec<MemoryUnit> = units
            .iter()
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
        })
        .await??;

        // 4. Automatic Semantic Linking (Parallelized)
        let units_for_org_publication = units.clone();
        let mut join_set = tokio::task::JoinSet::new();
        for unit in units {
            let engine = self.clone();
            join_set.spawn(async move {
                if !Self::is_local_domain(&unit.domain) {
                    return;
                }
                if run_reconciliation {
                    if let Err(e) = engine.reconcile_conflicting_memory_unit(&unit).await {
                        tracing::error!(
                            "Memory reconciliation failed for unit {}: {:?}",
                            unit.id,
                            e
                        );
                    }
                }
                match engine.is_visible_memory_unit(&unit) {
                    Ok(true) => {}
                    Ok(false) => return,
                    Err(e) => {
                        tracing::error!("Failed to check visibility for unit {}: {:?}", unit.id, e);
                        return;
                    }
                }
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

        self.publish_native_shared_knowledge(&units_for_org_publication)
            .await?;

        Ok(())
    }

    async fn auto_link_memory(&self, unit: &MemoryUnit) -> Result<()> {
        if let Some(ref embedding) = unit.embedding {
            let filter = self.build_user_filter(
                &unit.user_id,
                Some("(domain = 'agent' OR domain = 'user')".to_string()),
            );
            let similar = self
                .search_similar(&unit.user_id, embedding, 5, filter)
                .await?;

            for (peer, score) in similar {
                if peer.id != unit.id && score > self.auto_link_similarity_threshold {
                    // 使用配置值
                    let edge = GraphEdge::new(
                        unit.user_id.clone(),
                        unit.id,
                        peer.id,
                        RelationType::RelatedTo,
                        score,
                    );
                    self.graph.add_edge(&edge).await?;

                    // Set community marker since graph changed
                    self.set_needs_community(&unit.user_id)?;
                }
            }
        }
        Ok(())
    }

    async fn semantic_link_memory(&self, unit: &MemoryUnit) -> Result<()> {
        let context = self.fetch_recent_l1_units(&unit.user_id, 25).await?;

        let context: Vec<MemoryUnit> = context
            .into_iter()
            .filter(|u| u.id != unit.id)
            .take(5)
            .collect();

        if context.is_empty() {
            return Ok(());
        }

        let edges = self.arbitrator.analyze_relations(unit, &context).await?;

        if !edges.is_empty() {
            for edge in edges {
                self.graph.add_edge(&edge).await?;
            }
        }
        Ok(())
    }

    async fn fetch_memory_correction_candidates(
        &self,
        unit: &MemoryUnit,
        limit: usize,
    ) -> Result<Vec<MemoryUnit>> {
        let mut candidates = HashMap::new();
        let facts = self.resolve_memory_fact_descriptors(unit).await;
        if facts.is_empty() && fact_extraction::is_non_assertive_memory_content(&unit.content) {
            return Ok(Vec::new());
        }
        let focus_terms = Self::build_memory_correction_focus_terms(unit, &facts);
        let search_queries =
            Self::build_memory_correction_search_queries(unit, &facts, &focus_terms);

        for query in search_queries {
            for candidate in self
                .search_text(&unit.user_id, &query, limit * 2, false, None)
                .await?
            {
                candidates.entry(candidate.id).or_insert(candidate);
            }
        }

        for candidate in self
            .fetch_recent_l1_units(&unit.user_id, (limit * 6).max(24))
            .await?
        {
            candidates.entry(candidate.id).or_insert(candidate);
        }

        let mut ranked = candidates
            .into_values()
            .filter(|candidate| {
                candidate.id != unit.id
                    && candidate.level == 1
                    && candidate.memory_type == MemoryType::Factual
                    && Self::is_local_domain(&candidate.domain)
                    && candidate.transaction_time < unit.transaction_time
            })
            .map(|candidate| {
                let score = Self::memory_correction_candidate_score_for_facts(
                    unit,
                    &candidate,
                    &focus_terms,
                    &facts,
                );
                (candidate, score)
            })
            .filter(|(_, score)| *score > 0.0)
            .collect::<Vec<_>>();

        ranked.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right.0.transaction_time.cmp(&left.0.transaction_time))
        });
        ranked.truncate(limit);

        Ok(ranked.into_iter().map(|(candidate, _)| candidate).collect())
    }

    pub(crate) async fn detect_memory_correction_actions(
        &self,
        unit: &MemoryUnit,
        context: &[MemoryUnit],
    ) -> Result<Vec<MemoryCorrectionAction>> {
        if unit.level != 1
            || unit.memory_type != MemoryType::Factual
            || !Self::is_local_domain(&unit.domain)
            || context.is_empty()
        {
            return Ok(Vec::new());
        }

        let actions = self
            .arbitrator
            .detect_memory_corrections(unit, context)
            .await?;
        for action in &actions {
            let metric = match action.kind {
                MemoryCorrectionKind::Obsolete => "correction_action_obsolete_total",
                MemoryCorrectionKind::Contradicts => "correction_action_contradicts_total",
                MemoryCorrectionKind::Reaffirm => "correction_action_reaffirm_total",
                MemoryCorrectionKind::Ignore => "correction_action_ignore_total",
            };
            let _ = self.increment_rac_metric_counter(metric, 1);
        }
        Ok(actions)
    }

    pub(crate) async fn validate_memory_correction_relation(
        &self,
        unit: &MemoryUnit,
        target_unit: &MemoryUnit,
        kind: MemoryCorrectionKind,
        action_confidence: f32,
    ) -> ValidatedCorrectionDecision {
        match kind {
            MemoryCorrectionKind::Obsolete => {
                if action_confidence < OBSOLETE_ACTION_RELATION_ONLY_MIN_CONFIDENCE {
                    tracing::warn!(
                        "Skipping OBSOLETE correction from {} to {} because confidence {:.2} is below relation threshold {:.2}",
                        unit.id,
                        target_unit.id,
                        action_confidence,
                        OBSOLETE_ACTION_RELATION_ONLY_MIN_CONFIDENCE
                    );
                    return ValidatedCorrectionDecision::Skip {
                        effect: RacDecisionEffect::Rejected,
                        guard_reason: "obsolete_low_confidence".into(),
                    };
                }
                if target_unit.transaction_time > unit.transaction_time {
                    tracing::warn!(
                        "Skipping OBSOLETE correction from {} to {} because target is newer",
                        unit.id,
                        target_unit.id
                    );
                    return ValidatedCorrectionDecision::Skip {
                        effect: RacDecisionEffect::Rejected,
                        guard_reason: "target_newer_than_source".into(),
                    };
                }
                let Some((source_fact, _target_fact)) = self
                    .resolve_fact_descriptors_compatible(unit, target_unit)
                    .await
                else {
                    tracing::warn!(
                        "Skipping OBSOLETE correction from {} to {} because fact slots differ",
                        unit.id,
                        target_unit.id
                    );
                    return ValidatedCorrectionDecision::Skip {
                        effect: RacDecisionEffect::Rejected,
                        guard_reason: "fact_slots_mismatch".into(),
                    };
                };
                if !Self::fact_change_supports_obsolete(source_fact.change_type) {
                    tracing::warn!(
                        "Skipping OBSOLETE correction from {} to {} because change type {:?} is not replacement-safe",
                        unit.id,
                        target_unit.id,
                        source_fact.change_type
                    );
                    return ValidatedCorrectionDecision::Skip {
                        effect: RacDecisionEffect::Rejected,
                        guard_reason: "change_type_not_replacement_safe".into(),
                    };
                }
                if action_confidence < OBSOLETE_ACTION_MIN_CONFIDENCE {
                    return ValidatedCorrectionDecision::RelationOnly {
                        relation: RelationType::EvolvedTo,
                        guard_reason: Some("obsolete_relation_only_due_to_confidence".into()),
                    };
                }
                ValidatedCorrectionDecision::Tombstone {
                    relation: RelationType::EvolvedTo,
                }
            }
            MemoryCorrectionKind::Contradicts => {
                let Some((source_fact, _target_fact)) = self
                    .resolve_fact_descriptors_compatible(unit, target_unit)
                    .await
                else {
                    tracing::warn!(
                        "Skipping CONTRADICTS correction from {} to {} because fact slots differ",
                        unit.id,
                        target_unit.id
                    );
                    return ValidatedCorrectionDecision::Skip {
                        effect: RacDecisionEffect::Rejected,
                        guard_reason: "fact_slots_mismatch".into(),
                    };
                };
                if !Self::fact_change_supports_contradiction(source_fact.change_type) {
                    tracing::warn!(
                        "Skipping CONTRADICTS correction from {} to {} because change type {:?} does not indicate contradiction",
                        unit.id,
                        target_unit.id,
                        source_fact.change_type
                    );
                    return ValidatedCorrectionDecision::Skip {
                        effect: RacDecisionEffect::Rejected,
                        guard_reason: "change_type_not_contradiction_safe".into(),
                    };
                }
                ValidatedCorrectionDecision::RelationOnly {
                    relation: RelationType::Contradicts,
                    guard_reason: None,
                }
            }
            MemoryCorrectionKind::Reaffirm => ValidatedCorrectionDecision::Skip {
                effect: RacDecisionEffect::Noop,
                guard_reason: "reaffirm_no_mutation".into(),
            },
            MemoryCorrectionKind::Ignore => ValidatedCorrectionDecision::Skip {
                effect: RacDecisionEffect::Noop,
                guard_reason: "ignored_by_arbitrator".into(),
            },
        }
    }

    pub(crate) async fn reconcile_conflicting_memory_unit(
        &self,
        unit: &MemoryUnit,
    ) -> Result<Vec<Uuid>> {
        if unit.level != 1
            || unit.memory_type != MemoryType::Factual
            || !Self::is_local_domain(&unit.domain)
        {
            return Ok(Vec::new());
        }

        let context = self.fetch_memory_correction_candidates(unit, 8).await?;
        if context.is_empty() {
            return Ok(Vec::new());
        }

        let actions = self
            .detect_memory_correction_actions(unit, &context)
            .await?;
        self.apply_memory_correction_actions(unit, actions).await
    }

    async fn apply_memory_correction_actions_with_stage(
        &self,
        unit: &MemoryUnit,
        actions: Vec<MemoryCorrectionAction>,
        stage: &str,
    ) -> Result<Vec<Uuid>> {
        let mut affected_ids = Vec::new();

        for action in actions {
            if action.target_id == unit.id {
                continue;
            }

            let Some(target_unit) =
                self.get_memory_unit_including_forgotten(&unit.user_id, action.target_id)?
            else {
                let _ = self.record_rac_decision(&RacDecisionRecord {
                    created_at: Utc::now(),
                    stage: stage.into(),
                    user_id: unit.user_id.clone(),
                    org_id: unit.org_id.clone(),
                    source_unit_id: unit.id,
                    target_unit_id: Some(action.target_id),
                    action: format!("{:?}", action.kind).to_ascii_lowercase(),
                    confidence: action.confidence,
                    effect: RacDecisionEffect::Rejected,
                    relation: None,
                    reason: action.reason.clone(),
                    guard_reason: Some("target_missing".into()),
                });
                continue;
            };

            let decision = self
                .validate_memory_correction_relation(
                    unit,
                    &target_unit,
                    action.kind,
                    action.confidence,
                )
                .await;

            match decision {
                ValidatedCorrectionDecision::Tombstone { relation } => {
                    let reason = if action.reason.trim().is_empty() {
                        format!("Superseded by memory {}", unit.id)
                    } else {
                        action.reason.clone()
                    };
                    let tombstone = ForgettingTombstone {
                        user_id: unit.user_id.clone(),
                        org_id: unit.org_id.clone(),
                        target_kind: memorose_common::ForgetTargetKind::MemoryUnit,
                        target_id: action.target_id.to_string(),
                        reason_query: reason,
                        created_at: chrono::Utc::now(),
                        preview_id: Some(unit.id.to_string()),
                        mode: memorose_common::ForgetMode::Logical,
                    };
                    self.mark_memory_unit_forgotten(&unit.user_id, action.target_id, &tombstone)?;
                    let _ = self.increment_rac_metric_counter("tombstone_total", 1);
                    let relation_name = format!("{:?}", relation).to_ascii_lowercase();
                    let edge = GraphEdge::new(
                        unit.user_id.clone(),
                        unit.id,
                        action.target_id,
                        relation.clone(),
                        action.confidence,
                    );
                    self.graph.add_edge(&edge).await?;
                    let _ = self.record_rac_decision_with_review(&RacDecisionRecord {
                        created_at: Utc::now(),
                        stage: stage.into(),
                        user_id: unit.user_id.clone(),
                        org_id: unit.org_id.clone(),
                        source_unit_id: unit.id,
                        target_unit_id: Some(action.target_id),
                        action: format!("{:?}", action.kind).to_ascii_lowercase(),
                        confidence: action.confidence,
                        effect: RacDecisionEffect::Tombstone,
                        relation: Some(relation_name),
                        reason: tombstone.reason_query.clone(),
                        guard_reason: None,
                    });
                    affected_ids.push(action.target_id);
                }
                ValidatedCorrectionDecision::RelationOnly {
                    relation,
                    guard_reason,
                } => {
                    let relation_name = format!("{:?}", relation).to_ascii_lowercase();
                    let edge = GraphEdge::new(
                        unit.user_id.clone(),
                        unit.id,
                        action.target_id,
                        relation.clone(),
                        action.confidence,
                    );
                    self.graph.add_edge(&edge).await?;
                    let _ = self.record_rac_decision_with_review(&RacDecisionRecord {
                        created_at: Utc::now(),
                        stage: stage.into(),
                        user_id: unit.user_id.clone(),
                        org_id: unit.org_id.clone(),
                        source_unit_id: unit.id,
                        target_unit_id: Some(action.target_id),
                        action: format!("{:?}", action.kind).to_ascii_lowercase(),
                        confidence: action.confidence,
                        effect: RacDecisionEffect::RelationOnly,
                        relation: Some(relation_name),
                        reason: action.reason.clone(),
                        guard_reason,
                    });
                    affected_ids.push(action.target_id);
                }
                ValidatedCorrectionDecision::Skip {
                    effect,
                    guard_reason,
                } => {
                    let _ = self.record_rac_decision_with_review(&RacDecisionRecord {
                        created_at: Utc::now(),
                        stage: stage.into(),
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

        if !affected_ids.is_empty() {
            self.invalidate_query_cache(&unit.user_id).await;
            let _ = self.set_needs_community(&unit.user_id);
        }

        Ok(affected_ids)
    }

    async fn apply_memory_correction_actions(
        &self,
        unit: &MemoryUnit,
        actions: Vec<MemoryCorrectionAction>,
    ) -> Result<Vec<Uuid>> {
        self.apply_memory_correction_actions_with_stage(unit, actions, "post_store")
            .await
    }

    // ── Search ──────────────────────────────────────────────────────

    pub async fn search_similar(
        &self,
        user_id: &str,
        vector: &[f32],
        limit: usize,
        filter: Option<String>,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        let results = match self.vector.search("memories", vector, limit, filter).await {
            Ok(res) => res,
            Err(_) => return Ok(Vec::new()),
        };
        self.fetch_units_with_scores(user_id, results).await
    }

    /// Perform a BFS graph traversal to expand context from seed memories.
    async fn expand_subgraph(
        &self,
        user_id: &str,
        seeds: Vec<(MemoryUnit, f32)>,
        depth: usize,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        if depth == 0 || seeds.is_empty() {
            return Ok(seeds);
        }

        let mut results: HashMap<String, (MemoryUnit, f32)> = seeds
            .iter()
            .map(|(u, s)| (u.id.to_string(), (u.clone(), *s)))
            .collect();

        let mut frontier: Vec<String> = seeds.iter().map(|(u, _)| u.id.to_string()).collect();
        let mut visited: HashSet<String> = frontier.iter().cloned().collect();

        for _d in 0..depth {
            if frontier.is_empty() {
                break;
            }

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
            let node_ids: Vec<Uuid> = frontier
                .iter()
                .filter_map(|id_str| Uuid::parse_str(id_str).ok())
                .collect();

            if node_ids.is_empty() {
                break;
            }

            // 批量查询出边和入边
            let (out_map_res, in_map_res) = tokio::join!(
                self.batch_executor
                    .batch_get_outgoing_edges(user_id, &node_ids),
                self.batch_executor
                    .batch_get_incoming_edges(user_id, &node_ids)
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
                let neighbor_id = if is_outgoing {
                    edge.target_id
                } else {
                    edge.source_id
                };
                let neighbor_str = neighbor_id.to_string();

                if visited.contains(&neighbor_str) {
                    continue;
                }

                let is_relevant = match edge.relation {
                    RelationType::DerivedFrom | RelationType::EvolvedTo => true,
                    RelationType::RelatedTo
                        if edge.weight > self.auto_link_similarity_threshold =>
                    {
                        true
                    }
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
    pub async fn search_procedural(
        &self,
        user_id: &str,
        agent_id: Option<&str>,
        query_text: &str,
        vector: &[f32],
        limit: usize,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        let mut extra_filter =
            "(domain = 'agent' OR domain = 'user') AND memory_type = 'procedural'".to_string();
        if let Some(aid) = agent_id {
            extra_filter.push_str(&format!(" AND agent_id = '{}'", aid.replace('\'', "''")));
        }

        let vec_filter = self.build_user_filter(user_id, Some(extra_filter));
        let vector_future = self
            .vector
            .search("memories", vector, limit * 2, vec_filter);

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

        let candidates_to_fetch: Vec<String> =
            vector_hits.iter().map(|(id, _)| id.clone()).collect();
        let mut units: Vec<MemoryUnit> = self.fetch_units(user_id, candidates_to_fetch).await?;

        // Ensure strictly procedural
        units.retain(|u| u.memory_type == memorose_common::MemoryType::Procedural);

        let mut seeds = Vec::new();
        for unit in units {
            let score = vector_hits
                .iter()
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

    pub async fn search_hybrid(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        agent_id: Option<&str>,
        query_text: &str,
        vector: &[f32],
        limit: usize,
        enable_arbitration: bool,
        min_score: Option<f32>,
        graph_depth: usize,
        valid_time: Option<TimeRange>,
        transaction_time: Option<TimeRange>,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        self.search_hybrid_with_token_budget(
            user_id,
            org_id,
            agent_id,
            query_text,
            vector,
            limit,
            enable_arbitration,
            min_score,
            graph_depth,
            valid_time,
            transaction_time,
            None,
        )
        .await
    }

    pub async fn search_hybrid_with_token_budget(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        agent_id: Option<&str>,
        query_text: &str,
        vector: &[f32],
        limit: usize,
        enable_arbitration: bool,
        min_score: Option<f32>,
        graph_depth: usize,
        valid_time: Option<TimeRange>,
        transaction_time: Option<TimeRange>,
        token_budget: Option<usize>,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        let time_filter = self.build_time_filter(valid_time.clone());
        let agent_filter = agent_id.map(|aid| format!("agent_id = '{}'", aid.replace('\'', "''")));
        let org_filter = org_id.map(|oid| format!("org_id = '{}'", oid.replace('\'', "''")));
        let mut filters = vec!["(domain = 'agent' OR domain = 'user')".to_string()];
        if let Some(filter) = time_filter {
            filters.push(filter);
        }
        if let Some(filter) = agent_filter {
            filters.push(filter);
        }
        if let Some(filter) = org_filter {
            filters.push(filter);
        }
        let extra = Some(filters.join(" AND "));
        let vec_filter = self.build_user_filter(user_id, extra);

        let vector_future = self
            .vector
            .search("memories", vector, limit * 2, vec_filter);

        let index = self.index.clone();
        let q_text = query_text.to_string();
        let vt = valid_time.clone();
        let tt = transaction_time.clone();
        let oid = org_id.map(|s| s.to_string());
        let uid = Some(user_id.to_string());
        let agid = agent_id.map(|s| s.to_string());
        let text_future = tokio::task::spawn_blocking(move || {
            // Ensure reader sees latest committed segments before searching
            index.reload().ok();
            index.search_bitemporal(
                &q_text,
                limit * 2,
                vt,
                tt,
                oid.as_deref(),
                uid.as_deref(),
                agid.as_deref(),
                None,
            )
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

        let candidates_to_fetch: Vec<String> = sorted_ids
            .iter()
            .take(limit * 3)
            .map(|(id, _)| id.clone())
            .collect();
        let units: Vec<MemoryUnit> = self
            .fetch_units(user_id, candidates_to_fetch)
            .await?
            .into_iter()
            .filter(|unit| org_id.map_or(true, |oid| unit.org_id.as_deref() == Some(oid)))
            .collect();

        let mut seeds = Vec::new();
        for unit in units {
            let score = sorted_ids
                .iter()
                .find(|(id, _)| *id == unit.id.to_string())
                .map(|(_, s)| *s)
                .unwrap_or(0.0);
            seeds.push((unit, score));
        }

        // Graph Expansion (BFS)
        let mut expanded_units = self.expand_subgraph(user_id, seeds, graph_depth).await?;
        if let Some(org_id) = org_id {
            expanded_units.retain(|(unit, _)| unit.org_id.as_deref() == Some(org_id));
        }

        expanded_units.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Time and Importance Reranking
        let final_results = self
            .reranker
            .rerank(query_text, &self._kv, expanded_units)
            .await?;

        // Default threshold lowered: RRF scores are now normalized to [0,1], and the
        // reranker adds importance (0.2) + recency (0.1) components, so a reasonable
        // cutoff is ~0.3 to keep relevant results while filtering noise.
        let threshold = min_score.unwrap_or(0.3);
        let mut final_results: Vec<_> = final_results
            .into_iter()
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
                tracing::info!(
                    "Skipping arbitration due to high confidence in Top 1 (Score gap: {:.2})",
                    (top1_score - top2_score).abs()
                );
            }
        }

        if should_arbitrate {
            tracing::info!(
                "Executing LLM Arbitration for {} candidates...",
                results_for_arbitration.len()
            );
            let units_to_arbitrate: Vec<MemoryUnit> = results_for_arbitration
                .iter()
                .map(|(u, _)| u.clone())
                .collect();
            let arbitrated = self
                .arbitrator
                .arbitrate(units_to_arbitrate, Some(query_text))
                .await?;

            let mut arbitrated_results = Vec::new();
            for unit in arbitrated {
                if let Some((_, score)) = results_for_arbitration
                    .iter()
                    .find(|(u, _)| u.id == unit.id)
                {
                    arbitrated_results.push((unit, *score));
                }
            }
            Ok(Self::apply_token_budget_to_scored_memory_units(
                arbitrated_results,
                token_budget,
            ))
        } else {
            Ok(Self::apply_token_budget_to_scored_memory_units(
                results_for_arbitration,
                token_budget,
            ))
        }
    }

    async fn search_shared_scope(
        &self,
        domain: MemoryDomain,
        org_id: Option<&str>,
        agent_id: Option<&str>,
        query_text: &str,
        vector: &[f32],
        limit: usize,
        min_score: Option<f32>,
        valid_time: Option<TimeRange>,
    ) -> Result<Vec<(SharedSearchHit, f32)>> {
        if domain == MemoryDomain::Organization {
            let Some(org_id) = org_id else {
                return Ok(Vec::new());
            };
            let record_hits = self
                .search_organization_knowledge_records(
                    org_id, query_text, vector, limit, min_score, valid_time,
                )
                .await?;
            return self.materialize_organization_search_hits(record_hits).await;
        }

        let shared_agent_filter = match domain {
            MemoryDomain::Organization => None,
            _ => agent_id,
        };
        let filter = self.build_global_filter(
            domain,
            org_id,
            shared_agent_filter,
            self.build_time_filter(valid_time),
        );

        let hits = match self
            .vector
            .search("memories", vector, limit * 2, filter)
            .await
        {
            Ok(hits) => hits,
            Err(error) => {
                let msg = error.to_string().to_lowercase();
                if (msg.contains("table") || msg.contains("no such")) && msg.contains("not found") {
                    Vec::new()
                } else {
                    return Err(error);
                }
            }
        };

        if hits.is_empty() {
            return Ok(Vec::new());
        }

        let candidates = self.fetch_units_with_scores_global(hits).await?;
        let mut reranked = self
            .reranker
            .rerank(
                query_text,
                &self._kv,
                candidates
                    .iter()
                    .map(|(hit, score)| (hit.memory_unit().clone(), *score))
                    .collect(),
            )
            .await?;
        let threshold = min_score.unwrap_or(0.3);
        reranked.retain(|(_, score)| *score >= threshold);
        let mut scored_hits = Vec::with_capacity(reranked.len());
        for (unit, score) in reranked {
            if let Some((hit, _)) = candidates.iter().find(|(hit, _)| hit.id == unit.id) {
                scored_hits.push((hit.clone(), score));
            }
        }
        Ok(scored_hits)
    }

    pub async fn search_hybrid_with_shared(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        agent_id: Option<&str>,
        query_text: &str,
        vector: &[f32],
        limit: usize,
        enable_arbitration: bool,
        min_score: Option<f32>,
        graph_depth: usize,
        valid_time: Option<TimeRange>,
        transaction_time: Option<TimeRange>,
    ) -> Result<Vec<(SharedSearchHit, f32)>> {
        self.search_hybrid_with_shared_and_token_budget(
            user_id,
            org_id,
            agent_id,
            query_text,
            vector,
            limit,
            enable_arbitration,
            min_score,
            graph_depth,
            valid_time,
            transaction_time,
            None,
        )
        .await
    }

    pub async fn search_hybrid_with_shared_and_token_budget(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        agent_id: Option<&str>,
        query_text: &str,
        vector: &[f32],
        limit: usize,
        enable_arbitration: bool,
        min_score: Option<f32>,
        graph_depth: usize,
        valid_time: Option<TimeRange>,
        transaction_time: Option<TimeRange>,
        token_budget: Option<usize>,
    ) -> Result<Vec<(SharedSearchHit, f32)>> {
        let mut combined = self
            .search_hybrid(
                user_id,
                org_id,
                agent_id,
                query_text,
                vector,
                limit,
                false,
                min_score,
                graph_depth,
                valid_time.clone(),
                transaction_time,
            )
            .await?
            .into_iter()
            .map(|(unit, score)| (SharedSearchHit::native(unit), score))
            .collect::<Vec<_>>();

        if let Some(org_id) = org_id {
            let org_policy = self.get_org_share_policy(user_id, org_id)?;
            if org_policy.consume {
                let mut org_results = self
                    .search_shared_scope(
                        MemoryDomain::Organization,
                        Some(org_id),
                        agent_id,
                        query_text,
                        vector,
                        limit,
                        min_score,
                        valid_time,
                    )
                    .await?;
                for (_, score) in &mut org_results {
                    *score *= 0.7;
                }
                combined.extend(org_results);
            }
        }

        if combined.is_empty() {
            return Ok(Vec::new());
        }

        combined.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut deduped: Vec<(SharedSearchHit, f32)> = Vec::new();
        let mut seen_ids = HashSet::new();
        for (hit, score) in combined {
            if !seen_ids.insert(hit.id) {
                continue;
            }

            let mut is_duplicate = false;
            for (existing, _) in &deduped {
                if let (Some(v1), Some(v2)) = (&hit.embedding, &existing.embedding) {
                    if cosine_similarity(v1, v2) > 0.92 {
                        is_duplicate = true;
                        break;
                    }
                }
            }

            if !is_duplicate {
                deduped.push((hit, score));
            }

            if deduped.len() >= limit * 2 {
                break;
            }
        }

        let threshold = min_score.unwrap_or(0.3);
        deduped.retain(|(_, score)| *score >= threshold);
        if deduped.is_empty() {
            return Ok(Vec::new());
        }

        if deduped.len() > limit * 2 {
            deduped.truncate(limit * 2);
        }

        let should_arbitrate =
            enable_arbitration && deduped.len() >= 2 && (deduped[0].1 - deduped[1].1).abs() < 0.25;

        if should_arbitrate {
            let arbitrated = self
                .arbitrator
                .arbitrate(
                    deduped
                        .iter()
                        .map(|(hit, _)| hit.memory_unit().clone())
                        .collect(),
                    Some(query_text),
                )
                .await?;

            let mut final_results = Vec::new();
            for unit in arbitrated {
                if let Some((hit, score)) = deduped
                    .iter()
                    .find(|(candidate, _)| candidate.id == unit.id)
                {
                    final_results.push((hit.clone(), *score));
                }
            }
            Ok(Self::apply_token_budget_to_scored_shared_hits(
                final_results,
                token_budget,
            ))
        } else {
            deduped.truncate(limit);
            Ok(Self::apply_token_budget_to_scored_shared_hits(
                deduped,
                token_budget,
            ))
        }
    }

    pub async fn search_text(
        &self,
        user_id: &str,
        query: &str,
        limit: usize,
        enable_arbitration: bool,
        time_range: Option<TimeRange>,
    ) -> Result<Vec<MemoryUnit>> {
        let index = self.index.clone();
        tokio::task::spawn_blocking(move || {
            index.reload().ok();
        })
        .await?;

        let index = self.index.clone();
        let q = query.to_string();
        let tr = time_range.clone();
        let uid = Some(user_id.to_string());
        let ids =
            tokio::task::spawn_blocking(move || index.search(&q, limit, tr, None, uid.as_deref()))
                .await??;

        let mut units = self.fetch_units(user_id, ids).await?;
        units.retain(|unit| Self::is_local_domain(&unit.domain));

        if enable_arbitration {
            self.arbitrator.arbitrate(units, Some(query)).await
        } else {
            Ok(units)
        }
    }

    pub async fn search_text_with_shared(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        query: &str,
        limit: usize,
        enable_arbitration: bool,
        time_range: Option<TimeRange>,
    ) -> Result<Vec<SharedSearchHit>> {
        let index = self.index.clone();
        tokio::task::spawn_blocking(move || {
            index.reload().ok();
        })
        .await?;

        let k = 60.0;
        let mut combined_scores: HashMap<Uuid, (SharedSearchHit, f32)> = HashMap::new();
        for (rank, unit) in self
            .search_text(user_id, query, limit, false, time_range.clone())
            .await?
            .into_iter()
            .enumerate()
        {
            let score = 1.0 / (k + rank as f32);
            combined_scores
                .entry(unit.id)
                .and_modify(|(_, existing_score)| *existing_score += score)
                .or_insert((SharedSearchHit::native(unit), score));
        }

        if let Some(org_id) = org_id {
            let org_policy = self.get_org_share_policy(user_id, org_id)?;
            if org_policy.consume {
                for (rank, unit) in self
                    .search_organization_knowledge_text(org_id, query, limit, time_range.clone())
                    .await?
                    .into_iter()
                    .enumerate()
                {
                    let score = 0.7 / (k + rank as f32);
                    combined_scores
                        .entry(unit.id)
                        .and_modify(|(_, existing_score)| *existing_score += score)
                        .or_insert((unit, score));
                }
            }
        }

        if combined_scores.is_empty() {
            return Ok(Vec::new());
        }

        let mut hits: Vec<(SharedSearchHit, f32)> = combined_scores.into_values().collect();
        hits.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        if hits.len() > limit * 2 {
            hits.truncate(limit * 2);
        }

        if enable_arbitration {
            let arbitrated = self
                .arbitrator
                .arbitrate(
                    hits.iter()
                        .map(|(hit, _)| hit.memory_unit().clone())
                        .collect(),
                    Some(query),
                )
                .await?;
            let mut final_hits = Vec::new();
            for unit in arbitrated {
                if let Some((hit, _)) = hits.iter().find(|(candidate, _)| candidate.id == unit.id) {
                    final_hits.push(hit.clone());
                }
            }
            if final_hits.len() > limit {
                final_hits.truncate(limit);
            }
            Ok(final_hits)
        } else {
            let mut final_hits = hits.into_iter().map(|(hit, _)| hit).collect::<Vec<_>>();
            if final_hits.len() > limit {
                final_hits.truncate(limit);
            }
            Ok(final_hits)
        }
    }

    /// Search and then consolidate the results into a single narrative.
    pub async fn search_consolidated(
        &self,
        user_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<String> {
        let units = self.search_text(user_id, query, limit, false, None).await?;
        self.arbitrator.consolidate(units).await
    }

    // ── Memory Retrieval ────────────────────────────────────────────

    fn get_memory_unit_raw(&self, user_id: &str, id: Uuid) -> Result<Option<MemoryUnit>> {
        let key = format!("u:{}:unit:{}", user_id, id);
        let val = self._kv.get(key.as_bytes())?;
        match val {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    pub async fn get_memory_unit(&self, user_id: &str, id: Uuid) -> Result<Option<MemoryUnit>> {
        if self.is_memory_unit_forgotten(user_id, id)? {
            return Ok(None);
        }
        Ok(self
            .get_memory_unit_raw(user_id, id)?
            .filter(|unit| self.is_visible_memory_unit(unit).unwrap_or(false)))
    }

    /// Return a native memory unit even if it has been logically forgotten.
    /// This is useful for irreversible cleanup flows that must remove
    /// previously tombstoned storage from all backends.
    pub fn get_memory_unit_including_forgotten(
        &self,
        user_id: &str,
        id: Uuid,
    ) -> Result<Option<MemoryUnit>> {
        self.get_memory_unit_raw(user_id, id)
    }

    pub async fn get_native_memory_unit_by_index(&self, id: Uuid) -> Result<Option<MemoryUnit>> {
        let idx_key = format!("idx:unit:{}", id);
        if let Some(uid_bytes) = self._kv.get(idx_key.as_bytes())? {
            let user_id = String::from_utf8(uid_bytes)?;
            self.get_memory_unit(&user_id, id).await
        } else {
            Ok(None)
        }
    }

    pub async fn get_shared_search_hit_by_index(
        &self,
        id: Uuid,
    ) -> Result<Option<SharedSearchHit>> {
        if let Some(record) = self.load_organization_knowledge(id)? {
            let unit = self
                .materialize_organization_read_view_for_record(&record)
                .await?;
            return Ok(Some(SharedSearchHit::organization_knowledge(&record, unit)));
        }

        Ok(self
            .get_native_memory_unit_by_index(id)
            .await?
            .map(SharedSearchHit::native))
    }

    // ── Forgetting ──────────────────────────────────────────────────

    fn forgotten_memory_unit_key(user_id: &str, id: Uuid) -> String {
        format!("forget:unit:{}:{}", user_id, id)
    }

    fn forgotten_event_key(user_id: &str, id: &str) -> String {
        format!("forget:event:{}:{}", user_id, id)
    }

    fn materialization_job_key(job_id: Uuid) -> String {
        format!("materialize:job:{}", job_id)
    }

    fn materialization_due_key(next_attempt_at_micros: i64, job_id: Uuid) -> String {
        format!(
            "materialize:due:{:020}:{}",
            next_attempt_at_micros.max(0),
            job_id
        )
    }

    fn materialization_due_prefix() -> &'static [u8] {
        b"materialize:due:"
    }

    fn materialization_post_publish_key(unit_id: Uuid) -> String {
        format!("materialize:hooks:{}", unit_id)
    }

    fn requires_materialized_embedding(unit: &MemoryUnit) -> bool {
        unit.visible
            && unit.materialization_state == MaterializationState::Published
            && (1..=3).contains(&unit.level)
            && Self::is_local_domain(&unit.domain)
    }

    fn validate_materialized_units(units: &[MemoryUnit]) -> Result<()> {
        if cfg!(test) {
            return Ok(());
        }

        for unit in units {
            if Self::requires_materialized_embedding(unit)
                && unit.embedding.as_ref().map(|embedding| embedding.is_empty()) != Some(false)
            {
                return Err(anyhow!(
                    "memory unit {} (level {}) cannot be published without an embedding",
                    unit.id,
                    unit.level
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn materialization_post_publish_applied(&self, unit_id: Uuid) -> Result<bool> {
        Ok(self
            .system_kv()
            .get(Self::materialization_post_publish_key(unit_id).as_bytes())?
            .is_some())
    }

    pub(crate) fn mark_materialization_post_publish_applied(&self, unit_id: Uuid) -> Result<()> {
        let now = chrono::Utc::now().timestamp_micros().to_string();
        self.system_kv().put(
            Self::materialization_post_publish_key(unit_id).as_bytes(),
            now.as_bytes(),
        )
    }

    fn save_materialization_job(&self, job: &PendingMaterializationJob) -> Result<()> {
        let system_kv = self.system_kv();
        let job_key = Self::materialization_job_key(job.job_id);
        let due_key = Self::materialization_due_key(job.next_attempt_at_micros, job.job_id);
        system_kv.put(job_key.as_bytes(), &serde_json::to_vec(job)?)?;
        if job.status != PendingMaterializationJobStatus::Failed {
            system_kv.put(due_key.as_bytes(), &[])?;
        }
        Ok(())
    }

    pub fn enqueue_materialization_jobs(
        &self,
        jobs: Vec<PendingMaterializationJob>,
    ) -> Result<()> {
        let system_kv = self.system_kv();
        for job in jobs {
            let job_key = Self::materialization_job_key(job.job_id);
            let due_key = Self::materialization_due_key(job.next_attempt_at_micros, job.job_id);
            system_kv.put(job_key.as_bytes(), &serde_json::to_vec(&job)?)?;
            system_kv.put(due_key.as_bytes(), &[])?;
        }
        Ok(())
    }

    pub fn fetch_due_materialization_jobs(
        &self,
        limit: usize,
    ) -> Result<Vec<PendingMaterializationJob>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let system_kv = self.system_kv();
        let now = Utc::now().timestamp_micros().max(0);
        let due_entries = system_kv.scan(Self::materialization_due_prefix())?;
        let mut jobs = Vec::new();

        for (key, _) in due_entries {
            if jobs.len() >= limit {
                break;
            }

            let Ok(key_str) = String::from_utf8(key) else {
                continue;
            };
            let mut parts = key_str.split(':');
            let Some("materialize") = parts.next() else {
                continue;
            };
            let Some("due") = parts.next() else {
                continue;
            };
            let Some(due_raw) = parts.next() else {
                continue;
            };
            let Some(job_id_raw) = parts.next() else {
                continue;
            };

            let Ok(due_at) = due_raw.parse::<i64>() else {
                continue;
            };
            if due_at > now {
                break;
            }

            let Ok(job_id) = Uuid::parse_str(job_id_raw) else {
                continue;
            };
            let job_key = Self::materialization_job_key(job_id);
            let Some(bytes) = system_kv.get(job_key.as_bytes())? else {
                system_kv.delete(key_str.as_bytes()).ok();
                continue;
            };
            let Ok(job) = serde_json::from_slice::<PendingMaterializationJob>(&bytes) else {
                continue;
            };
            if job.status == PendingMaterializationJobStatus::Failed {
                continue;
            }
            jobs.push(job);
        }

        Ok(jobs)
    }

    pub fn reschedule_materialization_job(
        &self,
        job: &mut PendingMaterializationJob,
        error: impl ToString,
    ) -> Result<()> {
        let system_kv = self.system_kv();
        let old_due_key = Self::materialization_due_key(job.next_attempt_at_micros, job.job_id);
        system_kv.delete(old_due_key.as_bytes()).ok();

        job.attempts = job.attempts.saturating_add(1);
        job.status = PendingMaterializationJobStatus::RetryScheduled;
        job.last_error = Some(error.to_string());
        job.updated_at = Utc::now();

        let backoff_secs = match job.attempts {
            0 | 1 => 10,
            2 => 30,
            3 => 120,
            4 => 600,
            _ => 1800,
        };
        job.next_attempt_at_micros = (Utc::now() + chrono::Duration::seconds(backoff_secs))
            .timestamp_micros();

        self.save_materialization_job(job)
    }

    pub fn fail_materialization_job(
        &self,
        job: &mut PendingMaterializationJob,
        error: impl ToString,
    ) -> Result<()> {
        let system_kv = self.system_kv();
        let old_due_key = Self::materialization_due_key(job.next_attempt_at_micros, job.job_id);
        system_kv.delete(old_due_key.as_bytes()).ok();

        job.attempts = job.attempts.saturating_add(1);
        job.status = PendingMaterializationJobStatus::Failed;
        job.last_error = Some(error.to_string());
        job.updated_at = Utc::now();

        self.save_materialization_job(job)
    }

    pub fn delete_materialization_job(&self, job: &PendingMaterializationJob) -> Result<()> {
        let system_kv = self.system_kv();
        let job_key = Self::materialization_job_key(job.job_id);
        let due_key = Self::materialization_due_key(job.next_attempt_at_micros, job.job_id);
        system_kv.delete(job_key.as_bytes()).ok();
        system_kv.delete(due_key.as_bytes()).ok();
        Ok(())
    }

    pub fn mark_memory_unit_forgotten(
        &self,
        user_id: &str,
        unit_id: Uuid,
        tombstone: &ForgettingTombstone,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(tombstone)?;
        self.system_kv().put(
            Self::forgotten_memory_unit_key(user_id, unit_id).as_bytes(),
            &bytes,
        )
    }

    pub fn mark_event_forgotten(
        &self,
        user_id: &str,
        event_id: &str,
        tombstone: &ForgettingTombstone,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(tombstone)?;
        self.system_kv().put(
            Self::forgotten_event_key(user_id, event_id).as_bytes(),
            &bytes,
        )
    }

    pub fn is_memory_unit_forgotten(&self, user_id: &str, unit_id: Uuid) -> Result<bool> {
        Ok(self
            .system_kv()
            .get(Self::forgotten_memory_unit_key(user_id, unit_id).as_bytes())?
            .is_some())
    }

    pub fn is_event_forgotten(&self, user_id: &str, event_id: &str) -> Result<bool> {
        Ok(self
            .system_kv()
            .get(Self::forgotten_event_key(user_id, event_id).as_bytes())?
            .is_some())
    }

    pub fn clear_memory_unit_forgotten(&self, user_id: &str, unit_id: Uuid) -> Result<()> {
        self.system_kv()
            .delete(Self::forgotten_memory_unit_key(user_id, unit_id).as_bytes())?;
        Ok(())
    }

    pub fn clear_event_forgotten(&self, user_id: &str, event_id: &str) -> Result<()> {
        self.system_kv()
            .delete(Self::forgotten_event_key(user_id, event_id).as_bytes())?;
        Ok(())
    }

    pub fn is_visible_memory_unit(&self, unit: &MemoryUnit) -> Result<bool> {
        if unit.domain == MemoryDomain::Organization {
            return Ok(true);
        }

        Ok(unit.visible
            && unit.materialization_state == MaterializationState::Published
            && !self.is_memory_unit_forgotten(&unit.user_id, unit.id)?)
    }

    pub async fn delete_memory_unit_hard(&self, user_id: &str, unit_id: Uuid) -> Result<()> {
        let unit = self.get_memory_unit_raw(user_id, unit_id)?;
        let unit_key = format!("u:{}:unit:{}", user_id, unit_id).into_bytes();
        self.delete_memory_unit_storage_by_key(unit_key, unit_id)
            .await?;
        let _ = self.graph.delete_edges_for_node(user_id, unit_id).await?;
        self.invalidate_query_cache(user_id).await;
        self.clear_memory_unit_forgotten(user_id, unit_id)?;

        if let Some(unit) = unit {
            if unit.level == 1 {
                let key = format!("l1_count:{}", user_id);
                self.system_kv().delete(key.as_bytes())?;
            }
        }

        Ok(())
    }

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
        })
        .await??;

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
        })
        .await??;

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
        let keys_and_levels: Vec<(Vec<u8>, String, u8, String)> = to_prune
            .iter()
            .map(|(k, u)| {
                (
                    k.clone(),
                    u.id.to_string(),
                    u.level,
                    Self::materialization_post_publish_key(u.id),
                )
            })
            .collect();
        let user_id_owned = user_id.to_string();
        tokio::task::spawn_blocking(move || {
            for (key, id, level, hooks_key) in &keys_and_levels {
                kv_clone.delete(key)?;
                if *level == 1 {
                    let l1_key = format!("l1_idx:{}:{}", user_id_owned, id);
                    kv_clone.delete(l1_key.as_bytes()).ok();
                }
                kv_clone.delete(hooks_key.as_bytes()).ok();
            }
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        // 2. Delete from LanceDB vector store
        for (_, unit) in &to_prune {
            if let Err(e) = self
                .vector
                .delete_by_id("memories", &unit.id.to_string())
                .await
            {
                tracing::warn!(
                    "Failed to delete unit {} from vector store during pruning: {:?}",
                    unit.id,
                    e
                );
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
        })
        .await??;

        Ok(count)
    }

    // ── Community Detection ─────────────────────────────────────────

    /// Graph-driven L2 Generation for a specific user.
    pub async fn process_communities(&self, user_id: &str) -> Result<()> {
        self.process_communities_with_limits(user_id, 3, usize::MAX)
            .await?;
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
        })
        .await?;

        let mut community_groups: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
        for (node_id, community_id) in communities {
            community_groups
                .entry(community_id)
                .or_default()
                .push(node_id);
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
                None,
                user_id.to_string(),
                None,
                Uuid::new_v4(),
                memorose_common::MemoryType::Factual,
                insight.summary,
                None,
            );
            l2_unit.level = 2;
            l2_unit.keywords.push(insight.name.clone());
            l2_unit.keywords.extend(insight.keywords);
            l2_unit.references = members.clone();
            let l2_id = l2_unit.id;
            let uid2 = user_id.to_string();
            let post_publish_edges = members
                .iter()
                .map(|member_id| {
                    GraphEdge::new(
                        uid2.clone(),
                        l2_id,
                        *member_id,
                        RelationType::DerivedFrom,
                        1.0,
                    )
                })
                .collect::<Vec<_>>();
            self.enqueue_materialization_jobs(vec![PendingMaterializationJob::new(
                l2_unit,
                post_publish_edges,
                None,
            )])?;

            created += 1;
            tracing::info!(
                "Created L2 Insight '{}' from {} members for user {}",
                insight.name,
                units.len(),
                user_id
            );
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
            let batch_detector = BatchCommunityDetector::new(self.graph.clone(), config);
            batch_detector
                .detect_communities_for_user(user_id, &node_ids)
                .await
        } else {
            // 小图直接使用增强检测器
            let detector = EnhancedCommunityDetector::new(config);
            tokio::task::spawn_blocking(move || detector.detect(&edges)).await?
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

        let batch_detector = BatchCommunityDetector::new(self.graph.clone(), config);

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
                None,
                user_id.to_string(),
                None,
                Uuid::new_v4(),
                memorose_common::MemoryType::Factual,
                insight.summary,
                None,
            );
            l2_unit.level = 2;
            l2_unit.keywords.push(insight.name.clone());
            l2_unit.keywords.extend(insight.keywords);
            l2_unit.references = members.clone();
            let l2_id = l2_unit.id;
            let uid2 = user_id.to_string();
            let post_publish_edges = members
                .iter()
                .map(|member_id| {
                    GraphEdge::new(
                        uid2.clone(),
                        l2_id,
                        *member_id,
                        RelationType::DerivedFrom,
                        1.0,
                    )
                })
                .collect::<Vec<_>>();
            self.enqueue_materialization_jobs(vec![PendingMaterializationJob::new(
                l2_unit,
                post_publish_edges,
                None,
            )])?;

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
    pub async fn fetch_recent_l1_units(
        &self,
        user_id: &str,
        limit: usize,
    ) -> Result<Vec<MemoryUnit>> {
        self.fetch_recent_l1_units_with_min_tx(user_id, limit, None)
            .await
    }

    pub async fn fetch_recent_l1_units_since(
        &self,
        user_id: &str,
        min_transaction_time_micros: i64,
        limit: usize,
    ) -> Result<Vec<MemoryUnit>> {
        self.fetch_recent_l1_units_with_min_tx(user_id, limit, Some(min_transaction_time_micros))
            .await
    }

    async fn fetch_recent_l1_units_with_min_tx(
        &self,
        user_id: &str,
        limit: usize,
        min_transaction_time_micros: Option<i64>,
    ) -> Result<Vec<MemoryUnit>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
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
        })
        .await??;

        if index_pairs.is_empty() {
            // Fallback for nodes that pre-date the L1 index: scan full units.
            return self
                .fetch_recent_l1_units_fallback(prefix_bytes, limit, min_transaction_time_micros)
                .await;
        }

        // Keep only the top-k newest IDs without sorting the entire index.
        let mut heap: BinaryHeap<(Reverse<i64>, String)> = BinaryHeap::with_capacity(limit + 1);
        for (key, value) in index_pairs {
            let Some((id, ts)) = (|| {
                let key_str = String::from_utf8(key).ok()?;
                let id = key_str.strip_prefix(&strip_prefix)?.to_string();
                let ts = i64::from_le_bytes(value.as_slice().try_into().ok()?);
                Some((id, ts))
            })() else {
                continue;
            };
            if min_transaction_time_micros.is_some_and(|min_ts| ts < min_ts) {
                continue;
            }

            if heap.len() < limit {
                heap.push((Reverse(ts), id));
                continue;
            }

            if let Some((Reverse(oldest_ts), _)) = heap.peek() {
                if ts > *oldest_ts {
                    heap.pop();
                    heap.push((Reverse(ts), id));
                }
            }
        }

        let mut id_ts: Vec<(String, i64)> =
            heap.into_iter().map(|(Reverse(ts), id)| (id, ts)).collect();
        id_ts.sort_by(|a, b| b.1.cmp(&a.1));

        // Multi-get the actual units by their KV keys.
        let keys: Vec<String> = id_ts
            .iter()
            .map(|(id, _)| format!("u:{}:unit:{}", user_id, id))
            .collect();

        let values = tokio::task::spawn_blocking({
            let store = store.clone();
            let key_refs_owned: Vec<Vec<u8>> = keys.iter().map(|k| k.as_bytes().to_vec()).collect();
            move || {
                store.multi_get(
                    &key_refs_owned
                        .iter()
                        .map(|k| k.as_slice())
                        .collect::<Vec<_>>(),
                )
            }
        })
        .await??;

        let results: Vec<MemoryUnit> = values
            .into_iter()
            .filter_map(|v| v.and_then(|bytes| serde_json::from_slice::<MemoryUnit>(&bytes).ok()))
            .filter(|unit: &MemoryUnit| {
                Self::is_local_domain(&unit.domain)
                    && self.is_visible_memory_unit(unit).unwrap_or(false)
            })
            .collect();

        Ok(results)
    }

    async fn fetch_recent_l1_units_fallback(
        &self,
        prefix_bytes: Vec<u8>,
        limit: usize,
        min_transaction_time_micros: Option<i64>,
    ) -> Result<Vec<MemoryUnit>> {
        let store = self._kv.clone();
        let pairs = tokio::task::spawn_blocking(move || store.scan(&prefix_bytes)).await??;
        let mut results: Vec<MemoryUnit> = pairs
            .into_iter()
            .filter_map(|(_, val)| serde_json::from_slice::<MemoryUnit>(&val).ok())
            .filter(|u| {
                u.level == 1
                    && Self::is_local_domain(&u.domain)
                    && self.is_visible_memory_unit(u).unwrap_or(false)
            })
            .filter(|u| {
                min_transaction_time_micros
                    .map(|min_ts| u.transaction_time.timestamp_micros() >= min_ts)
                    .unwrap_or(true)
            })
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
        })
        .await??;

        if !index_pairs.is_empty() {
            return Ok(index_pairs.len());
        }

        // Fallback: scan all units and count level-1 ones.
        let count = tokio::task::spawn_blocking(move || {
            let pairs = store.scan(&prefix_bytes)?;
            let count = pairs
                .into_iter()
                .filter_map(|(_, val)| serde_json::from_slice::<MemoryUnit>(&val).ok())
                .filter(|u| u.level == 1 && Self::is_local_domain(&u.domain))
                .count();
            Ok::<usize, anyhow::Error>(count)
        })
        .await??;

        Ok(count)
    }

    /// Track cumulative L1 growth and return the count range crossed by this update.
    pub async fn bump_l1_count_and_get_range(
        &self,
        user_id: &str,
        delta: usize,
    ) -> Result<(usize, usize)> {
        if delta == 0 {
            let current = self.current_l1_count(user_id).await?;
            return Ok((current, current));
        }

        let key = format!("l1_count:{}", user_id);
        if let Some(bytes) = self.system_kv().get(key.as_bytes())? {
            let current = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])) as usize;
            let updated = current.saturating_add(delta);
            self.system_kv()
                .put(key.as_bytes(), &(updated as u64).to_le_bytes())?;
            return Ok((current, updated));
        }

        // Initialize from persisted storage when the counter has not been materialized yet.
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

    pub async fn fetch_units_with_scores(
        &self,
        user_id: &str,
        results: Vec<(String, f32)>,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        if results.is_empty() {
            return Ok(Vec::new());
        }

        let keys: Vec<String> = results
            .iter()
            .map(|(id, _)| format!("u:{}:unit:{}", user_id, id))
            .collect();
        let store = self._kv.clone();

        let db_results = tokio::task::spawn_blocking(move || {
            let key_bytes: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
            store.multi_get(&key_bytes)
        })
        .await??;

        let mut final_results = Vec::new();
        for (i, res) in db_results.into_iter().enumerate() {
            if let Some(bytes) = res {
                if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(&bytes) {
                    if !self.is_visible_memory_unit(&unit)? {
                        continue;
                    }
                    final_results.push((unit, results[i].1));
                }
            }
        }
        Ok(final_results)
    }

    pub async fn fetch_units_with_scores_global(
        &self,
        results: Vec<(String, f32)>,
    ) -> Result<Vec<(SharedSearchHit, f32)>> {
        if results.is_empty() {
            return Ok(Vec::new());
        }

        let mut final_results = Vec::new();
        for (id, score) in results {
            let parsed = match Uuid::parse_str(&id) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };

            if let Some(hit) = self.get_shared_search_hit_by_index(parsed).await? {
                final_results.push((hit, score));
            }
        }

        Ok(final_results)
    }

    pub async fn fetch_units(&self, user_id: &str, ids: Vec<String>) -> Result<Vec<MemoryUnit>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let keys: Vec<String> = ids
            .iter()
            .map(|id| format!("u:{}:unit:{}", user_id, id))
            .collect();
        let store = self._kv.clone();

        let results = tokio::task::spawn_blocking(move || {
            let key_bytes: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
            store.multi_get(&key_bytes)
        })
        .await??;

        let mut units = Vec::new();
        for res in results {
            if let Some(bytes) = res {
                if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(&bytes) {
                    if !self.is_visible_memory_unit(&unit)? {
                        continue;
                    }
                    units.push(unit);
                }
            }
        }
        Ok(units)
    }

    pub async fn list_memory_units_global(
        &self,
        user_id_filter: Option<&str>,
    ) -> Result<Vec<MemoryUnit>> {
        let prefix = if let Some(user_id) = user_id_filter {
            format!("u:{}:unit:", user_id).into_bytes()
        } else {
            b"u:".to_vec()
        };
        let kv = self._kv.clone();
        let pairs = tokio::task::spawn_blocking(move || kv.scan(&prefix)).await??;

        let mut units = Vec::new();
        for (key, val) in pairs {
            let is_unit_key = if user_id_filter.is_some() {
                true
            } else {
                key.windows(6).any(|window| window == b":unit:")
            };
            if !is_unit_key {
                continue;
            }
            let Ok(unit) = serde_json::from_slice::<MemoryUnit>(&val) else {
                continue;
            };
            if unit.domain == MemoryDomain::Organization || !self.is_visible_memory_unit(&unit)? {
                continue;
            }
            units.push(unit);
        }

        if user_id_filter.is_none() {
            units.extend(self.list_organization_read_units(None).await?);
        }

        Ok(units)
    }

    pub async fn list_organization_read_units(
        &self,
        org_id_filter: Option<&str>,
    ) -> Result<Vec<MemoryUnit>> {
        let mut units = Vec::new();
        for record in self
            .list_organization_knowledge_records(org_id_filter, None)
            .await?
        {
            units.push(
                self.materialize_organization_read_view_for_record(&record)
                    .await?,
            );
        }
        Ok(units)
    }

    pub async fn list_organization_knowledge_detail_records(
        &self,
        org_id_filter: Option<&str>,
    ) -> Result<Vec<OrganizationKnowledgeDetailRecord>> {
        let mut details = Vec::new();
        for snapshot in self
            .list_organization_knowledge_snapshots(org_id_filter)
            .await?
        {
            details.push(
                self.build_organization_knowledge_detail_record_from_snapshot(snapshot)
                    .await,
            );
        }
        Ok(details)
    }

    pub fn get_organization_automation_counter_snapshot(
        &self,
        org_id: &str,
    ) -> Result<OrganizationAutomationCounterSnapshot> {
        Ok(OrganizationAutomationCounterSnapshot {
            org_id: org_id.to_string(),
            auto_approved_total: self
                .get_organization_metric_counter(org_id, "auto_approved_total")?,
            auto_publish_total: self
                .get_organization_metric_counter(org_id, "auto_publish_total")?,
            rebuild_total: self.get_organization_metric_counter(org_id, "rebuild_total")?,
            revoke_total: self.get_organization_metric_counter(org_id, "revoke_total")?,
            merged_publication_total: self
                .get_organization_metric_counter(org_id, "merged_publication_total")?,
        })
    }

    pub fn get_rac_metric_snapshot(&self) -> Result<RacMetricSnapshot> {
        Ok(RacMetricSnapshot {
            fact_extraction_attempt_total: self
                .get_rac_metric_counter("fact_extraction_attempt_total")?,
            fact_extraction_success_total: self
                .get_rac_metric_counter("fact_extraction_success_total")?,
            correction_action_obsolete_total: self
                .get_rac_metric_counter("correction_action_obsolete_total")?,
            correction_action_contradicts_total: self
                .get_rac_metric_counter("correction_action_contradicts_total")?,
            correction_action_reaffirm_total: self
                .get_rac_metric_counter("correction_action_reaffirm_total")?,
            correction_action_ignore_total: self
                .get_rac_metric_counter("correction_action_ignore_total")?,
            tombstone_total: self.get_rac_metric_counter("tombstone_total")?,
        })
    }

    pub fn get_text_index_metric_snapshot(&self) -> TextIndexMetricSnapshot {
        self.index.metrics_snapshot()
    }

    pub fn get_rac_metric_history(&self, hours: usize) -> Result<Vec<RacMetricHistoryPoint>> {
        if hours == 0 {
            return Ok(Vec::new());
        }

        let aligned_now = Self::rac_metric_bucket_start(Utc::now());
        let mut points = std::collections::BTreeMap::new();
        for offset in (0..hours).rev() {
            let bucket_start = aligned_now - chrono::Duration::hours(offset as i64);
            let bucket_key = bucket_start.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
            points.insert(
                bucket_key.clone(),
                RacMetricHistoryPoint {
                    bucket_start: bucket_key,
                    ..Default::default()
                },
            );
        }

        for (key, value) in self.system_kv().scan(b"rac_metric_bucket:")? {
            let Ok(key_str) = String::from_utf8(key) else {
                continue;
            };
            let Some(rest) = key_str.strip_prefix("rac_metric_bucket:") else {
                continue;
            };
            let Some((bucket_start, metric)) = rest.rsplit_once(':') else {
                continue;
            };
            let Some(point) = points.get_mut(bucket_start) else {
                continue;
            };
            let count = u64::from_le_bytes(value.try_into().unwrap_or([0; 8])) as usize;
            match metric {
                "fact_extraction_attempt_total" => point.fact_extraction_attempt_total += count,
                "fact_extraction_success_total" => point.fact_extraction_success_total += count,
                "correction_action_obsolete_total" => {
                    point.correction_action_obsolete_total += count
                }
                "correction_action_contradicts_total" => {
                    point.correction_action_contradicts_total += count
                }
                "correction_action_reaffirm_total" => {
                    point.correction_action_reaffirm_total += count
                }
                "correction_action_ignore_total" => point.correction_action_ignore_total += count,
                "tombstone_total" => point.tombstone_total += count,
                _ => {}
            }
        }

        Ok(points.into_values().collect())
    }

    pub fn list_recent_rac_decisions(&self, limit: usize) -> Result<Vec<RacDecisionRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut records = self
            .system_kv()
            .scan(b"rac_decision:")?
            .into_iter()
            .filter_map(|(_, value)| serde_json::from_slice::<RacDecisionRecord>(&value).ok())
            .collect::<Vec<_>>();

        records.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.source_unit_id.cmp(&left.source_unit_id))
                .then_with(|| right.target_unit_id.cmp(&left.target_unit_id))
        });
        records.truncate(limit);
        Ok(records)
    }

    pub fn get_rac_review(&self, review_id: &str) -> Result<Option<RacReviewRecord>> {
        let key = Self::rac_review_key(review_id);
        Ok(self
            .system_kv()
            .get(key.as_bytes())?
            .and_then(|bytes| serde_json::from_slice::<RacReviewRecord>(&bytes).ok()))
    }

    pub fn list_rac_reviews(
        &self,
        status_filter: Option<RacReviewStatus>,
        user_id_filter: Option<&str>,
        org_id_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RacReviewRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut records = self
            .system_kv()
            .scan(b"rac_review:")?
            .into_iter()
            .filter_map(|(_, value)| serde_json::from_slice::<RacReviewRecord>(&value).ok())
            .filter(|record| {
                status_filter
                    .as_ref()
                    .map_or(true, |status| &record.status == status)
            })
            .filter(|record| user_id_filter.map_or(true, |user_id| record.user_id == user_id))
            .filter(|record| {
                org_id_filter.map_or(true, |org_id| record.org_id.as_deref() == Some(org_id))
            })
            .collect::<Vec<_>>();

        records.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.source_unit_id.cmp(&left.source_unit_id))
                .then_with(|| right.target_unit_id.cmp(&left.target_unit_id))
        });
        records.truncate(limit);
        Ok(records)
    }

    fn store_rac_review(&self, review: &RacReviewRecord) -> Result<()> {
        self.system_kv().put(
            Self::rac_review_key(&review.review_id).as_bytes(),
            &serde_json::to_vec(review)?,
        )
    }

    pub async fn apply_manual_memory_correction(
        &self,
        user_id: &str,
        source_unit_id: Uuid,
        target_unit_id: Uuid,
        kind: MemoryCorrectionKind,
        reason: String,
        confidence: f32,
        stage: &str,
    ) -> Result<Vec<Uuid>> {
        let source_unit = self
            .get_memory_unit_including_forgotten(user_id, source_unit_id)?
            .ok_or_else(|| anyhow!("source memory unit {} not found", source_unit_id))?;
        if source_unit.user_id != user_id {
            return Err(anyhow!("source memory unit scope mismatch"));
        }

        self.apply_memory_correction_actions_with_stage(
            &source_unit,
            vec![MemoryCorrectionAction {
                target_id: target_unit_id,
                kind,
                reason,
                confidence,
            }],
            stage,
        )
        .await
    }

    pub async fn resolve_rac_review(
        &self,
        review_id: &str,
        approve: bool,
        reviewer: Option<String>,
        reviewer_note: Option<String>,
    ) -> Result<Option<RacReviewRecord>> {
        let Some(mut review) = self.get_rac_review(review_id)? else {
            return Ok(None);
        };
        if review.status != RacReviewStatus::Pending {
            return Ok(Some(review));
        }

        if approve {
            let kind = match review.action.as_str() {
                "obsolete" => MemoryCorrectionKind::Obsolete,
                "contradicts" => MemoryCorrectionKind::Contradicts,
                "reaffirm" => MemoryCorrectionKind::Reaffirm,
                "ignore" => MemoryCorrectionKind::Ignore,
                _ => return Err(anyhow!("unsupported review action {}", review.action)),
            };
            let confidence = if kind == MemoryCorrectionKind::Obsolete {
                review.confidence.max(OBSOLETE_ACTION_MIN_CONFIDENCE)
            } else {
                review.confidence
            };
            self.apply_manual_memory_correction(
                &review.user_id,
                review.source_unit_id,
                review.target_unit_id,
                kind,
                reviewer_note
                    .clone()
                    .unwrap_or_else(|| review.reason.clone()),
                confidence,
                "review_approve",
            )
            .await?;
            review.status = RacReviewStatus::Approved;
        } else {
            review.status = RacReviewStatus::Rejected;
        }

        review.updated_at = Utc::now();
        review.reviewer = reviewer;
        review.reviewer_note = reviewer_note;
        self.store_rac_review(&review)?;
        Ok(Some(review))
    }

    async fn list_organization_knowledge_records(
        &self,
        org_id_filter: Option<&str>,
        valid_time: Option<&TimeRange>,
    ) -> Result<Vec<OrganizationKnowledgeRecord>> {
        let system_kv = self.system_kv();
        let pairs = tokio::task::spawn_blocking(move || system_kv.scan(b"organization_knowledge:"))
            .await??;

        Ok(pairs
            .into_iter()
            .filter_map(|(_, val)| serde_json::from_slice::<OrganizationKnowledgeRecord>(&val).ok())
            .filter(|record| {
                org_id_filter
                    .map(|org_id| record.org_id == org_id)
                    .unwrap_or(true)
            })
            .filter(|record| Self::matches_valid_time_filter(record.valid_time, valid_time))
            .collect())
    }

    async fn materialize_organization_search_hits(
        &self,
        hits: Vec<(OrganizationKnowledgeRecord, f32)>,
    ) -> Result<Vec<(SharedSearchHit, f32)>> {
        let mut materialized = Vec::with_capacity(hits.len());
        for (record, score) in hits {
            let unit = self
                .materialize_organization_read_view_for_record(&record)
                .await?;
            materialized.push((
                SharedSearchHit::organization_knowledge(&record, unit),
                score,
            ));
        }
        Ok(materialized)
    }

    async fn search_organization_knowledge_records(
        &self,
        org_id: &str,
        query_text: &str,
        vector: &[f32],
        limit: usize,
        min_score: Option<f32>,
        valid_time: Option<TimeRange>,
    ) -> Result<Vec<(OrganizationKnowledgeRecord, f32)>> {
        let mut candidates = Vec::new();
        for record in self
            .list_organization_knowledge_records(Some(org_id), valid_time.as_ref())
            .await?
        {
            let score = Self::organization_similarity_score(&record, query_text, vector);
            if score > 0.0 {
                candidates.push((record, score));
            }
        }

        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        if candidates.len() > limit * 3 {
            candidates.truncate(limit * 3);
        }

        let mut reranked = self
            .reranker
            .rerank(
                query_text,
                &self._kv,
                self.materialize_organization_search_hits(candidates)
                    .await?
                    .iter()
                    .map(|(hit, score)| (hit.memory_unit().clone(), *score))
                    .collect(),
            )
            .await?;
        let threshold = min_score.unwrap_or(0.3);
        reranked.retain(|(_, score)| *score >= threshold);
        let mut record_hits = Vec::with_capacity(reranked.len());
        for (unit, score) in reranked {
            let Some(record) = self.load_organization_knowledge(unit.id)? else {
                continue;
            };
            record_hits.push((record, score));
        }
        Ok(record_hits)
    }

    async fn search_organization_knowledge_text(
        &self,
        org_id: &str,
        query_text: &str,
        limit: usize,
        valid_time: Option<TimeRange>,
    ) -> Result<Vec<SharedSearchHit>> {
        let zero_vector = Vec::new();
        let mut scored = self
            .search_organization_knowledge_records(
                org_id,
                query_text,
                &zero_vector,
                limit,
                Some(0.01),
                valid_time,
            )
            .await?;
        if scored.len() > limit {
            scored.truncate(limit);
        }
        Ok(self
            .materialize_organization_search_hits(scored)
            .await?
            .into_iter()
            .map(|(hit, _)| hit)
            .collect())
    }

    // ── 图查询优化 API ──────────────────────────────────────────────────

    /// 批量查询多个节点的邻居（使用批量优化）
    pub async fn batch_get_neighbors(
        &self,
        user_id: &str,
        node_ids: &[Uuid],
    ) -> Result<HashMap<Uuid, Vec<GraphEdge>>> {
        self.batch_executor
            .batch_get_outgoing_edges(user_id, node_ids)
            .await
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
        self.batch_executor
            .batch_multi_hop_traverse(user_id, start_nodes, max_hops, min_weight)
            .await
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
    use crate::fact_extraction::{
        MemoryFactAttribute, MemoryFactSubject, MemoryFactValueKind, MemoryFactValuePayload,
    };
    use chrono::{TimeZone, Utc};
    use memorose_common::{
        EventContent, ForgetMode, ForgetTargetKind, ForgettingTombstone, StoredMemoryFact,
    };
    use std::sync::Arc;
    use tempfile::tempdir;
    use uuid::Uuid;

    const TEST_USER: &str = "test_user";
    #[tokio::test]
    async fn test_engine_integration() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        // 1. Test L0 Ingestion
        let stream_id = Uuid::new_v4();
        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Text("L0 Test".to_string()),
        );
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
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "L1 Insight".to_string(),
            Some(embedding.clone()),
        );

        engine.store_memory_unit(unit.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        // Search by Vector
        let filter = engine.build_user_filter(TEST_USER, None);
        let similar = engine
            .search_similar(TEST_USER, &embedding, 1, filter)
            .await?;
        assert_eq!(similar.len(), 1);
        assert_eq!(similar[0].0.id, unit.id);

        // Search by Text
        let text_hits = engine
            .search_text(TEST_USER, "Insight", 1, true, None)
            .await?;
        assert_eq!(text_hits.len(), 1);
        assert_eq!(text_hits[0].id, unit.id);

        // 3. Test Forgetting Mechanism
        let mut weak_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Weak Memory".to_string(),
            None,
        );
        weak_unit.importance = 0.15;
        engine.store_memory_unit(weak_unit.clone()).await?;

        // Decay: 0.15 * 0.5 = 0.075
        engine.decay_importance(TEST_USER, 0.5).await?;

        // Prune memories below 0.1
        let pruned_count = engine.prune_memories(TEST_USER, 0.1).await?;
        assert!(pruned_count >= 1);

        // Verify it's gone
        let search_gone = engine.search_text(TEST_USER, "Weak", 1, true, None).await?;
        assert!(search_gone.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_auto_linking() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        // 1. Store first memory
        let mut emb1 = vec![0.0; 384];
        emb1[0] = 1.0;
        let unit1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Apple is a fruit".to_string(),
            Some(emb1),
        );
        engine.store_memory_unit(unit1.clone()).await?;

        // 2. Store second similar memory
        let mut emb2 = vec![0.0; 384];
        emb2[0] = 0.99;
        let unit2 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Apples are sweet".to_string(),
            Some(emb2),
        );
        engine.store_memory_unit(unit2.clone()).await?;

        // Verify graph edge exists from unit2 to unit1
        let edges = engine
            .graph()
            .get_outgoing_edges(TEST_USER, unit2.id)
            .await?;
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
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut emb1 = vec![0.0; 384];
        emb1[0] = 1.0;
        let mut unit1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I love cats".to_string(),
            Some(emb1.clone()),
        );
        unit1.transaction_time = Utc::now() - chrono::Duration::days(1);
        engine.store_memory_unit(unit1.clone()).await?;

        let mut emb2 = vec![0.0; 384];
        emb2[0] = 0.95;
        let unit2 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I hate cats now".to_string(),
            Some(emb2.clone()),
        );
        engine.store_memory_unit(unit2.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let results = engine
            .search_text(TEST_USER, "cats", 10, true, None)
            .await?;

        println!(
            "Arbitration results: {:?}",
            results.iter().map(|u| &u.content).collect::<Vec<_>>()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_community_flow() -> Result<()> {
        let has_google = std::env::var("GOOGLE_API_KEY")
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        let has_openai = std::env::var("OPENAI_API_KEY")
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        if !has_google && !has_openai {
            return Ok(());
        }

        let temp_dir = tempdir()?;
        let engine =
            match MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
                .await
            {
                Ok(e) => e,
                Err(_) => return Ok(()), // skip if backend fails to initialize
            };
        let stream_id = Uuid::new_v4();

        let mut emb1 = vec![0.0; 768];
        emb1[0] = 1.0;
        let u1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Rust is memory safe".to_string(),
            Some(emb1.clone()),
        );
        engine.store_memory_unit(u1.clone()).await?;

        let mut emb2 = vec![0.0; 768];
        emb2[0] = 0.95;
        let u2 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "The borrow checker prevents data races".to_string(),
            Some(emb2.clone()),
        );
        engine.store_memory_unit(u2.clone()).await?;

        let mut emb3 = vec![0.0; 768];
        emb3[0] = 0.90;
        let u3 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Ownership is key to Rust".to_string(),
            Some(emb3.clone()),
        );
        engine.store_memory_unit(u3.clone()).await?;

        let _ = engine.process_communities(TEST_USER).await;

        let prefix = format!("u:{}:unit:", TEST_USER);
        let kv = engine._kv.clone();
        let prefix_bytes = prefix.into_bytes();
        let all_units: Vec<(Vec<u8>, Vec<u8>)> =
            tokio::task::spawn_blocking(move || kv.scan(&prefix_bytes)).await??;

        let l2_units: Vec<MemoryUnit> = all_units
            .into_iter()
            .filter_map(|(_, v): (Vec<u8>, Vec<u8>)| serde_json::from_slice::<MemoryUnit>(&v).ok())
            .filter(|u| u.level == 2)
            .collect();

        if !l2_units.is_empty() {
            let l2 = &l2_units[0];
            println!(
                "Generated L2: {} - {}",
                l2.keywords.first().unwrap_or(&"No Name".to_string()),
                l2.content
            );

            assert!(l2.references.len() >= 3);
            assert!(
                !l2.keywords.is_empty(),
                "L2 unit should have keywords (at least title)"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_feedback_loop() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let u1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Memory A".into(),
            None,
        );
        let u2 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Memory B".into(),
            None,
        );
        engine.store_memory_unit(u1.clone()).await?;
        engine.store_memory_unit(u2.clone()).await?;

        engine
            .apply_reranker_feedback(
                TEST_USER,
                vec![u1.id.to_string(), u2.id.to_string()],
                vec![],
            )
            .await?;

        let edges = engine.graph().get_outgoing_edges(TEST_USER, u1.id).await?;
        let edge = edges
            .iter()
            .find(|e| e.target_id == u2.id)
            .expect("Edge should be created by reinforcement");
        assert!((edge.weight - 0.1).abs() < 0.001);

        engine
            .apply_reranker_feedback(
                TEST_USER,
                vec![u1.id.to_string(), u2.id.to_string()],
                vec![],
            )
            .await?;
        let edges_updated = engine.graph().get_outgoing_edges(TEST_USER, u1.id).await?;
        let edge_updated = edges_updated.iter().find(|e| e.target_id == u2.id).unwrap();
        assert!((edge_updated.weight - 0.2).abs() < 0.001);

        Ok(())
    }

    #[tokio::test]
    async fn test_temporal_text_search() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut u1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Memorose started in 2020".into(),
            None,
        );
        u1.valid_time =
            Some(chrono::TimeZone::with_ymd_and_hms(&Utc, 2020, 1, 1, 0, 0, 0).unwrap());
        engine.store_memory_unit(u1.clone()).await?;

        let mut u2 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Memorose is advanced in 2026".into(),
            None,
        );
        u2.valid_time =
            Some(chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 1, 1, 0, 0, 0).unwrap());
        engine.store_memory_unit(u2.clone()).await?;

        engine.index.commit()?;
        engine.index.reload()?;

        let range = TimeRange {
            start: Some(chrono::TimeZone::with_ymd_and_hms(&Utc, 2025, 1, 1, 0, 0, 0).unwrap()),
            end: Some(chrono::TimeZone::with_ymd_and_hms(&Utc, 2027, 1, 1, 0, 0, 0).unwrap()),
        };

        let hits = engine
            .search_text(TEST_USER, "Memorose", 10, false, Some(range))
            .await?;

        assert_eq!(
            hits.len(),
            1,
            "Should only return 1 hit due to time filtering"
        );
        assert_eq!(hits[0].id, u2.id);

        Ok(())
    }

    #[tokio::test]
    async fn test_search_filters() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut u1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Highly relevant".into(),
            Some(vec![1.0; 768]),
        );
        u1.importance = 1.0;
        let mut u2 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Less relevant".into(),
            Some(vec![0.5; 768]),
        );
        u2.importance = 0.5;

        engine
            .store_memory_units(vec![u1.clone(), u2.clone()])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let results = engine
            .search_hybrid(
                TEST_USER,
                None,
                None,
                "relevant",
                &vec![1.0; 768],
                10,
                false,
                Some(0.3),
                0,
                None,
                None,
            )
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.id, u1.id);

        Ok(())
    }

    #[tokio::test]
    async fn test_search_hybrid_applies_org_filter_before_ranking() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut org_unit = MemoryUnit::new(
            Some("org_alpha".into()),
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Alpha org incident playbook".into(),
            Some(vec![1.0; 768]),
        );
        org_unit.importance = 1.0;

        let mut other_unit = MemoryUnit::new(
            Some("org_beta".into()),
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Beta org incident playbook".into(),
            Some(vec![1.0; 768]),
        );
        other_unit.importance = 1.0;

        engine
            .store_memory_units(vec![org_unit.clone(), other_unit.clone()])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let results = engine
            .search_hybrid(
                TEST_USER,
                Some("org_alpha"),
                None,
                "incident playbook",
                &vec![1.0; 768],
                10,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.id, org_unit.id);

        Ok(())
    }

    struct MockReranker;
    #[async_trait::async_trait]
    impl crate::reranker::Reranker for MockReranker {
        async fn rerank(
            &self,
            _query: &str,
            _store: &KvStore,
            _candidates: Vec<(MemoryUnit, f32)>,
        ) -> Result<Vec<(MemoryUnit, f32)>> {
            Ok(vec![])
        }
        async fn apply_feedback(
            &self,
            _store: &KvStore,
            _c: Vec<String>,
            _r: Vec<String>,
        ) -> Result<()> {
            Ok(())
        }
    }

    struct MockCorrectionLLM {
        response: String,
    }

    #[async_trait::async_trait]
    impl crate::llm::LLMClient for MockCorrectionLLM {
        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 3],
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn generate(&self, _prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: self.response.clone(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<crate::llm::CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: crate::llm::CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn summarize_group(
            &self,
            texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: texts.join("\n"),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn describe_image(
            &self,
            image_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: image_url_or_base64.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn transcribe(
            &self,
            audio_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: audio_url_or_base64.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn describe_video(&self, video_url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: video_url.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }
    }

    struct PromptMatchingCorrectionLLM {
        responses: Vec<(String, String)>,
    }

    struct PanicOnGenerateLLM;

    #[async_trait::async_trait]
    impl crate::llm::LLMClient for PromptMatchingCorrectionLLM {
        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 3],
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn generate(&self, prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            let data = self
                .responses
                .iter()
                .find_map(|(needle, response)| prompt.contains(needle).then(|| response.clone()))
                .unwrap_or_else(|| "null".to_string());

            Ok(crate::llm::LLMResponse {
                data,
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<crate::llm::CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: crate::llm::CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn summarize_group(
            &self,
            texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: texts.join("\n"),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn describe_image(
            &self,
            image_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: image_url_or_base64.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn transcribe(
            &self,
            audio_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: audio_url_or_base64.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn describe_video(&self, video_url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: video_url.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }
    }

    #[async_trait::async_trait]
    impl crate::llm::LLMClient for PanicOnGenerateLLM {
        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 3],
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn generate(&self, _prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            panic!("generate should not be called when persisted extracted facts exist")
        }

        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<crate::llm::CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: crate::llm::CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn summarize_group(
            &self,
            texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: texts.join("\n"),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn describe_image(
            &self,
            image_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: image_url_or_base64.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn transcribe(
            &self,
            audio_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: audio_url_or_base64.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn describe_video(&self, video_url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: video_url.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }
    }

    #[tokio::test]
    async fn test_custom_reranker() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_reranker(std::sync::Arc::new(MockReranker));

        let u1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Test".into(),
            Some(vec![1.0; 768]),
        );
        engine.store_memory_unit(u1).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let results = engine
            .search_hybrid(
                TEST_USER,
                None,
                None,
                "Test",
                &vec![1.0; 768],
                10,
                false,
                None,
                0,
                None,
                None,
            )
            .await?;
        assert!(results.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_hard_delete_clears_forgetting_tombstone() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Delete me after logical forgetting".into(),
            Some(vec![1.0; 768]),
        );
        let unit_id = unit.id;
        engine.store_memory_unit(unit).await?;

        let tombstone = ForgettingTombstone {
            user_id: TEST_USER.into(),
            org_id: None,
            target_kind: memorose_common::ForgetTargetKind::MemoryUnit,
            target_id: unit_id.to_string(),
            reason_query: "forget this".into(),
            created_at: Utc::now(),
            preview_id: Some(Uuid::new_v4().to_string()),
            mode: memorose_common::ForgetMode::Logical,
        };
        engine.mark_memory_unit_forgotten(TEST_USER, unit_id, &tombstone)?;
        assert!(engine.is_memory_unit_forgotten(TEST_USER, unit_id)?);

        engine.delete_memory_unit_hard(TEST_USER, unit_id).await?;

        assert!(!engine.is_memory_unit_forgotten(TEST_USER, unit_id)?);
        assert!(engine.get_memory_unit(TEST_USER, unit_id).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_residence_update() -> Result<()> {
        let mut unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing.".into(),
            None,
        );
        unit.keywords = vec!["Profile".into()];

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::User,
                subject_key: "user:self".into(),
                attribute: MemoryFactAttribute::Residence,
                value: "Beijing".into(),
                canonical_value: "beijing".into(),
                value_kind: MemoryFactValueKind::City,
                value_payload: MemoryFactValuePayload::City {
                    name: "beijing".into(),
                },
                change_type: MemoryFactChangeType::Update,
                confidence: 90,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_defaults_local_fact_subject_to_user() -> Result<()>
    {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Home base: Shanghai".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::User,
                subject_key: "user:self".into(),
                attribute: MemoryFactAttribute::Residence,
                value: "Shanghai".into(),
                canonical_value: "shanghai".into(),
                value_kind: MemoryFactValueKind::City,
                value_payload: MemoryFactValuePayload::City {
                    name: "shanghai".into(),
                },
                change_type: MemoryFactChangeType::Reaffirm,
                confidence: 90,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_preference_contradiction() -> Result<()> {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I do not like sushi".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::User,
                subject_key: "user:self".into(),
                attribute: MemoryFactAttribute::Preference,
                value: "sushi".into(),
                canonical_value: "sushi".into(),
                value_kind: MemoryFactValueKind::Freeform,
                value_payload: MemoryFactValuePayload::Freeform {
                    text: "sushi".into(),
                },
                change_type: MemoryFactChangeType::Contradiction,
                confidence: 85,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_contact_with_canonical_value() -> Result<()>
    {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "My email is Dylan@Example.COM.".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::User,
                subject_key: "user:self".into(),
                attribute: MemoryFactAttribute::Contact,
                value: "Dylan@Example.COM".into(),
                canonical_value: "dylan@example.com".into(),
                value_kind: MemoryFactValueKind::Email,
                value_payload: MemoryFactValuePayload::Email {
                    address: "dylan@example.com".into(),
                },
                change_type: MemoryFactChangeType::Reaffirm,
                confidence: 80,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_skill_addition() -> Result<()> {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I also speak Japanese.".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::User,
                subject_key: "user:self".into(),
                attribute: MemoryFactAttribute::Skill,
                value: "Japanese".into(),
                canonical_value: "japanese".into(),
                value_kind: MemoryFactValueKind::SkillName,
                value_payload: MemoryFactValuePayload::SkillName {
                    name: "japanese".into(),
                },
                change_type: MemoryFactChangeType::Addition,
                confidence: 75,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_external_named_subject() -> Result<()> {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Alice lives in Beijing".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::External,
                subject_key: "external:alice".into(),
                attribute: MemoryFactAttribute::Residence,
                value: "Beijing".into(),
                canonical_value: "beijing".into(),
                value_kind: MemoryFactValueKind::City,
                value_payload: MemoryFactValuePayload::City {
                    name: "beijing".into(),
                },
                change_type: MemoryFactChangeType::Reaffirm,
                confidence: 90,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_named_organization_subject() -> Result<()>
    {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Acme Corp is based in Shanghai".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::Organization,
                subject_key: "organization:acme_corp".into(),
                attribute: MemoryFactAttribute::Residence,
                value: "Shanghai".into(),
                canonical_value: "shanghai".into(),
                value_kind: MemoryFactValueKind::City,
                value_payload: MemoryFactValuePayload::City {
                    name: "shanghai".into(),
                },
                change_type: MemoryFactChangeType::Reaffirm,
                confidence: 90,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_phone_payload() -> Result<()> {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "My phone is +1 (415) 555-2671.".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::User,
                subject_key: "user:self".into(),
                attribute: MemoryFactAttribute::Contact,
                value: "+1 (415) 555-2671".into(),
                canonical_value: "14155552671".into(),
                value_kind: MemoryFactValueKind::Phone,
                value_payload: MemoryFactValuePayload::Phone {
                    digits: "14155552671".into(),
                },
                change_type: MemoryFactChangeType::Reaffirm,
                confidence: 80,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_schedule_payload() -> Result<()> {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "The meeting moved to 2026-05-01 15:00".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::External,
                subject_key: "external:unknown".into(),
                attribute: MemoryFactAttribute::Schedule,
                value: "2026-05-01 15:00".into(),
                canonical_value: "2026-05-01 15:00".into(),
                value_kind: MemoryFactValueKind::DateTimeLike,
                value_payload: MemoryFactValuePayload::DateTimeLike {
                    text: "2026-05-01 15:00".into(),
                },
                change_type: MemoryFactChangeType::Update,
                confidence: 70,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_prefers_slot_keyword_matches() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Home base: Shanghai".into(),
            None,
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_id = old_unit.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_unit, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert_eq!(candidates.first().map(|unit| unit.id), Some(old_id));
        assert!(candidates.iter().any(|unit| unit.id == old_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_prefers_persisted_candidate_facts_when_content_is_opaque(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Profile metadata sync completed".into(),
            None,
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(2);
        old_unit.extracted_facts = vec![StoredMemoryFact {
            subject: "user".into(),
            subject_ref: Some("user:self".into()),
            subject_name: None,
            attribute: "residence".into(),
            value: "Shanghai".into(),
            canonical_value: Some("shanghai".into()),
            change_type: "reaffirm".into(),
            temporal_status: Some("current".into()),
            polarity: Some("positive".into()),
            evidence_span: Some("home city is Shanghai".into()),
            confidence: 0.91,
        }];
        let old_id = old_unit.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_unit, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert_eq!(candidates.first().map(|unit| unit.id), Some(old_id));
        assert!(candidates.iter().any(|unit| unit.id == old_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_uses_llm_fact_fallback() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Home base: Shanghai".into(),
            None,
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_id = old_unit.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_unit, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            MockCorrectionLLM {
                response: r#"{"subject":"user","attribute":"residence","value":"Beijing","change_type":"update","confidence":0.92}"#
                    .into(),
            },
        ));

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Current city: Beijing".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert_eq!(candidates.first().map(|unit| unit.id), Some(old_id));
        assert!(candidates.iter().any(|unit| unit.id == old_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_multiple_llm_facts() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_residence = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Home base: Shanghai".into(),
            None,
        );
        old_residence.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_residence_id = old_residence.id;

        let mut old_contact = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "My email is old@example.com".into(),
            None,
        );
        old_contact.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_contact_id = old_contact.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_residence, old_contact, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            MockCorrectionLLM {
                response: r#"{"facts":[{"subject":"user","attribute":"residence","value":"Beijing","change_type":"update","confidence":0.93},{"subject":"user","attribute":"contact","value":"dylan@example.com","change_type":"update","confidence":0.91}]}"#
                    .into(),
            },
        ));

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I now live in Beijing and my email is dylan@example.com".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_residence_id));
        assert!(candidates.iter().any(|unit| unit.id == old_contact_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_multiple_rule_facts_bilingual(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_residence = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "我住在上海".into(),
            None,
        );
        old_residence.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_residence_id = old_residence.id;

        let mut old_contact = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "我的邮箱是 old@example.com".into(),
            None,
        );
        old_contact.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_contact_id = old_contact.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "我喜欢北京烤鸭".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_residence, old_contact, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "我现在住在北京，我的邮箱是 new@example.com".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_residence_id));
        assert!(candidates.iter().any(|unit| unit.id == old_contact_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_multi_clause_history_update(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_employment = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I work at OpenAI".into(),
            None,
        );
        old_employment.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_employment_id = old_employment.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Favorite food is ramen".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_employment, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I used to work at OpenAI, now work at Anthropic".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert_eq!(
            candidates.first().map(|unit| unit.id),
            Some(old_employment_id)
        );
        assert!(candidates.iter().any(|unit| unit.id == old_employment_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_explicit_contact_transition(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_contact = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "My email is old@example.com".into(),
            None,
        );
        old_contact.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_contact_id = old_contact.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Beijing".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_contact, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "My email changed from old@example.com to new@example.com".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert_eq!(candidates.first().map(|unit| unit.id), Some(old_contact_id));
        assert!(candidates.iter().any(|unit| unit.id == old_contact_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_same_sentence_mixed_slot_transitions(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_residence = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            None,
        );
        old_residence.transaction_time = Utc::now() - chrono::Duration::days(4);
        let old_residence_id = old_residence.id;

        let mut old_contact = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "My email is old@example.com".into(),
            None,
        );
        old_contact.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_contact_id = old_contact.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Favorite food is sushi".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_residence, old_contact, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I moved from Shanghai to Beijing and changed my email from old@example.com to new@example.com".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_residence_id));
        assert!(candidates.iter().any(|unit| unit.id == old_contact_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_long_mixed_input_with_noise(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_residence = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            None,
        );
        old_residence.transaction_time = Utc::now() - chrono::Duration::days(4);
        let old_residence_id = old_residence.id;

        let mut old_contact = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "My email is old@example.com".into(),
            None,
        );
        old_contact.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_contact_id = old_contact.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "The call is at 4pm tomorrow".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_residence, old_contact, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Can you remind me where I used to live? btw, quick update: I now live in Beijing, and my email changed from old@example.com to new@example.com. also, the call is at around 3pm tmrw lol".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_residence_id));
        assert!(candidates.iter().any(|unit| unit.id == old_contact_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_long_mixed_forget_and_update_input(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_employment = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I work at OpenAI".into(),
            None,
        );
        old_employment.transaction_time = Utc::now() - chrono::Duration::days(4);
        let old_employment_id = old_employment.id;

        let mut old_contact = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "My email is old@example.com".into(),
            None,
        );
        old_contact.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_contact_id = old_contact.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "The call is tomorrow at 3pm".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_employment, old_contact, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Actually, quick cleanup: I no longer work at OpenAI, and my email changed from old@example.com to new@example.com. Can you remind me about the call tomorrow?".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_employment_id));
        assert!(candidates.iter().any(|unit| unit.id == old_contact_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_plan_memory_correction_actions_supports_long_mixed_forget_and_update_input(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_employment = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I work at OpenAI".into(),
            Some(vec![1.0; 768]),
        );
        old_employment.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_employment_id = old_employment.id;

        let mut old_contact = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "My email is old@example.com".into(),
            Some(vec![1.0; 768]),
        );
        old_contact.transaction_time = Utc::now() - chrono::Duration::days(1);
        let old_contact_id = old_contact.id;

        engine
            .store_memory_units(vec![old_employment, old_contact])
            .await?;

        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            PromptMatchingCorrectionLLM {
                responses: vec![(
                    "memory correction engine".into(),
                    format!(
                        r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Employment removed","confidence":0.96}},{{"target_id":"{}","action":"OBSOLETE","reason":"Email updated","confidence":0.97}}]"#,
                        old_employment_id, old_contact_id
                    ),
                )],
            },
        ));

        let mut preview_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Actually, quick cleanup: I no longer work at OpenAI, and my email changed from old@example.com to new@example.com. Can you remind me about the call tomorrow?".into(),
            Some(vec![1.0; 768]),
        );
        preview_unit.transaction_time = Utc::now();

        let actions = engine
            .plan_memory_correction_actions(&preview_unit, 8)
            .await?;

        assert!(actions.iter().any(|action| {
            action.target_id == old_employment_id
                && action.kind == MemoryCorrectionKind::Obsolete
                && action.effect == RacDecisionEffect::Tombstone
                && action.relation == Some(RelationType::EvolvedTo)
        }));
        assert!(actions.iter().any(|action| {
            action.target_id == old_contact_id
                && action.kind == MemoryCorrectionKind::Obsolete
                && action.effect == RacDecisionEffect::Tombstone
                && action.relation == Some(RelationType::EvolvedTo)
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_long_mixed_forget_and_addition_input(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_employment = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I work at OpenAI".into(),
            None,
        );
        old_employment.transaction_time = Utc::now() - chrono::Duration::days(4);
        let old_employment_id = old_employment.id;

        let mut unrelated_preference = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I love ramen".into(),
            None,
        );
        unrelated_preference.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_employment, unrelated_preference])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Actually, I no longer work at OpenAI. I also speak Japanese. I also love skiing."
                .into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_employment_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_self_correction_reversal_input(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_residence = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            None,
        );
        old_residence.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_residence_id = old_residence.id;

        engine.store_memory_unit(old_residence).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Shanghai. Actually, scratch that, I now live in Singapore.".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_residence_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_ignores_non_assertive_hypothetical_input(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_residence = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Beijing".into(),
            None,
        );
        old_residence.transaction_time = Utc::now() - chrono::Duration::days(3);

        engine.store_memory_unit(old_residence).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut hypothetical_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "If I move to Beijing next month, remind me to update my profile.".into(),
            None,
        );
        hypothetical_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&hypothetical_unit, 4)
            .await?;

        assert!(candidates.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_reported_speech_subject_attribution(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_residence = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "John Doe lives in Shanghai".into(),
            None,
        );
        old_residence.transaction_time = Utc::now() - chrono::Duration::days(5);
        let old_residence_id = old_residence.id;

        engine.store_memory_unit(old_residence).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "John Doe said \"I now live in Beijing\"".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_residence_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_according_to_subject_carry(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_employment = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "John Doe works at Anthropic".into(),
            None,
        );
        old_employment.transaction_time = Utc::now() - chrono::Duration::days(6);
        let old_employment_id = old_employment.id;

        engine.store_memory_unit(old_employment).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "According to John Doe, he now works at OpenAI".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_employment_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_plan_memory_correction_actions_supports_long_mixed_forget_and_addition_input(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_employment = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I work at OpenAI".into(),
            Some(vec![1.0; 768]),
        );
        old_employment.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_employment_id = old_employment.id;

        engine.store_memory_unit(old_employment).await?;
        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            PromptMatchingCorrectionLLM {
                responses: vec![(
                    "memory correction engine".into(),
                    format!(
                        r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Employment removed","confidence":0.96}}]"#,
                        old_employment_id
                    ),
                )],
            },
        ));

        let mut preview_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Actually, I no longer work at OpenAI. I also speak Japanese. I also love skiing."
                .into(),
            Some(vec![1.0; 768]),
        );
        preview_unit.transaction_time = Utc::now();

        let actions = engine
            .plan_memory_correction_actions(&preview_unit, 8)
            .await?;

        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].target_id, old_employment_id);
        assert_eq!(actions[0].kind, MemoryCorrectionKind::Obsolete);
        assert_eq!(actions[0].effect, RacDecisionEffect::Tombstone);

        Ok(())
    }

    #[tokio::test]
    async fn test_rac_metric_snapshot_tracks_extraction_actions_and_tombstones() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Home base: Shanghai".into(),
            None,
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_id = old_unit.id;
        engine.store_memory_unit(old_unit).await?;

        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            PromptMatchingCorrectionLLM {
                responses: vec![
                    (
                        "memory fact extraction engine".into(),
                        r#"{"subject":"user","attribute":"residence","value":"Beijing","change_type":"update","confidence":0.92}"#
                            .into(),
                    ),
                    (
                        "memory correction engine".into(),
                        format!(
                            r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Residence updated","confidence":0.96}}]"#,
                            old_id
                        ),
                    ),
                ],
            },
        ));

        let before = engine.get_rac_metric_snapshot()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Current city: Beijing".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let affected = engine.reconcile_conflicting_memory_unit(&new_unit).await?;
        let after = engine.get_rac_metric_snapshot()?;
        let history = engine.get_rac_metric_history(4)?;

        assert_eq!(affected, vec![old_id]);
        assert!(
            after.fact_extraction_attempt_total > before.fact_extraction_attempt_total,
            "expected extraction attempts to increase"
        );
        assert!(
            after.fact_extraction_success_total > before.fact_extraction_success_total,
            "expected extraction successes to increase"
        );
        assert_eq!(
            after.correction_action_obsolete_total,
            before.correction_action_obsolete_total + 1
        );
        assert_eq!(after.tombstone_total, before.tombstone_total + 1);
        assert!(history.iter().any(|point| {
            point.fact_extraction_attempt_total > 0
                && point.correction_action_obsolete_total > 0
                && point.tombstone_total > 0
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_fact_descriptors_compatible_matches_best_multi_fact_pair() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            PromptMatchingCorrectionLLM {
                responses: vec![
                    (
                        "I now live in Beijing and my email is dylan@example.com".into(),
                        r#"{"facts":[{"subject":"user","attribute":"residence","value":"Beijing","change_type":"update","confidence":0.93},{"subject":"user","attribute":"contact","value":"dylan@example.com","change_type":"update","confidence":0.96}]}"#
                            .into(),
                    ),
                    (
                        "Favorite food is sushi and my email is old@example.com".into(),
                        r#"{"facts":[{"subject":"user","attribute":"preference","value":"sushi","change_type":"reaffirm","confidence":0.87},{"subject":"user","attribute":"contact","value":"old@example.com","change_type":"historical","confidence":0.92}]}"#
                            .into(),
                    ),
                ],
            },
        ));

        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing and my email is dylan@example.com".into(),
            None,
        );
        let target_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Favorite food is sushi and my email is old@example.com".into(),
            None,
        );

        let (source_fact, target_fact) = engine
            .resolve_fact_descriptors_compatible(&new_unit, &target_unit)
            .await
            .expect("expected a compatible fact pair");

        assert_eq!(source_fact.attribute, MemoryFactAttribute::Contact);
        assert_eq!(target_fact.attribute, MemoryFactAttribute::Contact);
        assert_eq!(source_fact.subject, MemoryFactSubject::User);
        assert_eq!(target_fact.subject, MemoryFactSubject::User);

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_memory_fact_descriptors_prefers_persisted_extracted_facts() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        engine.arbitrator =
            crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(PanicOnGenerateLLM));

        let mut unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Acme Corp previously worked with John Doe".into(),
            None,
        );
        unit.extracted_facts = vec![StoredMemoryFact {
            subject: "organization".into(),
            subject_ref: Some("organization:acme_corp".into()),
            subject_name: Some("Acme Corp".into()),
            attribute: "relationship".into(),
            value: "John Doe".into(),
            canonical_value: Some("john doe".into()),
            change_type: "reaffirm".into(),
            temporal_status: Some("current".into()),
            polarity: Some("positive".into()),
            evidence_span: Some("Acme Corp worked with John Doe".into()),
            confidence: 0.89,
        }];

        let descriptors = engine.resolve_memory_fact_descriptors(&unit).await;

        assert_eq!(descriptors.len(), 1);
        assert_eq!(descriptors[0].subject, MemoryFactSubject::Organization);
        assert_eq!(descriptors[0].subject_key, "organization:acme_corp");
        assert_eq!(descriptors[0].attribute, MemoryFactAttribute::Relationship);
        assert_eq!(
            descriptors[0].value_payload,
            MemoryFactValuePayload::PersonName {
                name: "john doe".into()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_memory_fact_descriptors_extracts_multiple_rule_facts() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing and my email is dylan@example.com".into(),
            None,
        );

        let descriptors = engine.resolve_memory_fact_descriptors(&unit).await;

        assert_eq!(descriptors.len(), 2);
        assert!(descriptors
            .iter()
            .any(|fact| fact.attribute == MemoryFactAttribute::Residence));
        assert!(descriptors
            .iter()
            .any(|fact| fact.attribute == MemoryFactAttribute::Contact));

        Ok(())
    }

    #[tokio::test]
    async fn test_hydrate_memory_unit_extracted_facts_populates_rule_facts() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing and my email is dylan@example.com".into(),
            None,
        );

        engine.hydrate_memory_unit_extracted_facts(&mut unit).await;

        assert!(unit.extracted_facts.len() >= 2);
        assert!(unit
            .extracted_facts
            .iter()
            .any(|fact| fact.attribute == "residence"));
        assert!(unit
            .extracted_facts
            .iter()
            .any(|fact| fact.attribute == "contact"));

        Ok(())
    }

    #[tokio::test]
    async fn test_reconcile_conflicting_memory_unit_uses_slot_aware_candidates() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Home base: Shanghai".into(),
            None,
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_id = old_unit.id;
        engine.store_memory_unit(old_unit).await?;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);
        engine.store_memory_unit(unrelated_unit).await?;

        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            MockCorrectionLLM {
                response: format!(
                    r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Residence updated","confidence":0.94}}]"#,
                    old_id
                ),
            },
        ));

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let affected = engine.reconcile_conflicting_memory_unit(&new_unit).await?;

        assert_eq!(affected, vec![old_id]);
        assert!(engine.is_memory_unit_forgotten(TEST_USER, old_id)?);

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_tombstones_target_and_links_relation(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;
        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Address updated".into(),
                    confidence: 0.95,
                }],
            )
            .await?;

        assert_eq!(affected, vec![old_id]);
        assert!(engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine.get_memory_unit(TEST_USER, old_id).await?.is_none());

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::EvolvedTo));

        Ok(())
    }

    #[tokio::test]
    async fn test_plan_memory_correction_actions_returns_validated_preview() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(1);
        let old_id = old_unit.id;

        engine.store_memory_unit(old_unit).await?;
        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            PromptMatchingCorrectionLLM {
                responses: vec![(
                    "memory correction engine".into(),
                    format!(
                        r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Residence updated","confidence":0.96}}]"#,
                        old_id
                    ),
                )],
            },
        ));

        let mut preview_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        preview_unit.transaction_time = Utc::now();

        let actions = engine
            .plan_memory_correction_actions(&preview_unit, 8)
            .await?;

        let action = actions
            .into_iter()
            .find(|action| action.target_id == old_id)
            .expect("expected planned correction action");
        assert_eq!(action.kind, MemoryCorrectionKind::Obsolete);
        assert_eq!(action.effect, RacDecisionEffect::Tombstone);
        assert_eq!(action.relation, Some(RelationType::EvolvedTo));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_keeps_contradicting_target_visible() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I have never lived in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;
        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Contradicts,
                    reason: "Conflicting claim".into(),
                    confidence: 0.82,
                }],
            )
            .await?;

        assert_eq!(affected, vec![old_id]);
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine.get_memory_unit(TEST_USER, old_id).await?.is_some());

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::Contradicts));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_skips_obsolete_for_mismatched_slots() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;
        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Incorrect cross-slot overwrite".into(),
                    confidence: 0.9,
                }],
            )
            .await?;

        assert!(affected.is_empty());
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine.get_memory_unit(TEST_USER, old_id).await?.is_some());

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(!outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::EvolvedTo));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_skips_obsolete_for_different_external_subjects(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Alice lives in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Bob now lives in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;
        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Different person should not overwrite".into(),
                    confidence: 0.9,
                }],
            )
            .await?;

        assert!(affected.is_empty());
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine.get_memory_unit(TEST_USER, old_id).await?.is_some());

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(!outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::EvolvedTo));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_skips_low_confidence_obsolete() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(1);
        let old_id = old_unit.id;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        new_unit.transaction_time = Utc::now();
        let new_id = new_unit.id;

        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Low confidence replacement".into(),
                    confidence: 0.62,
                }],
            )
            .await?;

        assert!(affected.is_empty());
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(!outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::EvolvedTo));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_downgrades_medium_confidence_obsolete_to_relation_only(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(1);
        let old_id = old_unit.id;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        new_unit.transaction_time = Utc::now();
        let new_id = new_unit.id;

        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Needs review before tombstone".into(),
                    confidence: 0.78,
                }],
            )
            .await?;

        assert_eq!(affected, vec![old_id]);
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::EvolvedTo));

        let decisions = engine.list_recent_rac_decisions(8)?;
        let recent = decisions
            .into_iter()
            .find(|decision| {
                decision.source_unit_id == new_id
                    && decision.target_unit_id == Some(old_id)
                    && decision.action == "obsolete"
            })
            .expect("expected rac decision record");
        assert_eq!(recent.effect, RacDecisionEffect::RelationOnly);
        assert_eq!(
            recent.guard_reason.as_deref(),
            Some("obsolete_relation_only_due_to_confidence")
        );

        let reviews =
            engine.list_rac_reviews(Some(RacReviewStatus::Pending), Some(TEST_USER), None, 8)?;
        let review = reviews
            .into_iter()
            .find(|review| {
                review.source_unit_id == new_id
                    && review.target_unit_id == old_id
                    && review.action == "obsolete"
            })
            .expect("expected pending rac review record");
        assert_eq!(review.stage, "post_store");
        assert_eq!(review.status, RacReviewStatus::Pending);
        assert_eq!(
            review.guard_reason.as_deref(),
            Some("obsolete_relation_only_due_to_confidence")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_rac_review_approval_tombstones_target() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(1);
        let old_id = old_unit.id;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        new_unit.transaction_time = Utc::now();
        let new_id = new_unit.id;

        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Needs review before tombstone".into(),
                    confidence: 0.78,
                }],
            )
            .await?;

        let review = engine
            .list_rac_reviews(Some(RacReviewStatus::Pending), Some(TEST_USER), None, 8)?
            .into_iter()
            .find(|review| review.source_unit_id == new_id && review.target_unit_id == old_id)
            .expect("expected pending review");

        let resolved = engine
            .resolve_rac_review(
                &review.review_id,
                true,
                Some("qa-reviewer".into()),
                Some("approved after inspection".into()),
            )
            .await?
            .expect("review should resolve");

        assert_eq!(resolved.status, RacReviewStatus::Approved);
        assert_eq!(resolved.reviewer.as_deref(), Some("qa-reviewer"));
        assert_eq!(
            resolved.reviewer_note.as_deref(),
            Some("approved after inspection")
        );
        assert!(engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine
            .list_rac_reviews(Some(RacReviewStatus::Pending), Some(TEST_USER), None, 8)?
            .is_empty());

        let decisions = engine.list_recent_rac_decisions(16)?;
        let approval_decision = decisions
            .into_iter()
            .find(|decision| {
                decision.stage == "review_approve"
                    && decision.source_unit_id == new_id
                    && decision.target_unit_id == Some(old_id)
                    && decision.action == "obsolete"
            })
            .expect("expected review approval decision");
        assert_eq!(approval_decision.effect, RacDecisionEffect::Tombstone);

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_rac_review_rejection_keeps_target_visible() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(1);
        let old_id = old_unit.id;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        new_unit.transaction_time = Utc::now();
        let new_id = new_unit.id;

        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Needs review before tombstone".into(),
                    confidence: 0.78,
                }],
            )
            .await?;

        let review = engine
            .list_rac_reviews(Some(RacReviewStatus::Pending), Some(TEST_USER), None, 8)?
            .into_iter()
            .find(|review| review.source_unit_id == new_id && review.target_unit_id == old_id)
            .expect("expected pending review");

        let resolved = engine
            .resolve_rac_review(
                &review.review_id,
                false,
                Some("qa-reviewer".into()),
                Some("rejected".into()),
            )
            .await?
            .expect("review should resolve");

        assert_eq!(resolved.status, RacReviewStatus::Rejected);
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine
            .list_rac_reviews(Some(RacReviewStatus::Pending), Some(TEST_USER), None, 8)?
            .is_empty());
        assert!(engine
            .list_rac_reviews(Some(RacReviewStatus::Rejected), Some(TEST_USER), None, 8)?
            .into_iter()
            .any(|review| review.review_id == resolved.review_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_manual_memory_correction_supports_manual_contradicts() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I have never lived in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;

        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_manual_memory_correction(
                TEST_USER,
                new_id,
                old_id,
                MemoryCorrectionKind::Contradicts,
                "manual contradiction".into(),
                0.86,
                "manual_api",
            )
            .await?;

        assert_eq!(affected, vec![old_id]);
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::Contradicts));

        let decisions = engine.list_recent_rac_decisions(8)?;
        let recent = decisions
            .into_iter()
            .find(|decision| {
                decision.stage == "manual_api"
                    && decision.source_unit_id == new_id
                    && decision.target_unit_id == Some(old_id)
                    && decision.action == "contradicts"
            })
            .expect("expected manual correction decision");
        assert_eq!(recent.effect, RacDecisionEffect::RelationOnly);

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_skips_obsolete_when_target_is_newer() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        old_unit.transaction_time = Utc::now() + chrono::Duration::minutes(5);
        let old_id = old_unit.id;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        new_unit.transaction_time = Utc::now();
        let new_id = new_unit.id;

        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Should not obsolete newer memory".into(),
                    confidence: 0.97,
                }],
            )
            .await?;

        assert!(affected.is_empty());
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(!outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::EvolvedTo));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_uses_llm_fact_fallback_for_obsolete() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Current city: Beijing".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;
        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;
        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            MockCorrectionLLM {
                response: r#"{"subject":"user","attribute":"residence","value":"Beijing","change_type":"update","confidence":0.92}"#
                    .into(),
            },
        ));

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Residence updated".into(),
                    confidence: 0.95,
                }],
            )
            .await?;

        assert_eq!(affected, vec![old_id]);
        assert!(engine.is_memory_unit_forgotten(TEST_USER, old_id)?);

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::EvolvedTo));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_ignores_reaffirm_action() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I still live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;
        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Reaffirm,
                    reason: "Same fact".into(),
                    confidence: 0.7,
                }],
            )
            .await?;

        assert!(affected.is_empty());
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine.get_memory_unit(TEST_USER, old_id).await?.is_some());

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(!outgoing.iter().any(|edge| {
            edge.target_id == old_id
                && matches!(
                    edge.relation,
                    RelationType::EvolvedTo | RelationType::Contradicts
                )
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_ignores_ignore_action() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;
        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Ignore,
                    reason: "Unrelated candidate".into(),
                    confidence: 0.4,
                }],
            )
            .await?;

        assert!(affected.is_empty());
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine.get_memory_unit(TEST_USER, old_id).await?.is_some());

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(!outgoing.iter().any(|edge| {
            edge.target_id == old_id
                && matches!(
                    edge.relation,
                    RelationType::EvolvedTo | RelationType::Contradicts
                )
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrency_progress_update() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        // 1. Create parent L2
        let mut parent = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Parent Task".into(),
            None,
        );
        parent.level = 2;
        parent.task_metadata = Some(memorose_common::TaskMetadata {
            status: memorose_common::TaskStatus::InProgress,
            progress: 0.0,
        });
        let parent_id = parent.id;
        engine.store_memory_unit(parent).await?;

        // 2. Create 10 children L1s and link them
        for i in 0..10 {
            let mut child = MemoryUnit::new(
                None,
                TEST_USER.into(),
                None,
                stream_id,
                memorose_common::MemoryType::Factual,
                format!("Child {}", i),
                None,
            );
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
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        // Store memory for user A
        let unit_a = MemoryUnit::new(
            None,
            "user_a".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Secret of user A".into(),
            None,
        );
        engine.store_memory_unit(unit_a.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        // Store memory for user B
        let unit_b = MemoryUnit::new(
            None,
            "user_b".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Secret of user B".into(),
            None,
        );
        engine.store_memory_unit(unit_b.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        // User A should only see their own data
        let results_a = engine
            .search_text("user_a", "Secret", 10, false, None)
            .await?;
        assert_eq!(results_a.len(), 1);
        assert_eq!(results_a[0].user_id, "user_a");

        // User B should only see their own data
        let results_b = engine
            .search_text("user_b", "Secret", 10, false, None)
            .await?;
        assert_eq!(results_b.len(), 1);
        assert_eq!(results_b[0].user_id, "user_b");

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_event_failed_clears_retry_state() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("retry me".into()),
        );
        let event_id = event.id.to_string();
        engine.ingest_event_directly(event).await?;

        assert_eq!(
            engine.increment_retry_count_if_pending(&event_id).await?,
            Some(1)
        );
        assert_eq!(engine.get_retry_count(&event_id).await?, 1);

        engine
            .mark_event_failed(&event_id, "simulated failure")
            .await?;

        assert_eq!(engine.get_retry_count(&event_id).await?, 0);
        assert_eq!(
            engine.increment_retry_count_if_pending(&event_id).await?,
            None
        );
        assert!(engine.fetch_pending_events().await?.is_empty());
        let failed_key = format!("failed:{}", event_id);
        assert!(engine.system_kv().get(failed_key.as_bytes())?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_pending_events_sorts_by_transaction_time() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut later = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("later".into()),
        );
        later.transaction_time = Utc::now() + chrono::Duration::seconds(30);

        let mut earlier = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("earlier".into()),
        );
        earlier.transaction_time = Utc::now() - chrono::Duration::seconds(30);

        engine.ingest_event_directly(later.clone()).await?;
        engine.ingest_event_directly(earlier.clone()).await?;

        let pending = engine.fetch_pending_events_limited(10).await?;
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].id, earlier.id);
        assert_eq!(pending[1].id, later.id);

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_pending_events_marks_orphaned_entries_failed() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let orphan_id = Uuid::new_v4().to_string();
        let pending_key = format!("pending:{}", orphan_id);
        let pending_val = serde_json::to_vec(&serde_json::json!({
            "user_id": TEST_USER
        }))?;
        engine
            .system_kv()
            .put(pending_key.as_bytes(), &pending_val)?;

        let pending = engine.fetch_pending_events_limited(10).await?;
        assert!(pending.is_empty());
        let failed_key = format!("failed:{}", orphan_id);
        assert!(engine.system_kv().get(failed_key.as_bytes())?.is_some());
        assert!(engine.system_kv().get(pending_key.as_bytes())?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_pending_events_limit_zero_short_circuits() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("queued".into()),
        );
        engine.ingest_event_directly(event).await?;

        let pending = engine.fetch_pending_events_limited(0).await?;
        assert!(pending.is_empty());
        assert_eq!(engine.count_pending_events().await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_pending_events_limited_respects_limit() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut first = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("first".into()),
        );
        first.id = Uuid::from_u128(1);

        let mut second = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("second".into()),
        );
        second.id = Uuid::from_u128(2);

        engine.ingest_event_directly(first.clone()).await?;
        engine.ingest_event_directly(second.clone()).await?;

        let pending = engine.fetch_pending_events_limited(1).await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, first.id);
        assert_eq!(engine.count_pending_events().await?, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_pending_events_ignores_nonstandard_pending_keys() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        engine.system_kv().put(b"pending:bad:marker", b"{}")?;

        let pending = engine.fetch_pending_events_limited(10).await?;
        assert!(pending.is_empty());
        assert!(engine.system_kv().get(b"pending:bad:marker")?.is_some());
        assert!(engine.system_kv().get(b"failed:bad")?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_pending_events_marks_malformed_and_missing_user_metadata_failed(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let malformed_id = Uuid::new_v4().to_string();
        engine
            .system_kv()
            .put(format!("pending:{malformed_id}").as_bytes(), b"{bad-json")?;

        let missing_user_id = Uuid::new_v4().to_string();
        engine.system_kv().put(
            format!("pending:{missing_user_id}").as_bytes(),
            &serde_json::to_vec(&serde_json::json!({"other":"value"}))?,
        )?;

        let empty_metadata = Uuid::new_v4().to_string();
        engine
            .system_kv()
            .put(format!("pending:{empty_metadata}").as_bytes(), b"")?;

        let pending = engine.fetch_pending_events_limited(10).await?;
        assert!(pending.is_empty());

        for event_id in [&malformed_id, &missing_user_id, &empty_metadata] {
            let failed_key = format!("failed:{event_id}");
            let failed = engine
                .system_kv()
                .get(failed_key.as_bytes())?
                .expect("failed marker should exist");
            let failed_json: serde_json::Value = serde_json::from_slice(&failed)?;
            let error = failed_json["error"].as_str().unwrap_or("");
            assert!(
                error.contains("Pending metadata") || error.contains("Malformed pending metadata")
            );
            assert!(engine
                .system_kv()
                .get(format!("pending:{event_id}").as_bytes())?
                .is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_event_clears_pending_retry_failed_and_forget_markers() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("to delete".into()),
        );
        let event_id = event.id.to_string();
        engine.ingest_event_directly(event.clone()).await?;
        assert_eq!(engine.count_pending_events().await?, 1);
        assert_eq!(
            engine.increment_retry_count_if_pending(&event_id).await?,
            Some(1)
        );

        engine.system_kv().put(
            format!("failed:{event_id}").as_bytes(),
            &serde_json::to_vec(&serde_json::json!({"error":"boom"}))?,
        )?;
        engine.mark_event_forgotten(
            TEST_USER,
            &event_id,
            &ForgettingTombstone {
                user_id: TEST_USER.into(),
                org_id: None,
                target_kind: ForgetTargetKind::Event,
                target_id: event_id.clone(),
                reason_query: "cleanup".into(),
                created_at: Utc::now(),
                preview_id: None,
                mode: ForgetMode::Logical,
            },
        )?;

        assert!(engine.is_event_forgotten(TEST_USER, &event_id)?);
        engine.delete_event(TEST_USER, &event_id).await?;

        assert_eq!(engine.count_pending_events().await?, 0);
        assert_eq!(engine.get_retry_count(&event_id).await?, 0);
        assert!(engine.get_event(TEST_USER, &event_id).await?.is_none());
        assert!(!engine.is_event_forgotten(TEST_USER, &event_id)?);
        assert!(engine
            .system_kv()
            .get(format!("failed:{event_id}").as_bytes())?
            .is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_retry_count_invalid_payload_defaults_to_zero() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        engine.system_kv().put(b"retry_count:broken", &[1, 2])?;
        assert_eq!(engine.get_retry_count("broken").await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_event_hides_and_restores_forgotten_event() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("forgettable".into()),
        );
        let event_id = event.id.to_string();
        engine.ingest_event_directly(event.clone()).await?;

        engine.mark_event_forgotten(
            TEST_USER,
            &event_id,
            &ForgettingTombstone {
                user_id: TEST_USER.into(),
                org_id: None,
                target_kind: ForgetTargetKind::Event,
                target_id: event_id.clone(),
                reason_query: "hide".into(),
                created_at: Utc::now(),
                preview_id: None,
                mode: ForgetMode::Logical,
            },
        )?;

        assert!(engine.get_event(TEST_USER, &event_id).await?.is_none());
        engine.clear_event_forgotten(TEST_USER, &event_id)?;
        assert!(engine.get_event(TEST_USER, &event_id).await?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_org_policy_and_backfill_status_default_invalid_and_valid_payloads() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let org_id = "org-alpha";

        assert_eq!(
            engine.get_org_share_policy(TEST_USER, org_id)?,
            SharePolicy::default()
        );

        let policy_key = MemoroseEngine::org_share_policy_key(TEST_USER, org_id);
        engine.system_kv().put(policy_key.as_bytes(), b"not-json")?;
        assert_eq!(
            engine.get_org_share_policy(TEST_USER, org_id)?,
            SharePolicy::default()
        );

        assert_eq!(engine.get_org_backfill_status(TEST_USER, org_id)?, None);

        let status_key =
            MemoroseEngine::backfill_status_key(&MemoryDomain::Organization, TEST_USER, org_id);
        engine.system_kv().put(status_key.as_bytes(), b"not-json")?;
        assert_eq!(engine.get_org_backfill_status(TEST_USER, org_id)?, None);

        let status = serde_json::json!({"state":"completed","processed":3});
        engine
            .system_kv()
            .put(status_key.as_bytes(), &serde_json::to_vec(&status)?)?;
        assert_eq!(
            engine.get_org_backfill_status(TEST_USER, org_id)?,
            Some(status)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_reflect_on_session_creates_l2_topics_and_graph_edges() -> Result<()> {
        let temp_dir = tempdir()?;
        let stream_id = Uuid::new_v4();
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(Arc::new(
                MockCorrectionLLM {
                    response: format!(
                        r#"[{{"summary":"Residence topic","source_ids":["{}","{}"]}}]"#,
                        Uuid::from_u128(11),
                        Uuid::from_u128(12)
                    ),
                },
            )));

        let mut first = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Beijing".into(),
            None,
        );
        first.id = Uuid::from_u128(11);
        let mut second = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "My office is in Chaoyang".into(),
            None,
        );
        second.id = Uuid::from_u128(12);

        engine
            .store_memory_units(vec![first.clone(), second.clone()])
            .await?;
        engine.reflect_on_session(TEST_USER, stream_id).await?;

        let prefix = format!("u:{}:unit:", TEST_USER);
        let kv = engine._kv.clone();
        let prefix_bytes = prefix.into_bytes();
        let all_units: Vec<(Vec<u8>, Vec<u8>)> =
            tokio::task::spawn_blocking(move || kv.scan(&prefix_bytes)).await??;

        let l2s: Vec<MemoryUnit> = all_units
            .into_iter()
            .filter_map(|(_, v)| serde_json::from_slice::<MemoryUnit>(&v).ok())
            .filter(|unit| unit.level == 2)
            .collect();
        assert_eq!(l2s.len(), 1);
        assert_eq!(l2s[0].content, "Residence topic");
        assert_eq!(l2s[0].embedding.as_deref(), Some(&[0.0, 0.0, 0.0][..]));

        let outgoing = engine
            .graph()
            .get_outgoing_edges(TEST_USER, l2s[0].id)
            .await?;
        assert_eq!(outgoing.len(), 2);
        assert!(outgoing.iter().any(|edge| edge.target_id == first.id));
        assert!(outgoing.iter().any(|edge| edge.target_id == second.id));

        Ok(())
    }

    #[tokio::test]
    async fn test_reflect_on_user_window_batches_across_streams() -> Result<()> {
        let temp_dir = tempdir()?;
        let older_stream_id = Uuid::new_v4();
        let newer_stream_id = Uuid::new_v4();
        let now = Utc::now();
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(Arc::new(
                MockCorrectionLLM {
                    response: format!(
                        r#"[{{"summary":"Cross-stream topic","source_ids":["{}","{}"]}}]"#,
                        Uuid::from_u128(21),
                        Uuid::from_u128(22)
                    ),
                },
            )));

        let mut older = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            older_stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Beijing".into(),
            None,
        );
        older.id = Uuid::from_u128(21);
        older.transaction_time = now - chrono::Duration::seconds(10);

        let mut newer = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            newer_stream_id,
            memorose_common::MemoryType::Factual,
            "My office is in Chaoyang".into(),
            None,
        );
        newer.id = Uuid::from_u128(22);
        newer.transaction_time = now;

        engine
            .store_memory_units(vec![older.clone(), newer.clone()])
            .await?;
        let created_topics = engine.reflect_on_user_window(TEST_USER, 10).await?;
        assert_eq!(created_topics, 1);

        let prefix = format!("u:{}:unit:", TEST_USER);
        let kv = engine._kv.clone();
        let prefix_bytes = prefix.into_bytes();
        let all_units: Vec<(Vec<u8>, Vec<u8>)> =
            tokio::task::spawn_blocking(move || kv.scan(&prefix_bytes)).await??;

        let l2s: Vec<MemoryUnit> = all_units
            .into_iter()
            .filter_map(|(_, v)| serde_json::from_slice::<MemoryUnit>(&v).ok())
            .filter(|unit| unit.level == 2)
            .collect();
        assert_eq!(l2s.len(), 1);
        assert_eq!(l2s[0].content, "Cross-stream topic");
        assert_eq!(l2s[0].stream_id, newer_stream_id);
        assert_eq!(l2s[0].embedding.as_deref(), Some(&[0.0, 0.0, 0.0][..]));

        let outgoing = engine
            .graph()
            .get_outgoing_edges(TEST_USER, l2s[0].id)
            .await?;
        assert_eq!(outgoing.len(), 2);
        assert!(outgoing.iter().any(|edge| edge.target_id == older.id));
        assert!(outgoing.iter().any(|edge| edge.target_id == newer.id));

        Ok(())
    }

    #[test]
    fn test_reflection_and_community_markers_roundtrip() -> Result<()> {
        let temp_dir = tempdir()?;
        let rt = tokio::runtime::Runtime::new()?;
        let engine = rt.block_on(MemoroseEngine::new_with_default_threshold(
            temp_dir.path(),
            1000,
            true,
            true,
        ))?;

        engine.set_needs_reflect("alice")?;
        engine.set_needs_reflect("bob")?;
        engine.set_needs_community("alice")?;
        engine.set_needs_community("carol")?;

        let mut reflections = engine.get_pending_reflections()?;
        reflections.sort();
        assert_eq!(reflections, vec!["alice".to_string(), "bob".to_string()]);

        let mut communities = engine.get_pending_communities()?;
        communities.sort();
        assert_eq!(communities, vec!["alice".to_string(), "carol".to_string()]);

        engine.clear_reflection_marker("alice")?;
        engine.clear_community_marker("carol")?;

        assert_eq!(engine.get_pending_reflections()?, vec!["bob".to_string()]);
        assert_eq!(engine.get_pending_communities()?, vec!["alice".to_string()]);
        Ok(())
    }

    #[test]
    fn test_reflection_marker_accumulates_pending_units_and_tokens() -> Result<()> {
        let temp_dir = tempdir()?;
        let rt = tokio::runtime::Runtime::new()?;
        let engine = rt.block_on(MemoroseEngine::new_with_default_threshold(
            temp_dir.path(),
            1000,
            true,
            true,
        ))?;

        engine.bump_reflection_marker("alice", 2, 120)?;
        engine.bump_reflection_marker("alice", 3, 80)?;

        let markers = engine.get_pending_reflection_markers()?;
        let alice = markers
            .into_iter()
            .find(|(user_id, _)| user_id == "alice")
            .expect("alice marker should exist");
        assert_eq!(alice.1.pending_units, 5);
        assert_eq!(alice.1.pending_tokens, 200);
        assert_eq!(alice.1.first_event_tx_micros, 0);
        assert_eq!(alice.1.last_event_tx_micros, 0);
        assert!(alice.1.last_event_at_ts >= alice.1.first_event_at_ts);
        Ok(())
    }

    #[tokio::test]
    async fn test_store_l2_units_does_not_schedule_reflection() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Derived topic".into(),
            None,
        );
        unit.level = 2;

        engine.store_memory_units(vec![unit]).await?;

        assert!(engine.get_pending_reflections()?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_event_directly_rejects_empty_variants() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let variants = vec![
            EventContent::Text("   ".into()),
            EventContent::Image(" ".into()),
            EventContent::Audio(" ".into()),
            EventContent::Video(" ".into()),
            EventContent::Json(serde_json::Value::Null),
            EventContent::Json(serde_json::Value::String(" ".into())),
        ];

        for content in variants {
            let err = engine
                .ingest_event_directly(Event::new(None, TEST_USER.into(), None, stream_id, content))
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("Rejected empty event"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_events_directly_batches_pending_writes() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();
        let event_a = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Text("alpha".into()),
        );
        let event_b = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Text("beta".into()),
        );

        engine
            .ingest_events_directly(vec![event_a.clone(), event_b.clone()])
            .await?;

        assert_eq!(engine.count_pending_events().await?, 2);
        let pending = engine.fetch_pending_events_limited(10).await?;
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().any(|event| event.id == event_a.id));
        assert!(pending.iter().any(|event| event.id == event_b.id));
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_recent_l1_units_returns_top_k_without_full_scan_order_loss() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut older = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "older".into(),
            None,
        );
        older.transaction_time = Utc::now() - chrono::Duration::minutes(10);

        let mut newest = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "newest".into(),
            None,
        );
        newest.transaction_time = Utc::now();

        let mut middle = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "middle".into(),
            None,
        );
        middle.transaction_time = Utc::now() - chrono::Duration::minutes(3);

        let newest_id = newest.id;
        let middle_id = middle.id;

        engine
            .store_memory_units(vec![older, newest, middle])
            .await?;

        let recent = engine.fetch_recent_l1_units(TEST_USER, 2).await?;
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].id, newest_id);
        assert_eq!(recent[1].id, middle_id);

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_recent_l1_units_since_filters_to_incremental_window() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut older = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "older".into(),
            None,
        );
        older.transaction_time = Utc::now() - chrono::Duration::minutes(10);

        let mut newer = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "newer".into(),
            None,
        );
        newer.transaction_time = Utc::now();

        let newer_id = newer.id;
        let min_tx_micros = newer.transaction_time.timestamp_micros();

        engine.store_memory_units(vec![older, newer]).await?;

        let recent = engine
            .fetch_recent_l1_units_since(TEST_USER, min_tx_micros, 10)
            .await?;
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].id, newer_id);

        Ok(())
    }

    #[tokio::test]
    async fn test_export_snapshot_writes_archive() -> Result<()> {
        let temp_dir = tempdir()?;
        let output_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("snapshot me".into()),
        );
        engine.ingest_event_directly(event).await?;

        let output_path = output_dir.path().join("snapshot.tar.gz");
        engine.export_snapshot(output_path.clone()).await?;

        assert!(output_path.exists());
        assert!(std::fs::metadata(&output_path)?.len() > 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_restore_from_snapshot_replaces_existing_target_dir() -> Result<()> {
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let archive_dir = tempdir()?;
        let target_root = tempdir()?;
        let snapshot_path = archive_dir.path().join("snapshot.tar.gz");
        let target_dir = target_root.path().join("restore-target");

        std::fs::create_dir_all(target_dir.join("stale"))?;
        std::fs::write(target_dir.join("stale/old.txt"), b"old")?;

        let file = std::fs::File::create(&snapshot_path)?;
        let enc = GzEncoder::new(file, Compression::default());
        let mut tar = tar::Builder::new(enc);
        let mut header = tar::Header::new_gnu();
        let payload = b"fresh";
        header.set_size(payload.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append_data(&mut header, "rocksdb/new.txt", &payload[..])?;
        let enc = tar.into_inner()?;
        enc.finish()?;

        MemoroseEngine::restore_from_snapshot(snapshot_path, target_dir.clone()).await?;

        assert!(target_dir.join("rocksdb/new.txt").exists());
        assert!(!target_dir.join("stale/old.txt").exists());
        assert_eq!(std::fs::read(target_dir.join("rocksdb/new.txt"))?, b"fresh");
        Ok(())
    }

    #[tokio::test]
    async fn test_bump_l1_count_tracks_threshold_crossing() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        for i in 0..4 {
            let unit = MemoryUnit::new(
                None,
                TEST_USER.into(),
                None,
                stream_id,
                memorose_common::MemoryType::Factual,
                format!("base {}", i),
                None,
            );
            engine.store_memory_unit(unit).await?;
        }

        for i in 0..2 {
            let unit = MemoryUnit::new(
                None,
                TEST_USER.into(),
                None,
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
            None,
            TEST_USER.into(),
            None,
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

    #[tokio::test]
    async fn test_text_search_returns_local_memories() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let primary = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Cross stream retrieval phrase".into(),
            None,
        );
        let secondary = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Cross stream retrieval phrase".into(),
            None,
        );

        engine
            .store_memory_units(vec![primary.clone(), secondary.clone()])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let results = engine
            .search_text(TEST_USER, "cross stream retrieval", 10, false, None)
            .await?;

        assert!(!results.is_empty());
        assert_eq!(results.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_org_shared_memory_is_visible_across_consumers() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Organization onboarding standard".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        engine.store_memory_unit(source.clone()).await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "onboarding standard",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        assert!(!shared.is_empty());
        assert!(shared
            .iter()
            .any(|(unit, _)| unit.domain == MemoryDomain::Organization
                && unit.user_id == MemoroseEngine::organization_read_view_owner("org_alpha")
                && unit.agent_id.is_none()
                && unit.stream_id.is_nil()
                && unit.references.is_empty()
                && unit.assets.is_empty()));

        let read_view = shared
            .iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit.clone())
            .expect("expected organization read view");
        let record = engine
            .load_organization_knowledge(read_view.id)?
            .expect("expected canonical organization knowledge");
        assert_eq!(
            engine
                .resolve_organization_record_source_ids(&record)
                .await?,
            vec![source.id]
        );
        assert_eq!(record.org_id, "org_alpha");
        assert_eq!(record.content, read_view.content);
        assert_eq!(record.keywords, read_view.keywords);

        Ok(())
    }

    #[tokio::test]
    async fn test_org_read_view_does_not_persist_view_unit() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Organization onboarding standard".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        engine.store_memory_unit(source).await?;

        let read_view = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "onboarding standard",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        let unit_key = format!(
            "u:{}:unit:{}",
            MemoroseEngine::organization_read_view_owner("org_alpha"),
            read_view.id
        );
        assert!(engine.kv().get(unit_key.as_bytes())?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_disabling_org_contribution_removes_org_read_view() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_cleanup",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_cleanup",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_cleanup".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Org cleanup knowledge".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        engine.store_memory_unit(source.clone()).await?;

        let before = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_cleanup"),
                None,
                "cleanup knowledge",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;
        assert!(!before.is_empty());

        let removed = engine
            .disable_org_contribution("author", "org_cleanup")
            .await?;
        assert_eq!(removed, 1);

        let after = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_cleanup"),
                None,
                "cleanup knowledge",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;
        assert!(after.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_disabling_org_contribution_marks_contribution_revoked() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        for user_id in ["author_a", "author_b"] {
            engine.set_org_share_policy(
                user_id,
                "org_cleanup",
                &memorose_common::SharePolicy {
                    contribute: true,
                    consume: false,
                    include_history: false,
                    targets: vec![],
                },
            )?;
        }
        engine.set_org_share_policy(
            "consumer",
            "org_cleanup",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source_a = MemoryUnit::new(
            Some("org_cleanup".into()),
            "author_a".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Cleanup Playbook: restart the cleanup worker.".into(),
            Some(vec![1.0; 768]),
        );
        source_a.level = 2;
        source_a.keywords = vec!["Cleanup Playbook".into(), "Restart".into()];

        let mut source_b = MemoryUnit::new(
            Some("org_cleanup".into()),
            "author_b".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Cleanup Playbook: retry failed cleanup jobs.".into(),
            Some(vec![1.0; 768]),
        );
        source_b.level = 2;
        source_b.keywords = vec!["Cleanup Playbook".into(), "Retry".into()];

        engine
            .store_memory_units(vec![source_a.clone(), source_b.clone()])
            .await?;

        let read_view = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_cleanup"),
                None,
                "cleanup playbook",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        engine
            .disable_org_contribution("author_a", "org_cleanup")
            .await?;

        let contribution = engine
            .load_organization_contribution(read_view.id, source_a.id)?
            .expect("expected contribution record");
        assert!(matches!(
            contribution.status,
            OrganizationKnowledgeContributionStatus::Revoked
        ));
        assert!(contribution.revoked_at.is_some());

        let hydrated = engine
            .get_shared_search_hit_by_index(read_view.id)
            .await?
            .expect("expected rebuilt organization read view")
            .into_memory_unit();
        let hydrated_record = engine
            .load_organization_knowledge(hydrated.id)?
            .expect("expected rebuilt organization knowledge record");
        assert_eq!(
            engine
                .resolve_organization_record_source_ids(&hydrated_record)
                .await?,
            vec![source_b.id]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_contribution_is_activated_from_candidate() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Escalation Playbook: page the incident commander.".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Escalation Playbook".into()];
        engine.store_memory_unit(source.clone()).await?;

        let read_view = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "escalation playbook",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        let contribution = engine
            .load_organization_contribution(read_view.id, source.id)?
            .expect("expected contribution record");
        assert!(matches!(
            contribution.status,
            OrganizationKnowledgeContributionStatus::Active
        ));
        assert_eq!(contribution.contributor_user_id, "author");
        assert!(contribution.candidate_at.is_some());
        assert!(contribution.activated_at.is_some());
        assert!(matches!(
            contribution.approval_mode,
            Some(OrganizationKnowledgeApprovalMode::Auto)
        ));
        assert_eq!(
            contribution.approved_by.as_deref(),
            Some("system:auto_publish")
        );
        assert!(contribution.revoked_at.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_detail_record_exposes_membership_and_history() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Runbook: rotate credentials after incident closure.".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Credential Rotation".into()];
        engine.store_memory_unit(source.clone()).await?;

        let read_view = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "credential rotation",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        let detail = engine
            .get_organization_knowledge_detail_record(read_view.id)
            .await?
            .expect("expected organization knowledge detail");

        assert_eq!(detail.record.id, read_view.id);
        assert_eq!(detail.read_view.id, read_view.id);
        assert_eq!(detail.memberships.len(), 1);
        assert_eq!(detail.contributions.len(), 1);

        let membership = &detail.memberships[0];
        assert_eq!(membership.membership.source_id, source.id);
        assert_eq!(membership.membership.contributor_user_id, "author");
        assert_eq!(membership.source_unit.memory_type, MemoryType::Factual);
        assert_eq!(membership.source_unit.level, 2);
        assert_eq!(membership.source_unit.keywords, vec!["Credential Rotation"]);
        assert!(membership
            .source_unit
            .content
            .contains("rotate credentials"));
        assert!(membership.contribution.is_some());
        assert!(matches!(
            membership
                .contribution
                .as_ref()
                .and_then(|record| record.approval_mode.as_ref()),
            Some(OrganizationKnowledgeApprovalMode::Auto)
        ));
        assert_eq!(
            membership
                .contribution
                .as_ref()
                .and_then(|record| record.approved_by.as_deref()),
            Some("system:auto_publish")
        );

        let contribution = &detail.contributions[0];
        assert_eq!(contribution.contribution.source_id, source.id);
        assert_eq!(contribution.contribution.contributor_user_id, "author");
        assert!(matches!(
            contribution.contribution.status,
            OrganizationKnowledgeContributionStatus::Active
        ));
        assert!(matches!(
            contribution.contribution.approval_mode.as_ref(),
            Some(OrganizationKnowledgeApprovalMode::Auto)
        ));
        assert_eq!(
            contribution.contribution.approved_by.as_deref(),
            Some("system:auto_publish")
        );
        let contribution_source = contribution
            .source_unit
            .as_ref()
            .expect("expected contribution source unit");
        assert_eq!(contribution_source.memory_type, MemoryType::Factual);
        assert_eq!(contribution_source.level, 2);
        assert_eq!(contribution_source.keywords, vec!["Credential Rotation"]);
        assert!(contribution_source.content.contains("rotate credentials"));

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_requires_l2_user_memory() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let l1_source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Raw user note that should stay local".into(),
            Some(vec![1.0; 768]),
        );
        engine.store_memory_unit(l1_source).await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "raw user note",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        assert!(shared.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_excludes_agent_memory() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut procedural = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            Some("agent_writer".into()),
            stream_id,
            memorose_common::MemoryType::Procedural,
            "Agent-specific recovery pattern".into(),
            Some(vec![1.0; 768]),
        );
        procedural.level = 2;
        engine.store_memory_unit(procedural).await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "recovery pattern",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        assert!(shared.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_org_shared_memory_ignores_agent_filter() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Shared organization troubleshooting playbook".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        engine.store_memory_unit(source).await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                Some("consumer_agent"),
                "troubleshooting playbook",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        assert!(shared
            .iter()
            .any(|(unit, _)| unit.domain == MemoryDomain::Organization));

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_canonicalizes_content() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I restart our payment service when my alert fires.".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Incident Recovery".into(), "Incident Recovery".into()];
        engine.store_memory_unit(source).await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "incident recovery payment service",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        let read_view = shared
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        assert_eq!(read_view.keywords, vec!["Incident Recovery".to_string()]);
        assert!(read_view.content.starts_with("Incident Recovery:"));
        assert!(read_view.content.contains("the contributor restart"));
        assert!(read_view
            .content
            .contains("the organization's payment service"));
        assert!(read_view.content.contains("the contributor's alert"));

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_skips_placeholder_l2_content() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "LLM not available".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        engine.store_memory_unit(source).await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "llm not available",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        assert!(shared.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_merges_same_topic_sources() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        for user_id in ["author_a", "author_b"] {
            engine.set_org_share_policy(
                user_id,
                "org_alpha",
                &memorose_common::SharePolicy {
                    contribute: true,
                    consume: false,
                    include_history: false,
                    targets: vec![],
                },
            )?;
        }
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source_a = MemoryUnit::new(
            Some("org_alpha".into()),
            "author_a".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I restart our payment service after alert storms.".into(),
            Some(vec![1.0; 768]),
        );
        source_a.level = 2;
        source_a.keywords = vec!["Incident Recovery".into(), "Restart".into()];

        let mut source_b = MemoryUnit::new(
            Some("org_alpha".into()),
            "author_b".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "We roll back the payment service after failed deploys.".into(),
            Some(vec![0.5; 768]),
        );
        source_b.level = 2;
        source_b.keywords = vec!["Incident Recovery".into(), "Rollback".into()];

        engine
            .store_memory_units(vec![source_a.clone(), source_b.clone()])
            .await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "incident recovery payment service",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        let org_units: Vec<MemoryUnit> = shared
            .into_iter()
            .filter(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit.into_memory_unit())
            .collect();

        assert_eq!(org_units.len(), 1);
        let read_view = &org_units[0];
        assert_eq!(
            read_view.user_id,
            MemoroseEngine::organization_read_view_owner("org_alpha")
        );
        assert_eq!(read_view.keywords.len(), 3);
        assert_eq!(read_view.keywords[0], "Incident Recovery");
        assert!(read_view.keywords.contains(&"Restart".to_string()));
        assert!(read_view.keywords.contains(&"Rollback".to_string()));
        assert!(read_view.agent_id.is_none());
        assert!(read_view.stream_id.is_nil());
        let record = engine
            .load_organization_knowledge(read_view.id)?
            .expect("expected organization knowledge record");
        let source_ids = engine
            .resolve_organization_record_source_ids(&record)
            .await?;
        assert_eq!(source_ids.len(), 2);
        assert!(source_ids.contains(&source_a.id));
        assert!(source_ids.contains(&source_b.id));

        Ok(())
    }

    #[tokio::test]
    async fn test_org_global_read_prefers_canonical_record_over_stored_view() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Organization incident playbook".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Incident Playbook".into()];
        engine.store_memory_unit(source).await?;

        let read_view = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "incident playbook",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        let record = engine
            .load_organization_knowledge(read_view.id)?
            .expect("expected canonical organization knowledge");
        let key = format!(
            "u:{}:unit:{}",
            MemoroseEngine::organization_read_view_owner("org_alpha"),
            read_view.id
        );
        let mut stale_view = read_view.clone();
        stale_view.content = "STALE VIEW".into();
        stale_view.keywords = vec!["STALE".into()];
        engine
            .kv()
            .put(key.as_bytes(), &serde_json::to_vec(&stale_view)?)?;

        let hydrated = engine
            .get_shared_search_hit_by_index(read_view.id)
            .await?
            .expect("expected organization knowledge hit by index")
            .into_memory_unit();

        assert_eq!(hydrated.content, record.content);
        assert_eq!(hydrated.keywords, record.keywords);
        assert_ne!(hydrated.content, "STALE VIEW");

        Ok(())
    }

    #[tokio::test]
    async fn test_org_global_list_prefers_canonical_record_over_stored_view() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Organization onboarding guide".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Onboarding Guide".into()];
        engine.store_memory_unit(source).await?;

        let read_view = engine
            .list_memory_units_global(None)
            .await?
            .into_iter()
            .find(|unit| unit.domain == MemoryDomain::Organization)
            .expect("expected organization knowledge read view in global list");

        let key = format!(
            "u:{}:unit:{}",
            MemoroseEngine::organization_read_view_owner("org_alpha"),
            read_view.id
        );
        let mut stale_view = read_view.clone();
        stale_view.content = "STALE LIST VIEW".into();
        engine
            .kv()
            .put(key.as_bytes(), &serde_json::to_vec(&stale_view)?)?;

        let listed = engine
            .list_memory_units_global(None)
            .await?
            .into_iter()
            .find(|unit| unit.id == read_view.id)
            .expect("expected organization knowledge read view in global list");

        assert_ne!(listed.content, "STALE LIST VIEW");
        assert_eq!(listed.content, read_view.content);

        Ok(())
    }

    #[tokio::test]
    async fn test_org_text_search_prefers_canonical_record_over_stored_view() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Organization troubleshooting playbook".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Troubleshooting Playbook".into()];
        engine.store_memory_unit(source).await?;

        let read_view = engine
            .search_text_with_shared(
                "consumer",
                Some("org_alpha"),
                "troubleshooting",
                5,
                false,
                None,
            )
            .await?
            .into_iter()
            .find(|unit| unit.domain == MemoryDomain::Organization)
            .expect("expected organization result");

        let key = format!(
            "u:{}:unit:{}",
            MemoroseEngine::organization_read_view_owner("org_alpha"),
            read_view.id
        );
        let mut stale_view = read_view.clone();
        stale_view.content = "Completely unrelated cached view".into();
        stale_view.keywords = vec!["Unrelated".into()];
        engine
            .kv()
            .put(key.as_bytes(), &serde_json::to_vec(&stale_view)?)?;

        let results = engine
            .search_text_with_shared(
                "consumer",
                Some("org_alpha"),
                "troubleshooting",
                5,
                false,
                None,
            )
            .await?;

        assert!(results.iter().any(|unit| {
            unit.domain == MemoryDomain::Organization
                && unit.id == read_view.id
                && unit.content.contains("troubleshooting")
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_merges_by_shared_topic_alias() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        for user_id in ["author_a", "author_b"] {
            engine.set_org_share_policy(
                user_id,
                "org_alpha",
                &memorose_common::SharePolicy {
                    contribute: true,
                    consume: false,
                    include_history: false,
                    targets: vec![],
                },
            )?;
        }
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source_a = MemoryUnit::new(
            Some("org_alpha".into()),
            "author_a".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I restart our cleanup worker when alerts fire.".into(),
            Some(vec![1.0; 768]),
        );
        source_a.level = 2;
        source_a.keywords = vec!["Restart Runbook".into(), "Cleanup Playbook".into()];

        let mut source_b = MemoryUnit::new(
            Some("org_alpha".into()),
            "author_b".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "We retry the cleanup worker after failed jobs.".into(),
            Some(vec![1.0; 768]),
        );
        source_b.level = 2;
        source_b.keywords = vec!["Retry Procedure".into(), "Cleanup Playbook".into()];

        engine.store_memory_unit(source_a.clone()).await?;
        engine.store_memory_unit(source_b.clone()).await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "cleanup playbook worker",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        let read_view = shared
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        assert_eq!(read_view.keywords[0], "Cleanup Playbook");
        assert!(read_view.keywords.contains(&"Restart Runbook".to_string()));
        assert!(read_view.keywords.contains(&"Retry Procedure".to_string()));
        let record = engine
            .load_organization_knowledge(read_view.id)?
            .expect("expected organization knowledge record");
        assert_eq!(
            engine
                .resolve_organization_record_source_ids(&record)
                .await?
                .len(),
            2
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_disabling_org_contribution_preserves_other_topic_sources() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        for user_id in ["author_a", "author_b"] {
            engine.set_org_share_policy(
                user_id,
                "org_cleanup",
                &memorose_common::SharePolicy {
                    contribute: true,
                    consume: false,
                    include_history: false,
                    targets: vec![],
                },
            )?;
        }
        engine.set_org_share_policy(
            "consumer",
            "org_cleanup",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source_a = MemoryUnit::new(
            Some("org_cleanup".into()),
            "author_a".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I restart our cleanup worker when alerts fire.".into(),
            Some(vec![1.0; 768]),
        );
        source_a.level = 2;
        source_a.keywords = vec!["Cleanup Playbook".into(), "Restart".into()];

        let mut source_b = MemoryUnit::new(
            Some("org_cleanup".into()),
            "author_b".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "We re-run the cleanup worker after failed jobs.".into(),
            Some(vec![1.0; 768]),
        );
        source_b.level = 2;
        source_b.keywords = vec!["Cleanup Playbook".into(), "Retry".into()];

        engine.store_memory_unit(source_a.clone()).await?;
        engine.store_memory_unit(source_b.clone()).await?;

        let removed = engine
            .disable_org_contribution("author_a", "org_cleanup")
            .await?;
        assert_eq!(removed, 1);
        assert!(engine.load_organization_membership(source_a.id)?.is_none());

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_cleanup"),
                None,
                "cleanup worker",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        let read_view = shared
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view to remain");

        assert_eq!(
            read_view.keywords,
            vec!["Cleanup Playbook".to_string(), "Retry".to_string()]
        );
        assert!(read_view.content.contains("the organization"));
        let record = engine
            .load_organization_knowledge(read_view.id)?
            .expect("expected organization knowledge record");
        assert_eq!(
            engine
                .resolve_organization_record_source_ids(&record)
                .await?,
            vec![source_b.id]
        );

        let removed_second = engine
            .disable_org_contribution("author_b", "org_cleanup")
            .await?;
        assert_eq!(removed_second, 1);

        let after = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_cleanup"),
                None,
                "cleanup worker",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;
        assert!(after.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_disabling_org_contribution_rebinds_topic_alias_mappings() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        for user_id in ["author_a", "author_b"] {
            engine.set_org_share_policy(
                user_id,
                "org_cleanup",
                &memorose_common::SharePolicy {
                    contribute: true,
                    consume: false,
                    include_history: false,
                    targets: vec![],
                },
            )?;
        }
        engine.set_org_share_policy(
            "consumer",
            "org_cleanup",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source_a = MemoryUnit::new(
            Some("org_cleanup".into()),
            "author_a".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I restart our cleanup worker when alerts fire.".into(),
            Some(vec![1.0; 768]),
        );
        source_a.level = 2;
        source_a.keywords = vec!["Restart Runbook".into(), "Cleanup Playbook".into()];

        let mut source_b = MemoryUnit::new(
            Some("org_cleanup".into()),
            "author_b".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "We retry the cleanup worker after failed jobs.".into(),
            Some(vec![1.0; 768]),
        );
        source_b.level = 2;
        source_b.keywords = vec!["Retry Procedure".into(), "Cleanup Playbook".into()];

        engine.store_memory_unit(source_a).await?;
        engine.store_memory_unit(source_b.clone()).await?;

        engine
            .disable_org_contribution("author_a", "org_cleanup")
            .await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_cleanup"),
                None,
                "retry procedure cleanup",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;
        let read_view = shared
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        let retry_mapping = MemoroseEngine::organization_topic_relation_key(
            "org_cleanup",
            &MemoroseEngine::build_organization_topic_key("Retry Procedure"),
        );
        let shared_mapping = MemoroseEngine::organization_topic_relation_key(
            "org_cleanup",
            &MemoroseEngine::build_organization_topic_key("Cleanup Playbook"),
        );

        assert_eq!(read_view.keywords[0], "Retry Procedure");
        let record = engine
            .load_organization_knowledge(read_view.id)?
            .expect("expected organization knowledge record");
        assert_eq!(
            engine
                .resolve_organization_record_source_ids(&record)
                .await?,
            vec![source_b.id]
        );
        assert_eq!(
            engine
                .load_organization_topic_relation(
                    "org_cleanup",
                    &MemoroseEngine::build_organization_topic_key("Retry Procedure"),
                )?
                .map(|relation| relation.knowledge_id),
            Some(read_view.id)
        );
        assert_eq!(
            engine
                .load_organization_topic_relation(
                    "org_cleanup",
                    &MemoroseEngine::build_organization_topic_key("Cleanup Playbook"),
                )?
                .map(|relation| relation.knowledge_id),
            Some(read_view.id)
        );
        assert!(engine.system_kv().get(retry_mapping.as_bytes())?.is_some());
        assert!(engine.system_kv().get(shared_mapping.as_bytes())?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_startup_reconcile_removes_persisted_org_views() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut stale_view = MemoryUnit::new_with_domain(
            Some("org_stale".into()),
            "stale_owner".into(),
            None,
            Uuid::nil(),
            memorose_common::MemoryType::Factual,
            MemoryDomain::Organization,
            "Stale persisted organization read view".into(),
            Some(vec![1.0; 768]),
        );
        stale_view.level = 2;
        stale_view.keywords = vec!["Stale".into()];

        let unit_key = format!("u:{}:unit:{}", stale_view.user_id, stale_view.id);
        let index_key = format!("idx:unit:{}", stale_view.id);
        engine
            .kv()
            .put(unit_key.as_bytes(), &serde_json::to_vec(&stale_view)?)?;
        engine
            .kv()
            .put(index_key.as_bytes(), stale_view.user_id.as_bytes())?;

        drop(engine);

        let reopened =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        assert!(reopened.kv().get(unit_key.as_bytes())?.is_none());
        assert!(reopened.kv().get(index_key.as_bytes())?.is_none());
        assert!(reopened
            .get_shared_search_hit_by_index(stale_view.id)
            .await?
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_startup_reconcile_removes_org_record_without_live_sources() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_reconcile",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_reconcile",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_reconcile".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Reconcile startup should remove orphaned org records.".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Startup Reconcile".into()];
        engine.store_memory_unit(source.clone()).await?;

        let read_view = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_reconcile"),
                None,
                "startup reconcile",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        let source_key = format!("u:{}:unit:{}", source.user_id, source.id);
        let source_index_key = format!("idx:unit:{}", source.id);
        engine.kv().delete(source_key.as_bytes())?;
        engine.kv().delete(source_index_key.as_bytes())?;

        drop(engine);

        let reopened =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        assert!(reopened
            .load_organization_knowledge(read_view.id)?
            .is_none());
        assert!(reopened
            .search_hybrid_with_shared(
                "consumer",
                Some("org_reconcile"),
                None,
                "startup reconcile",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?
            .into_iter()
            .all(|(unit, _)| unit.id != read_view.id));

        Ok(())
    }

    #[tokio::test]
    async fn test_startup_reconcile_cleans_stale_org_source_relations() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let relation = OrganizationKnowledgeRelationRecord {
            org_id: "org_stale_relation".into(),
            knowledge_id: Uuid::new_v4(),
            relation: OrganizationKnowledgeRelationKind::Source {
                source_id: Uuid::new_v4(),
            },
            updated_at: Utc::now(),
        };
        let primary_key = MemoroseEngine::organization_relation_key(&relation);
        let index_key = MemoroseEngine::organization_knowledge_relation_index_key(&relation);
        let bytes = serde_json::to_vec(&relation)?;

        engine.system_kv().put(primary_key.as_bytes(), &bytes)?;
        engine.system_kv().put(index_key.as_bytes(), &bytes)?;

        drop(engine);

        let reopened =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        assert!(reopened.system_kv().get(primary_key.as_bytes())?.is_none());
        assert!(reopened.system_kv().get(index_key.as_bytes())?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_org_relation_index_is_written_for_knowledge() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_index",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_index".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Index the organization relation structure.".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Relation Index".into(), "Org Index".into()];
        engine.store_memory_unit(source).await?;

        let record = engine
            .list_organization_knowledge_records(Some("org_index"), None)
            .await?
            .into_iter()
            .next()
            .expect("expected organization knowledge record");
        let relations = engine
            .list_organization_relations_for_knowledge(record.id)
            .await?;

        assert!(!relations.is_empty());
        for relation in relations {
            let index_key = MemoroseEngine::organization_knowledge_relation_index_key(&relation);
            assert!(engine.system_kv().get(index_key.as_bytes())?.is_some());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_org_relation_index_is_removed_with_read_view() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_index_cleanup",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_index_cleanup".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Remove relation index when organization read view is deleted.".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Relation Cleanup".into()];
        engine.store_memory_unit(source).await?;

        let record = engine
            .list_organization_knowledge_records(Some("org_index_cleanup"), None)
            .await?
            .into_iter()
            .next()
            .expect("expected organization knowledge record");
        let relation_prefix =
            MemoroseEngine::organization_knowledge_relation_index_prefix(record.id);
        assert!(!engine
            .system_kv()
            .scan(relation_prefix.as_bytes())?
            .is_empty());

        let removed = engine
            .disable_org_contribution("author", "org_index_cleanup")
            .await?;
        assert_eq!(removed, 1);
        assert!(engine.load_organization_knowledge(record.id)?.is_none());
        assert!(engine
            .system_kv()
            .scan(relation_prefix.as_bytes())?
            .is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_local_text_search_excludes_shared_org_read_view() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let source = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Read view should not leak into local text search".into(),
            None,
        );
        engine.store_memory_unit(source.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let local_results = engine
            .search_text(TEST_USER, "read view leak", 10, false, None)
            .await?;

        assert_eq!(local_results.len(), 1);
        assert!(local_results
            .iter()
            .all(|unit| MemoroseEngine::is_local_domain(&unit.domain)));

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_get_neighbors_and_multi_hop_traverse() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();
        let node_c = Uuid::new_v4();
        let node_d = Uuid::new_v4();

        for edge in [
            memorose_common::GraphEdge::new(
                TEST_USER.into(),
                node_a,
                node_b,
                memorose_common::RelationType::RelatedTo,
                0.9,
            ),
            memorose_common::GraphEdge::new(
                TEST_USER.into(),
                node_b,
                node_c,
                memorose_common::RelationType::RelatedTo,
                0.8,
            ),
            memorose_common::GraphEdge::new(
                TEST_USER.into(),
                node_b,
                node_d,
                memorose_common::RelationType::RelatedTo,
                0.2,
            ),
        ] {
            engine.graph().add_edge(&edge).await?;
        }
        engine.graph().flush().await?;

        let neighbors = engine
            .batch_get_neighbors(TEST_USER, &[node_a, node_b])
            .await?;
        assert_eq!(neighbors.get(&node_a).map(Vec::len), Some(1));
        assert_eq!(neighbors.get(&node_b).map(Vec::len), Some(2));

        let traversed = engine
            .multi_hop_traverse(TEST_USER, vec![node_a], 2, Some(0.5))
            .await?;
        assert!(traversed.contains(&node_a));
        assert!(traversed.contains(&node_b));
        assert!(traversed.contains(&node_c));
        assert!(!traversed.contains(&node_d));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_neighbors_cached_query_cache_stats_and_invalidate() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();
        let edge = memorose_common::GraphEdge::new(
            TEST_USER.into(),
            node_a,
            node_b,
            memorose_common::RelationType::RelatedTo,
            0.9,
        );
        engine.graph().add_edge(&edge).await?;
        engine.graph().flush().await?;

        let first = engine.get_neighbors_cached(TEST_USER, node_a).await?;
        assert_eq!(first.len(), 1);

        let stats = engine.query_cache_stats().await;
        assert_eq!(stats.edge_cache_size, 1);

        engine
            .graph()
            .delete_edges_for_node(TEST_USER, node_a)
            .await?;
        let cached = engine.get_neighbors_cached(TEST_USER, node_a).await?;
        assert_eq!(cached.len(), 1);

        engine.invalidate_query_cache(TEST_USER).await;
        let stats_after_invalidate = engine.query_cache_stats().await;
        assert_eq!(stats_after_invalidate.edge_cache_size, 1);
        engine.query_cache.clear().await;
        let after_invalidate = engine.get_neighbors_cached(TEST_USER, node_a).await?;
        assert!(after_invalidate.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_engine_filter_and_key_helpers() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        assert_eq!(engine.build_time_filter(None), None);
        assert_eq!(
            engine.build_time_filter(Some(TimeRange {
                start: Some(Utc.timestamp_micros(1_700_000_000_000_000).unwrap()),
                end: Some(Utc.timestamp_micros(1_700_000_300_000_000).unwrap()),
            })),
            Some("valid_time >= 1700000000000000 AND valid_time <= 1700000300000000".into())
        );
        assert_eq!(
            engine.build_user_filter("o'hara", Some("importance > 0.7".into())),
            Some("user_id = 'o''hara' AND importance > 0.7".into())
        );
        assert_eq!(
            engine.build_global_filter(
                MemoryDomain::Organization,
                Some("org'o"),
                Some("agent'a"),
                Some("importance > 0.5".into()),
            ),
            Some(
                "domain = 'organization' AND org_id = 'org''o' AND agent_id = 'agent''a' AND importance > 0.5"
                    .into(),
            )
        );

        assert_eq!(
            MemoroseEngine::org_share_policy_key("user_a", "org_b"),
            "share_policy:user:user_a:org:org_b"
        );
        let knowledge_id = Uuid::nil();
        assert_eq!(
            MemoroseEngine::organization_knowledge_key(knowledge_id),
            format!("organization_knowledge:{knowledge_id}")
        );

        Ok(())
    }

    #[test]
    fn test_engine_text_and_topic_helpers() {
        assert_eq!(
            MemoroseEngine::normalize_whitespace("  hello\t there\n   world  "),
            "hello there world"
        );
        assert_eq!(
            MemoroseEngine::neutralize_first_person_language(
                "I moved my project; we changed our plan for me."
            ),
            "the contributor moved the contributor's project; the organization changed the organization's plan for the contributor."
        );
        assert_eq!(
            MemoroseEngine::build_organization_topic_key(" Retry / Procedure! 2026 "),
            "retry-procedure-2026"
        );
        assert_eq!(
            MemoroseEngine::fallback_organization_topic_label(
                "  This   fallback topic uses six words max here  "
            ),
            Some("This fallback topic uses six words".into())
        );
        assert_eq!(
            MemoroseEngine::fallback_organization_topic_label(" \n\t "),
            None
        );

        assert_eq!(
            MemoroseEngine::organization_topic_candidates_from_keywords_and_content(
                &[
                    "Retry Procedure".into(),
                    "retry procedure".into(),
                    "Cleanup Playbook".into(),
                ],
                "ignored fallback content",
            ),
            vec![
                ("Retry Procedure".into(), "retry-procedure".into()),
                ("Cleanup Playbook".into(), "cleanup-playbook".into()),
            ]
        );
        assert_eq!(
            MemoroseEngine::organization_topic_candidates_from_keywords_and_content(
                &[],
                "Incident coordination playbook for regional outages and drills",
            ),
            vec![(
                "Incident coordination playbook for regional outages".into(),
                "incident-coordination-playbook-for-regional-outages".into(),
            )]
        );
    }

    #[test]
    fn test_engine_similarity_policy_and_metric_helpers() {
        let now = Utc.with_ymd_and_hms(2026, 4, 6, 9, 47, 58).unwrap();
        assert_eq!(
            MemoroseEngine::rac_metric_bucket_start(now),
            Utc.with_ymd_and_hms(2026, 4, 6, 9, 0, 0).unwrap()
        );

        let base_record = OrganizationKnowledgeRecord {
            id: Uuid::new_v4(),
            org_id: "org_similarity".into(),
            topic_label: "Cleanup Playbook".into(),
            topic_alias_keys: vec!["cleanup-playbook".into()],
            memory_type: MemoryType::Factual,
            content: "Cleanup worker retry steps for incidents".into(),
            embedding: Some(vec![1.0, 0.0]),
            keywords: vec!["Cleanup Playbook".into(), "Retry".into()],
            importance: 0.9,
            valid_time: None,
            created_at: now,
            updated_at: now,
        };

        let both = MemoroseEngine::organization_similarity_score(
            &base_record,
            "cleanup retry",
            &[1.0, 1.0],
        );
        let semantic_only = MemoroseEngine::organization_similarity_score(
            &base_record,
            "unrelated topic",
            &[1.0, 1.0],
        );
        let lexical_only = MemoroseEngine::organization_similarity_score(
            &OrganizationKnowledgeRecord {
                embedding: None,
                ..base_record.clone()
            },
            "cleanup retry",
            &[0.0, 1.0],
        );
        let none = MemoroseEngine::organization_similarity_score(
            &OrganizationKnowledgeRecord {
                embedding: None,
                keywords: vec!["totally different".into()],
                content: "nothing overlapping".into(),
                ..base_record
            },
            "cleanup retry",
            &[0.0, 1.0],
        );

        assert!(both > semantic_only);
        assert!(both > 0.0);
        assert!(semantic_only > 0.0);
        assert!(lexical_only > 0.0);
        assert_eq!(none, 0.0);

        let normalized = MemoroseEngine::normalize_share_policy(
            SharePolicy {
                contribute: true,
                consume: false,
                include_history: true,
                targets: vec![],
            },
            ShareTarget::Organization,
        );
        assert_eq!(normalized.targets, vec![ShareTarget::Organization]);
        assert!(normalized.contribute);
        assert!(!normalized.consume);
        assert!(normalized.include_history);
    }

    #[tokio::test]
    async fn test_engine_l3_task_helpers_and_auto_plan_goal() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(Arc::new(
                MockCorrectionLLM {
                    response: r#"[{"summary":"Plan","dependencies":[]},{"summary":"Execute","description":"Ship it","dependencies":["Plan"]}]"#.into(),
                },
            )));

        let user_id = "planner_user";
        let stream_id = Uuid::new_v4();
        let goal_id = Uuid::new_v4();

        engine
            .auto_plan_goal(
                Some("org_plan".into()),
                user_id.into(),
                Some("agent_plan".into()),
                stream_id,
                goal_id,
                "ship release".into(),
                0,
            )
            .await?;

        let tasks = engine.list_l3_tasks(user_id).await?;
        assert_eq!(tasks.len(), 2);

        let plan = tasks.iter().find(|task| task.title == "Plan").unwrap();
        let execute = tasks.iter().find(|task| task.title == "Execute").unwrap();
        assert_eq!(plan.parent_id, Some(goal_id));
        assert_eq!(execute.parent_id, Some(goal_id));
        assert_eq!(execute.description, "Ship it");
        assert_eq!(execute.dependencies, vec![plan.task_id]);
        assert_eq!(
            engine
                .get_l3_task(user_id, plan.task_id)
                .await?
                .map(|task| task.title),
            Some("Plan".into())
        );

        let outgoing = engine
            .graph()
            .get_outgoing_edges(user_id, execute.task_id)
            .await?;
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].target_id, goal_id);
        assert_eq!(outgoing[0].relation, RelationType::IsSubTaskOf);

        let empty_engine = MemoroseEngine::new_with_default_threshold(
            temp_dir.path().join("empty"),
            1000,
            true,
            true,
        )
        .await?
        .with_arbitrator(crate::arbitrator::Arbitrator::with_client(Arc::new(
            MockCorrectionLLM {
                response: "[]".into(),
            },
        )));
        empty_engine
            .auto_plan_goal(
                None,
                "nobody".into(),
                None,
                Uuid::new_v4(),
                Uuid::new_v4(),
                "noop".into(),
                0,
            )
            .await?;
        assert!(empty_engine.list_l3_tasks("nobody").await?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_ready_l3_tasks_filters_blocked_and_missing_dependencies() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let user_id = "task_user";

        let mut completed =
            memorose_common::L3Task::new(None, user_id.into(), None, "Done".into(), "done".into());
        completed.status = memorose_common::TaskStatus::Completed;

        let ready = memorose_common::L3Task::new(
            None,
            user_id.into(),
            None,
            "Ready".into(),
            "ready".into(),
        );

        let mut blocked_by_progress = memorose_common::L3Task::new(
            None,
            user_id.into(),
            None,
            "Waiting".into(),
            "waiting".into(),
        );
        blocked_by_progress.dependencies = vec![completed.task_id];
        blocked_by_progress.status = memorose_common::TaskStatus::InProgress;

        let mut dependent_ready = memorose_common::L3Task::new(
            None,
            user_id.into(),
            None,
            "DependentReady".into(),
            "dep ready".into(),
        );
        dependent_ready.dependencies = vec![completed.task_id];

        let mut blocked_missing_dep = memorose_common::L3Task::new(
            None,
            user_id.into(),
            None,
            "MissingDep".into(),
            "missing dep".into(),
        );
        blocked_missing_dep.dependencies = vec![Uuid::new_v4()];

        let mut blocked_incomplete_dep = memorose_common::L3Task::new(
            None,
            user_id.into(),
            None,
            "BlockedIncomplete".into(),
            "blocked incomplete".into(),
        );
        blocked_incomplete_dep.dependencies = vec![ready.task_id];

        for task in [
            completed.clone(),
            ready.clone(),
            blocked_by_progress,
            dependent_ready.clone(),
            blocked_missing_dep,
            blocked_incomplete_dep,
        ] {
            engine.store_l3_task(&task).await?;
        }

        let mut titles = engine
            .get_ready_l3_tasks(user_id)
            .await?
            .into_iter()
            .map(|task| task.title)
            .collect::<Vec<_>>();
        titles.sort();

        assert_eq!(
            titles,
            vec!["DependentReady".to_string(), "Ready".to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_engine_organization_snapshot_helpers_and_detail_sorting() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let now = Utc.with_ymd_and_hms(2026, 4, 6, 10, 0, 0).unwrap();

        let source_a = MemoryUnit::new(
            Some("org_snapshot".into()),
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "source a".into(),
            None,
        );
        let source_b = MemoryUnit::new(
            Some("org_snapshot".into()),
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "source b".into(),
            None,
        );
        let fallback_source = MemoryUnit::new(
            Some("org_snapshot".into()),
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "fallback source".into(),
            None,
        );
        engine
            .store_memory_units(vec![
                source_a.clone(),
                source_b.clone(),
                fallback_source.clone(),
            ])
            .await?;

        let record = OrganizationKnowledgeRecord {
            id: Uuid::new_v4(),
            org_id: "org_snapshot".into(),
            topic_label: "Release Guide".into(),
            topic_alias_keys: vec!["release-guide".into()],
            memory_type: MemoryType::Factual,
            content: "release guide body".into(),
            embedding: None,
            keywords: vec!["release".into()],
            importance: 0.8,
            valid_time: None,
            created_at: now,
            updated_at: now,
        };
        let read_view = MemoroseEngine::materialize_organization_read_view(&record);

        let contribution_active = OrganizationKnowledgeContributionRecord {
            org_id: "org_snapshot".into(),
            knowledge_id: record.id,
            source_id: source_a.id,
            contributor_user_id: TEST_USER.into(),
            status: OrganizationKnowledgeContributionStatus::Active,
            candidate_at: Some(now),
            activated_at: Some(now),
            approval_mode: Some(OrganizationKnowledgeApprovalMode::Auto),
            approved_by: Some("system".into()),
            updated_at: now,
            revoked_at: None,
        };
        let contribution_candidate = OrganizationKnowledgeContributionRecord {
            org_id: "org_snapshot".into(),
            knowledge_id: record.id,
            source_id: source_b.id,
            contributor_user_id: TEST_USER.into(),
            status: OrganizationKnowledgeContributionStatus::Candidate,
            candidate_at: Some(now),
            activated_at: None,
            approval_mode: None,
            approved_by: None,
            updated_at: now + chrono::Duration::seconds(5),
            revoked_at: None,
        };
        let contribution_revoked = OrganizationKnowledgeContributionRecord {
            org_id: "org_snapshot".into(),
            knowledge_id: record.id,
            source_id: fallback_source.id,
            contributor_user_id: TEST_USER.into(),
            status: OrganizationKnowledgeContributionStatus::Revoked,
            candidate_at: Some(now),
            activated_at: None,
            approval_mode: None,
            approved_by: None,
            updated_at: now + chrono::Duration::seconds(10),
            revoked_at: Some(now + chrono::Duration::seconds(10)),
        };

        let membership_a = OrganizationKnowledgeMembershipRecord {
            org_id: "org_snapshot".into(),
            knowledge_id: record.id,
            source_id: source_a.id,
            contributor_user_id: TEST_USER.into(),
            updated_at: now,
        };
        let membership_b = OrganizationKnowledgeMembershipRecord {
            org_id: "org_snapshot".into(),
            knowledge_id: record.id,
            source_id: source_b.id,
            contributor_user_id: TEST_USER.into(),
            updated_at: now + chrono::Duration::seconds(1),
        };

        let detail = engine
            .build_organization_knowledge_detail_record_from_snapshot(
                OrganizationKnowledgeSnapshot {
                    record: record.clone(),
                    read_view: read_view.clone(),
                    membership_sources: vec![
                        (membership_b.clone(), source_b.clone()),
                        (membership_a.clone(), source_a.clone()),
                    ],
                    contributions: vec![
                        contribution_revoked.clone(),
                        contribution_candidate.clone(),
                        contribution_active.clone(),
                    ],
                },
            )
            .await;

        assert_eq!(detail.record.id, record.id);
        assert_eq!(detail.read_view.id, read_view.id);
        assert_eq!(detail.memberships.len(), 2);
        assert_eq!(detail.memberships[0].membership.source_id, source_a.id);
        assert_eq!(detail.memberships[1].membership.source_id, source_b.id);
        assert_eq!(detail.contributions.len(), 3);
        assert_eq!(detail.contributions[0].contribution.source_id, source_a.id);
        assert_eq!(detail.contributions[1].contribution.source_id, source_b.id);
        assert_eq!(
            detail.contributions[2].contribution.source_id,
            fallback_source.id
        );
        assert_eq!(
            detail.contributions[2]
                .source_unit
                .as_ref()
                .map(|unit| unit.id),
            Some(fallback_source.id)
        );

        let active_memberships = MemoroseEngine::organization_memberships_from_contributions(&[
            contribution_active.clone(),
            contribution_candidate,
            contribution_revoked,
        ]);
        assert_eq!(active_memberships.len(), 1);
        assert_eq!(active_memberships[0].source_id, source_a.id);

        Ok(())
    }

    #[tokio::test]
    async fn test_engine_list_organization_knowledge_snapshots_orders_and_filters() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let now = Utc.with_ymd_and_hms(2026, 4, 6, 11, 0, 0).unwrap();

        let record_old = OrganizationKnowledgeRecord {
            id: Uuid::new_v4(),
            org_id: "org_a".into(),
            topic_label: "Alpha".into(),
            topic_alias_keys: vec!["alpha".into()],
            memory_type: MemoryType::Factual,
            content: "alpha".into(),
            embedding: None,
            keywords: vec!["alpha".into()],
            importance: 0.5,
            valid_time: None,
            created_at: now,
            updated_at: now,
        };
        let record_new = OrganizationKnowledgeRecord {
            id: Uuid::new_v4(),
            org_id: "org_a".into(),
            topic_label: "Beta".into(),
            topic_alias_keys: vec!["beta".into()],
            memory_type: MemoryType::Factual,
            content: "beta".into(),
            embedding: None,
            keywords: vec!["beta".into()],
            importance: 0.7,
            valid_time: None,
            created_at: now,
            updated_at: now + chrono::Duration::seconds(30),
        };
        let other_org = OrganizationKnowledgeRecord {
            id: Uuid::new_v4(),
            org_id: "org_b".into(),
            topic_label: "Gamma".into(),
            topic_alias_keys: vec!["gamma".into()],
            memory_type: MemoryType::Factual,
            content: "gamma".into(),
            embedding: None,
            keywords: vec!["gamma".into()],
            importance: 0.4,
            valid_time: None,
            created_at: now,
            updated_at: now + chrono::Duration::seconds(10),
        };

        engine.store_organization_knowledge(&record_old)?;
        engine.store_organization_knowledge(&record_new)?;
        engine.store_organization_knowledge(&other_org)?;

        let filtered = engine
            .list_organization_knowledge_snapshots(Some("org_a"))
            .await?;
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].record.id, record_new.id);
        assert_eq!(filtered[1].record.id, record_old.id);

        let all = engine.list_organization_knowledge_snapshots(None).await?;
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].record.id, record_new.id);

        assert!(engine
            .get_organization_knowledge_detail_record(Uuid::new_v4())
            .await?
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_engine_native_hit_and_metric_counter_helpers() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "native hit".into(),
            None,
        );
        let mut hit = SharedSearchHit::native(unit.clone());
        hit.keywords.push("tag".into());
        assert_eq!(hit.memory_unit().id, unit.id);
        assert_eq!(
            hit.clone().into_memory_unit().keywords,
            vec!["tag".to_string()]
        );

        let mut metrics = RacMetricSnapshot {
            fact_extraction_attempt_total: 1,
            fact_extraction_success_total: 2,
            correction_action_obsolete_total: 3,
            correction_action_contradicts_total: 4,
            correction_action_reaffirm_total: 5,
            correction_action_ignore_total: 6,
            tombstone_total: 7,
        };
        metrics.merge(&RacMetricSnapshot {
            fact_extraction_attempt_total: 10,
            fact_extraction_success_total: 20,
            correction_action_obsolete_total: 30,
            correction_action_contradicts_total: 40,
            correction_action_reaffirm_total: 50,
            correction_action_ignore_total: 60,
            tombstone_total: 70,
        });
        assert_eq!(metrics.fact_extraction_attempt_total, 11);
        assert_eq!(metrics.tombstone_total, 77);

        assert!(matches!(
            OrganizationKnowledgeContributionStatus::default(),
            OrganizationKnowledgeContributionStatus::Active
        ));

        engine.increment_organization_metric_counter("org_metrics", "auto_approved_total", 0)?;
        engine.increment_organization_metric_counter("org_metrics", "auto_approved_total", 2)?;
        engine.increment_organization_metric_counter("org_metrics", "revoke_total", 1)?;
        let org_snapshot = engine.get_organization_automation_counter_snapshot("org_metrics")?;
        assert_eq!(org_snapshot.auto_approved_total, 2);
        assert_eq!(org_snapshot.revoke_total, 1);
        assert_eq!(
            engine.get_organization_metric_counter("org_metrics", "missing")?,
            0
        );

        engine.increment_rac_metric_counter("fact_extraction_attempt_total", 0)?;
        engine.increment_rac_metric_counter("fact_extraction_attempt_total", 3)?;
        engine.increment_rac_metric_counter("tombstone_total", 2)?;
        let rac_snapshot = engine.get_rac_metric_snapshot()?;
        assert_eq!(rac_snapshot.fact_extraction_attempt_total, 3);
        assert_eq!(rac_snapshot.tombstone_total, 2);

        Ok(())
    }

    #[test]
    fn test_apply_token_budget_to_scored_memory_units_truncates_ranked_results() {
        let first = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "alpha beta gamma delta".into(),
            None,
        );
        let second = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "epsilon zeta eta theta iota kappa".into(),
            None,
        );

        let first_cost = MemoroseEngine::memory_unit_token_cost(&first);
        let second_cost = MemoroseEngine::memory_unit_token_cost(&second);
        let results = vec![(first.clone(), 0.9), (second.clone(), 0.8)];

        let budgeted =
            MemoroseEngine::apply_token_budget_to_scored_memory_units(results, Some(first_cost));
        assert_eq!(budgeted.len(), 1);
        assert_eq!(budgeted[0].0.id, first.id);

        let unbounded = MemoroseEngine::apply_token_budget_to_scored_memory_units(
            vec![(first.clone(), 0.9), (second.clone(), 0.8)],
            Some(first_cost + second_cost),
        );
        assert_eq!(unbounded.len(), 2);
    }

    #[test]
    fn test_apply_token_budget_to_scored_shared_hits_truncates_ranked_results() {
        let first = SharedSearchHit::native(MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "shared alpha beta gamma".into(),
            None,
        ));
        let second = SharedSearchHit::native(MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "shared epsilon zeta eta theta".into(),
            None,
        ));

        let first_cost = MemoroseEngine::memory_unit_token_cost(first.memory_unit());
        let budgeted = MemoroseEngine::apply_token_budget_to_scored_shared_hits(
            vec![(first.clone(), 0.9), (second, 0.8)],
            Some(first_cost),
        );
        assert_eq!(budgeted.len(), 1);
        assert_eq!(budgeted[0].0.id, first.id);
    }

    #[test]
    fn test_apply_token_budget_skips_oversized_item_and_keeps_later_fit() {
        let oversized = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu".into(),
            None,
        );
        let small = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "tiny fit".into(),
            None,
        );

        let budget = MemoroseEngine::memory_unit_token_cost(&small);
        let budgeted = MemoroseEngine::apply_token_budget_to_scored_memory_units(
            vec![(oversized, 0.95), (small.clone(), 0.80)],
            Some(budget),
        );

        assert_eq!(budgeted.len(), 1);
        assert_eq!(budgeted[0].0.id, small.id);
    }
}

#[cfg(test)]
mod missing_coverage_tests {
    use super::*;

    #[test]
    fn test_rac_metric_history_point_merge() {
        let mut p1 = RacMetricHistoryPoint {
            bucket_start: "100".into(),
            fact_extraction_attempt_total: 1,
            fact_extraction_success_total: 2,
            correction_action_obsolete_total: 3,
            correction_action_contradicts_total: 4,
            correction_action_reaffirm_total: 5,
            correction_action_ignore_total: 6,
            tombstone_total: 7,
        };
        let p2 = RacMetricHistoryPoint {
            bucket_start: "200".into(),
            fact_extraction_attempt_total: 10,
            fact_extraction_success_total: 20,
            correction_action_obsolete_total: 30,
            correction_action_contradicts_total: 40,
            correction_action_reaffirm_total: 50,
            correction_action_ignore_total: 60,
            tombstone_total: 70,
        };
        p1.merge(&p2);
        assert_eq!(p1.fact_extraction_attempt_total, 11);
        assert_eq!(p1.fact_extraction_success_total, 22);
        assert_eq!(p1.correction_action_obsolete_total, 33);
        assert_eq!(p1.correction_action_contradicts_total, 44);
        assert_eq!(p1.correction_action_reaffirm_total, 55);
        assert_eq!(p1.correction_action_ignore_total, 66);
        assert_eq!(p1.tombstone_total, 77);
    }

    #[test]
    fn test_matches_valid_time_filter() {
        use chrono::TimeZone;
        let t1 = Utc.timestamp_opt(1000, 0).unwrap();
        let t2 = Utc.timestamp_opt(2000, 0).unwrap();
        let t3 = Utc.timestamp_opt(3000, 0).unwrap();

        // No range
        assert!(MemoroseEngine::matches_valid_time_filter(Some(t2), None));

        // No valid_time but range exists
        let range = TimeRange {
            start: Some(t1),
            end: Some(t3),
        };
        assert!(!MemoroseEngine::matches_valid_time_filter(
            None,
            Some(&range)
        ));

        // valid_time < start
        assert!(!MemoroseEngine::matches_valid_time_filter(
            Some(t1 - chrono::Duration::seconds(1)),
            Some(&range)
        ));

        // valid_time > end
        assert!(!MemoroseEngine::matches_valid_time_filter(
            Some(t3 + chrono::Duration::seconds(1)),
            Some(&range)
        ));

        // Inside range
        assert!(MemoroseEngine::matches_valid_time_filter(
            Some(t2),
            Some(&range)
        ));
    }

    #[test]
    fn test_memory_unit_token_cost() {
        let mut unit = MemoryUnit::new(
            None,
            "user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Hello world".into(),
            None,
        );
        unit.keywords = vec!["keyword1".into(), "keyword2".into()];
        unit.assets.push(memorose_common::Asset {
            asset_type: "image".into(),
            storage_key: "http://example.com/image.png".into(),
            original_name: "image.png".into(),
            description: Some("A nice image".into()),
            metadata: std::collections::HashMap::new(),
        });

        let cost = MemoroseEngine::memory_unit_token_cost(&unit);
        assert!(cost > 0);
    }

    #[tokio::test]
    async fn test_memorose_engine_new_helper() {
        let temp = tempfile::tempdir().unwrap();
        let engine = MemoroseEngine::new(temp.path(), 1000, false, false, 0.5, 128)
            .await
            .unwrap();
        assert_eq!(engine.auto_planner, false);
    }
}
