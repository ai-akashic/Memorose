use crate::arbitrator::MemoryCorrectionKind;
use chrono::{DateTime, Utc};
use memorose_common::{GraphEdge, MaterializationState, MemoryType, MemoryUnit, RelationType};
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};
use uuid::Uuid;

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
    Multimodal {
        parts: Vec<PendingMaterializationPart>,
    },
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
pub(crate) struct OrganizationProjectionTopic {
    pub(crate) label: String,
    pub(crate) alias_keys: Vec<String>,
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
pub(crate) enum OrganizationKnowledgeRelationKind {
    Source { source_id: Uuid },
    TopicAlias { topic_key: String },
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct OrganizationKnowledgeRelationRecord {
    pub(crate) org_id: String,
    pub(crate) knowledge_id: Uuid,
    pub(crate) relation: OrganizationKnowledgeRelationKind,
    pub(crate) updated_at: DateTime<Utc>,
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

    pub(crate) fn organization_knowledge(
        record: &OrganizationKnowledgeRecord,
        unit: MemoryUnit,
    ) -> Self {
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
pub(crate) enum OrganizationPublicationKind {
    New,
    Rebuild,
}

pub(crate) struct OrganizationKnowledgeMutation {
    pub(crate) topic_relations: Vec<OrganizationKnowledgeRelationRecord>,
    pub(crate) candidate_contribution_records: Vec<OrganizationKnowledgeContributionRecord>,
    pub(crate) stale_relation_keys: Vec<String>,
    pub(crate) obsolete_records: Vec<OrganizationKnowledgeRecord>,
    pub(crate) record: OrganizationKnowledgeRecord,
    pub(crate) unit: MemoryUnit,
}

pub(crate) struct OrganizationKnowledgeSnapshot {
    pub(crate) record: OrganizationKnowledgeRecord,
    pub(crate) read_view: MemoryUnit,
    pub(crate) membership_sources: Vec<(OrganizationKnowledgeMembershipRecord, MemoryUnit)>,
    pub(crate) contributions: Vec<OrganizationKnowledgeContributionRecord>,
}

#[derive(Default)]
pub(crate) struct OrganizationStorageReconciliationStats {
    pub(crate) removed_persisted_views: usize,
    pub(crate) reconciled_records: usize,
    pub(crate) removed_records: usize,
    pub(crate) removed_stale_source_relations: usize,
}
