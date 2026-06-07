use crate::arbitrator::MemoryCorrectionKind;
use crate::engine::review_queue::ReviewStatus;
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
    pub status: ReviewStatus,
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

// ---------------------------------------------------------------------------
// Profile memory layer
// ---------------------------------------------------------------------------

/// Outcome of attempting to promote a single L1 fact into a profile slot.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProfilePromotionOutcome {
    /// A new value was added to (or created) the slot.
    Promoted,
    /// An existing value was reaffirmed / updated in place.
    Merged,
    /// Queued for manual review (low confidence or pinned-slot conflict).
    QueuedForReview,
    /// Not eligible (unparsable, out-of-scope subject, below threshold).
    Skipped,
}

/// Result of a manual profile patch. Distinguishes client-side conditions
/// (slot/value not found) from genuine storage errors so the HTTP layer can map
/// them to the right status code instead of collapsing everything to 400.
pub enum ProfilePatchOutcome {
    /// The patch was applied; carries the updated slot.
    Applied(Box<memorose_common::ProfileSlot>),
    /// The addressed slot does not exist.
    SlotNotFound,
    /// The addressed canonical value does not exist in the slot.
    ValueNotFound { canonical_value: String },
}

/// A manual edit applied to a profile slot via the PATCH API or review approval.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "op")]
pub enum ProfileSlotPatch {
    /// Force a canonical value to `Active` (creating it if absent), demoting
    /// incompatible siblings to `Obsoleted` for single-value attributes.
    SetActiveValue { canonical_value: String },
    /// Mark a canonical value `Obsoleted`.
    ObsoleteValue { canonical_value: String },
    /// Remove a canonical value entirely.
    RemoveValue { canonical_value: String },
    /// Pin the slot so automatic promotion cannot obsolete its values.
    Pin,
    /// Unpin the slot.
    Unpin,
    /// Override a value's confidence.
    SetConfidence {
        canonical_value: String,
        confidence: f32,
    },
}

/// A queued profile promotion awaiting human review. Mirrors [`RacReviewRecord`]
/// but carries the proposed slot value instead of a source/target unit pair.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfileReviewRecord {
    pub review_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub user_id: String,
    pub org_id: Option<String>,
    pub slot_key: String,
    pub attribute: String,
    pub subject: String,
    /// Canonical subject ref (e.g. "user:self"), stored at enqueue so approval
    /// never has to re-parse it out of `slot_key`. `default` for older records.
    #[serde(default)]
    pub subject_ref: Option<String>,
    pub proposed_value: String,
    pub proposed_canonical_value: String,
    pub proposed_confidence: f32,
    pub change_type: String,
    pub source_unit_id: Uuid,
    pub reason: String,
    pub status: ReviewStatus,
    pub reviewer: Option<String>,
    pub reviewer_note: Option<String>,
}

/// Append-only audit entry recording how a profile slot changed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfileAuditEntry {
    pub created_at: DateTime<Utc>,
    pub user_id: String,
    pub slot_key: String,
    /// e.g. "promote", "merge", "obsolete", "negate", "patch", "review_approve".
    pub action: String,
    pub canonical_value: String,
    pub confidence: f32,
    pub change_type: String,
    pub source_unit_id: Option<Uuid>,
    pub reason: Option<String>,
}
