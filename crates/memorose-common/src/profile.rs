use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Lifecycle status of an individual value inside a profile slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProfileValueStatus {
    /// Currently believed to be true.
    #[default]
    Active,
    /// Superseded by a newer, incompatible value within the same slot.
    Obsoleted,
    /// Explicitly negated by the user (e.g. "I no longer drink coffee").
    Negated,
    /// Known to have been true in the past, retained for history.
    Historical,
}

/// Overall state of a profile slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProfileSlotState {
    /// Normal, automatically maintained slot.
    #[default]
    Stable,
    /// A low-confidence or contradictory promotion is awaiting manual review.
    PendingReview,
    /// A human pinned this slot; automatic promotion must not obsolete its values.
    ManuallyPinned,
}

/// Maximum number of source memory ids tracked per value for provenance.
pub const PROFILE_VALUE_MAX_SOURCES: usize = 16;

/// A single ranked value within a profile slot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProfileSlotValue {
    /// Human-facing value, e.g. "Berlin".
    pub value: String,
    /// Normalized comparison key (== `MemoryFactDescriptor.value_payload.comparison_key()`).
    pub canonical_value: String,
    /// Aggregated confidence in `[0, 1]` after merges.
    pub confidence: f32,
    /// Number of distinct reaffirming memories.
    pub support_count: u32,
    pub status: ProfileValueStatus,
    /// Most recent change type label that touched this value.
    pub last_change_type: String,
    /// Provenance: source memory unit ids (capped at `PROFILE_VALUE_MAX_SOURCES`).
    #[serde(default)]
    pub source_unit_ids: Vec<Uuid>,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
}

impl ProfileSlotValue {
    pub fn is_active(&self) -> bool {
        matches!(self.status, ProfileValueStatus::Active)
    }
}

/// A profile slot aggregating all values for one `(subject, attribute)` pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileSlot {
    /// `"{subject:?}|{subject_key}|{attribute:?}"` — stable identity.
    pub slot_key: String,
    pub user_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,
    /// "self" | "organization" | "agent" | "external".
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_name: Option<String>,
    /// Attribute label, e.g. "residence".
    pub attribute: String,
    #[serde(default)]
    pub state: ProfileSlotState,
    /// Values, persisted already sorted in rank order.
    pub values: Vec<ProfileSlotValue>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub version: u32,
}

impl ProfileSlot {
    /// Active values only, in persisted rank order.
    pub fn active_values(&self) -> impl Iterator<Item = &ProfileSlotValue> {
        self.values.iter().filter(|v| v.is_active())
    }

    /// Re-sort values into canonical rank order:
    /// active first -> confidence desc -> last_seen desc -> support desc -> canonical asc.
    pub fn sort_values(&mut self) {
        self.values.sort_by(|a, b| {
            b.is_active()
                .cmp(&a.is_active())
                .then(
                    b.confidence
                        .partial_cmp(&a.confidence)
                        .unwrap_or(std::cmp::Ordering::Equal),
                )
                .then(b.last_seen_at.cmp(&a.last_seen_at))
                .then(b.support_count.cmp(&a.support_count))
                .then(a.canonical_value.cmp(&b.canonical_value))
        });
    }
}

// ---------------------------------------------------------------------------
// Org-level aggregation (read-only analytics across an organization's users)
// ---------------------------------------------------------------------------

/// How many distinct users in an org hold a given canonical value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrgProfileValueCount {
    pub canonical_value: String,
    /// A representative display value for the canonical value.
    pub value: String,
    pub user_count: usize,
}

/// Distribution of active values for one attribute across an org's users.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrgProfileAttribute {
    pub attribute: String,
    /// Values sorted by `user_count` descending.
    pub values: Vec<OrgProfileValueCount>,
}

/// Aggregate of active profile values across all users in an organization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrgProfileAggregate {
    pub org_id: String,
    /// Distinct users in the org with at least one matching active profile value.
    pub user_count: usize,
    /// Attributes sorted by name.
    pub attributes: Vec<OrgProfileAttribute>,
}
