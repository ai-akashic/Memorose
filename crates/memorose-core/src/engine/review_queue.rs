use super::types::{ProfileReviewRecord, RacReviewRecord};
use super::MemoroseEngine;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::cmp::Ordering;

/// Lifecycle status shared by every review-queue record. Serialized as the same
/// snake_case strings the per-queue enums used before, so on-disk and HTTP wire
/// formats are unchanged.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewStatus {
    Pending,
    Approved,
    Rejected,
}

/// Common header every review-queue record exposes. The storage/query plumbing
/// (`store_review`/`get_review`/`list_reviews`) is generic over this trait so the
/// RAC and profile queues share one implementation; each record keeps its own
/// flat serde shape, key prefix, and payload.
pub(crate) trait ReviewRecord: Serialize + DeserializeOwned + Clone {
    /// system_kv key prefix, e.g. `"rac_review:"` / `"profile_review:"`.
    const KEY_PREFIX: &'static str;
    fn review_id(&self) -> &str;
    fn status(&self) -> ReviewStatus;
    fn user_id(&self) -> &str;
    fn org_id(&self) -> Option<&str>;
    fn created_at(&self) -> DateTime<Utc>;
    fn set_status(&mut self, status: ReviewStatus);
    /// Stamp reviewer attribution on resolve.
    fn stamp_review(
        &mut self,
        reviewer: Option<String>,
        reviewer_note: Option<String>,
        updated_at: DateTime<Utc>,
    );
    /// Deterministic tiebreaker applied after the primary `created_at`-descending
    /// sort. Defaults to `Equal`; queues that need stable ordering override it.
    fn list_tiebreak(&self, _other: &Self) -> Ordering {
        Ordering::Equal
    }
}

/// Outcome of loading a review for resolution. Lets the type-specific
/// `resolve_*` methods share the load + ownership-guard + pending-check head
/// while keeping their distinct approve/reject effects inline.
pub(crate) enum ResolveStart<R> {
    /// No such review, or it belongs to a different owner (cross-tenant).
    NotFound,
    /// Already approved/rejected; return as-is without re-applying any effect.
    AlreadyResolved(R),
    /// Pending and owned — caller runs its effect then calls `finish_resolve_review`.
    Pending(R),
}

impl MemoroseEngine {
    pub(crate) fn review_storage_key<R: ReviewRecord>(review_id: &str) -> String {
        format!("{}{}", R::KEY_PREFIX, review_id)
    }

    pub(crate) fn store_review<R: ReviewRecord>(&self, review: &R) -> Result<()> {
        self.system_kv().put(
            Self::review_storage_key::<R>(review.review_id()).as_bytes(),
            &serde_json::to_vec(review)?,
        )
    }

    pub(crate) fn get_review<R: ReviewRecord>(&self, review_id: &str) -> Result<Option<R>> {
        let key = Self::review_storage_key::<R>(review_id);
        Ok(self
            .system_kv()
            .get(key.as_bytes())?
            .and_then(|bytes| serde_json::from_slice::<R>(&bytes).ok()))
    }

    pub(crate) fn list_reviews<R: ReviewRecord>(
        &self,
        status_filter: Option<ReviewStatus>,
        user_id_filter: Option<&str>,
        org_id_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<R>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut records = self
            .system_kv()
            .scan(R::KEY_PREFIX.as_bytes())?
            .into_iter()
            .filter_map(|(_, value)| serde_json::from_slice::<R>(&value).ok())
            .filter(|record| status_filter.is_none_or(|status| record.status() == status))
            .filter(|record| user_id_filter.is_none_or(|user_id| record.user_id() == user_id))
            .filter(|record| org_id_filter.is_none_or(|org_id| record.org_id() == Some(org_id)))
            .collect::<Vec<_>>();

        records.sort_by(|left, right| {
            right
                .created_at()
                .cmp(&left.created_at())
                .then_with(|| left.list_tiebreak(right))
        });
        records.truncate(limit);
        Ok(records)
    }

    /// Short-circuiting existence check over a queue: returns `true` as soon as a
    /// record (optionally scoped to `user_id`) satisfies `predicate`. Cheaper than
    /// `list_reviews(.., usize::MAX)` for boolean checks — no collect/sort/alloc.
    pub(crate) fn any_review_matches<R: ReviewRecord>(
        &self,
        user_id: Option<&str>,
        predicate: impl Fn(&R) -> bool,
    ) -> Result<bool> {
        for (_, value) in self.system_kv().scan(R::KEY_PREFIX.as_bytes())? {
            let Ok(record) = serde_json::from_slice::<R>(&value) else {
                continue;
            };
            if user_id.is_none_or(|uid| record.user_id() == uid) && predicate(&record) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Load a review for resolution, applying the shared head: not-found,
    /// cross-tenant (when `expected_user_id` is set), and already-resolved are all
    /// surfaced as [`ResolveStart`] variants. A `Pending` result is owned and ready
    /// for the caller's type-specific effect.
    pub(crate) fn begin_resolve_review<R: ReviewRecord>(
        &self,
        review_id: &str,
        expected_user_id: Option<&str>,
    ) -> Result<ResolveStart<R>> {
        let Some(review) = self.get_review::<R>(review_id)? else {
            return Ok(ResolveStart::NotFound);
        };
        if expected_user_id.is_some_and(|uid| review.user_id() != uid) {
            return Ok(ResolveStart::NotFound);
        }
        if review.status() != ReviewStatus::Pending {
            return Ok(ResolveStart::AlreadyResolved(review));
        }
        Ok(ResolveStart::Pending(review))
    }

    /// Shared resolve tail: set the terminal status, stamp reviewer attribution,
    /// and persist. Call after the type-specific approve/reject effect has run.
    pub(crate) fn finish_resolve_review<R: ReviewRecord>(
        &self,
        review: &mut R,
        approve: bool,
        reviewer: Option<String>,
        reviewer_note: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        review.set_status(if approve {
            ReviewStatus::Approved
        } else {
            ReviewStatus::Rejected
        });
        review.stamp_review(reviewer, reviewer_note, now);
        self.store_review(review)
    }
}

impl ReviewRecord for RacReviewRecord {
    const KEY_PREFIX: &'static str = "rac_review:";

    fn review_id(&self) -> &str {
        &self.review_id
    }
    fn status(&self) -> ReviewStatus {
        self.status
    }
    fn user_id(&self) -> &str {
        &self.user_id
    }
    fn org_id(&self) -> Option<&str> {
        self.org_id.as_deref()
    }
    fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    fn set_status(&mut self, status: ReviewStatus) {
        self.status = status;
    }
    fn stamp_review(
        &mut self,
        reviewer: Option<String>,
        reviewer_note: Option<String>,
        updated_at: DateTime<Utc>,
    ) {
        self.reviewer = reviewer;
        self.reviewer_note = reviewer_note;
        self.updated_at = updated_at;
    }
    fn list_tiebreak(&self, other: &Self) -> Ordering {
        // Preserve the prior ordering: source then target unit id, descending.
        other
            .source_unit_id
            .cmp(&self.source_unit_id)
            .then_with(|| other.target_unit_id.cmp(&self.target_unit_id))
    }
}

impl ReviewRecord for ProfileReviewRecord {
    const KEY_PREFIX: &'static str = "profile_review:";

    fn review_id(&self) -> &str {
        &self.review_id
    }
    fn status(&self) -> ReviewStatus {
        self.status
    }
    fn user_id(&self) -> &str {
        &self.user_id
    }
    fn org_id(&self) -> Option<&str> {
        self.org_id.as_deref()
    }
    fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    fn set_status(&mut self, status: ReviewStatus) {
        self.status = status;
    }
    fn stamp_review(
        &mut self,
        reviewer: Option<String>,
        reviewer_note: Option<String>,
        updated_at: DateTime<Utc>,
    ) {
        self.reviewer = reviewer;
        self.reviewer_note = reviewer_note;
        self.updated_at = updated_at;
    }
}
