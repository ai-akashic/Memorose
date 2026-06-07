use super::review_queue::{ResolveStart, ReviewStatus};
use super::types::{
    ProfileAuditEntry, ProfilePatchOutcome, ProfilePromotionOutcome, ProfileReviewRecord,
    ProfileSlotPatch,
};
use super::MemoroseEngine;
use crate::fact_extraction::{project_profile_fact, ProfileFactProjection};
use anyhow::Result;
use chrono::{DateTime, Utc};
use memorose_common::{
    ProfileSlot, ProfileSlotState, ProfileSlotValue, ProfileValueStatus, StoredMemoryFact,
    PROFILE_VALUE_MAX_SOURCES,
};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Facts at or above this aggregated confidence are promoted automatically.
pub(crate) const PROFILE_PROMOTE_MIN_CONFIDENCE: f32 = 0.55;
/// Facts in `[PROFILE_REVIEW_MIN_CONFIDENCE, PROFILE_PROMOTE_MIN_CONFIDENCE)`
/// are queued for manual review rather than applied directly.
pub(crate) const PROFILE_REVIEW_MIN_CONFIDENCE: f32 = 0.30;
/// Ceiling for confidence derived purely from automatic reaffirmation. Repetition
/// can raise confidence toward — but never to — this bound; `1.0` (absolute
/// certainty) is reserved for explicit human edits via the patch API.
const PROFILE_REAFFIRM_CEILING: f32 = 0.95;
/// Fraction of the remaining gap to the ceiling that each reaffirmation closes.
/// Diminishing returns: a fact does not ratchet to high confidence by sheer
/// repetition — each restatement adds progressively less.
const PROFILE_REAFFIRM_GAIN: f32 = 0.25;

impl MemoroseEngine {
    fn profile_slot_storage_key(user_id: &str, slot_key: &str) -> String {
        format!("u:{user_id}:profile:{slot_key}")
    }

    fn profile_slot_prefix(user_id: &str) -> String {
        format!("u:{user_id}:profile:")
    }

    fn profile_audit_key(user_id: &str, slot_key: &str, ts_micros: i64, nonce: &str) -> String {
        format!("u:{user_id}:profile_audit:{slot_key}:{ts_micros:020}:{nonce}")
    }

    fn profile_audit_prefix(user_id: &str, slot_key: Option<&str>) -> String {
        match slot_key {
            Some(slot_key) => format!("u:{user_id}:profile_audit:{slot_key}:"),
            None => format!("u:{user_id}:profile_audit:"),
        }
    }

    // -- reads -------------------------------------------------------------

    /// Aggregate active profile values across every user in `org_id` on this
    /// shard. Users partition across shards, so per-shard aggregates merge by
    /// summing. Read-only analytics — enumerates users via the `active_user:` set.
    pub fn aggregate_org_profile(
        &self,
        org_id: &str,
    ) -> Result<memorose_common::OrgProfileAggregate> {
        let mut users_with_match: HashSet<String> = HashSet::new();
        // attribute -> canonical_value -> (display value, distinct users)
        let mut agg: HashMap<String, HashMap<String, (String, HashSet<String>)>> = HashMap::new();

        for (key, _) in self.system_kv().scan(b"active_user:")? {
            let Some(raw) = key.strip_prefix(b"active_user:") else {
                continue;
            };
            let user_id = String::from_utf8_lossy(raw).to_string();
            for slot in self.list_profile_slots(&user_id)? {
                if slot.org_id.as_deref() != Some(org_id) {
                    continue;
                }
                let mut contributed = false;
                for value in slot.active_values() {
                    contributed = true;
                    let entry = agg
                        .entry(slot.attribute.clone())
                        .or_default()
                        .entry(value.canonical_value.clone())
                        .or_insert_with(|| (value.value.clone(), HashSet::new()));
                    entry.1.insert(user_id.clone());
                }
                if contributed {
                    users_with_match.insert(user_id.clone());
                }
            }
        }

        let mut attributes: Vec<memorose_common::OrgProfileAttribute> = agg
            .into_iter()
            .map(|(attribute, values)| {
                let mut values: Vec<memorose_common::OrgProfileValueCount> = values
                    .into_iter()
                    .map(|(canonical_value, (value, users))| {
                        memorose_common::OrgProfileValueCount {
                            canonical_value,
                            value,
                            user_count: users.len(),
                        }
                    })
                    .collect();
                values.sort_by(|a, b| {
                    b.user_count
                        .cmp(&a.user_count)
                        .then_with(|| a.canonical_value.cmp(&b.canonical_value))
                });
                memorose_common::OrgProfileAttribute { attribute, values }
            })
            .collect();
        attributes.sort_by(|a, b| a.attribute.cmp(&b.attribute));

        Ok(memorose_common::OrgProfileAggregate {
            org_id: org_id.to_string(),
            user_count: users_with_match.len(),
            attributes,
        })
    }

    pub fn get_profile_slot(&self, user_id: &str, slot_key: &str) -> Result<Option<ProfileSlot>> {
        let key = Self::profile_slot_storage_key(user_id, slot_key);
        Ok(self
            .kv()
            .get(key.as_bytes())?
            .and_then(|bytes| serde_json::from_slice::<ProfileSlot>(&bytes).ok()))
    }

    pub fn list_profile_slots(&self, user_id: &str) -> Result<Vec<ProfileSlot>> {
        let prefix = Self::profile_slot_prefix(user_id);
        let mut slots = self
            .kv()
            .scan(prefix.as_bytes())?
            .into_iter()
            .filter_map(|(_, value)| serde_json::from_slice::<ProfileSlot>(&value).ok())
            .collect::<Vec<_>>();
        slots.sort_by(|a, b| a.slot_key.cmp(&b.slot_key));
        Ok(slots)
    }

    pub fn list_profile_audit(
        &self,
        user_id: &str,
        slot_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ProfileAuditEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = Self::profile_audit_prefix(user_id, slot_key);
        let mut entries = self
            .kv()
            .scan(prefix.as_bytes())?
            .into_iter()
            .filter_map(|(_, value)| serde_json::from_slice::<ProfileAuditEntry>(&value).ok())
            .collect::<Vec<_>>();
        // Newest first.
        entries.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        entries.truncate(limit);
        Ok(entries)
    }

    // -- writes ------------------------------------------------------------

    fn put_profile_slot(&self, slot: &ProfileSlot) -> Result<()> {
        let key = Self::profile_slot_storage_key(&slot.user_id, &slot.slot_key);
        self.kv().put(key.as_bytes(), &serde_json::to_vec(slot)?)
    }

    fn append_profile_audit(&self, entry: &ProfileAuditEntry) -> Result<()> {
        let ts_micros = entry.created_at.timestamp_micros();
        let nonce = Uuid::new_v4().simple().to_string();
        let key = Self::profile_audit_key(&entry.user_id, &entry.slot_key, ts_micros, &nonce);
        self.kv().put(key.as_bytes(), &serde_json::to_vec(entry)?)
    }

    /// Reconcile the user's profile when one of their memory units is forgotten,
    /// so derived profile values never outlive their source memory.
    ///
    /// Removes `unit_id` from every profile value's provenance. When a value
    /// loses its last source: a `hard` delete (right-to-be-forgotten) erases the
    /// value outright — and an emptied slot is dropped entirely — while a logical
    /// forget only obsoletes it (retained, non-destructive). RAC's routine
    /// tombstoning does NOT call this; the profile merge already reflects those.
    pub fn reconcile_profile_after_forget(
        &self,
        user_id: &str,
        unit_id: Uuid,
        hard: bool,
    ) -> Result<()> {
        let now = Utc::now();
        for mut slot in self.list_profile_slots(user_id)? {
            // Hard (right-to-be-forgotten) overrides a pin; a logical forget must
            // not obsolete a human-pinned value.
            let pinned = matches!(slot.state, ProfileSlotState::ManuallyPinned);
            let mut affected: Vec<String> = Vec::new();
            let mut erase: Vec<String> = Vec::new();
            for value in slot.values.iter_mut() {
                let Some(pos) = value.source_unit_ids.iter().position(|id| *id == unit_id) else {
                    continue;
                };
                value.source_unit_ids.remove(pos);
                affected.push(value.canonical_value.clone());
                // Only act when THIS value just lost its last supporting memory.
                if value.source_unit_ids.is_empty() {
                    if hard {
                        erase.push(value.canonical_value.clone());
                    } else if value.is_active() && !pinned {
                        value.status = ProfileValueStatus::Obsoleted;
                        value.last_seen_at = now;
                    }
                }
            }
            if affected.is_empty() {
                continue;
            }
            if !erase.is_empty() {
                // Erase only values this forget actually orphaned — never unrelated
                // values that happened to have no sources (e.g. manual edits).
                slot.values
                    .retain(|value| !erase.contains(&value.canonical_value));
            }
            self.append_profile_audit(&ProfileAuditEntry {
                created_at: now,
                user_id: user_id.to_string(),
                slot_key: slot.slot_key.clone(),
                action: if hard {
                    "forget_hard_cascade".to_string()
                } else {
                    "forget_logical_cascade".to_string()
                },
                canonical_value: affected.join(","),
                confidence: 0.0,
                change_type: "forget".to_string(),
                source_unit_id: Some(unit_id),
                reason: None,
            })?;
            if hard && slot.values.is_empty() {
                self.kv()
                    .delete(Self::profile_slot_storage_key(user_id, &slot.slot_key).as_bytes())?;
            } else {
                slot.updated_at = now;
                slot.version = slot.version.saturating_add(1);
                slot.sort_values();
                self.put_profile_slot(&slot)?;
            }
        }
        Ok(())
    }

    /// Promote a single L1 fact into its profile slot. Thin convenience wrapper
    /// over [`Self::upsert_profile_facts`]; used by tests (production promotion
    /// goes through the batch method).
    #[cfg(test)]
    pub(crate) fn upsert_profile_slot_from_fact(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        fact: &StoredMemoryFact,
        source_unit_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<ProfilePromotionOutcome> {
        Ok(self
            .upsert_profile_facts(
                user_id,
                org_id,
                std::slice::from_ref(fact),
                source_unit_id,
                now,
            )?
            .into_iter()
            .next()
            .unwrap_or(ProfilePromotionOutcome::Skipped))
    }

    /// Promote all facts extracted from one memory unit into their profile slots.
    /// Facts that target the same `(subject, attribute)` slot are read, merged, and
    /// written exactly once — a unit like "I like tea and coffee" touches the
    /// `preference` slot a single time instead of once per fact. Synchronous;
    /// intended to be called from the consolidation worker after RAC reconciliation.
    pub(crate) fn upsert_profile_facts(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        facts: &[StoredMemoryFact],
        source_unit_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Vec<ProfilePromotionOutcome>> {
        let mut cache: HashMap<String, ProfileSlot> = HashMap::new();
        let mut dirty: HashSet<String> = HashSet::new();
        let mut outcomes = Vec::with_capacity(facts.len());

        for fact in facts {
            let Some(projection) = project_profile_fact(fact) else {
                outcomes.push(ProfilePromotionOutcome::Skipped);
                continue;
            };
            if !projection.is_user_or_agent_subject
                || projection.confidence < PROFILE_REVIEW_MIN_CONFIDENCE
            {
                outcomes.push(ProfilePromotionOutcome::Skipped);
                continue;
            }

            // Load each slot from storage at most once per call.
            if !cache.contains_key(&projection.slot_key) {
                let slot = self
                    .get_profile_slot(user_id, &projection.slot_key)?
                    .unwrap_or_else(|| ProfileSlot {
                        slot_key: projection.slot_key.clone(),
                        user_id: user_id.to_string(),
                        org_id: org_id.map(str::to_string),
                        subject: projection.subject_label.clone(),
                        subject_ref: Some(projection.subject_key.clone()),
                        subject_name: None,
                        attribute: projection.attribute_label.clone(),
                        state: ProfileSlotState::Stable,
                        values: Vec::new(),
                        created_at: now,
                        updated_at: now,
                        version: 0,
                    });
                cache.insert(projection.slot_key.clone(), slot);
            }
            let slot = cache
                .get_mut(&projection.slot_key)
                .expect("slot just inserted");

            // Low-confidence promotions and conflicts with a pinned slot go to
            // review. A positive re-mention of an already-Active value is a
            // harmless reaffirmation — never route it to review.
            let pinned = matches!(slot.state, ProfileSlotState::ManuallyPinned);
            let positive_reaffirmation = !projection.is_negative
                && !projection.is_historical
                && slot
                    .values
                    .iter()
                    .any(|v| v.canonical_value == projection.canonical_value && v.is_active());
            let needs_review = !positive_reaffirmation
                && (projection.confidence < PROFILE_PROMOTE_MIN_CONFIDENCE
                    || (pinned && Self::projection_would_obsolete_siblings(&projection)));

            if needs_review {
                self.queue_profile_review(user_id, org_id, &projection, source_unit_id, now)?;
                if !pinned {
                    slot.state = ProfileSlotState::PendingReview;
                }
                slot.updated_at = now;
                dirty.insert(projection.slot_key.clone());
                outcomes.push(ProfilePromotionOutcome::QueuedForReview);
                continue;
            }

            let outcome = self.apply_projection_to_slot(slot, &projection, source_unit_id, now);
            slot.updated_at = now;
            slot.version = slot.version.saturating_add(1);
            dirty.insert(projection.slot_key.clone());
            self.append_profile_audit(&ProfileAuditEntry {
                created_at: now,
                user_id: user_id.to_string(),
                slot_key: projection.slot_key.clone(),
                action: match outcome {
                    ProfilePromotionOutcome::Merged => "merge".to_string(),
                    _ => "promote".to_string(),
                },
                canonical_value: projection.canonical_value.clone(),
                confidence: projection.confidence,
                change_type: projection.change_type_label.clone(),
                source_unit_id: Some(source_unit_id),
                reason: None,
            })?;
            outcomes.push(outcome);
        }

        // Flush each touched slot exactly once.
        for slot_key in &dirty {
            if let Some(slot) = cache.get_mut(slot_key) {
                slot.sort_values();
                self.put_profile_slot(slot)?;
            }
        }
        Ok(outcomes)
    }

    /// Whether a projected fact would, if applied, demote existing active siblings.
    /// Only single-value attributes collapse, and only when the change type
    /// licenses obsoletion (Update/Negation/Historical) or contradiction.
    fn projection_would_obsolete_siblings(projection: &ProfileFactProjection) -> bool {
        projection.is_single_value_attribute
            && (projection.supports_obsolete || projection.supports_contradiction)
    }

    /// Core merge logic. Mutates `slot.values` in place and returns whether a
    /// value was reaffirmed (`Merged`) or newly added (`Promoted`).
    fn apply_projection_to_slot(
        &self,
        slot: &mut ProfileSlot,
        projection: &ProfileFactProjection,
        source_unit_id: Uuid,
        now: DateTime<Utc>,
    ) -> ProfilePromotionOutcome {
        let cv = projection.canonical_value.clone();

        // Negative assertion ("I no longer like coffee", "I don't live in Berlin"):
        // deny the matching value in place regardless of slot cardinality. Covers
        // both the Negation change type and a negative-polarity Contradiction.
        if projection.is_negative {
            if let Some(value) = slot.values.iter_mut().find(|v| v.canonical_value == cv) {
                value.status = ProfileValueStatus::Negated;
                value.last_change_type = projection.change_type_label.clone();
                value.last_seen_at = now;
                Self::push_source(value, source_unit_id);
                return ProfilePromotionOutcome::Merged;
            }
            // The denied value was never recorded. Store it as Negated for
            // traceability — a denial must never materialize an Active value.
            slot.values.push(Self::new_value(
                projection,
                source_unit_id,
                now,
                ProfileValueStatus::Negated,
            ));
            return ProfilePromotionOutcome::Promoted;
        }

        // Historical ("I used to ..."): a bare historical mention of the value we
        // currently believe is Active is ambiguous (often just reminiscing), so we
        // do NOT downgrade it — that would leave the slot with no current value.
        // A value that is already inactive is (re)marked Historical; an unseen
        // value is recorded as Historical. A genuine move is carried by an Update
        // to a *new* value, which obsoletes the old one through the normal path.
        if projection.is_historical {
            if let Some(value) = slot.values.iter_mut().find(|v| v.canonical_value == cv) {
                if !value.is_active() {
                    value.status = ProfileValueStatus::Historical;
                }
                value.last_change_type = projection.change_type_label.clone();
                value.last_seen_at = now;
                Self::push_source(value, source_unit_id);
                return ProfilePromotionOutcome::Merged;
            }
            slot.values.push(Self::new_value(
                projection,
                source_unit_id,
                now,
                ProfileValueStatus::Historical,
            ));
            return ProfilePromotionOutcome::Promoted;
        }

        // Contradiction with obsolete-eligible change: collapse incompatible
        // siblings for single-value attributes; otherwise leave siblings alone.
        let obsolete_siblings = Self::projection_would_obsolete_siblings(projection);

        if let Some(value) = slot.values.iter_mut().find(|v| v.canonical_value == cv) {
            // Reaffirm / update an existing value in place. Confidence rises with
            // diminishing returns toward a sub-certain ceiling so repetition alone
            // cannot manufacture certainty.
            value.support_count = value.support_count.saturating_add(1);
            let base = value.confidence.max(projection.confidence);
            // Close part of the gap to the ceiling; the gap term is 0 once base is
            // already at/above the ceiling, so a manually-set or already-high
            // confidence is preserved, never reduced.
            value.confidence =
                base + (PROFILE_REAFFIRM_CEILING - base).max(0.0) * PROFILE_REAFFIRM_GAIN;
            value.status = ProfileValueStatus::Active;
            value.last_change_type = projection.change_type_label.clone();
            value.last_seen_at = now;
            Self::push_source(value, source_unit_id);
            if obsolete_siblings {
                Self::obsolete_other_active(slot, &cv, now);
            }
            return ProfilePromotionOutcome::Merged;
        }

        // New value.
        if obsolete_siblings {
            Self::obsolete_other_active(slot, &cv, now);
        }
        slot.values.push(Self::new_value(
            projection,
            source_unit_id,
            now,
            ProfileValueStatus::Active,
        ));
        ProfilePromotionOutcome::Promoted
    }

    fn new_value(
        projection: &ProfileFactProjection,
        source_unit_id: Uuid,
        now: DateTime<Utc>,
        status: ProfileValueStatus,
    ) -> ProfileSlotValue {
        ProfileSlotValue {
            value: projection.display_value.clone(),
            canonical_value: projection.canonical_value.clone(),
            confidence: projection.confidence,
            support_count: 1,
            status,
            last_change_type: projection.change_type_label.clone(),
            source_unit_ids: vec![source_unit_id],
            first_seen_at: now,
            last_seen_at: now,
        }
    }

    fn push_source(value: &mut ProfileSlotValue, source_unit_id: Uuid) {
        if value.source_unit_ids.contains(&source_unit_id) {
            return;
        }
        value.source_unit_ids.push(source_unit_id);
        if value.source_unit_ids.len() > PROFILE_VALUE_MAX_SOURCES {
            let overflow = value.source_unit_ids.len() - PROFILE_VALUE_MAX_SOURCES;
            value.source_unit_ids.drain(0..overflow);
        }
    }

    fn obsolete_other_active(slot: &mut ProfileSlot, keep_canonical: &str, now: DateTime<Utc>) {
        for value in slot.values.iter_mut() {
            if value.canonical_value != keep_canonical && value.is_active() {
                value.status = ProfileValueStatus::Obsoleted;
                value.last_seen_at = now;
            }
        }
    }

    // -- review queue ------------------------------------------------------

    fn queue_profile_review(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        projection: &ProfileFactProjection,
        source_unit_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<ProfileReviewRecord> {
        let review = ProfileReviewRecord {
            review_id: Uuid::new_v4().to_string(),
            created_at: now,
            updated_at: now,
            user_id: user_id.to_string(),
            org_id: org_id.map(str::to_string),
            slot_key: projection.slot_key.clone(),
            attribute: projection.attribute_label.clone(),
            subject: projection.subject_label.clone(),
            subject_ref: Some(projection.subject_key.clone()),
            proposed_value: projection.display_value.clone(),
            proposed_canonical_value: projection.canonical_value.clone(),
            proposed_confidence: projection.confidence,
            change_type: projection.change_type_label.clone(),
            source_unit_id,
            reason: format!(
                "auto-promotion withheld (confidence {:.2})",
                projection.confidence
            ),
            status: ReviewStatus::Pending,
            reviewer: None,
            reviewer_note: None,
        };
        self.store_profile_review(&review)?;
        Ok(review)
    }

    pub(crate) fn store_profile_review(&self, review: &ProfileReviewRecord) -> Result<()> {
        self.store_review(review)
    }

    pub fn get_profile_review(&self, review_id: &str) -> Result<Option<ProfileReviewRecord>> {
        self.get_review::<ProfileReviewRecord>(review_id)
    }

    pub fn list_profile_reviews(
        &self,
        status_filter: Option<ReviewStatus>,
        user_id_filter: Option<&str>,
        org_id_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ProfileReviewRecord>> {
        self.list_reviews::<ProfileReviewRecord>(
            status_filter,
            user_id_filter,
            org_id_filter,
            limit,
        )
    }

    /// Approve or reject a pending profile review. Approval re-runs the merge
    /// with the proposed value as if it had cleared the auto-promote threshold.
    /// `user_id` scopes the operation: a review owned by a different user (even on
    /// the same shard) is treated as not found, preventing cross-tenant writes.
    pub async fn resolve_profile_review(
        &self,
        user_id: &str,
        review_id: &str,
        approve: bool,
        reviewer: Option<String>,
        reviewer_note: Option<String>,
    ) -> Result<Option<ProfileReviewRecord>> {
        let mut review =
            match self.begin_resolve_review::<ProfileReviewRecord>(review_id, Some(user_id))? {
                ResolveStart::NotFound => return Ok(None),
                ResolveStart::AlreadyResolved(review) => return Ok(Some(review)),
                ResolveStart::Pending(review) => review,
            };
        let now = Utc::now();

        if approve {
            // Reconstruct a stored fact from the proposed value and re-run merge
            // with a confidence floored at the auto-promote threshold.
            let fact = StoredMemoryFact {
                subject: review.subject.clone(),
                subject_ref: review.subject_ref.clone(),
                subject_name: None,
                attribute: review.attribute.clone(),
                value: review.proposed_value.clone(),
                canonical_value: Some(review.proposed_canonical_value.clone()),
                change_type: review.change_type.clone(),
                temporal_status: None,
                polarity: None,
                evidence_span: None,
                confidence: review
                    .proposed_confidence
                    .max(PROFILE_PROMOTE_MIN_CONFIDENCE),
            };
            if let Some(projection) = project_profile_fact(&fact) {
                let mut slot = self
                    .get_profile_slot(&review.user_id, &review.slot_key)?
                    .unwrap_or_else(|| ProfileSlot {
                        slot_key: review.slot_key.clone(),
                        user_id: review.user_id.clone(),
                        org_id: review.org_id.clone(),
                        subject: review.subject.clone(),
                        subject_ref: Some(projection.subject_key.clone()),
                        subject_name: None,
                        attribute: review.attribute.clone(),
                        state: ProfileSlotState::Stable,
                        values: Vec::new(),
                        created_at: now,
                        updated_at: now,
                        version: 0,
                    });
                self.apply_projection_to_slot(&mut slot, &projection, review.source_unit_id, now);
                if matches!(slot.state, ProfileSlotState::PendingReview) {
                    slot.state = ProfileSlotState::Stable;
                }
                slot.updated_at = now;
                slot.version = slot.version.saturating_add(1);
                slot.sort_values();
                self.put_profile_slot(&slot)?;
                self.append_profile_audit(&ProfileAuditEntry {
                    created_at: now,
                    user_id: review.user_id.clone(),
                    slot_key: review.slot_key.clone(),
                    action: "review_approve".to_string(),
                    canonical_value: review.proposed_canonical_value.clone(),
                    confidence: fact.confidence,
                    change_type: review.change_type.clone(),
                    source_unit_id: Some(review.source_unit_id),
                    reason: reviewer_note.clone(),
                })?;
            }
        } else {
            // Clear the slot's pending flag only if no other review for this slot
            // is still outstanding, so a sibling pending review keeps it flagged.
            let others_pending =
                self.any_review_matches::<ProfileReviewRecord>(Some(&review.user_id), |other| {
                    other.status == ReviewStatus::Pending
                        && other.review_id != review.review_id
                        && other.slot_key == review.slot_key
                })?;
            if !others_pending {
                if let Some(mut slot) = self.get_profile_slot(&review.user_id, &review.slot_key)? {
                    if matches!(slot.state, ProfileSlotState::PendingReview) {
                        slot.state = ProfileSlotState::Stable;
                        slot.updated_at = now;
                        self.put_profile_slot(&slot)?;
                    }
                }
            }
        }

        self.finish_resolve_review(&mut review, approve, reviewer, reviewer_note, now)?;
        Ok(Some(review))
    }

    // -- manual patch ------------------------------------------------------

    /// Apply a patch op to a slot in memory (no I/O). Returns the audit metadata
    /// on success, or the missing canonical value as `Err` for `SetConfidence`.
    /// Shared by `patch_profile_slot` (execute) and `preview_profile_patch`.
    fn apply_patch_to_slot(
        slot: &mut ProfileSlot,
        op: &ProfileSlotPatch,
        now: DateTime<Utc>,
    ) -> std::result::Result<PatchMeta, String> {
        let meta = match op {
            ProfileSlotPatch::SetActiveValue { canonical_value } => {
                let audit_conf = if let Some(value) = slot
                    .values
                    .iter_mut()
                    .find(|v| &v.canonical_value == canonical_value)
                {
                    value.status = ProfileValueStatus::Active;
                    value.last_seen_at = now;
                    value.confidence
                } else {
                    slot.values.push(ProfileSlotValue {
                        value: canonical_value.clone(),
                        canonical_value: canonical_value.clone(),
                        confidence: 1.0,
                        support_count: 1,
                        status: ProfileValueStatus::Active,
                        last_change_type: "manual".to_string(),
                        source_unit_ids: Vec::new(),
                        first_seen_at: now,
                        last_seen_at: now,
                    });
                    1.0
                };
                Self::obsolete_other_active(slot, canonical_value, now);
                PatchMeta {
                    audit_action: "patch_set_active",
                    audit_cv: canonical_value.clone(),
                    audit_conf,
                }
            }
            ProfileSlotPatch::ObsoleteValue { canonical_value } => {
                if let Some(value) = slot
                    .values
                    .iter_mut()
                    .find(|v| &v.canonical_value == canonical_value)
                {
                    value.status = ProfileValueStatus::Obsoleted;
                    value.last_seen_at = now;
                }
                PatchMeta {
                    audit_action: "patch_obsolete",
                    audit_cv: canonical_value.clone(),
                    audit_conf: 0.0,
                }
            }
            ProfileSlotPatch::RemoveValue { canonical_value } => {
                slot.values
                    .retain(|v| &v.canonical_value != canonical_value);
                PatchMeta {
                    audit_action: "patch_remove",
                    audit_cv: canonical_value.clone(),
                    audit_conf: 0.0,
                }
            }
            ProfileSlotPatch::Pin => {
                slot.state = ProfileSlotState::ManuallyPinned;
                PatchMeta {
                    audit_action: "patch_pin",
                    audit_cv: String::new(),
                    audit_conf: 0.0,
                }
            }
            ProfileSlotPatch::Unpin => {
                slot.state = ProfileSlotState::Stable;
                PatchMeta {
                    audit_action: "patch_unpin",
                    audit_cv: String::new(),
                    audit_conf: 0.0,
                }
            }
            ProfileSlotPatch::SetConfidence {
                canonical_value,
                confidence,
            } => {
                let clamped = confidence.clamp(0.0, 1.0);
                if let Some(value) = slot
                    .values
                    .iter_mut()
                    .find(|v| &v.canonical_value == canonical_value)
                {
                    value.confidence = clamped;
                    value.last_seen_at = now;
                } else {
                    return Err(canonical_value.clone());
                }
                PatchMeta {
                    audit_action: "patch_confidence",
                    audit_cv: canonical_value.clone(),
                    audit_conf: clamped,
                }
            }
        };
        Ok(meta)
    }

    /// Apply a manual edit to a profile slot. Returns a [`ProfilePatchOutcome`]
    /// distinguishing success, a missing slot, and a missing value; `Err` is
    /// reserved for genuine storage failures.
    pub async fn patch_profile_slot(
        &self,
        user_id: &str,
        slot_key: &str,
        op: ProfileSlotPatch,
    ) -> Result<ProfilePatchOutcome> {
        let Some(mut slot) = self.get_profile_slot(user_id, slot_key)? else {
            return Ok(ProfilePatchOutcome::SlotNotFound);
        };
        let now = Utc::now();
        let meta = match Self::apply_patch_to_slot(&mut slot, &op, now) {
            Ok(meta) => meta,
            Err(canonical_value) => {
                return Ok(ProfilePatchOutcome::ValueNotFound { canonical_value })
            }
        };

        slot.updated_at = now;
        slot.version = slot.version.saturating_add(1);
        slot.sort_values();
        self.put_profile_slot(&slot)?;
        self.append_profile_audit(&ProfileAuditEntry {
            created_at: now,
            user_id: user_id.to_string(),
            slot_key: slot_key.to_string(),
            action: meta.audit_action.to_string(),
            canonical_value: meta.audit_cv,
            confidence: meta.audit_conf,
            change_type: "manual".to_string(),
            source_unit_id: None,
            reason: None,
        })?;
        Ok(ProfilePatchOutcome::Applied(Box::new(slot)))
    }

    /// Compute what [`Self::patch_profile_slot`] would produce WITHOUT persisting
    /// or auditing — the "preview" half of preview→execute. `Applied` carries the
    /// would-be slot.
    pub async fn preview_profile_patch(
        &self,
        user_id: &str,
        slot_key: &str,
        op: ProfileSlotPatch,
    ) -> Result<ProfilePatchOutcome> {
        let Some(mut slot) = self.get_profile_slot(user_id, slot_key)? else {
            return Ok(ProfilePatchOutcome::SlotNotFound);
        };
        let now = Utc::now();
        if let Err(canonical_value) = Self::apply_patch_to_slot(&mut slot, &op, now) {
            return Ok(ProfilePatchOutcome::ValueNotFound { canonical_value });
        }
        slot.sort_values();
        Ok(ProfilePatchOutcome::Applied(Box::new(slot)))
    }
}

struct PatchMeta {
    audit_action: &'static str,
    audit_cv: String,
    audit_conf: f32,
}
