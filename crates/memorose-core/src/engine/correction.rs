use super::helpers::{
    cosine_similarity, OBSOLETE_ACTION_MIN_CONFIDENCE, OBSOLETE_ACTION_RELATION_ONLY_MIN_CONFIDENCE,
};
use super::types::*;
use crate::arbitrator::{ExtractedMemoryFact, MemoryCorrectionAction, MemoryCorrectionKind};
use crate::fact_extraction::{self, MemoryFactChangeType, MemoryFactDescriptor};
use crate::storage::index::TextIndexMetricSnapshot;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Timelike, Utc};
use memorose_common::{ForgettingTombstone, GraphEdge, MemoryType, MemoryUnit, RelationType};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

impl super::MemoroseEngine {
    pub(crate) fn detect_memory_fact(unit: &MemoryUnit) -> Option<MemoryFactDescriptor> {
        fact_extraction::detect_memory_fact(unit)
    }

    pub(crate) fn fact_change_supports_obsolete(change_type: MemoryFactChangeType) -> bool {
        fact_extraction::fact_change_supports_obsolete(change_type)
    }

    pub(crate) fn fact_change_supports_contradiction(change_type: MemoryFactChangeType) -> bool {
        fact_extraction::fact_change_supports_contradiction(change_type)
    }

    pub(crate) fn build_memory_correction_focus_terms_with_fact(
        unit: &MemoryUnit,
        fact: Option<&MemoryFactDescriptor>,
    ) -> Vec<String> {
        fact_extraction::build_memory_correction_focus_terms_with_fact(unit, fact)
    }

    pub(crate) fn keyword_overlap_score(
        query_text: &str,
        content: &str,
        keywords: &[String],
    ) -> f32 {
        fact_extraction::keyword_overlap_score(query_text, content, keywords)
    }

    pub(crate) fn memory_correction_candidate_score(
        unit: &MemoryUnit,
        candidate: &MemoryUnit,
        focus_terms: &[String],
        query_fact: Option<&MemoryFactDescriptor>,
    ) -> f32 {
        fact_extraction::memory_correction_candidate_score(unit, candidate, focus_terms, query_fact)
    }

    pub(crate) fn subject_keys_compatible(left: &str, right: &str) -> bool {
        fact_extraction::subject_keys_compatible(left, right)
    }

    pub(crate) fn descriptor_from_extracted_fact(
        fact: ExtractedMemoryFact,
    ) -> Option<MemoryFactDescriptor> {
        fact_extraction::descriptor_from_extracted_fact(fact)
    }

    pub(crate) fn push_unique_memory_terms(
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

    pub(crate) fn memory_fact_descriptor_key(descriptor: &MemoryFactDescriptor) -> String {
        format!(
            "{:?}|{}|{:?}|{}|{:?}",
            descriptor.subject,
            descriptor.subject_key,
            descriptor.attribute,
            descriptor.value_payload.comparison_key(),
            descriptor.change_type
        )
    }

    pub(crate) fn compatible_fact_pair_score(
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

    pub(crate) fn build_memory_correction_focus_terms(
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

    pub(crate) fn build_memory_correction_search_queries(
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

    pub(crate) fn memory_correction_candidate_score_for_facts(
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

    pub(crate) async fn resolve_memory_fact_descriptors(
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

    pub(crate) async fn resolve_fact_descriptors_compatible(
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

    pub(crate) fn organization_similarity_score(
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

    pub(crate) fn organization_metric_counter_key(org_id: &str, metric: &str) -> String {
        format!("organization_metric:{}:{}", org_id, metric)
    }

    pub(crate) fn rac_metric_counter_key(metric: &str) -> String {
        format!("rac_metric:{}", metric)
    }

    pub(crate) fn rac_metric_bucket_counter_key(metric: &str, bucket_start: &str) -> String {
        format!("rac_metric_bucket:{}:{}", bucket_start, metric)
    }

    pub(crate) fn rac_decision_key(
        created_at: DateTime<Utc>,
        source_unit_id: Uuid,
        nonce: Uuid,
    ) -> String {
        format!(
            "rac_decision:{:020}:{}:{}",
            created_at.timestamp_micros(),
            source_unit_id,
            nonce
        )
    }

    pub(crate) fn rac_review_key(review_id: &str) -> String {
        format!("rac_review:{}", review_id)
    }

    pub(crate) fn rac_metric_bucket_start(now: DateTime<Utc>) -> DateTime<Utc> {
        now.with_minute(0)
            .and_then(|dt| dt.with_second(0))
            .and_then(|dt| dt.with_nanosecond(0))
            .unwrap_or(now)
    }

    pub(crate) async fn materialize_organization_read_view_for_record(
        &self,
        record: &OrganizationKnowledgeRecord,
    ) -> Result<MemoryUnit> {
        Ok(Self::materialize_organization_read_view(record))
    }

    pub(crate) fn increment_organization_metric_counter(
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

    pub(crate) fn get_organization_metric_counter(
        &self,
        org_id: &str,
        metric: &str,
    ) -> Result<usize> {
        let key = Self::organization_metric_counter_key(org_id, metric);
        Ok(self
            .system_kv()
            .get(key.as_bytes())?
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])) as usize)
            .unwrap_or(0))
    }

    pub(crate) fn increment_rac_metric_counter(&self, metric: &str, delta: usize) -> Result<()> {
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

    pub(crate) fn get_rac_metric_counter(&self, metric: &str) -> Result<usize> {
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

    pub(crate) fn should_enqueue_rac_review(record: &RacDecisionRecord) -> bool {
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

    pub(crate) fn organization_contribution_sort_key(
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

    pub(crate) async fn fetch_memory_correction_candidates(
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

    pub(crate) async fn apply_memory_correction_actions_with_stage(
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

    pub(crate) async fn apply_memory_correction_actions(
        &self,
        unit: &MemoryUnit,
        actions: Vec<MemoryCorrectionAction>,
    ) -> Result<Vec<Uuid>> {
        self.apply_memory_correction_actions_with_stage(unit, actions, "post_store")
            .await
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

    pub(crate) fn store_rac_review(&self, review: &RacReviewRecord) -> Result<()> {
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
}
