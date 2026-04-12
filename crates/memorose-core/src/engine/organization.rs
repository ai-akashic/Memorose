use anyhow::Result;
use chrono::{DateTime, Utc};
use memorose_common::{
    tokenizer::count_tokens, GraphEdge, MemoryDomain, MemoryUnit,
    RelationType, TimeRange,
};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use super::types::*;
use crate::fact_extraction;

impl super::MemoroseEngine {
    pub(crate) async fn build_organization_knowledge_detail_record_from_snapshot(
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

    pub(crate) async fn load_organization_knowledge_snapshot(
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

    pub(crate) async fn list_organization_knowledge_snapshots(
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

    pub(crate) fn organization_knowledge_key(id: Uuid) -> String {
        format!("organization_knowledge:{}", id)
    }

    pub(crate) fn organization_source_relation_key(source_id: Uuid) -> String {
        format!("organization_knowledge_relation:source:{}", source_id)
    }

    pub(crate) fn organization_topic_relation_key(org_id: &str, topic_key: &str) -> String {
        format!(
            "organization_knowledge_relation:topic:{}:{}",
            org_id, topic_key
        )
    }

    pub(crate) fn organization_knowledge_contribution_key(knowledge_id: Uuid, source_id: Uuid) -> String {
        format!(
            "organization_knowledge_contribution:{}:{}",
            knowledge_id, source_id
        )
    }

    pub(crate) fn organization_knowledge_contribution_prefix(knowledge_id: Uuid) -> String {
        format!("organization_knowledge_contribution:{}:", knowledge_id)
    }

    pub(crate) fn organization_membership_source_key(source_id: Uuid) -> String {
        format!("organization_knowledge_membership:source:{}", source_id)
    }

    pub(crate) fn organization_membership_by_knowledge_prefix(knowledge_id: Uuid) -> String {
        format!(
            "organization_knowledge_membership_by_knowledge:{}:",
            knowledge_id
        )
    }

    pub(crate) fn organization_membership_by_knowledge_key(
        membership: &OrganizationKnowledgeMembershipRecord,
    ) -> String {
        format!(
            "{}{}",
            Self::organization_membership_by_knowledge_prefix(membership.knowledge_id),
            membership.source_id
        )
    }

    pub(crate) fn organization_knowledge_relation_index_prefix(knowledge_id: Uuid) -> String {
        format!(
            "organization_knowledge_relation_by_knowledge:{}:",
            knowledge_id
        )
    }

    pub(crate) fn organization_knowledge_relation_index_key(
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

    pub(crate) fn organization_relation_key(record: &OrganizationKnowledgeRelationRecord) -> String {
        match &record.relation {
            OrganizationKnowledgeRelationKind::Source { source_id } => {
                Self::organization_source_relation_key(*source_id)
            }
            OrganizationKnowledgeRelationKind::TopicAlias { topic_key } => {
                Self::organization_topic_relation_key(&record.org_id, topic_key)
            }
        }
    }

    pub(crate) fn organization_read_view_owner(org_id: &str) -> String {
        format!("__organization__:{}", org_id)
    }

    pub(crate) fn normalize_whitespace(text: &str) -> String {
        text.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    pub(crate) fn normalize_memory_keywords(keywords: &[String], limit: usize) -> Vec<String> {
        fact_extraction::normalize_memory_keywords(keywords, limit)
    }

    pub(crate) fn neutralize_first_person_language(text: &str) -> String {
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

    pub(crate) fn normalize_organization_keywords(source: &MemoryUnit) -> Vec<String> {
        Self::normalize_memory_keywords(&source.keywords, 8)
    }

    pub(crate) fn build_organization_topic_key(label: &str) -> String {
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

    pub(crate) fn fallback_organization_topic_label(text: &str) -> Option<String> {
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

    pub(crate) fn organization_topic_candidates_from_keywords_and_content(
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

    pub(crate) fn organization_source_topic_candidates(source: &MemoryUnit) -> Vec<(String, String)> {
        let keywords = Self::normalize_organization_keywords(source);
        Self::organization_topic_candidates_from_keywords_and_content(&keywords, &source.content)
    }

    pub(crate) fn select_organization_topic_from_candidates(
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

    pub(crate) fn select_organization_topic(sources: &[MemoryUnit]) -> Option<OrganizationProjectionTopic> {
        let candidate_groups = sources
            .iter()
            .map(Self::organization_source_topic_candidates)
            .filter(|group| !group.is_empty())
            .collect::<Vec<_>>();
        Self::select_organization_topic_from_candidates(&candidate_groups)
    }

    pub(crate) fn merge_organization_keywords(primary_label: &str, sources: &[MemoryUnit]) -> Vec<String> {
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

    pub(crate) fn merge_organization_embedding(
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

    pub(crate) fn compose_organization_knowledge_record(
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

    pub(crate) fn materialize_organization_read_view(record: &OrganizationKnowledgeRecord) -> MemoryUnit {
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

    pub(crate) fn organization_memberships_from_contributions(
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

    pub(crate) fn organization_topic_relations(
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

    pub(crate) fn organization_candidate_contribution_records(
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

    pub(crate) fn activate_organization_contribution_records(
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

    pub(crate) fn build_organization_knowledge_content(source: &MemoryUnit, keywords: &[String]) -> String {
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

    pub(crate) async fn run_share_backfill(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        domain: MemoryDomain,
    ) -> Result<usize> {
        let prefix = format!("u:{}:unit:", user_id);
        let store = self.kv_store.clone();
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

    pub(crate) fn should_publish_to_organization(source: &MemoryUnit) -> bool {
        source.domain == MemoryDomain::User
            && source.level == 2
            && !source.content.trim().is_empty()
            && source.content != "LLM not available"
            && source.content != "No memories provided."
    }

    pub(crate) async fn load_organization_source_units(&self, source_ids: &[Uuid]) -> Result<Vec<MemoryUnit>> {
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

    pub(crate) fn load_organization_knowledge(&self, id: Uuid) -> Result<Option<OrganizationKnowledgeRecord>> {
        let key = Self::organization_knowledge_key(id);
        match self.system_kv().get(key.as_bytes())? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            None => Ok(None),
        }
    }

    pub(crate) fn store_organization_knowledge(&self, record: &OrganizationKnowledgeRecord) -> Result<()> {
        let key = Self::organization_knowledge_key(record.id);
        self.system_kv()
            .put(key.as_bytes(), &serde_json::to_vec(record)?)
    }

    pub(crate) fn load_organization_membership(
        &self,
        source_id: Uuid,
    ) -> Result<Option<OrganizationKnowledgeMembershipRecord>> {
        let key = Self::organization_membership_source_key(source_id);
        match self.system_kv().get(key.as_bytes())? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            None => Ok(None),
        }
    }

    pub(crate) fn load_organization_topic_relation(
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

    pub(crate) fn store_organization_relation(
        &self,
        relation: &OrganizationKnowledgeRelationRecord,
    ) -> Result<()> {
        let primary_key = Self::organization_relation_key(relation);
        let index_key = Self::organization_knowledge_relation_index_key(relation);
        let value = serde_json::to_vec(relation)?;
        self.system_kv().put(primary_key.as_bytes(), &value)?;
        self.system_kv().put(index_key.as_bytes(), &value)
    }

    pub(crate) fn store_organization_relations(
        &self,
        relations: &[OrganizationKnowledgeRelationRecord],
    ) -> Result<()> {
        for relation in relations {
            self.store_organization_relation(relation)?;
        }
        Ok(())
    }

    pub(crate) fn store_organization_membership(
        &self,
        membership: &OrganizationKnowledgeMembershipRecord,
    ) -> Result<()> {
        let primary_key = Self::organization_membership_source_key(membership.source_id);
        let index_key = Self::organization_membership_by_knowledge_key(membership);
        let value = serde_json::to_vec(membership)?;
        self.system_kv().put(primary_key.as_bytes(), &value)?;
        self.system_kv().put(index_key.as_bytes(), &value)
    }

    pub(crate) fn store_organization_memberships(
        &self,
        memberships: &[OrganizationKnowledgeMembershipRecord],
    ) -> Result<()> {
        for membership in memberships {
            self.store_organization_membership(membership)?;
        }
        Ok(())
    }

    pub(crate) fn delete_organization_membership(&self, source_id: Uuid) -> Result<()> {
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

    pub(crate) fn delete_organization_relation_by_primary_key(&self, primary_key: &str) -> Result<()> {
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

    pub(crate) fn delete_organization_membership_or_relation_by_key(&self, key: &str) -> Result<()> {
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

    pub(crate) fn store_organization_contribution(
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

    pub(crate) fn store_organization_contributions(
        &self,
        contributions: &[OrganizationKnowledgeContributionRecord],
    ) -> Result<()> {
        for contribution in contributions {
            self.store_organization_contribution(contribution)?;
        }
        Ok(())
    }

    pub(crate) fn submit_organization_contribution_candidates(
        &self,
        candidates: &[OrganizationKnowledgeContributionRecord],
    ) -> Result<()> {
        self.store_organization_contributions(candidates)
    }

    pub(crate) fn approve_organization_contribution_candidates(
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

    pub(crate) async fn list_organization_contributions(
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

    pub(crate) async fn list_organization_memberships(
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

    pub(crate) async fn resolve_organization_record_source_ids(
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

    pub(crate) async fn delete_organization_contributions(&self, knowledge_id: Uuid) -> Result<()> {
        let prefix = Self::organization_knowledge_contribution_prefix(knowledge_id);
        let system_kv = self.system_kv();
        let pairs =
            tokio::task::spawn_blocking(move || system_kv.scan(prefix.as_bytes())).await??;
        for (key, _) in pairs {
            self.system_kv().delete(&key).ok();
        }
        Ok(())
    }

    pub(crate) async fn delete_organization_memberships(&self, knowledge_id: Uuid) -> Result<()> {
        for membership in self.list_organization_memberships(knowledge_id).await? {
            self.delete_organization_membership(membership.source_id)?;
        }
        Ok(())
    }

    pub(crate) async fn load_organization_membership_sources(
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

    pub(crate) fn load_organization_contribution(
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

    pub(crate) fn revoke_organization_contribution(&self, knowledge_id: Uuid, source_id: Uuid) -> Result<()> {
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

    pub(crate) async fn list_organization_relations_for_knowledge(
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

    pub(crate) async fn cleanup_stale_organization_source_relations(&self) -> Result<usize> {
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

    pub(crate) fn select_retained_organization_knowledge(
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

    pub(crate) async fn build_organization_knowledge_mutation(
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

    pub(crate) async fn find_org_knowledge_records_for_contributor(
        &self,
        user_id: &str,
        org_id: &str,
    ) -> Result<Vec<(OrganizationKnowledgeRecord, Vec<Uuid>)>> {
        let prefix = format!("u:{}:unit:", user_id);
        let store = self.kv_store.clone();
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

    pub(crate) async fn delete_organization_knowledge_records(
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

    pub(crate) async fn list_persisted_organization_read_view_units(
        &self,
    ) -> Result<Vec<(Vec<u8>, MemoryUnit)>> {
        let kv = self.kv_store.clone();
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

    pub(crate) async fn delete_memory_unit_storage_by_key(
        &self,
        unit_key: Vec<u8>,
        unit_id: Uuid,
    ) -> Result<()> {
        let kv = self.kv_store.clone();
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

    pub(crate) async fn delete_materialized_organization_view_storage(&self, unit: &MemoryUnit) -> Result<()> {
        let unit_key = format!("u:{}:unit:{}", unit.user_id, unit.id).into_bytes();
        self.delete_memory_unit_storage_by_key(unit_key, unit.id)
            .await
    }

    pub(crate) async fn upsert_organization_knowledge(
        &self,
        record: OrganizationKnowledgeRecord,
        unit: MemoryUnit,
    ) -> Result<()> {
        self.store_organization_knowledge(&record)?;
        self.delete_materialized_organization_view_storage(&unit)
            .await
    }

    pub(crate) async fn publish_organization_knowledge(
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

    pub(crate) async fn load_reconciled_organization_source_units(
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

    pub(crate) fn organization_record_matches_reconciled_state(
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

    pub(crate) fn reconcile_active_organization_contributions(
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

    pub(crate) async fn reconcile_organization_record(
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

    pub(crate) async fn reconcile_organization_storage(
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

    pub(crate) async fn publish_native_shared_knowledge(&self, units: &[MemoryUnit]) -> Result<usize> {
        self.publish_native_shared_knowledge_for_domain(units, None)
            .await
    }

    pub(crate) async fn publish_native_shared_knowledge_for_domain(
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

    pub(crate) async fn write_materialized_search_storage(&self, unit: &MemoryUnit) -> Result<()> {
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

    pub(crate) async fn write_published_memory_unit_metadata(&self, unit: &MemoryUnit) -> Result<()> {
        let kv = self.kv_store.clone();
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

    pub(crate) async fn auto_link_memory(&self, unit: &MemoryUnit) -> Result<()> {
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

    pub(crate) async fn semantic_link_memory(&self, unit: &MemoryUnit) -> Result<()> {
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
    pub(crate) async fn list_organization_knowledge_records(
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

    pub(crate) async fn materialize_organization_search_hits(
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

    pub(crate) async fn search_organization_knowledge_records(
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
                &self.kv_store,
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

    pub(crate) async fn search_organization_knowledge_text(
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

}
