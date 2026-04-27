use chrono::{DateTime, Utc};
use memorose_common::tokenizer::count_tokens;
use memorose_common::{MemoryDomain, MemoryUnit, SharePolicy, ShareTarget, TimeRange};

/// Escape a string value for use in LanceDB SQL filter expressions.
pub(crate) fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Validate that an ID (user_id, org_id, agent_id) is safe for use in storage keys and SQL filters.
/// Rejects IDs containing SQL injection characters or control characters.
pub(crate) fn validate_id(id: &str) -> anyhow::Result<()> {
    if id.is_empty() {
        return Err(anyhow::anyhow!("ID must not be empty"));
    }
    if id.len() > 256 {
        return Err(anyhow::anyhow!("ID must not exceed 256 characters"));
    }
    if id.contains('\'') || id.contains(';') || id.contains("--") {
        return Err(anyhow::anyhow!("ID contains invalid characters"));
    }
    if id.bytes().any(|b| b < 0x20 && b != b'\t') {
        return Err(anyhow::anyhow!("ID contains control characters"));
    }
    Ok(())
}

pub(crate) fn cosine_similarity(v1: &[f32], v2: &[f32]) -> f32 {
    let dot_product: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
    let magnitude_v1: f32 = v1.iter().map(|v| v * v).sum::<f32>().sqrt();
    let magnitude_v2: f32 = v2.iter().map(|v| v * v).sum::<f32>().sqrt();
    if magnitude_v1 < f32::EPSILON || magnitude_v2 < f32::EPSILON {
        return 0.0;
    }
    (dot_product / (magnitude_v1 * magnitude_v2)).clamp(-1.0, 1.0)
}

pub(crate) const OBSOLETE_ACTION_MIN_CONFIDENCE: f32 = 0.85;
pub(crate) const OBSOLETE_ACTION_RELATION_ONLY_MIN_CONFIDENCE: f32 = 0.70;

impl super::MemoroseEngine {
    pub(crate) fn is_local_domain(domain: &MemoryDomain) -> bool {
        matches!(domain, MemoryDomain::Agent | MemoryDomain::User)
    }

    pub(crate) fn build_time_filter(&self, range: Option<TimeRange>) -> Option<String> {
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
        let mut conditions = vec![format!("user_id = '{}'", escape_sql_string(user_id))];
        if let Some(e) = extra {
            conditions.push(e);
        }
        Some(conditions.join(" AND "))
    }

    pub(crate) fn build_global_filter(
        &self,
        domain: MemoryDomain,
        org_id: Option<&str>,
        agent_id: Option<&str>,
        extra: Option<String>,
    ) -> Option<String> {
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

    pub(crate) fn matches_valid_time_filter(
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

    pub(crate) fn tokenize_search_text(text: &str) -> Vec<String> {
        crate::fact_extraction::tokenize_search_text(text)
    }

    pub(crate) fn memory_unit_token_cost(unit: &MemoryUnit) -> usize {
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

    pub(crate) fn truncate_scored_results_to_token_budget<T>(
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

    pub(crate) fn apply_token_budget_to_scored_memory_units(
        results: Vec<(MemoryUnit, f32)>,
        token_budget: Option<usize>,
    ) -> Vec<(MemoryUnit, f32)> {
        Self::truncate_scored_results_to_token_budget(results, token_budget, |unit| {
            Self::memory_unit_token_cost(unit)
        })
    }

    pub(crate) fn apply_token_budget_to_scored_shared_hits(
        results: Vec<(super::SharedSearchHit, f32)>,
        token_budget: Option<usize>,
    ) -> Vec<(super::SharedSearchHit, f32)> {
        Self::truncate_scored_results_to_token_budget(results, token_budget, |hit| {
            Self::memory_unit_token_cost(hit.memory_unit())
        })
    }

    pub(crate) fn normalize_share_policy(
        mut policy: SharePolicy,
        target: ShareTarget,
    ) -> SharePolicy {
        policy.targets = vec![target];
        policy
    }
}
