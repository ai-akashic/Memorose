use axum::{response::IntoResponse, Json};
use memorose_common::{
    Asset, EventContent, MemoryDomain, MemoryType, MemoryUnit,
};
use memorose_core::engine::{
    OrganizationAutomationCounterSnapshot, OrganizationKnowledgeContributionRecord,
    OrganizationKnowledgeContributionStatus, OrganizationKnowledgeDetailRecord,
    OrganizationKnowledgeMembershipEntry,
};
use serde::Serialize;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[derive(Clone, Default, serde::Serialize)]
pub struct DomainBreakdown {
    pub agent: usize,
    pub user: usize,
    pub organization: usize,
}

impl DomainBreakdown {
    pub fn record(&mut self, domain: &MemoryDomain) {
        match domain {
            MemoryDomain::Agent => self.agent += 1,
            MemoryDomain::User => self.user += 1,
            MemoryDomain::Organization => self.organization += 1,
        }
    }

    pub fn total(&self) -> usize {
        self.agent + self.user + self.organization
    }

    pub fn local_total(&self) -> usize {
        self.agent + self.user
    }

    pub fn shared_total(&self) -> usize {
        self.organization
    }
}

#[derive(Clone, Default, serde::Serialize)]
pub struct LevelBreakdown {
    pub l1: usize,
    pub l2: usize,
    pub l3: usize,
}

#[derive(Clone, Default)]
pub struct MemoryAggregate {
    pub by_domain: DomainBreakdown,
    pub local_levels: LevelBreakdown,
    pub shared_levels: LevelBreakdown,
}

impl MemoryAggregate {
    pub fn record_unit(&mut self, unit: &MemoryUnit) {
        let domain = unit.domain.clone();
        self.by_domain.record(&domain);

        let target = if is_local_domain(&domain) {
            &mut self.local_levels
        } else {
            &mut self.shared_levels
        };

        match unit.level {
            1 => target.l1 += 1,
            2 => target.l2 += 1,
            3 => target.l3 += 1,
            _ => {}
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.by_domain.agent += other.by_domain.agent;
        self.by_domain.user += other.by_domain.user;
        self.by_domain.organization += other.by_domain.organization;
        self.local_levels.l1 += other.local_levels.l1;
        self.local_levels.l2 += other.local_levels.l2;
        self.local_levels.l3 += other.local_levels.l3;
        self.shared_levels.l1 += other.shared_levels.l1;
        self.shared_levels.l2 += other.shared_levels.l2;
        self.shared_levels.l3 += other.shared_levels.l3;
    }

    pub fn total_memories(&self) -> usize {
        self.by_domain.total()
    }

    pub fn local_memories(&self) -> usize {
        self.by_domain.local_total()
    }

    pub fn shared_memories(&self) -> usize {
        self.by_domain.shared_total()
    }

    pub fn total_l1(&self) -> usize {
        self.local_levels.l1 + self.shared_levels.l1
    }

    pub fn total_l2(&self) -> usize {
        self.local_levels.l2 + self.shared_levels.l2
    }

    pub fn total_l3(&self) -> usize {
        self.local_levels.l3 + self.shared_levels.l3
    }
}

pub fn is_local_domain(domain: &MemoryDomain) -> bool {
    matches!(domain, MemoryDomain::Agent | MemoryDomain::User)
}

pub fn matches_dashboard_org_scope(
    record_org_id: Option<&str>,
    requested_org_id: Option<&str>,
) -> bool {
    match requested_org_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        None => true,
        Some("default") => record_org_id.is_none() || record_org_id == Some("default"),
        Some(expected) => record_org_id == Some(expected),
    }
}

pub fn update_last_activity(last_activity: &mut Option<i64>, timestamp: i64) {
    if last_activity.is_none() || *last_activity < Some(timestamp) {
        *last_activity = Some(timestamp);
    }
}

pub fn validate_registry_id(value: &str, field: &str) -> Result<(), axum::response::Response> {
    if value.trim().is_empty() {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": format!("{field} must not be empty") })),
        )
            .into_response());
    }

    if value.len() > 256 {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": format!("{field} must not exceed 256 characters") })),
        )
            .into_response());
    }

    Ok(())
}

#[derive(Clone, Serialize)]
pub struct DashboardAssetView {
    pub storage_key: String,
    pub original_name: String,
    pub asset_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl From<&Asset> for DashboardAssetView {
    fn from(asset: &Asset) -> Self {
        Self {
            storage_key: public_asset_storage_key(asset),
            original_name: asset.original_name.clone(),
            asset_type: asset.asset_type.clone(),
            description: asset.description.clone(),
        }
    }
}

#[derive(Clone, Serialize)]
pub struct DashboardMemoryDetailUnitView {
    pub id: uuid::Uuid,
    pub org_id: Option<String>,
    pub user_id: String,
    pub content: String,
    pub keywords: Vec<String>,
    pub importance: f32,
    pub level: u8,
    pub transaction_time: chrono::DateTime<chrono::Utc>,
    pub assets: Vec<DashboardAssetView>,
}

impl From<&MemoryUnit> for DashboardMemoryDetailUnitView {
    fn from(unit: &MemoryUnit) -> Self {
        Self {
            id: unit.id,
            org_id: unit.org_id.clone(),
            user_id: unit.user_id.clone(),
            content: unit.content.clone(),
            keywords: unit.keywords.clone(),
            importance: unit.importance,
            level: unit.level,
            transaction_time: unit.transaction_time,
            assets: unit.assets.iter().map(DashboardAssetView::from).collect(),
        }
    }
}

#[derive(Serialize)]
pub struct DashboardMemoryDetailResponse {
    #[serde(flatten)]
    pub unit: DashboardMemoryDetailUnitView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organization_knowledge: Option<DashboardOrganizationKnowledgeView>,
}

#[derive(Clone, Serialize)]
pub struct DashboardSearchMemoryUnitView {
    pub id: uuid::Uuid,
    pub memory_type: MemoryType,
    pub content: String,
    pub keywords: Vec<String>,
    pub level: u8,
    pub assets: Vec<DashboardAssetView>,
}

impl From<&MemoryUnit> for DashboardSearchMemoryUnitView {
    fn from(unit: &MemoryUnit) -> Self {
        Self {
            id: unit.id,
            memory_type: unit.memory_type.clone(),
            content: unit.content.clone(),
            keywords: unit.keywords.clone(),
            level: unit.level,
            assets: unit.assets.iter().map(DashboardAssetView::from).collect(),
        }
    }
}

#[derive(Clone, Serialize)]
pub struct DashboardOrganizationKnowledgeUnitView {
    pub id: uuid::Uuid,
    pub content: String,
    pub keywords: Vec<String>,
    pub transaction_time: chrono::DateTime<chrono::Utc>,
}

impl From<&MemoryUnit> for DashboardOrganizationKnowledgeUnitView {
    fn from(unit: &MemoryUnit) -> Self {
        Self {
            id: unit.id,
            content: unit.content.clone(),
            keywords: unit.keywords.clone(),
            transaction_time: unit.transaction_time,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct DashboardOrganizationContributionView {
    pub source_id: uuid::Uuid,
    pub contributor_user_id: String,
    pub status: String,
    pub source_memory_type: Option<String>,
    pub source_level: Option<u8>,
    pub source_keywords: Vec<String>,
    pub source_content_preview: Option<String>,
    pub candidate_at: Option<chrono::DateTime<chrono::Utc>>,
    pub activated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub approval_mode: Option<String>,
    pub approved_by: Option<String>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Serialize)]
pub struct DashboardOrganizationMembershipView {
    pub source_id: uuid::Uuid,
    pub contributor_user_id: String,
    pub source_memory_type: Option<String>,
    pub source_level: Option<u8>,
    pub source_keywords: Vec<String>,
    pub source_content_preview: Option<String>,
    pub activated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub approval_mode: Option<String>,
    pub approved_by: Option<String>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Serialize)]
pub struct DashboardOrganizationMembershipContributorSummaryView {
    pub contributor_user_id: String,
    pub membership_count: usize,
    pub source_ids: Vec<uuid::Uuid>,
    pub source_memory_types: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct DashboardOrganizationMembershipSourceTypeSummaryView {
    pub source_memory_type: String,
    pub membership_count: usize,
    pub contributor_user_ids: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct DashboardOrganizationKnowledgeMembershipSummaryView {
    pub contributors: Vec<DashboardOrganizationMembershipContributorSummaryView>,
    pub source_types: Vec<DashboardOrganizationMembershipSourceTypeSummaryView>,
}

#[derive(Clone, Serialize)]
pub struct DashboardOrganizationKnowledgeMembershipStateView {
    pub membership_count: usize,
    pub summary: DashboardOrganizationKnowledgeMembershipSummaryView,
    pub memberships: Vec<DashboardOrganizationMembershipView>,
}

#[derive(Clone, Serialize)]
pub struct DashboardOrganizationHistoryContributorSummaryView {
    pub contributor_user_id: String,
    pub contribution_count: usize,
    pub candidate_contribution_count: usize,
    pub active_contribution_count: usize,
    pub revoked_contribution_count: usize,
    pub source_ids: Vec<uuid::Uuid>,
    pub source_memory_types: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct DashboardOrganizationHistorySourceTypeSummaryView {
    pub source_memory_type: String,
    pub contribution_count: usize,
    pub candidate_contribution_count: usize,
    pub active_contribution_count: usize,
    pub revoked_contribution_count: usize,
    pub contributor_user_ids: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct DashboardOrganizationKnowledgeHistorySummaryView {
    pub contributors: Vec<DashboardOrganizationHistoryContributorSummaryView>,
    pub source_types: Vec<DashboardOrganizationHistorySourceTypeSummaryView>,
}

#[derive(Clone, Serialize)]
pub struct DashboardOrganizationKnowledgeHistoryView {
    pub contribution_count: usize,
    pub candidate_contribution_count: usize,
    pub active_contribution_count: usize,
    pub revoked_contribution_count: usize,
    pub summary: DashboardOrganizationKnowledgeHistorySummaryView,
    pub contributions: Vec<DashboardOrganizationContributionView>,
}

#[derive(Clone, Serialize)]
pub struct DashboardOrganizationKnowledgeView {
    pub membership: DashboardOrganizationKnowledgeMembershipStateView,
    pub history: DashboardOrganizationKnowledgeHistoryView,
}

#[derive(Clone, Serialize)]
pub struct DashboardOrganizationKnowledgeListItemView {
    pub unit: DashboardOrganizationKnowledgeUnitView,
    pub contribution_count: usize,
    pub membership_count: usize,
    pub contributor_user_ids: Vec<String>,
    pub top_contributor_user_id: Option<String>,
    pub source_memory_types: Vec<String>,
    pub primary_source_memory_type: Option<String>,
    pub published_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
pub struct DashboardOrganizationKnowledgeDetailView {
    pub unit: DashboardOrganizationKnowledgeUnitView,
    pub knowledge: DashboardOrganizationKnowledgeView,
}

#[derive(Clone, Serialize)]
pub struct DashboardOrganizationAutomationMetricCountView {
    pub key: String,
    pub value: usize,
}

#[derive(Serialize)]
pub struct DashboardOrganizationAutomationMetricsView {
    pub org_id: String,
    pub knowledge_count: usize,
    pub contribution_count: usize,
    pub membership_count: usize,
    pub candidate_contribution_count: usize,
    pub revoked_contribution_count: usize,
    pub contributor_count: usize,
    pub auto_approved_total: usize,
    pub auto_publish_total: usize,
    pub rebuild_total: usize,
    pub revoke_total: usize,
    pub merged_publication_total: usize,
    pub source_type_distribution: Vec<DashboardOrganizationAutomationMetricCountView>,
}

pub struct DashboardOrganizationKnowledgeRollup {
    pub contribution_count: usize,
    pub candidate_contribution_count: usize,
    pub revoked_contribution_count: usize,
    pub membership_count: usize,
    pub contributor_user_ids: Vec<String>,
    pub top_contributor_user_id: Option<String>,
    pub source_memory_types: Vec<String>,
    pub primary_source_memory_type: Option<String>,
    pub source_type_distribution: Vec<(String, usize)>,
    pub published_at: chrono::DateTime<chrono::Utc>,
}

pub fn dashboard_organization_rollup_from_detail(
    detail: &OrganizationKnowledgeDetailRecord,
) -> DashboardOrganizationKnowledgeRollup {
    let membership_count = detail.memberships.len();
    let mut contributor_counts: HashMap<String, usize> = HashMap::new();
    let mut source_type_counts: HashMap<String, usize> = HashMap::new();

    for entry in &detail.memberships {
        *contributor_counts
            .entry(entry.membership.contributor_user_id.clone())
            .or_default() += 1;
        let source_type = dashboard_memory_type_label(&entry.source_unit.memory_type);
        *source_type_counts.entry(source_type).or_default() += 1;
    }

    let mut contributor_user_ids = contributor_counts.keys().cloned().collect::<Vec<_>>();
    contributor_user_ids.sort();

    let top_contributor_user_id = contributor_counts
        .into_iter()
        .max_by(|(left_id, left_count), (right_id, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| right_id.cmp(left_id))
        })
        .map(|(user_id, _)| user_id);

    let mut source_type_distribution = source_type_counts.into_iter().collect::<Vec<_>>();
    source_type_distribution.sort_by(|(left_key, left_value), (right_key, right_value)| {
        right_value
            .cmp(left_value)
            .then_with(|| left_key.cmp(right_key))
    });
    let source_memory_types = source_type_distribution
        .iter()
        .map(|(source_type, _)| source_type.clone())
        .collect::<Vec<_>>();
    let primary_source_memory_type = source_type_distribution
        .first()
        .map(|(source_type, _)| source_type.clone());

    let mut contribution_count = 0;
    let mut candidate_contribution_count = 0;
    let mut revoked_contribution_count = 0;
    for entry in &detail.contributions {
        contribution_count += 1;
        match entry.contribution.status {
            OrganizationKnowledgeContributionStatus::Candidate => {
                candidate_contribution_count += 1;
            }
            OrganizationKnowledgeContributionStatus::Active => {}
            OrganizationKnowledgeContributionStatus::Revoked => {
                revoked_contribution_count += 1;
            }
        }
    }

    let published_at = detail
        .contributions
        .iter()
        .filter_map(|entry| entry.contribution.activated_at)
        .max()
        .unwrap_or(detail.record.updated_at);

    DashboardOrganizationKnowledgeRollup {
        contribution_count,
        candidate_contribution_count,
        revoked_contribution_count,
        membership_count,
        contributor_user_ids,
        top_contributor_user_id,
        source_memory_types,
        primary_source_memory_type,
        source_type_distribution,
        published_at,
    }
}

impl DashboardOrganizationAutomationMetricsView {
    pub fn from_detail_records(
        org_id: &str,
        details: &[OrganizationKnowledgeDetailRecord],
        counters: OrganizationAutomationCounterSnapshot,
    ) -> Self {
        let rollups = details
            .iter()
            .map(dashboard_organization_rollup_from_detail)
            .collect::<Vec<_>>();
        let knowledge_count = rollups.len();
        let contribution_count = rollups.iter().map(|rollup| rollup.contribution_count).sum();
        let membership_count = rollups.iter().map(|rollup| rollup.membership_count).sum();
        let candidate_contribution_count = rollups
            .iter()
            .map(|rollup| rollup.candidate_contribution_count)
            .sum();
        let revoked_contribution_count = rollups
            .iter()
            .map(|rollup| rollup.revoked_contribution_count)
            .sum();
        let mut contributor_user_ids = std::collections::BTreeSet::new();
        let mut source_type_distribution: HashMap<String, usize> = HashMap::new();
        for rollup in &rollups {
            for user_id in &rollup.contributor_user_ids {
                contributor_user_ids.insert(user_id.clone());
            }
            for (source_type, count) in &rollup.source_type_distribution {
                *source_type_distribution
                    .entry(source_type.clone())
                    .or_default() += count;
            }
        }
        let mut source_type_distribution = source_type_distribution
            .into_iter()
            .map(|(key, value)| DashboardOrganizationAutomationMetricCountView { key, value })
            .collect::<Vec<_>>();
        source_type_distribution.sort_by(|left, right| {
            right
                .value
                .cmp(&left.value)
                .then_with(|| left.key.cmp(&right.key))
        });

        Self {
            org_id: org_id.to_string(),
            knowledge_count,
            contribution_count,
            membership_count,
            candidate_contribution_count,
            revoked_contribution_count,
            contributor_count: contributor_user_ids.len(),
            auto_approved_total: counters.auto_approved_total,
            auto_publish_total: counters.auto_publish_total,
            rebuild_total: counters.rebuild_total,
            revoke_total: counters.revoke_total,
            merged_publication_total: counters.merged_publication_total,
            source_type_distribution,
        }
    }
}

pub fn default_page() -> usize {
    1
}
pub fn default_limit() -> usize {
    20
}
pub fn default_sort() -> String {
    "importance".to_string()
}

#[derive(Clone)]
pub struct DashboardMemoryRow {
    pub id: String,
    pub user_id: String,
    pub agent_id: Option<String>,
    pub content: String,
    pub level: u8,
    pub importance: f32,
    pub keywords: Vec<String>,
    pub access_count: u64,
    pub transaction_time: chrono::DateTime<chrono::Utc>,
    pub reference_count: usize,
    pub item_type: &'static str,
    pub memory_type: Option<String>,
}

#[derive(Serialize)]
pub struct DashboardMemoryListItemView {
    pub id: String,
    pub user_id: String,
    pub agent_id: Option<String>,
    pub content: String,
    pub level: u8,
    pub importance: f32,
    pub keywords: Vec<String>,
    pub access_count: u64,
    pub reference_count: usize,
    pub item_type: &'static str,
    pub memory_type: Option<String>,
}

impl From<DashboardMemoryRow> for DashboardMemoryListItemView {
    fn from(row: DashboardMemoryRow) -> Self {
        Self {
            id: row.id,
            user_id: row.user_id,
            agent_id: row.agent_id,
            content: row.content,
            level: row.level,
            importance: row.importance,
            keywords: row.keywords,
            access_count: row.access_count,
            reference_count: row.reference_count,
            item_type: row.item_type,
            memory_type: row.memory_type,
        }
    }
}

#[derive(Serialize)]
pub struct DashboardMemoryListResponse {
    pub items: Vec<DashboardMemoryListItemView>,
    pub total: usize,
    pub page: usize,
    pub limit: usize,
}

pub fn display_identity_for_memory(unit: &MemoryUnit) -> (String, Option<String>) {
    if unit.domain == MemoryDomain::Organization {
        (String::new(), None)
    } else {
        (unit.user_id.clone(), unit.agent_id.clone())
    }
}

pub fn event_content_preview(content: &EventContent) -> (String, bool) {
    match content {
        EventContent::Text(text) => (text.clone(), false),
        EventContent::Image(url) => (format!("[Image] {}", url), true),
        EventContent::Audio(url) => (format!("[Audio] {}", url), true),
        EventContent::Video(url) => (format!("[Video] {}", url), true),
        EventContent::Json(value) => (value.to_string(), false),
    }
}

pub fn public_asset_storage_key(asset: &Asset) -> String {
    let key = asset.storage_key.trim();
    if key.starts_with("http://")
        || key.starts_with("https://")
        || key.starts_with("s3://")
        || key.starts_with("local://")
        || key.starts_with("inline://")
    {
        return key.to_string();
    }

    if key.is_empty() {
        return "inline://asset".to_string();
    }

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    asset.asset_type.hash(&mut hasher);
    key.hash(&mut hasher);
    format!("inline://{}/{:016x}", asset.asset_type, hasher.finish())
}

pub fn asset_kind_label(asset_type: &str) -> &'static str {
    let normalized = asset_type.to_ascii_lowercase();
    if normalized.starts_with("image") {
        "Image"
    } else if normalized.starts_with("audio") {
        "Audio"
    } else if normalized.starts_with("video") {
        "Video"
    } else {
        "Asset"
    }
}

pub fn asset_source_reference(asset: &Asset) -> String {
    let key = public_asset_storage_key(asset);
    let key = key.trim();
    if key.starts_with("http://")
        || key.starts_with("https://")
        || key.starts_with("s3://")
        || key.starts_with("local://")
        || key.starts_with("inline://")
    {
        key.to_string()
    } else if !asset.original_name.trim().is_empty() {
        format!("inline://{}", asset.original_name)
    } else {
        "inline://asset".to_string()
    }
}

pub fn format_asset_context(asset: &Asset) -> String {
    let kind = asset_kind_label(&asset.asset_type);
    let source = asset_source_reference(asset);
    match asset.description.as_deref().map(str::trim) {
        Some(description) if !description.is_empty() => {
            format!(
                "[{}: {}] (Source: {})",
                kind,
                dashboard_build_content_preview(description, 240),
                source
            )
        }
        _ => format!("[{}] (Source: {})", kind, source),
    }
}

pub fn format_memory_unit_context(unit: &MemoryUnit) -> String {
    let mut lines = vec![format!(
        "- {}",
        dashboard_build_content_preview(&unit.content, 320)
    )];
    if !unit.keywords.is_empty() {
        lines.push(format!(
            "  Keywords: {}",
            unit.keywords
                .iter()
                .take(6)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    for asset in unit.assets.iter().take(3) {
        lines.push(format!("  {}", format_asset_context(asset)));
    }
    if unit.assets.len() > 3 {
        lines.push(format!(
            "  [Assets: {} more omitted]",
            unit.assets.len() - 3
        ));
    }
    lines.join("\n")
}

pub fn append_context_with_budget(context_text: &mut String, block: &str, budget_chars: usize) -> bool {
    let used = context_text.chars().count();
    if used >= budget_chars {
        return false;
    }

    let remaining = budget_chars - used;
    let block_len = block.chars().count();

    if block_len + 1 <= remaining {
        context_text.push_str(block);
        context_text.push('\n');
        return true;
    }

    if remaining > 24 {
        context_text.push_str(&dashboard_build_content_preview(
            block,
            remaining.saturating_sub(1),
        ));
        context_text.push('\n');
    }
    false
}

pub fn dashboard_memory_detail_view(
    unit: &MemoryUnit,
    organization_knowledge: Option<DashboardOrganizationKnowledgeView>,
) -> DashboardMemoryDetailResponse {
    DashboardMemoryDetailResponse {
        unit: DashboardMemoryDetailUnitView::from(unit),
        organization_knowledge,
    }
}

pub fn dashboard_build_content_preview(text: &str, max_chars: usize) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        return normalized;
    }
    let truncated = normalized.chars().take(max_chars).collect::<String>();
    format!("{}...", truncated.trim_end())
}

pub fn dashboard_memory_type_label(memory_type: &MemoryType) -> String {
    match memory_type {
        MemoryType::Factual => "factual".to_string(),
        MemoryType::Procedural => "procedural".to_string(),
    }
}

pub fn dashboard_contribution_status_label(
    status: &OrganizationKnowledgeContributionStatus,
) -> &'static str {
    match status {
        OrganizationKnowledgeContributionStatus::Candidate => "candidate",
        OrganizationKnowledgeContributionStatus::Active => "active",
        OrganizationKnowledgeContributionStatus::Revoked => "revoked",
    }
}

pub fn dashboard_approval_mode_label(mode: &impl Serialize) -> String {
    serde_json::to_string(mode)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

pub fn dashboard_organization_contribution_view_from_record(
    contribution: OrganizationKnowledgeContributionRecord,
    source_unit: Option<MemoryUnit>,
) -> DashboardOrganizationContributionView {
    DashboardOrganizationContributionView {
        source_id: contribution.source_id,
        contributor_user_id: contribution.contributor_user_id,
        status: dashboard_contribution_status_label(&contribution.status).to_string(),
        source_memory_type: source_unit
            .as_ref()
            .map(|unit| dashboard_memory_type_label(&unit.memory_type)),
        source_level: source_unit.as_ref().map(|unit| unit.level),
        source_keywords: source_unit
            .as_ref()
            .map(|unit| unit.keywords.clone())
            .unwrap_or_default(),
        source_content_preview: source_unit
            .as_ref()
            .map(|unit| dashboard_build_content_preview(&unit.content, 160)),
        candidate_at: contribution.candidate_at,
        activated_at: contribution.activated_at,
        approval_mode: contribution
            .approval_mode
            .map(|mode| dashboard_approval_mode_label(&mode)),
        approved_by: contribution.approved_by,
        revoked_at: contribution.revoked_at,
    }
}

pub fn dashboard_organization_membership_view_from_entry(
    entry: OrganizationKnowledgeMembershipEntry,
) -> DashboardOrganizationMembershipView {
    DashboardOrganizationMembershipView {
        source_id: entry.membership.source_id,
        contributor_user_id: entry.membership.contributor_user_id,
        source_memory_type: Some(dashboard_memory_type_label(&entry.source_unit.memory_type)),
        source_level: Some(entry.source_unit.level),
        source_keywords: entry.source_unit.keywords,
        source_content_preview: Some(dashboard_build_content_preview(
            &entry.source_unit.content,
            160,
        )),
        activated_at: entry
            .contribution
            .as_ref()
            .and_then(|record| record.activated_at),
        approval_mode: entry
            .contribution
            .as_ref()
            .and_then(|record| record.approval_mode.as_ref())
            .map(dashboard_approval_mode_label),
        approved_by: entry.contribution.and_then(|record| record.approved_by),
        updated_at: entry.membership.updated_at,
    }
}

pub fn dashboard_organization_membership_summary(
    membership_views: &[DashboardOrganizationMembershipView],
) -> DashboardOrganizationKnowledgeMembershipSummaryView {
    let mut membership_contributors = std::collections::BTreeMap::<
        String,
        DashboardOrganizationMembershipContributorSummaryView,
    >::new();
    let mut membership_source_types = std::collections::BTreeMap::<
        String,
        DashboardOrganizationMembershipSourceTypeSummaryView,
    >::new();

    for membership in membership_views {
        let contributor = membership_contributors
            .entry(membership.contributor_user_id.clone())
            .or_insert(DashboardOrganizationMembershipContributorSummaryView {
                contributor_user_id: membership.contributor_user_id.clone(),
                membership_count: 0,
                source_ids: Vec::new(),
                source_memory_types: Vec::new(),
            });
        contributor.membership_count += 1;
        contributor.source_ids.push(membership.source_id);
        if let Some(source_memory_type) = membership.source_memory_type.as_ref() {
            if !contributor.source_memory_types.contains(source_memory_type) {
                contributor
                    .source_memory_types
                    .push(source_memory_type.clone());
            }
            let source_type = membership_source_types
                .entry(source_memory_type.clone())
                .or_insert(DashboardOrganizationMembershipSourceTypeSummaryView {
                    source_memory_type: source_memory_type.clone(),
                    membership_count: 0,
                    contributor_user_ids: Vec::new(),
                });
            source_type.membership_count += 1;
            if !source_type
                .contributor_user_ids
                .contains(&membership.contributor_user_id)
            {
                source_type
                    .contributor_user_ids
                    .push(membership.contributor_user_id.clone());
            }
        }
    }

    let mut contributors = membership_contributors.into_values().collect::<Vec<_>>();
    contributors.sort_by(|left, right| {
        right
            .membership_count
            .cmp(&left.membership_count)
            .then_with(|| left.contributor_user_id.cmp(&right.contributor_user_id))
    });
    for summary in &mut contributors {
        summary.source_ids.sort();
        summary.source_ids.dedup();
        summary.source_memory_types.sort();
    }

    let mut source_types = membership_source_types.into_values().collect::<Vec<_>>();
    source_types.sort_by(|left, right| {
        right
            .membership_count
            .cmp(&left.membership_count)
            .then_with(|| left.source_memory_type.cmp(&right.source_memory_type))
    });
    for summary in &mut source_types {
        summary.contributor_user_ids.sort();
    }

    DashboardOrganizationKnowledgeMembershipSummaryView {
        contributors,
        source_types,
    }
}

pub fn dashboard_organization_contribution_counts(
    contribution_views: &[DashboardOrganizationContributionView],
) -> (usize, usize, usize, usize) {
    let mut contribution_count = 0;
    let mut candidate_contribution_count = 0;
    let mut active_contribution_count = 0;
    let mut revoked_contribution_count = 0;

    for contribution in contribution_views {
        contribution_count += 1;
        match contribution.status.as_str() {
            "candidate" => candidate_contribution_count += 1,
            "active" => active_contribution_count += 1,
            "revoked" => revoked_contribution_count += 1,
            _ => {}
        }
    }

    (
        contribution_count,
        candidate_contribution_count,
        active_contribution_count,
        revoked_contribution_count,
    )
}

pub fn dashboard_organization_history_summary(
    contribution_views: &[DashboardOrganizationContributionView],
) -> DashboardOrganizationKnowledgeHistorySummaryView {
    let mut history_contributors = std::collections::BTreeMap::<
        String,
        DashboardOrganizationHistoryContributorSummaryView,
    >::new();
    let mut history_source_types = std::collections::BTreeMap::<
        String,
        DashboardOrganizationHistorySourceTypeSummaryView,
    >::new();

    for contribution in contribution_views {
        let contributor = history_contributors
            .entry(contribution.contributor_user_id.clone())
            .or_insert(DashboardOrganizationHistoryContributorSummaryView {
                contributor_user_id: contribution.contributor_user_id.clone(),
                contribution_count: 0,
                candidate_contribution_count: 0,
                active_contribution_count: 0,
                revoked_contribution_count: 0,
                source_ids: Vec::new(),
                source_memory_types: Vec::new(),
            });
        contributor.contribution_count += 1;
        match contribution.status.as_str() {
            "candidate" => contributor.candidate_contribution_count += 1,
            "active" => contributor.active_contribution_count += 1,
            "revoked" => contributor.revoked_contribution_count += 1,
            _ => {}
        }
        contributor.source_ids.push(contribution.source_id);
        if let Some(source_memory_type) = contribution.source_memory_type.as_ref() {
            if !contributor.source_memory_types.contains(source_memory_type) {
                contributor
                    .source_memory_types
                    .push(source_memory_type.clone());
            }
            let source_type = history_source_types
                .entry(source_memory_type.clone())
                .or_insert(DashboardOrganizationHistorySourceTypeSummaryView {
                    source_memory_type: source_memory_type.clone(),
                    contribution_count: 0,
                    candidate_contribution_count: 0,
                    active_contribution_count: 0,
                    revoked_contribution_count: 0,
                    contributor_user_ids: Vec::new(),
                });
            source_type.contribution_count += 1;
            match contribution.status.as_str() {
                "candidate" => source_type.candidate_contribution_count += 1,
                "active" => source_type.active_contribution_count += 1,
                "revoked" => source_type.revoked_contribution_count += 1,
                _ => {}
            }
            if !source_type
                .contributor_user_ids
                .contains(&contribution.contributor_user_id)
            {
                source_type
                    .contributor_user_ids
                    .push(contribution.contributor_user_id.clone());
            }
        }
    }

    let mut contributors = history_contributors.into_values().collect::<Vec<_>>();
    contributors.sort_by(|left, right| {
        right
            .active_contribution_count
            .cmp(&left.active_contribution_count)
            .then_with(|| right.contribution_count.cmp(&left.contribution_count))
            .then_with(|| left.contributor_user_id.cmp(&right.contributor_user_id))
    });
    for summary in &mut contributors {
        summary.source_ids.sort();
        summary.source_ids.dedup();
        summary.source_memory_types.sort();
    }

    let mut source_types = history_source_types.into_values().collect::<Vec<_>>();
    source_types.sort_by(|left, right| {
        right
            .active_contribution_count
            .cmp(&left.active_contribution_count)
            .then_with(|| right.contribution_count.cmp(&left.contribution_count))
            .then_with(|| left.source_memory_type.cmp(&right.source_memory_type))
    });
    for summary in &mut source_types {
        summary.contributor_user_ids.sort();
    }

    DashboardOrganizationKnowledgeHistorySummaryView {
        contributors,
        source_types,
    }
}

pub fn dashboard_organization_knowledge_view_from_detail(
    detail: &OrganizationKnowledgeDetailRecord,
) -> DashboardOrganizationKnowledgeView {
    let membership_views = detail
        .memberships
        .clone()
        .into_iter()
        .map(dashboard_organization_membership_view_from_entry)
        .collect::<Vec<_>>();
    let contribution_views = detail
        .contributions
        .clone()
        .into_iter()
        .map(|entry| {
            dashboard_organization_contribution_view_from_record(
                entry.contribution,
                entry.source_unit,
            )
        })
        .collect::<Vec<_>>();
    let membership_summary = dashboard_organization_membership_summary(&membership_views);
    let history_summary = dashboard_organization_history_summary(&contribution_views);
    let (
        contribution_count,
        candidate_contribution_count,
        active_contribution_count,
        revoked_contribution_count,
    ) = dashboard_organization_contribution_counts(&contribution_views);

    DashboardOrganizationKnowledgeView {
        membership: DashboardOrganizationKnowledgeMembershipStateView {
            membership_count: membership_views.len(),
            summary: membership_summary,
            memberships: membership_views,
        },
        history: DashboardOrganizationKnowledgeHistoryView {
            contribution_count,
            candidate_contribution_count,
            active_contribution_count,
            revoked_contribution_count,
            summary: history_summary,
            contributions: contribution_views,
        },
    }
}

pub fn dashboard_organization_list_item_from_detail(
    detail: &OrganizationKnowledgeDetailRecord,
) -> DashboardOrganizationKnowledgeListItemView {
    let rollup = dashboard_organization_rollup_from_detail(detail);

    DashboardOrganizationKnowledgeListItemView {
        unit: DashboardOrganizationKnowledgeUnitView::from(&detail.read_view),
        contribution_count: rollup.contribution_count,
        membership_count: rollup.membership_count,
        contributor_user_ids: rollup.contributor_user_ids,
        top_contributor_user_id: rollup.top_contributor_user_id,
        source_memory_types: rollup.source_memory_types,
        primary_source_memory_type: rollup.primary_source_memory_type,
        published_at: rollup.published_at,
    }
}


#[cfg(test)]
mod test_aggregates {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_domain_breakdown_record_and_totals() {
        let mut breakdown = DomainBreakdown::default();
        breakdown.record(&MemoryDomain::Agent);
        breakdown.record(&MemoryDomain::Agent);
        breakdown.record(&MemoryDomain::User);
        breakdown.record(&MemoryDomain::Organization);

        assert_eq!(breakdown.agent, 2);
        assert_eq!(breakdown.user, 1);
        assert_eq!(breakdown.organization, 1);

        assert_eq!(breakdown.total(), 4);
        assert_eq!(breakdown.local_total(), 3);
        assert_eq!(breakdown.shared_total(), 1);
    }

    #[test]
    fn test_memory_aggregate_record_and_merge() {
        let mut agg1 = MemoryAggregate::default();
        let u1 = MemoryUnit::new(
            None,
            "user1".into(),
            Some("agent1".into()),
            Uuid::new_v4(),
            memorose_common::MemoryType::Procedural,
            "test".into(),
            None,
        );
        agg1.record_unit(&u1);

        let mut u2 = MemoryUnit::new(
            Some("org1".into()),
            "user1".into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "test2".into(),
            None,
        );
        u2.domain = MemoryDomain::Organization;
        u2.level = 2;
        agg1.record_unit(&u2);

        let mut agg2 = MemoryAggregate::default();
        let mut u3 = MemoryUnit::new(
            None,
            "user1".into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "test3".into(),
            None,
        );
        u3.level = 3;
        agg2.record_unit(&u3);

        agg1.merge(&agg2);

        assert_eq!(agg1.by_domain.agent, 1);
        assert_eq!(agg1.by_domain.organization, 1);
        assert_eq!(agg1.by_domain.user, 1);

        assert_eq!(agg1.total_memories(), 3);
        assert_eq!(agg1.local_memories(), 2);
        assert_eq!(agg1.shared_memories(), 1);

        assert_eq!(agg1.total_l1(), 1);
        assert_eq!(agg1.total_l2(), 1);
        assert_eq!(agg1.total_l3(), 1);
    }

    #[test]
    fn test_matches_dashboard_org_scope_treats_default_as_null_or_default() {
        assert!(matches_dashboard_org_scope(None, None));
        assert!(matches_dashboard_org_scope(None, Some("default")));
        assert!(matches_dashboard_org_scope(
            Some("default"),
            Some("default")
        ));
        assert!(!matches_dashboard_org_scope(Some("org-a"), Some("default")));
        assert!(matches_dashboard_org_scope(Some("org-a"), Some("org-a")));
        assert!(!matches_dashboard_org_scope(None, Some("org-a")));
    }

    #[test]
    fn test_update_last_activity() {
        let mut last_activity = None;
        update_last_activity(&mut last_activity, 100);
        assert_eq!(last_activity, Some(100));

        update_last_activity(&mut last_activity, 50);
        assert_eq!(last_activity, Some(100)); // Should not update if older

        update_last_activity(&mut last_activity, 150);
        assert_eq!(last_activity, Some(150)); // Should update if newer
    }

    #[test]
    fn test_validate_registry_id() {
        assert!(validate_registry_id("valid-id", "field_name").is_ok());
        assert!(validate_registry_id("  valid-id  ", "field_name").is_ok());
        
        let err = validate_registry_id("", "field_name").unwrap_err();
        assert_eq!(err.status(), axum::http::StatusCode::BAD_REQUEST);
        
        let err = validate_registry_id("   ", "field_name").unwrap_err();
        assert_eq!(err.status(), axum::http::StatusCode::BAD_REQUEST);
    }
}
