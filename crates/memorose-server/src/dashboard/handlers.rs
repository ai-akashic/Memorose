use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use memorose_common::{
    Asset, Event as MemoryEvent, EventContent, MemoryDomain, MemoryType, MemoryUnit,
};
use memorose_core::engine::{
    OrganizationAutomationCounterSnapshot, OrganizationKnowledgeContributionRecord,
    OrganizationKnowledgeContributionStatus, OrganizationKnowledgeDetailRecord,
    OrganizationKnowledgeMembershipEntry,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Clone, Default, serde::Serialize)]
struct DomainBreakdown {
    agent: usize,
    user: usize,
    organization: usize,
}

impl DomainBreakdown {
    fn record(&mut self, domain: &MemoryDomain) {
        match domain {
            MemoryDomain::Agent => self.agent += 1,
            MemoryDomain::User => self.user += 1,
            MemoryDomain::Organization => self.organization += 1,
        }
    }

    fn total(&self) -> usize {
        self.agent + self.user + self.organization
    }

    fn local_total(&self) -> usize {
        self.agent + self.user
    }

    fn shared_total(&self) -> usize {
        self.organization
    }
}

#[derive(Clone, Default, serde::Serialize)]
struct LevelBreakdown {
    l1: usize,
    l2: usize,
}

#[derive(Clone, Default)]
struct MemoryAggregate {
    by_domain: DomainBreakdown,
    local_levels: LevelBreakdown,
    shared_levels: LevelBreakdown,
}

impl MemoryAggregate {
    fn record_unit(&mut self, unit: &MemoryUnit) {
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
            _ => {}
        }
    }

    fn merge(&mut self, other: &Self) {
        self.by_domain.agent += other.by_domain.agent;
        self.by_domain.user += other.by_domain.user;
        self.by_domain.organization += other.by_domain.organization;
        self.local_levels.l1 += other.local_levels.l1;
        self.local_levels.l2 += other.local_levels.l2;
        self.shared_levels.l1 += other.shared_levels.l1;
        self.shared_levels.l2 += other.shared_levels.l2;
    }

    fn total_memories(&self) -> usize {
        self.by_domain.total()
    }

    fn local_memories(&self) -> usize {
        self.by_domain.local_total()
    }

    fn shared_memories(&self) -> usize {
        self.by_domain.shared_total()
    }

    fn total_l1(&self) -> usize {
        self.local_levels.l1 + self.shared_levels.l1
    }

    fn total_l2(&self) -> usize {
        self.local_levels.l2 + self.shared_levels.l2
    }
}

fn is_local_domain(domain: &MemoryDomain) -> bool {
    matches!(domain, MemoryDomain::Agent | MemoryDomain::User)
}

fn update_last_activity(last_activity: &mut Option<i64>, timestamp: i64) {
    if last_activity.is_none() || *last_activity < Some(timestamp) {
        *last_activity = Some(timestamp);
    }
}

fn validate_registry_id(value: &str, field: &str) -> Result<(), axum::response::Response> {
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

// ── Auth ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct LoginRequest {
    username: String,
    password: String,
}

pub async fn login(
    State(state): State<Arc<crate::AppState>>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<LoginRequest>,
) -> axum::response::Response {
    let client_ip = headers
        .get("x-forwarded-for")
        .or_else(|| headers.get("x-real-ip"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .split(',')
        .next()
        .unwrap_or("unknown")
        .trim()
        .to_string();

    let attempts = state.login_limiter.get(&client_ip).await.unwrap_or(0);
    if attempts >= 5 {
        return (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({ "error": "Too many login attempts. Try again later." })),
        )
            .into_response();
    }

    let username = payload.username.clone();

    let _lock = state.dashboard_auth.file_lock.lock().await;

    let auth_path = state.dashboard_auth.auth_path.clone();
    let password = payload.password.clone();
    let u = username.clone();
    let dummy_hash = state.dashboard_auth.dummy_hash.clone();
    let verify_result = tokio::task::spawn_blocking(move || -> anyhow::Result<(bool, bool)> {
        let data = std::fs::read_to_string(&auth_path)?;
        let auth_data: super::auth::AuthData = serde_json::from_str(&data)?;
        let allow_default_admin = auth_data.username == "admin"
            && u == "admin"
            && password == "admin"
            && auth_data.must_change_password;

        let hash_to_check = if auth_data.username == u {
            auth_data.password_hash.clone()
        } else {
            dummy_hash
        };
        let valid = bcrypt::verify(&password, &hash_to_check).unwrap_or(false);
        let is_valid = allow_default_admin || (valid && auth_data.username == u);
        Ok((is_valid, auth_data.must_change_password))
    })
    .await;

    match verify_result {
        Ok(Ok((true, must_change))) => {
            state.login_limiter.invalidate(&client_ip).await;
            match state.dashboard_auth.create_token(&username) {
                Ok(token) => Json(serde_json::json!({
                    "token": token,
                    "expires_in": 86400,
                    "must_change_password": must_change,
                }))
                .into_response(),
                Err(e) => {
                    tracing::error!("Token creation failed: {}", e);
                    (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": "Internal server error" })),
                    )
                        .into_response()
                }
            }
        }
        Ok(Ok((false, _))) => {
            state.login_limiter.insert(client_ip, attempts + 1).await;
            (
                axum::http::StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Invalid credentials" })),
            )
                .into_response()
        }
        Ok(Err(e)) => {
            tracing::error!("Auth error: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Internal server error" })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Auth task error: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Internal server error" })),
            )
                .into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct ChangePasswordRequest {
    current_password: String,
    new_password: String,
}

pub async fn change_password(
    State(state): State<Arc<crate::AppState>>,
    Json(payload): Json<ChangePasswordRequest>,
) -> axum::response::Response {
    let auth_path = state.dashboard_auth.auth_path.clone();
    let current = payload.current_password.clone();
    let new_pw = payload.new_password.clone();
    let _lock = state.dashboard_auth.file_lock.lock().await;

    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<bool> {
        let data = std::fs::read_to_string(&auth_path)?;
        let auth_data: super::auth::AuthData = serde_json::from_str(&data)?;

        if !bcrypt::verify(&current, &auth_data.password_hash)? {
            return Ok(false);
        }

        if new_pw.len() < 8 {
            anyhow::bail!("Password must be at least 8 characters");
        }

        let new_hash = bcrypt::hash(&new_pw, bcrypt::DEFAULT_COST)?;
        let new_auth = serde_json::json!({
            "username": auth_data.username,
            "password_hash": new_hash,
            "must_change_password": false,
        });
        let json = serde_json::to_string_pretty(&new_auth)?;
        std::fs::write(&auth_path, json)?;
        Ok(true)
    })
    .await;

    match result {
        Ok(Ok(true)) => Json(serde_json::json!({ "status": "updated" })).into_response(),
        Ok(Ok(false)) => (
            axum::http::StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Current password is incorrect" })),
        )
            .into_response(),
        Ok(Err(e)) => {
            let msg = e.to_string();
            if msg.contains("at least") {
                (
                    axum::http::StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": msg })),
                )
                    .into_response()
            } else {
                tracing::error!("Password change error: {}", e);
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": "Internal server error" })),
                )
                    .into_response()
            }
        }
        Err(e) => {
            tracing::error!("Password change task error: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Internal server error" })),
            )
                .into_response()
        }
    }
}

// ── Organizations / API Keys ─────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateOrganizationRequest {
    org_id: String,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Deserialize)]
pub struct CreateApiKeyRequest {
    org_id: String,
    #[serde(default)]
    name: Option<String>,
}

pub async fn list_organizations(
    State(state): State<Arc<crate::AppState>>,
) -> axum::response::Response {
    match state.management_registry.list_organizations().await {
        Ok(organizations) => Json(serde_json::json!({
            "organizations": organizations,
            "total_count": organizations.len(),
        }))
        .into_response(),
        Err(error) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

pub async fn create_organization(
    State(state): State<Arc<crate::AppState>>,
    Json(payload): Json<CreateOrganizationRequest>,
) -> axum::response::Response {
    if let Err(response) = validate_registry_id(&payload.org_id, "org_id") {
        return response;
    }

    match state
        .management_registry
        .create_organization(payload.org_id.trim(), payload.name)
        .await
    {
        Ok(record) => Json(record).into_response(),
        Err(error) if error.to_string().contains("already exists") => (
            axum::http::StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

pub async fn list_api_keys(State(state): State<Arc<crate::AppState>>) -> axum::response::Response {
    match state.management_registry.list_api_keys().await {
        Ok(api_keys) => Json(serde_json::json!({
            "api_keys": api_keys,
            "total_count": api_keys.len(),
        }))
        .into_response(),
        Err(error) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

pub async fn create_api_key(
    State(state): State<Arc<crate::AppState>>,
    Json(payload): Json<CreateApiKeyRequest>,
) -> axum::response::Response {
    if let Err(response) = validate_registry_id(&payload.org_id, "org_id") {
        return response;
    }

    match state
        .management_registry
        .create_api_key(payload.org_id.trim(), payload.name)
        .await
    {
        Ok(record) => Json(record).into_response(),
        Err(error) if error.to_string().contains("organization does not exist") => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

pub async fn revoke_api_key(
    State(state): State<Arc<crate::AppState>>,
    Path(key_id): Path<String>,
) -> axum::response::Response {
    if let Err(response) = validate_registry_id(&key_id, "key_id") {
        return response;
    }

    match state
        .management_registry
        .revoke_api_key(key_id.trim())
        .await
    {
        Ok(Some(record)) => Json(record).into_response(),
        Ok(None) => (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "API key not found" })),
        )
            .into_response(),
        Err(error) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

#[derive(Serialize)]
struct OrganizationKnowledgeListSummary {
    knowledge_count: usize,
    contribution_count: usize,
    membership_count: usize,
    contributor_count: usize,
}

#[derive(Serialize)]
struct OrganizationKnowledgeListResponse {
    items: Vec<DashboardOrganizationKnowledgeListItemView>,
    total_count: usize,
    summary: OrganizationKnowledgeListSummary,
}

#[derive(Clone, Serialize)]
struct DashboardAssetView {
    storage_key: String,
    original_name: String,
    asset_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
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
struct DashboardMemoryDetailUnitView {
    id: uuid::Uuid,
    org_id: Option<String>,
    user_id: String,
    content: String,
    keywords: Vec<String>,
    importance: f32,
    level: u8,
    transaction_time: chrono::DateTime<chrono::Utc>,
    assets: Vec<DashboardAssetView>,
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
struct DashboardMemoryDetailResponse {
    #[serde(flatten)]
    unit: DashboardMemoryDetailUnitView,
    #[serde(skip_serializing_if = "Option::is_none")]
    organization_knowledge: Option<DashboardOrganizationKnowledgeView>,
}

#[derive(Clone, Serialize)]
struct DashboardSearchMemoryUnitView {
    id: uuid::Uuid,
    memory_type: MemoryType,
    content: String,
    keywords: Vec<String>,
    level: u8,
    assets: Vec<DashboardAssetView>,
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
struct DashboardOrganizationKnowledgeUnitView {
    id: uuid::Uuid,
    content: String,
    keywords: Vec<String>,
    transaction_time: chrono::DateTime<chrono::Utc>,
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
struct DashboardOrganizationContributionView {
    source_id: uuid::Uuid,
    contributor_user_id: String,
    status: String,
    source_memory_type: Option<String>,
    source_level: Option<u8>,
    source_keywords: Vec<String>,
    source_content_preview: Option<String>,
    candidate_at: Option<chrono::DateTime<chrono::Utc>>,
    activated_at: Option<chrono::DateTime<chrono::Utc>>,
    approval_mode: Option<String>,
    approved_by: Option<String>,
    revoked_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Serialize)]
struct DashboardOrganizationMembershipView {
    source_id: uuid::Uuid,
    contributor_user_id: String,
    source_memory_type: Option<String>,
    source_level: Option<u8>,
    source_keywords: Vec<String>,
    source_content_preview: Option<String>,
    activated_at: Option<chrono::DateTime<chrono::Utc>>,
    approval_mode: Option<String>,
    approved_by: Option<String>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Serialize)]
struct DashboardOrganizationMembershipContributorSummaryView {
    contributor_user_id: String,
    membership_count: usize,
    source_ids: Vec<uuid::Uuid>,
    source_memory_types: Vec<String>,
}

#[derive(Clone, Serialize)]
struct DashboardOrganizationMembershipSourceTypeSummaryView {
    source_memory_type: String,
    membership_count: usize,
    contributor_user_ids: Vec<String>,
}

#[derive(Clone, Serialize)]
struct DashboardOrganizationKnowledgeMembershipSummaryView {
    contributors: Vec<DashboardOrganizationMembershipContributorSummaryView>,
    source_types: Vec<DashboardOrganizationMembershipSourceTypeSummaryView>,
}

#[derive(Clone, Serialize)]
struct DashboardOrganizationKnowledgeMembershipStateView {
    membership_count: usize,
    summary: DashboardOrganizationKnowledgeMembershipSummaryView,
    memberships: Vec<DashboardOrganizationMembershipView>,
}

#[derive(Clone, Serialize)]
struct DashboardOrganizationHistoryContributorSummaryView {
    contributor_user_id: String,
    contribution_count: usize,
    candidate_contribution_count: usize,
    active_contribution_count: usize,
    revoked_contribution_count: usize,
    source_ids: Vec<uuid::Uuid>,
    source_memory_types: Vec<String>,
}

#[derive(Clone, Serialize)]
struct DashboardOrganizationHistorySourceTypeSummaryView {
    source_memory_type: String,
    contribution_count: usize,
    candidate_contribution_count: usize,
    active_contribution_count: usize,
    revoked_contribution_count: usize,
    contributor_user_ids: Vec<String>,
}

#[derive(Clone, Serialize)]
struct DashboardOrganizationKnowledgeHistorySummaryView {
    contributors: Vec<DashboardOrganizationHistoryContributorSummaryView>,
    source_types: Vec<DashboardOrganizationHistorySourceTypeSummaryView>,
}

#[derive(Clone, Serialize)]
struct DashboardOrganizationKnowledgeHistoryView {
    contribution_count: usize,
    candidate_contribution_count: usize,
    active_contribution_count: usize,
    revoked_contribution_count: usize,
    summary: DashboardOrganizationKnowledgeHistorySummaryView,
    contributions: Vec<DashboardOrganizationContributionView>,
}

#[derive(Clone, Serialize)]
struct DashboardOrganizationKnowledgeView {
    membership: DashboardOrganizationKnowledgeMembershipStateView,
    history: DashboardOrganizationKnowledgeHistoryView,
}

#[derive(Clone, Serialize)]
struct DashboardOrganizationKnowledgeListItemView {
    unit: DashboardOrganizationKnowledgeUnitView,
    contribution_count: usize,
    membership_count: usize,
    contributor_user_ids: Vec<String>,
    top_contributor_user_id: Option<String>,
    source_memory_types: Vec<String>,
    primary_source_memory_type: Option<String>,
    published_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
struct DashboardOrganizationKnowledgeDetailView {
    unit: DashboardOrganizationKnowledgeUnitView,
    knowledge: DashboardOrganizationKnowledgeView,
}

#[derive(Clone, Serialize)]
struct DashboardOrganizationAutomationMetricCountView {
    key: String,
    value: usize,
}

#[derive(Serialize)]
struct DashboardOrganizationAutomationMetricsView {
    org_id: String,
    knowledge_count: usize,
    contribution_count: usize,
    membership_count: usize,
    candidate_contribution_count: usize,
    revoked_contribution_count: usize,
    contributor_count: usize,
    auto_approved_total: usize,
    auto_publish_total: usize,
    rebuild_total: usize,
    revoke_total: usize,
    merged_publication_total: usize,
    source_type_distribution: Vec<DashboardOrganizationAutomationMetricCountView>,
}

struct DashboardOrganizationKnowledgeRollup {
    contribution_count: usize,
    candidate_contribution_count: usize,
    revoked_contribution_count: usize,
    membership_count: usize,
    contributor_user_ids: Vec<String>,
    top_contributor_user_id: Option<String>,
    source_memory_types: Vec<String>,
    primary_source_memory_type: Option<String>,
    source_type_distribution: Vec<(String, usize)>,
    published_at: chrono::DateTime<chrono::Utc>,
}

fn dashboard_organization_rollup_from_detail(
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
    fn from_detail_records(
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

#[derive(Deserialize)]
pub struct OrganizationKnowledgeListQuery {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    contributor: Option<String>,
    #[serde(default)]
    source_type: Option<String>,
    #[serde(default = "default_organization_knowledge_sort")]
    sort: String,
}

fn default_organization_knowledge_sort() -> String {
    "published_desc".to_string()
}

pub async fn list_organization_knowledge(
    State(state): State<Arc<crate::AppState>>,
    Path(org_id): Path<String>,
    Query(params): Query<OrganizationKnowledgeListQuery>,
) -> axum::response::Response {
    let mut details = Vec::new();

    for (_, shard) in state.shard_manager.all_shards() {
        match shard
            .engine
            .list_organization_knowledge_detail_records(Some(org_id.as_str()))
            .await
        {
            Ok(mut shard_items) => details.append(&mut shard_items),
            Err(error) => {
                tracing::error!("List organization knowledge error: {}", error);
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": error.to_string() })),
                )
                    .into_response();
            }
        }
    }

    let query_text = params.q.as_deref().map(str::trim).unwrap_or_default();
    let query_text = query_text.to_ascii_lowercase();
    let contributor_filter = params
        .contributor
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let source_type_filter = params
        .source_type
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase();

    let mut items = details
        .iter()
        .map(dashboard_organization_list_item_from_detail)
        .collect::<Vec<_>>();

    items.retain(|list_item| {
        let matches_query = if query_text.is_empty() {
            true
        } else {
            list_item
                .unit
                .content
                .to_ascii_lowercase()
                .contains(&query_text)
                || list_item
                    .unit
                    .keywords
                    .iter()
                    .any(|keyword| keyword.to_ascii_lowercase().contains(&query_text))
        };
        let matches_contributor = if contributor_filter.is_empty() {
            true
        } else {
            list_item
                .contributor_user_ids
                .iter()
                .any(|user_id| user_id.to_ascii_lowercase().contains(&contributor_filter))
        };
        let matches_source_type = if source_type_filter.is_empty() {
            true
        } else {
            list_item
                .source_memory_types
                .iter()
                .any(|source_type| source_type.to_ascii_lowercase() == source_type_filter)
        };

        matches_query && matches_contributor && matches_source_type
    });

    match params.sort.as_str() {
        "contributions_desc" => items.sort_by(|left, right| {
            right
                .contribution_count
                .cmp(&left.contribution_count)
                .then_with(|| right.membership_count.cmp(&left.membership_count))
                .then_with(|| right.published_at.cmp(&left.published_at))
                .then_with(|| left.unit.id.cmp(&right.unit.id))
        }),
        "active_desc" => items.sort_by(|left, right| {
            right
                .membership_count
                .cmp(&left.membership_count)
                .then_with(|| right.contribution_count.cmp(&left.contribution_count))
                .then_with(|| right.published_at.cmp(&left.published_at))
                .then_with(|| left.unit.id.cmp(&right.unit.id))
        }),
        "topic_asc" => items.sort_by(|left, right| {
            let left_topic = left
                .unit
                .keywords
                .first()
                .cloned()
                .unwrap_or_else(|| left.unit.content.clone());
            let right_topic = right
                .unit
                .keywords
                .first()
                .cloned()
                .unwrap_or_else(|| right.unit.content.clone());
            left_topic
                .cmp(&right_topic)
                .then_with(|| right.published_at.cmp(&left.published_at))
                .then_with(|| left.unit.id.cmp(&right.unit.id))
        }),
        _ => items.sort_by(|left, right| {
            right
                .published_at
                .cmp(&left.published_at)
                .then_with(|| left.unit.id.cmp(&right.unit.id))
        }),
    }

    let contribution_count = items.iter().map(|item| item.contribution_count).sum();
    let membership_count = items.iter().map(|item| item.membership_count).sum();
    let mut contributor_user_ids = std::collections::BTreeSet::new();
    for item in &items {
        for user_id in &item.contributor_user_ids {
            contributor_user_ids.insert(user_id.clone());
        }
    }

    let knowledge_count = items.len();

    Json(OrganizationKnowledgeListResponse {
        total_count: knowledge_count,
        items,
        summary: OrganizationKnowledgeListSummary {
            knowledge_count,
            contribution_count,
            membership_count,
            contributor_count: contributor_user_ids.len(),
        },
    })
    .into_response()
}

pub async fn get_organization_knowledge(
    State(state): State<Arc<crate::AppState>>,
    Path((org_id, id)): Path<(String, String)>,
) -> axum::response::Response {
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "Invalid organization knowledge ID format" })),
            )
                .into_response()
        }
    };

    for (_, shard) in state.shard_manager.all_shards() {
        match shard
            .engine
            .get_organization_knowledge_detail_record(uuid)
            .await
        {
            Ok(Some(mut detail)) => {
                if detail.read_view.org_id.as_deref() != Some(org_id.as_str()) {
                    continue;
                }
                detail.read_view.embedding = None;
                detail.read_view.user_id.clear();
                detail.read_view.agent_id = None;
                return Json(DashboardOrganizationKnowledgeDetailView {
                    unit: DashboardOrganizationKnowledgeUnitView::from(&detail.read_view),
                    knowledge: dashboard_organization_knowledge_view_from_detail(&detail),
                })
                .into_response();
            }
            Ok(None) => continue,
            Err(error) => {
                tracing::error!("Get organization knowledge error: {}", error);
                continue;
            }
        }
    }

    (
        axum::http::StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": "Organization knowledge not found" })),
    )
        .into_response()
}

pub async fn get_organization_knowledge_metrics(
    State(state): State<Arc<crate::AppState>>,
    Path(org_id): Path<String>,
) -> axum::response::Response {
    for (_, shard) in state.shard_manager.all_shards() {
        let details = match shard
            .engine
            .list_organization_knowledge_detail_records(Some(org_id.as_str()))
            .await
        {
            Ok(details) => details,
            Err(error) => {
                tracing::error!("Get organization automation metrics error: {}", error);
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": error.to_string() })),
                )
                    .into_response();
            }
        };
        let counters = match shard
            .engine
            .get_organization_automation_counter_snapshot(&org_id)
        {
            Ok(counters) => counters,
            Err(error) => {
                tracing::error!("Get organization automation counters error: {}", error);
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": error.to_string() })),
                )
                    .into_response();
            }
        };
        return Json(
            DashboardOrganizationAutomationMetricsView::from_detail_records(
                &org_id, &details, counters,
            ),
        )
        .into_response();
    }

    Json(DashboardOrganizationAutomationMetricsView {
        org_id,
        knowledge_count: 0,
        contribution_count: 0,
        membership_count: 0,
        candidate_contribution_count: 0,
        revoked_contribution_count: 0,
        contributor_count: 0,
        auto_approved_total: 0,
        auto_publish_total: 0,
        rebuild_total: 0,
        revoke_total: 0,
        merged_publication_total: 0,
        source_type_distribution: Vec::new(),
    })
    .into_response()
}

// ── Cluster Status ────────────────────────────────────────────────

pub async fn cluster_status(State(state): State<Arc<crate::AppState>>) -> Json<serde_json::Value> {
    let mut shard_statuses = Vec::new();

    for (shard_id, shard) in state.shard_manager.all_shards() {
        let metrics = shard.raft.metrics().borrow().clone();

        let raft_state = if metrics.current_leader == Some(metrics.id) {
            "Leader"
        } else if metrics.current_leader.is_some() {
            "Follower"
        } else {
            "Candidate"
        };

        let last_log_index = metrics.last_log_index.unwrap_or_default();
        let last_applied = metrics.last_applied.map(|l| l.index).unwrap_or_default();

        let voters: Vec<u64> = metrics.membership_config.membership().voter_ids().collect();
        let learners: Vec<u64> = metrics
            .membership_config
            .membership()
            .learner_ids()
            .collect();

        shard_statuses.push(serde_json::json!({
            "shard_id": shard_id,
            "raft_state": raft_state,
            "current_leader": metrics.current_leader,
            "current_term": metrics.current_term,
            "last_log_index": last_log_index,
            "last_applied": last_applied,
            "replication_lag": last_log_index.saturating_sub(last_applied),
            "voters": voters,
            "learners": learners,
        }));
    }

    // Sort by shard_id for deterministic output
    shard_statuses.sort_by_key(|s| s["shard_id"].as_u64().unwrap_or(0));

    // Keep the single-shard payload flat so the dashboard can render either topology shape.
    if state.shard_manager.shard_count() <= 1 {
        if let Some(first) = shard_statuses.first() {
            let mut result = first.clone();
            result["node_id"] = serde_json::json!(state.shard_manager.physical_node_id());
            result["snapshot_policy_logs"] = serde_json::json!(state.config.raft.snapshot_logs);
            result["config"] = serde_json::json!({
                "heartbeat_interval_ms": state.config.raft.heartbeat_interval_ms,
                "election_timeout_min_ms": state.config.raft.election_timeout_min_ms,
            });
            return Json(result);
        }
    }

    Json(serde_json::json!({
        "physical_node_id": state.shard_manager.physical_node_id(),
        "shard_count": state.shard_manager.shard_count(),
        "shards": shard_statuses,
        "config": {
            "heartbeat_interval_ms": state.config.raft.heartbeat_interval_ms,
            "election_timeout_min_ms": state.config.raft.election_timeout_min_ms,
        }
    }))
}

// ── Stats ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct StatsQuery {
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
}

pub async fn stats(
    State(state): State<Arc<crate::AppState>>,
    Query(params): Query<StatsQuery>,
) -> axum::response::Response {
    let cache_key = format!(
        "stats:{}:{}",
        params.org_id.as_deref().unwrap_or("_all"),
        params.user_id.as_deref().unwrap_or("_all")
    );
    if let Some(cached) = state.dashboard_cache.get(&cache_key).await {
        return Json(cached).into_response();
    }

    let user_id_filter = params.user_id.clone();

    // Determine which shards to scan
    let shard_ids: Vec<u32> = if let Some(ref uid) = user_id_filter {
        // Single shard for known user
        let sid =
            memorose_common::sharding::user_id_to_shard(uid, state.shard_manager.shard_count());
        vec![sid]
    } else {
        // All shards
        state.shard_manager.all_shards().map(|(id, _)| id).collect()
    };

    let mut total_pending = 0usize;
    let mut total_events = 0usize;
    let mut total_edges = 0usize;
    let mut total_memory = MemoryAggregate::default();

    for shard_id in shard_ids {
        let shard = match state.shard_manager.shard(shard_id) {
            Some(s) => s,
            None => continue,
        };
        let engine = shard.engine.clone();
        let uid_filter = user_id_filter.clone();

        let edge_count = if let Some(ref uid) = uid_filter {
            match engine.graph().get_all_edges_for_user(uid).await {
                Ok(edges) => edges.len(),
                Err(e) => {
                    tracing::warn!("Failed to load graph edges for user {}: {:?}", uid, e);
                    0
                }
            }
        } else {
            match engine.graph().scan_all_edges().await {
                Ok(edges) => edges.len(),
                Err(e) => {
                    tracing::warn!("Failed to scan graph edges: {:?}", e);
                    0
                }
            }
        };

        let uid_filter = user_id_filter.clone();
        let org_filter = params.org_id.clone();
        let org_filter_for_shared = params.org_id.clone();

        let scan_result = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let kv = engine.kv();

            let pending_count = engine.system_kv().scan(b"pending:")?.len();

            let (event_count, memory) = if let Some(ref uid) = uid_filter {
                let event_prefix = format!("u:{}:event:", uid);
                let event_count = kv
                    .scan(event_prefix.as_bytes())?
                    .into_iter()
                    .filter(|(_, val)| {
                        if let Ok(event) = serde_json::from_slice::<MemoryEvent>(val) {
                            org_filter.is_none() || event.org_id == org_filter
                        } else {
                            false
                        }
                    })
                    .count();

                let unit_prefix = format!("u:{}:unit:", uid);
                let unit_pairs = kv.scan(unit_prefix.as_bytes())?;
                let mut memory = MemoryAggregate::default();
                for (_, val) in &unit_pairs {
                    if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(val) {
                        if unit.domain != MemoryDomain::Organization
                            && (org_filter.is_none() || unit.org_id == org_filter)
                        {
                            memory.record_unit(&unit);
                        }
                    }
                }
                (event_count, memory)
            } else {
                let all_pairs = kv.scan(b"u:")?;
                tracing::debug!(
                    "Scanning all pairs: found {} keys starting with 'u:'",
                    all_pairs.len()
                );
                let mut event_count = 0usize;
                let mut memory = MemoryAggregate::default();
                for (k, val) in &all_pairs {
                    if k.windows(7).any(|w| w == b":event:") {
                        if let Ok(event) = serde_json::from_slice::<MemoryEvent>(val) {
                            if org_filter.is_none() || event.org_id == org_filter {
                                event_count += 1;
                            }
                        }
                    } else if k.windows(6).any(|w| w == b":unit:") {
                        if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(val) {
                            if unit.domain != MemoryDomain::Organization
                                && (org_filter.is_none() || unit.org_id == org_filter)
                            {
                                memory.record_unit(&unit);
                            }
                        }
                    }
                }
                tracing::debug!(
                    "Scan results: events={}, units={}, local={}, shared={}",
                    event_count,
                    memory.total_memories(),
                    memory.local_memories(),
                    memory.shared_memories()
                );
                (event_count, memory)
            };

            Ok((pending_count, event_count, memory))
        })
        .await;

        if let Ok(Ok((pending, events, memory))) = scan_result {
            total_pending += pending;
            total_events += events;
            total_edges += edge_count;
            total_memory.merge(&memory);

            if user_id_filter.is_none() {
                if let Ok(shared_units) = shard
                    .engine
                    .list_organization_read_units(org_filter_for_shared.as_deref())
                    .await
                {
                    for unit in shared_units {
                        total_memory.record_unit(&unit);
                    }
                }
            }
        }
    }

    let uptime = state.start_time.elapsed().as_secs();
    let memory_by_domain = total_memory.by_domain.clone();
    let local_levels = total_memory.local_levels.clone();
    let shared_levels = total_memory.shared_levels.clone();

    let result = serde_json::json!({
        "total_events": total_events,
        "pending_events": total_pending,
        "total_memory_units": total_memory.total_memories(),
        "total_edges": total_edges,
        "memory_by_level": {
            "l1": total_memory.total_l1(),
            "l2": total_memory.total_l2(),
        },
        "memory_by_scope": {
            "local": total_memory.local_memories(),
            "shared": total_memory.shared_memories(),
        },
        "memory_by_domain": memory_by_domain,
        "memory_by_level_and_scope": {
            "local": local_levels,
            "shared": shared_levels,
        },
        "uptime_seconds": uptime,
    });

    state
        .dashboard_cache
        .insert(cache_key, result.clone())
        .await;

    Json(result).into_response()
}

// ── Memories ──────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct ListMemoriesQuery {
    level: Option<u8>,
    #[serde(default = "default_page")]
    page: usize,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default = "default_sort")]
    sort: String,
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
}

fn default_page() -> usize {
    1
}
fn default_limit() -> usize {
    20
}
fn default_sort() -> String {
    "importance".to_string()
}

#[derive(Clone)]
struct DashboardMemoryRow {
    id: String,
    user_id: String,
    agent_id: Option<String>,
    content: String,
    level: u8,
    importance: f32,
    keywords: Vec<String>,
    access_count: u64,
    transaction_time: chrono::DateTime<chrono::Utc>,
    reference_count: usize,
    item_type: &'static str,
    memory_type: Option<String>,
}

#[derive(Serialize)]
struct DashboardMemoryListItemView {
    id: String,
    user_id: String,
    agent_id: Option<String>,
    content: String,
    level: u8,
    importance: f32,
    keywords: Vec<String>,
    access_count: u64,
    reference_count: usize,
    item_type: &'static str,
    memory_type: Option<String>,
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
struct DashboardMemoryListResponse {
    items: Vec<DashboardMemoryListItemView>,
    total: usize,
    page: usize,
    limit: usize,
}

fn display_identity_for_memory(unit: &MemoryUnit) -> (String, Option<String>) {
    if unit.domain == MemoryDomain::Organization {
        (String::new(), None)
    } else {
        (unit.user_id.clone(), unit.agent_id.clone())
    }
}

fn event_content_preview(content: &EventContent) -> (String, bool) {
    match content {
        EventContent::Text(text) => (text.clone(), false),
        EventContent::Image(url) => (format!("[Image] {}", url), true),
        EventContent::Audio(url) => (format!("[Audio] {}", url), true),
        EventContent::Video(url) => (format!("[Video] {}", url), true),
        EventContent::Json(value) => (value.to_string(), false),
    }
}

fn public_asset_storage_key(asset: &Asset) -> String {
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

fn asset_kind_label(asset_type: &str) -> &'static str {
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

fn asset_source_reference(asset: &Asset) -> String {
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

fn format_asset_context(asset: &Asset) -> String {
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

fn format_memory_unit_context(unit: &MemoryUnit) -> String {
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

fn append_context_with_budget(context_text: &mut String, block: &str, budget_chars: usize) -> bool {
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

fn dashboard_memory_detail_view(
    unit: &MemoryUnit,
    organization_knowledge: Option<DashboardOrganizationKnowledgeView>,
) -> DashboardMemoryDetailResponse {
    DashboardMemoryDetailResponse {
        unit: DashboardMemoryDetailUnitView::from(unit),
        organization_knowledge,
    }
}

fn dashboard_build_content_preview(text: &str, max_chars: usize) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        return normalized;
    }
    let truncated = normalized.chars().take(max_chars).collect::<String>();
    format!("{}...", truncated.trim_end())
}

fn dashboard_memory_type_label(memory_type: &MemoryType) -> String {
    match memory_type {
        MemoryType::Factual => "factual".to_string(),
        MemoryType::Procedural => "procedural".to_string(),
    }
}

fn dashboard_contribution_status_label(
    status: &OrganizationKnowledgeContributionStatus,
) -> &'static str {
    match status {
        OrganizationKnowledgeContributionStatus::Candidate => "candidate",
        OrganizationKnowledgeContributionStatus::Active => "active",
        OrganizationKnowledgeContributionStatus::Revoked => "revoked",
    }
}

fn dashboard_approval_mode_label(mode: &impl Serialize) -> String {
    serde_json::to_string(mode)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

fn dashboard_organization_contribution_view_from_record(
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

fn dashboard_organization_membership_view_from_entry(
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

fn dashboard_organization_membership_summary(
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

fn dashboard_organization_contribution_counts(
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

fn dashboard_organization_history_summary(
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

fn dashboard_organization_knowledge_view_from_detail(
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

fn dashboard_organization_list_item_from_detail(
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

pub async fn list_memories(
    State(state): State<Arc<crate::AppState>>,
    Query(params): Query<ListMemoriesQuery>,
) -> axum::response::Response {
    let level_filter = params.level;
    let sort = params.sort.clone();
    let org_id_filter = params.org_id.clone();
    let user_id_filter = params.user_id.clone();
    let agent_id_filter = params.agent_id.clone();
    let include_events = level_filter.map_or(true, |l| l == 0);
    let include_units = level_filter.map_or(true, |l| l > 0);

    // Determine which shards to scan
    let shard_ids: Vec<u32> = if let Some(ref uid) = user_id_filter {
        let sid =
            memorose_common::sharding::user_id_to_shard(uid, state.shard_manager.shard_count());
        vec![sid]
    } else {
        state.shard_manager.all_shards().map(|(id, _)| id).collect()
    };

    let mut rows: Vec<DashboardMemoryRow> = Vec::new();

    for shard_id in shard_ids {
        let shard = match state.shard_manager.shard(shard_id) {
            Some(s) => s,
            None => continue,
        };
        let engine = shard.engine.clone();

        if include_units {
            let uid_filter = user_id_filter.clone();
            let agent_filter = agent_id_filter.clone();
            let org_filter = org_id_filter.clone();
            let level_filter_for_units = level_filter;
            match engine.list_memory_units_global(uid_filter.as_deref()).await {
                Ok(units) => {
                    let units: Vec<DashboardMemoryRow> = units
                        .into_iter()
                        .filter(|u| level_filter_for_units.map_or(true, |l| u.level == l))
                        .filter(|u| {
                            if let Some(ref aid) = agent_filter {
                                u.agent_id.as_deref() == Some(aid.as_str())
                            } else {
                                true
                            }
                        })
                        .filter(|u| {
                            if let Some(ref oid) = org_filter {
                                u.org_id.as_deref() == Some(oid.as_str())
                            } else {
                                true
                            }
                        })
                        .map(|u| {
                            let memory_type_str = match u.memory_type {
                                memorose_common::MemoryType::Factual => "factual",
                                memorose_common::MemoryType::Procedural => "procedural",
                            }
                            .to_string();
                            let (user_id, agent_id) = display_identity_for_memory(&u);

                            DashboardMemoryRow {
                                id: u.id.to_string(),
                                user_id,
                                agent_id,
                                content: u.content,
                                level: u.level,
                                importance: u.importance,
                                keywords: u.keywords,
                                access_count: u.access_count,
                                transaction_time: u.transaction_time,
                                reference_count: u.references.len(),
                                item_type: "memory",
                                memory_type: Some(memory_type_str),
                            }
                        })
                        .take(10_000)
                        .collect();
                    rows.extend(units);
                }
                Err(error) => {
                    tracing::error!("List memories units error on shard {}: {}", shard_id, error);
                }
            }
        }

        if include_events {
            let engine = shard.engine.clone();
            let uid_filter = user_id_filter.clone();
            let org_filter = org_id_filter.clone();
            let events_result =
                tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<DashboardMemoryRow>> {
                    let kv = engine.kv();
                    let prefix = if let Some(ref uid) = uid_filter {
                        format!("u:{}:event:", uid).into_bytes()
                    } else {
                        b"u:".to_vec()
                    };
                    let pairs = kv.scan(&prefix)?;

                    let events: Vec<DashboardMemoryRow> = pairs
                        .into_iter()
                        .filter(|(k, _)| {
                            if uid_filter.is_none() {
                                k.windows(7).any(|w| w == b":event:")
                            } else {
                                true
                            }
                        })
                        .filter_map(|(_, val)| serde_json::from_slice::<MemoryEvent>(&val).ok())
                        .filter(|e| {
                            if let Some(ref oid) = org_filter {
                                e.org_id.as_deref() == Some(oid.as_str())
                            } else {
                                true
                            }
                        })
                        .map(|event| {
                            let (content, _) = event_content_preview(&event.content);
                            DashboardMemoryRow {
                                id: event.id.to_string(),
                                user_id: event.user_id,
                                agent_id: event.agent_id,
                                content,
                                level: 0,
                                importance: 0.0,
                                keywords: Vec::new(),
                                access_count: 0,
                                transaction_time: event.transaction_time,
                                reference_count: 0,
                                item_type: "event",
                                memory_type: None,
                            }
                        })
                        .take(10_000)
                        .collect();

                    Ok(events)
                })
                .await;

            if let Ok(Ok(events)) = events_result {
                rows.extend(events);
            }
        }
    }

    let total = rows.len();

    match sort.as_str() {
        "importance" => rows.sort_by(|a, b| {
            b.importance
                .partial_cmp(&a.importance)
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
        "access_count" => rows.sort_by(|a, b| b.access_count.cmp(&a.access_count)),
        "recent" => rows.sort_by(|a, b| b.transaction_time.cmp(&a.transaction_time)),
        _ => rows.sort_by(|a, b| {
            b.importance
                .partial_cmp(&a.importance)
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
    }

    let page = params.page.max(1);
    let limit = params.limit.min(100);
    let offset = (page - 1) * limit;

    let items = rows
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(DashboardMemoryListItemView::from)
        .collect();

    Json(DashboardMemoryListResponse {
        items,
        total,
        page,
        limit,
    })
    .into_response()
}

pub async fn get_memory(
    State(state): State<Arc<crate::AppState>>,
    Path(id): Path<String>,
) -> axum::response::Response {
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "Invalid memory ID format" })),
            )
                .into_response()
        }
    };

    // Try all shards (shard count is small, acceptable overhead)
    for (_, shard) in state.shard_manager.all_shards() {
        match shard
            .engine
            .get_organization_knowledge_detail_record(uuid)
            .await
        {
            Ok(Some(mut detail)) => {
                detail.read_view.embedding = None;
                detail.read_view.user_id.clear();
                detail.read_view.agent_id = None;
                return Json(dashboard_memory_detail_view(
                    &detail.read_view,
                    Some(dashboard_organization_knowledge_view_from_detail(&detail)),
                ))
                .into_response();
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!("Get organization knowledge detail error: {}", e);
                continue;
            }
        }

        match shard.engine.get_native_memory_unit_by_index(uuid).await {
            Ok(Some(mut unit)) => {
                unit.embedding = None;
                return Json(dashboard_memory_detail_view(&unit, None)).into_response();
            }
            Ok(None) => continue,
            Err(e) => {
                tracing::error!("Get memory error: {}", e);
                continue;
            }
        }
    }

    (
        axum::http::StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": "Memory not found" })),
    )
        .into_response()
}

// ── Graph ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct GraphQuery {
    #[serde(default = "default_graph_limit")]
    limit: usize,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    org_id: Option<String>,
}

fn default_graph_limit() -> usize {
    500
}

pub async fn graph_data(
    State(state): State<Arc<crate::AppState>>,
    Query(params): Query<GraphQuery>,
) -> axum::response::Response {
    let limit = params.limit.min(1000);
    let user_id_filter = params.user_id.clone();
    let org_id_filter = params.org_id.clone();

    // Determine which shards to scan
    let shard_ids: Vec<u32> = if let Some(ref uid) = user_id_filter {
        let sid =
            memorose_common::sharding::user_id_to_shard(uid, state.shard_manager.shard_count());
        vec![sid]
    } else {
        state.shard_manager.all_shards().map(|(id, _)| id).collect()
    };

    let mut all_nodes = Vec::new();
    let mut all_edge_data = Vec::new();
    let mut all_relation_dist: HashMap<String, usize> = HashMap::new();
    let mut total_edge_count = 0usize;

    for shard_id in shard_ids {
        let shard = match state.shard_manager.shard(shard_id) {
            Some(s) => s,
            None => continue,
        };
        let engine = shard.engine.clone();
        let uid_filter = user_id_filter.clone();
        let org_filter = org_id_filter.clone();

        let result: anyhow::Result<serde_json::Value> = async move {
            let graph = engine.graph();

            let edges = if let Some(ref uid) = uid_filter {
                graph.get_all_edges_for_user(uid).await?
            } else {
                graph.scan_all_edges().await?
            };

            let mut node_ids = std::collections::HashSet::new();
            for edge in &edges {
                node_ids.insert(edge.source_id);
                node_ids.insert(edge.target_id);
            }

            let node_ids_vec: Vec<_> = node_ids.into_iter().collect();
            let mut nodes = Vec::new();
            let mut retained_node_ids = std::collections::HashSet::new();
            for unit_id in node_ids_vec {
                if let Some(hit) = engine.get_shared_search_hit_by_index(unit_id).await? {
                    let unit = hit.memory_unit();
                    if let Some(ref oid) = org_filter {
                        if unit.org_id.as_deref() != Some(oid.as_str()) {
                            continue;
                        }
                    }
                    let label = if unit.content.chars().count() > 80 {
                        let end = unit
                            .content
                            .char_indices()
                            .nth(80)
                            .map(|(i, _)| i)
                            .unwrap_or(unit.content.len());
                        format!("{}...", &unit.content[..end])
                    } else {
                        unit.content.clone()
                    };
                    retained_node_ids.insert(unit.id);
                    let display_user_id = if unit.domain == MemoryDomain::Organization {
                        String::new()
                    } else {
                        unit.user_id.clone()
                    };
                    nodes.push(serde_json::json!({
                        "id": unit.id,
                        "label": label,
                        "level": unit.level,
                        "importance": unit.importance,
                        "user_id": display_user_id,
                    }));
                }
            }

            let mut relation_dist: HashMap<String, usize> = HashMap::new();
            let edge_data: Vec<serde_json::Value> = edges
                .iter()
                .filter(|e| {
                    org_filter.as_ref().map_or(true, |_| {
                        retained_node_ids.contains(&e.source_id)
                            && retained_node_ids.contains(&e.target_id)
                    })
                })
                .map(|e| {
                    let rel = format!("{:?}", e.relation);
                    *relation_dist.entry(rel.clone()).or_default() += 1;
                    serde_json::json!({
                        "source": e.source_id,
                        "target": e.target_id,
                        "relation": rel,
                        "weight": e.weight,
                    })
                })
                .collect();
            let edge_count = edge_data.len();

            Ok(serde_json::json!({
                "nodes": nodes,
                "edges": edge_data,
                "edge_count": edge_count,
                "relation_distribution": relation_dist,
            }))
        }
        .await;

        if let Ok(data) = result {
            if let Some(nodes) = data["nodes"].as_array() {
                all_nodes.extend(nodes.clone());
            }
            if let Some(edges) = data["edges"].as_array() {
                all_edge_data.extend(edges.clone());
            }
            total_edge_count += data["edge_count"].as_u64().unwrap_or(0) as usize;
            if let Some(dist) = data["relation_distribution"].as_object() {
                for (k, v) in dist {
                    *all_relation_dist.entry(k.clone()).or_default() +=
                        v.as_u64().unwrap_or(0) as usize;
                }
            }
        }
    }

    let nodes = if all_nodes.len() > limit {
        all_nodes[..limit].to_vec()
    } else {
        all_nodes.clone()
    };

    let retained: std::collections::HashSet<String> = nodes
        .iter()
        .filter_map(|n| n["id"].as_str().map(String::from))
        .collect();
    let filtered_edges: Vec<_> = all_edge_data
        .into_iter()
        .filter(|e| {
            retained.contains(e["source"].as_str().unwrap_or(""))
                && retained.contains(e["target"].as_str().unwrap_or(""))
        })
        .collect();

    Json(serde_json::json!({
        "nodes": nodes,
        "edges": filtered_edges,
        "stats": {
            "node_count": nodes.len(),
            "edge_count": total_edge_count,
            "relation_distribution": all_relation_dist,
        }
    }))
    .into_response()
}

// ── Search ────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct SearchRequest {
    query: String,
    #[serde(default = "default_search_mode")]
    mode: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
    #[serde(default)]
    enable_arbitration: bool,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
}

fn default_search_mode() -> String {
    "hybrid".to_string()
}
fn default_search_limit() -> usize {
    10
}

pub async fn search(
    State(state): State<Arc<crate::AppState>>,
    Json(payload): Json<SearchRequest>,
) -> axum::response::Response {
    let limit = payload.limit.min(100);
    let start = std::time::Instant::now();
    let Some(user_id) = payload.user_id.as_deref() else {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "user_id is required" })),
        )
            .into_response();
    };
    let org_id = payload.org_id.as_deref();
    let agent_id = payload.agent_id.as_deref();

    // Route to the correct shard for this user
    let shard = state.shard_manager.shard_for_user(user_id);

    let results = match payload.mode.as_str() {
        "text_local" => {
            match shard
                .engine
                .search_text(
                    user_id,
                    &payload.query,
                    limit,
                    payload.enable_arbitration,
                    None,
                )
                .await
            {
                Ok(units) => units
                    .into_iter()
                    .map(|u| (DashboardSearchMemoryUnitView::from(&u), 0.0f32))
                    .collect::<Vec<_>>(),
                Err(e) => {
                    return (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": e.to_string() })),
                    )
                        .into_response();
                }
            }
        }
        "text" | "text_shared" => {
            match shard
                .engine
                .search_text_with_shared(
                    user_id,
                    org_id,
                    &payload.query,
                    limit,
                    payload.enable_arbitration,
                    None,
                )
                .await
            {
                Ok(units) => units
                    .into_iter()
                    .map(|u| (DashboardSearchMemoryUnitView::from(u.memory_unit()), 0.0f32))
                    .collect::<Vec<_>>(),
                Err(e) => {
                    return (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": e.to_string() })),
                    )
                        .into_response();
                }
            }
        }
        "vector" => match state.llm_client.embed(&payload.query).await {
            Ok(embedding) => {
                let filter = shard.engine.build_user_filter(user_id, None);
                match shard
                    .engine
                    .search_similar(user_id, &embedding.data, limit, filter)
                    .await
                {
                    Ok(results) => results
                        .into_iter()
                        .map(|(u, score)| (DashboardSearchMemoryUnitView::from(&u), score))
                        .collect(),
                    Err(e) => {
                        return (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            Json(serde_json::json!({ "error": e.to_string() })),
                        )
                            .into_response();
                    }
                }
            }
            Err(e) => {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": format!("Embedding failed: {}", e) })),
                )
                    .into_response();
            }
        },
        _ => {
            // hybrid (default)
            match state.llm_client.embed(&payload.query).await {
                Ok(embedding) => {
                    match shard
                        .engine
                        .search_hybrid_with_shared(
                            user_id,
                            org_id,
                            agent_id,
                            &payload.query,
                            &embedding.data,
                            limit,
                            payload.enable_arbitration,
                            None,
                            1,
                            None,
                            None,
                        )
                        .await
                    {
                        Ok(results) => results
                            .into_iter()
                            .map(|(u, score)| {
                                (DashboardSearchMemoryUnitView::from(u.memory_unit()), score)
                            })
                            .collect(),
                        Err(e) => {
                            return (
                                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                                Json(serde_json::json!({ "error": e.to_string() })),
                            )
                                .into_response();
                        }
                    }
                }
                Err(e) => {
                    return (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": format!("Embedding failed: {}", e) })),
                    )
                        .into_response();
                }
            }
        }
    };

    let query_time_ms = start.elapsed().as_millis();

    #[derive(Serialize)]
    struct DashboardSearchResultView {
        unit: DashboardSearchMemoryUnitView,
        score: f32,
    }

    #[derive(Serialize)]
    struct DashboardSearchResponse {
        results: Vec<DashboardSearchResultView>,
        query_time_ms: u128,
    }

    let result_items = results
        .into_iter()
        .map(|(unit, score)| DashboardSearchResultView { score, unit })
        .collect();

    Json(DashboardSearchResponse {
        results: result_items,
        query_time_ms,
    })
    .into_response()
}

// ── Config ────────────────────────────────────────────────────────

pub async fn get_config(State(state): State<Arc<crate::AppState>>) -> Json<serde_json::Value> {
    let config = &state.config;

    let mut result = serde_json::json!({
        "raft": {
            "node_id": config.raft.node_id,
            "raft_addr": config.raft.raft_addr,
            "heartbeat_interval_ms": config.raft.heartbeat_interval_ms,
            "election_timeout_min_ms": config.raft.election_timeout_min_ms,
            "election_timeout_max_ms": config.raft.election_timeout_max_ms,
            "snapshot_logs": config.raft.snapshot_logs,
        },
        "worker": {
            "llm_concurrency": config.worker.llm_concurrency,
            "consolidation_interval_ms": config.worker.consolidation_interval_ms,
            "community_interval_ms": config.worker.community_interval_ms,
            "insight_interval_ms": config.worker.insight_interval_ms,
        },
        "llm": {
            "provider": format!("{:?}", config.llm.provider),
            "model": config.llm.model,
            "embedding_model": config.llm.embedding_model,
        },
        "storage": {
            "root_dir": config.storage.root_dir,
            "index_commit_interval_ms": config.storage.index_commit_interval_ms,
        },
    });

    if config.is_sharded() {
        result["sharding"] = serde_json::json!({
            "enabled": true,
            "shard_count": config.shard_count(),
            "physical_node_id": config.physical_node_id(),
        });
    }

    Json(result)
}

// ── Chat ──────────────────────────────────────────────────────────

use axum::response::sse::{Event, Sse};
use futures_util::stream::Stream;

#[derive(Deserialize)]
pub struct ChatRequest {
    message: String,
    user_id: String,
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default = "default_chat_limit")]
    context_limit: usize,
}

fn default_chat_limit() -> usize {
    5
}

pub async fn chat(
    State(state): State<Arc<crate::AppState>>,
    Json(payload): Json<ChatRequest>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let user_id = payload.user_id.clone();
    let org_id = payload.org_id.clone();
    let message = payload.message.clone();
    let context_limit = payload.context_limit;

    let stream = async_stream::stream! {
        // Step 1: Search for relevant context using hybrid search
        let shard = state.shard_manager.shard_for_user(&user_id);

        let context_results = match state.llm_client.embed(&message).await {
            Ok(embedding) => {
                match shard.engine.search_hybrid_with_shared(
                    &user_id,
                    org_id.as_deref(),
                    None,
                    &message,
                    &embedding.data,
                    context_limit,
                    false,
                    None,
                    1,
                    None,
                    None,
                ).await {
                    Ok(results) => results,
                    Err(e) => {
                        yield Ok(Event::default().event("error").data(format!("Search failed: {}", e)));
                        return;
                    }
                }
            }
            Err(e) => {
                yield Ok(Event::default().event("error").data(format!("Embedding failed: {}", e)));
                return;
            }
        };

        // Step 2: Build context from search results
        let mut context_text = String::new();
        let context_budget = context_limit.clamp(1, 10) * 500;
        if !context_results.is_empty() {
            context_text.push_str("## Relevant Context from Memory:\n");
            for (unit, _score) in &context_results {
                if !append_context_with_budget(
                    &mut context_text,
                    &format_memory_unit_context(unit.memory_unit()),
                    context_budget,
                ) {
                    break;
                }
            }
            context_text.push_str("\n");
        }

        // Step 3: Build prompt
        let system_prompt = format!(
            "You are a helpful AI assistant with access to the user's memory system. \
    Use the provided memory context when it is relevant, especially multimodal descriptions and source references. \
    If the memory context is insufficient, answer honestly and do not invent remembered facts.\n\n{}",
            context_text
        );

        // Step 4: Generate response using LLM
        let full_prompt = format!("{}\nUser: {}", system_prompt, message);
        match state.llm_client.generate(&full_prompt).await {
            Ok(response) => {
                // Stream the response word by word for better UX
                let words: Vec<&str> = response.data.split_whitespace().collect();
                for (i, word) in words.iter().enumerate() {
                    let text = if i == words.len() - 1 {
                        word.to_string()
                    } else {
                        format!("{} ", word)
                    };
                    yield Ok(Event::default().event("message").data(text));
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                }

                yield Ok(Event::default().event("done").data(""));
            }
            Err(e) => {
                yield Ok(Event::default().event("error").data(format!("Generation failed: {}", e)));
            }
        }
    };

    Sse::new(stream)
}

// ── Agents ────────────────────────────────────────────────────────

#[derive(serde::Serialize)]
pub struct AgentSummary {
    agent_id: String,
    total_memories: usize,
    l1_count: usize,
    l2_count: usize,
    total_events: usize,
    last_activity: Option<i64>,
}

pub async fn list_agents(State(state): State<Arc<crate::AppState>>) -> axum::response::Response {
    let cache_key = "agents:list".to_string();
    if let Some(cached) = state.dashboard_cache.get(&cache_key).await {
        return Json(cached).into_response();
    }

    let mut agent_data: HashMap<String, AgentSummary> = HashMap::new();

    for (_, shard) in state.shard_manager.all_shards() {
        let engine = shard.engine.clone();

        let scan_result = tokio::task::spawn_blocking(
            move || -> anyhow::Result<HashMap<String, AgentSummary>> {
                let kv = engine.kv();
                let mut local_agents: HashMap<String, AgentSummary> = HashMap::new();

                let all_pairs = kv.scan(b"u:")?;

                // Scan memory units grouped by agent_id
                for (k, val) in &all_pairs {
                    if k.windows(6).any(|w| w == b":unit:") {
                        if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(val) {
                            if unit.domain != MemoryDomain::Agent {
                                continue;
                            }
                            if let Some(ref aid) = unit.agent_id {
                                if aid.is_empty() {
                                    continue;
                                }
                                let entry = local_agents.entry(aid.clone()).or_insert_with(|| {
                                    AgentSummary {
                                        agent_id: aid.clone(),
                                        total_memories: 0,
                                        l1_count: 0,
                                        l2_count: 0,
                                        total_events: 0,
                                        last_activity: None,
                                    }
                                });
                                entry.total_memories += 1;
                                match unit.level {
                                    1 => entry.l1_count += 1,
                                    2 => entry.l2_count += 1,
                                    _ => {}
                                }
                                let ts = unit.transaction_time.timestamp();
                                update_last_activity(&mut entry.last_activity, ts);
                            }
                        }
                    }
                }

                // Count events per agent_id
                for (k, val) in &all_pairs {
                    if k.windows(7).any(|w| w == b":event:") {
                        if let Ok(event) = serde_json::from_slice::<memorose_common::Event>(val) {
                            if let Some(ref aid) = event.agent_id {
                                if aid.is_empty() {
                                    continue;
                                }
                                if let Some(entry) = local_agents.get_mut(aid) {
                                    entry.total_events += 1;
                                    update_last_activity(
                                        &mut entry.last_activity,
                                        event.transaction_time.timestamp(),
                                    );
                                } else {
                                    local_agents.insert(
                                        aid.clone(),
                                        AgentSummary {
                                            agent_id: aid.clone(),
                                            total_memories: 0,
                                            l1_count: 0,
                                            l2_count: 0,
                                            total_events: 1,
                                            last_activity: Some(event.transaction_time.timestamp()),
                                        },
                                    );
                                }
                            }
                        }
                    }
                }

                Ok(local_agents)
            },
        )
        .await;

        if let Ok(Ok(shard_agents)) = scan_result {
            for (aid, summary) in shard_agents {
                let entry = agent_data.entry(aid).or_insert_with(|| AgentSummary {
                    agent_id: summary.agent_id,
                    total_memories: 0,
                    l1_count: 0,
                    l2_count: 0,
                    total_events: 0,
                    last_activity: None,
                });
                entry.total_memories += summary.total_memories;
                entry.l1_count += summary.l1_count;
                entry.l2_count += summary.l2_count;
                entry.total_events += summary.total_events;
                if entry.last_activity.is_none()
                    || (summary.last_activity.is_some()
                        && entry.last_activity < summary.last_activity)
                {
                    entry.last_activity = summary.last_activity;
                }
            }
        }
    }

    let mut agents: Vec<AgentSummary> = agent_data.into_values().collect();
    agents.sort_by(|a, b| b.last_activity.cmp(&a.last_activity));

    let total_count = agents.len();
    let result = serde_json::json!({
        "agents": agents,
        "total_count": total_count,
    });

    state
        .dashboard_cache
        .insert(cache_key, result.clone())
        .await;

    Json(result).into_response()
}
