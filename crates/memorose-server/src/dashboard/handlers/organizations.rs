use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::types::*;

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
