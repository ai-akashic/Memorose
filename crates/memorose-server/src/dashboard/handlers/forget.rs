use axum::{extract::State, response::IntoResponse, Json};
use memorose_common::{ForgetMode, ForgetTargetKind, ForgettingTombstone, MemoryUnit};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

use super::types::*;

pub(super) const FORGET_PREVIEW_TTL_SECS: i64 = 15 * 60;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct ForgetPreviewRecord {
    pub preview_id: String,
    pub user_id: String,
    pub org_id: Option<String>,
    pub query: String,
    pub mode: ForgetMode,
    pub memory_unit_ids: Vec<uuid::Uuid>,
    pub event_ids: Vec<uuid::Uuid>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Deserialize)]
pub struct ForgetPreviewRequest {
    pub user_id: String,
    pub query: String,
    #[serde(default)]
    pub org_id: Option<String>,
    #[serde(default = "default_forget_mode")]
    pub mode: ForgetMode,
    #[serde(default = "default_forget_limit")]
    pub limit: usize,
}

#[derive(Deserialize)]
pub struct ForgetExecuteRequest {
    pub user_id: String,
    pub preview_id: String,
    #[serde(default)]
    pub org_id: Option<String>,
    pub confirm: bool,
}

#[derive(Serialize)]
pub(super) struct ForgetPreviewSummary {
    pub memory_unit_count: usize,
    pub event_count: usize,
}

#[derive(Clone, Serialize)]
pub(super) struct ForgetEventPreviewView {
    pub id: uuid::Uuid,
    pub content: String,
    pub transaction_time: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
}

#[derive(Serialize)]
pub(super) struct ForgetPreviewResponse {
    pub preview_id: String,
    pub query: String,
    pub mode: ForgetMode,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub summary: ForgetPreviewSummary,
    pub matched_units: Vec<DashboardSearchMemoryUnitView>,
    pub matched_events: Vec<ForgetEventPreviewView>,
}

#[derive(Serialize)]
pub(super) struct ForgetExecuteResponse {
    pub status: &'static str,
    pub preview_id: String,
    pub mode: ForgetMode,
    pub query: String,
    pub forgotten_memory_unit_count: usize,
    pub forgotten_event_count: usize,
}

pub(super) fn default_forget_mode() -> ForgetMode {
    ForgetMode::Logical
}

pub(super) fn default_forget_limit() -> usize {
    10
}

pub(super) fn forget_preview_key(preview_id: &str) -> String {
    format!("forget_preview:{}", preview_id)
}

pub(super) fn store_forget_preview(
    engine: &memorose_core::MemoroseEngine,
    preview: &ForgetPreviewRecord,
) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(preview)?;
    engine
        .system_kv()
        .put(forget_preview_key(&preview.preview_id).as_bytes(), &bytes)?;
    Ok(())
}

pub(super) fn load_forget_preview(
    engine: &memorose_core::MemoroseEngine,
    preview_id: &str,
) -> anyhow::Result<Option<ForgetPreviewRecord>> {
    let key = forget_preview_key(preview_id);
    let Some(bytes) = engine.system_kv().get(key.as_bytes())? else {
        return Ok(None);
    };
    let preview: ForgetPreviewRecord = serde_json::from_slice(&bytes)?;
    if preview.expires_at < chrono::Utc::now() {
        engine.system_kv().delete(key.as_bytes())?;
        return Ok(None);
    }
    Ok(Some(preview))
}

pub(super) fn delete_forget_preview(
    engine: &memorose_core::MemoroseEngine,
    preview_id: &str,
) -> anyhow::Result<()> {
    engine
        .system_kv()
        .delete(forget_preview_key(preview_id).as_bytes())?;
    Ok(())
}

pub(super) async fn build_forget_preview_artifacts(
    state: &Arc<crate::AppState>,
    user_id: &str,
    org_id: Option<&str>,
    query: &str,
    mode: ForgetMode,
    limit: usize,
) -> anyhow::Result<(
    ForgetPreviewRecord,
    Vec<MemoryUnit>,
    Vec<ForgetEventPreviewView>,
)> {
    let shard = state.shard_manager.shard_for_user(user_id);
    let embedding = state.llm_client.embed(query).await?.data;
    let org_id_owned = org_id.map(str::to_string);

    let mut matched_units = shard
        .engine
        .search_hybrid(
            user_id,
            org_id,
            None,
            query,
            &embedding,
            limit.clamp(1, 25),
            false,
            Some(0.35),
            1,
            None,
            None,
        )
        .await?
        .into_iter()
        .filter(|(unit, _)| {
            org_id_owned.as_ref().map_or(true, |expected_org| {
                unit.org_id.as_deref() == Some(expected_org.as_str())
            })
        })
        .map(|(unit, _)| unit)
        .collect::<Vec<_>>();

    let mut seen_events = HashSet::new();
    let mut matched_events = Vec::new();
    for unit in &matched_units {
        for reference in &unit.references {
            if !seen_events.insert(*reference) {
                continue;
            }
            match shard
                .engine
                .get_event(user_id, &reference.to_string())
                .await
            {
                Ok(Some(event)) => {
                    if org_id_owned.as_ref().map_or(false, |expected_org| {
                        event.org_id.as_deref() != Some(expected_org.as_str())
                    }) {
                        continue;
                    }
                    let (content, _) = event_content_preview(&event.content);
                    matched_events.push(ForgetEventPreviewView {
                        id: event.id,
                        content: dashboard_build_content_preview(&content, 220),
                        transaction_time: event.transaction_time,
                        org_id: event.org_id,
                        agent_id: event.agent_id,
                    });
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!("Forget preview skipped event {}: {}", reference, error);
                }
            }
        }
    }
    matched_events.sort_by(|left, right| right.transaction_time.cmp(&left.transaction_time));

    if !matched_events.is_empty() {
        let matched_event_ids = matched_events
            .iter()
            .map(|event| event.id)
            .collect::<HashSet<_>>();
        let mut seen_unit_ids = matched_units
            .iter()
            .map(|unit| unit.id)
            .collect::<HashSet<_>>();
        match shard.engine.list_memory_units_global(Some(user_id)).await {
            Ok(extra_units) => {
                let mut related_units = extra_units
                    .into_iter()
                    .filter(|unit| {
                        org_id_owned.as_ref().map_or(true, |expected_org| {
                            unit.org_id.as_deref() == Some(expected_org.as_str())
                        })
                    })
                    .filter(|unit| !seen_unit_ids.contains(&unit.id))
                    .filter(|unit| {
                        unit.references
                            .iter()
                            .any(|reference| matched_event_ids.contains(reference))
                    })
                    .collect::<Vec<_>>();
                related_units
                    .sort_by(|left, right| right.transaction_time.cmp(&left.transaction_time));
                for unit in related_units {
                    seen_unit_ids.insert(unit.id);
                    matched_units.push(unit);
                }
            }
            Err(error) => {
                tracing::warn!("Forget preview failed to expand derived units: {}", error);
            }
        }
    }

    let preview = ForgetPreviewRecord {
        preview_id: uuid::Uuid::new_v4().to_string(),
        user_id: user_id.to_string(),
        org_id: org_id_owned,
        query: query.to_string(),
        mode,
        memory_unit_ids: matched_units.iter().map(|unit| unit.id).collect(),
        event_ids: matched_events.iter().map(|event| event.id).collect(),
        created_at: chrono::Utc::now(),
        expires_at: chrono::Utc::now() + chrono::Duration::seconds(FORGET_PREVIEW_TTL_SECS),
    };

    Ok((preview, matched_units, matched_events))
}

pub(super) async fn execute_forget_preview_record(
    engine: &memorose_core::MemoroseEngine,
    preview: &ForgetPreviewRecord,
) -> anyhow::Result<()> {
    match preview.mode {
        ForgetMode::Logical => {
            for unit_id in &preview.memory_unit_ids {
                let tombstone = build_forgetting_tombstone(
                    &preview.user_id,
                    preview.org_id.clone(),
                    ForgetTargetKind::MemoryUnit,
                    unit_id.to_string(),
                    &preview.query,
                    &preview.preview_id,
                    preview.mode.clone(),
                );
                engine.mark_memory_unit_forgotten(&preview.user_id, *unit_id, &tombstone)?;
            }

            for event_id in &preview.event_ids {
                let tombstone = build_forgetting_tombstone(
                    &preview.user_id,
                    preview.org_id.clone(),
                    ForgetTargetKind::Event,
                    event_id.to_string(),
                    &preview.query,
                    &preview.preview_id,
                    preview.mode.clone(),
                );
                engine.mark_event_forgotten(&preview.user_id, &event_id.to_string(), &tombstone)?;
                if let Err(error) = engine.mark_event_processed(&event_id.to_string()).await {
                    tracing::warn!(
                        "Failed to clear pending marker for forgotten event {}: {}",
                        event_id,
                        error
                    );
                }
            }
        }
        ForgetMode::Hard => {
            for event_id in &preview.event_ids {
                engine
                    .delete_event(&preview.user_id, &event_id.to_string())
                    .await?;
            }

            for unit_id in &preview.memory_unit_ids {
                engine
                    .delete_memory_unit_hard(&preview.user_id, *unit_id)
                    .await?;
            }
        }
    }

    Ok(())
}

pub(super) fn build_forgetting_tombstone(
    user_id: &str,
    org_id: Option<String>,
    target_kind: ForgetTargetKind,
    target_id: String,
    reason_query: &str,
    preview_id: &str,
    mode: ForgetMode,
) -> ForgettingTombstone {
    ForgettingTombstone {
        user_id: user_id.to_string(),
        org_id,
        target_kind,
        target_id,
        reason_query: reason_query.to_string(),
        created_at: chrono::Utc::now(),
        preview_id: Some(preview_id.to_string()),
        mode,
    }
}

pub async fn forget_preview(
    State(state): State<Arc<crate::AppState>>,
    Json(payload): Json<ForgetPreviewRequest>,
) -> axum::response::Response {
    if let Err(response) = validate_registry_id(payload.user_id.trim(), "user_id") {
        return response;
    }
    if let Some(org_id) = payload.org_id.as_deref() {
        if let Err(response) = validate_registry_id(org_id.trim(), "org_id") {
            return response;
        }
    }
    if payload.query.trim().is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "query must not be empty" })),
        )
            .into_response();
    }
    let limit = payload.limit.clamp(1, 25);
    let user_id = payload.user_id.trim().to_string();
    let org_id = payload
        .org_id
        .as_ref()
        .map(|value| value.trim().to_string());
    let shard = state.shard_manager.shard_for_user(&user_id);

    let (preview, matched_units, matched_events) = match build_forget_preview_artifacts(
        &state,
        &user_id,
        org_id.as_deref(),
        payload.query.trim(),
        payload.mode.clone(),
        limit,
    )
    .await
    {
        Ok(data) => data,
        Err(error) => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Failed to build forget preview: {}", error) })),
            )
                .into_response();
        }
    };

    if let Err(error) = store_forget_preview(&shard.engine, &preview) {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("Failed to store preview: {}", error) })),
        )
            .into_response();
    }

    Json(ForgetPreviewResponse {
        preview_id: preview.preview_id,
        query: preview.query,
        mode: preview.mode,
        created_at: preview.created_at,
        expires_at: preview.expires_at,
        summary: ForgetPreviewSummary {
            memory_unit_count: matched_units.len(),
            event_count: matched_events.len(),
        },
        matched_units: matched_units
            .iter()
            .map(DashboardSearchMemoryUnitView::from)
            .collect(),
        matched_events,
    })
    .into_response()
}

pub async fn forget_execute(
    State(state): State<Arc<crate::AppState>>,
    Json(payload): Json<ForgetExecuteRequest>,
) -> axum::response::Response {
    if let Err(response) = validate_registry_id(payload.user_id.trim(), "user_id") {
        return response;
    }
    if let Some(org_id) = payload.org_id.as_deref() {
        if let Err(response) = validate_registry_id(org_id.trim(), "org_id") {
            return response;
        }
    }
    if !payload.confirm {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "confirm must be true" })),
        )
            .into_response();
    }

    let user_id = payload.user_id.trim().to_string();
    let org_id = payload
        .org_id
        .as_ref()
        .map(|value| value.trim().to_string());
    let shard = state.shard_manager.shard_for_user(&user_id);

    let preview = match load_forget_preview(&shard.engine, payload.preview_id.trim()) {
        Ok(Some(preview)) => preview,
        Ok(None) => {
            return (
                axum::http::StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "Forget preview not found or expired" })),
            )
                .into_response();
        }
        Err(error) => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Failed to load preview: {}", error) })),
            )
                .into_response();
        }
    };

    if preview.user_id != user_id || preview.org_id != org_id {
        return (
            axum::http::StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Preview scope does not match request" })),
        )
            .into_response();
    }
    if let Err(error) = execute_forget_preview_record(&shard.engine, &preview).await {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }

    if let Err(error) = delete_forget_preview(&shard.engine, &preview.preview_id) {
        tracing::warn!(
            "Failed to delete forget preview {} after execute: {}",
            preview.preview_id,
            error
        );
    }

    Json(ForgetExecuteResponse {
        status: "executed",
        preview_id: preview.preview_id,
        mode: preview.mode,
        query: preview.query,
        forgotten_memory_unit_count: preview.memory_unit_ids.len(),
        forgotten_event_count: preview.event_ids.len(),
    })
    .into_response()
}
