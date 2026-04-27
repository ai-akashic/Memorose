use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use memorose_common::{
    Event as MemoryEvent, EventContent, ForgetMode, MemoryType, MemoryUnit, StoredMemoryFact,
};
use memorose_core::arbitrator::MemoryCorrectionKind;
use memorose_core::engine::{RacDecisionEffect, RacReviewRecord, RacReviewStatus};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::forget::{
    build_forget_preview_artifacts, default_forget_limit, default_forget_mode,
    delete_forget_preview, execute_forget_preview_record, load_forget_preview,
    store_forget_preview, ForgetPreviewResponse, ForgetPreviewSummary,
};
use super::types::*;

const SEMANTIC_PLAN_TTL_SECS: i64 = 15 * 60;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SemanticMemoryPlanKind {
    Forget,
    Update,
}

#[derive(Clone, Serialize, Deserialize)]
struct SemanticCorrectionPlanActionRecord {
    target_unit_id: uuid::Uuid,
    action: String,
    confidence: f32,
    reason: String,
    effect: RacDecisionEffect,
    relation: Option<String>,
    guard_reason: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct SemanticMemoryPlanRecord {
    plan_id: String,
    user_id: String,
    org_id: Option<String>,
    instruction: String,
    kind: SemanticMemoryPlanKind,
    forget_mode: ForgetMode,
    linked_forget_preview_id: Option<String>,
    source_content: Option<String>,
    extracted_facts: Vec<StoredMemoryFact>,
    planned_actions: Vec<SemanticCorrectionPlanActionRecord>,
    created_at: chrono::DateTime<chrono::Utc>,
    expires_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Deserialize)]
pub struct SemanticMemoryPreviewRequest {
    user_id: String,
    instruction: String,
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default = "default_forget_mode")]
    forget_mode: ForgetMode,
    #[serde(default = "default_forget_limit")]
    limit: usize,
}

#[derive(Deserialize)]
pub struct UserSemanticMemoryPreviewRequest {
    instruction: String,
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default = "default_forget_mode")]
    forget_mode: ForgetMode,
    #[serde(default = "default_forget_limit")]
    limit: usize,
}

#[derive(Deserialize)]
pub struct SemanticMemoryExecuteRequest {
    user_id: String,
    plan_id: String,
    #[serde(default)]
    org_id: Option<String>,
    confirm: bool,
    #[serde(default)]
    reviewer: Option<String>,
    #[serde(default)]
    note: Option<String>,
}

#[derive(Deserialize)]
pub struct UserSemanticMemoryExecuteRequest {
    plan_id: String,
    #[serde(default)]
    org_id: Option<String>,
    confirm: bool,
    #[serde(default)]
    reviewer: Option<String>,
    #[serde(default)]
    note: Option<String>,
}

#[derive(Serialize)]
struct SemanticCorrectionActionView {
    target_unit_id: uuid::Uuid,
    action: String,
    confidence: f32,
    reason: String,
    effect: RacDecisionEffect,
    #[serde(skip_serializing_if = "Option::is_none")]
    relation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    guard_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_unit: Option<DashboardSearchMemoryUnitView>,
}

#[derive(Serialize)]
struct SemanticUpdatePreview {
    source_content: String,
    extracted_facts: Vec<StoredMemoryFact>,
    actions: Vec<SemanticCorrectionActionView>,
}

#[derive(Serialize)]
struct SemanticMemoryPreviewResponse {
    plan_id: String,
    instruction: String,
    kind: SemanticMemoryPlanKind,
    created_at: chrono::DateTime<chrono::Utc>,
    expires_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    forget_preview: Option<ForgetPreviewResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    update_preview: Option<SemanticUpdatePreview>,
}

#[derive(Serialize)]
struct SemanticMemoryExecuteResponse {
    status: &'static str,
    plan_id: String,
    kind: SemanticMemoryPlanKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_memory_unit_id: Option<uuid::Uuid>,
    affected_unit_ids: Vec<uuid::Uuid>,
}

#[derive(Deserialize)]
pub struct ManualCorrectionRequest {
    user_id: String,
    source_unit_id: uuid::Uuid,
    target_unit_id: uuid::Uuid,
    action: String,
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default)]
    confidence: Option<f32>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    reviewer: Option<String>,
}

#[derive(Serialize)]
struct ManualCorrectionResponse {
    status: &'static str,
    affected_unit_ids: Vec<uuid::Uuid>,
}

#[derive(Deserialize)]
pub struct RacReviewListQuery {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Serialize)]
struct RacReviewView {
    review: RacReviewRecord,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_unit: Option<DashboardSearchMemoryUnitView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_unit: Option<DashboardSearchMemoryUnitView>,
}

#[derive(Deserialize)]
pub struct ResolveRacReviewRequest {
    user_id: String,
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default)]
    reviewer: Option<String>,
    #[serde(default)]
    note: Option<String>,
}

fn parse_memory_correction_kind(value: &str) -> Option<MemoryCorrectionKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "obsolete" => Some(MemoryCorrectionKind::Obsolete),
        "contradicts" | "contradict" => Some(MemoryCorrectionKind::Contradicts),
        "reaffirm" => Some(MemoryCorrectionKind::Reaffirm),
        "ignore" => Some(MemoryCorrectionKind::Ignore),
        _ => None,
    }
}

fn parse_rac_review_status(value: Option<&str>) -> Option<RacReviewStatus> {
    match value?.trim().to_ascii_lowercase().as_str() {
        "pending" => Some(RacReviewStatus::Pending),
        "approved" => Some(RacReviewStatus::Approved),
        "rejected" => Some(RacReviewStatus::Rejected),
        _ => None,
    }
}

fn parse_semantic_plan_kind(value: Option<&str>, instruction: &str) -> SemanticMemoryPlanKind {
    match value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        Some("forget") => SemanticMemoryPlanKind::Forget,
        Some("update") => SemanticMemoryPlanKind::Update,
        _ => {
            let lowered = instruction.to_ascii_lowercase();
            let forget_keywords = [
                "forget ", "delete ", "remove ", "erase ", "clear ", "drop ", "忘掉", "忘记",
                "删除", "移除", "清掉", "清除",
            ];
            if forget_keywords
                .iter()
                .any(|keyword| lowered.contains(keyword) || instruction.contains(keyword))
            {
                SemanticMemoryPlanKind::Forget
            } else {
                SemanticMemoryPlanKind::Update
            }
        }
    }
}

fn semantic_plan_key(plan_id: &str) -> String {
    format!("semantic_memory_plan:{}", plan_id)
}

fn store_semantic_plan(
    engine: &memorose_core::MemoroseEngine,
    plan: &SemanticMemoryPlanRecord,
) -> anyhow::Result<()> {
    engine.system_kv().put(
        semantic_plan_key(&plan.plan_id).as_bytes(),
        &serde_json::to_vec(plan)?,
    )?;
    Ok(())
}

fn load_semantic_plan(
    engine: &memorose_core::MemoroseEngine,
    plan_id: &str,
) -> anyhow::Result<Option<SemanticMemoryPlanRecord>> {
    let key = semantic_plan_key(plan_id);
    let Some(bytes) = engine.system_kv().get(key.as_bytes())? else {
        return Ok(None);
    };
    let plan: SemanticMemoryPlanRecord = serde_json::from_slice(&bytes)?;
    if plan.expires_at <= chrono::Utc::now() {
        engine.system_kv().delete(key.as_bytes()).ok();
        return Ok(None);
    }
    Ok(Some(plan))
}

fn delete_semantic_plan(
    engine: &memorose_core::MemoroseEngine,
    plan_id: &str,
) -> anyhow::Result<()> {
    engine
        .system_kv()
        .delete(semantic_plan_key(plan_id).as_bytes())?;
    Ok(())
}

fn build_semantic_update_event_metadata(
    plan: &SemanticMemoryPlanRecord,
    reviewer: Option<&str>,
    note: Option<&str>,
) -> serde_json::Value {
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "source".to_string(),
        serde_json::json!("semantic_orchestrator"),
    );
    metadata.insert("semantic_orchestrator".to_string(), serde_json::json!(true));
    metadata.insert("semantic_kind".to_string(), serde_json::json!("update"));
    metadata.insert(
        "semantic_plan_id".to_string(),
        serde_json::json!(plan.plan_id),
    );
    metadata.insert(
        "semantic_instruction".to_string(),
        serde_json::json!(plan.instruction),
    );

    if let Some(reviewer) = reviewer {
        metadata.insert("reviewer".to_string(), serde_json::json!(reviewer));
    }
    if let Some(note) = note {
        metadata.insert("note".to_string(), serde_json::json!(note));
    }

    serde_json::Value::Object(metadata)
}

async fn materialize_semantic_update_source(
    engine: &memorose_core::MemoroseEngine,
    plan: &SemanticMemoryPlanRecord,
    embedding: Option<Vec<f32>>,
    reviewer: Option<&str>,
    note: Option<&str>,
) -> anyhow::Result<MemoryUnit> {
    let Some(source_content) = plan.source_content.clone() else {
        anyhow::bail!("semantic update plan is missing source content");
    };
    let Some(embedding) = embedding.filter(|embedding| !embedding.is_empty()) else {
        anyhow::bail!(
            "semantic update requires a valid embedding before materializing the source memory"
        );
    };

    let mut source_event = MemoryEvent::new(
        plan.org_id.clone(),
        plan.user_id.clone(),
        None,
        uuid::Uuid::new_v4(),
        EventContent::Text(source_content.clone()),
    );
    source_event.metadata = build_semantic_update_event_metadata(plan, reviewer, note);
    engine.ingest_event_directly(source_event.clone()).await?;

    let mut source_unit = MemoryUnit::new(
        plan.org_id.clone(),
        plan.user_id.clone(),
        None,
        source_event.stream_id,
        MemoryType::Factual,
        source_content,
        Some(embedding),
    );
    source_unit.transaction_time = source_event.transaction_time;
    source_unit.valid_time = source_event.valid_time;
    source_unit.references.push(source_event.id);
    source_unit.extracted_facts = plan.extracted_facts.clone();

    engine.store_memory_unit(source_unit.clone()).await?;
    engine
        .mark_event_processed(&source_event.id.to_string())
        .await?;

    Ok(source_unit)
}

fn memory_correction_kind_label(kind: MemoryCorrectionKind) -> &'static str {
    match kind {
        MemoryCorrectionKind::Obsolete => "obsolete",
        MemoryCorrectionKind::Contradicts => "contradicts",
        MemoryCorrectionKind::Reaffirm => "reaffirm",
        MemoryCorrectionKind::Ignore => "ignore",
    }
}

fn semantic_action_view(
    engine: &memorose_core::MemoroseEngine,
    user_id: &str,
    action: SemanticCorrectionPlanActionRecord,
) -> SemanticCorrectionActionView {
    let target_unit = engine
        .get_memory_unit_including_forgotten(user_id, action.target_unit_id)
        .ok()
        .flatten()
        .as_ref()
        .map(DashboardSearchMemoryUnitView::from);
    SemanticCorrectionActionView {
        target_unit_id: action.target_unit_id,
        action: action.action,
        confidence: action.confidence,
        reason: action.reason,
        effect: action.effect,
        relation: action.relation,
        guard_reason: action.guard_reason,
        target_unit,
    }
}

fn correction_review_view(
    engine: &memorose_core::MemoroseEngine,
    review: RacReviewRecord,
) -> RacReviewView {
    let source_unit = engine
        .get_memory_unit_including_forgotten(&review.user_id, review.source_unit_id)
        .ok()
        .flatten()
        .as_ref()
        .map(DashboardSearchMemoryUnitView::from);
    let target_unit = engine
        .get_memory_unit_including_forgotten(&review.user_id, review.target_unit_id)
        .ok()
        .flatten()
        .as_ref()
        .map(DashboardSearchMemoryUnitView::from);
    RacReviewView {
        review,
        source_unit,
        target_unit,
    }
}

async fn semantic_memory_preview_internal(
    state: Arc<crate::AppState>,
    payload: SemanticMemoryPreviewRequest,
) -> axum::response::Response {
    if let Err(response) = validate_registry_id(payload.user_id.trim(), "user_id") {
        return response;
    }
    if let Some(org_id) = payload.org_id.as_deref() {
        if let Err(response) = validate_registry_id(org_id.trim(), "org_id") {
            return response;
        }
    }
    if payload.instruction.trim().is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "instruction must not be empty" })),
        )
            .into_response();
    }

    let user_id = payload.user_id.trim().to_string();
    let org_id = payload.org_id.as_deref().map(str::trim).map(str::to_string);
    let kind = parse_semantic_plan_kind(payload.mode.as_deref(), payload.instruction.trim());
    let shard = state.shard_manager.shard_for_user(&user_id);

    match kind {
        SemanticMemoryPlanKind::Forget => {
            let limit = payload.limit.clamp(1, 25);
            let (forget_preview, matched_units, matched_events) =
                match build_forget_preview_artifacts(
                    &state,
                    &user_id,
                    org_id.as_deref(),
                    payload.instruction.trim(),
                    payload.forget_mode.clone(),
                    limit,
                )
                .await
                {
                    Ok(data) => data,
                    Err(error) => {
                        return (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": format!("Failed to build semantic forget preview: {}", error) })),
                    )
                        .into_response();
                    }
                };

            if let Err(error) = store_forget_preview(&shard.engine, &forget_preview) {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": format!("Failed to store forget preview: {}", error) })),
                )
                    .into_response();
            }

            let plan = SemanticMemoryPlanRecord {
                plan_id: uuid::Uuid::new_v4().to_string(),
                user_id: user_id.clone(),
                org_id: org_id.clone(),
                instruction: payload.instruction.trim().to_string(),
                kind: SemanticMemoryPlanKind::Forget,
                forget_mode: payload.forget_mode,
                linked_forget_preview_id: Some(forget_preview.preview_id.clone()),
                source_content: None,
                extracted_facts: Vec::new(),
                planned_actions: Vec::new(),
                created_at: chrono::Utc::now(),
                expires_at: chrono::Utc::now() + chrono::Duration::seconds(SEMANTIC_PLAN_TTL_SECS),
            };

            if let Err(error) = store_semantic_plan(&shard.engine, &plan) {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": format!("Failed to store semantic plan: {}", error) })),
                )
                    .into_response();
            }

            Json(SemanticMemoryPreviewResponse {
                plan_id: plan.plan_id,
                instruction: plan.instruction,
                kind: plan.kind,
                created_at: plan.created_at,
                expires_at: plan.expires_at,
                forget_preview: Some(ForgetPreviewResponse {
                    preview_id: forget_preview.preview_id,
                    query: forget_preview.query,
                    mode: forget_preview.mode,
                    created_at: forget_preview.created_at,
                    expires_at: forget_preview.expires_at,
                    summary: ForgetPreviewSummary {
                        memory_unit_count: matched_units.len(),
                        event_count: matched_events.len(),
                    },
                    matched_units: matched_units
                        .iter()
                        .map(DashboardSearchMemoryUnitView::from)
                        .collect(),
                    matched_events,
                }),
                update_preview: None,
            })
            .into_response()
        }
        SemanticMemoryPlanKind::Update => {
            let mut preview_unit = MemoryUnit::new(
                org_id.clone(),
                user_id.clone(),
                None,
                uuid::Uuid::new_v4(),
                MemoryType::Factual,
                payload.instruction.trim().to_string(),
                None,
            );
            shard
                .engine
                .hydrate_memory_unit_extracted_facts(&mut preview_unit)
                .await;

            let planned_actions = match shard
                .engine
                .plan_memory_correction_actions(&preview_unit, payload.limit.clamp(1, 12))
                .await
            {
                Ok(actions) => actions,
                Err(error) => {
                    return (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": format!("Failed to plan semantic update: {}", error) })),
                    )
                        .into_response();
                }
            };

            let action_records = planned_actions
                .into_iter()
                .map(|action| SemanticCorrectionPlanActionRecord {
                    target_unit_id: action.target_id,
                    action: memory_correction_kind_label(action.kind).to_string(),
                    confidence: action.confidence,
                    reason: action.reason,
                    effect: action.effect,
                    relation: action
                        .relation
                        .map(|relation| relation.as_str().to_ascii_lowercase()),
                    guard_reason: action.guard_reason,
                })
                .collect::<Vec<_>>();

            let plan = SemanticMemoryPlanRecord {
                plan_id: uuid::Uuid::new_v4().to_string(),
                user_id: user_id.clone(),
                org_id: org_id.clone(),
                instruction: payload.instruction.trim().to_string(),
                kind: SemanticMemoryPlanKind::Update,
                forget_mode: payload.forget_mode,
                linked_forget_preview_id: None,
                source_content: Some(payload.instruction.trim().to_string()),
                extracted_facts: preview_unit.extracted_facts.clone(),
                planned_actions: action_records.clone(),
                created_at: chrono::Utc::now(),
                expires_at: chrono::Utc::now() + chrono::Duration::seconds(SEMANTIC_PLAN_TTL_SECS),
            };

            if let Err(error) = store_semantic_plan(&shard.engine, &plan) {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": format!("Failed to store semantic plan: {}", error) })),
                )
                    .into_response();
            }

            Json(SemanticMemoryPreviewResponse {
                plan_id: plan.plan_id,
                instruction: plan.instruction,
                kind: plan.kind,
                created_at: plan.created_at,
                expires_at: plan.expires_at,
                forget_preview: None,
                update_preview: Some(SemanticUpdatePreview {
                    source_content: plan.source_content.unwrap_or_default(),
                    extracted_facts: plan.extracted_facts,
                    actions: action_records
                        .into_iter()
                        .map(|action| semantic_action_view(&shard.engine, &user_id, action))
                        .collect(),
                }),
            })
            .into_response()
        }
    }
}

pub async fn semantic_memory_preview(
    State(state): State<Arc<crate::AppState>>,
    Json(payload): Json<SemanticMemoryPreviewRequest>,
) -> axum::response::Response {
    semantic_memory_preview_internal(state, payload).await
}

pub async fn user_semantic_memory_preview(
    State(state): State<Arc<crate::AppState>>,
    Path(user_id): Path<String>,
    Json(payload): Json<UserSemanticMemoryPreviewRequest>,
) -> axum::response::Response {
    semantic_memory_preview_internal(
        state,
        SemanticMemoryPreviewRequest {
            user_id,
            instruction: payload.instruction,
            org_id: payload.org_id,
            mode: payload.mode,
            forget_mode: payload.forget_mode,
            limit: payload.limit,
        },
    )
    .await
}

async fn semantic_memory_execute_internal(
    state: Arc<crate::AppState>,
    payload: SemanticMemoryExecuteRequest,
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
    let org_id = payload.org_id.as_deref().map(str::trim).map(str::to_string);
    let shard = state.shard_manager.shard_for_user(&user_id);

    let plan = match load_semantic_plan(&shard.engine, payload.plan_id.trim()) {
        Ok(Some(plan)) => plan,
        Ok(None) => {
            return (
                axum::http::StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "semantic plan not found or expired" })),
            )
                .into_response();
        }
        Err(error) => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Failed to load semantic plan: {}", error) })),
            )
                .into_response();
        }
    };

    if plan.user_id != user_id || plan.org_id != org_id {
        return (
            axum::http::StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "semantic plan scope mismatch" })),
        )
            .into_response();
    }

    match plan.kind {
        SemanticMemoryPlanKind::Forget => {
            let Some(forget_preview_id) = plan.linked_forget_preview_id.as_deref() else {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": "semantic forget plan is missing linked preview" })),
                )
                    .into_response();
            };
            let forget_preview = match load_forget_preview(&shard.engine, forget_preview_id) {
                Ok(Some(preview)) => preview,
                Ok(None) => {
                    return (
                        axum::http::StatusCode::NOT_FOUND,
                        Json(serde_json::json!({ "error": "linked forget preview not found or expired" })),
                    )
                        .into_response();
                }
                Err(error) => {
                    return (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": format!("Failed to load linked forget preview: {}", error) })),
                    )
                        .into_response();
                }
            };

            if let Err(error) = execute_forget_preview_record(&shard.engine, &forget_preview).await
            {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": error.to_string() })),
                )
                    .into_response();
            }

            let _ = delete_forget_preview(&shard.engine, forget_preview_id);
            let _ = delete_semantic_plan(&shard.engine, &plan.plan_id);

            Json(SemanticMemoryExecuteResponse {
                status: "executed",
                plan_id: plan.plan_id,
                kind: plan.kind,
                created_memory_unit_id: None,
                affected_unit_ids: forget_preview.memory_unit_ids,
            })
            .into_response()
        }
        SemanticMemoryPlanKind::Update => {
            let reviewer = payload
                .reviewer
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty());
            let note = payload
                .note
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty());
            let source_content = match plan.source_content.as_deref() {
                Some(content) => content,
                None => {
                    return (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": "semantic update plan is missing source content" })),
                    )
                        .into_response();
                }
            };
            let embedding = match state.llm_client.embed(source_content).await {
                Ok(embedding) => Some(embedding.data),
                Err(error) => {
                    tracing::warn!(
                        "Semantic update embedding failed for user {}: {}",
                        user_id,
                        error
                    );
                    None
                }
            };

            let source_unit = match materialize_semantic_update_source(
                &shard.engine,
                &plan,
                embedding,
                reviewer,
                note,
            )
            .await
            {
                Ok(unit) => unit,
                Err(error) => {
                    return (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": error.to_string() })),
                    )
                        .into_response();
                }
            };

            let created_memory_unit_id = source_unit.id;
            let mut affected_unit_ids = Vec::new();

            for action in &plan.planned_actions {
                let Some(kind) = parse_memory_correction_kind(&action.action) else {
                    continue;
                };
                let reason = match (note, reviewer) {
                    (Some(note), Some(reviewer)) => {
                        format!("{} | {} (reviewer: {})", action.reason, note, reviewer)
                    }
                    (Some(note), None) => format!("{} | {}", action.reason, note),
                    (None, Some(reviewer)) => format!("{} (reviewer: {})", action.reason, reviewer),
                    (None, None) => action.reason.clone(),
                };

                match shard
                    .engine
                    .apply_manual_memory_correction(
                        &user_id,
                        created_memory_unit_id,
                        action.target_unit_id,
                        kind,
                        reason,
                        action.confidence,
                        "semantic_orchestrator",
                    )
                    .await
                {
                    Ok(mut affected_ids) => affected_unit_ids.append(&mut affected_ids),
                    Err(error) => {
                        return (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            Json(serde_json::json!({ "error": format!("Failed to apply planned action to {}: {}", action.target_unit_id, error) })),
                        )
                            .into_response();
                    }
                }
            }

            affected_unit_ids.sort_unstable();
            affected_unit_ids.dedup();
            let _ = delete_semantic_plan(&shard.engine, &plan.plan_id);

            Json(SemanticMemoryExecuteResponse {
                status: "executed",
                plan_id: plan.plan_id,
                kind: plan.kind,
                created_memory_unit_id: Some(created_memory_unit_id),
                affected_unit_ids,
            })
            .into_response()
        }
    }
}

pub async fn semantic_memory_execute(
    State(state): State<Arc<crate::AppState>>,
    Json(payload): Json<SemanticMemoryExecuteRequest>,
) -> axum::response::Response {
    semantic_memory_execute_internal(state, payload).await
}

pub async fn user_semantic_memory_execute(
    State(state): State<Arc<crate::AppState>>,
    Path(user_id): Path<String>,
    Json(payload): Json<UserSemanticMemoryExecuteRequest>,
) -> axum::response::Response {
    semantic_memory_execute_internal(
        state,
        SemanticMemoryExecuteRequest {
            user_id,
            plan_id: payload.plan_id,
            org_id: payload.org_id,
            confirm: payload.confirm,
            reviewer: payload.reviewer,
            note: payload.note,
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_materialize_semantic_update_source_creates_event_backed_memory() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = memorose_core::MemoroseEngine::new_with_default_threshold(
            temp_dir.path(),
            1000,
            true,
            true,
        )
        .await?;

        let extracted_facts = vec![StoredMemoryFact {
            subject: "self".into(),
            subject_ref: Some("user:test-user".into()),
            subject_name: Some("test-user".into()),
            attribute: "location".into(),
            value: "Beijing".into(),
            canonical_value: Some("beijing".into()),
            change_type: "update".into(),
            temporal_status: Some("current".into()),
            polarity: None,
            evidence_span: Some("I moved to Beijing".into()),
            confidence: 0.98,
        }];

        let plan = SemanticMemoryPlanRecord {
            plan_id: uuid::Uuid::new_v4().to_string(),
            user_id: "test-user".into(),
            org_id: Some("test-org".into()),
            instruction: "I moved to Beijing".into(),
            kind: SemanticMemoryPlanKind::Update,
            forget_mode: ForgetMode::Logical,
            linked_forget_preview_id: None,
            source_content: Some("I moved to Beijing".into()),
            extracted_facts: extracted_facts.clone(),
            planned_actions: Vec::new(),
            created_at: chrono::Utc::now(),
            expires_at: chrono::Utc::now() + chrono::Duration::minutes(10),
        };

        let source_unit = materialize_semantic_update_source(
            &engine,
            &plan,
            Some(vec![0.25; 8]),
            Some("reviewer-1"),
            Some("semantic execute"),
        )
        .await?;

        let stored_unit = engine
            .get_memory_unit_including_forgotten("test-user", source_unit.id)?
            .expect("source unit should be stored");
        assert_eq!(stored_unit.extracted_facts, extracted_facts);
        assert_eq!(stored_unit.references.len(), 1);

        let source_event_id = stored_unit.references[0];
        let source_event = engine
            .get_event("test-user", &source_event_id.to_string())
            .await?
            .expect("semantic source event should be stored");

        assert_eq!(stored_unit.stream_id, source_event.stream_id);
        assert_eq!(stored_unit.transaction_time, source_event.transaction_time);
        assert_eq!(
            source_event.metadata["source"],
            serde_json::json!("semantic_orchestrator")
        );
        assert_eq!(
            source_event.metadata["semantic_plan_id"],
            serde_json::json!(plan.plan_id)
        );
        assert_eq!(
            source_event.metadata["semantic_instruction"],
            serde_json::json!(plan.instruction)
        );
        assert_eq!(
            source_event.metadata["reviewer"],
            serde_json::json!("reviewer-1")
        );
        assert_eq!(
            source_event.metadata["note"],
            serde_json::json!("semantic execute")
        );

        let pending = engine.fetch_pending_events_limited(10).await?;
        assert!(pending.is_empty());

        Ok(())
    }
}

pub async fn apply_manual_correction(
    State(state): State<Arc<crate::AppState>>,
    Json(payload): Json<ManualCorrectionRequest>,
) -> axum::response::Response {
    if let Err(response) = validate_registry_id(payload.user_id.trim(), "user_id") {
        return response;
    }
    if let Some(org_id) = payload.org_id.as_deref() {
        if let Err(response) = validate_registry_id(org_id.trim(), "org_id") {
            return response;
        }
    }
    let Some(kind) = parse_memory_correction_kind(&payload.action) else {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "unsupported action" })),
        )
            .into_response();
    };

    let user_id = payload.user_id.trim().to_string();
    let shard = state.shard_manager.shard_for_user(&user_id);
    let source_unit = match shard
        .engine
        .get_memory_unit_including_forgotten(&user_id, payload.source_unit_id)
    {
        Ok(Some(unit)) => unit,
        Ok(None) => {
            return (
                axum::http::StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "source memory not found" })),
            )
                .into_response();
        }
        Err(error) => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Failed to load source memory: {}", error) })),
            )
                .into_response();
        }
    };
    if payload.org_id.as_deref().map_or(false, |org_id| {
        source_unit.org_id.as_deref() != Some(org_id.trim())
    }) {
        return (
            axum::http::StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "source memory scope mismatch" })),
        )
            .into_response();
    }

    let confidence = payload.confidence.unwrap_or(1.0).clamp(0.0, 1.0);
    let reviewer = payload
        .reviewer
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let reason = match (payload.reason.clone(), reviewer) {
        (Some(reason), Some(reviewer)) => format!("{} (reviewer: {})", reason, reviewer),
        (Some(reason), None) => reason,
        (None, Some(reviewer)) => format!(
            "{} by {}",
            format!("Manual {:?} correction", kind).to_ascii_lowercase(),
            reviewer
        ),
        (None, None) => format!("Manual {:?} correction", kind).to_ascii_lowercase(),
    };

    match shard
        .engine
        .apply_manual_memory_correction(
            &user_id,
            payload.source_unit_id,
            payload.target_unit_id,
            kind,
            reason,
            confidence,
            "manual_api",
        )
        .await
    {
        Ok(affected_unit_ids) => Json(ManualCorrectionResponse {
            status: "applied",
            affected_unit_ids,
        })
        .into_response(),
        Err(error) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

pub async fn list_rac_reviews(
    State(state): State<Arc<crate::AppState>>,
    Query(query): Query<RacReviewListQuery>,
) -> axum::response::Response {
    if let Some(user_id) = query.user_id.as_deref() {
        if let Err(response) = validate_registry_id(user_id.trim(), "user_id") {
            return response;
        }
    }
    if let Some(org_id) = query.org_id.as_deref() {
        if let Err(response) = validate_registry_id(org_id.trim(), "org_id") {
            return response;
        }
    }

    let status_filter = if query.status.is_some() {
        let Some(parsed) = parse_rac_review_status(query.status.as_deref()) else {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "unsupported review status" })),
            )
                .into_response();
        };
        Some(parsed)
    } else {
        Some(RacReviewStatus::Pending)
    };

    let limit = query.limit.unwrap_or(25).clamp(1, 100);
    let shard_ids: Vec<u32> = if let Some(ref user_id) = query.user_id {
        vec![memorose_common::sharding::user_id_to_shard(
            user_id,
            state.shard_manager.shard_count(),
        )]
    } else {
        state.shard_manager.all_shards().map(|(id, _)| id).collect()
    };

    let mut views = Vec::new();
    for shard_id in shard_ids {
        let Some(shard) = state.shard_manager.shard(shard_id) else {
            continue;
        };
        let records = match shard.engine.list_rac_reviews(
            status_filter.clone(),
            query.user_id.as_deref(),
            query.org_id.as_deref(),
            limit,
        ) {
            Ok(records) => records,
            Err(error) => {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": format!("Failed to load reviews: {}", error) })),
                )
                    .into_response();
            }
        };
        views.extend(
            records
                .into_iter()
                .map(|record| correction_review_view(&shard.engine, record)),
        );
    }

    views.sort_by(|left, right| right.review.created_at.cmp(&left.review.created_at));
    views.truncate(limit);
    Json(serde_json::json!({ "reviews": views })).into_response()
}

pub async fn approve_rac_review(
    State(state): State<Arc<crate::AppState>>,
    Path(review_id): Path<String>,
    Json(payload): Json<ResolveRacReviewRequest>,
) -> axum::response::Response {
    resolve_rac_review_internal(state, review_id, payload, true).await
}

pub async fn reject_rac_review(
    State(state): State<Arc<crate::AppState>>,
    Path(review_id): Path<String>,
    Json(payload): Json<ResolveRacReviewRequest>,
) -> axum::response::Response {
    resolve_rac_review_internal(state, review_id, payload, false).await
}

async fn resolve_rac_review_internal(
    state: Arc<crate::AppState>,
    review_id: String,
    payload: ResolveRacReviewRequest,
    approve: bool,
) -> axum::response::Response {
    if let Err(response) = validate_registry_id(payload.user_id.trim(), "user_id") {
        return response;
    }
    if let Some(org_id) = payload.org_id.as_deref() {
        if let Err(response) = validate_registry_id(org_id.trim(), "org_id") {
            return response;
        }
    }
    let user_id = payload.user_id.trim().to_string();
    let shard = state.shard_manager.shard_for_user(&user_id);
    let existing = match shard.engine.get_rac_review(review_id.trim()) {
        Ok(Some(review)) => review,
        Ok(None) => {
            return (
                axum::http::StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "review not found" })),
            )
                .into_response();
        }
        Err(error) => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Failed to load review: {}", error) })),
            )
                .into_response();
        }
    };
    if existing.user_id != user_id
        || payload.org_id.as_deref().map_or(false, |org_id| {
            existing.org_id.as_deref() != Some(org_id.trim())
        })
    {
        return (
            axum::http::StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "review scope mismatch" })),
        )
            .into_response();
    }

    match shard
        .engine
        .resolve_rac_review(
            review_id.trim(),
            approve,
            payload.reviewer.clone(),
            payload.note.clone(),
        )
        .await
    {
        Ok(Some(review)) => Json(serde_json::json!({
            "status": if approve { "approved" } else { "rejected" },
            "review": correction_review_view(&shard.engine, review)
        }))
        .into_response(),
        Ok(None) => (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "review not found" })),
        )
            .into_response(),
        Err(error) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}
