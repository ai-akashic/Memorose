use axum::{
    extract::State,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::types::DashboardSearchMemoryUnitView;

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
