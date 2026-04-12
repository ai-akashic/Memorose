use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use memorose_common::Event as MemoryEvent;
use serde::Deserialize;
use std::sync::Arc;

use super::types::*;

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
                            matches_dashboard_org_scope(u.org_id.as_deref(), org_filter.as_deref())
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
                        .filter(|event| {
                            !engine
                                .is_event_forgotten(&event.user_id, &event.id.to_string())
                                .unwrap_or(false)
                        })
                        .filter(|e| {
                            matches_dashboard_org_scope(e.org_id.as_deref(), org_filter.as_deref())
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
