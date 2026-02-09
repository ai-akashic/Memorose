use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use memorose_common::MemoryUnit;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

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
        .split(',').next().unwrap_or("unknown")
        .trim()
        .to_string();

    let attempts = state.login_limiter.get(&client_ip).await.unwrap_or(0);
    if attempts >= 5 {
        return (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({ "error": "Too many login attempts. Try again later." })),
        ).into_response();
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
        let hash_to_check = if auth_data.username == u {
            auth_data.password_hash.clone()
        } else {
            dummy_hash
        };
        let valid = bcrypt::verify(&password, &hash_to_check).unwrap_or(false);
        let is_valid = valid && auth_data.username == u;
        Ok((is_valid, auth_data.must_change_password))
    }).await;

    match verify_result {
        Ok(Ok((true, must_change))) => {
            state.login_limiter.invalidate(&client_ip).await;
            match state.dashboard_auth.create_token(&username) {
                Ok(token) => Json(serde_json::json!({
                    "token": token,
                    "expires_in": 86400,
                    "must_change_password": must_change,
                })).into_response(),
                Err(e) => {
                    tracing::error!("Token creation failed: {}", e);
                    (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": "Internal server error" })),
                    ).into_response()
                }
            }
        }
        Ok(Ok((false, _))) => {
            state.login_limiter.insert(client_ip, attempts + 1).await;
            (
                axum::http::StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Invalid credentials" })),
            ).into_response()
        }
        Ok(Err(e)) => {
            tracing::error!("Auth error: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Internal server error" })),
            ).into_response()
        }
        Err(e) => {
            tracing::error!("Auth task error: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Internal server error" })),
            ).into_response()
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
    }).await;

    match result {
        Ok(Ok(true)) => Json(serde_json::json!({ "status": "updated" })).into_response(),
        Ok(Ok(false)) => (
            axum::http::StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Current password is incorrect" })),
        ).into_response(),
        Ok(Err(e)) => {
            let msg = e.to_string();
            if msg.contains("at least") {
                (axum::http::StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": msg }))).into_response()
            } else {
                tracing::error!("Password change error: {}", e);
                (axum::http::StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Internal server error" }))).into_response()
            }
        }
        Err(e) => {
            tracing::error!("Password change task error: {}", e);
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Internal server error" }))).into_response()
        }
    }
}

// ── Cluster Status ────────────────────────────────────────────────

pub async fn cluster_status(
    State(state): State<Arc<crate::AppState>>,
) -> Json<serde_json::Value> {
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
        let learners: Vec<u64> = metrics.membership_config.membership().learner_ids().collect();

        shard_statuses.push(serde_json::json!({
            "shard_id": shard_id,
            "raft_node_id": metrics.id,
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

    // For backward compat: if single shard, include top-level fields
    if state.shard_manager.shard_count() <= 1 {
        if let Some(first) = shard_statuses.first() {
            let mut result = first.clone();
            result["node_id"] = result["raft_node_id"].clone();
            result["snapshot_policy_logs"] = serde_json::json!(state.config.raft.snapshot_logs);
            result["config"] = serde_json::json!({
                "heartbeat_interval_ms": state.config.raft.heartbeat_interval_ms,
                "election_timeout_min_ms": state.config.raft.election_timeout_min_ms,
                "election_timeout_max_ms": state.config.raft.election_timeout_max_ms,
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
            "election_timeout_max_ms": state.config.raft.election_timeout_max_ms,
        }
    }))
}

// ── Stats ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct StatsQuery {
    #[serde(default)]
    user_id: Option<String>,
}

pub async fn stats(
    State(state): State<Arc<crate::AppState>>,
    Query(params): Query<StatsQuery>,
) -> axum::response::Response {
    let cache_key = format!("stats:{}", params.user_id.as_deref().unwrap_or("_all"));
    if let Some(cached) = state.dashboard_cache.get(&cache_key).await {
        return Json(cached).into_response();
    }

    let user_id_filter = params.user_id.clone();

    // Determine which shards to scan
    let shard_ids: Vec<u32> = if let Some(ref uid) = user_id_filter {
        // Single shard for known user
        let sid = memorose_common::sharding::user_id_to_shard(uid, state.shard_manager.shard_count());
        vec![sid]
    } else {
        // All shards
        state.shard_manager.all_shards().map(|(id, _)| id).collect()
    };

    let mut total_pending = 0usize;
    let mut total_events = 0usize;
    let mut total_edges = 0usize;
    let mut total_units = 0usize;
    let mut total_l1 = 0usize;
    let mut total_l2 = 0usize;

    for shard_id in shard_ids {
        let shard = match state.shard_manager.shard(shard_id) {
            Some(s) => s,
            None => continue,
        };
        let engine = shard.engine.clone();
        let uid_filter = user_id_filter.clone();

        let scan_result = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let kv = engine.kv();

            let pending_count = engine.system_kv().scan(b"pending:")?.len();

            let (event_count, edge_count, units, l1_count, l2_count) = if let Some(ref uid) = uid_filter {
                let event_prefix = format!("u:{}:event:", uid);
                let event_count = kv.scan(event_prefix.as_bytes())?.len();

                let edge_prefix = format!("u:{}:edge:out:", uid);
                let edge_count = kv.scan(edge_prefix.as_bytes())?.len();

                let unit_prefix = format!("u:{}:unit:", uid);
                let unit_pairs = kv.scan(unit_prefix.as_bytes())?;
                let mut total_units = 0usize;
                let mut l1_count = 0usize;
                let mut l2_count = 0usize;
                for (_, val) in &unit_pairs {
                    if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(val) {
                        total_units += 1;
                        match unit.level {
                            1 => l1_count += 1,
                            2 => l2_count += 1,
                            _ => {}
                        }
                    }
                }
                (event_count, edge_count, total_units, l1_count, l2_count)
            } else {
                let all_pairs = kv.scan(b"u:")?;
                let mut event_count = 0usize;
                let mut edge_count = 0usize;
                let mut total_units = 0usize;
                let mut l1_count = 0usize;
                let mut l2_count = 0usize;
                for (k, val) in &all_pairs {
                    if k.windows(7).any(|w| w == b":event:") {
                        event_count += 1;
                    } else if k.windows(10).any(|w| w == b":edge:out:") {
                        edge_count += 1;
                    } else if k.windows(6).any(|w| w == b":unit:") {
                        if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(val) {
                            total_units += 1;
                            match unit.level {
                                1 => l1_count += 1,
                                2 => l2_count += 1,
                                _ => {}
                            }
                        }
                    }
                }
                (event_count, edge_count, total_units, l1_count, l2_count)
            };

            Ok((pending_count, event_count, edge_count, units, l1_count, l2_count))
        }).await;

        if let Ok(Ok((pending, events, edges, units, l1, l2))) = scan_result {
            total_pending += pending;
            total_events += events;
            total_edges += edges;
            total_units += units;
            total_l1 += l1;
            total_l2 += l2;
        }
    }

    let uptime = state.start_time.elapsed().as_secs();

    let result = serde_json::json!({
        "total_events": total_events,
        "pending_events": total_pending,
        "total_memory_units": total_units,
        "total_edges": total_edges,
        "memory_by_level": {
            "l1": total_l1,
            "l2": total_l2,
        },
        "uptime_seconds": uptime,
    });

    state.dashboard_cache.insert(cache_key, result.clone()).await;

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
    user_id: Option<String>,
}

fn default_page() -> usize { 1 }
fn default_limit() -> usize { 20 }
fn default_sort() -> String { "importance".to_string() }

pub async fn list_memories(
    State(state): State<Arc<crate::AppState>>,
    Query(params): Query<ListMemoriesQuery>,
) -> axum::response::Response {
    let level_filter = params.level;
    let sort = params.sort.clone();
    let user_id_filter = params.user_id.clone();

    // Determine which shards to scan
    let shard_ids: Vec<u32> = if let Some(ref uid) = user_id_filter {
        let sid = memorose_common::sharding::user_id_to_shard(uid, state.shard_manager.shard_count());
        vec![sid]
    } else {
        state.shard_manager.all_shards().map(|(id, _)| id).collect()
    };

    let mut all_units: Vec<MemoryUnit> = Vec::new();

    for shard_id in shard_ids {
        let shard = match state.shard_manager.shard(shard_id) {
            Some(s) => s,
            None => continue,
        };
        let engine = shard.engine.clone();
        let uid_filter = user_id_filter.clone();

        let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<MemoryUnit>> {
            let kv = engine.kv();
            let prefix = if let Some(ref uid) = uid_filter {
                format!("u:{}:unit:", uid).into_bytes()
            } else {
                b"u:".to_vec()
            };
            let pairs = kv.scan(&prefix)?;

            let units: Vec<MemoryUnit> = pairs
                .into_iter()
                .filter(|(k, _)| {
                    if uid_filter.is_none() {
                        k.windows(6).any(|w| w == b":unit:")
                    } else {
                        true
                    }
                })
                .filter_map(|(_, val)| serde_json::from_slice::<MemoryUnit>(&val).ok())
                .filter(|u| level_filter.map_or(true, |l| u.level == l))
                .take(10_000)
                .collect();

            Ok(units)
        }).await;

        if let Ok(Ok(units)) = result {
            all_units.extend(units);
        }
    }

    let total = all_units.len();

    match sort.as_str() {
        "importance" => all_units.sort_by(|a, b| b.importance.partial_cmp(&a.importance).unwrap_or(std::cmp::Ordering::Equal)),
        "access_count" => all_units.sort_by(|a, b| b.access_count.cmp(&a.access_count)),
        "recent" => all_units.sort_by(|a, b| b.transaction_time.cmp(&a.transaction_time)),
        _ => all_units.sort_by(|a, b| b.importance.partial_cmp(&a.importance).unwrap_or(std::cmp::Ordering::Equal)),
    }

    let page = params.page.max(1);
    let limit = params.limit.min(100);
    let offset = (page - 1) * limit;

    let items: Vec<serde_json::Value> = all_units
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|u| serde_json::json!({
            "id": u.id,
            "user_id": u.user_id,
            "app_id": u.app_id,
            "content": u.content,
            "level": u.level,
            "importance": u.importance,
            "keywords": u.keywords,
            "access_count": u.access_count,
            "last_accessed_at": u.last_accessed_at,
            "transaction_time": u.transaction_time,
            "reference_count": u.references.len(),
            "has_assets": !u.assets.is_empty(),
        }))
        .collect();

    Json(serde_json::json!({
        "items": items,
        "total": total,
        "page": page,
        "limit": limit,
    })).into_response()
}

pub async fn get_memory(
    State(state): State<Arc<crate::AppState>>,
    Path(id): Path<String>,
) -> axum::response::Response {
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Invalid memory ID format" })),
        ).into_response(),
    };

    // Try all shards (shard count is small, acceptable overhead)
    for (_, shard) in state.shard_manager.all_shards() {
        match shard.engine.get_memory_unit_by_index(uuid).await {
            Ok(Some(mut unit)) => {
                unit.embedding = None;
                return Json(serde_json::json!(unit)).into_response();
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
    ).into_response()
}

// ── Graph ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct GraphQuery {
    #[serde(default = "default_graph_limit")]
    limit: usize,
    #[serde(default)]
    user_id: Option<String>,
}

fn default_graph_limit() -> usize { 500 }

pub async fn graph_data(
    State(state): State<Arc<crate::AppState>>,
    Query(params): Query<GraphQuery>,
) -> axum::response::Response {
    let limit = params.limit.min(1000);
    let user_id_filter = params.user_id.clone();

    // Determine which shards to scan
    let shard_ids: Vec<u32> = if let Some(ref uid) = user_id_filter {
        let sid = memorose_common::sharding::user_id_to_shard(uid, state.shard_manager.shard_count());
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

        let result: anyhow::Result<serde_json::Value> = async move {
            let kv = engine.kv();
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
            let node_keys: Vec<String> = node_ids_vec.iter().map(|id| format!("idx:unit:{}", id)).collect();
            let key_refs: Vec<&[u8]> = node_keys.iter().map(|k| k.as_bytes()).collect();

            if !key_refs.is_empty() {
                let idx_values = kv.multi_get(&key_refs)?;
                for (i, idx_val) in idx_values.into_iter().enumerate() {
                    if let Some(uid_bytes) = idx_val {
                        let uid = String::from_utf8_lossy(&uid_bytes);
                        let unit_id = node_ids_vec[i];
                        let unit_key = format!("u:{}:unit:{}", uid, unit_id);
                        if let Ok(Some(bytes)) = kv.get(unit_key.as_bytes()) {
                            if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(&bytes) {
                                let label = if unit.content.chars().count() > 80 {
                                    let end = unit.content.char_indices().nth(80).map(|(i, _)| i).unwrap_or(unit.content.len());
                                    format!("{}...", &unit.content[..end])
                                } else {
                                    unit.content.clone()
                                };
                                nodes.push(serde_json::json!({
                                    "id": unit.id,
                                    "label": label,
                                    "level": unit.level,
                                    "importance": unit.importance,
                                    "user_id": unit.user_id,
                                }));
                            }
                        }
                    }
                }
            }

            let mut relation_dist: HashMap<String, usize> = HashMap::new();
            let edge_data: Vec<serde_json::Value> = edges.iter().map(|e| {
                let rel = format!("{:?}", e.relation);
                *relation_dist.entry(rel.clone()).or_default() += 1;
                serde_json::json!({
                    "source": e.source_id,
                    "target": e.target_id,
                    "relation": rel,
                    "weight": e.weight,
                })
            }).collect();

            Ok(serde_json::json!({
                "nodes": nodes,
                "edges": edge_data,
                "edge_count": edges.len(),
                "relation_distribution": relation_dist,
            }))
        }.await;

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
                    *all_relation_dist.entry(k.clone()).or_default() += v.as_u64().unwrap_or(0) as usize;
                }
            }
        }
    }

    let nodes = if all_nodes.len() > limit { all_nodes[..limit].to_vec() } else { all_nodes.clone() };

    let retained: std::collections::HashSet<String> = nodes.iter()
        .filter_map(|n| n["id"].as_str().map(String::from))
        .collect();
    let filtered_edges: Vec<_> = all_edge_data.into_iter()
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
    })).into_response()
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
    app_id: Option<String>,
}

fn default_search_mode() -> String { "hybrid".to_string() }
fn default_search_limit() -> usize { 10 }

pub async fn search(
    State(state): State<Arc<crate::AppState>>,
    Json(payload): Json<SearchRequest>,
) -> axum::response::Response {
    let limit = payload.limit.min(100);
    let start = std::time::Instant::now();
    let user_id = payload.user_id.as_deref().unwrap_or("_legacy");
    let app_id = payload.app_id.as_deref();

    // Route to the correct shard for this user
    let shard = state.shard_manager.shard_for_user(user_id);

    let results = match payload.mode.as_str() {
        "text" => {
            match shard.engine.search_text(
                user_id,
                app_id,
                &payload.query,
                limit,
                payload.enable_arbitration,
                None,
            ).await {
                Ok(units) => units.into_iter().map(|u| (u, 0.0f32)).collect::<Vec<_>>(),
                Err(e) => {
                    return (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": e.to_string() })),
                    ).into_response();
                }
            }
        }
        "vector" => {
            match state.llm_client.embed(&payload.query).await {
                Ok(embedding) => {
                    let filter = shard.engine.build_user_filter(user_id, app_id, None);
                    match shard.engine.search_similar(user_id, app_id, &embedding, limit, filter).await {
                        Ok(results) => results,
                        Err(e) => {
                            return (
                                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                                Json(serde_json::json!({ "error": e.to_string() })),
                            ).into_response();
                        }
                    }
                }
                Err(e) => {
                    return (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": format!("Embedding failed: {}", e) })),
                    ).into_response();
                }
            }
        }
        _ => {
            // hybrid (default)
            match state.llm_client.embed(&payload.query).await {
                Ok(embedding) => {
                    match shard.engine.search_hybrid(
                        user_id,
                        app_id,
                        &payload.query,
                        &embedding,
                        limit,
                        payload.enable_arbitration,
                        None,
                        1,
                        None,
                        None,
                    ).await {
                        Ok(results) => results,
                        Err(e) => {
                            return (
                                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                                Json(serde_json::json!({ "error": e.to_string() })),
                            ).into_response();
                        }
                    }
                }
                Err(e) => {
                    return (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": format!("Embedding failed: {}", e) })),
                    ).into_response();
                }
            }
        }
    };

    let query_time_ms = start.elapsed().as_millis();

    let result_items: Vec<serde_json::Value> = results.into_iter().map(|(mut u, score)| {
        u.embedding = None;
        serde_json::json!({
            "unit": u,
            "score": score,
        })
    }).collect();

    Json(serde_json::json!({
        "results": result_items,
        "query_time_ms": query_time_ms,
    })).into_response()
}

// ── Config ────────────────────────────────────────────────────────

pub async fn get_config(
    State(state): State<Arc<crate::AppState>>,
) -> Json<serde_json::Value> {
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

// ── Version ───────────────────────────────────────────────────────

pub async fn version() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "build_time": env!("BUILD_TIME"),
        "features": ["raft", "bitemporal", "knowledge-graph", "dashboard", "sharding"],
    }))
}
