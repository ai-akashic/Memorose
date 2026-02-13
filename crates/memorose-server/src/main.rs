use axum::{
    extract::{Path, State},
    routing::{get, post, delete},
    Json, Router,
    response::IntoResponse,
    middleware as axum_middleware,
};
use memorose_common::{Event, EventContent, GraphEdge, MemoryUnit, RelationType, TimeRange, config::AppConfig};
use memorose_common::sharding::decode_raft_node_id;
use memorose_core::{MemoroseEngine, LLMClient, GeminiClient};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;
use moka::future::Cache;
use tower_http::trace::TraceLayer;
use tower_http::services::ServeDir;
use tower::ServiceBuilder;
use chrono::{DateTime, Utc};

mod dashboard;
mod shard_manager;

use shard_manager::ShardManager;

struct AppState {
    shard_manager: ShardManager,
    llm_client: Arc<dyn LLMClient>,
    embedding_cache: Cache<String, Vec<f32>>,
    config: AppConfig,
    start_time: std::time::Instant,
    dashboard_auth: dashboard::auth::DashboardAuth,
    login_limiter: Cache<String, u32>,
    dashboard_cache: Cache<String, serde_json::Value>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    match dotenvy::dotenv() {
        Ok(path) => tracing::info!("Loaded .env from: {:?}", path),
        Err(e) => tracing::warn!("Failed to load .env file: {}. Using system environment variables.", e),
    }

    let google_key = std::env::var("GOOGLE_API_KEY").unwrap_or_default();
    tracing::info!("GOOGLE_API_KEY loaded: {} (len={})", !google_key.is_empty(), google_key.len());

    let config = AppConfig::load().expect("Failed to load configuration");
    tracing::info!("Using LLM Provider: {:?}", config.llm.provider);
    tracing::info!("Using LLM Model: {}", config.llm.model);
    tracing::info!("Using Embedding Model: {}", config.llm.embedding_model);

    let data_dir = config.storage.root_dir.clone();

    // Initialize shard manager (handles engine, raft, workers for all shards)
    let shard_manager = if config.is_sharded() {
        tracing::info!("Starting in sharded mode: {} shards, physical_node_id={}",
            config.shard_count(), config.physical_node_id());
        ShardManager::new(&config).await.expect("Failed to start ShardManager")
    } else {
        tracing::info!("Starting in single-shard mode (node_id={})", config.raft.node_id);
        ShardManager::new_single_shard(&config).await.expect("Failed to start single-shard ShardManager")
    };

    let api_key = config.get_active_key().expect("Fatal: API Key is required for Memorose Server");
    let llm_client: Arc<dyn LLMClient> = Arc::new(GeminiClient::new(
        api_key,
        config.get_model_name(),
        config.get_embedding_model_name(),
    ));
    tracing::info!("Initialized Gemini client (model: {}, embedding: {})",
        config.get_model_name(), config.get_embedding_model_name());

    let embedding_cache = Cache::new(10_000);

    // Initialize dashboard auth
    let auth_dir = std::path::Path::new(&data_dir);
    let dashboard_auth = dashboard::auth::DashboardAuth::new(auth_dir)
        .expect("Failed to initialize dashboard auth");

    let login_limiter = Cache::builder()
        .time_to_live(std::time::Duration::from_secs(900))
        .max_capacity(10_000)
        .build();

    let dashboard_cache = Cache::builder()
        .time_to_live(std::time::Duration::from_secs(300))
        .max_capacity(100)
        .build();

    let state = Arc::new(AppState {
        shard_manager,
        llm_client,
        embedding_cache,
        config: config.clone(),
        start_time: std::time::Instant::now(),
        dashboard_auth,
        login_limiter,
        dashboard_cache,
    });

    // Dashboard API routes (auth-protected)
    let dashboard_protected = Router::new()
        .route("/auth/password", post(dashboard::handlers::change_password))
        .route("/cluster/status", get(dashboard::handlers::cluster_status))
        .route("/stats", get(dashboard::handlers::stats))
        .route("/memories", get(dashboard::handlers::list_memories))
        .route("/memories/:id", get(dashboard::handlers::get_memory))
        .route("/graph", get(dashboard::handlers::graph_data))
        .route("/search", post(dashboard::handlers::search))
        .route("/chat", post(dashboard::handlers::chat))
        .route("/config", get(dashboard::handlers::get_config))
        .route("/version", get(dashboard::handlers::version))
        .route("/apps", get(dashboard::handlers::list_apps))
        .route("/apps/:app_id/stats", get(dashboard::handlers::get_app_stats))
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            dashboard::auth::auth_middleware,
        ));

    // Dashboard public routes (no auth)
    let dashboard_public = Router::new()
        .route("/auth/login", post(dashboard::handlers::login));

    let dashboard_routes = Router::new()
        .merge(dashboard_public)
        .merge(dashboard_protected);

    // Static file serving for dashboard UI
    let dashboard_dir = ["static/dashboard", "crates/memorose-server/static/dashboard"]
        .iter()
        .map(std::path::PathBuf::from)
        .chain(std::env::current_exe().ok().and_then(|p| p.parent().map(|d| d.join("static/dashboard"))))
        .find(|p| p.join("_next").exists() || p.join("index.html").exists())
        .unwrap_or_else(|| std::path::PathBuf::from("static/dashboard"));

    tracing::info!("Dashboard static dir: {:?} (exists: {})", dashboard_dir, dashboard_dir.exists());

    // Ensure root index.html exists (Next.js static export may not generate one)
    let root_index = dashboard_dir.join("index.html");
    if dashboard_dir.exists() && !root_index.exists() {
        let redirect_html = r#"<!DOCTYPE html><html><head><meta http-equiv="refresh" content="0;url=/dashboard/login/"></head></html>"#;
        let _ = std::fs::write(&root_index, redirect_html);
    }

    let dashboard_static = ServeDir::new(&dashboard_dir)
        .append_index_html_on_directories(true);

    let app = Router::new()
        .route("/", get(root))
        .route("/v1/users/:user_id/apps/:app_id/streams/:stream_id/events", post(ingest_event))
        .route("/v1/users/:user_id/apps/:app_id/streams/:stream_id/retrieve", post(retrieve_memory))
        .route("/v1/users/:user_id/apps/:app_id/streams/:stream_id/tasks/tree", get(get_task_tree))
        .route("/v1/users/:user_id/graph/edges", post(add_edge))
        .route("/v1/status/pending", get(pending_count))
        .route("/v1/cluster/initialize", post(initialize_cluster))
        .route("/v1/cluster/join", post(join_cluster))
        .route("/v1/cluster/nodes/:node_id", delete(leave_cluster))
        .nest("/v1/dashboard", dashboard_routes)
        .nest_service("/dashboard", dashboard_static)
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .into_inner(),
        )
        .with_state(state.clone());

    // Determine HTTP listen address
    let http_addr: SocketAddr = if config.is_sharded() {
        let sharding = config.sharding.as_ref().unwrap();
        let this_node = sharding.nodes.iter()
            .find(|n| n.id == sharding.physical_node_id)
            .expect("Physical node not found in sharding.nodes");
        this_node.http_addr.parse().expect("Invalid http_addr in sharding config")
    } else {
        let http_port = 3000 + (config.raft.node_id as u16 - 1);
        SocketAddr::from(([127, 0, 0, 1], http_port))
    };

    tracing::info!("HTTP API listening on {}", http_addr);
    tracing::info!("Dashboard available at http://{}/dashboard", http_addr);
    let listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();

    let state_for_shutdown = state.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install CTRL+C handler");
            tracing::info!("Shutdown signal received. Stopping Raft nodes...");
            state_for_shutdown.shard_manager.shutdown_all().await;
        })
        .await
        .unwrap();

    tracing::info!("Memorose Server stopped.");
}

/// Build a "Not Leader" response with shard info when applicable.
fn not_leader_response(current_leader: Option<u64>, is_sharded: bool) -> axum::response::Response {
    if is_sharded {
        let (shard_id, leader_physical_node) = current_leader
            .map(|id| decode_raft_node_id(id))
            .unwrap_or((0, 0));
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Not Leader",
                "current_leader": current_leader,
                "shard_id": shard_id,
                "leader_physical_node": leader_physical_node,
            }))
        ).into_response()
    } else {
        let hint = current_leader.map(|id| {
            format!("Node {} (Try Port {})", id, 3000 + id - 1)
        }).unwrap_or_else(|| "Unknown".to_string());
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Not Leader",
                "current_leader": current_leader,
                "hint": hint,
            }))
        ).into_response()
    }
}

/// Forward request to leader node
async fn forward_to_leader<T: serde::Serialize>(
    _state: &AppState,
    leader_id: u64,
    path: &str,
    payload: &T,
) -> Result<axum::response::Response, axum::response::Response> {
    // Calculate leader address
    let leader_port = 3000 + leader_id - 1;
    let leader_url = format!("http://localhost:{}{}", leader_port, path);

    tracing::info!("Forwarding request to leader node {} at {}", leader_id, leader_url);

    // Create HTTP client (can be optimized to reuse)
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| {
            tracing::error!("Failed to create HTTP client: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to create HTTP client",
                    "details": e.to_string()
                }))
            ).into_response()
        })?;

    // Forward request
    let response = client
        .post(&leader_url)
        .json(payload)
        .send()
        .await
        .map_err(|e| {
            tracing::error!("Failed to forward to leader: {}", e);
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": "Failed to forward to leader",
                    "leader_id": leader_id,
                    "details": e.to_string()
                }))
            ).into_response()
        })?;

    // Convert response
    let status = response.status();
    let headers = response.headers().clone();
    let body_bytes = response.bytes().await.map_err(|e| {
        tracing::error!("Failed to read leader response: {}", e);
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Failed to read leader response",
                "details": e.to_string()
            }))
        ).into_response()
    })?;

    // Build response
    let mut builder = axum::http::Response::builder().status(status);

    // Copy important headers
    for (key, value) in headers.iter() {
        if key == "content-type" || key == "content-length" {
            builder = builder.header(key, value);
        }
    }

    builder
        .body(axum::body::Body::from(body_bytes))
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to build response",
                    "details": e.to_string()
                }))
            ).into_response()
        })
}

async fn add_edge(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
    Json(payload): Json<AddEdgeRequest>,
) -> axum::response::Response {
    let shard = state.shard_manager.shard_for_user(&user_id);
    let metrics = shard.raft.metrics().borrow().clone();
    let current_leader = metrics.current_leader;
    let node_id = metrics.id;

    // Forward to leader if not leader
    if current_leader != Some(node_id) {
        if let Some(leader_id) = current_leader {
            let path = format!("/v1/users/{}/graph/edges", user_id);

            tracing::info!(
                "Not leader (I'm {}, leader is {}), forwarding request",
                node_id,
                leader_id
            );

            match forward_to_leader(&state, leader_id, &path, &payload).await {
                Ok(response) => return response,
                Err(err_response) => return err_response,
            }
        }

        // If no leader, return error
        return not_leader_response(current_leader, state.config.is_sharded());
    }

    let edge = GraphEdge::new(user_id, payload.source_id, payload.target_id, payload.relation, payload.weight.unwrap_or(1.0));

    match shard.raft.client_write(memorose_core::raft::types::ClientRequest::UpdateGraph(edge)).await {
        Ok(_) => Json(serde_json::json!({ "status": "accepted" })).into_response(),
        Err(e) => {
            tracing::error!("Raft write error (graph): {:?}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() }))
            ).into_response()
        }
    }
}

#[derive(Deserialize, Serialize)]
struct AddEdgeRequest {
    source_id: Uuid,
    target_id: Uuid,
    relation: RelationType,
    weight: Option<f32>,
}

async fn root() -> &'static str {
    "Memorose is running."
}

/// Returns the number of pending (un-consolidated) events across all shards.
/// Useful for benchmarks to poll until consolidation is complete.
async fn pending_count(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let mut total_pending: usize = 0;
    for (_shard_id, shard) in state.shard_manager.all_shards() {
        if let Ok(events) = shard.engine.fetch_pending_events().await {
            total_pending += events.len();
        }
    }
    Json(serde_json::json!({
        "pending": total_pending,
        "ready": total_pending == 0,
    }))
}

#[derive(Deserialize, Serialize)]
struct IngestRequest {
    content: String,
    #[serde(default = "default_content_type")]
    content_type: String,
    #[serde(default)]
    level: Option<u8>,
    #[serde(default)]
    parent_id: Option<String>,
    #[serde(default)]
    task_status: Option<String>,
    #[serde(default)]
    task_progress: Option<f32>,
}

fn default_content_type() -> String {
    "text".to_string()
}

async fn ingest_event(
    State(state): State<Arc<AppState>>,
    Path((user_id, app_id, stream_id)): Path<(String, String, Uuid)>,
    Json(payload): Json<IngestRequest>,
) -> axum::response::Response {
    let shard = state.shard_manager.shard_for_user(&user_id);
    let metrics = shard.raft.metrics().borrow().clone();
    let current_leader = metrics.current_leader;
    let node_id = metrics.id;

    // Forward to leader if not leader
    if current_leader != Some(node_id) {
        if let Some(leader_id) = current_leader {
            let path = format!(
                "/v1/users/{}/apps/{}/streams/{}/events",
                user_id, app_id, stream_id
            );

            tracing::info!(
                "Not leader (I'm {}, leader is {}), forwarding request",
                node_id,
                leader_id
            );

            match forward_to_leader(&state, leader_id, &path, &payload).await {
                Ok(response) => return response,
                Err(err_response) => return err_response,
            }
        }

        // If no leader, return error
        return not_leader_response(current_leader, state.config.is_sharded());
    }

    let content = match payload.content_type.to_lowercase().as_str() {
        "image" => EventContent::Image(payload.content),
        "audio" => EventContent::Audio(payload.content),
        "json" => EventContent::Json(serde_json::from_str(&payload.content).unwrap_or(serde_json::json!({"error": "invalid json"}))),
        _ => EventContent::Text(payload.content),
    };
    let mut event = Event::new(user_id, app_id, stream_id, content);
    if let Some(l) = payload.level {
        event.metadata["target_level"] = serde_json::json!(l);
    }
    if let Some(ref p) = payload.parent_id {
        event.metadata["parent_id"] = serde_json::json!(p);
    }
    if let Some(ref s) = payload.task_status {
        event.metadata["task_status"] = serde_json::json!(s);
    }
    if let Some(p) = payload.task_progress {
        event.metadata["task_progress"] = serde_json::json!(p);
    }
    let event_id = event.id;

    match shard.raft.client_write(memorose_core::raft::types::ClientRequest::IngestEvent(event)).await {
        Ok(_) => Json(serde_json::json!({
            "status": "accepted",
            "event_id": event_id
        })).into_response(),
        Err(e) => {
            tracing::error!("Raft write error: {:?}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "status": "error",
                    "message": e.to_string()
                }))
            ).into_response()
        }
    }
}

#[derive(Deserialize)]
struct RetrieveRequest {
    query: String,
    #[serde(default)]
    include_vector: bool,
    #[serde(default)]
    enable_arbitration: bool,
    #[serde(default)]
    min_score: Option<f32>,
    #[serde(default = "default_graph_depth")]
    graph_depth: usize,
    #[serde(default)]
    start_time: Option<DateTime<Utc>>,
    #[serde(default)]
    end_time: Option<DateTime<Utc>>,
    #[serde(default)]
    as_of: Option<DateTime<Utc>>,
}

fn default_graph_depth() -> usize {
    1
}

async fn retrieve_memory(
    State(state): State<Arc<AppState>>,
    Path((user_id, app_id, stream_id)): Path<(String, String, Uuid)>,
    Json(payload): Json<RetrieveRequest>,
) -> axum::response::Response {
    let shard = state.shard_manager.shard_for_user(&user_id);

    let query_key = payload.query.clone();
    let embedding_f32 = if let Some(cached) = state.embedding_cache.get(&query_key).await {
        tracing::debug!("Embedding Cache Hit for: '{}'", query_key);
        Ok(cached)
    } else {
        tracing::debug!("Embedding Cache Miss. Calling LLM...");
        let res = state.llm_client.embed(&payload.query).await;
        if let Ok(ref vec) = res {
            state.embedding_cache.insert(query_key, vec.clone()).await;
        }
        res
    };

    match embedding_f32 {
        Ok(embedding_f32) => {
            let valid_range = if payload.start_time.is_some() || payload.end_time.is_some() {
                Some(TimeRange {
                    start: payload.start_time,
                    end: payload.end_time,
                })
            } else {
                None
            };

            let tx_range = payload.as_of.map(|t| TimeRange {
                start: None,
                end: Some(t),
            });

            match shard.engine.search_hybrid(
                &user_id,
                Some(&app_id),
                &payload.query,
                &embedding_f32,
                5,
                payload.enable_arbitration,
                payload.min_score,
                payload.graph_depth,
                valid_range,
                tx_range,
            ).await {
                Ok(units) => {
                    let processed_units: Vec<_> = units.into_iter().map(|(mut u, score)| {
                        if !payload.include_vector {
                            u.embedding = None;
                        }
                        (u, score)
                    }).collect();

                    Json(serde_json::json!({
                        "stream_id": stream_id,
                        "query": payload.query,
                        "results": processed_units
                    })).into_response()
                },
                Err(e) => {
                    tracing::error!("Search error: {:?}", e);
                    (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": e.to_string() }))
                    ).into_response()
                }
            }
        },
        Err(e) => {
             tracing::error!("Embedding error: {:?}", e);
             (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Failed to generate embedding: {:?}", e) }))
             ).into_response()
        }
    }
}

#[derive(Deserialize)]
struct JoinRequest {
    node_id: u32,
    #[serde(default)]
    address: String,
}

async fn initialize_cluster(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let results = state.shard_manager.initialize_all(&state.config).await;
    Json(serde_json::json!({
        "status": "initialized",
        "shards": results,
    }))
}

async fn join_cluster(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<JoinRequest>,
) -> Json<serde_json::Value> {
    if state.config.is_sharded() {
        // Multi-shard: join all raft groups
        let results = state.shard_manager.join_all(payload.node_id, &state.config).await;
        Json(serde_json::json!({
            "status": "joined",
            "node_id": payload.node_id,
            "shards": results,
        }))
    } else {
        // Single-shard: legacy behavior with address field
        let shard = state.shard_manager.shard(0).unwrap();
        let node_id = payload.node_id as u64;

        // Check if already a voter — idempotent on restart
        let metrics = shard.raft.metrics().borrow().clone();
        let existing_voters: std::collections::BTreeSet<u64> = metrics.membership_config.membership().voter_ids().collect();
        if existing_voters.contains(&node_id) {
            return Json(serde_json::json!({
                "status": "already_joined",
                "node_id": node_id,
                "role": "voter"
            }));
        }

        // Wait for leader election if needed (up to 10s)
        let mut leader = metrics.current_leader;
        if leader.is_none() {
            for _ in 0..20 {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                leader = shard.raft.metrics().borrow().current_leader;
                if leader.is_some() { break; }
            }
        }
        if leader.is_none() {
            return Json(serde_json::json!({
                "error": "No leader elected yet, try again later"
            }));
        }

        let node = openraft::BasicNode { addr: payload.address.clone() };

        match shard.raft.add_learner(node_id, node, true).await {
            Ok(_) => {}
            Err(e) => {
                return Json(serde_json::json!({ "error": format!("Add learner failed: {:?}", e) }));
            }
        }

        tokio::task::yield_now().await;

        let metrics = shard.raft.metrics().borrow().clone();
        let mut members: std::collections::BTreeSet<u64> = metrics.membership_config.membership().voter_ids().collect();
        members.insert(node_id);

        match shard.raft.change_membership(members, false).await {
            Ok(_) => Json(serde_json::json!({
                "status": "joined",
                "node_id": node_id,
                "role": "voter"
            })),
            Err(e) => Json(serde_json::json!({
                "error": format!("Change membership failed: {:?}", e)
            })),
        }
    }
}

async fn leave_cluster(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<u32>,
) -> Json<serde_json::Value> {
    if state.config.is_sharded() {
        let results = state.shard_manager.leave_all(node_id).await;
        Json(serde_json::json!({
            "status": "left",
            "node_id": node_id,
            "shards": results,
        }))
    } else {
        let shard = state.shard_manager.shard(0).unwrap();
        let metrics = shard.raft.metrics().borrow().clone();
        let mut members: std::collections::BTreeSet<u64> = metrics.membership_config.membership().voter_ids().collect();

        if !members.remove(&(node_id as u64)) {
            return Json(serde_json::json!({ "error": "Node not found in cluster" }));
        }

        match shard.raft.change_membership(members, false).await {
            Ok(_) => Json(serde_json::json!({
                "status": "left",
                "node_id": node_id
            })),
            Err(e) => Json(serde_json::json!({
                "error": format!("Remove node failed: {:?}", e)
            })),
        }
    }
}

#[derive(serde::Serialize)]
struct TaskTreeNode {
    unit: MemoryUnit,
    children: Vec<TaskTreeNode>,
}

async fn get_task_tree(
    State(state): State<Arc<AppState>>,
    Path((user_id, app_id, stream_id)): Path<(String, String, Uuid)>,
) -> axum::response::Response {
    let shard = state.shard_manager.shard_for_user(&user_id);
    let kv = shard.engine.kv();
    let prefix = format!("u:{}:unit:", user_id);
    let pairs = match kv.scan(prefix.as_bytes()) {
        Ok(p) => p,
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let all_units: Vec<MemoryUnit> = pairs.into_iter()
        .filter_map(|(_, v)| serde_json::from_slice::<MemoryUnit>(&v).ok())
        .filter(|u| u.stream_id == stream_id && (app_id.is_empty() || u.app_id == app_id))
        .collect();

    let mut root_nodes = Vec::new();
    for unit in all_units.iter().filter(|u| u.level == 3) {
        root_nodes.push(build_tree_node(unit, &all_units, &shard.engine, &user_id, 0).await);
    }

    Json(root_nodes).into_response()
}

use async_recursion::async_recursion;

const MAX_TASK_DEPTH: usize = 10;

#[async_recursion]
async fn build_tree_node(unit: &MemoryUnit, all: &[MemoryUnit], engine: &MemoroseEngine, user_id: &str, depth: usize) -> TaskTreeNode {
    let mut children = Vec::new();

    if depth < MAX_TASK_DEPTH {
        if let Ok(incoming) = engine.graph().get_incoming_edges(user_id, unit.id).await {
            for edge in incoming {
                if edge.relation == RelationType::IsSubTaskOf {
                    if let Some(child_unit) = all.iter().find(|u| u.id == edge.source_id) {
                        children.push(build_tree_node(child_unit, all, engine, user_id, depth + 1).await);
                    }
                }
            }
        }
    }

    TaskTreeNode {
        unit: unit.clone(),
        children,
    }
}
