use axum::{
    extract::{OriginalUri, Path, State},
    middleware as axum_middleware,
    response::{IntoResponse, Redirect},
    routing::{delete, get, post, put},
    Json, Router,
};
use chrono::{DateTime, Utc};
use memorose_common::sharding::decode_raft_node_id;
use memorose_common::{
    config::AppConfig, Event, EventContent, GraphEdge, MemoryType, MemoryUnit, RelationType,
    TimeRange,
};
use memorose_core::{LLMClient, MemoroseEngine};
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

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
    management_registry: dashboard::registry::ManagementRegistry,
    login_limiter: Cache<String, u32>,
    dashboard_cache: Cache<String, serde_json::Value>,
    /// Shared HTTP client for leader-forwarding; reusing it preserves connection pools.
    http_client: reqwest::Client,
}

#[derive(Clone, Serialize)]
struct RetrievalMemoryUnitView {
    id: Uuid,
    memory_type: MemoryType,
    content: String,
    keywords: Vec<String>,
    level: u8,
}

impl From<&MemoryUnit> for RetrievalMemoryUnitView {
    fn from(unit: &MemoryUnit) -> Self {
        Self {
            id: unit.id,
            memory_type: unit.memory_type.clone(),
            content: unit.content.clone(),
            keywords: unit.keywords.clone(),
            level: unit.level,
        }
    }
}

#[derive(Clone, Serialize)]
struct GoalMemoryUnitView {
    id: Uuid,
    content: String,
    transaction_time: DateTime<Utc>,
}

impl From<&MemoryUnit> for GoalMemoryUnitView {
    fn from(unit: &MemoryUnit) -> Self {
        Self {
            id: unit.id,
            content: unit.content.clone(),
            transaction_time: unit.transaction_time,
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    match dotenvy::dotenv() {
        Ok(path) => tracing::info!("Loaded .env from: {:?}", path),
        Err(e) => tracing::warn!(
            "Failed to load .env file: {}. Using system environment variables.",
            e
        ),
    }

    let google_key = std::env::var("GOOGLE_API_KEY").unwrap_or_default();
    tracing::info!(
        "GOOGLE_API_KEY loaded: {} (len={})",
        !google_key.is_empty(),
        google_key.len()
    );

    let config = AppConfig::load().expect("Failed to load configuration");
    tracing::info!("Using LLM Provider: {:?}", config.llm.provider);
    tracing::info!("Using LLM Model: {}", config.llm.model);
    tracing::info!("Using Embedding Model: {}", config.llm.embedding_model);

    let data_dir = config.storage.root_dir.clone();

    // Initialize shard manager (handles engine, raft, workers for all shards)
    let shard_manager = if config.is_sharded() {
        tracing::info!(
            "Starting in sharded mode: {} shards, physical_node_id={}",
            config.shard_count(),
            config.physical_node_id()
        );
        ShardManager::new(&config)
            .await
            .expect("Failed to start ShardManager")
    } else {
        tracing::info!(
            "Starting in single-shard mode (node_id={})",
            config.raft.node_id
        );
        ShardManager::new_single_shard(&config)
            .await
            .expect("Failed to start single-shard ShardManager")
    };

    let llm_client: Arc<dyn LLMClient> = memorose_core::llm::create_llm_client(&config.llm).expect(
        "Fatal: API Key is required. Set GOOGLE_API_KEY (Gemini) or OPENAI_API_KEY (OpenAI).",
    );
    tracing::info!(
        "Initialized {:?} LLM client (model: {}, embedding: {})",
        config.llm.provider,
        config.get_model_name(),
        config.get_embedding_model_name()
    );

    let embedding_cache = Cache::new(10_000);

    // Initialize dashboard auth
    let auth_dir = std::path::Path::new(&data_dir);
    let dashboard_auth =
        dashboard::auth::DashboardAuth::new(auth_dir).expect("Failed to initialize dashboard auth");
    let management_registry = dashboard::registry::ManagementRegistry::new(auth_dir)
        .expect("Failed to initialize dashboard registry");

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
        management_registry,
        login_limiter,
        dashboard_cache,
        http_client: reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client"),
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
        .route(
            "/organizations",
            get(dashboard::handlers::list_organizations)
                .post(dashboard::handlers::create_organization),
        )
        .route(
            "/api-keys",
            get(dashboard::handlers::list_api_keys)
                .post(dashboard::handlers::create_api_key),
        )
        .route(
            "/api-keys/:key_id",
            delete(dashboard::handlers::revoke_api_key),
        )
        .route(
            "/organizations/:org_id/knowledge",
            get(dashboard::handlers::list_organization_knowledge),
        )
        .route(
            "/organizations/:org_id/knowledge/:id",
            get(dashboard::handlers::get_organization_knowledge),
        )
        .route(
            "/organizations/:org_id/knowledge/metrics",
            get(dashboard::handlers::get_organization_knowledge_metrics),
        )
        .route("/agents", get(dashboard::handlers::list_agents))
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            dashboard::auth::auth_middleware,
        ));

    // Dashboard public routes (no auth)
    let dashboard_public = Router::new().route("/auth/login", post(dashboard::handlers::login));

    let dashboard_routes = Router::new()
        .merge(dashboard_public)
        .merge(dashboard_protected);

    // API routes that require optional key auth
    let v1_routes = Router::new()
        .route(
            "/v1/users/:user_id/streams/:stream_id/events",
            post(ingest_event),
        )
        .route(
            "/v1/users/:user_id/streams/:stream_id/retrieve",
            post(retrieve_memory),
        )
        .route(
            "/v1/users/:user_id/streams/:stream_id/tasks/tree",
            get(get_task_tree),
        )
        .route("/v1/users/:user_id/tasks/tree", get(get_all_task_trees))
        .route("/v1/users/:user_id/tasks/ready", get(get_ready_tasks))
        .route(
            "/v1/users/:user_id/tasks/:task_id/status",
            put(update_task_status),
        )
        .route("/v1/users/:user_id/graph/edges", post(add_edge))
        .route("/v1/status/pending", get(pending_count))
        .route(
            "/v1/organizations/:org_id/knowledge",
            get(dashboard::handlers::list_organization_knowledge),
        )
        .route(
            "/v1/organizations/:org_id/knowledge/:id",
            get(dashboard::handlers::get_organization_knowledge),
        )
        .route(
            "/v1/organizations/:org_id/knowledge/metrics",
            get(dashboard::handlers::get_organization_knowledge_metrics),
        )
        .route("/v1/cluster/initialize", post(initialize_cluster))
        .route("/v1/cluster/join", post(join_cluster))
        .route("/v1/cluster/nodes/:node_id", delete(leave_cluster))
        .route_layer(axum_middleware::from_fn_with_state(
            state.clone(),
            api_key_auth,
        ));

    let app = Router::new()
        .route("/", get(root))
        .merge(v1_routes)
        .nest("/v1/dashboard", dashboard_routes)
        .route("/dashboard", get(redirect_dashboard_ui))
        .route("/dashboard/*path", get(redirect_dashboard_ui))
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .into_inner(),
        )
        .with_state(state.clone());

    // Determine HTTP listen address
    let http_addr: SocketAddr = if config.is_sharded() {
        let sharding = config.sharding.as_ref().unwrap();
        let this_node = sharding
            .nodes
            .iter()
            .find(|n| n.id == sharding.physical_node_id)
            .expect("Physical node not found in sharding.nodes");
        this_node
            .http_addr
            .parse()
            .expect("Invalid http_addr in sharding config")
    } else {
        let http_port = 3000 + (config.raft.node_id as u16 - 1);
        SocketAddr::from(([0, 0, 0, 0], http_port))
    };

    tracing::info!("HTTP API listening on {}", http_addr);
    tracing::info!(
        "Dashboard redirect available at http://{}/dashboard -> {}",
        http_addr,
        dashboard_ui_origin()
    );
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

fn dashboard_ui_origin() -> String {
    std::env::var("DASHBOARD_UI_ORIGIN")
        .unwrap_or_else(|_| "http://127.0.0.1:3100".to_string())
        .trim_end_matches('/')
        .to_string()
}

async fn redirect_dashboard_ui(uri: OriginalUri) -> Redirect {
    let destination = match uri.0.path_and_query() {
        Some(path_and_query) => format!("{}{}", dashboard_ui_origin(), path_and_query.as_str()),
        None => format!("{}/dashboard", dashboard_ui_origin()),
    };
    Redirect::temporary(&destination)
}

/// Middleware: allow dashboard JWTs for internal UI calls, otherwise require a
/// valid API key on `/v1/` routes.
async fn api_key_auth(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum_middleware::Next,
) -> axum::response::Response {
    if let Some(token) = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|header| header.strip_prefix("Bearer "))
    {
        if state.dashboard_auth.verify_token(token).is_ok() {
            return next.run(req).await;
        }
    }

    let Some(raw_key) = req.headers().get("x-api-key").and_then(|v| v.to_str().ok()) else {
        return (
            axum::http::StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Missing API key" })),
        )
            .into_response();
    };

    match state
        .management_registry
        .authenticate_api_key(raw_key)
        .await
    {
        Ok(Some(_)) => next.run(req).await,
        Ok(None) => (
            axum::http::StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Invalid API key" })),
        )
            .into_response(),
        Err(error) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

/// Validate that a path-supplied identifier is within acceptable bounds.
/// Returns an error response if the value is too long or contains characters that
/// would break the internal RocksDB key scheme.
fn validate_id(value: &str, field: &str) -> Result<(), axum::response::Response> {
    if value.len() > 256 {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!("{} must not exceed 256 characters", field)
            })),
        )
            .into_response());
    }
    if value.is_empty() {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!("{} must not be empty", field)
            })),
        )
            .into_response());
    }
    Ok(())
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
            })),
        )
            .into_response()
    } else {
        let hint = current_leader
            .map(|id| format!("Node {} (Try Port {})", id, 3000 + id - 1))
            .unwrap_or_else(|| "Unknown".to_string());
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Not Leader",
                "current_leader": current_leader,
                "hint": hint,
            })),
        )
            .into_response()
    }
}

/// Forward request to leader node
async fn forward_to_leader<T: serde::Serialize>(
    state: &AppState,
    leader_id: u64,
    path: &str,
    payload: &T,
) -> Result<axum::response::Response, axum::response::Response> {
    // Calculate leader address
    let leader_port = 3000 + leader_id - 1;
    let leader_url = format!("http://localhost:{}{}", leader_port, path);

    tracing::info!(
        "Forwarding request to leader node {} at {}",
        leader_id,
        leader_url
    );

    // Reuse the shared HTTP client — avoids creating a new connection pool per request.
    let response = state
        .http_client
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
                })),
            )
                .into_response()
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
            })),
        )
            .into_response()
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
                })),
            )
                .into_response()
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

    let edge = GraphEdge::new(
        user_id,
        payload.source_id,
        payload.target_id,
        payload.relation,
        payload.weight.unwrap_or(1.0),
    );

    match shard
        .raft
        .client_write(memorose_core::raft::types::ClientRequest::UpdateGraph(edge))
        .await
    {
        Ok(_) => Json(serde_json::json!({ "status": "accepted" })).into_response(),
        Err(e) => {
            tracing::error!("Raft write error (graph): {:?}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
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
async fn pending_count(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let mut total_pending: usize = 0;
    for (_shard_id, shard) in state.shard_manager.all_shards() {
        if let Ok(n) = shard.engine.count_pending_events().await {
            total_pending += n;
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
    org_id: Option<String>,
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

fn parse_ingest_content(
    content_type: &str,
    raw_content: String,
) -> std::result::Result<EventContent, String> {
    match content_type.to_lowercase().as_str() {
        "image" => Ok(EventContent::Image(raw_content)),
        "audio" => Ok(EventContent::Audio(raw_content)),
        "video" => Ok(EventContent::Video(raw_content)),
        "json" => serde_json::from_str(&raw_content)
            .map(EventContent::Json)
            .map_err(|e| format!("invalid json payload: {}", e)),
        _ => Ok(EventContent::Text(raw_content)),
    }
}

async fn ingest_event(
    State(state): State<Arc<AppState>>,
    Path((user_id, stream_id)): Path<(String, Uuid)>,
    Json(payload): Json<IngestRequest>,
) -> axum::response::Response {
    if let Err(r) = validate_id(&user_id, "user_id") {
        return r;
    }
    if let Some(org_id) = payload.org_id.as_deref() {
        if let Err(r) = validate_id(org_id, "org_id") {
            return r;
        }
    }
    let shard = state.shard_manager.shard_for_user(&user_id);
    let metrics = shard.raft.metrics().borrow().clone();
    let current_leader = metrics.current_leader;
    let node_id = metrics.id;

    // Forward to leader if not leader
    if current_leader != Some(node_id) {
        if let Some(leader_id) = current_leader {
            let path = format!("/v1/users/{}/streams/{}/events", user_id, stream_id);

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

    let content = match parse_ingest_content(&payload.content_type, payload.content) {
        Ok(content) => content,
        Err(message) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "status": "error",
                    "message": message
                })),
            )
                .into_response();
        }
    };
    let mut event = Event::new(payload.org_id.clone(), user_id, None, stream_id, content);
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

    match shard
        .raft
        .client_write(memorose_core::raft::types::ClientRequest::IngestEvent(
            event,
        ))
        .await
    {
        Ok(_) => Json(serde_json::json!({
            "status": "accepted",
            "event_id": event_id
        }))
        .into_response(),
        Err(e) => {
            tracing::error!("Raft write error: {:?}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "status": "error",
                    "message": e.to_string()
                })),
            )
                .into_response()
        }
    }
}

#[derive(Deserialize)]
struct RetrieveRequest {
    query: String,
    #[serde(default = "default_retrieve_limit")]
    limit: usize,
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
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    /// Base64-encoded image for cross-modal retrieval
    #[serde(default)]
    image: Option<String>,
    /// Base64-encoded audio for cross-modal retrieval
    #[serde(default)]
    audio: Option<String>,
    /// Base64-encoded video for cross-modal retrieval
    #[serde(default)]
    video: Option<String>,
}

fn default_graph_depth() -> usize {
    1
}

fn default_retrieve_limit() -> usize {
    10
}

async fn retrieve_memory(
    State(state): State<Arc<AppState>>,
    Path((user_id, stream_id)): Path<(String, Uuid)>,
    Json(payload): Json<RetrieveRequest>,
) -> axum::response::Response {
    let start = std::time::Instant::now();
    if let Err(r) = validate_id(&user_id, "user_id") {
        return r;
    }
    if let Some(org_id) = payload.org_id.as_deref() {
        if let Err(r) = validate_id(org_id, "org_id") {
            return r;
        }
    }
    let shard = state.shard_manager.shard_for_user(&user_id);

    let query_key = payload.query.clone();

    // Build EmbedInput from request: multimodal if image/audio/video provided, otherwise text
    let has_multimodal =
        payload.image.is_some() || payload.audio.is_some() || payload.video.is_some();

    let embedding_f32 = if has_multimodal {
        use memorose_core::llm::{EmbedInput, EmbedPart};
        let mut parts = vec![EmbedPart::Text(payload.query.clone())];
        if let Some(ref img) = payload.image {
            parts.push(EmbedPart::InlineData {
                mime_type: "image/jpeg".to_string(),
                data: img.clone(),
            });
        }
        if let Some(ref aud) = payload.audio {
            parts.push(EmbedPart::InlineData {
                mime_type: "audio/mp3".to_string(),
                data: aud.clone(),
            });
        }
        if let Some(ref vid) = payload.video {
            parts.push(EmbedPart::InlineData {
                mime_type: "video/mp4".to_string(),
                data: vid.clone(),
            });
        }
        let input = EmbedInput::Multimodal { parts };
        match state.llm_client.embed_content(input).await {
            Ok(res) => Ok(res.data),
            Err(e) => Err(e),
        }
    } else if let Some(cached) = state.embedding_cache.get(&query_key).await {
        tracing::debug!("Embedding Cache Hit for: '{}'", query_key);
        Ok(cached)
    } else {
        tracing::debug!("Embedding Cache Miss. Calling LLM...");
        match state.llm_client.embed(&payload.query).await {
            Ok(res) => {
                state
                    .embedding_cache
                    .insert(query_key, res.data.clone())
                    .await;
                Ok(res.data)
            }
            Err(e) => Err(e),
        }
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

            match shard
                .engine
                .search_hybrid_with_shared(
                    &user_id,
                    payload.org_id.as_deref(),
                    payload.agent_id.as_deref(),
                    &payload.query,
                    &embedding_f32,
                    payload.limit.min(100),
                    payload.enable_arbitration,
                    payload.min_score,
                    payload.graph_depth,
                    valid_range,
                    tx_range,
                )
                .await
            {
                Ok(units) => {
                    #[derive(Serialize)]
                    struct RetrieveResultItem {
                        unit: RetrievalMemoryUnitView,
                        score: f32,
                    }

                    #[derive(Serialize)]
                    struct RetrieveResponse {
                        stream_id: Uuid,
                        query: String,
                        results: Vec<RetrieveResultItem>,
                        query_time_ms: u128,
                    }

                    let processed_units = units
                        .into_iter()
                        .map(|(u, score)| RetrieveResultItem {
                            unit: RetrievalMemoryUnitView::from(u.memory_unit()),
                            score,
                        })
                        .collect();

                    Json(RetrieveResponse {
                        stream_id,
                        query: payload.query,
                        results: processed_units,
                        query_time_ms: start.elapsed().as_millis(),
                    })
                    .into_response()
                }
                Err(e) => {
                    tracing::error!("Search error: {:?}", e);
                    (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": e.to_string() })),
                    )
                        .into_response()
                }
            }
        }
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

async fn initialize_cluster(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
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
        let results = state
            .shard_manager
            .join_all(payload.node_id, &state.config)
            .await;
        Json(serde_json::json!({
            "status": "joined",
            "node_id": payload.node_id,
            "shards": results,
        }))
    } else {
        // Single-shard: join the local raft group using the provided node address
        let shard = state.shard_manager.shard(0).unwrap();
        let node_id = payload.node_id as u64;

        // Check if already a voter — idempotent on restart
        let metrics = shard.raft.metrics().borrow().clone();
        let existing_voters: std::collections::BTreeSet<u64> =
            metrics.membership_config.membership().voter_ids().collect();
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
                if leader.is_some() {
                    break;
                }
            }
        }
        if leader.is_none() {
            return Json(serde_json::json!({
                "error": "No leader elected yet, try again later"
            }));
        }

        let node = openraft::BasicNode {
            addr: payload.address.clone(),
        };

        match shard.raft.add_learner(node_id, node, true).await {
            Ok(_) => {}
            Err(e) => {
                return Json(
                    serde_json::json!({ "error": format!("Add learner failed: {:?}", e) }),
                );
            }
        }

        tokio::task::yield_now().await;

        let metrics = shard.raft.metrics().borrow().clone();
        let mut members: std::collections::BTreeSet<u64> =
            metrics.membership_config.membership().voter_ids().collect();
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
        let mut members: std::collections::BTreeSet<u64> =
            metrics.membership_config.membership().voter_ids().collect();

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
struct GoalTree {
    goal: GoalMemoryUnitView,
    tasks: Vec<L3TaskTree>,
}

#[derive(serde::Serialize)]
struct L3TaskTree {
    task: memorose_common::L3Task,
    children: Vec<L3TaskTree>,
}

async fn get_ready_tasks(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
) -> axum::response::Response {
    let shard = state.shard_manager.shard_for_user(&user_id);
    let engine = &shard.engine;

    match engine.get_ready_l3_tasks(&user_id).await {
        Ok(tasks) => axum::response::Json(tasks).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(serde::Deserialize)]
struct UpdateTaskStatusRequest {
    status: memorose_common::TaskStatus,
    progress: Option<f32>,
    result_summary: Option<String>,
}

async fn update_task_status(
    State(state): State<Arc<AppState>>,
    Path((user_id, task_id)): Path<(String, Uuid)>,
    axum::Json(req): axum::Json<UpdateTaskStatusRequest>,
) -> axum::response::Response {
    let shard = state.shard_manager.shard_for_user(&user_id);
    let engine = &shard.engine;

    match engine.get_l3_task(&user_id, task_id).await {
        Ok(Some(mut task)) => {
            task.status = req.status.clone();
            if let Some(p) = req.progress {
                task.progress = p;
            }
            if let Some(summary) = req.result_summary {
                task.result_summary = Some(summary);
            }
            task.updated_at = chrono::Utc::now();

            if let Err(e) = engine.store_l3_task(&task).await {
                return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
                    .into_response();
            }

            // Downward Sedimentation: If completed, log it to L0
            if task.status == memorose_common::TaskStatus::Completed {
                let event_content = format!(
                    "Agent completed milestone '{}'. Summary: {}",
                    task.title,
                    task.result_summary.as_deref().unwrap_or("None")
                );

                // Assuming stream_id can be derived or ignored for pure system tasks,
                // generating a dummy one here, or it should be passed via task model.
                let event = memorose_common::Event::new(
                    task.org_id.clone(),
                    task.user_id.clone(),
                    task.agent_id.clone(),
                    Uuid::new_v4(), // Need stream context actually
                    memorose_common::EventContent::Text(event_content),
                );
                if let Err(e) = engine.ingest_event(event).await {
                    tracing::warn!("Failed to sediment L3 task completion to L0: {}", e);
                }
            }

            axum::response::Json(task).into_response()
        }
        Ok(None) => (axum::http::StatusCode::NOT_FOUND, "Task not found").into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_all_task_trees(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
) -> axum::response::Response {
    let shard = state.shard_manager.shard_for_user(&user_id);
    let kv = shard.engine.kv();
    let prefix = format!("u:{}:unit:", user_id);
    let pairs = match kv.scan(prefix.as_bytes()) {
        Ok(p) => p,
        Err(e) => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    };

    let all_units: Vec<MemoryUnit> = pairs
        .into_iter()
        .filter_map(|(_, v)| serde_json::from_slice::<MemoryUnit>(&v).ok())
        .collect();

    let all_tasks = match shard.engine.list_l3_tasks(&user_id).await {
        Ok(t) => t,
        Err(e) => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    };

    let mut root_nodes = Vec::new();
    for unit in all_units.into_iter().filter(|u| u.level == 3) {
        let tasks = build_l3_task_tree(unit.id, &all_tasks, &shard.engine, &user_id, 0).await;
        root_nodes.push(GoalTree {
            goal: GoalMemoryUnitView::from(&unit),
            tasks,
        });
    }

    // Sort by transaction time descending
    root_nodes.sort_by(|a, b| b.goal.transaction_time.cmp(&a.goal.transaction_time));

    Json(root_nodes).into_response()
}

async fn get_task_tree(
    State(state): State<Arc<AppState>>,
    Path((user_id, stream_id)): Path<(String, Uuid)>,
) -> axum::response::Response {
    let shard = state.shard_manager.shard_for_user(&user_id);
    let kv = shard.engine.kv();
    let prefix = format!("u:{}:unit:", user_id);
    let pairs = match kv.scan(prefix.as_bytes()) {
        Ok(p) => p,
        Err(e) => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    };

    let all_units: Vec<MemoryUnit> = pairs
        .into_iter()
        .filter_map(|(_, v)| serde_json::from_slice::<MemoryUnit>(&v).ok())
        .filter(|u| u.stream_id == stream_id)
        .collect();

    let all_tasks = match shard.engine.list_l3_tasks(&user_id).await {
        Ok(t) => t,
        Err(e) => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    };

    let mut root_nodes = Vec::new();
    for unit in all_units.into_iter().filter(|u| u.level == 3) {
        let tasks = build_l3_task_tree(unit.id, &all_tasks, &shard.engine, &user_id, 0).await;
        root_nodes.push(GoalTree {
            goal: GoalMemoryUnitView::from(&unit),
            tasks,
        });
    }

    Json(root_nodes).into_response()
}

use async_recursion::async_recursion;

const MAX_TASK_DEPTH: usize = 10;

#[async_recursion]
async fn build_l3_task_tree(
    parent_id: Uuid,
    all_tasks: &[memorose_common::L3Task],
    engine: &MemoroseEngine,
    user_id: &str,
    depth: usize,
) -> Vec<L3TaskTree> {
    let mut children = Vec::new();

    if depth < MAX_TASK_DEPTH {
        for task in all_tasks {
            if task.parent_id == Some(parent_id) {
                let task_children =
                    build_l3_task_tree(task.task_id, all_tasks, engine, user_id, depth + 1).await;
                children.push(L3TaskTree {
                    task: task.clone(),
                    children: task_children,
                });
            }
        }
    }

    children
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ingest_content_rejects_invalid_json() {
        let result = parse_ingest_content("json", "{not valid json}".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_ingest_content_accepts_valid_json() {
        let result = parse_ingest_content("json", "{\"ok\":true}".to_string()).unwrap();
        match result {
            EventContent::Json(value) => assert_eq!(value["ok"], serde_json::json!(true)),
            _ => panic!("expected json content"),
        }
    }
}
