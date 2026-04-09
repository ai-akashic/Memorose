use axum::{
    extract::{OriginalUri, Path, State},
    http::HeaderMap,
    middleware as axum_middleware,
    response::{IntoResponse, Redirect},
    routing::{delete, get, post, put},
    Json, Router,
};
use chrono::{DateTime, Utc};
use memorose_common::sharding::decode_raft_node_id;
use memorose_common::{
    config::AppConfig, tokenizer::count_tokens, Asset, Event, EventContent, GraphEdge, MemoryType,
    MemoryUnit, RelationType, TimeRange,
};
use memorose_core::{LLMClient, MemoroseEngine, SharedSearchHit};
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

mod dashboard;
mod shard_manager;

use shard_manager::ShardManager;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimeMode {
    Standalone,
    Cluster,
}

struct AppState {
    shard_manager: ShardManager,
    llm_client: Arc<dyn LLMClient>,
    embedding_cache: Cache<String, Vec<f32>>,
    config: AppConfig,
    runtime_mode: RuntimeMode,
    start_time: std::time::Instant,
    dashboard_auth: dashboard::auth::DashboardAuth,
    management_registry: dashboard::registry::ManagementRegistry,
    login_limiter: Cache<String, u32>,
    dashboard_cache: Cache<String, serde_json::Value>,
    /// Shared HTTP client for leader-forwarding; reusing it preserves connection pools.
    http_client: reqwest::Client,
}

impl AppState {
    fn is_standalone_mode(&self) -> bool {
        self.runtime_mode == RuntimeMode::Standalone
    }

    fn is_cluster_mode(&self) -> bool {
        self.runtime_mode == RuntimeMode::Cluster
    }

    fn write_path_name(&self) -> &'static str {
        match self.runtime_mode {
            RuntimeMode::Standalone => "local_bypass",
            RuntimeMode::Cluster => "raft_consensus",
        }
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

#[derive(Clone, Serialize)]
struct RetrievalAssetView {
    storage_key: String,
    original_name: String,
    asset_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

impl From<&Asset> for RetrievalAssetView {
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
struct RetrievalMemoryUnitView {
    id: Uuid,
    memory_type: MemoryType,
    content: String,
    keywords: Vec<String>,
    level: u8,
    assets: Vec<RetrievalAssetView>,
}

impl From<&MemoryUnit> for RetrievalMemoryUnitView {
    fn from(unit: &MemoryUnit) -> Self {
        Self {
            id: unit.id,
            memory_type: unit.memory_type.clone(),
            content: unit.content.clone(),
            keywords: unit.keywords.clone(),
            level: unit.level,
            assets: unit.assets.iter().map(RetrievalAssetView::from).collect(),
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
    let runtime_mode = if config.is_standalone_mode() {
        RuntimeMode::Standalone
    } else {
        RuntimeMode::Cluster
    };
    tracing::info!("Using LLM Provider: {:?}", config.llm.provider);
    tracing::info!("Using LLM Model: {}", config.llm.model);
    tracing::info!("Using Embedding Model: {}", config.llm.embedding_model);
    tracing::info!(
        "Runtime mode: {:?}, write path: {}",
        runtime_mode,
        match runtime_mode {
            RuntimeMode::Standalone => "local_bypass",
            RuntimeMode::Cluster => "raft_consensus",
        }
    );

    let data_dir = config.storage.root_dir.clone();

    // Initialize shard manager (handles engine, raft, workers for all shards)
    let shard_manager = if config.is_sharded() {
        tracing::info!(
            "Starting in cluster mode: {} shards, physical_node_id={}",
            config.shard_count(),
            config.physical_node_id()
        );
        ShardManager::new(&config)
            .await
            .expect("Failed to start ShardManager")
    } else {
        if config.is_cluster_mode() {
            tracing::info!(
                "Starting in single-shard cluster mode (node_id={}, cluster_nodes={})",
                config.raft.node_id,
                config.cluster_node_count()
            );
        } else {
            tracing::info!(
                "Starting in standalone mode (node_id={})",
                config.raft.node_id
            );
        }
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
        runtime_mode,
        start_time: std::time::Instant::now(),
        dashboard_auth,
        management_registry,
        login_limiter,
        dashboard_cache,
        http_client: reqwest::Client::builder()
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
        .route("/forget/preview", post(dashboard::handlers::forget_preview))
        .route("/forget/execute", post(dashboard::handlers::forget_execute))
        .route(
            "/corrections/semantic/preview",
            post(dashboard::handlers::semantic_memory_preview),
        )
        .route(
            "/corrections/semantic/execute",
            post(dashboard::handlers::semantic_memory_execute),
        )
        .route(
            "/corrections/manual",
            post(dashboard::handlers::apply_manual_correction),
        )
        .route(
            "/corrections/reviews",
            get(dashboard::handlers::list_rac_reviews),
        )
        .route(
            "/corrections/reviews/:review_id/approve",
            post(dashboard::handlers::approve_rac_review),
        )
        .route(
            "/corrections/reviews/:review_id/reject",
            post(dashboard::handlers::reject_rac_review),
        )
        .route("/chat", post(dashboard::handlers::chat))
        .route("/config", get(dashboard::handlers::get_config))
        .route(
            "/organizations",
            get(dashboard::handlers::list_organizations)
                .post(dashboard::handlers::create_organization),
        )
        .route(
            "/api-keys",
            get(dashboard::handlers::list_api_keys).post(dashboard::handlers::create_api_key),
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
            "/v1/users/:user_id/streams/:stream_id/events/batch",
            post(ingest_events_batch),
        )
        .route(
            "/v1/users/:user_id/streams/:stream_id/retrieve",
            post(retrieve_memory),
        )
        .route("/v1/memory/context", post(build_memory_context))
        .route(
            "/v1/users/:user_id/memories/:id",
            delete(delete_memory_unit_hard),
        )
        .route(
            "/v1/users/:user_id/memories/semantic/preview",
            post(dashboard::handlers::user_semantic_memory_preview),
        )
        .route(
            "/v1/users/:user_id/memories/semantic/execute",
            post(dashboard::handlers::user_semantic_memory_execute),
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

    if config.needs_explicit_bootstrap_seed() {
        tracing::warn!(
            "Raft auto-bootstrap is enabled, but multi-node topology has no explicit seed; \
             set `raft.bootstrap_seed_node_id` to the physical node that should initialize the cluster."
        );
    }

    if config.should_auto_initialize_raft() {
        let bootstrap_seed = config
            .raft
            .bootstrap_seed_node_id
            .unwrap_or_else(|| config.physical_node_id());
        tracing::info!(
            "Auto-initializing Raft cluster on bootstrap seed node {}...",
            bootstrap_seed
        );
        let results = state.shard_manager.initialize_all(&config).await;
        let bootstrap_errors = bootstrap_initialize_errors(&results);
        if !bootstrap_errors.is_empty() {
            panic!(
                "raft auto-bootstrap failed during startup: {}",
                bootstrap_errors.join("; ")
            );
        }
    }

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

fn bootstrap_initialize_errors(results: &[serde_json::Value]) -> Vec<String> {
    results
        .iter()
        .filter_map(|result| {
            result
                .get("error")
                .and_then(|value| value.as_str())
                .map(|error| {
                    let shard_id = result
                        .get("shard_id")
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unknown".to_string());
                    format!("shard {shard_id}: {error}")
                })
        })
        .collect()
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
    if state.is_cluster_mode() {
        let raft = shard.raft.as_ref().expect("cluster mode requires raft");
        let metrics = raft.metrics().borrow().clone();
        let current_leader = metrics.current_leader;
        let node_id = metrics.id;

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

            return not_leader_response(current_leader, state.config.is_sharded());
        }
    }

    let edge = GraphEdge::new(
        user_id.clone(),
        payload.source_id,
        payload.target_id,
        payload.relation,
        payload.weight.unwrap_or(1.0),
    );

    if state.is_standalone_mode() {
        return match shard.engine.graph().add_edge(&edge).await {
            Ok(_) => Json(serde_json::json!({
                "status": "accepted",
                "write_path": state.write_path_name(),
            }))
            .into_response(),
            Err(e) => {
                tracing::error!("Direct write error (graph): {:?}", e);
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": e.to_string() })),
                )
                    .into_response()
            }
        };
    }

    match shard
        .raft
        .as_ref()
        .expect("cluster mode requires raft")
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

#[derive(Deserialize, Serialize)]
struct BatchIngestRequest {
    events: Vec<IngestRequest>,
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
    if state.is_cluster_mode() {
        let raft = shard.raft.as_ref().expect("cluster mode requires raft");
        let metrics = raft.metrics().borrow().clone();
        let current_leader = metrics.current_leader;
        let node_id = metrics.id;

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

            return not_leader_response(current_leader, state.config.is_sharded());
        }
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
    let mut event = Event::new(
        payload.org_id.clone(),
        user_id.clone(),
        None,
        stream_id,
        content,
    );
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
    if state.is_standalone_mode() {
        return match shard.engine.ingest_event_directly(event).await {
            Ok(_) => Json(serde_json::json!({
                "status": "accepted",
                "event_id": event_id,
                "write_path": state.write_path_name(),
            }))
            .into_response(),
            Err(e) => {
                tracing::error!("Direct write error (event): {:?}", e);
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "status": "error",
                        "message": e.to_string()
                    })),
                )
                    .into_response()
            }
        };
    }

    match shard
        .raft
        .as_ref()
        .expect("cluster mode requires raft")
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

async fn ingest_events_batch(
    State(state): State<Arc<AppState>>,
    Path((user_id, stream_id)): Path<(String, Uuid)>,
    Json(payload): Json<BatchIngestRequest>,
) -> axum::response::Response {
    if let Err(r) = validate_id(&user_id, "user_id") {
        return r;
    }
    if payload.events.is_empty() {
        return Json(serde_json::json!({
            "status": "accepted",
            "event_ids": Vec::<String>::new(),
            "count": 0
        }))
        .into_response();
    }

    for event in &payload.events {
        if let Some(org_id) = event.org_id.as_deref() {
            if let Err(r) = validate_id(org_id, "org_id") {
                return r;
            }
        }
    }
    let shard = state.shard_manager.shard_for_user(&user_id);
    if state.is_cluster_mode() {
        let raft = shard.raft.as_ref().expect("cluster mode requires raft");
        let metrics = raft.metrics().borrow().clone();
        let current_leader = metrics.current_leader;
        let node_id = metrics.id;

        if current_leader != Some(node_id) {
            if let Some(leader_id) = current_leader {
                let path = format!("/v1/users/{}/streams/{}/events/batch", user_id, stream_id);
                tracing::info!(
                    "Not leader (I'm {}, leader is {}), forwarding batch request",
                    node_id,
                    leader_id
                );
                match forward_to_leader(&state, leader_id, &path, &payload).await {
                    Ok(response) => return response,
                    Err(err_response) => return err_response,
                }
            }

            return not_leader_response(current_leader, state.config.is_sharded());
        }
    }

    let mut events = Vec::with_capacity(payload.events.len());
    let mut event_ids = Vec::with_capacity(payload.events.len());
    for item in payload.events {
        let content = match parse_ingest_content(&item.content_type, item.content) {
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

        let mut event = Event::new(
            item.org_id.clone(),
            user_id.clone(),
            None,
            stream_id,
            content,
        );
        if let Some(level) = item.level {
            event.metadata["target_level"] = serde_json::json!(level);
        }
        if let Some(parent_id) = item.parent_id {
            event.metadata["parent_id"] = serde_json::json!(parent_id);
        }
        if let Some(task_status) = item.task_status {
            event.metadata["task_status"] = serde_json::json!(task_status);
        }
        if let Some(task_progress) = item.task_progress {
            event.metadata["task_progress"] = serde_json::json!(task_progress);
        }
        event_ids.push(event.id.to_string());
        events.push(event);
    }

    if state.is_standalone_mode() {
        return match shard.engine.ingest_events_directly(events).await {
            Ok(_) => Json(serde_json::json!({
                "status": "accepted",
                "event_ids": event_ids,
                "count": event_ids.len(),
                "write_path": state.write_path_name(),
            }))
            .into_response(),
            Err(e) => {
                tracing::error!("Direct batch write error: {:?}", e);
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "status": "error",
                        "message": e.to_string()
                    })),
                )
                    .into_response()
            }
        };
    }

    match shard
        .raft
        .as_ref()
        .expect("cluster mode requires raft")
        .client_write(memorose_core::raft::types::ClientRequest::IngestEvents(
            events,
        ))
        .await
    {
        Ok(_) => Json(serde_json::json!({
            "status": "accepted",
            "event_ids": event_ids,
            "count": event_ids.len()
        }))
        .into_response(),
        Err(e) => {
            tracing::error!("Raft batch write error: {:?}", e);
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
    #[serde(default)]
    token_budget: Option<usize>,
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

#[derive(Deserialize)]
struct MemoryContextRequest {
    user_id: String,
    query: String,
    #[serde(default = "default_context_limit")]
    limit: usize,
    #[serde(default)]
    enable_arbitration: bool,
    #[serde(default)]
    min_score: Option<f32>,
    #[serde(default)]
    token_budget: Option<usize>,
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
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    image: Option<String>,
    #[serde(default)]
    audio: Option<String>,
    #[serde(default)]
    video: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ContextFormat {
    Text,
    Xml,
}

impl ContextFormat {
    fn from_raw(raw: Option<&str>) -> Self {
        match raw.map(str::trim) {
            Some(value) if value.eq_ignore_ascii_case("xml") => Self::Xml,
            _ => Self::Text,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Xml => "xml",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ContextCompressionTier {
    Tiny,
    Compact,
    Detailed,
}

impl ContextCompressionTier {
    fn as_str(self) -> &'static str {
        match self {
            Self::Tiny => "dense_l2_l3",
            Self::Compact => "adaptive_compact",
            Self::Detailed => "detailed_l1_first",
        }
    }
}

#[derive(Clone, Serialize)]
struct MemoryContextHitView {
    id: Uuid,
    level: u8,
    memory_type: MemoryType,
    domain: String,
    score: f32,
}

#[derive(Serialize)]
struct MemoryContextResponse {
    query: String,
    format: String,
    strategy: String,
    token_budget: usize,
    used_token_estimate: usize,
    matched_count: usize,
    included_count: usize,
    truncated: bool,
    context: String,
    hits: Vec<MemoryContextHitView>,
    query_time_ms: u128,
}

struct RenderedMemoryContext {
    context: String,
    used_token_estimate: usize,
    matched_count: usize,
    included_count: usize,
    truncated: bool,
    strategy: &'static str,
    hits: Vec<MemoryContextHitView>,
}

fn default_graph_depth() -> usize {
    1
}

fn default_retrieve_limit() -> usize {
    10
}

fn default_context_limit() -> usize {
    12
}

fn default_context_token_budget() -> usize {
    800
}

fn memory_budget_from_headers(
    headers: &HeaderMap,
) -> Result<Option<usize>, axum::response::Response> {
    let Some(raw_budget) = headers.get("x-memory-budget") else {
        return Ok(None);
    };
    let value = raw_budget.to_str().map_err(|_| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "X-Memory-Budget must be a valid UTF-8 integer" })),
        )
            .into_response()
    })?;
    let budget = value.trim().parse::<usize>().map_err(|_| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "X-Memory-Budget must be a positive integer" })),
        )
            .into_response()
    })?;
    if budget == 0 {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            Json(
                serde_json::json!({ "error": "X-Memory-Budget must be a positive integer greater than zero" }),
            ),
        )
            .into_response());
    }
    Ok(Some(budget))
}

fn validate_payload_token_budget(
    budget: Option<usize>,
) -> Result<Option<usize>, axum::response::Response> {
    match budget {
        Some(0) => Err((
            axum::http::StatusCode::BAD_REQUEST,
            Json(
                serde_json::json!({ "error": "token_budget must be a positive integer greater than zero" }),
            ),
        )
            .into_response()),
        _ => Ok(budget),
    }
}

async fn embed_query_with_optional_multimodal(
    state: &Arc<AppState>,
    query: &str,
    image: Option<&str>,
    audio: Option<&str>,
    video: Option<&str>,
) -> Result<Vec<f32>, String> {
    let has_multimodal = image.is_some() || audio.is_some() || video.is_some();
    let query_key = query.to_string();

    if has_multimodal {
        use memorose_core::llm::{EmbedInput, EmbedPart};
        let mut parts = vec![EmbedPart::Text(query.to_string())];
        if let Some(img) = image {
            parts.push(EmbedPart::InlineData {
                mime_type: "image/jpeg".to_string(),
                data: img.to_string(),
            });
        }
        if let Some(aud) = audio {
            parts.push(EmbedPart::InlineData {
                mime_type: "audio/mp3".to_string(),
                data: aud.to_string(),
            });
        }
        if let Some(vid) = video {
            parts.push(EmbedPart::InlineData {
                mime_type: "video/mp4".to_string(),
                data: vid.to_string(),
            });
        }
        let input = EmbedInput::Multimodal { parts };
        return state
            .llm_client
            .embed_content(input)
            .await
            .map(|res| res.data)
            .map_err(|e| e.to_string());
    }

    if let Some(cached) = state.embedding_cache.get(&query_key).await {
        tracing::debug!("Embedding Cache Hit for: '{}'", query);
        return Ok(cached);
    }

    tracing::debug!("Embedding Cache Miss. Calling LLM...");
    state
        .llm_client
        .embed(query)
        .await
        .map(|res| {
            let data = res.data;
            let cache_data = data.clone();
            let key = query_key.clone();
            let cache = state.embedding_cache.clone();
            tokio::spawn(async move {
                cache.insert(key, cache_data).await;
            });
            data
        })
        .map_err(|e| e.to_string())
}

fn build_content_preview(text: &str, max_chars: usize) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        return normalized;
    }
    let truncated = normalized.chars().take(max_chars).collect::<String>();
    format!("{}...", truncated.trim_end())
}

fn nth_char_boundary(text: &str, char_count: usize) -> usize {
    if char_count == 0 {
        return 0;
    }
    text.char_indices()
        .nth(char_count)
        .map(|(idx, _)| idx)
        .unwrap_or(text.len())
}

fn truncate_to_token_budget(text: &str, token_budget: usize) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() || token_budget == 0 {
        return String::new();
    }
    if count_tokens(&normalized) <= token_budget {
        return normalized;
    }

    let total_chars = normalized.chars().count();
    let mut low = 0usize;
    let mut high = total_chars;
    while low < high {
        let mid = (low + high).div_ceil(2);
        let end = nth_char_boundary(&normalized, mid);
        let candidate = format!("{}...", normalized[..end].trim_end());
        if count_tokens(&candidate) <= token_budget {
            low = mid;
        } else {
            high = mid.saturating_sub(1);
        }
    }

    if low == 0 {
        return String::new();
    }

    let end = nth_char_boundary(&normalized, low);
    let truncated = normalized[..end].trim_end();
    let candidate = format!("{}...", truncated);
    if count_tokens(&candidate) <= token_budget {
        candidate
    } else if count_tokens(truncated) <= token_budget {
        truncated.to_string()
    } else {
        String::new()
    }
}

fn memory_type_label(memory_type: &MemoryType) -> &'static str {
    match memory_type {
        MemoryType::Factual => "factual",
        MemoryType::Procedural => "procedural",
    }
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

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn context_compression_tier(token_budget: usize) -> ContextCompressionTier {
    match token_budget {
        0..=160 => ContextCompressionTier::Tiny,
        161..=480 => ContextCompressionTier::Compact,
        _ => ContextCompressionTier::Detailed,
    }
}

fn context_search_limit(limit: usize, tier: ContextCompressionTier) -> usize {
    let base = limit.clamp(1, 24);
    match tier {
        ContextCompressionTier::Tiny => base.saturating_mul(4).min(64),
        ContextCompressionTier::Compact => base.saturating_mul(3).min(48),
        ContextCompressionTier::Detailed => base.saturating_mul(2).min(32),
    }
}

fn context_priority(level: u8, tier: ContextCompressionTier) -> u8 {
    match tier {
        ContextCompressionTier::Tiny => match level {
            2 => 0,
            3 => 1,
            1 => 2,
            _ => 3,
        },
        ContextCompressionTier::Compact => match level {
            2 => 0,
            1 => 1,
            3 => 2,
            _ => 3,
        },
        ContextCompressionTier::Detailed => match level {
            1 => 0,
            2 => 1,
            3 => 2,
            _ => 3,
        },
    }
}

fn format_memory_text_block(unit: &MemoryUnit, tier: ContextCompressionTier) -> String {
    let preview_chars = match tier {
        ContextCompressionTier::Tiny => 88,
        ContextCompressionTier::Compact => 160,
        ContextCompressionTier::Detailed => 320,
    };
    let keyword_limit = match tier {
        ContextCompressionTier::Tiny => 0,
        ContextCompressionTier::Compact => 3,
        ContextCompressionTier::Detailed => 6,
    };
    let asset_limit = match tier {
        ContextCompressionTier::Tiny => 0,
        ContextCompressionTier::Compact => 1,
        ContextCompressionTier::Detailed => 3,
    };

    let mut lines = vec![format!(
        "- [L{} {} {}] {}",
        unit.level,
        memory_type_label(&unit.memory_type),
        unit.domain.as_str(),
        build_content_preview(&unit.content, preview_chars)
    )];

    if keyword_limit > 0 && !unit.keywords.is_empty() {
        lines.push(format!(
            "  Keywords: {}",
            unit.keywords
                .iter()
                .take(keyword_limit)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    if asset_limit > 0 {
        for asset in unit.assets.iter().take(asset_limit) {
            let source = asset_source_reference(asset);
            if let Some(description) = asset.description.as_deref().map(str::trim) {
                if !description.is_empty() {
                    lines.push(format!(
                        "  [{}: {}] ({})",
                        asset_kind_label(&asset.asset_type),
                        build_content_preview(
                            description,
                            if tier == ContextCompressionTier::Compact {
                                90
                            } else {
                                160
                            }
                        ),
                        source
                    ));
                    continue;
                }
            }
            lines.push(format!(
                "  [{}] ({})",
                asset_kind_label(&asset.asset_type),
                source
            ));
        }
    }

    lines.join("\n")
}

fn format_memory_xml_block(unit: &MemoryUnit, tier: ContextCompressionTier) -> String {
    let preview_chars = match tier {
        ContextCompressionTier::Tiny => 88,
        ContextCompressionTier::Compact => 160,
        ContextCompressionTier::Detailed => 320,
    };
    let keyword_limit = match tier {
        ContextCompressionTier::Tiny => 0,
        ContextCompressionTier::Compact => 3,
        ContextCompressionTier::Detailed => 6,
    };
    let asset_limit = match tier {
        ContextCompressionTier::Tiny => 0,
        ContextCompressionTier::Compact => 1,
        ContextCompressionTier::Detailed => 3,
    };

    let mut xml = format!(
        "<memory id=\"{}\" level=\"{}\" type=\"{}\" domain=\"{}\"><content>{}</content>",
        unit.id,
        unit.level,
        memory_type_label(&unit.memory_type),
        unit.domain.as_str(),
        xml_escape(&build_content_preview(&unit.content, preview_chars))
    );

    if keyword_limit > 0 && !unit.keywords.is_empty() {
        xml.push_str("<keywords>");
        for keyword in unit.keywords.iter().take(keyword_limit) {
            xml.push_str(&format!("<keyword>{}</keyword>", xml_escape(keyword)));
        }
        xml.push_str("</keywords>");
    }

    if asset_limit > 0 && !unit.assets.is_empty() {
        xml.push_str("<assets>");
        for asset in unit.assets.iter().take(asset_limit) {
            xml.push_str(&format!(
                "<asset kind=\"{}\" source=\"{}\">",
                xml_escape(asset_kind_label(&asset.asset_type)),
                xml_escape(&asset_source_reference(asset))
            ));
            if let Some(description) = asset.description.as_deref().map(str::trim) {
                if !description.is_empty() {
                    xml.push_str(&xml_escape(&build_content_preview(
                        description,
                        if tier == ContextCompressionTier::Compact {
                            90
                        } else {
                            160
                        },
                    )));
                }
            }
            xml.push_str("</asset>");
        }
        xml.push_str("</assets>");
    }

    xml.push_str("</memory>");
    xml
}

fn render_memory_context(
    results: &[(SharedSearchHit, f32)],
    token_budget: usize,
    format: ContextFormat,
) -> RenderedMemoryContext {
    let tier = context_compression_tier(token_budget);
    let mut ordered = results.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        let left_priority = context_priority(left.0.memory_unit().level, tier);
        let right_priority = context_priority(right.0.memory_unit().level, tier);
        left_priority
            .cmp(&right_priority)
            .then_with(|| right.1.partial_cmp(&left.1).unwrap_or(Ordering::Equal))
    });

    let mut context = match format {
        ContextFormat::Text => String::new(),
        ContextFormat::Xml => "<memory_context>".to_string(),
    };
    let mut hits = Vec::new();
    let mut truncated = false;

    for (hit, score) in ordered {
        let unit = hit.memory_unit();
        let block = match format {
            ContextFormat::Text => format_memory_text_block(unit, tier),
            ContextFormat::Xml => format_memory_xml_block(unit, tier),
        };

        let separator = if context.is_empty() || context == "<memory_context>" {
            ""
        } else if format == ContextFormat::Text {
            "\n"
        } else {
            ""
        };
        let candidate = format!("{context}{separator}{block}");
        if count_tokens(&candidate) <= token_budget {
            context = candidate;
            hits.push(MemoryContextHitView {
                id: unit.id,
                level: unit.level,
                memory_type: unit.memory_type.clone(),
                domain: unit.domain.as_str().to_string(),
                score: *score,
            });
            continue;
        }

        let remaining_budget = token_budget.saturating_sub(count_tokens(&context));
        if format == ContextFormat::Text && remaining_budget > 12 {
            let available = remaining_budget.saturating_sub(count_tokens(separator));
            let truncated_block = truncate_to_token_budget(&block, available);
            if !truncated_block.is_empty() {
                context.push_str(separator);
                context.push_str(&truncated_block);
                hits.push(MemoryContextHitView {
                    id: unit.id,
                    level: unit.level,
                    memory_type: unit.memory_type.clone(),
                    domain: unit.domain.as_str().to_string(),
                    score: *score,
                });
            }
        }
        truncated = true;
        break;
    }

    if format == ContextFormat::Xml {
        let closing = "</memory_context>";
        if count_tokens(&(context.clone() + closing)) <= token_budget {
            context.push_str(closing);
        } else {
            truncated = true;
            let available = token_budget.saturating_sub(count_tokens(&context));
            if available >= count_tokens(closing) {
                context.push_str(closing);
            }
        }
    }

    let used = count_tokens(&context);
    RenderedMemoryContext {
        context,
        used_token_estimate: used,
        matched_count: results.len(),
        included_count: hits.len(),
        truncated: truncated || hits.len() < results.len(),
        strategy: tier.as_str(),
        hits,
    }
}

async fn retrieve_memory(
    State(state): State<Arc<AppState>>,
    Path((user_id, stream_id)): Path<(String, Uuid)>,
    headers: HeaderMap,
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
    let header_token_budget = match memory_budget_from_headers(&headers) {
        Ok(budget) => budget,
        Err(response) => return response,
    };
    let payload_token_budget = match validate_payload_token_budget(payload.token_budget) {
        Ok(budget) => budget,
        Err(response) => return response,
    };
    let token_budget = payload_token_budget.or(header_token_budget);

    let embedding_f32 = embed_query_with_optional_multimodal(
        &state,
        &payload.query,
        payload.image.as_deref(),
        payload.audio.as_deref(),
        payload.video.as_deref(),
    )
    .await;

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
                .search_hybrid_with_shared_and_token_budget(
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
                    token_budget,
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
                Json(
                    serde_json::json!({ "error": format!("Failed to generate embedding: {}", e) }),
                ),
            )
                .into_response()
        }
    }
}

async fn build_memory_context(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<MemoryContextRequest>,
) -> axum::response::Response {
    let start = std::time::Instant::now();
    if let Err(r) = validate_id(&payload.user_id, "user_id") {
        return r;
    }
    if let Some(org_id) = payload.org_id.as_deref() {
        if let Err(r) = validate_id(org_id, "org_id") {
            return r;
        }
    }

    let header_token_budget = match memory_budget_from_headers(&headers) {
        Ok(budget) => budget,
        Err(response) => return response,
    };
    let payload_token_budget = match validate_payload_token_budget(payload.token_budget) {
        Ok(budget) => budget,
        Err(response) => return response,
    };
    let token_budget = payload_token_budget
        .or(header_token_budget)
        .unwrap_or(default_context_token_budget())
        .clamp(64, 4096);
    let format = ContextFormat::from_raw(payload.format.as_deref());
    let compression_tier = context_compression_tier(token_budget);
    let search_limit = context_search_limit(payload.limit, compression_tier);
    let shard = state.shard_manager.shard_for_user(&payload.user_id);

    let embedding_f32 = embed_query_with_optional_multimodal(
        &state,
        &payload.query,
        payload.image.as_deref(),
        payload.audio.as_deref(),
        payload.video.as_deref(),
    )
    .await;

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
                .search_hybrid_with_shared_and_token_budget(
                    &payload.user_id,
                    payload.org_id.as_deref(),
                    payload.agent_id.as_deref(),
                    &payload.query,
                    &embedding_f32,
                    search_limit,
                    payload.enable_arbitration,
                    payload.min_score,
                    payload.graph_depth,
                    valid_range,
                    tx_range,
                    None,
                )
                .await
            {
                Ok(results) => {
                    let rendered = render_memory_context(&results, token_budget, format);
                    Json(MemoryContextResponse {
                        query: payload.query,
                        format: format.as_str().to_string(),
                        strategy: rendered.strategy.to_string(),
                        token_budget,
                        used_token_estimate: rendered.used_token_estimate,
                        matched_count: rendered.matched_count,
                        included_count: rendered.included_count,
                        truncated: rendered.truncated,
                        context: rendered.context,
                        hits: rendered.hits,
                        query_time_ms: start.elapsed().as_millis(),
                    })
                    .into_response()
                }
                Err(e) => {
                    tracing::error!("Context search error: {:?}", e);
                    (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": e.to_string() })),
                    )
                        .into_response()
                }
            }
        }
        Err(e) => {
            tracing::error!("Context embedding error: {:?}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(
                    serde_json::json!({ "error": format!("Failed to generate embedding: {}", e) }),
                ),
            )
                .into_response()
        }
    }
}

async fn hard_delete_memory_unit_for_user(
    engine: &MemoroseEngine,
    user_id: &str,
    unit_id: Uuid,
) -> anyhow::Result<bool> {
    if engine
        .get_memory_unit_including_forgotten(user_id, unit_id)?
        .is_none()
    {
        return Ok(false);
    }

    engine.delete_memory_unit_hard(user_id, unit_id).await?;
    Ok(true)
}

async fn delete_memory_unit_hard(
    State(state): State<Arc<AppState>>,
    Path((user_id, id)): Path<(String, String)>,
) -> axum::response::Response {
    if let Err(r) = validate_id(&user_id, "user_id") {
        return r;
    }

    let unit_id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "Invalid memory ID format" })),
            )
                .into_response();
        }
    };

    let shard = state.shard_manager.shard_for_user(&user_id);
    match hard_delete_memory_unit_for_user(&shard.engine, &user_id, unit_id).await {
        Ok(true) => Json(serde_json::json!({
            "status": "deleted",
            "memory_id": unit_id,
            "mode": "hard"
        }))
        .into_response(),
        Ok(false) => (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "Memory not found" })),
        )
            .into_response(),
        Err(error) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct JoinRequest {
    node_id: u32,
    #[serde(default)]
    address: String,
}

async fn initialize_cluster(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    if state.is_standalone_mode() {
        return Json(serde_json::json!({
            "status": "skipped",
            "message": "Cluster initialization is disabled in standalone mode",
            "write_path": state.write_path_name(),
        }));
    }
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
    if state.is_standalone_mode() {
        return Json(serde_json::json!({
            "error": "join_cluster is disabled in standalone mode"
        }));
    }
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
        let raft = shard.raft.as_ref().expect("cluster mode requires raft");
        let node_id = payload.node_id as u64;

        // Check if already a voter — idempotent on restart
        let metrics = raft.metrics().borrow().clone();
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
                leader = raft.metrics().borrow().current_leader;
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

        match raft.add_learner(node_id, node, true).await {
            Ok(_) => {}
            Err(e) => {
                return Json(
                    serde_json::json!({ "error": format!("Add learner failed: {:?}", e) }),
                );
            }
        }

        tokio::task::yield_now().await;

        let metrics = raft.metrics().borrow().clone();
        let mut members: std::collections::BTreeSet<u64> =
            metrics.membership_config.membership().voter_ids().collect();
        members.insert(node_id);

        match raft.change_membership(members, false).await {
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
    if state.is_standalone_mode() {
        return Json(serde_json::json!({
            "error": "leave_cluster is disabled in standalone mode"
        }));
    }
    if state.config.is_sharded() {
        let results = state.shard_manager.leave_all(node_id).await;
        Json(serde_json::json!({
            "status": "left",
            "node_id": node_id,
            "shards": results,
        }))
    } else {
        let shard = state.shard_manager.shard(0).unwrap();
        let raft = shard.raft.as_ref().expect("cluster mode requires raft");
        let metrics = raft.metrics().borrow().clone();
        let mut members: std::collections::BTreeSet<u64> =
            metrics.membership_config.membership().voter_ids().collect();

        if !members.remove(&(node_id as u64)) {
            return Json(serde_json::json!({ "error": "Node not found in cluster" }));
        }

        match raft.change_membership(members, false).await {
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
    use memorose_common::{ForgetMode, ForgetTargetKind, ForgettingTombstone, MemoryType};
    use tempfile::tempdir;

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

    #[tokio::test]
    async fn test_hard_delete_memory_unit_for_user_deletes_forgotten_memory() -> anyhow::Result<()>
    {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let unit = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "Delete me permanently".into(),
            Some(vec![1.0; 768]),
        );
        let unit_id = unit.id;
        engine.store_memory_unit(unit).await?;

        let tombstone = ForgettingTombstone {
            user_id: "test-user".into(),
            org_id: None,
            target_kind: ForgetTargetKind::MemoryUnit,
            target_id: unit_id.to_string(),
            reason_query: "forget this".into(),
            created_at: Utc::now(),
            preview_id: Some(Uuid::new_v4().to_string()),
            mode: ForgetMode::Logical,
        };
        engine.mark_memory_unit_forgotten("test-user", unit_id, &tombstone)?;

        assert!(hard_delete_memory_unit_for_user(&engine, "test-user", unit_id).await?);
        assert!(engine
            .get_memory_unit_including_forgotten("test-user", unit_id)?
            .is_none());
        assert!(!engine.is_memory_unit_forgotten("test-user", unit_id)?);

        Ok(())
    }

    #[tokio::test]
    async fn test_hard_delete_memory_unit_for_user_returns_false_when_missing() -> anyhow::Result<()>
    {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        assert!(!hard_delete_memory_unit_for_user(&engine, "test-user", Uuid::new_v4()).await?);

        Ok(())
    }

    #[test]
    fn test_bootstrap_initialize_errors_collects_failures() {
        let results = vec![
            serde_json::json!({"shard_id": 0, "status": "initialized"}),
            serde_json::json!({"shard_id": 1, "error": "network timeout"}),
            serde_json::json!({"shard_id": 2, "error": "storage unavailable"}),
        ];

        assert_eq!(
            bootstrap_initialize_errors(&results),
            vec![
                "shard 1: network timeout".to_string(),
                "shard 2: storage unavailable".to_string(),
            ]
        );
    }

    #[test]
    fn test_bootstrap_initialize_errors_ignores_success_records() {
        let results = vec![
            serde_json::json!({"shard_id": 0, "status": "initialized"}),
            serde_json::json!({"shard_id": 1, "status": "already_initialized"}),
        ];

        assert!(bootstrap_initialize_errors(&results).is_empty());
    }

    #[tokio::test]
    async fn test_index_handler_returns_online() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;
        let app = Router::new().route("/", axum::routing::get(root));
        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

    fn test_memory_unit(content: &str, level: u8, memory_type: MemoryType) -> MemoryUnit {
        let mut unit = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            Uuid::new_v4(),
            memory_type,
            content.to_string(),
            None,
        );
        unit.level = level;
        unit
    }

    #[test]
    fn test_render_memory_context_prefers_l2_when_budget_is_tiny() {
        let mut l1 = test_memory_unit(
            "User moved to Beijing and now works onsite.",
            1,
            MemoryType::Factual,
        );
        l1.keywords = vec!["beijing".into(), "work".into()];
        let l2 = test_memory_unit(
            "Insight: current residence and work arrangement both changed recently.",
            2,
            MemoryType::Factual,
        );
        let l3 = test_memory_unit(
            "Goal: keep onboarding context current for future task execution.",
            3,
            MemoryType::Procedural,
        );
        let results = vec![
            (SharedSearchHit::native(l1), 0.98),
            (SharedSearchHit::native(l2), 0.91),
            (SharedSearchHit::native(l3), 0.88),
        ];

        let rendered = render_memory_context(&results, 96, ContextFormat::Text);

        assert!(!rendered.hits.is_empty());
        assert_eq!(rendered.hits[0].level, 2);
        assert!(count_tokens(&rendered.context) <= 96);
    }

    #[test]
    fn test_render_memory_context_respects_text_budget() {
        let unit = test_memory_unit(
            "This is a deliberately long memory block about a user changing cities, jobs, email addresses, and project ownership all at once.",
            1,
            MemoryType::Factual,
        );
        let results = vec![(SharedSearchHit::native(unit), 0.95)];

        let rendered = render_memory_context(&results, 24, ContextFormat::Text);

        assert!(count_tokens(&rendered.context) <= 24);
        assert!(rendered.truncated);
    }

    #[test]
    fn test_render_memory_context_xml_wraps_output() {
        let unit = test_memory_unit(
            "Agent derived a compact org insight.",
            2,
            MemoryType::Procedural,
        );
        let results = vec![(SharedSearchHit::native(unit), 0.8)];

        let rendered = render_memory_context(&results, 128, ContextFormat::Xml);

        assert!(rendered.context.starts_with("<memory_context>"));
        assert!(rendered.context.ends_with("</memory_context>"));
        assert!(count_tokens(&rendered.context) <= 128);
    }

    #[test]
    fn test_memory_budget_from_headers_rejects_zero() {
        let mut headers = HeaderMap::new();
        headers.insert("x-memory-budget", "0".parse().unwrap());

        let response = memory_budget_from_headers(&headers).unwrap_err();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_validate_payload_token_budget_rejects_zero() {
        let response = validate_payload_token_budget(Some(0)).unwrap_err();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    }
}
