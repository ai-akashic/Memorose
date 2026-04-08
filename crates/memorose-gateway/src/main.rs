use axum::body::to_bytes;
use futures::future::join_all;
use serde_json::Value;

trait AggregationStrategy {
    fn aggregate(&self, responses: Vec<Value>, limit: usize, offset: usize) -> Response;
}

struct ListMergeSortStrategy;
impl AggregationStrategy for ListMergeSortStrategy {
    fn aggregate(&self, responses: Vec<Value>, limit: usize, offset: usize) -> Response {
        let mut all_results: Vec<Value> = Vec::new();

        for json in responses {
            if let Some(items) = json.get("results").and_then(|v| v.as_array()) {
                all_results.extend(items.clone());
            } else if let Some(items) = json.as_array() {
                all_results.extend(items.clone());
            }
        }

        all_results.sort_by(|a, b| {
            let ts_a = a
                .get("timestamp")
                .or_else(|| a.get("created_at"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let ts_b = b
                .get("timestamp")
                .or_else(|| b.get("created_at"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            ts_b.cmp(ts_a)
        });

        let sliced: Vec<Value> = all_results.into_iter().skip(offset).take(limit).collect();

        (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({
                "results": sliced,
                "total": sliced.len(),
                "scatter_gather": true
            })),
        )
            .into_response()
    }
}

fn merge_sum(a: Value, b: Value) -> Value {
    match (a, b) {
        (Value::Object(mut map_a), Value::Object(map_b)) => {
            for (k, v) in map_b {
                let existing = map_a.remove(&k).unwrap_or(Value::Null);
                map_a.insert(k, merge_sum(existing, v));
            }
            Value::Object(map_a)
        }
        (Value::Number(num_a), Value::Number(num_b)) => {
            if num_a.is_u64() && num_b.is_u64() {
                Value::Number(serde_json::Number::from(
                    num_a.as_u64().unwrap() + num_b.as_u64().unwrap(),
                ))
            } else if num_a.is_i64() && num_b.is_i64() {
                Value::Number(serde_json::Number::from(
                    num_a.as_i64().unwrap() + num_b.as_i64().unwrap(),
                ))
            } else if let (Some(fa), Some(fb)) = (num_a.as_f64(), num_b.as_f64()) {
                if let Some(n) = serde_json::Number::from_f64(fa + fb) {
                    Value::Number(n)
                } else {
                    Value::Number(num_a)
                }
            } else {
                Value::Number(num_a)
            }
        }
        (Value::Bool(ba), Value::Bool(bb)) => Value::Bool(ba && bb),
        (Value::Null, v) => v,
        (v, Value::Null) => v,
        (a, _) => a,
    }
}

struct SummationStrategy;
impl AggregationStrategy for SummationStrategy {
    fn aggregate(&self, responses: Vec<Value>, _limit: usize, _offset: usize) -> Response {
        let mut result = Value::Object(serde_json::Map::new());
        for json in responses {
            result = merge_sum(result, json);
        }

        if let Value::Object(ref mut map) = result {
            map.insert("scatter_gather".to_string(), Value::Bool(true));
        }

        (axum::http::StatusCode::OK, Json(result)).into_response()
    }
}

async fn scatter_gather_request(
    state: Arc<AppState>,
    headers: HeaderMap,
    method: axum::http::Method,
    path: &str,
    query: Option<String>,
) -> Response {
    let mut limit = 100;
    let mut offset = 0;

    if let Some(ref q) = query {
        for pair in q.split('&') {
            let mut kv = pair.split('=');
            if let (Some(k), Some(v)) = (kv.next(), kv.next()) {
                if k == "limit" {
                    limit = v.parse().unwrap_or(100);
                }
                if k == "page" {
                    offset = v.parse::<usize>().unwrap_or(0) * limit;
                }
                if k == "offset" {
                    offset = v.parse().unwrap_or(0);
                }
            }
        }
    }

    let scatter_limit = offset + limit;
    let mut scatter_query = query.clone().unwrap_or_default();
    if scatter_query.contains("limit=") {
        scatter_query = scatter_query.replace(
            &format!("limit={}", limit),
            &format!("limit={}", scatter_limit),
        );
    } else if !scatter_query.is_empty() {
        scatter_query = format!("{}&limit={}", scatter_query, scatter_limit);
    } else {
        scatter_query = format!("limit={}", scatter_limit);
    }

    let mut futures = Vec::new();
    let client = &state.http_client;

    for shard_id in 0..state.shard_count {
        let addr = state
            .resolve_shard_addr(shard_id)
            .await
            .unwrap_or_else(|| format!("127.0.0.1:{}", 3000 + shard_id));
        let url = if scatter_query.is_empty() {
            format!("http://{}/{}", addr, path)
        } else {
            format!("http://{}/{}?{}", addr, path, scatter_query)
        };
        let req = client
            .request(method.clone(), &url)
            .headers(headers.clone())
            .send();
        futures.push(req);
    }

    let responses = join_all(futures).await;
    let mut all_results: Vec<Value> = Vec::new();

    for res in responses {
        if let Ok(r) = res {
            if r.status().is_success() {
                if let Ok(json) = r.json::<Value>().await {
                    all_results.push(json);
                }
            }
        }
    }

    let strategy: Box<dyn AggregationStrategy> =
        if path.ends_with("stats") || path.ends_with("pending") {
            Box::new(SummationStrategy)
        } else {
            Box::new(ListMergeSortStrategy)
        };

    strategy.aggregate(all_results, limit, offset)
}

use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json, Router,
};
use bytes::Bytes;
use memorose_common::sharding::{decode_raft_node_id, user_id_to_shard};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

struct AppState {
    shard_count: u32,
    /// Maps physical_node_id -> HTTP address
    node_addresses: HashMap<u32, String>,
    /// Maps shard_id -> leader physical_node_id (cached)
    shard_leaders: RwLock<HashMap<u32, u32>>,
    http_client: reqwest::Client,
}

fn max_body_bytes() -> usize {
    std::env::var("GATEWAY_MAX_BODY_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(10 * 1024 * 1024)
}

impl AppState {
    /// Resolve an HTTP address for a shard, preferring cached leader.
    async fn resolve_shard_addr(&self, shard_id: u32) -> Option<String> {
        // Check cached leader first
        let leader = {
            let cache = self.shard_leaders.read().await;
            cache.get(&shard_id).cloned()
        };

        if let Some(leader_node) = leader {
            if let Some(addr) = self.node_addresses.get(&leader_node) {
                return Some(addr.clone());
            }
        }

        // Fallback: pick any node
        self.node_addresses.values().next().cloned()
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let shard_count: u32 = std::env::var("SHARD_COUNT")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .expect("SHARD_COUNT must be a number");

    // Parse node addresses from NODES env var: "1=127.0.0.1:3000,2=127.0.0.1:3001"
    let nodes_str = std::env::var("NODES").unwrap_or_else(|_| "1=127.0.0.1:3000".to_string());

    let node_addresses: HashMap<u32, String> = nodes_str
        .split(',')
        .filter_map(|entry| {
            let parts: Vec<&str> = entry.trim().splitn(2, '=').collect();
            if parts.len() == 2 {
                let id: u32 = parts[0].parse().ok()?;
                let addr = if parts[1].starts_with("http") {
                    parts[1].to_string()
                } else {
                    format!("http://{}", parts[1])
                };
                Some((id, addr))
            } else {
                None
            }
        })
        .collect();

    tracing::info!(
        "Gateway starting: {} shards, {} nodes: {:?}",
        shard_count,
        node_addresses.len(),
        node_addresses
    );

    let state = Arc::new(AppState {
        shard_count,
        node_addresses,
        shard_leaders: RwLock::new(HashMap::new()),
        http_client: reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to build gateway HTTP client"),
    });

    let app = Router::new().fallback(proxy_handler).with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    tracing::info!(
        "Gateway listening on {}, routing to {} shards",
        addr,
        shard_count
    );
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn proxy_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    req: Request,
) -> Response {
    let path = req.uri().path().trim_start_matches('/').to_string();
    let query = req.uri().query().map(|q| q.to_string());
    let method = req.method().clone();

    let body_bytes = if method == axum::http::Method::GET || method == axum::http::Method::HEAD {
        None
    } else {
        let limit = max_body_bytes();
        match to_bytes(req.into_body(), limit).await {
            Ok(bytes) => Some(bytes),
            Err(e) => {
                tracing::warn!("Gateway request body too large or unreadable: {}", e);
                return (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    format!("Request body exceeds limit ({} bytes)", limit),
                )
                    .into_response();
            }
        }
    };

    proxy_request_with_retry(state, headers, method, &path, query, body_bytes).await
}

/// Extract user_id from the URL pattern `/v1/users/{user_id}/...`
fn extract_routing_key(path: &str) -> Option<&str> {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() >= 3 && parts[0] == "v1" {
        match parts[1] {
            "users" | "organizations" | "agents" => Some(parts[2]),
            _ => None,
        }
    } else {
        None
    }
}

async fn proxy_request_with_retry(
    state: Arc<AppState>,
    headers: HeaderMap,
    method: axum::http::Method,
    path: &str,
    query: Option<String>,
    body: Option<Bytes>,
) -> Response {
    // Route based on user_id hash
    let routing_key = extract_routing_key(path);
    if routing_key.is_none() && method == axum::http::Method::GET {
        return scatter_gather_request(state, headers, method, path, query).await;
    }

    let user_id = routing_key;
    let shard_id = user_id
        .map(|uid| user_id_to_shard(uid, state.shard_count))
        .unwrap_or(0); // Non-user routes go to shard 0

    let mut target_addr: Option<String> = state.resolve_shard_addr(shard_id).await;

    let client = &state.http_client;
    let max_retries = 3;

    for attempt in 0..max_retries {
        let addr = match &target_addr {
            Some(a) => a.clone(),
            None => match state.node_addresses.values().next() {
                Some(a) => a.clone(),
                None => {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "No backend nodes configured",
                    )
                        .into_response()
                }
            },
        };

        let target_url = format!("{}/{}", addr, path);
        let target_uri_string = if let Some(ref q) = query {
            format!("{}?{}", target_url, q)
        } else {
            target_url
        };

        tracing::info!(
            "Proxy attempt {} for '{}' (shard {}): {}",
            attempt + 1,
            path,
            shard_id,
            target_uri_string
        );

        let mut builder = client.request(method.clone(), &target_uri_string);
        for (key, value) in &headers {
            if key.as_str() != "host" && key.as_str() != "content-length" {
                builder = builder.header(key, value);
            }
        }

        if let Some(ref bytes) = body {
            builder = builder.body(bytes.clone());
        }

        match builder.send().await {
            Ok(resp) => {
                let status = resp.status();

                // Stop retrying on client errors (4xx) - return immediately
                if status.is_client_error() {
                    let res_headers = resp.headers().clone();
                    let res_body = axum::body::Body::from_stream(resp.bytes_stream());
                    let mut response = res_body.into_response();
                    *response.status_mut() = status;
                    for (k, v) in res_headers {
                        if let Some(k) = k {
                            response.headers_mut().insert(k, v);
                        }
                    }
                    return response;
                }

                // RAFT REDIRECTION LOGIC
                if status == StatusCode::SERVICE_UNAVAILABLE {
                    let res_bytes = resp.bytes().await.unwrap_or_default();
                    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&res_bytes) {
                        if json["error"] == "Not Leader" {
                            // Try leader_physical_node first (sharded response),
                            // but only trust it when > 0 (0 means leader unknown) and the
                            // node actually exists in our config.
                            if let Some(leader_node) = json["leader_physical_node"].as_u64() {
                                let leader_node = leader_node as u32;
                                if leader_node > 0
                                    && state.node_addresses.contains_key(&leader_node)
                                {
                                    let mut cache = state.shard_leaders.write().await;
                                    cache.insert(shard_id, leader_node);
                                    target_addr = state.node_addresses.get(&leader_node).cloned();
                                    continue;
                                }
                            }
                            // Fallback: current_leader is a raw Raft node ID,
                            // decode it to extract the physical_node_id.
                            if let Some(raft_leader_id) = json["current_leader"].as_u64() {
                                let (_leader_shard, physical_node_id) =
                                    decode_raft_node_id(raft_leader_id);
                                if physical_node_id > 0
                                    && state.node_addresses.contains_key(&physical_node_id)
                                {
                                    let mut cache = state.shard_leaders.write().await;
                                    cache.insert(shard_id, physical_node_id);
                                    target_addr =
                                        state.node_addresses.get(&physical_node_id).cloned();
                                    continue;
                                }
                            }
                            // Leader unknown or not in our node list - clear stale cache and retry
                            tracing::warn!(
                                "Shard {} has no known leader, clearing cache and retrying",
                                shard_id
                            );
                            {
                                let mut cache = state.shard_leaders.write().await;
                                cache.remove(&shard_id);
                            }
                            target_addr = None;
                            if attempt < max_retries - 1 {
                                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                            }
                            continue;
                        }
                    }
                    // Non-"Not Leader" 503: retry with backoff instead of
                    // returning immediately.
                    tracing::warn!(
                        "Proxy attempt {} got non-raft 503 for shard {}",
                        attempt + 1,
                        shard_id
                    );
                    if attempt == max_retries - 1 {
                        return (status, axum::body::Body::from(res_bytes)).into_response();
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }

                // SUCCESS or other server error: Return to client
                let res_headers = resp.headers().clone();
                let res_body = axum::body::Body::from_stream(resp.bytes_stream());
                let mut response = res_body.into_response();
                *response.status_mut() = status;
                for (k, v) in res_headers {
                    if let Some(k) = k {
                        response.headers_mut().insert(k, v);
                    }
                }
                return response;
            }
            Err(e) => {
                tracing::error!("Proxy attempt {} failed: {}", attempt + 1, e);
                {
                    let mut cache = state.shard_leaders.write().await;
                    cache.remove(&shard_id);
                }
                target_addr = None;
                if attempt == max_retries - 1 {
                    return (StatusCode::BAD_GATEWAY, format!("Gateway Error: {}", e))
                        .into_response();
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }

    (StatusCode::SERVICE_UNAVAILABLE, "Max retries exceeded").into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_routing_key() {
        assert_eq!(
            extract_routing_key("v1/users/alice/streams/123/events"),
            Some("alice")
        );
        assert_eq!(
            extract_routing_key("v1/organizations/org_abc/knowledge"),
            Some("org_abc")
        );
        assert_eq!(
            extract_routing_key("v1/agents/agent_007/memory"),
            Some("agent_007")
        );
        assert_eq!(extract_routing_key("v1/memories"), None);
        assert_eq!(extract_routing_key("v1/stats"), None);
        assert_eq!(extract_routing_key("invalid/path"), None);
    }

    #[test]
    fn test_shard_routing_determinism() {
        let shard_count = 3;

        let uid1 = extract_routing_key("v1/users/alice/streams/abc123/events").unwrap();
        let uid2 = extract_routing_key("v1/users/alice/streams/def456/retrieve").unwrap();
        assert_eq!(uid1, uid2);

        let shard_a = user_id_to_shard(uid1, shard_count);
        let shard_b = user_id_to_shard(uid2, shard_count);
        assert_eq!(shard_a, shard_b, "Same user should route to same shard");
    }

    #[test]
    fn test_merge_sum_objects() {
        use serde_json::json;
        let a = json!({
            "count": 10,
            "nested": { "total": 5 }
        });
        let b = json!({
            "count": 5,
            "nested": { "total": 10 }
        });

        let merged = merge_sum(a, b);
        assert_eq!(merged["count"], json!(15));
        assert_eq!(merged["nested"]["total"], json!(15));
    }

    #[test]
    fn test_merge_sum_numbers() {
        use serde_json::json;
        assert_eq!(merge_sum(json!(10), json!(5)), json!(15));
        assert_eq!(merge_sum(json!(10.5), json!(5.5)), json!(16.0));
        assert_eq!(merge_sum(json!(-10), json!(-5)), json!(-15));
    }

    #[test]
    fn test_merge_sum_arrays() {
        use serde_json::json;
        let a = json!(["a", "b"]);
        let b = json!(["c", "d"]);
        let merged = merge_sum(a, b);
        assert_eq!(merged, json!(["a", "b"])); // Unsupported array merge fallback to A
    }

    #[test]
    fn test_merge_sum_fallback() {
        use serde_json::json;
        // Fallback prefers A
        assert_eq!(merge_sum(json!("a"), json!("b")), json!("a"));
        // Null merging
        assert_eq!(merge_sum(json!(null), json!("a")), json!("a"));
    }
}
