use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use memorose_common::MemoryDomain;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

use super::types::matches_dashboard_org_scope;

// ── Graph ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct GraphQuery {
    #[serde(default = "default_graph_limit")]
    limit: usize,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    org_id: Option<String>,
}

fn default_graph_limit() -> usize {
    500
}

fn graph_edge_fetch_limit(node_limit: usize) -> usize {
    node_limit.saturating_mul(4).clamp(100, 5_000)
}

pub async fn graph_data(
    State(state): State<Arc<crate::AppState>>,
    Query(params): Query<GraphQuery>,
) -> axum::response::Response {
    let limit = params.limit.min(1000);
    let user_id_filter = params.user_id.clone();
    let org_id_filter = params.org_id.clone();

    if limit == 0 {
        return Json(serde_json::json!({
            "nodes": [],
            "edges": [],
            "stats": {
                "node_count": 0,
                "edge_count": 0,
                "relation_distribution": {},
            }
        }))
        .into_response();
    }

    // Determine which shards to scan
    let shard_ids: Vec<u32> = if let Some(ref uid) = user_id_filter {
        let sid =
            memorose_common::sharding::user_id_to_shard(uid, state.shard_manager.shard_count());
        vec![sid]
    } else {
        state.shard_manager.all_shards().map(|(id, _)| id).collect()
    };
    let edge_fetch_limit = graph_edge_fetch_limit(limit);

    let mut all_nodes = Vec::new();
    let mut all_edge_data = Vec::new();

    for shard_id in shard_ids {
        let shard = match state.shard_manager.shard(shard_id) {
            Some(s) => s,
            None => continue,
        };
        let engine = shard.engine.clone();
        let uid_filter = user_id_filter.clone();
        let org_filter = org_id_filter.clone();
        let shard_edge_fetch_limit = edge_fetch_limit;
        let shard_node_limit = limit;

        let result: anyhow::Result<serde_json::Value> = async move {
            let graph = engine.graph();

            let edges = if let Some(ref uid) = uid_filter {
                graph
                    .get_edges_for_user_limited(uid, shard_edge_fetch_limit)
                    .await?
            } else {
                graph.scan_edges_limited(shard_edge_fetch_limit).await?
            };

            let mut node_ids = std::collections::HashSet::new();
            for edge in &edges {
                node_ids.insert(edge.source_id);
                node_ids.insert(edge.target_id);
            }

            let node_ids_vec: Vec<_> = node_ids.into_iter().collect();
            let mut nodes = Vec::new();
            let mut retained_node_ids = std::collections::HashSet::new();
            for unit_id in node_ids_vec {
                if nodes.len() >= shard_node_limit {
                    break;
                }
                if let Some(hit) = engine.get_shared_search_hit_by_index(unit_id).await? {
                    let unit = hit.memory_unit();
                    if !matches_dashboard_org_scope(unit.org_id.as_deref(), org_filter.as_deref()) {
                        continue;
                    }
                    let label = if unit.content.chars().count() > 80 {
                        let end = unit
                            .content
                            .char_indices()
                            .nth(80)
                            .map(|(i, _)| i)
                            .unwrap_or(unit.content.len());
                        format!("{}...", &unit.content[..end])
                    } else {
                        unit.content.clone()
                    };
                    retained_node_ids.insert(unit.id);
                    let display_user_id = if unit.domain == MemoryDomain::Organization {
                        String::new()
                    } else {
                        unit.user_id.clone()
                    };
                    nodes.push(serde_json::json!({
                        "id": unit.id,
                        "label": label,
                        "level": unit.level,
                        "importance": unit.importance,
                        "user_id": display_user_id,
                    }));
                }
            }

            let edge_data: Vec<serde_json::Value> = edges
                .iter()
                .filter(|e| {
                    org_filter.as_ref().map_or(true, |_| {
                        retained_node_ids.contains(&e.source_id)
                            && retained_node_ids.contains(&e.target_id)
                    })
                })
                .map(|e| {
                    let rel = format!("{:?}", e.relation);
                    serde_json::json!({
                        "source": e.source_id,
                        "target": e.target_id,
                        "relation": rel,
                        "weight": e.weight,
                    })
                })
                .collect();
            Ok(serde_json::json!({
                "nodes": nodes,
                "edges": edge_data,
            }))
        }
        .await;

        if let Ok(data) = result {
            if let Some(nodes) = data["nodes"].as_array() {
                all_nodes.extend(nodes.clone());
            }
            if let Some(edges) = data["edges"].as_array() {
                all_edge_data.extend(edges.clone());
            }
        }
    }

    let nodes = if all_nodes.len() > limit {
        all_nodes[..limit].to_vec()
    } else {
        all_nodes.clone()
    };

    let retained: std::collections::HashSet<String> = nodes
        .iter()
        .filter_map(|n| n["id"].as_str().map(String::from))
        .collect();
    let filtered_edges: Vec<_> = all_edge_data
        .into_iter()
        .filter(|e| {
            retained.contains(e["source"].as_str().unwrap_or(""))
                && retained.contains(e["target"].as_str().unwrap_or(""))
        })
        .collect();
    let mut relation_distribution: HashMap<String, usize> = HashMap::new();
    for edge in &filtered_edges {
        if let Some(relation) = edge["relation"].as_str() {
            *relation_distribution
                .entry(relation.to_string())
                .or_default() += 1;
        }
    }

    Json(serde_json::json!({
        "nodes": nodes,
        "edges": filtered_edges,
        "stats": {
            "node_count": nodes.len(),
            "edge_count": filtered_edges.len(),
            "sampled": true,
            "edge_fetch_limit": edge_fetch_limit,
            "relation_distribution": relation_distribution,
        }
    }))
    .into_response()
}
