use axum::{extract::State, response::IntoResponse, Json};
use memorose_common::{MemoryDomain, MemoryUnit};
use std::collections::HashMap;
use std::sync::Arc;

use super::types::update_last_activity;

// ── Agents ────────────────────────────────────────────────────────

#[derive(serde::Serialize)]
pub struct AgentSummary {
    agent_id: String,
    total_memories: usize,
    l1_count: usize,
    l2_count: usize,
    total_events: usize,
    last_activity: Option<i64>,
}

pub async fn list_agents(State(state): State<Arc<crate::AppState>>) -> axum::response::Response {
    let cache_key = "agents:list".to_string();
    if let Some(cached) = state.dashboard_cache.get(&cache_key).await {
        return Json(cached).into_response();
    }

    let mut agent_data: HashMap<String, AgentSummary> = HashMap::new();

    for (_, shard) in state.shard_manager.all_shards() {
        let engine = shard.engine.clone();

        let scan_result = tokio::task::spawn_blocking(
            move || -> anyhow::Result<HashMap<String, AgentSummary>> {
                let kv = engine.kv();
                let mut local_agents: HashMap<String, AgentSummary> = HashMap::new();

                let all_pairs = kv.scan(b"u:")?;

                // Scan memory units grouped by agent_id
                for (k, val) in &all_pairs {
                    if k.windows(6).any(|w| w == b":unit:") {
                        if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(val) {
                            if unit.domain != MemoryDomain::Agent {
                                continue;
                            }
                            if let Some(ref aid) = unit.agent_id {
                                if aid.is_empty() {
                                    continue;
                                }
                                let entry = local_agents.entry(aid.clone()).or_insert_with(|| {
                                    AgentSummary {
                                        agent_id: aid.clone(),
                                        total_memories: 0,
                                        l1_count: 0,
                                        l2_count: 0,
                                        total_events: 0,
                                        last_activity: None,
                                    }
                                });
                                entry.total_memories += 1;
                                match unit.level {
                                    1 => entry.l1_count += 1,
                                    2 => entry.l2_count += 1,
                                    _ => {}
                                }
                                let ts = unit.transaction_time.timestamp();
                                update_last_activity(&mut entry.last_activity, ts);
                            }
                        }
                    }
                }

                // Count events per agent_id
                for (k, val) in &all_pairs {
                    if k.windows(7).any(|w| w == b":event:") {
                        if let Ok(event) = serde_json::from_slice::<memorose_common::Event>(val) {
                            if let Some(ref aid) = event.agent_id {
                                if aid.is_empty() {
                                    continue;
                                }
                                if let Some(entry) = local_agents.get_mut(aid) {
                                    entry.total_events += 1;
                                    update_last_activity(
                                        &mut entry.last_activity,
                                        event.transaction_time.timestamp(),
                                    );
                                } else {
                                    local_agents.insert(
                                        aid.clone(),
                                        AgentSummary {
                                            agent_id: aid.clone(),
                                            total_memories: 0,
                                            l1_count: 0,
                                            l2_count: 0,
                                            total_events: 1,
                                            last_activity: Some(event.transaction_time.timestamp()),
                                        },
                                    );
                                }
                            }
                        }
                    }
                }

                Ok(local_agents)
            },
        )
        .await;

        if let Ok(Ok(shard_agents)) = scan_result {
            for (aid, summary) in shard_agents {
                let entry = agent_data.entry(aid).or_insert_with(|| AgentSummary {
                    agent_id: summary.agent_id,
                    total_memories: 0,
                    l1_count: 0,
                    l2_count: 0,
                    total_events: 0,
                    last_activity: None,
                });
                entry.total_memories += summary.total_memories;
                entry.l1_count += summary.l1_count;
                entry.l2_count += summary.l2_count;
                entry.total_events += summary.total_events;
                if entry.last_activity.is_none()
                    || (summary.last_activity.is_some()
                        && entry.last_activity < summary.last_activity)
                {
                    entry.last_activity = summary.last_activity;
                }
            }
        }
    }

    let mut agents: Vec<AgentSummary> = agent_data.into_values().collect();
    agents.sort_by(|a, b| b.last_activity.cmp(&a.last_activity));

    let total_count = agents.len();
    let result = serde_json::json!({
        "agents": agents,
        "total_count": total_count,
    });

    state
        .dashboard_cache
        .insert(cache_key, result.clone())
        .await;

    Json(result).into_response()
}
