use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use memorose_common::{Event as MemoryEvent, MemoryDomain, MemoryUnit};
use memorose_core::engine::{RacDecisionRecord, RacMetricHistoryPoint, RacMetricSnapshot};
use memorose_core::storage::index::TextIndexMetricSnapshot;
use serde::Deserialize;
use std::sync::Arc;

use super::types::{matches_dashboard_org_scope, MemoryAggregate};

// ── Cluster Status ────────────────────────────────────────────────

pub async fn cluster_status(State(state): State<Arc<crate::AppState>>) -> Json<serde_json::Value> {
    let mut shard_statuses = Vec::new();

    for (shard_id, shard) in state.shard_manager.all_shards() {
        let index_metrics = shard.engine.get_text_index_metric_snapshot();
        if let Some(raft) = shard.raft.as_ref() {
            let metrics = raft.metrics().borrow().clone();

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
            let learners: Vec<u64> = metrics
                .membership_config
                .membership()
                .learner_ids()
                .collect();

            shard_statuses.push(serde_json::json!({
                "shard_id": shard_id,
                "raft_state": raft_state,
                "current_leader": metrics.current_leader,
                "current_term": metrics.current_term,
                "last_log_index": last_log_index,
                "last_applied": last_applied,
                "replication_lag": last_log_index.saturating_sub(last_applied),
                "voters": voters,
                "learners": learners,
                "text_index_metrics": index_metrics,
            }));
        } else {
            let node_id = state.shard_manager.physical_node_id() as u64;
            shard_statuses.push(serde_json::json!({
                "shard_id": shard_id,
                "raft_state": "Standalone",
                "current_leader": node_id,
                "current_term": 0,
                "last_log_index": 0,
                "last_applied": 0,
                "replication_lag": 0,
                "voters": [node_id],
                "learners": [],
                "text_index_metrics": index_metrics,
            }));
        }
    }

    // Sort by shard_id for deterministic output
    shard_statuses.sort_by_key(|s| s["shard_id"].as_u64().unwrap_or(0));

    // Keep the single-shard payload flat so the dashboard can render either topology shape.
    if state.shard_manager.shard_count() <= 1 {
        if let Some(first) = shard_statuses.first() {
            let mut result = first.clone();
            result["node_id"] = serde_json::json!(state.shard_manager.physical_node_id());
            result["snapshot_policy_logs"] = serde_json::json!(state.config.raft.snapshot_logs);
            result["runtime_mode"] = serde_json::json!(if state.is_standalone_mode() {
                "standalone"
            } else {
                "cluster"
            });
            result["write_path"] = serde_json::json!(state.write_path_name());
            result["config"] = serde_json::json!({
                "heartbeat_interval_ms": state.config.raft.heartbeat_interval_ms,
                "election_timeout_min_ms": state.config.raft.election_timeout_min_ms,
                "worker": {
                    "insight_interval_ms": state.config.worker.insight_interval_ms,
                    "insight_min_pending_tokens": state.config.worker.insight_min_pending_tokens,
                    "insight_min_pending_l1": state.config.worker.insight_min_pending_l1,
                    "insight_max_delay_ms": state.config.worker.insight_max_delay_ms,
                    "insight_batch_target_tokens": state.config.worker.insight_batch_target_tokens,
                    "insight_max_l1_per_batch": state.config.worker.insight_max_l1_per_batch,
                    "insight_max_batches_per_cycle": state.config.worker.insight_max_batches_per_cycle,
                }
            });
            return Json(result);
        }
    }

    Json(serde_json::json!({
        "physical_node_id": state.shard_manager.physical_node_id(),
        "shard_count": state.shard_manager.shard_count(),
        "runtime_mode": if state.is_standalone_mode() { "standalone" } else { "cluster" },
        "write_path": state.write_path_name(),
        "shards": shard_statuses,
        "config": {
            "heartbeat_interval_ms": state.config.raft.heartbeat_interval_ms,
            "election_timeout_min_ms": state.config.raft.election_timeout_min_ms,
            "worker": {
                "insight_interval_ms": state.config.worker.insight_interval_ms,
                "insight_min_pending_tokens": state.config.worker.insight_min_pending_tokens,
                "insight_min_pending_l1": state.config.worker.insight_min_pending_l1,
                "insight_max_delay_ms": state.config.worker.insight_max_delay_ms,
                "insight_batch_target_tokens": state.config.worker.insight_batch_target_tokens,
                "insight_max_l1_per_batch": state.config.worker.insight_max_l1_per_batch,
                "insight_max_batches_per_cycle": state.config.worker.insight_max_batches_per_cycle,
            }
        }
    }))
}

// ── Stats ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct StatsQuery {
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    history_hours: Option<usize>,
}

pub async fn stats(
    State(state): State<Arc<crate::AppState>>,
    Query(params): Query<StatsQuery>,
) -> axum::response::Response {
    let history_hours = params.history_hours.unwrap_or(24).clamp(1, 24 * 7);
    let cache_key = format!(
        "stats:{}:{}:{}",
        params.org_id.as_deref().unwrap_or("_all"),
        params.user_id.as_deref().unwrap_or("_all"),
        history_hours,
    );
    if let Some(cached) = state.dashboard_cache.get(&cache_key).await {
        return Json(cached).into_response();
    }

    let user_id_filter = params.user_id.clone();

    // Determine which shards to scan
    let shard_ids: Vec<u32> = if let Some(ref uid) = user_id_filter {
        // Single shard for known user
        let sid =
            memorose_common::sharding::user_id_to_shard(uid, state.shard_manager.shard_count());
        vec![sid]
    } else {
        // All shards
        state.shard_manager.all_shards().map(|(id, _)| id).collect()
    };

    let mut total_pending = 0usize;
    let mut total_events = 0usize;
    let mut total_edges = 0usize;
    let mut total_memory = MemoryAggregate::default();
    let mut rac_metrics = RacMetricSnapshot::default();
    let mut text_index_metrics = TextIndexMetricSnapshot::default();
    let mut rac_history = std::collections::BTreeMap::<String, RacMetricHistoryPoint>::new();
    let mut rac_recent_decisions = Vec::<RacDecisionRecord>::new();

    for shard_id in shard_ids {
        let shard = match state.shard_manager.shard(shard_id) {
            Some(s) => s,
            None => continue,
        };
        let engine = shard.engine.clone();
        let uid_filter = user_id_filter.clone();

        let edge_count = if let Some(ref uid) = uid_filter {
            match engine.graph().get_all_edges_for_user(uid).await {
                Ok(edges) => edges.len(),
                Err(e) => {
                    tracing::warn!("Failed to load graph edges for user {}: {:?}", uid, e);
                    0
                }
            }
        } else {
            match engine.graph().scan_all_edges().await {
                Ok(edges) => edges.len(),
                Err(e) => {
                    tracing::warn!("Failed to scan graph edges: {:?}", e);
                    0
                }
            }
        };

        let uid_filter = user_id_filter.clone();
        let org_filter = params.org_id.clone();
        let org_filter_for_shared = params.org_id.clone();

        let scan_result = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let kv = engine.kv();

            let pending_count = engine.system_kv().scan(b"pending:")?.len();

            let (event_count, memory) = if let Some(ref uid) = uid_filter {
                let event_prefix = format!("u:{}:event:", uid);
                let event_count = kv
                    .scan(event_prefix.as_bytes())?
                    .into_iter()
                    .filter(|(_, val)| {
                        if let Ok(event) = serde_json::from_slice::<MemoryEvent>(val) {
                            matches_dashboard_org_scope(
                                event.org_id.as_deref(),
                                org_filter.as_deref(),
                            )
                        } else {
                            false
                        }
                    })
                    .count();

                let unit_prefix = format!("u:{}:unit:", uid);
                let unit_pairs = kv.scan(unit_prefix.as_bytes())?;
                let mut memory = MemoryAggregate::default();
                for (_, val) in &unit_pairs {
                    if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(val) {
                        if unit.domain != MemoryDomain::Organization
                            && matches_dashboard_org_scope(
                                unit.org_id.as_deref(),
                                org_filter.as_deref(),
                            )
                        {
                            memory.record_unit(&unit);
                        }
                    }
                }
                (event_count, memory)
            } else {
                let all_pairs = kv.scan(b"u:")?;
                tracing::debug!(
                    "Scanning all pairs: found {} keys starting with 'u:'",
                    all_pairs.len()
                );
                let mut event_count = 0usize;
                let mut memory = MemoryAggregate::default();
                for (k, val) in &all_pairs {
                    if k.windows(7).any(|w| w == b":event:") {
                        if let Ok(event) = serde_json::from_slice::<MemoryEvent>(val) {
                            if matches_dashboard_org_scope(
                                event.org_id.as_deref(),
                                org_filter.as_deref(),
                            ) {
                                event_count += 1;
                            }
                        }
                    } else if k.windows(6).any(|w| w == b":unit:") {
                        if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(val) {
                            if unit.domain != MemoryDomain::Organization
                                && matches_dashboard_org_scope(
                                    unit.org_id.as_deref(),
                                    org_filter.as_deref(),
                                )
                            {
                                memory.record_unit(&unit);
                            }
                        }
                    }
                }
                tracing::debug!(
                    "Scan results: events={}, units={}, local={}, shared={}",
                    event_count,
                    memory.total_memories(),
                    memory.local_memories(),
                    memory.shared_memories()
                );
                (event_count, memory)
            };

            Ok((pending_count, event_count, memory))
        })
        .await;

        if let Ok(Ok((pending, events, memory))) = scan_result {
            total_pending += pending;
            total_events += events;
            total_edges += edge_count;
            total_memory.merge(&memory);
            if let Ok(snapshot) = shard.engine.get_rac_metric_snapshot() {
                rac_metrics.merge(&snapshot);
            }
            text_index_metrics.merge(&shard.engine.get_text_index_metric_snapshot());
            if let Ok(history) = shard.engine.get_rac_metric_history(history_hours) {
                for point in history {
                    rac_history
                        .entry(point.bucket_start.clone())
                        .and_modify(|existing| existing.merge(&point))
                        .or_insert(point);
                }
            }
            if let Ok(mut decisions) = shard.engine.list_recent_rac_decisions(16) {
                decisions.retain(|decision| {
                    user_id_filter
                        .as_ref()
                        .is_none_or(|uid| decision.user_id == *uid)
                        && matches_dashboard_org_scope(
                            decision.org_id.as_deref(),
                            params.org_id.as_deref(),
                        )
                });
                rac_recent_decisions.append(&mut decisions);
            }

            if user_id_filter.is_none() {
                if let Ok(shared_units) = shard
                    .engine
                    .list_organization_read_units(org_filter_for_shared.as_deref())
                    .await
                {
                    for unit in shared_units {
                        total_memory.record_unit(&unit);
                    }
                }
            }
        }
    }

    let uptime = state.start_time.elapsed().as_secs();
    let memory_by_domain = total_memory.by_domain.clone();
    let local_levels = total_memory.local_levels.clone();
    let shared_levels = total_memory.shared_levels.clone();
    rac_recent_decisions.sort_by(|left, right| right.created_at.cmp(&left.created_at));
    rac_recent_decisions.truncate(16);

    let result = serde_json::json!({
        "total_events": total_events,
        "pending_events": total_pending,
        "total_memory_units": total_memory.total_memories(),
        "total_edges": total_edges,
        "memory_by_level": {
            "l1": total_memory.total_l1(),
            "l2": total_memory.total_l2(),
            "l3": total_memory.total_l3(),
        },
        "memory_by_scope": {
            "local": total_memory.local_memories(),
            "shared": total_memory.shared_memories(),
        },
        "memory_by_domain": memory_by_domain,
        "memory_by_level_and_scope": {
            "local": local_levels,
            "shared": shared_levels,
        },
        "text_index_metrics": text_index_metrics,
        "rac_metrics": rac_metrics,
        "rac_metrics_history": rac_history.into_values().collect::<Vec<_>>(),
        "rac_recent_decisions": rac_recent_decisions,
        "uptime_seconds": uptime,
    });

    state
        .dashboard_cache
        .insert(cache_key, result.clone())
        .await;

    Json(result).into_response()
}
