use anyhow::Result;
use memorose_common::{MemoryDomain, MemoryUnit, RelationType, TimeRange};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use super::helpers::{cosine_similarity, escape_sql_string, validate_id};
use super::types::SharedSearchHit;

impl super::MemoroseEngine {
    // ── Search ──────────────────────────────────────────────────────

    pub async fn search_similar(
        &self,
        user_id: &str,
        vector: &[f32],
        limit: usize,
        filter: Option<String>,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        let results = match self.vector.search("memories", vector, limit, filter).await {
            Ok(res) => res,
            Err(_) => return Ok(Vec::new()),
        };
        self.fetch_units_with_scores(user_id, results).await
    }

    /// Perform a BFS graph traversal to expand context from seed memories.
    pub(crate) async fn expand_subgraph(
        &self,
        user_id: &str,
        seeds: Vec<(MemoryUnit, f32)>,
        depth: usize,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        if depth == 0 || seeds.is_empty() {
            return Ok(seeds);
        }

        let mut results: HashMap<String, (MemoryUnit, f32)> = seeds
            .iter()
            .map(|(u, s)| (u.id.to_string(), (u.clone(), *s)))
            .collect();

        let mut frontier: Vec<String> = seeds.iter().map(|(u, _)| u.id.to_string()).collect();
        let mut visited: HashSet<String> = frontier.iter().cloned().collect();

        for _d in 0..depth {
            if frontier.is_empty() {
                break;
            }

            // Guard against unbounded expansion
            if results.len() > 500 {
                tracing::warn!("Graph expansion hit limit of 500 nodes, stopping early.");
                break;
            }

            if frontier.len() > 10 {
                frontier.truncate(10);
            }

            let mut next_frontier = HashSet::new();

            // 优化：使用 BatchExecutor 批量查询
            let node_ids: Vec<Uuid> = frontier
                .iter()
                .filter_map(|id_str| Uuid::parse_str(id_str).ok())
                .collect();

            if node_ids.is_empty() {
                break;
            }

            // 批量查询出边和入边
            let (out_map_res, in_map_res) = tokio::join!(
                self.batch_executor
                    .batch_get_outgoing_edges(user_id, &node_ids),
                self.batch_executor
                    .batch_get_incoming_edges(user_id, &node_ids)
            );

            let out_map = out_map_res?;
            let in_map = in_map_res?;

            let mut edges_to_process = Vec::new();

            for node_id in &node_ids {
                if let Some(edges) = out_map.get(node_id) {
                    edges_to_process.extend(edges.iter().cloned());
                }
                if let Some(edges) = in_map.get(node_id) {
                    edges_to_process.extend(edges.iter().cloned());
                }
            }

            let mut neighbor_ids_to_fetch = HashSet::new();

            for edge in edges_to_process {
                let is_outgoing = visited.contains(&edge.source_id.to_string());
                let neighbor_id = if is_outgoing {
                    edge.target_id
                } else {
                    edge.source_id
                };
                let neighbor_str = neighbor_id.to_string();

                if visited.contains(&neighbor_str) {
                    continue;
                }

                let is_relevant = match edge.relation {
                    RelationType::DerivedFrom | RelationType::EvolvedTo => true,
                    RelationType::RelatedTo
                        if edge.weight > self.auto_link_similarity_threshold =>
                    {
                        true
                    }
                    _ => false,
                };

                if is_relevant {
                    neighbor_ids_to_fetch.insert(neighbor_str.clone());
                    next_frontier.insert(neighbor_str);
                }
            }

            let ids_list: Vec<String> = neighbor_ids_to_fetch.into_iter().collect();
            if !ids_list.is_empty() {
                let units = self.fetch_units(user_id, ids_list).await?;
                for unit in units {
                    let score = 0.8_f32.powi((_d + 1) as i32) * 0.8;

                    let unit_id_str = unit.id.to_string();
                    results.insert(unit_id_str.clone(), (unit, score));
                    visited.insert(unit_id_str);
                }
            }

            frontier = next_frontier.into_iter().collect();
        }

        Ok(results.into_values().collect())
    }

    /// Perform hybrid search combining vector similarity and full-text search using Reciprocal Rank Fusion (RRF).
    pub async fn search_procedural(
        &self,
        user_id: &str,
        agent_id: Option<&str>,
        query_text: &str,
        vector: &[f32],
        limit: usize,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        let mut extra_filter =
            "(domain = 'agent' OR domain = 'user') AND memory_type = 'procedural'".to_string();
        if let Some(aid) = agent_id {
            extra_filter.push_str(&format!(" AND agent_id = '{}'", escape_sql_string(aid)));
        }

        let vec_filter = self.build_user_filter(user_id, Some(extra_filter));
        let vector_future = self
            .vector
            .search("memories", vector, limit * 2, vec_filter);

        // Skip Tantivy full-text for procedural, vector is better for behavior trajectories, or we can use it
        // Let's stick to vector-only for now, to ensure tight behavioral trajectory matches.
        let vector_hits = match vector_future.await {
            Ok(hits) => hits,
            Err(e) => {
                if e.to_string().contains("not found") {
                    Vec::new()
                } else {
                    return Err(e);
                }
            }
        };

        if vector_hits.is_empty() {
            return Ok(Vec::new());
        }

        let candidates_to_fetch: Vec<String> =
            vector_hits.iter().map(|(id, _)| id.clone()).collect();
        let mut units: Vec<MemoryUnit> = self.fetch_units(user_id, candidates_to_fetch).await?;

        // Ensure strictly procedural
        units.retain(|u| u.memory_type == memorose_common::MemoryType::Procedural);

        let mut seeds = Vec::new();
        for unit in units {
            let score = vector_hits
                .iter()
                .find(|(id, _)| *id == unit.id.to_string())
                .map(|(_, s)| *s)
                .unwrap_or(0.0);
            seeds.push((unit, score));
        }

        // We can do chronological trajectory tracking here in the future
        // For now, rerank and return
        let final_results = self.reranker.rerank(query_text, &self.kv_store, seeds).await?;

        Ok(final_results.into_iter().take(limit).collect())
    }

    pub async fn search_hybrid(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        agent_id: Option<&str>,
        query_text: &str,
        vector: &[f32],
        limit: usize,
        enable_arbitration: bool,
        min_score: Option<f32>,
        graph_depth: usize,
        valid_time: Option<TimeRange>,
        transaction_time: Option<TimeRange>,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        self.search_hybrid_with_token_budget(
            user_id,
            org_id,
            agent_id,
            query_text,
            vector,
            limit,
            enable_arbitration,
            min_score,
            graph_depth,
            valid_time,
            transaction_time,
            None,
        )
        .await
    }

    pub async fn search_hybrid_with_token_budget(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        agent_id: Option<&str>,
        query_text: &str,
        vector: &[f32],
        limit: usize,
        enable_arbitration: bool,
        min_score: Option<f32>,
        graph_depth: usize,
        valid_time: Option<TimeRange>,
        transaction_time: Option<TimeRange>,
        token_budget: Option<usize>,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        validate_id(user_id)?;
        if let Some(oid) = org_id { validate_id(oid)?; }
        if let Some(aid) = agent_id { validate_id(aid)?; }
        let time_filter = self.build_time_filter(valid_time.clone());
        let agent_filter = agent_id.map(|aid| format!("agent_id = '{}'", escape_sql_string(aid)));
        let org_filter = org_id.map(|oid| format!("org_id = '{}'", escape_sql_string(oid)));
        let mut filters = vec!["(domain = 'agent' OR domain = 'user')".to_string()];
        if let Some(filter) = time_filter {
            filters.push(filter);
        }
        if let Some(filter) = agent_filter {
            filters.push(filter);
        }
        if let Some(filter) = org_filter {
            filters.push(filter);
        }
        let extra = Some(filters.join(" AND "));
        let vec_filter = self.build_user_filter(user_id, extra);

        let vector_future = self
            .vector
            .search("memories", vector, limit * 2, vec_filter);

        let index = self.index.clone();
        let q_text = query_text.to_string();
        let vt = valid_time.clone();
        let tt = transaction_time.clone();
        let oid = org_id.map(|s| s.to_string());
        let uid = Some(user_id.to_string());
        let agid = agent_id.map(|s| s.to_string());
        let text_future = tokio::task::spawn_blocking(move || {
            // Ensure reader sees latest committed segments before searching
            index.reload().ok();
            index.search_bitemporal(
                &q_text,
                limit * 2,
                vt,
                tt,
                oid.as_deref(),
                uid.as_deref(),
                agid.as_deref(),
                None,
            )
        });

        let (vector_results, text_results) = tokio::join!(vector_future, text_future);

        let vector_hits = match vector_results {
            Ok(hits) => hits,
            Err(e) => {
                let msg = e.to_string().to_lowercase();
                // "Table 'memories' not found" is expected on a fresh node with no ingested data.
                // Require both a table-related term AND "not found" to avoid swallowing real errors.
                if (msg.contains("table") || msg.contains("no such")) && msg.contains("not found") {
                    Vec::new()
                } else {
                    return Err(e);
                }
            }
        };

        let text_hits = text_results??;

        // RRF Fusion on IDs
        let k = 60.0;
        let mut rrf_scores: HashMap<String, f32> = HashMap::new();

        for (rank, (id, _sim_score)) in vector_hits.into_iter().enumerate() {
            *rrf_scores.entry(id).or_default() += 1.0 / (k + rank as f32);
        }

        for (rank, id) in text_hits.into_iter().enumerate() {
            *rrf_scores.entry(id).or_default() += 1.0 / (k + rank as f32);
        }

        // Normalize RRF scores to [0, 1] range so they are compatible with reranker weights
        let max_rrf = rrf_scores.values().cloned().fold(0.0_f32, f32::max);
        if max_rrf > 0.0 {
            for score in rrf_scores.values_mut() {
                *score /= max_rrf;
            }
        }

        let mut sorted_ids: Vec<(String, f32)> = rrf_scores.into_iter().collect();
        sorted_ids.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let candidates_to_fetch: Vec<String> = sorted_ids
            .iter()
            .take(limit * 3)
            .map(|(id, _)| id.clone())
            .collect();
        let units: Vec<MemoryUnit> = self
            .fetch_units(user_id, candidates_to_fetch)
            .await?
            .into_iter()
            .filter(|unit| org_id.map_or(true, |oid| unit.org_id.as_deref() == Some(oid)))
            .collect();

        let mut seeds = Vec::new();
        for unit in units {
            let score = sorted_ids
                .iter()
                .find(|(id, _)| *id == unit.id.to_string())
                .map(|(_, s)| *s)
                .unwrap_or(0.0);
            seeds.push((unit, score));
        }

        // Graph Expansion (BFS)
        let mut expanded_units = self.expand_subgraph(user_id, seeds, graph_depth).await?;
        if let Some(org_id) = org_id {
            expanded_units.retain(|(unit, _)| unit.org_id.as_deref() == Some(org_id));
        }

        expanded_units.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Time and Importance Reranking
        let final_results = self
            .reranker
            .rerank(query_text, &self.kv_store, expanded_units)
            .await?;

        // Default threshold lowered: RRF scores are now normalized to [0,1], and the
        // reranker adds importance (0.2) + recency (0.1) components, so a reasonable
        // cutoff is ~0.3 to keep relevant results while filtering noise.
        let threshold = min_score.unwrap_or(0.3);
        let mut final_results: Vec<_> = final_results
            .into_iter()
            .filter(|(_, score)| *score >= threshold)
            .collect();

        if final_results.is_empty() {
            return Ok(Vec::new());
        }

        // Semantic Dedup — O(N²·D) where N = min(results, dedup_cap) and D = embedding dimension.
        // The dedup_cap (limit * 4, min 20) bounds N to a small constant in practice (typically < 80),
        // making the quadratic cost acceptable. SimHash/MinHash would add complexity without meaningful
        // speedup at these sizes.
        let dedup_cap = (limit * 4).max(20);
        if final_results.len() > dedup_cap {
            final_results.truncate(dedup_cap);
        }
        let mut deduped_results: Vec<(MemoryUnit, f32)> = Vec::new();
        for (unit, score) in final_results {
            let mut is_duplicate = false;
            for (existing_unit, _) in &deduped_results {
                if let (Some(v1), Some(v2)) = (&unit.embedding, &existing_unit.embedding) {
                    if cosine_similarity(v1, v2) > 0.92 {
                        is_duplicate = true;
                        break;
                    }
                }
            }
            if !is_duplicate {
                deduped_results.push((unit, score));
            }
        }
        final_results = deduped_results;

        let mut results_for_arbitration = final_results;
        if results_for_arbitration.len() > limit * 2 {
            results_for_arbitration.truncate(limit * 2);
        }

        // Heuristic Arbitration Trigger
        let mut should_arbitrate = false;
        if enable_arbitration && results_for_arbitration.len() >= 2 {
            let top1_score = results_for_arbitration[0].1;
            let top2_score = results_for_arbitration[1].1;

            if (top1_score - top2_score).abs() < 0.25 {
                should_arbitrate = true;
            } else {
                tracing::info!(
                    "Skipping arbitration due to high confidence in Top 1 (Score gap: {:.2})",
                    (top1_score - top2_score).abs()
                );
            }
        }

        if should_arbitrate {
            tracing::info!(
                "Executing LLM Arbitration for {} candidates...",
                results_for_arbitration.len()
            );
            let units_to_arbitrate: Vec<MemoryUnit> = results_for_arbitration
                .iter()
                .map(|(u, _)| u.clone())
                .collect();
            let arbitrated = self
                .arbitrator
                .arbitrate(units_to_arbitrate, Some(query_text))
                .await?;

            let mut arbitrated_results = Vec::new();
            for unit in arbitrated {
                if let Some((_, score)) = results_for_arbitration
                    .iter()
                    .find(|(u, _)| u.id == unit.id)
                {
                    arbitrated_results.push((unit, *score));
                }
            }
            Ok(Self::apply_token_budget_to_scored_memory_units(
                arbitrated_results,
                token_budget,
            ))
        } else {
            Ok(Self::apply_token_budget_to_scored_memory_units(
                results_for_arbitration,
                token_budget,
            ))
        }
    }

    pub(crate) async fn search_shared_scope(
        &self,
        domain: MemoryDomain,
        org_id: Option<&str>,
        agent_id: Option<&str>,
        query_text: &str,
        vector: &[f32],
        limit: usize,
        min_score: Option<f32>,
        valid_time: Option<TimeRange>,
    ) -> Result<Vec<(SharedSearchHit, f32)>> {
        if domain == MemoryDomain::Organization {
            let Some(org_id) = org_id else {
                return Ok(Vec::new());
            };
            let record_hits = self
                .search_organization_knowledge_records(
                    org_id, query_text, vector, limit, min_score, valid_time,
                )
                .await?;
            return self.materialize_organization_search_hits(record_hits).await;
        }

        let shared_agent_filter = match domain {
            MemoryDomain::Organization => None,
            _ => agent_id,
        };
        let filter = self.build_global_filter(
            domain,
            org_id,
            shared_agent_filter,
            self.build_time_filter(valid_time),
        );

        let hits = match self
            .vector
            .search("memories", vector, limit * 2, filter)
            .await
        {
            Ok(hits) => hits,
            Err(error) => {
                let msg = error.to_string().to_lowercase();
                if (msg.contains("table") || msg.contains("no such")) && msg.contains("not found") {
                    Vec::new()
                } else {
                    return Err(error);
                }
            }
        };

        if hits.is_empty() {
            return Ok(Vec::new());
        }

        let candidates = self.fetch_units_with_scores_global(hits).await?;
        let mut reranked = self
            .reranker
            .rerank(
                query_text,
                &self.kv_store,
                candidates
                    .iter()
                    .map(|(hit, score)| (hit.memory_unit().clone(), *score))
                    .collect(),
            )
            .await?;
        let threshold = min_score.unwrap_or(0.3);
        reranked.retain(|(_, score)| *score >= threshold);
        let mut scored_hits = Vec::with_capacity(reranked.len());
        for (unit, score) in reranked {
            if let Some((hit, _)) = candidates.iter().find(|(hit, _)| hit.id == unit.id) {
                scored_hits.push((hit.clone(), score));
            }
        }
        Ok(scored_hits)
    }

    pub async fn search_hybrid_with_shared(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        agent_id: Option<&str>,
        query_text: &str,
        vector: &[f32],
        limit: usize,
        enable_arbitration: bool,
        min_score: Option<f32>,
        graph_depth: usize,
        valid_time: Option<TimeRange>,
        transaction_time: Option<TimeRange>,
    ) -> Result<Vec<(SharedSearchHit, f32)>> {
        self.search_hybrid_with_shared_and_token_budget(
            user_id,
            org_id,
            agent_id,
            query_text,
            vector,
            limit,
            enable_arbitration,
            min_score,
            graph_depth,
            valid_time,
            transaction_time,
            None,
        )
        .await
    }

    pub async fn search_hybrid_with_shared_and_token_budget(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        agent_id: Option<&str>,
        query_text: &str,
        vector: &[f32],
        limit: usize,
        enable_arbitration: bool,
        min_score: Option<f32>,
        graph_depth: usize,
        valid_time: Option<TimeRange>,
        transaction_time: Option<TimeRange>,
        token_budget: Option<usize>,
    ) -> Result<Vec<(SharedSearchHit, f32)>> {
        let mut combined = self
            .search_hybrid(
                user_id,
                org_id,
                agent_id,
                query_text,
                vector,
                limit,
                false,
                min_score,
                graph_depth,
                valid_time.clone(),
                transaction_time,
            )
            .await?
            .into_iter()
            .map(|(unit, score)| (SharedSearchHit::native(unit), score))
            .collect::<Vec<_>>();

        if let Some(org_id) = org_id {
            let org_policy = self.get_org_share_policy(user_id, org_id)?;
            if org_policy.consume {
                let mut org_results = self
                    .search_shared_scope(
                        MemoryDomain::Organization,
                        Some(org_id),
                        agent_id,
                        query_text,
                        vector,
                        limit,
                        min_score,
                        valid_time,
                    )
                    .await?;
                for (_, score) in &mut org_results {
                    *score *= 0.7;
                }
                combined.extend(org_results);
            }
        }

        if combined.is_empty() {
            return Ok(Vec::new());
        }

        combined.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut deduped: Vec<(SharedSearchHit, f32)> = Vec::new();
        let mut seen_ids = HashSet::new();
        for (hit, score) in combined {
            if !seen_ids.insert(hit.id) {
                continue;
            }

            let mut is_duplicate = false;
            for (existing, _) in &deduped {
                if let (Some(v1), Some(v2)) = (&hit.embedding, &existing.embedding) {
                    if cosine_similarity(v1, v2) > 0.92 {
                        is_duplicate = true;
                        break;
                    }
                }
            }

            if !is_duplicate {
                deduped.push((hit, score));
            }

            if deduped.len() >= limit * 2 {
                break;
            }
        }

        let threshold = min_score.unwrap_or(0.3);
        deduped.retain(|(_, score)| *score >= threshold);
        if deduped.is_empty() {
            return Ok(Vec::new());
        }

        if deduped.len() > limit * 2 {
            deduped.truncate(limit * 2);
        }

        let should_arbitrate =
            enable_arbitration && deduped.len() >= 2 && (deduped[0].1 - deduped[1].1).abs() < 0.25;

        if should_arbitrate {
            let arbitrated = self
                .arbitrator
                .arbitrate(
                    deduped
                        .iter()
                        .map(|(hit, _)| hit.memory_unit().clone())
                        .collect(),
                    Some(query_text),
                )
                .await?;

            let mut final_results = Vec::new();
            for unit in arbitrated {
                if let Some((hit, score)) = deduped
                    .iter()
                    .find(|(candidate, _)| candidate.id == unit.id)
                {
                    final_results.push((hit.clone(), *score));
                }
            }
            Ok(Self::apply_token_budget_to_scored_shared_hits(
                final_results,
                token_budget,
            ))
        } else {
            deduped.truncate(limit);
            Ok(Self::apply_token_budget_to_scored_shared_hits(
                deduped,
                token_budget,
            ))
        }
    }

    pub async fn search_text(
        &self,
        user_id: &str,
        query: &str,
        limit: usize,
        enable_arbitration: bool,
        time_range: Option<TimeRange>,
    ) -> Result<Vec<MemoryUnit>> {
        let index = self.index.clone();
        tokio::task::spawn_blocking(move || {
            index.reload().ok();
        })
        .await?;

        let index = self.index.clone();
        let q = query.to_string();
        let tr = time_range.clone();
        let uid = Some(user_id.to_string());
        let ids =
            tokio::task::spawn_blocking(move || index.search(&q, limit, tr, None, uid.as_deref()))
                .await??;

        let mut units = self.fetch_units(user_id, ids).await?;
        units.retain(|unit| Self::is_local_domain(&unit.domain));

        if enable_arbitration {
            self.arbitrator.arbitrate(units, Some(query)).await
        } else {
            Ok(units)
        }
    }

    pub async fn search_text_with_shared(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        query: &str,
        limit: usize,
        enable_arbitration: bool,
        time_range: Option<TimeRange>,
    ) -> Result<Vec<SharedSearchHit>> {
        let index = self.index.clone();
        tokio::task::spawn_blocking(move || {
            index.reload().ok();
        })
        .await?;

        let k = 60.0;
        let mut combined_scores: HashMap<Uuid, (SharedSearchHit, f32)> = HashMap::new();
        for (rank, unit) in self
            .search_text(user_id, query, limit, false, time_range.clone())
            .await?
            .into_iter()
            .enumerate()
        {
            let score = 1.0 / (k + rank as f32);
            combined_scores
                .entry(unit.id)
                .and_modify(|(_, existing_score)| *existing_score += score)
                .or_insert((SharedSearchHit::native(unit), score));
        }

        if let Some(org_id) = org_id {
            let org_policy = self.get_org_share_policy(user_id, org_id)?;
            if org_policy.consume {
                for (rank, unit) in self
                    .search_organization_knowledge_text(org_id, query, limit, time_range.clone())
                    .await?
                    .into_iter()
                    .enumerate()
                {
                    let score = 0.7 / (k + rank as f32);
                    combined_scores
                        .entry(unit.id)
                        .and_modify(|(_, existing_score)| *existing_score += score)
                        .or_insert((unit, score));
                }
            }
        }

        if combined_scores.is_empty() {
            return Ok(Vec::new());
        }

        let mut hits: Vec<(SharedSearchHit, f32)> = combined_scores.into_values().collect();
        hits.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        if hits.len() > limit * 2 {
            hits.truncate(limit * 2);
        }

        if enable_arbitration {
            let arbitrated = self
                .arbitrator
                .arbitrate(
                    hits.iter()
                        .map(|(hit, _)| hit.memory_unit().clone())
                        .collect(),
                    Some(query),
                )
                .await?;
            let mut final_hits = Vec::new();
            for unit in arbitrated {
                if let Some((hit, _)) = hits.iter().find(|(candidate, _)| candidate.id == unit.id) {
                    final_hits.push(hit.clone());
                }
            }
            if final_hits.len() > limit {
                final_hits.truncate(limit);
            }
            Ok(final_hits)
        } else {
            let mut final_hits = hits.into_iter().map(|(hit, _)| hit).collect::<Vec<_>>();
            if final_hits.len() > limit {
                final_hits.truncate(limit);
            }
            Ok(final_hits)
        }
    }

    /// Search and then consolidate the results into a single narrative.
    pub async fn search_consolidated(
        &self,
        user_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<String> {
        let units = self.search_text(user_id, query, limit, false, None).await?;
        self.arbitrator.consolidate(units).await
    }
}
