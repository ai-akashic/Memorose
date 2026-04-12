use anyhow::Result;
use memorose_common::{
    tokenizer::count_tokens, GraphEdge, MemoryDomain, MemoryUnit,
    RelationType,
};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use uuid::Uuid;
use super::types::SharedSearchHit;

impl super::MemoroseEngine {
    pub async fn store_memory_unit(&self, unit: MemoryUnit) -> Result<()> {
        self.store_memory_unit_with_depth(unit, 0).await
    }

    pub(crate) async fn store_memory_unit_with_depth(&self, unit: MemoryUnit, depth: usize) -> Result<()> {
        let is_goal = unit.level == 3;
        let unit_id = unit.id;
        let user_id = unit.user_id.clone();
        let org_id = unit.org_id.clone();
        let agent_id = unit.agent_id.clone();
        let stream_id = unit.stream_id;
        let content = unit.content.clone();
        let references = unit.references.clone();

        self.store_memory_units(vec![unit]).await?;

        // Handle Explicit Linking (Task Hierarchy)
        if !references.is_empty() {
            for parent_id in references {
                let edge = GraphEdge::new(
                    user_id.clone(),
                    unit_id,
                    parent_id,
                    RelationType::IsSubTaskOf,
                    1.0,
                );
                self.graph.add_edge(&edge).await?;
            }
        }

        // Handle Auto-Planning for L3 Goals
        if is_goal && self.auto_planner && depth < 5 {
            // Write a "pending" marker so callers can observe the in-flight planning state.
            // The task clears it to "done" or "failed" when it finishes.
            let planning_key = format!("planning:{}", unit_id);
            let _ = self.system_kv().put(planning_key.as_bytes(), b"pending");

            let engine = self.clone();
            let uid = user_id.clone();
            let cnt = content.clone();
            let org = org_id.clone();
            let agent = agent_id.clone();
            tokio::spawn(async move {
                let key = format!("planning:{}", unit_id);
                match engine
                    .auto_plan_goal(org, uid, agent, stream_id, unit_id, cnt, depth + 1)
                    .await
                {
                    Ok(()) => {
                        let _ = engine.system_kv().put(key.as_bytes(), b"done");
                    }
                    Err(e) => {
                        tracing::error!("Auto-planning failed for goal {}: {:?}", unit_id, e);
                        let _ = engine.system_kv().put(key.as_bytes(), b"failed");
                    }
                }
            });
        }
        Ok(())
    }
    pub async fn store_memory_units(&self, units: Vec<MemoryUnit>) -> Result<()> {
        self.store_memory_units_internal(units, true).await
    }

    pub(crate) async fn store_memory_units_internal(
        &self,
        units: Vec<MemoryUnit>,
        run_reconciliation: bool,
    ) -> Result<()> {
        if units.is_empty() {
            return Ok(());
        }

        Self::validate_materialized_units(&units)?;

        // 1. Store Metadata in KV (user-prefixed keys + global index)
        let kv = self.kv_store.clone();
        let mut kv_batch = rocksdb::WriteBatch::default();
        let mut reflection_deltas: HashMap<String, (usize, usize, i64, i64, String)> =
            HashMap::new();
        for unit in &units {
            let key = format!("u:{}:unit:{}", unit.user_id, unit.id);
            let val = serde_json::to_vec(unit)?;
            kv_batch.put(key.as_bytes(), &val);

            // Global index for dashboard lookups
            let idx_key = format!("idx:unit:{}", unit.id);
            kv_batch.put(idx_key.as_bytes(), unit.user_id.as_bytes());

            if unit.level == 1 && Self::is_local_domain(&unit.domain) {
                let tx_micros = unit.transaction_time.timestamp_micros();
                let entry = reflection_deltas.entry(unit.user_id.clone()).or_insert((
                    0,
                    0,
                    tx_micros,
                    tx_micros,
                    unit.id.to_string(),
                ));
                entry.0 = entry.0.saturating_add(1);
                entry.1 = entry.1.saturating_add(count_tokens(&unit.content));
                if tx_micros < entry.2 || (tx_micros == entry.2 && unit.id.to_string() < entry.4) {
                    entry.2 = tx_micros;
                    entry.4 = unit.id.to_string();
                }
                entry.3 = entry.3.max(tx_micros);
            }
        }

        tokio::task::spawn_blocking(move || {
            kv.write_batch(kv_batch)?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        for (
            user_id,
            (pending_units, pending_tokens, first_tx_micros, last_tx_micros, first_event_id),
        ) in reflection_deltas
        {
            self.bump_reflection_marker_with_window(
                &user_id,
                pending_units,
                pending_tokens,
                Some(first_tx_micros),
                Some(last_tx_micros),
                Some(first_event_id),
            )?;
        }

        // Maintain L1 secondary index for efficient fetch_recent_l1_units.
        // Key: "l1_idx:{user_id}:{id}" -> timestamp_micros as little-endian bytes (fast sort, no JSON).
        // The user_id prefix is critical: without it the global scan mixes all users' L1 units.
        let l1_units: Vec<(String, String, i64)> = units
            .iter()
            .filter(|u| u.level == 1 && Self::is_local_domain(&u.domain))
            .map(|u| {
                (
                    u.user_id.clone(),
                    u.id.to_string(),
                    u.transaction_time.timestamp_micros(),
                )
            })
            .collect();
        if !l1_units.is_empty() {
            let kv_l1 = self.kv_store.clone();
            tokio::task::spawn_blocking(move || {
                let mut batch = rocksdb::WriteBatch::default();
                for (uid, id, ts_micros) in &l1_units {
                    let key = format!("l1_idx:{}:{}", uid, id);
                    batch.put(key.as_bytes(), ts_micros.to_le_bytes());
                }
                kv_l1.write_batch(batch)?;
                Ok::<(), anyhow::Error>(())
            })
            .await??;
        }

        // 2. Store Vector in Lance (single "memories" table)
        let units_with_embeddings: Vec<MemoryUnit> = units
            .iter()
            .filter(|u| u.embedding.is_some())
            .cloned()
            .collect();

        if !units_with_embeddings.is_empty() {
            self.vector.ensure_table("memories").await?;
            self.vector.add("memories", units_with_embeddings).await?;
        }

        // 3. Index Text in Tantivy
        // Optimization: Removed immediate commit/reload to improve write throughput.
        // We rely on the background commit loop (configured via commit_interval_ms) for eventual consistency.
        let index = self.index.clone();
        let units_for_index = units.clone();
        tokio::task::spawn_blocking(move || {
            for unit in &units_for_index {
                index.index_unit(unit)?;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        // 4. Automatic Semantic Linking (Parallelized)
        let units_for_org_publication = units.clone();
        let mut join_set = tokio::task::JoinSet::new();
        for unit in units {
            let engine = self.clone();
            join_set.spawn(async move {
                if !Self::is_local_domain(&unit.domain) {
                    return;
                }
                if run_reconciliation {
                    if let Err(e) = engine.reconcile_conflicting_memory_unit(&unit).await {
                        tracing::error!(
                            "Memory reconciliation failed for unit {}: {:?}",
                            unit.id,
                            e
                        );
                    }
                }
                match engine.is_visible_memory_unit(&unit) {
                    Ok(true) => {}
                    Ok(false) => return,
                    Err(e) => {
                        tracing::error!("Failed to check visibility for unit {}: {:?}", unit.id, e);
                        return;
                    }
                }
                if let Err(e) = engine.auto_link_memory(&unit).await {
                    tracing::error!("Auto-linking failed for unit {}: {:?}", unit.id, e);
                }
                if let Err(e) = engine.semantic_link_memory(&unit).await {
                    tracing::error!("Semantic linking failed for unit {}: {:?}", unit.id, e);
                }
            });
        }

        while let Some(res) = join_set.join_next().await {
            if let Err(e) = res {
                tracing::error!("Parallel linking task panicked: {:?}", e);
            }
        }

        self.publish_native_shared_knowledge(&units_for_org_publication)
            .await?;

        Ok(())
    }

    // ── Memory Retrieval ────────────────────────────────────────────

    pub(crate) fn get_memory_unit_raw(&self, user_id: &str, id: Uuid) -> Result<Option<MemoryUnit>> {
        let key = format!("u:{}:unit:{}", user_id, id);
        let val = self.kv_store.get(key.as_bytes())?;
        match val {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    pub async fn get_memory_unit(&self, user_id: &str, id: Uuid) -> Result<Option<MemoryUnit>> {
        if self.is_memory_unit_forgotten(user_id, id)? {
            return Ok(None);
        }
        Ok(self
            .get_memory_unit_raw(user_id, id)?
            .filter(|unit| self.is_visible_memory_unit(unit).unwrap_or(false)))
    }

    /// Return a native memory unit even if it has been logically forgotten.
    /// This is useful for irreversible cleanup flows that must remove
    /// previously tombstoned storage from all backends.
    pub fn get_memory_unit_including_forgotten(
        &self,
        user_id: &str,
        id: Uuid,
    ) -> Result<Option<MemoryUnit>> {
        self.get_memory_unit_raw(user_id, id)
    }

    pub async fn get_native_memory_unit_by_index(&self, id: Uuid) -> Result<Option<MemoryUnit>> {
        let idx_key = format!("idx:unit:{}", id);
        if let Some(uid_bytes) = self.kv_store.get(idx_key.as_bytes())? {
            let user_id = String::from_utf8(uid_bytes)?;
            self.get_memory_unit(&user_id, id).await
        } else {
            Ok(None)
        }
    }

    pub async fn get_shared_search_hit_by_index(
        &self,
        id: Uuid,
    ) -> Result<Option<SharedSearchHit>> {
        if let Some(record) = self.load_organization_knowledge(id)? {
            let unit = self
                .materialize_organization_read_view_for_record(&record)
                .await?;
            return Ok(Some(SharedSearchHit::organization_knowledge(&record, unit)));
        }

        Ok(self
            .get_native_memory_unit_by_index(id)
            .await?
            .map(SharedSearchHit::native))
    }

    pub async fn fetch_recent_l1_units(
        &self,
        user_id: &str,
        limit: usize,
    ) -> Result<Vec<MemoryUnit>> {
        self.fetch_recent_l1_units_with_min_tx(user_id, limit, None)
            .await
    }

    pub async fn fetch_recent_l1_units_since(
        &self,
        user_id: &str,
        min_transaction_time_micros: i64,
        limit: usize,
    ) -> Result<Vec<MemoryUnit>> {
        self.fetch_recent_l1_units_with_min_tx(user_id, limit, Some(min_transaction_time_micros))
            .await
    }

    pub(crate) async fn fetch_recent_l1_units_with_min_tx(
        &self,
        user_id: &str,
        limit: usize,
        min_transaction_time_micros: Option<i64>,
    ) -> Result<Vec<MemoryUnit>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = format!("u:{}:unit:", user_id);
        let store = self.kv_store.clone();
        let prefix_bytes = prefix.into_bytes();

        // Scan the compact L1 index (values are 8-byte timestamps, much cheaper than full units).
        // The prefix is user-scoped so we only read this user's entries.
        let l1_index_prefix = format!("l1_idx:{}:", user_id).into_bytes();
        let strip_prefix = format!("l1_idx:{}:", user_id);
        let index_pairs = tokio::task::spawn_blocking({
            let store = store.clone();
            move || store.scan(&l1_index_prefix)
        })
        .await??;

        if index_pairs.is_empty() {
            // Fallback for nodes that pre-date the L1 index: scan full units.
            return self
                .fetch_recent_l1_units_fallback(prefix_bytes, limit, min_transaction_time_micros)
                .await;
        }

        // Keep only the top-k newest IDs without sorting the entire index.
        let mut heap: BinaryHeap<(Reverse<i64>, String)> = BinaryHeap::with_capacity(limit + 1);
        for (key, value) in index_pairs {
            let Some((id, ts)) = (|| {
                let key_str = String::from_utf8(key).ok()?;
                let id = key_str.strip_prefix(&strip_prefix)?.to_string();
                let ts = i64::from_le_bytes(value.as_slice().try_into().ok()?);
                Some((id, ts))
            })() else {
                continue;
            };
            if min_transaction_time_micros.is_some_and(|min_ts| ts < min_ts) {
                continue;
            }

            if heap.len() < limit {
                heap.push((Reverse(ts), id));
                continue;
            }

            if let Some((Reverse(oldest_ts), _)) = heap.peek() {
                if ts > *oldest_ts {
                    heap.pop();
                    heap.push((Reverse(ts), id));
                }
            }
        }

        let mut id_ts: Vec<(String, i64)> =
            heap.into_iter().map(|(Reverse(ts), id)| (id, ts)).collect();
        id_ts.sort_by(|a, b| b.1.cmp(&a.1));

        // Multi-get the actual units by their KV keys.
        let keys: Vec<String> = id_ts
            .iter()
            .map(|(id, _)| format!("u:{}:unit:{}", user_id, id))
            .collect();

        let values = tokio::task::spawn_blocking({
            let store = store.clone();
            let key_refs_owned: Vec<Vec<u8>> = keys.iter().map(|k| k.as_bytes().to_vec()).collect();
            move || {
                store.multi_get(
                    &key_refs_owned
                        .iter()
                        .map(|k| k.as_slice())
                        .collect::<Vec<_>>(),
                )
            }
        })
        .await??;

        let results: Vec<MemoryUnit> = values
            .into_iter()
            .filter_map(|v| v.and_then(|bytes| serde_json::from_slice::<MemoryUnit>(&bytes).ok()))
            .filter(|unit: &MemoryUnit| {
                Self::is_local_domain(&unit.domain)
                    && self.is_visible_memory_unit(unit).unwrap_or(false)
            })
            .collect();

        Ok(results)
    }

    pub(crate) async fn fetch_recent_l1_units_fallback(
        &self,
        prefix_bytes: Vec<u8>,
        limit: usize,
        min_transaction_time_micros: Option<i64>,
    ) -> Result<Vec<MemoryUnit>> {
        let store = self.kv_store.clone();
        let pairs = tokio::task::spawn_blocking(move || store.scan(&prefix_bytes)).await??;
        let mut results: Vec<MemoryUnit> = pairs
            .into_iter()
            .filter_map(|(_, val)| serde_json::from_slice::<MemoryUnit>(&val).ok())
            .filter(|u| {
                u.level == 1
                    && Self::is_local_domain(&u.domain)
                    && self.is_visible_memory_unit(u).unwrap_or(false)
            })
            .filter(|u| {
                min_transaction_time_micros
                    .map(|min_ts| u.transaction_time.timestamp_micros() >= min_ts)
                    .unwrap_or(true)
            })
            .collect();
        results.sort_by(|a, b| b.transaction_time.cmp(&a.transaction_time));
        results.truncate(limit);
        Ok(results)
    }

    /// Count the total number of L1 memory units for a specific user.
    pub async fn count_l1_units(&self, user_id: &str) -> Result<usize> {
        let prefix = format!("u:{}:unit:", user_id);
        let store = self.kv_store.clone();
        let prefix_bytes = prefix.into_bytes();

        // Try the L1 index first (only counts IDs, much cheaper).
        // Prefix is user-scoped so this returns only this user's L1 count.
        let l1_index_prefix = format!("l1_idx:{}:", user_id).into_bytes();
        let index_pairs = tokio::task::spawn_blocking({
            let store = store.clone();
            move || store.scan(&l1_index_prefix)
        })
        .await??;

        if !index_pairs.is_empty() {
            return Ok(index_pairs.len());
        }

        // Fallback: scan all units and count level-1 ones.
        let count = tokio::task::spawn_blocking(move || {
            let pairs = store.scan(&prefix_bytes)?;
            let count = pairs
                .into_iter()
                .filter_map(|(_, val)| serde_json::from_slice::<MemoryUnit>(&val).ok())
                .filter(|u| u.level == 1 && Self::is_local_domain(&u.domain))
                .count();
            Ok::<usize, anyhow::Error>(count)
        })
        .await??;

        Ok(count)
    }

    /// Track cumulative L1 growth and return the count range crossed by this update.
    pub async fn bump_l1_count_and_get_range(
        &self,
        user_id: &str,
        delta: usize,
    ) -> Result<(usize, usize)> {
        if delta == 0 {
            let current = self.current_l1_count(user_id).await?;
            return Ok((current, current));
        }

        let key = format!("l1_count:{}", user_id);
        if let Some(bytes) = self.system_kv().get(key.as_bytes())? {
            let current = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])) as usize;
            let updated = current.saturating_add(delta);
            self.system_kv()
                .put(key.as_bytes(), &(updated as u64).to_le_bytes())?;
            return Ok((current, updated));
        }

        // Initialize from persisted storage when the counter has not been materialized yet.
        let current_after_store = self.count_l1_units(user_id).await?;
        let current_before_store = current_after_store.saturating_sub(delta);
        self.system_kv()
            .put(key.as_bytes(), &(current_after_store as u64).to_le_bytes())?;
        Ok((current_before_store, current_after_store))
    }

    pub(crate) async fn current_l1_count(&self, user_id: &str) -> Result<usize> {
        let key = format!("l1_count:{}", user_id);
        if let Some(bytes) = self.system_kv().get(key.as_bytes())? {
            return Ok(u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])) as usize);
        }

        let current = self.count_l1_units(user_id).await?;
        self.system_kv()
            .put(key.as_bytes(), &(current as u64).to_le_bytes())?;
        Ok(current)
    }

    pub async fn fetch_units_with_scores(
        &self,
        user_id: &str,
        results: Vec<(String, f32)>,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        if results.is_empty() {
            return Ok(Vec::new());
        }

        let keys: Vec<String> = results
            .iter()
            .map(|(id, _)| format!("u:{}:unit:{}", user_id, id))
            .collect();
        let store = self.kv_store.clone();

        let db_results = tokio::task::spawn_blocking(move || {
            let key_bytes: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
            store.multi_get(&key_bytes)
        })
        .await??;

        let mut final_results = Vec::new();
        for (i, res) in db_results.into_iter().enumerate() {
            if let Some(bytes) = res {
                if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(&bytes) {
                    if !self.is_visible_memory_unit(&unit)? {
                        continue;
                    }
                    final_results.push((unit, results[i].1));
                }
            }
        }
        Ok(final_results)
    }

    pub async fn fetch_units_with_scores_global(
        &self,
        results: Vec<(String, f32)>,
    ) -> Result<Vec<(SharedSearchHit, f32)>> {
        if results.is_empty() {
            return Ok(Vec::new());
        }

        let mut final_results = Vec::new();
        for (id, score) in results {
            let parsed = match Uuid::parse_str(&id) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };

            if let Some(hit) = self.get_shared_search_hit_by_index(parsed).await? {
                final_results.push((hit, score));
            }
        }

        Ok(final_results)
    }

    pub async fn fetch_units(&self, user_id: &str, ids: Vec<String>) -> Result<Vec<MemoryUnit>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let keys: Vec<String> = ids
            .iter()
            .map(|id| format!("u:{}:unit:{}", user_id, id))
            .collect();
        let store = self.kv_store.clone();

        let results = tokio::task::spawn_blocking(move || {
            let key_bytes: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
            store.multi_get(&key_bytes)
        })
        .await??;

        let mut units = Vec::new();
        for res in results {
            if let Some(bytes) = res {
                if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(&bytes) {
                    if !self.is_visible_memory_unit(&unit)? {
                        continue;
                    }
                    units.push(unit);
                }
            }
        }
        Ok(units)
    }

    pub async fn list_memory_units_global(
        &self,
        user_id_filter: Option<&str>,
    ) -> Result<Vec<MemoryUnit>> {
        let prefix = if let Some(user_id) = user_id_filter {
            format!("u:{}:unit:", user_id).into_bytes()
        } else {
            b"u:".to_vec()
        };
        let kv = self.kv_store.clone();
        let pairs = tokio::task::spawn_blocking(move || kv.scan(&prefix)).await??;

        let mut units = Vec::new();
        for (key, val) in pairs {
            let is_unit_key = if user_id_filter.is_some() {
                true
            } else {
                key.windows(6).any(|window| window == b":unit:")
            };
            if !is_unit_key {
                continue;
            }
            let Ok(unit) = serde_json::from_slice::<MemoryUnit>(&val) else {
                continue;
            };
            if unit.domain == MemoryDomain::Organization || !self.is_visible_memory_unit(&unit)? {
                continue;
            }
            units.push(unit);
        }

        if user_id_filter.is_none() {
            units.extend(self.list_organization_read_units(None).await?);
        }

        Ok(units)
    }

}
