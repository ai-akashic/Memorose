use anyhow::Result;
use memorose_common::tokenizer::count_tokens;
use memorose_common::{GraphEdge, MemoryUnit, RelationType};
use uuid::Uuid;
use super::types::{PendingMaterializationJob, ReflectionBatchOutcome, ReflectionMarker};

impl super::MemoroseEngine {
    pub fn set_needs_reflect(&self, user_id: &str) -> Result<()> {
        self.bump_reflection_marker(user_id, 1, 0)
    }

    pub fn bump_reflection_marker(
        &self,
        user_id: &str,
        pending_units_delta: usize,
        pending_tokens_delta: usize,
    ) -> Result<()> {
        self.bump_reflection_marker_with_window(
            user_id,
            pending_units_delta,
            pending_tokens_delta,
            None,
            None,
            None,
        )
    }

    pub fn bump_reflection_marker_with_window(
        &self,
        user_id: &str,
        pending_units_delta: usize,
        pending_tokens_delta: usize,
        first_event_tx_micros: Option<i64>,
        last_event_tx_micros: Option<i64>,
        first_event_id: Option<String>,
    ) -> Result<()> {
        let key = format!("needs_reflect:{}", user_id);
        let now = chrono::Utc::now().timestamp_millis();
        let next = match self.system_kv().get(key.as_bytes())? {
            Some(raw) => {
                let mut marker =
                    serde_json::from_slice::<ReflectionMarker>(&raw).unwrap_or_else(|_| {
                        ReflectionMarker {
                            first_event_at_ts: now,
                            last_event_at_ts: now,
                            pending_units: 0,
                            pending_tokens: 0,
                            first_event_tx_micros: 0,
                            last_event_tx_micros: 0,
                            first_event_id: None,
                        }
                    });
                if marker.first_event_at_ts <= 0 {
                    marker.first_event_at_ts = now;
                }
                marker.last_event_at_ts = now;
                marker.pending_units = marker.pending_units.saturating_add(pending_units_delta);
                marker.pending_tokens = marker.pending_tokens.saturating_add(pending_tokens_delta);
                if let Some(first_tx) = first_event_tx_micros {
                    let should_update_cursor = marker.first_event_tx_micros <= 0
                        || first_tx < marker.first_event_tx_micros
                        || (first_tx == marker.first_event_tx_micros
                            && first_event_id
                                .as_ref()
                                .zip(marker.first_event_id.as_ref())
                                .map(|(new_id, old_id)| new_id < old_id)
                                .unwrap_or(marker.first_event_id.is_none()));
                    if should_update_cursor {
                        marker.first_event_tx_micros = first_tx;
                        marker.first_event_id = first_event_id.clone();
                    }
                }
                if let Some(last_tx) = last_event_tx_micros {
                    marker.last_event_tx_micros = marker.last_event_tx_micros.max(last_tx);
                }
                marker
            }
            None => ReflectionMarker {
                first_event_at_ts: now,
                last_event_at_ts: now,
                pending_units: pending_units_delta,
                pending_tokens: pending_tokens_delta,
                first_event_tx_micros: first_event_tx_micros.unwrap_or_default(),
                last_event_tx_micros: last_event_tx_micros.unwrap_or_default(),
                first_event_id,
            },
        };
        self.system_kv()
            .put(key.as_bytes(), &serde_json::to_vec(&next)?)
    }

    pub fn consume_reflection_marker_batch(
        &self,
        user_id: &str,
        consumed_units: usize,
        consumed_tokens: usize,
        next_first_event_tx_micros: Option<i64>,
        next_first_event_id: Option<String>,
    ) -> Result<()> {
        let key = format!("needs_reflect:{}", user_id);
        let Some(raw) = self.system_kv().get(key.as_bytes())? else {
            return Ok(());
        };
        let mut marker = serde_json::from_slice::<ReflectionMarker>(&raw).unwrap_or_default();
        marker.pending_units = marker.pending_units.saturating_sub(consumed_units);
        marker.pending_tokens = marker.pending_tokens.saturating_sub(consumed_tokens);

        if marker.pending_units == 0 || next_first_event_tx_micros.is_none() {
            return self.clear_reflection_marker(user_id);
        }

        marker.first_event_tx_micros = next_first_event_tx_micros.unwrap_or_default();
        marker.first_event_id = next_first_event_id;
        self.system_kv()
            .put(key.as_bytes(), &serde_json::to_vec(&marker)?)
    }

    pub fn set_needs_community(&self, user_id: &str) -> Result<()> {
        let key = format!("needs_community:{}", user_id);
        let ts = chrono::Utc::now().timestamp().to_string();
        self.system_kv().put(key.as_bytes(), ts.as_bytes())
    }

    pub fn get_pending_reflections(&self) -> Result<Vec<String>> {
        Ok(self
            .get_pending_reflection_markers()?
            .into_iter()
            .map(|(user_id, _)| user_id)
            .collect())
    }

    pub fn get_pending_reflection_markers(&self) -> Result<Vec<(String, ReflectionMarker)>> {
        let pairs = self.system_kv().scan(b"needs_reflect:")?;
        let mut markers = Vec::new();
        for (key, value) in pairs {
            let key_str = String::from_utf8(key)?;
            if let Some(uid) = key_str.strip_prefix("needs_reflect:") {
                let marker = serde_json::from_slice::<ReflectionMarker>(&value).unwrap_or_default();
                markers.push((uid.to_string(), marker));
            }
        }
        Ok(markers)
    }

    pub fn clear_reflection_marker(&self, user_id: &str) -> Result<()> {
        let key = format!("needs_reflect:{}", user_id);
        self.system_kv().delete(key.as_bytes())
    }

    pub fn get_pending_communities(&self) -> Result<Vec<String>> {
        let pairs = self.system_kv().scan(b"needs_community:")?;
        let mut user_ids = Vec::new();
        for (key, _) in pairs {
            let key_str = String::from_utf8(key)?;
            if let Some(uid) = key_str.strip_prefix("needs_community:") {
                user_ids.push(uid.to_string());
            }
        }
        Ok(user_ids)
    }

    pub fn clear_community_marker(&self, user_id: &str) -> Result<()> {
        let key = format!("needs_community:{}", user_id);
        self.system_kv().delete(key.as_bytes())
    }

    // ── Reflection ──────────────────────────────────────────────────

    pub(crate) async fn populate_missing_embeddings(&self, units: &mut [MemoryUnit]) {
        let Some(client) = self.arbitrator.get_llm_client() else {
            return;
        };

        let mut texts = Vec::new();
        let mut indices = Vec::new();
        for (index, unit) in units.iter().enumerate() {
            if unit.embedding.is_none() && !unit.content.trim().is_empty() {
                indices.push(index);
                texts.push(unit.content.clone());
            }
        }

        if texts.is_empty() {
            return;
        }

        let embeddings = match client.embed_batch(texts.clone()).await {
            Ok(response) if response.data.len() == indices.len() => response.data,
            _ => {
                let mut fallback = Vec::with_capacity(texts.len());
                for text in texts {
                    fallback.push(
                        client
                            .embed(&text)
                            .await
                            .map(|response| response.data)
                            .unwrap_or_default(),
                    );
                }
                fallback
            }
        };

        for (index, embedding) in indices.into_iter().zip(embeddings.into_iter()) {
            if !embedding.is_empty() {
                units[index].embedding = Some(embedding);
            }
        }
    }

    pub(crate) async fn reflect_on_units(
        &self,
        user_id: &str,
        stream_id: uuid::Uuid,
        source_units: Vec<MemoryUnit>,
    ) -> Result<usize> {
        if source_units.is_empty() {
            return Ok(0);
        }

        let topic_units = self
            .arbitrator
            .extract_topics(user_id, stream_id, source_units)
            .await?;

        if topic_units.is_empty() {
            return Ok(0);
        }

        let topic_count = topic_units.len();
        let jobs = topic_units
            .into_iter()
            .map(|topic| {
                let post_publish_edges = topic
                    .references
                    .iter()
                    .map(|source_id| {
                        GraphEdge::new(
                            topic.user_id.clone(),
                            topic.id,
                            *source_id,
                            RelationType::DerivedFrom,
                            1.0,
                        )
                    })
                    .collect::<Vec<_>>();
                PendingMaterializationJob::new(topic, post_publish_edges, None)
            })
            .collect::<Vec<_>>();
        self.enqueue_materialization_jobs(jobs)?;

        Ok(topic_count)
    }

    pub(crate) async fn fetch_l1_units_for_reflection_batch(
        &self,
        user_id: &str,
        min_transaction_time_micros: Option<i64>,
        min_event_id: Option<&str>,
        max_units: usize,
        max_tokens: usize,
    ) -> Result<(Vec<MemoryUnit>, usize, Option<(i64, String)>)> {
        if max_units == 0 {
            return Ok((Vec::new(), 0, None));
        }

        let store = self.kv_store.clone();
        let l1_index_prefix = format!("l1_idx:{}:", user_id).into_bytes();
        let strip_prefix = format!("l1_idx:{}:", user_id);
        let index_pairs = tokio::task::spawn_blocking({
            let store = store.clone();
            move || store.scan(&l1_index_prefix)
        })
        .await??;

        let cursor = min_transaction_time_micros.zip(min_event_id.map(str::to_string));
        if index_pairs.is_empty() {
            let prefix = format!("u:{}:unit:", user_id).into_bytes();
            let pairs = tokio::task::spawn_blocking({
                let store = store.clone();
                move || store.scan(&prefix)
            })
            .await??;

            let mut ordered_units: Vec<(String, i64, MemoryUnit)> = pairs
                .into_iter()
                .filter_map(|(_, value)| serde_json::from_slice::<MemoryUnit>(&value).ok())
                .filter(|unit| unit.level == 1 && Self::is_local_domain(&unit.domain))
                .filter_map(|unit| {
                    let id = unit.id.to_string();
                    let ts = unit.transaction_time.timestamp_micros();
                    let include = match &cursor {
                        Some((cursor_ts, cursor_id)) => {
                            ts > *cursor_ts || (ts == *cursor_ts && id >= *cursor_id)
                        }
                        None => true,
                    };
                    include.then_some((id, ts, unit))
                })
                .collect();
            ordered_units.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
            return Self::pack_reflection_batch_units(ordered_units, max_units, max_tokens);
        }

        let mut id_ts: Vec<(String, i64)> = index_pairs
            .into_iter()
            .filter_map(|(key, value)| {
                let key_str = String::from_utf8(key).ok()?;
                let id = key_str.strip_prefix(&strip_prefix)?.to_string();
                let ts = i64::from_le_bytes(value.as_slice().try_into().ok()?);
                let include = match &cursor {
                    Some((cursor_ts, cursor_id)) => {
                        ts > *cursor_ts || (ts == *cursor_ts && id >= *cursor_id)
                    }
                    None => true,
                };
                include.then_some((id, ts))
            })
            .collect();

        if id_ts.is_empty() {
            return Ok((Vec::new(), 0, None));
        }

        id_ts.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        if id_ts.len() > max_units.saturating_add(1) {
            id_ts.truncate(max_units.saturating_add(1));
        }

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

        let ordered_units: Vec<(String, i64, MemoryUnit)> = id_ts
            .into_iter()
            .zip(values.into_iter())
            .filter_map(|((id, ts), value)| {
                let unit =
                    value.and_then(|bytes| serde_json::from_slice::<MemoryUnit>(&bytes).ok())?;
                (unit.level == 1 && Self::is_local_domain(&unit.domain)).then_some((id, ts, unit))
            })
            .collect();
        Self::pack_reflection_batch_units(ordered_units, max_units, max_tokens)
    }

    pub(crate) fn pack_reflection_batch_units(
        mut ordered_units: Vec<(String, i64, MemoryUnit)>,
        max_units: usize,
        max_tokens: usize,
    ) -> Result<(Vec<MemoryUnit>, usize, Option<(i64, String)>)> {
        ordered_units.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

        let mut batch = Vec::new();
        let mut consumed_tokens = 0usize;
        let mut next_cursor = None;

        for (id, ts, unit) in ordered_units {
            let unit_tokens = count_tokens(&unit.content).max(1);
            let would_exceed_tokens = !batch.is_empty()
                && consumed_tokens.saturating_add(unit_tokens) > max_tokens.max(1);
            let would_exceed_units = batch.len() >= max_units;

            if would_exceed_tokens || would_exceed_units {
                next_cursor = Some((ts, id));
                break;
            }

            consumed_tokens = consumed_tokens.saturating_add(unit_tokens);
            batch.push(unit);
        }

        Ok((batch, consumed_tokens, next_cursor))
    }

    /// Prospective Reflection: Summarize recent L1 memories into L2 Topic memories.
    pub async fn reflect_on_session(&self, user_id: &str, stream_id: uuid::Uuid) -> Result<()> {
        let recent_l1 = self.fetch_recent_l1_units(user_id, 20).await?;
        let session_units: Vec<MemoryUnit> = recent_l1
            .into_iter()
            .filter(|u| u.stream_id == stream_id)
            .collect();

        self.reflect_on_units(user_id, stream_id, session_units)
            .await
            .map(|_| ())
    }

    /// User-window reflection: summarize a bounded recent L1 window, even across streams.
    pub async fn reflect_on_user_window(&self, user_id: &str, limit: usize) -> Result<usize> {
        self.reflect_on_user_window_since(user_id, None, limit)
            .await
    }

    /// User-window reflection with an optional lower transaction-time bound.
    pub async fn reflect_on_user_window_since(
        &self,
        user_id: &str,
        min_transaction_time_micros: Option<i64>,
        limit: usize,
    ) -> Result<usize> {
        let recent_l1 = match min_transaction_time_micros.filter(|ts| *ts > 0) {
            Some(min_ts) => {
                self.fetch_recent_l1_units_since(user_id, min_ts, limit.max(1))
                    .await?
            }
            None => self.fetch_recent_l1_units(user_id, limit.max(1)).await?,
        };
        let Some(stream_id) = recent_l1
            .iter()
            .max_by_key(|unit| unit.transaction_time)
            .map(|unit| unit.stream_id)
        else {
            return Ok(0);
        };

        self.reflect_on_units(user_id, stream_id, recent_l1).await
    }

    pub async fn reflect_on_user_window_batch(
        &self,
        user_id: &str,
        min_transaction_time_micros: Option<i64>,
        min_event_id: Option<&str>,
        max_units: usize,
        max_tokens: usize,
    ) -> Result<ReflectionBatchOutcome> {
        let (source_units, consumed_tokens, next_cursor) = self
            .fetch_l1_units_for_reflection_batch(
                user_id,
                min_transaction_time_micros,
                min_event_id,
                max_units.max(1),
                max_tokens.max(1),
            )
            .await?;

        let consumed_units = source_units.len();
        let Some(stream_id) = source_units
            .iter()
            .max_by_key(|unit| unit.transaction_time)
            .map(|unit| unit.stream_id)
        else {
            return Ok(ReflectionBatchOutcome::default());
        };

        let created_topics = self
            .reflect_on_units(user_id, stream_id, source_units)
            .await?;
        Ok(ReflectionBatchOutcome {
            created_topics,
            consumed_units,
            consumed_tokens,
            next_first_event_tx_micros: next_cursor.as_ref().map(|(ts, _)| *ts),
            next_first_event_id: next_cursor.map(|(_, id)| id),
        })
    }

    /// Retrospective Reflection: Apply feedback to the reranker and reinforce graph associations.
    pub async fn apply_reranker_feedback(
        &self,
        user_id: &str,
        cited_ids: Vec<String>,
        retrieved_ids: Vec<String>,
    ) -> Result<()> {
        self.reranker
            .apply_feedback(&self.kv_store, cited_ids.clone(), retrieved_ids)
            .await?;

        if cited_ids.len() >= 2 {
            self.reinforce_associations(user_id, cited_ids).await?;
        }

        Ok(())
    }

    /// Internal method to increase edge weights between memories that were useful together.
    pub(crate) async fn reinforce_associations(&self, user_id: &str, cited_ids: Vec<String>) -> Result<()> {
        let uid = user_id.to_string();

        let uuids: Vec<Uuid> = cited_ids
            .iter()
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect();

        if uuids.len() < 2 {
            return Ok(());
        }

        for i in 0..uuids.len() {
            for j in (i + 1)..uuids.len() {
                let id_a = uuids[i];
                let id_b = uuids[j];

                self.graph.reinforce_edge(&uid, id_a, id_b, 0.1).await?;
                self.graph.reinforce_edge(&uid, id_b, id_a, 0.1).await?;
            }
        }

        Ok(())
    }

}
