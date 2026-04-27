use super::helpers::validate_id;
use anyhow::Result;
use memorose_common::Event;

impl super::MemoroseEngine {
    pub async fn ingest_event(&self, event: Event) -> Result<()> {
        self.ingest_event_directly(event).await
    }

    pub async fn ingest_event_directly(&self, event: Event) -> Result<()> {
        self.ingest_events_directly(vec![event]).await
    }

    pub(crate) fn validate_event_not_empty(event: &Event) -> Result<()> {
        let is_empty = match &event.content {
            memorose_common::EventContent::Text(text) => text.trim().is_empty(),
            memorose_common::EventContent::Image(url) => url.trim().is_empty(),
            memorose_common::EventContent::Audio(url) => url.trim().is_empty(),
            memorose_common::EventContent::Video(url) => url.trim().is_empty(),
            memorose_common::EventContent::Json(val) => {
                val.is_null() || (val.is_string() && val.as_str().unwrap_or("").trim().is_empty())
            }
        };

        if is_empty {
            return Err(anyhow::anyhow!(
                "Rejected empty event: event_id={}, user_id={}, content type={:?}",
                event.id,
                event.user_id,
                std::mem::discriminant(&event.content)
            ));
        }
        Ok(())
    }

    pub async fn ingest_events_directly(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut batch = rocksdb::WriteBatch::default();
        for event in &events {
            Self::validate_event_not_empty(event)?;
            validate_id(&event.user_id)?;
            if let Some(ref org_id) = event.org_id {
                validate_id(org_id)?;
            }
            if let Some(ref agent_id) = event.agent_id {
                validate_id(agent_id)?;
            }

            let event_id = event.id.to_string();
            let user_id = event.user_id.clone();
            let key = format!("u:{}:event:{}", user_id, event_id);
            let val = serde_json::to_vec(event)?;
            batch.put(key.as_bytes(), &val);

            let pending_key = format!("pending:{}", event_id);
            let pending_val = serde_json::to_vec(&serde_json::json!({
                "user_id": user_id
            }))?;
            batch.put(pending_key.as_bytes(), &pending_val);

            let active_key = format!("active_user:{}", event.user_id);
            batch.put(active_key.as_bytes(), []);
        }

        self.kv_store.write_batch(batch)?;
        Ok(())
    }

    pub async fn fetch_pending_events(&self) -> Result<Vec<Event>> {
        self.fetch_pending_events_limited(usize::MAX).await
    }

    /// Count pending events without deserialising their bodies -- much cheaper than
    /// `fetch_pending_events().len()` for systems with many pending events.
    pub async fn count_pending_events(&self) -> Result<usize> {
        let skv = self.system_kv();
        tokio::task::spawn_blocking(move || skv.count_prefix(b"pending:")).await?
    }

    pub async fn fetch_pending_events_limited(&self, limit: usize) -> Result<Vec<Event>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let skv = self.system_kv();
        // Over-fetch by 4x to account for invalid entries that get skipped during parsing.
        let scan_limit = limit.saturating_mul(4).max(20);
        let pending_pairs =
            tokio::task::spawn_blocking(move || skv.scan_limited(b"pending:", scan_limit))
                .await??;

        let mut events = Vec::new();
        let mut invalid_pending_entries = Vec::new();
        for (key, val) in pending_pairs {
            if events.len() >= limit {
                break;
            }

            let key_str = String::from_utf8(key)?;
            let parts: Vec<&str> = key_str.split(':').collect();
            if parts.len() == 2 {
                let event_id = parts[1];
                // Parse user_id from the pending value.
                let user_id = if !val.is_empty() {
                    if let Ok(info) = serde_json::from_slice::<serde_json::Value>(&val) {
                        match info["user_id"].as_str() {
                            Some(user_id) if !user_id.is_empty() => user_id.to_string(),
                            _ => {
                                invalid_pending_entries.push((
                                    event_id.to_string(),
                                    "Pending metadata missing user_id".to_string(),
                                ));
                                continue;
                            }
                        }
                    } else {
                        invalid_pending_entries.push((
                            event_id.to_string(),
                            "Malformed pending metadata".to_string(),
                        ));
                        continue;
                    }
                } else {
                    invalid_pending_entries.push((
                        event_id.to_string(),
                        "Pending metadata missing user_id".to_string(),
                    ));
                    continue;
                };
                if let Some(event) = self.get_event(&user_id, event_id).await? {
                    events.push(event);
                } else {
                    invalid_pending_entries.push((
                        event_id.to_string(),
                        format!("Pending entry missing source event for user {}", user_id),
                    ));
                }
            }
        }

        for (event_id, reason) in invalid_pending_entries {
            if let Err(err) = self.mark_event_failed(&event_id, &reason).await {
                tracing::warn!(
                    "Failed to move invalid pending entry {} to failed queue: {:?}",
                    event_id,
                    err
                );
            }
        }

        events.sort_by(|a, b| a.transaction_time.cmp(&b.transaction_time));
        Ok(events)
    }

    pub async fn mark_event_processed(&self, id: &str) -> Result<()> {
        let key = format!("pending:{}", id);
        self.system_kv().delete(key.as_bytes())?;
        let retry_key = format!("retry_count:{}", id);
        self.system_kv().delete(retry_key.as_bytes())?;
        Ok(())
    }

    pub async fn get_retry_count(&self, id: &str) -> Result<u32> {
        let key = format!("retry_count:{}", id);
        match self.system_kv().get(key.as_bytes())? {
            Some(bytes) => {
                let count = u32::from_le_bytes(bytes.try_into().unwrap_or([0, 0, 0, 0]));
                Ok(count)
            }
            None => Ok(0),
        }
    }

    pub async fn increment_retry_count(&self, id: &str) -> Result<u32> {
        let key = format!("retry_count:{}", id);
        let current = self.get_retry_count(id).await?;
        let new_count = current + 1;
        self.system_kv()
            .put(key.as_bytes(), &new_count.to_le_bytes())?;
        Ok(new_count)
    }

    pub async fn increment_retry_count_if_pending(&self, id: &str) -> Result<Option<u32>> {
        let pending_key = format!("pending:{}", id);
        if self.system_kv().get(pending_key.as_bytes())?.is_none() {
            return Ok(None);
        }
        let count = self.increment_retry_count(id).await?;
        Ok(Some(count))
    }

    pub async fn mark_event_failed(&self, id: &str, error: &str) -> Result<()> {
        let pending_key = format!("pending:{}", id);
        self.system_kv().delete(pending_key.as_bytes())?;

        let retry_count = self.get_retry_count(id).await?;
        let failed_key = format!("failed:{}", id);
        let failed_info = serde_json::json!({
            "error": error,
            "failed_at": chrono::Utc::now().to_rfc3339(),
            "retry_count": retry_count
        });
        self.system_kv()
            .put(failed_key.as_bytes(), &serde_json::to_vec(&failed_info)?)?;

        let retry_key = format!("retry_count:{}", id);
        self.system_kv().delete(retry_key.as_bytes())?;

        Ok(())
    }

    pub(crate) fn get_event_raw(&self, user_id: &str, id: &str) -> Result<Option<Event>> {
        let key = format!("u:{}:event:{}", user_id, id);
        let val = self.kv_store.get(key.as_bytes())?;
        match val {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    pub async fn get_event(&self, user_id: &str, id: &str) -> Result<Option<Event>> {
        if self.is_event_forgotten(user_id, id)? {
            return Ok(None);
        }
        self.get_event_raw(user_id, id)
    }

    pub async fn delete_event(&self, user_id: &str, id: &str) -> Result<()> {
        let key = format!("u:{}:event:{}", user_id, id);
        let pending_key = format!("pending:{}", id);
        let retry_key = format!("retry_count:{}", id);
        let failed_key = format!("failed:{}", id);
        let forgotten_key = Self::forgotten_event_key(user_id, id);

        let mut batch = rocksdb::WriteBatch::default();
        batch.delete(key.as_bytes());
        batch.delete(pending_key.as_bytes());
        batch.delete(retry_key.as_bytes());
        batch.delete(failed_key.as_bytes());
        batch.delete(forgotten_key.as_bytes());

        self.kv_store.write_batch(batch)?;
        Ok(())
    }
}
