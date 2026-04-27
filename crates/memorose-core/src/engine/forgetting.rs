use super::types::{PendingMaterializationJob, PendingMaterializationJobStatus};
use anyhow::{anyhow, Result};
use chrono::Utc;
use memorose_common::{ForgettingTombstone, MaterializationState, MemoryDomain, MemoryUnit};
use std::collections::HashSet;
use uuid::Uuid;

const PRUNE_SCAN_BATCH_SIZE: usize = 512;
const PRUNE_DELETE_BATCH_SIZE: usize = 128;

impl super::MemoroseEngine {
    // ── Forgetting ──────────────────────────────────────────────────

    pub(crate) fn forgotten_memory_unit_key(user_id: &str, id: Uuid) -> String {
        format!("forget:unit:{}:{}", user_id, id)
    }

    pub(crate) fn forgotten_event_key(user_id: &str, id: &str) -> String {
        format!("forget:event:{}:{}", user_id, id)
    }

    pub(crate) fn materialization_job_key(job_id: Uuid) -> String {
        format!("materialize:job:{}", job_id)
    }

    pub(crate) fn materialization_due_key(next_attempt_at_micros: i64, job_id: Uuid) -> String {
        format!(
            "materialize:due:{:020}:{}",
            next_attempt_at_micros.max(0),
            job_id
        )
    }

    pub(crate) fn materialization_due_prefix() -> &'static [u8] {
        b"materialize:due:"
    }

    pub(crate) fn materialization_post_publish_key(unit_id: Uuid) -> String {
        format!("materialize:hooks:{}", unit_id)
    }

    pub(crate) fn requires_materialized_embedding(unit: &MemoryUnit) -> bool {
        unit.visible
            && unit.materialization_state == MaterializationState::Published
            && (1..=3).contains(&unit.level)
            && Self::is_local_domain(&unit.domain)
    }

    pub(crate) fn validate_materialized_units(units: &[MemoryUnit]) -> Result<()> {
        if cfg!(test) {
            return Ok(());
        }

        for unit in units {
            if Self::requires_materialized_embedding(unit)
                && unit
                    .embedding
                    .as_ref()
                    .map(|embedding| embedding.is_empty())
                    != Some(false)
            {
                return Err(anyhow!(
                    "memory unit {} (level {}) cannot be published without an embedding",
                    unit.id,
                    unit.level
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn materialization_post_publish_applied(&self, unit_id: Uuid) -> Result<bool> {
        Ok(self
            .system_kv()
            .get(Self::materialization_post_publish_key(unit_id).as_bytes())?
            .is_some())
    }

    pub(crate) fn mark_materialization_post_publish_applied(&self, unit_id: Uuid) -> Result<()> {
        let now = chrono::Utc::now().timestamp_micros().to_string();
        self.system_kv().put(
            Self::materialization_post_publish_key(unit_id).as_bytes(),
            now.as_bytes(),
        )
    }

    pub(crate) fn save_materialization_job(&self, job: &PendingMaterializationJob) -> Result<()> {
        let system_kv = self.system_kv();
        let job_key = Self::materialization_job_key(job.job_id);
        let due_key = Self::materialization_due_key(job.next_attempt_at_micros, job.job_id);
        system_kv.put(job_key.as_bytes(), &serde_json::to_vec(job)?)?;
        if job.status != PendingMaterializationJobStatus::Failed {
            system_kv.put(due_key.as_bytes(), &[])?;
        }
        Ok(())
    }

    pub fn enqueue_materialization_jobs(&self, jobs: Vec<PendingMaterializationJob>) -> Result<()> {
        let system_kv = self.system_kv();
        for job in jobs {
            let job_key = Self::materialization_job_key(job.job_id);
            let due_key = Self::materialization_due_key(job.next_attempt_at_micros, job.job_id);
            system_kv.put(job_key.as_bytes(), &serde_json::to_vec(&job)?)?;
            system_kv.put(due_key.as_bytes(), &[])?;
        }
        Ok(())
    }

    pub fn fetch_due_materialization_jobs(
        &self,
        limit: usize,
    ) -> Result<Vec<PendingMaterializationJob>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let system_kv = self.system_kv();
        let now = Utc::now().timestamp_micros().max(0);
        let due_entries = system_kv.scan(Self::materialization_due_prefix())?;
        let mut jobs = Vec::new();

        for (key, _) in due_entries {
            if jobs.len() >= limit {
                break;
            }

            let Ok(key_str) = String::from_utf8(key) else {
                continue;
            };
            let mut parts = key_str.split(':');
            let Some("materialize") = parts.next() else {
                continue;
            };
            let Some("due") = parts.next() else {
                continue;
            };
            let Some(due_raw) = parts.next() else {
                continue;
            };
            let Some(job_id_raw) = parts.next() else {
                continue;
            };

            let Ok(due_at) = due_raw.parse::<i64>() else {
                continue;
            };
            if due_at > now {
                break;
            }

            let Ok(job_id) = Uuid::parse_str(job_id_raw) else {
                continue;
            };
            let job_key = Self::materialization_job_key(job_id);
            let Some(bytes) = system_kv.get(job_key.as_bytes())? else {
                system_kv.delete(key_str.as_bytes()).ok();
                continue;
            };
            let Ok(job) = serde_json::from_slice::<PendingMaterializationJob>(&bytes) else {
                continue;
            };
            if job.status == PendingMaterializationJobStatus::Failed {
                continue;
            }
            jobs.push(job);
        }

        Ok(jobs)
    }

    pub fn reschedule_materialization_job(
        &self,
        job: &mut PendingMaterializationJob,
        error: impl ToString,
    ) -> Result<()> {
        let system_kv = self.system_kv();
        let old_due_key = Self::materialization_due_key(job.next_attempt_at_micros, job.job_id);
        system_kv.delete(old_due_key.as_bytes()).ok();

        job.attempts = job.attempts.saturating_add(1);
        job.status = PendingMaterializationJobStatus::RetryScheduled;
        job.last_error = Some(error.to_string());
        job.updated_at = Utc::now();

        let backoff_secs = match job.attempts {
            0 | 1 => 10,
            2 => 30,
            3 => 120,
            4 => 600,
            _ => 1800,
        };
        job.next_attempt_at_micros =
            (Utc::now() + chrono::Duration::seconds(backoff_secs)).timestamp_micros();

        self.save_materialization_job(job)
    }

    pub fn fail_materialization_job(
        &self,
        job: &mut PendingMaterializationJob,
        error: impl ToString,
    ) -> Result<()> {
        let system_kv = self.system_kv();
        let old_due_key = Self::materialization_due_key(job.next_attempt_at_micros, job.job_id);
        system_kv.delete(old_due_key.as_bytes()).ok();

        job.attempts = job.attempts.saturating_add(1);
        job.status = PendingMaterializationJobStatus::Failed;
        job.last_error = Some(error.to_string());
        job.updated_at = Utc::now();

        self.save_materialization_job(job)
    }

    pub fn delete_materialization_job(&self, job: &PendingMaterializationJob) -> Result<()> {
        let system_kv = self.system_kv();
        let job_key = Self::materialization_job_key(job.job_id);
        let due_key = Self::materialization_due_key(job.next_attempt_at_micros, job.job_id);
        system_kv.delete(job_key.as_bytes()).ok();
        system_kv.delete(due_key.as_bytes()).ok();
        Ok(())
    }

    pub fn mark_memory_unit_forgotten(
        &self,
        user_id: &str,
        unit_id: Uuid,
        tombstone: &ForgettingTombstone,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(tombstone)?;
        self.system_kv().put(
            Self::forgotten_memory_unit_key(user_id, unit_id).as_bytes(),
            &bytes,
        )
    }

    pub fn mark_event_forgotten(
        &self,
        user_id: &str,
        event_id: &str,
        tombstone: &ForgettingTombstone,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(tombstone)?;
        self.system_kv().put(
            Self::forgotten_event_key(user_id, event_id).as_bytes(),
            &bytes,
        )
    }

    pub fn is_memory_unit_forgotten(&self, user_id: &str, unit_id: Uuid) -> Result<bool> {
        Ok(self
            .system_kv()
            .get(Self::forgotten_memory_unit_key(user_id, unit_id).as_bytes())?
            .is_some())
    }

    pub fn is_event_forgotten(&self, user_id: &str, event_id: &str) -> Result<bool> {
        Ok(self
            .system_kv()
            .get(Self::forgotten_event_key(user_id, event_id).as_bytes())?
            .is_some())
    }

    pub fn clear_memory_unit_forgotten(&self, user_id: &str, unit_id: Uuid) -> Result<()> {
        self.system_kv()
            .delete(Self::forgotten_memory_unit_key(user_id, unit_id).as_bytes())?;
        Ok(())
    }

    pub fn clear_event_forgotten(&self, user_id: &str, event_id: &str) -> Result<()> {
        self.system_kv()
            .delete(Self::forgotten_event_key(user_id, event_id).as_bytes())?;
        Ok(())
    }

    pub fn is_visible_memory_unit(&self, unit: &MemoryUnit) -> Result<bool> {
        if unit.domain == MemoryDomain::Organization {
            return Ok(true);
        }

        Ok(unit.visible
            && unit.materialization_state == MaterializationState::Published
            && !self.is_memory_unit_forgotten(&unit.user_id, unit.id)?)
    }

    pub async fn delete_memory_unit_hard(&self, user_id: &str, unit_id: Uuid) -> Result<()> {
        let unit = self.get_memory_unit_raw(user_id, unit_id)?;
        let unit_key = format!("u:{}:unit:{}", user_id, unit_id).into_bytes();
        self.delete_memory_unit_storage_by_key(unit_key, unit_id)
            .await?;
        let _ = self.graph.delete_edges_for_node(user_id, unit_id).await?;
        self.invalidate_query_cache(user_id).await;
        self.clear_memory_unit_forgotten(user_id, unit_id)?;

        if let Some(unit) = unit {
            if unit.level == 1 {
                let key = format!("l1_count:{}", user_id);
                self.system_kv().delete(key.as_bytes())?;
            }
        }

        Ok(())
    }

    /// Apply importance decay to memories for a specific user.
    /// Updates only the KV store — does NOT re-index into LanceDB/Tantivy
    /// or trigger auto-linking/LLM calls.
    pub async fn decay_importance(&self, user_id: &str, factor: f32) -> Result<()> {
        let prefix = format!("u:{}:unit:", user_id);
        let kv = self.kv_store.clone();
        let prefix_bytes = prefix.into_bytes();

        let pairs = tokio::task::spawn_blocking(move || kv.scan(&prefix_bytes)).await??;

        let kv = self.kv_store.clone();
        tokio::task::spawn_blocking(move || {
            for (key, val) in pairs {
                if let Ok(mut unit) = serde_json::from_slice::<MemoryUnit>(&val) {
                    unit.importance *= factor;
                    if let Ok(new_val) = serde_json::to_vec(&unit) {
                        kv.put(&key, &new_val)?;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        Ok(())
    }

    /// Remove memories with importance below the threshold for a specific user.
    /// L1 units referenced by visible L2/L3 units are retained for provenance.
    /// Pruned units are deleted from KV, LanceDB vector store, and Tantivy text index.
    pub async fn prune_memories(&self, user_id: &str, threshold: f32) -> Result<usize> {
        let kv = self.kv_store.clone();
        let prefix_bytes = format!("u:{}:unit:", user_id).into_bytes();

        let mut l2_referenced_l1_ids = HashSet::new();
        let mut after: Option<Vec<u8>> = None;
        loop {
            let page = tokio::task::spawn_blocking({
                let kv = kv.clone();
                let prefix = prefix_bytes.clone();
                let after = after.clone();
                move || kv.scan_prefix_after(&prefix, after.as_deref(), PRUNE_SCAN_BATCH_SIZE)
            })
            .await??;
            if page.is_empty() {
                break;
            }

            for (_, val) in &page {
                if let Ok(unit) = serde_json::from_slice::<MemoryUnit>(val) {
                    if unit.level >= 2
                        && Self::is_local_domain(&unit.domain)
                        && self.is_visible_memory_unit(&unit).unwrap_or(false)
                    {
                        l2_referenced_l1_ids.extend(unit.references.iter().copied());
                    }
                }
            }
            after = page.last().map(|(key, _)| key.clone());
        }

        let mut total_pruned = 0;
        let mut to_prune: Vec<(Vec<u8>, MemoryUnit)> = Vec::with_capacity(PRUNE_DELETE_BATCH_SIZE);
        let mut after: Option<Vec<u8>> = None;
        loop {
            let page = tokio::task::spawn_blocking({
                let kv = kv.clone();
                let prefix = prefix_bytes.clone();
                let after = after.clone();
                move || kv.scan_prefix_after(&prefix, after.as_deref(), PRUNE_SCAN_BATCH_SIZE)
            })
            .await??;
            if page.is_empty() {
                break;
            }

            for (key, val) in &page {
                let Ok(unit) = serde_json::from_slice::<MemoryUnit>(val) else {
                    continue;
                };
                if unit.level == 1 && l2_referenced_l1_ids.contains(&unit.id) {
                    continue;
                }
                if unit.importance < threshold {
                    to_prune.push((key.clone(), unit));
                    if to_prune.len() >= PRUNE_DELETE_BATCH_SIZE {
                        total_pruned += self.delete_pruned_units(user_id, &mut to_prune).await?;
                    }
                }
            }
            after = page.last().map(|(key, _)| key.clone());
        }

        total_pruned += self.delete_pruned_units(user_id, &mut to_prune).await?;
        Ok(total_pruned)
    }

    async fn delete_pruned_units(
        &self,
        user_id: &str,
        to_prune: &mut Vec<(Vec<u8>, MemoryUnit)>,
    ) -> Result<usize> {
        if to_prune.is_empty() {
            return Ok(0);
        }
        let batch = std::mem::take(to_prune);
        let count = batch.len();

        // 1. Delete from KV + L1 secondary index
        let kv_clone = self.kv_store.clone();
        let keys_and_levels: Vec<(Vec<u8>, String, u8, String)> = batch
            .iter()
            .map(|(k, u)| {
                (
                    k.clone(),
                    u.id.to_string(),
                    u.level,
                    Self::materialization_post_publish_key(u.id),
                )
            })
            .collect();
        let user_id_owned = user_id.to_string();
        tokio::task::spawn_blocking(move || {
            for (key, id, level, hooks_key) in &keys_and_levels {
                kv_clone.delete(key)?;
                if *level == 1 {
                    let l1_key = format!("l1_idx:{}:{}", user_id_owned, id);
                    kv_clone.delete(l1_key.as_bytes()).ok();
                }
                kv_clone.delete(hooks_key.as_bytes()).ok();
            }
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        // 2. Delete from LanceDB vector store
        if let Some(vector) = &self.vector {
            for (_, unit) in &batch {
                if let Err(e) = vector.delete_by_id("memories", &unit.id.to_string()).await {
                    tracing::warn!(
                        "Failed to delete unit {} from vector store during pruning: {:?}",
                        unit.id,
                        e
                    );
                }
            }
        }

        // 3. Delete from Tantivy text index
        let index = self.index.clone();
        let ids: Vec<String> = batch.iter().map(|(_, u)| u.id.to_string()).collect();
        tokio::task::spawn_blocking(move || {
            for id in &ids {
                index.delete_unit(id)?;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        Ok(count)
    }
}
