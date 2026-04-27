use crate::storage::kv::KvStore;
use crate::storage::vector::{VectorStore, VECTOR_SCHEMA_VERSION};
use anyhow::{anyhow, Context, Result};
use memorose_common::MemoryUnit;
use serde::Serialize;
use std::path::{Path, PathBuf};

const MEMORY_SCAN_PREFIX: &[u8] = b"u:";
const REPAIR_SCAN_BATCH_SIZE: usize = 512;

#[derive(Debug, Clone, Serialize)]
pub struct VectorStatusReport {
    pub data_dir: PathBuf,
    pub rocksdb_exists: bool,
    pub lancedb_exists: bool,
    pub lancedb_size_bytes: u64,
    pub memory_units_total: usize,
    pub memory_units_with_embeddings: usize,
    pub decode_errors: usize,
    pub vector_rows: Option<usize>,
    pub expected_vector_schema_version: u32,
    pub vector_schema_version: Option<u32>,
    pub vector_schema_status: Option<String>,
    pub vector_schema_columns: Option<Vec<String>>,
    pub max_index_size_gb: Option<u64>,
    pub lancedb_exceeds_max_index_size: bool,
    pub vector_open_error: Option<String>,
    pub recommendation: String,
}

#[derive(Debug, Clone)]
pub struct VectorRebuildOptions {
    pub data_dir: PathBuf,
    pub embedding_dim: i32,
    pub batch_size: usize,
    pub force: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct VectorRebuildReport {
    pub data_dir: PathBuf,
    pub rebuilt_path: PathBuf,
    pub backup_path: Option<PathBuf>,
    pub scanned_units: usize,
    pub indexed_units: usize,
    pub skipped_without_embedding: usize,
    pub decode_errors: usize,
}

pub async fn vector_status(
    data_dir: impl AsRef<Path>,
    open_lancedb: bool,
) -> Result<VectorStatusReport> {
    vector_status_with_limits(data_dir, open_lancedb, None).await
}

pub async fn vector_status_with_limits(
    data_dir: impl AsRef<Path>,
    open_lancedb: bool,
    max_index_size_gb: Option<u64>,
) -> Result<VectorStatusReport> {
    let data_dir = data_dir.as_ref().to_path_buf();
    let rocksdb_path = data_dir.join("rocksdb");
    let lancedb_path = data_dir.join("lancedb");
    let rocksdb_exists = rocksdb_path.exists();
    let lancedb_exists = lancedb_path.exists();
    let lancedb_size_bytes = dir_size_bytes(&lancedb_path)?;

    let mut counts = MemoryScanCounts::default();
    if rocksdb_exists {
        let kv = KvStore::open(&rocksdb_path)?;
        counts = scan_memory_counts(&kv)?;
    }

    let mut vector_rows = None;
    let mut vector_schema_version = None;
    let mut vector_schema_status = None;
    let mut vector_schema_columns = None;
    let mut vector_open_error = None;
    if open_lancedb && lancedb_exists {
        let vector_uri = lancedb_path.to_string_lossy().to_string();
        match VectorStore::new(&vector_uri, 1).await {
            Ok(store) => {
                match store.count_rows("memories").await {
                    Ok(rows) => vector_rows = Some(rows),
                    Err(error) => vector_open_error = Some(error.to_string()),
                }
                match store.table_schema_status("memories").await {
                    Ok((columns, version, status)) => {
                        vector_schema_columns = Some(columns);
                        vector_schema_version = version;
                        vector_schema_status = Some(status);
                    }
                    Err(error) => vector_open_error = Some(error.to_string()),
                }
            }
            Err(error) => vector_open_error = Some(error.to_string()),
        }
    }

    let lancedb_exceeds_max_index_size = max_index_size_gb
        .map(|gb| lancedb_size_bytes > gb.saturating_mul(1024 * 1024 * 1024))
        .unwrap_or(false);

    let recommendation = if !rocksdb_exists {
        "rocksdb_missing".to_string()
    } else if !lancedb_exists {
        "run_vector_rebuild".to_string()
    } else if lancedb_exceeds_max_index_size {
        "run_vector_rebuild".to_string()
    } else if lancedb_size_bytes > 0 && counts.memory_units_with_embeddings == 0 {
        "inspect_lancedb_or_disable_vector".to_string()
    } else if vector_schema_status
        .as_deref()
        .is_some_and(|status| status != "current")
    {
        "run_vector_rebuild".to_string()
    } else if vector_open_error.is_some() {
        "run_vector_rebuild".to_string()
    } else {
        "ok".to_string()
    };

    Ok(VectorStatusReport {
        data_dir,
        rocksdb_exists,
        lancedb_exists,
        lancedb_size_bytes,
        memory_units_total: counts.memory_units_total,
        memory_units_with_embeddings: counts.memory_units_with_embeddings,
        decode_errors: counts.decode_errors,
        vector_rows,
        expected_vector_schema_version: VECTOR_SCHEMA_VERSION,
        vector_schema_version,
        vector_schema_status,
        vector_schema_columns,
        max_index_size_gb,
        lancedb_exceeds_max_index_size,
        vector_open_error,
        recommendation,
    })
}

pub async fn rebuild_vector_index(options: VectorRebuildOptions) -> Result<VectorRebuildReport> {
    let batch_size = options.batch_size.max(1);
    let data_dir = options.data_dir;
    let rocksdb_path = data_dir.join("rocksdb");
    if !rocksdb_path.exists() {
        return Err(anyhow!(
            "RocksDB directory does not exist: {}",
            rocksdb_path.display()
        ));
    }

    std::fs::create_dir_all(&data_dir)?;
    let lock_path = data_dir.join("lancedb.rebuild.lock");
    let _lock = RebuildLock::acquire(&lock_path, options.force)?;

    let stamp = timestamp_micros();
    let rebuilding_path = data_dir.join(format!("lancedb.rebuilding.{}", stamp));
    let final_path = data_dir.join("lancedb");
    let backup_path = if final_path.exists() {
        Some(data_dir.join(format!("lancedb.backup.{}", stamp)))
    } else {
        None
    };

    if rebuilding_path.exists() {
        return Err(anyhow!(
            "rebuild directory already exists: {}",
            rebuilding_path.display()
        ));
    }

    let kv = KvStore::open(&rocksdb_path)?;
    let vector_uri = rebuilding_path.to_string_lossy().to_string();
    let vector = VectorStore::new(&vector_uri, options.embedding_dim).await?;
    vector.ensure_table("memories").await?;

    let mut report = VectorRebuildReport {
        data_dir: data_dir.clone(),
        rebuilt_path: final_path.clone(),
        backup_path: backup_path.clone(),
        scanned_units: 0,
        indexed_units: 0,
        skipped_without_embedding: 0,
        decode_errors: 0,
    };

    let mut after: Option<Vec<u8>> = None;
    let mut vector_batch = Vec::with_capacity(batch_size);
    loop {
        let page =
            kv.scan_prefix_after(MEMORY_SCAN_PREFIX, after.as_deref(), REPAIR_SCAN_BATCH_SIZE)?;
        if page.is_empty() {
            break;
        }
        for (key, value) in &page {
            if !is_memory_unit_key(key) {
                continue;
            }
            report.scanned_units += 1;
            match serde_json::from_slice::<MemoryUnit>(value) {
                Ok(unit) if unit.embedding.is_some() => {
                    vector_batch.push(unit);
                    if vector_batch.len() >= batch_size {
                        report.indexed_units += vector_batch.len();
                        vector
                            .add("memories", std::mem::take(&mut vector_batch))
                            .await?;
                    }
                }
                Ok(_) => report.skipped_without_embedding += 1,
                Err(_) => report.decode_errors += 1,
            }
        }
        after = page.last().map(|(key, _)| key.clone());
    }

    if !vector_batch.is_empty() {
        report.indexed_units += vector_batch.len();
        vector.add("memories", vector_batch).await?;
    }
    drop(vector);

    if let Some(backup) = &backup_path {
        std::fs::rename(&final_path, backup).with_context(|| {
            format!(
                "failed to move old LanceDB {} to {}",
                final_path.display(),
                backup.display()
            )
        })?;
    }

    if let Err(error) = std::fs::rename(&rebuilding_path, &final_path) {
        if let Some(backup) = &backup_path {
            let _ = std::fs::rename(backup, &final_path);
        }
        return Err(error).with_context(|| {
            format!(
                "failed to move rebuilt LanceDB {} to {}",
                rebuilding_path.display(),
                final_path.display()
            )
        });
    }

    Ok(report)
}

#[derive(Default)]
struct MemoryScanCounts {
    memory_units_total: usize,
    memory_units_with_embeddings: usize,
    decode_errors: usize,
}

fn scan_memory_counts(kv: &KvStore) -> Result<MemoryScanCounts> {
    let mut counts = MemoryScanCounts::default();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let page =
            kv.scan_prefix_after(MEMORY_SCAN_PREFIX, after.as_deref(), REPAIR_SCAN_BATCH_SIZE)?;
        if page.is_empty() {
            break;
        }
        for (key, value) in &page {
            if !is_memory_unit_key(key) {
                continue;
            }
            counts.memory_units_total += 1;
            match serde_json::from_slice::<MemoryUnit>(value) {
                Ok(unit) if unit.embedding.is_some() => counts.memory_units_with_embeddings += 1,
                Ok(_) => {}
                Err(_) => counts.decode_errors += 1,
            }
        }
        after = page.last().map(|(key, _)| key.clone());
    }
    Ok(counts)
}

fn is_memory_unit_key(key: &[u8]) -> bool {
    key.starts_with(MEMORY_SCAN_PREFIX)
        && std::str::from_utf8(key)
            .map(|key| key.contains(":unit:"))
            .unwrap_or(false)
}

fn dir_size_bytes(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        return Ok(0);
    }

    let mut total = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        total += dir_size_bytes(&entry.path())?;
    }
    Ok(total)
}

fn timestamp_micros() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

struct RebuildLock {
    path: PathBuf,
}

impl RebuildLock {
    fn acquire(path: &Path, force: bool) -> Result<Self> {
        if force && path.exists() {
            std::fs::remove_file(path)?;
        }
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
        {
            Ok(_) => Ok(Self {
                path: path.to_path_buf(),
            }),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Err(anyhow!(
                "vector rebuild lock already exists: {}",
                path.display()
            )),
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for RebuildLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::KvStore;
    use memorose_common::{MemoryType, MemoryUnit};
    use tempfile::tempdir;
    use uuid::Uuid;

    fn test_unit(user_id: &str, content: &str, embedding: Option<Vec<f32>>) -> MemoryUnit {
        MemoryUnit::new(
            None,
            user_id.to_string(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            content.to_string(),
            embedding,
        )
    }

    fn put_unit(kv: &KvStore, unit: &MemoryUnit) -> anyhow::Result<()> {
        let key = format!("u:{}:unit:{}", unit.user_id, unit.id);
        kv.put(key.as_bytes(), &serde_json::to_vec(unit)?)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_vector_status_counts_rocksdb_units_without_opening_lancedb() -> anyhow::Result<()>
    {
        let temp = tempdir()?;
        let data_dir = temp.path();
        let kv = KvStore::open(data_dir.join("rocksdb"))?;
        put_unit(
            &kv,
            &test_unit("u1", "with embedding", Some(vec![1.0, 0.0, 0.0, 0.0])),
        )?;
        put_unit(&kv, &test_unit("u1", "without embedding", None))?;
        drop(kv);
        std::fs::create_dir(data_dir.join("lancedb"))?;

        let report = vector_status(data_dir, false).await?;

        assert!(report.lancedb_exists);
        assert_eq!(report.memory_units_total, 2);
        assert_eq!(report.memory_units_with_embeddings, 1);
        assert_eq!(report.vector_rows, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_vector_status_recommends_rebuild_when_size_exceeds_limit() -> anyhow::Result<()> {
        let temp = tempdir()?;
        let data_dir = temp.path();
        let kv = KvStore::open(data_dir.join("rocksdb"))?;
        put_unit(
            &kv,
            &test_unit("u1", "with embedding", Some(vec![1.0, 0.0, 0.0, 0.0])),
        )?;
        drop(kv);
        std::fs::create_dir(data_dir.join("lancedb"))?;
        std::fs::write(data_dir.join("lancedb").join("large-fragment"), b"large")?;

        let report = vector_status_with_limits(data_dir, false, Some(0)).await?;

        assert!(report.lancedb_exceeds_max_index_size);
        assert_eq!(report.recommendation, "run_vector_rebuild");
        Ok(())
    }

    #[tokio::test]
    async fn test_vector_rebuild_replaces_existing_lancedb_from_rocksdb() -> anyhow::Result<()> {
        let temp = tempdir()?;
        let data_dir = temp.path();
        let kv = KvStore::open(data_dir.join("rocksdb"))?;
        put_unit(
            &kv,
            &test_unit("u1", "first", Some(vec![1.0, 0.0, 0.0, 0.0])),
        )?;
        put_unit(
            &kv,
            &test_unit("u1", "second", Some(vec![0.0, 1.0, 0.0, 0.0])),
        )?;
        put_unit(&kv, &test_unit("u1", "skip", None))?;
        drop(kv);
        std::fs::create_dir(data_dir.join("lancedb"))?;
        std::fs::write(data_dir.join("lancedb").join("old-marker"), b"old")?;

        let report = rebuild_vector_index(VectorRebuildOptions {
            data_dir: data_dir.to_path_buf(),
            embedding_dim: 4,
            batch_size: 1,
            force: false,
        })
        .await?;

        assert_eq!(report.scanned_units, 3);
        assert_eq!(report.indexed_units, 2);
        assert_eq!(report.skipped_without_embedding, 1);
        assert!(report.backup_path.is_some());
        assert!(report
            .backup_path
            .as_ref()
            .unwrap()
            .join("old-marker")
            .exists());

        let status = vector_status(data_dir, true).await?;
        assert_eq!(status.vector_rows, Some(2));
        assert_eq!(status.vector_schema_version, Some(2));
        assert_eq!(status.vector_schema_status.as_deref(), Some("current"));
        Ok(())
    }
}
