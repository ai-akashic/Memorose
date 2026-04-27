use anyhow::Result;
use arrow_array::{
    Array, FixedSizeListArray, Float32Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::StreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::table::OptimizeAction;
use lancedb::{connect, Connection};
use memorose_common::MemoryUnit;
use std::sync::Arc;

pub const VECTOR_SCHEMA_VERSION: u32 = 2;

#[derive(Clone)]
pub struct VectorStore {
    conn: Connection,
    dim: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorOptimizeReport {
    pub compaction_ran: bool,
    pub prune_ran: bool,
}

impl VectorStore {
    pub fn expected_columns() -> Vec<&'static str> {
        vec![
            "id",
            "user_id",
            "org_id",
            "agent_id",
            "domain",
            "namespace_key",
            "level",
            "transaction_time",
            "valid_time",
            "vector",
        ]
    }

    pub async fn new(path: &str, dim: i32) -> Result<Self> {
        let conn = connect(path).execute().await?;
        Ok(Self { conn, dim })
    }

    pub async fn table_schema_status(
        &self,
        table_name: &str,
    ) -> Result<(Vec<String>, Option<u32>, String)> {
        let table = self.conn.open_table(table_name).execute().await?;
        let schema = table.schema().await?;
        let actual_columns: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        let expected_columns: Vec<String> = Self::expected_columns()
            .into_iter()
            .map(|name| name.to_string())
            .collect();

        let has_legacy_columns = actual_columns
            .iter()
            .any(|column| matches!(column.as_str(), "content" | "memory_type" | "stream_id"));

        let (version, status) = if actual_columns == expected_columns {
            (Some(VECTOR_SCHEMA_VERSION), "current".to_string())
        } else if has_legacy_columns {
            (Some(1), "legacy".to_string())
        } else {
            (None, "mismatch".to_string())
        };

        Ok((actual_columns, version, status))
    }

    pub async fn ensure_table(&self, table_name: &str) -> Result<()> {
        let tables = self.conn.table_names().execute().await?;
        if tables.contains(&table_name.to_string()) {
            let table = self.conn.open_table(table_name).execute().await?;
            let schema = table.schema().await?;
            let actual_columns: Vec<String> = schema
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect();
            let expected_columns: Vec<String> = Self::expected_columns()
                .into_iter()
                .map(|name| name.to_string())
                .collect();
            if actual_columns != expected_columns {
                tracing::warn!(
                    "LanceDB table '{}' has legacy schema {:?}, recreating with {:?}",
                    table_name,
                    actual_columns,
                    expected_columns
                );
                self.conn.drop_table(table_name, &[]).await?;
            } else {
                return Ok(());
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("user_id", DataType::Utf8, false),
            Field::new("org_id", DataType::Utf8, true),
            Field::new("agent_id", DataType::Utf8, true),
            Field::new("domain", DataType::Utf8, false),
            Field::new("namespace_key", DataType::Utf8, false),
            Field::new("level", DataType::UInt8, false),
            Field::new(
                "transaction_time",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new(
                "valid_time",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    self.dim,
                ),
                false,
            ),
        ]));

        self.conn
            .create_empty_table(table_name, schema)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn add(&self, table_name: &str, units: Vec<MemoryUnit>) -> Result<()> {
        if units.is_empty() {
            return Ok(());
        }

        let table = self.conn.open_table(table_name).execute().await?;

        let mut ids = Vec::new();
        let mut user_ids = Vec::new();
        let mut org_ids: Vec<Option<String>> = Vec::new();
        let mut agent_ids: Vec<Option<String>> = Vec::new();
        let mut domains = Vec::new();
        let mut namespace_keys = Vec::new();
        let mut levels = Vec::new();
        let mut transaction_times = Vec::new();
        let mut valid_ats = Vec::new();
        let mut vectors_flat = Vec::new();

        for unit in &units {
            ids.push(unit.id.to_string());
            user_ids.push(unit.user_id.clone());
            org_ids.push(unit.org_id.clone());
            agent_ids.push(unit.agent_id.clone());
            domains.push(unit.domain.as_str().to_string());
            namespace_keys.push(unit.namespace_key.clone());

            levels.push(unit.level);
            transaction_times.push(unit.transaction_time.timestamp_micros());
            valid_ats.push(unit.valid_time.map(|t| t.timestamp_micros()));

            if let Some(emb) = &unit.embedding {
                if emb.len() != self.dim as usize {
                    let mut e = emb.clone();
                    e.resize(self.dim as usize, 0.0);
                    vectors_flat.extend(e);
                } else {
                    vectors_flat.extend(emb);
                }
            } else {
                vectors_flat.extend(vec![0.0; self.dim as usize]);
            }
        }

        let id_array = Arc::new(StringArray::from(ids));
        let user_id_array = Arc::new(StringArray::from(user_ids));
        let org_id_array = Arc::new(StringArray::from(org_ids));
        let agent_id_array = Arc::new(StringArray::from(agent_ids));
        let domain_array = Arc::new(StringArray::from(domains));
        let namespace_key_array = Arc::new(StringArray::from(namespace_keys));
        let level_array = Arc::new(arrow_array::UInt8Array::from(levels));
        let transaction_time_array =
            Arc::new(TimestampMicrosecondArray::from(transaction_times).with_timezone("UTC"));
        let valid_time_array =
            Arc::new(TimestampMicrosecondArray::from(valid_ats).with_timezone("UTC"));

        let field = Arc::new(Field::new("item", DataType::Float32, true));
        let values = Arc::new(Float32Array::from(vectors_flat));
        let vector_array = Arc::new(FixedSizeListArray::new(field, self.dim, values, None));

        let schema = table.schema().await?;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                id_array as Arc<dyn Array>,
                user_id_array as Arc<dyn Array>,
                org_id_array as Arc<dyn Array>,
                agent_id_array as Arc<dyn Array>,
                domain_array as Arc<dyn Array>,
                namespace_key_array as Arc<dyn Array>,
                level_array as Arc<dyn Array>,
                transaction_time_array as Arc<dyn Array>,
                valid_time_array as Arc<dyn Array>,
                vector_array as Arc<dyn Array>,
            ],
        )?;

        table.add(vec![batch]).execute().await?;

        Ok(())
    }

    pub async fn optimize_table(&self, table_name: &str) -> Result<VectorOptimizeReport> {
        let table = match self.conn.open_table(table_name).execute().await {
            Ok(table) => table,
            Err(error) if error.to_string().to_lowercase().contains("not found") => {
                return Ok(VectorOptimizeReport {
                    compaction_ran: false,
                    prune_ran: false,
                });
            }
            Err(error) => return Err(error.into()),
        };
        let stats = table.optimize(OptimizeAction::All).await?;
        Ok(VectorOptimizeReport {
            compaction_ran: stats.compaction.is_some(),
            prune_ran: stats.prune.is_some(),
        })
    }

    pub async fn compact_files(&self, table_name: &str) -> Result<()> {
        let stats = self.optimize_table(table_name).await?;
        tracing::info!(
            compaction_ran = stats.compaction_ran,
            prune_ran = stats.prune_ran,
            "LanceDB table optimized"
        );
        Ok(())
    }

    /// Delete a single memory unit from the vector table by its ID.
    pub async fn delete_by_id(&self, table_name: &str, id: &str) -> Result<()> {
        let table = match self.conn.open_table(table_name).execute().await {
            Ok(t) => t,
            Err(e) if e.to_string().to_lowercase().contains("not found") => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let escaped = id.replace('\'', "''");
        table.delete(&format!("id = '{}'", escaped)).await?;
        Ok(())
    }

    pub async fn delete_table(&self, table_name: &str) -> Result<()> {
        match self.conn.drop_table(table_name, &[]).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("not found") || msg.contains("No such file") {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!(e))
                }
            }
        }
    }

    pub async fn count_rows(&self, table_name: &str) -> Result<usize> {
        let table = self.conn.open_table(table_name).execute().await?;
        Ok(table.count_rows(None).await?)
    }

    pub async fn search(
        &self,
        table_name: &str,
        query_vector: &[f32],
        limit: usize,
        filter: Option<String>,
    ) -> Result<Vec<(String, f32)>> {
        let table = self.conn.open_table(table_name).execute().await?;

        let mut q = query_vector.to_vec();
        q.resize(self.dim as usize, 0.0);

        let mut query = table.query().nearest_to(q.as_slice())?.limit(limit);

        if let Some(f) = filter {
            query = query.only_if(f);
        }

        let mut stream = query.execute().await?;

        let mut results = Vec::new();
        while let Some(batch_res) = stream.next().await {
            let batch: RecordBatch = batch_res?;
            let id_col = batch
                .column_by_name("id")
                .ok_or_else(|| anyhow::anyhow!("id column not found"))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("failed to downcast id column"))?;

            let dist_col = batch
                .column_by_name("_distance")
                .ok_or_else(|| anyhow::anyhow!("_distance column not found"))?
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| anyhow::anyhow!("failed to downcast _distance column"))?;

            for i in 0..id_col.len() {
                let id = id_col.value(i).to_string();
                let dist = dist_col.value(i);
                let score = 1.0 / (1.0 + dist);
                results.push((id, score));
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tempfile::tempdir;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_vector_store() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().to_str().unwrap();

        let store = VectorStore::new(db_path, 384).await?;
        store.ensure_table("memories").await?;

        let stream_id = Uuid::new_v4();

        // Create unit with a specific vector
        let mut embedding = vec![0.0; 384];
        embedding[0] = 1.0; // Mark first dim

        let unit = MemoryUnit::new(
            None,
            "u1".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Vector Test".to_string(),
            Some(embedding.clone()),
        );
        store.add("memories", vec![unit.clone()]).await?;

        // Search with exact same vector
        let results = store.search("memories", &embedding, 5, None).await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, unit.id.to_string());
        assert!(results[0].1 > 0.99); // Should be very high similarity

        Ok(())
    }

    #[tokio::test]
    async fn test_vector_store_uses_slim_schema() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().to_str().unwrap();

        let store = VectorStore::new(db_path, 384).await?;
        store.ensure_table("memories").await?;

        let table = store.conn.open_table("memories").execute().await?;
        let schema = table.schema().await?;
        let columns: Vec<&str> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();

        assert_eq!(
            columns,
            vec![
                "id",
                "user_id",
                "org_id",
                "agent_id",
                "domain",
                "namespace_key",
                "level",
                "transaction_time",
                "valid_time",
                "vector",
            ]
        );
        assert!(!columns.contains(&"content"));
        assert!(!columns.contains(&"memory_type"));
        assert!(!columns.contains(&"stream_id"));

        Ok(())
    }

    #[tokio::test]
    async fn test_temporal_search() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().to_str().unwrap();

        let store = VectorStore::new(db_path, 384).await?;
        store.ensure_table("memories").await?;

        let stream_id = Uuid::new_v4();
        let embedding = vec![0.0; 384];

        // 1. Store OLD memory (valid last year)
        let mut u1 = MemoryUnit::new(
            None,
            "u1".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Old info".into(),
            Some(embedding.clone()),
        );
        u1.valid_time = Some(Utc::now() - chrono::Duration::days(365));
        store.add("memories", vec![u1.clone()]).await?;

        // 2. Store NEW memory (valid now)
        let mut u2 = MemoryUnit::new(
            None,
            "u1".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "New info".into(),
            Some(embedding.clone()),
        );
        u2.valid_time = Some(Utc::now());
        store.add("memories", vec![u2.clone()]).await?;

        // 3. Search for recent only (> 7 days ago)
        let cutoff = Utc::now() - chrono::Duration::days(7);
        let cutoff_str = cutoff.format("%Y-%m-%d %H:%M:%S").to_string();
        let filter = format!("valid_time > timestamp '{}'", cutoff_str);

        let results = store
            .search("memories", &embedding, 5, Some(filter))
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, u2.id.to_string());

        Ok(())
    }

    #[tokio::test]
    async fn test_vector_store_optimize_table_runs_lancedb_maintenance() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().to_str().unwrap();

        let store = VectorStore::new(db_path, 4).await?;
        store.ensure_table("memories").await?;

        let stream_id = Uuid::new_v4();
        let unit = MemoryUnit::new(
            None,
            "u1".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "optimize me".into(),
            Some(vec![1.0, 0.0, 0.0, 0.0]),
        );
        store.add("memories", vec![unit.clone()]).await?;

        let stats = store.optimize_table("memories").await?;

        assert!(stats.compaction_ran);
        assert!(stats.prune_ran);

        let results = store
            .search("memories", &[1.0, 0.0, 0.0, 0.0], 5, None)
            .await?;
        assert_eq!(results[0].0, unit.id.to_string());
        Ok(())
    }
}
