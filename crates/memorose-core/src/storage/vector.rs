use anyhow::Result;
use lancedb::{connect, Connection};
use lancedb::query::{QueryBase, ExecutableQuery};
use arrow_schema::{Schema, Field, DataType, TimeUnit};
use arrow_array::{RecordBatch, StringArray, Float32Array, FixedSizeListArray, Array, TimestampMicrosecondArray};
use std::sync::Arc;
use memorose_common::MemoryUnit;
use futures::StreamExt;

#[derive(Clone)]
pub struct VectorStore {
    conn: Connection,
    dim: i32,
}

impl VectorStore {
    pub async fn new(path: &str, dim: i32) -> Result<Self> {
        let conn = connect(path).execute().await?;
        Ok(Self { conn, dim })
    }

    pub async fn ensure_table(&self, table_name: &str) -> Result<()> {
        let tables = self.conn.table_names().execute().await?;
        if tables.contains(&table_name.to_string()) {
            // Check if existing table has user_id column; if not, drop and recreate
            let table = self.conn.open_table(table_name).execute().await?;
            let schema = table.schema().await?;
            let has_user_id = schema.fields().iter().any(|f| f.name() == "user_id");
            if !has_user_id {
                tracing::warn!("LanceDB table '{}' missing user_id column, recreating...", table_name);
                self.conn.drop_table(table_name).await?;
            } else {
                return Ok(());
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("user_id", DataType::Utf8, false),
            Field::new("app_id", DataType::Utf8, false),
            Field::new("stream_id", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
            Field::new("level", DataType::UInt8, false),
            Field::new("transaction_time", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("valid_time", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    self.dim,
                ),
                false,
            ),
        ]));

        self.conn.create_empty_table(table_name, schema).execute().await?;
        Ok(())
    }

    pub async fn add(&self, table_name: &str, units: Vec<MemoryUnit>) -> Result<()> {
        if units.is_empty() {
            return Ok(());
        }

        let table = self.conn.open_table(table_name).execute().await?;

        let mut ids = Vec::new();
        let mut user_ids = Vec::new();
        let mut app_ids = Vec::new();
        let mut stream_ids = Vec::new();
        let mut contents = Vec::new();
        let mut levels = Vec::new();
        let mut transaction_times = Vec::new();
        let mut valid_ats = Vec::new();
        let mut vectors_flat = Vec::new();

        for unit in &units {
            ids.push(unit.id.to_string());
            user_ids.push(unit.user_id.clone());
            app_ids.push(unit.app_id.clone());
            stream_ids.push(unit.stream_id.to_string());
            contents.push(unit.content.clone());
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
        let app_id_array = Arc::new(StringArray::from(app_ids));
        let stream_id_array = Arc::new(StringArray::from(stream_ids));
        let content_array = Arc::new(StringArray::from(contents));
        let level_array = Arc::new(arrow_array::UInt8Array::from(levels));
        let transaction_time_array = Arc::new(TimestampMicrosecondArray::from(transaction_times).with_timezone("UTC"));
        let valid_time_array = Arc::new(TimestampMicrosecondArray::from(valid_ats).with_timezone("UTC"));
        
        let field = Arc::new(Field::new("item", DataType::Float32, true));
        let values = Arc::new(Float32Array::from(vectors_flat));
        let vector_array = Arc::new(FixedSizeListArray::new(
            field,
            self.dim,
            values,
            None,
        ));

        let schema = table.schema().await?;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                id_array as Arc<dyn Array>,
                user_id_array as Arc<dyn Array>,
                app_id_array as Arc<dyn Array>,
                stream_id_array as Arc<dyn Array>,
                content_array as Arc<dyn Array>,
                level_array as Arc<dyn Array>,
                transaction_time_array as Arc<dyn Array>,
                valid_time_array as Arc<dyn Array>,
                vector_array as Arc<dyn Array>,
            ]
        )?;

        let batch_iter = arrow_array::RecordBatchIterator::new(
             vec![Ok(batch)].into_iter(),
             table.schema().await?
        );
        
        table.add(batch_iter).execute().await?;

        Ok(())
    }

    pub async fn compact_files(&self, _table_name: &str) -> Result<()> {
        tracing::warn!("Compaction skipped: API unstable");
        Ok(())
    }

    pub async fn delete_table(&self, table_name: &str) -> Result<()> {
        match self.conn.drop_table(table_name).await {
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

    pub async fn search(&self, table_name: &str, query_vector: &[f32], limit: usize, filter: Option<String>) -> Result<Vec<(String, f32)>> {
         let table = self.conn.open_table(table_name).execute().await?;
         
         let mut q = query_vector.to_vec();
         q.resize(self.dim as usize, 0.0);

         let mut query = table
            .query()
            .nearest_to(q.as_slice())?
            .limit(limit);
        
        if let Some(f) = filter {
            query = query.only_if(f);
        }

        let mut stream = query.execute().await?;
        
        let mut results = Vec::new();
        while let Some(batch_res) = stream.next().await {
            let batch: RecordBatch = batch_res?;
            let id_col = batch.column_by_name("id")
                .ok_or_else(|| anyhow::anyhow!("id column not found"))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("failed to downcast id column"))?;
            
            let dist_col = batch.column_by_name("_distance")
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
    use tempfile::tempdir;
    use uuid::Uuid;
    use chrono::Utc;

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
        
        let unit = MemoryUnit::new("u1".into(), None, "a1".into(), stream_id, memorose_common::MemoryType::Factual, "Vector Test".to_string(), Some(embedding.clone()));
        store.add("memories", vec![unit.clone()]).await?;

        // Search with exact same vector
        let results = store.search("memories", &embedding, 5, None).await?;
        
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, unit.id.to_string());
        assert!(results[0].1 > 0.99); // Should be very high similarity

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
        let mut u1 = MemoryUnit::new("u1".into(), None, "a1".into(), stream_id, memorose_common::MemoryType::Factual, "Old info".into(), Some(embedding.clone()));
        u1.valid_time = Some(Utc::now() - chrono::Duration::days(365));
        store.add("memories", vec![u1.clone()]).await?;

        // 2. Store NEW memory (valid now)
        let mut u2 = MemoryUnit::new("u1".into(), None, "a1".into(), stream_id, memorose_common::MemoryType::Factual, "New info".into(), Some(embedding.clone()));
        u2.valid_time = Some(Utc::now());
        store.add("memories", vec![u2.clone()]).await?;

        // 3. Search for recent only (> 7 days ago)
        let cutoff = Utc::now() - chrono::Duration::days(7);
        let cutoff_str = cutoff.format("%Y-%m-%d %H:%M:%S").to_string();
        let filter = format!("valid_time > timestamp '{}'", cutoff_str);
        
        let results = store.search("memories", &embedding, 5, Some(filter)).await?;
        
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, u2.id.to_string());

        Ok(())
    }
}