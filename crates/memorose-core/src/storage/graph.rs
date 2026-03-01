use anyhow::{Result, Context};
use std::sync::Arc;
use tokio::sync::Mutex;
use memorose_common::{GraphEdge, RelationType};
use uuid::Uuid;
use lancedb::Connection;
use lancedb::query::{ExecutableQuery, QueryBase};
use arrow_array::{RecordBatch, StringArray, Float32Array, TimestampMicrosecondArray, RecordBatchIterator};
use arrow_schema::{Schema, Field, DataType, TimeUnit};
use futures::TryStreamExt;
use chrono::Utc;
use std::time::Duration;

fn create_graph_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("source_id", DataType::Utf8, false),
        Field::new("target_id", DataType::Utf8, false),
        Field::new("relation", DataType::Utf8, false),
        Field::new("weight", DataType::Float32, false),
        Field::new("transaction_time", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ]))
}

#[derive(Clone)]
pub struct GraphStore {
    db: Arc<Connection>,
    buffer: Arc<Mutex<Vec<GraphEdge>>>,
    table_name: String,
    _shutdown: Arc<tokio::sync::Notify>,
    _flush_task: Arc<tokio::task::JoinHandle<()>>,
}

impl Drop for GraphStore {
    fn drop(&mut self) {
        // Signal the background task to exit when the last GraphStore clone is dropped.
        // At that point strong_count is 2: this struct's copy + the task's copy.
        if Arc::strong_count(&self._shutdown) == 2 {
            self._shutdown.notify_one();
        }
    }
}

impl GraphStore {
    pub async fn new(db: Arc<Connection>) -> Result<Self> {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let table_name = "relationships".to_string();
        let shutdown = Arc::new(tokio::sync::Notify::new());

        let db_clone = db.clone();
        let buffer_clone = buffer.clone();
        let table_clone = table_name.clone();
        let shutdown_clone = shutdown.clone();

        let flush_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = GraphStore::flush_with_refs(&db_clone, &buffer_clone, &table_clone).await {
                            tracing::error!("GraphStore periodic flush failed: {:?}", e);
                        }
                    }
                    _ = shutdown_clone.notified() => {
                        tracing::debug!("GraphStore background flush task stopping");
                        break;
                    }
                }
            }
        });

        let store = Self {
            db,
            buffer,
            table_name,
            _shutdown: shutdown,
            _flush_task: Arc::new(flush_task),
        };
        store.init().await?;
        Ok(store)
    }

    async fn init(&self) -> Result<()> {
        let tables = self.db.table_names().execute().await?;
        if !tables.contains(&self.table_name) {
            let schema = create_graph_schema();
            let batch = RecordBatch::new_empty(schema.clone());
            let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);

            self.db.create_table(&self.table_name, reader).execute().await?;
        }
        Ok(())
    }

    pub async fn add_edge(&self, edge: &GraphEdge) -> Result<()> {
        let should_flush = {
            let mut buf = self.buffer.lock().await;
            buf.push(edge.clone());
            buf.len() >= 100
        };

        if should_flush {
            self.flush().await?;
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        Self::flush_with_refs(&self.db, &self.buffer, &self.table_name).await
    }

    async fn flush_with_refs(
        db: &Arc<Connection>,
        buffer: &Arc<Mutex<Vec<GraphEdge>>>,
        table_name: &str,
    ) -> Result<()> {
        // Atomically drain the buffer. New edges can be added concurrently while we write.
        let edges = {
            let mut buf = buffer.lock().await;
            if buf.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *buf)
        };

        let schema = create_graph_schema();

        let user_ids: Vec<String> = edges.iter().map(|e| e.user_id.clone()).collect();
        let source_ids: Vec<String> = edges.iter().map(|e| e.source_id.to_string()).collect();
        let target_ids: Vec<String> = edges.iter().map(|e| e.target_id.to_string()).collect();
        let relations: Vec<String> = edges.iter().map(|e| e.relation.as_str().to_string()).collect();
        let weights: Vec<f32> = edges.iter().map(|e| e.weight).collect();
        let times: Vec<i64> = edges.iter().map(|e| e.transaction_time.timestamp_micros()).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(user_ids)),
                Arc::new(StringArray::from(source_ids)),
                Arc::new(StringArray::from(target_ids)),
                Arc::new(StringArray::from(relations)),
                Arc::new(Float32Array::from(weights)),
                Arc::new(TimestampMicrosecondArray::from(times).with_timezone("UTC")),
            ],
        );

        let write_result = async {
            let batch = batch?;
            let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
            let table = db.open_table(table_name).execute().await?;
            table.add(reader).execute().await?;
            Ok::<_, anyhow::Error>(())
        }.await;

        // On failure, put the edges back into the buffer so they are retried on the next flush.
        if let Err(e) = write_result {
            let mut buf = buffer.lock().await;
            let new_edges = std::mem::take(&mut *buf);
            *buf = edges;
            buf.extend(new_edges);
            return Err(e);
        }

        Ok(())
    }

    pub async fn reinforce_edge(&self, user_id: &str, source_id: Uuid, target_id: Uuid, delta: f32) -> Result<()> {
        // Try to find existing edge
        let existing_edges = self.get_outgoing_edges(user_id, source_id).await?;
        let existing_edge = existing_edges.iter()
            .find(|e| e.target_id == target_id && e.relation == RelationType::RelatedTo);

        let new_weight = if let Some(edge) = existing_edge {
            (edge.weight + delta).min(1.0)
        } else {
            delta.min(1.0)
        };

        // Delete stale rows for this (source, target, RelatedTo) tuple from LanceDB
        // and from the in-memory buffer. Without this, every reinforce_edge call
        // appends a new row, causing unbounded storage growth.
        if existing_edge.is_some() {
            let escaped_user = user_id.replace('\'', "''");
            let filter = format!(
                "user_id = '{}' AND source_id = '{}' AND target_id = '{}' AND relation = 'RelatedTo'",
                escaped_user, source_id, target_id
            );
            let table = self.db.open_table(&self.table_name).execute().await?;
            table.delete(&filter).await?;

            // Also purge from the in-memory write buffer so a pending flush doesn't
            // re-insert the old weight on top of the new row.
            let mut buf = self.buffer.lock().await;
            buf.retain(|e| !(
                e.user_id == user_id
                && e.source_id == source_id
                && e.target_id == target_id
                && e.relation == RelationType::RelatedTo
            ));
        }

        let edge = GraphEdge::new(user_id.to_string(), source_id, target_id, RelationType::RelatedTo, new_weight);
        self.add_edge(&edge).await
    }

    pub async fn get_outgoing_edges(&self, user_id: &str, source_id: Uuid) -> Result<Vec<GraphEdge>> {
        let mut edges = {
            let buf = self.buffer.lock().await;
            buf.iter()
               .filter(|e| e.user_id == user_id && e.source_id == source_id)
               .cloned()
               .collect::<Vec<_>>()
        };

        let table = self.db.open_table(&self.table_name).execute().await?;
        let escaped_user_id = user_id.replace("'", "''");
        let batches: Vec<RecordBatch> = table.query()
            .only_if(format!("user_id = '{}' AND source_id = '{}'", escaped_user_id, source_id))
            .execute().await?.try_collect().await?;

        edges.extend(self.batches_to_edges(batches)?);
        Ok(Self::dedup_edges(edges))
    }

    pub async fn get_incoming_edges(&self, user_id: &str, target_id: Uuid) -> Result<Vec<GraphEdge>> {
        let mut edges = {
            let buf = self.buffer.lock().await;
            buf.iter()
               .filter(|e| e.user_id == user_id && e.target_id == target_id)
               .cloned()
               .collect::<Vec<_>>()
        };

        let table = self.db.open_table(&self.table_name).execute().await?;
        let escaped_user_id = user_id.replace("'", "''");
        let batches: Vec<RecordBatch> = table.query()
            .only_if(format!("user_id = '{}' AND target_id = '{}'", escaped_user_id, target_id))
            .execute().await?.try_collect().await?;

        edges.extend(self.batches_to_edges(batches)?);
        Ok(Self::dedup_edges(edges))
    }

    pub async fn get_all_edges_for_user(&self, user_id: &str) -> Result<Vec<GraphEdge>> {
        let mut edges = {
            let buf = self.buffer.lock().await;
            buf.iter()
               .filter(|e| e.user_id == user_id)
               .cloned()
               .collect::<Vec<_>>()
        };

        let table = self.db.open_table(&self.table_name).execute().await?;
        let escaped_user_id = user_id.replace("'", "''");
        let batches: Vec<RecordBatch> = table.query()
            .only_if(format!("user_id = '{}'", escaped_user_id))
            .execute().await?.try_collect().await?;

        edges.extend(self.batches_to_edges(batches)?);
        Ok(Self::dedup_edges(edges))
    }

    pub async fn scan_all_edges(&self) -> Result<Vec<GraphEdge>> {
        let mut edges = {
            let buf = self.buffer.lock().await;
            buf.clone()
        };

        let table = self.db.open_table(&self.table_name).execute().await?;
        let batches: Vec<RecordBatch> = table.query()
            .execute()
            .await?
            .try_collect()
            .await?;

        edges.extend(self.batches_to_edges(batches)?);
        Ok(Self::dedup_edges(edges))
    }

    /// 批量查询多个节点的出边（使用 SQL IN 子句，性能优化版本）
    pub async fn batch_get_outgoing_edges(
        &self,
        user_id: &str,
        source_ids: &[Uuid],
    ) -> Result<std::collections::HashMap<Uuid, Vec<GraphEdge>>> {
        use std::collections::HashMap;

        if source_ids.is_empty() {
            return Ok(HashMap::new());
        }

        // 先检查缓冲区
        let mut result: HashMap<Uuid, Vec<GraphEdge>> = HashMap::new();
        {
            let buf = self.buffer.lock().await;
            for edge in buf.iter() {
                if edge.user_id == user_id && source_ids.contains(&edge.source_id) {
                    result.entry(edge.source_id)
                        .or_insert_with(Vec::new)
                        .push(edge.clone());
                }
            }
        }

        // 构建 SQL IN 子句
        let source_ids_str = source_ids.iter()
            .map(|id| format!("'{}'", id))
            .collect::<Vec<_>>()
            .join(", ");

        let escaped_user_id = user_id.replace("'", "''");
        let filter = format!(
            "user_id = '{}' AND source_id IN ({})",
            escaped_user_id,
            source_ids_str
        );

        // 单次批量查询
        let table = self.db.open_table(&self.table_name).execute().await?;
        let batches: Vec<RecordBatch> = table.query()
            .only_if(filter)
            .execute()
            .await?
            .try_collect()
            .await?;

        // 分组
        let db_edges = self.batches_to_edges(batches)?;
        for edge in db_edges {
            result.entry(edge.source_id)
                .or_insert_with(Vec::new)
                .push(edge);
        }

        for edges in result.values_mut() {
            let deduped = Self::dedup_edges(std::mem::take(edges));
            *edges = deduped;
        }

        Ok(result)
    }

    /// 批量查询多个节点的入边（使用 SQL IN 子句）
    pub async fn batch_get_incoming_edges(
        &self,
        user_id: &str,
        target_ids: &[Uuid],
    ) -> Result<std::collections::HashMap<Uuid, Vec<GraphEdge>>> {
        use std::collections::HashMap;

        if target_ids.is_empty() {
            return Ok(HashMap::new());
        }

        // 先检查缓冲区
        let mut result: HashMap<Uuid, Vec<GraphEdge>> = HashMap::new();
        {
            let buf = self.buffer.lock().await;
            for edge in buf.iter() {
                if edge.user_id == user_id && target_ids.contains(&edge.target_id) {
                    result.entry(edge.target_id)
                        .or_insert_with(Vec::new)
                        .push(edge.clone());
                }
            }
        }

        // 构建 SQL IN 子句
        let target_ids_str = target_ids.iter()
            .map(|id| format!("'{}'", id))
            .collect::<Vec<_>>()
            .join(", ");

        let escaped_user_id = user_id.replace("'", "''");
        let filter = format!(
            "user_id = '{}' AND target_id IN ({})",
            escaped_user_id,
            target_ids_str
        );

        // 单次批量查询
        let table = self.db.open_table(&self.table_name).execute().await?;
        let batches: Vec<RecordBatch> = table.query()
            .only_if(filter)
            .execute()
            .await?
            .try_collect()
            .await?;

        // 分组
        let db_edges = self.batches_to_edges(batches)?;
        for edge in db_edges {
            result.entry(edge.target_id)
                .or_insert_with(Vec::new)
                .push(edge);
        }

        for edges in result.values_mut() {
            let deduped = Self::dedup_edges(std::mem::take(edges));
            *edges = deduped;
        }

        Ok(result)
    }

    fn dedup_edges(edges: Vec<GraphEdge>) -> Vec<GraphEdge> {
        use std::collections::HashMap;

        let mut map: HashMap<(String, Uuid, Uuid, &'static str), GraphEdge> = HashMap::new();
        for edge in edges {
            let key = (
                edge.user_id.clone(),
                edge.source_id,
                edge.target_id,
                edge.relation.as_str(),
            );

            match map.get_mut(&key) {
                Some(existing) => {
                    if edge.transaction_time > existing.transaction_time
                        || (edge.transaction_time == existing.transaction_time && edge.weight > existing.weight)
                    {
                        *existing = edge;
                    }
                }
                None => {
                    map.insert(key, edge);
                }
            }
        }

        map.into_values().collect()
    }

    fn batches_to_edges(&self, batches: Vec<RecordBatch>) -> Result<Vec<GraphEdge>> {
        let mut edges = Vec::new();
        for batch in batches {
            let user_col = batch.column(0).as_any().downcast_ref::<StringArray>().context("col 0")?;
            let source_col = batch.column(1).as_any().downcast_ref::<StringArray>().context("col 1")?;
            let target_col = batch.column(2).as_any().downcast_ref::<StringArray>().context("col 2")?;
            let rel_col = batch.column(3).as_any().downcast_ref::<StringArray>().context("col 3")?;
            let weight_col = batch.column(4).as_any().downcast_ref::<Float32Array>().context("col 4")?;
            let time_col = batch.column(5).as_any().downcast_ref::<TimestampMicrosecondArray>().context("col 5")?;

            for i in 0..batch.num_rows() {
                let user_id = user_col.value(i).to_string();
                let source = Uuid::parse_str(source_col.value(i)).unwrap_or_default();
                let target = Uuid::parse_str(target_col.value(i)).unwrap_or_default();
                let rel_str = rel_col.value(i);
                let relation = match rel_str {
                    "Next" => RelationType::Next,
                    "IsSubTaskOf" => RelationType::IsSubTaskOf,
                    "Contradicts" => RelationType::Contradicts,
                    "DerivedFrom" => RelationType::DerivedFrom,
                    "EvolvedTo" => RelationType::EvolvedTo,
                    "Supports" => RelationType::Supports,
                    "Abstracts" => RelationType::Abstracts,
                    "CausedBy" => RelationType::CausedBy,
                    "Blocks" => RelationType::Blocks,
                    "Accomplishes" => RelationType::Accomplishes,
                    _ => RelationType::RelatedTo,
                };
                let ts_micros = time_col.value(i);
                // Use Euclidean division so the nanosecond remainder is always non-negative,
                // which correctly handles timestamps before the Unix epoch.
                let secs = ts_micros.div_euclid(1_000_000);
                let nanos = (ts_micros.rem_euclid(1_000_000) * 1_000) as u32;
                let timestamp = chrono::TimeZone::timestamp_opt(&Utc, secs, nanos)
                    .single()
                    .unwrap_or_else(|| {
                        tracing::warn!(
                            "Invalid graph edge timestamp {}µs (secs={}, nanos={}), defaulting to current time",
                            ts_micros, secs, nanos
                        );
                        Utc::now()
                    });

                edges.push(GraphEdge {
                    user_id,
                    source_id: source,
                    target_id: target,
                    relation,
                    weight: weight_col.value(i),
                    transaction_time: timestamp,
                });
            }
        }
        Ok(edges)
    }
}
