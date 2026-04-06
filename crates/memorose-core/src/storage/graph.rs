use anyhow::{Context, Result};
use arrow_array::{
    Float32Array, RecordBatch, RecordBatchIterator, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::Utc;
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::Connection;
use memorose_common::{EdgeKind, GraphEdge, RelationType};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use uuid::Uuid;

fn create_graph_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("namespace_key", DataType::Utf8, false),
        Field::new("source_namespace_key", DataType::Utf8, false),
        Field::new("target_namespace_key", DataType::Utf8, false),
        Field::new("source_id", DataType::Utf8, false),
        Field::new("target_id", DataType::Utf8, false),
        Field::new("edge_kind", DataType::Utf8, false),
        Field::new("relation", DataType::Utf8, false),
        Field::new("weight", DataType::Float32, false),
        Field::new(
            "transaction_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
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
        if tables.contains(&self.table_name) {
            let table = self.db.open_table(&self.table_name).execute().await?;
            let schema = table.schema().await?;
            let required_columns = [
                "user_id",
                "namespace_key",
                "source_namespace_key",
                "target_namespace_key",
                "source_id",
                "target_id",
                "edge_kind",
                "relation",
                "weight",
                "transaction_time",
            ];
            let has_required_columns = required_columns
                .iter()
                .all(|name| schema.fields().iter().any(|field| field.name() == *name));
            if !has_required_columns {
                tracing::warn!(
                    "Graph table '{}' missing scoped edge columns, recreating...",
                    self.table_name
                );
                self.db.drop_table(&self.table_name).await?;
            } else {
                return Ok(());
            }
        }

        let schema = create_graph_schema();
        let batch = RecordBatch::new_empty(schema.clone());
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);

        self.db
            .create_table(&self.table_name, reader)
            .execute()
            .await?;
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
        let namespace_keys: Vec<String> = edges.iter().map(|e| e.namespace_key.clone()).collect();
        let source_namespace_keys: Vec<String> = edges
            .iter()
            .map(|e| e.source_namespace_key.clone())
            .collect();
        let target_namespace_keys: Vec<String> = edges
            .iter()
            .map(|e| e.target_namespace_key.clone())
            .collect();
        let source_ids: Vec<String> = edges.iter().map(|e| e.source_id.to_string()).collect();
        let target_ids: Vec<String> = edges.iter().map(|e| e.target_id.to_string()).collect();
        let edge_kinds: Vec<String> = edges
            .iter()
            .map(|e| e.edge_kind.as_str().to_string())
            .collect();
        let relations: Vec<String> = edges
            .iter()
            .map(|e| e.relation.as_str().to_string())
            .collect();
        let weights: Vec<f32> = edges.iter().map(|e| e.weight).collect();
        let times: Vec<i64> = edges
            .iter()
            .map(|e| e.transaction_time.timestamp_micros())
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(user_ids)),
                Arc::new(StringArray::from(namespace_keys)),
                Arc::new(StringArray::from(source_namespace_keys)),
                Arc::new(StringArray::from(target_namespace_keys)),
                Arc::new(StringArray::from(source_ids)),
                Arc::new(StringArray::from(target_ids)),
                Arc::new(StringArray::from(edge_kinds)),
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
        }
        .await;

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

    pub async fn reinforce_edge(
        &self,
        user_id: &str,
        source_id: Uuid,
        target_id: Uuid,
        delta: f32,
    ) -> Result<()> {
        // Try to find existing edge
        let existing_edges = self.get_outgoing_edges(user_id, source_id).await?;
        let existing_edge = existing_edges
            .iter()
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
            let escaped_namespace = existing_edge
                .map(|edge| edge.namespace_key.replace('\'', "''"))
                .unwrap_or_default();
            let filter = format!(
                "user_id = '{}' AND namespace_key = '{}' AND source_id = '{}' AND target_id = '{}' AND relation = 'RelatedTo'",
                escaped_user, escaped_namespace, source_id, target_id
            );
            let table = self.db.open_table(&self.table_name).execute().await?;
            table.delete(&filter).await?;

            // Also purge from the in-memory write buffer so a pending flush doesn't
            // re-insert the old weight on top of the new row.
            let mut buf = self.buffer.lock().await;
            buf.retain(|e| {
                !(e.user_id == user_id
                    && e.namespace_key
                        == existing_edge
                            .map(|edge| edge.namespace_key.clone())
                            .unwrap_or_default()
                    && e.source_id == source_id
                    && e.target_id == target_id
                    && e.relation == RelationType::RelatedTo)
            });
        }

        let edge = if let Some(existing_edge) = existing_edge {
            GraphEdge::new_scoped(
                user_id.to_string(),
                source_id,
                target_id,
                RelationType::RelatedTo,
                new_weight,
                existing_edge.namespace_key.clone(),
                Some(existing_edge.source_namespace_key.clone()),
                Some(existing_edge.target_namespace_key.clone()),
                existing_edge.edge_kind.clone(),
            )
        } else {
            GraphEdge::new(
                user_id.to_string(),
                source_id,
                target_id,
                RelationType::RelatedTo,
                new_weight,
            )
        };
        self.add_edge(&edge).await
    }

    pub async fn get_outgoing_edges(
        &self,
        user_id: &str,
        source_id: Uuid,
    ) -> Result<Vec<GraphEdge>> {
        let mut edges = {
            let buf = self.buffer.lock().await;
            buf.iter()
                .filter(|e| e.user_id == user_id && e.source_id == source_id)
                .cloned()
                .collect::<Vec<_>>()
        };

        let table = self.db.open_table(&self.table_name).execute().await?;
        let escaped_user_id = user_id.replace("'", "''");
        let batches: Vec<RecordBatch> = table
            .query()
            .only_if(format!(
                "user_id = '{}' AND source_id = '{}'",
                escaped_user_id, source_id
            ))
            .execute()
            .await?
            .try_collect()
            .await?;

        edges.extend(self.batches_to_edges(batches)?);
        Ok(Self::dedup_edges(edges))
    }

    pub async fn get_incoming_edges(
        &self,
        user_id: &str,
        target_id: Uuid,
    ) -> Result<Vec<GraphEdge>> {
        let mut edges = {
            let buf = self.buffer.lock().await;
            buf.iter()
                .filter(|e| e.user_id == user_id && e.target_id == target_id)
                .cloned()
                .collect::<Vec<_>>()
        };

        let table = self.db.open_table(&self.table_name).execute().await?;
        let escaped_user_id = user_id.replace("'", "''");
        let batches: Vec<RecordBatch> = table
            .query()
            .only_if(format!(
                "user_id = '{}' AND target_id = '{}'",
                escaped_user_id, target_id
            ))
            .execute()
            .await?
            .try_collect()
            .await?;

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
        let batches: Vec<RecordBatch> = table
            .query()
            .only_if(format!("user_id = '{}'", escaped_user_id))
            .execute()
            .await?
            .try_collect()
            .await?;

        edges.extend(self.batches_to_edges(batches)?);
        Ok(Self::dedup_edges(edges))
    }

    pub async fn scan_all_edges(&self) -> Result<Vec<GraphEdge>> {
        let mut edges = {
            let buf = self.buffer.lock().await;
            buf.clone()
        };

        let table = self.db.open_table(&self.table_name).execute().await?;
        let batches: Vec<RecordBatch> = table.query().execute().await?.try_collect().await?;

        edges.extend(self.batches_to_edges(batches)?);
        Ok(Self::dedup_edges(edges))
    }

    pub async fn delete_edges_for_node(&self, user_id: &str, node_id: Uuid) -> Result<usize> {
        let escaped_user_id = user_id.replace("'", "''");
        let filter = format!(
            "user_id = '{}' AND (source_id = '{}' OR target_id = '{}')",
            escaped_user_id, node_id, node_id
        );

        let deleted_in_buffer = {
            let mut buf = self.buffer.lock().await;
            let before = buf.len();
            buf.retain(|edge| {
                !(edge.user_id == user_id
                    && (edge.source_id == node_id || edge.target_id == node_id))
            });
            before.saturating_sub(buf.len())
        };

        let table = self.db.open_table(&self.table_name).execute().await?;
        let existing = table
            .query()
            .only_if(filter.clone())
            .execute()
            .await?
            .try_collect::<Vec<RecordBatch>>()
            .await?;
        let deleted_in_store = self.batches_to_edges(existing)?.len();

        table.delete(&filter).await?;

        Ok(deleted_in_buffer + deleted_in_store)
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
                    result
                        .entry(edge.source_id)
                        .or_insert_with(Vec::new)
                        .push(edge.clone());
                }
            }
        }

        // 构建 SQL IN 子句
        let source_ids_str = source_ids
            .iter()
            .map(|id| format!("'{}'", id))
            .collect::<Vec<_>>()
            .join(", ");

        let escaped_user_id = user_id.replace("'", "''");
        let filter = format!(
            "user_id = '{}' AND source_id IN ({})",
            escaped_user_id, source_ids_str
        );

        // 单次批量查询
        let table = self.db.open_table(&self.table_name).execute().await?;
        let batches: Vec<RecordBatch> = table
            .query()
            .only_if(filter)
            .execute()
            .await?
            .try_collect()
            .await?;

        // 分组
        let db_edges = self.batches_to_edges(batches)?;
        for edge in db_edges {
            result
                .entry(edge.source_id)
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
                    result
                        .entry(edge.target_id)
                        .or_insert_with(Vec::new)
                        .push(edge.clone());
                }
            }
        }

        // 构建 SQL IN 子句
        let target_ids_str = target_ids
            .iter()
            .map(|id| format!("'{}'", id))
            .collect::<Vec<_>>()
            .join(", ");

        let escaped_user_id = user_id.replace("'", "''");
        let filter = format!(
            "user_id = '{}' AND target_id IN ({})",
            escaped_user_id, target_ids_str
        );

        // 单次批量查询
        let table = self.db.open_table(&self.table_name).execute().await?;
        let batches: Vec<RecordBatch> = table
            .query()
            .only_if(filter)
            .execute()
            .await?
            .try_collect()
            .await?;

        // 分组
        let db_edges = self.batches_to_edges(batches)?;
        for edge in db_edges {
            result
                .entry(edge.target_id)
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

        let mut map: HashMap<
            (String, String, String, String, Uuid, Uuid, String, String),
            GraphEdge,
        > = HashMap::new();
        for edge in edges {
            let key = (
                edge.user_id.clone(),
                edge.namespace_key.clone(),
                edge.source_namespace_key.clone(),
                edge.target_namespace_key.clone(),
                edge.source_id,
                edge.target_id,
                edge.relation.as_str().to_string(),
                edge.edge_kind.as_str().to_string(),
            );

            match map.get_mut(&key) {
                Some(existing) => {
                    if edge.transaction_time > existing.transaction_time
                        || (edge.transaction_time == existing.transaction_time
                            && edge.weight > existing.weight)
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
            let user_col = batch
                .column_by_name("user_id")
                .context("user_id column missing")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("user_id")?;
            let namespace_col = batch
                .column_by_name("namespace_key")
                .context("namespace_key column missing")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("namespace_key")?;
            let source_namespace_col = batch
                .column_by_name("source_namespace_key")
                .context("source_namespace_key column missing")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("source_namespace_key")?;
            let target_namespace_col = batch
                .column_by_name("target_namespace_key")
                .context("target_namespace_key column missing")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("target_namespace_key")?;
            let source_col = batch
                .column_by_name("source_id")
                .context("source_id column missing")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("source_id")?;
            let target_col = batch
                .column_by_name("target_id")
                .context("target_id column missing")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("target_id")?;
            let edge_kind_col = batch
                .column_by_name("edge_kind")
                .context("edge_kind column missing")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("edge_kind")?;
            let rel_col = batch
                .column_by_name("relation")
                .context("relation column missing")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("relation")?;
            let weight_col = batch
                .column_by_name("weight")
                .context("weight column missing")?
                .as_any()
                .downcast_ref::<Float32Array>()
                .context("weight")?;
            let time_col = batch
                .column_by_name("transaction_time")
                .context("transaction_time column missing")?
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .context("transaction_time")?;

            for i in 0..batch.num_rows() {
                let user_id = user_col.value(i).to_string();
                let namespace_key = namespace_col.value(i).to_string();
                let source_namespace_key = source_namespace_col.value(i).to_string();
                let target_namespace_key = target_namespace_col.value(i).to_string();
                let source = Uuid::parse_str(source_col.value(i)).unwrap_or_default();
                let target = Uuid::parse_str(target_col.value(i)).unwrap_or_default();
                let edge_kind = EdgeKind::from_str(edge_kind_col.value(i));
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
                    namespace_key,
                    source_namespace_key,
                    target_namespace_key,
                    source_id: source,
                    target_id: target,
                    edge_kind,
                    relation,
                    weight: weight_col.value(i),
                    transaction_time: timestamp,
                });
            }
        }
        Ok(edges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use lancedb::connect;
    use memorose_common::{EdgeKind, RelationType};
    use std::sync::Arc;

    async fn test_store() -> Result<GraphStore> {
        let db_path = std::env::temp_dir().join(format!("memorose-graph-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&db_path)?;
        let db = Arc::new(connect(db_path.to_str().unwrap()).execute().await?);
        GraphStore::new(db).await
    }

    fn scoped_edge(
        user_id: &str,
        source_id: Uuid,
        target_id: Uuid,
        relation: RelationType,
        weight: f32,
        namespace_key: &str,
    ) -> GraphEdge {
        GraphEdge::new_scoped(
            user_id.to_string(),
            source_id,
            target_id,
            relation,
            weight,
            namespace_key.to_string(),
            None,
            None,
            EdgeKind::Native,
        )
    }

    #[test]
    fn test_dedup_edges_prefers_newer_then_heavier() {
        let source_id = Uuid::new_v4();
        let target_id = Uuid::new_v4();

        let mut older = scoped_edge(
            "user1",
            source_id,
            target_id,
            RelationType::RelatedTo,
            0.2,
            "ns:user1",
        );
        older.transaction_time = Utc::now() - Duration::seconds(5);

        let mut newer = older.clone();
        newer.weight = 0.1;
        newer.transaction_time = older.transaction_time + Duration::seconds(1);

        let deduped = GraphStore::dedup_edges(vec![older.clone(), newer.clone()]);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].transaction_time, newer.transaction_time);

        let mut same_time_lighter = newer.clone();
        same_time_lighter.weight = 0.3;
        let mut same_time_heavier = newer;
        same_time_heavier.weight = 0.9;

        let deduped = GraphStore::dedup_edges(vec![same_time_lighter, same_time_heavier.clone()]);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].weight, same_time_heavier.weight);
    }

    #[tokio::test]
    async fn test_graph_store_roundtrip_and_batch_queries() -> Result<()> {
        let store = test_store().await?;
        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();
        let node_c = Uuid::new_v4();
        let node_d = Uuid::new_v4();

        let edge_ab = scoped_edge("user1", node_a, node_b, RelationType::RelatedTo, 0.4, "ns:u1");
        let edge_ac = scoped_edge("user1", node_a, node_c, RelationType::Supports, 0.6, "ns:u1");
        let edge_db = scoped_edge("user1", node_d, node_b, RelationType::Blocks, 0.9, "ns:u1");

        store.add_edge(&edge_ab).await?;
        store.add_edge(&edge_ac).await?;
        store.add_edge(&edge_db).await?;
        store.flush().await?;

        let outgoing = store.get_outgoing_edges("user1", node_a).await?;
        assert_eq!(outgoing.len(), 2);

        let incoming = store.get_incoming_edges("user1", node_b).await?;
        assert_eq!(incoming.len(), 2);

        let all_edges = store.get_all_edges_for_user("user1").await?;
        assert_eq!(all_edges.len(), 3);

        let scanned = store.scan_all_edges().await?;
        assert_eq!(scanned.len(), 3);

        let batch_outgoing = store
            .batch_get_outgoing_edges("user1", &[node_a, node_d, Uuid::new_v4()])
            .await?;
        assert_eq!(batch_outgoing.get(&node_a).map(Vec::len), Some(2));
        assert_eq!(batch_outgoing.get(&node_d).map(Vec::len), Some(1));

        let batch_incoming = store
            .batch_get_incoming_edges("user1", &[node_b, node_c, Uuid::new_v4()])
            .await?;
        assert_eq!(batch_incoming.get(&node_b).map(Vec::len), Some(2));
        assert_eq!(batch_incoming.get(&node_c).map(Vec::len), Some(1));

        Ok(())
    }

    #[tokio::test]
    async fn test_reinforce_edge_and_delete_edges_for_node() -> Result<()> {
        let store = test_store().await?;
        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();
        let node_c = Uuid::new_v4();

        let existing = scoped_edge("user1", node_a, node_b, RelationType::RelatedTo, 0.3, "ns:u1");
        store.add_edge(&existing).await?;
        store.flush().await?;

        store.reinforce_edge("user1", node_a, node_b, 0.5).await?;
        store.flush().await?;

        let outgoing = store.get_outgoing_edges("user1", node_a).await?;
        assert_eq!(outgoing.len(), 1);
        assert!((outgoing[0].weight - 0.8).abs() < f32::EPSILON);

        let buffered = scoped_edge("user1", node_b, node_c, RelationType::Supports, 0.2, "ns:u1");
        store.add_edge(&buffered).await?;

        let deleted = store.delete_edges_for_node("user1", node_b).await?;
        assert_eq!(deleted, 2);
        assert!(store.get_outgoing_edges("user1", node_a).await?.is_empty());
        assert!(store.get_incoming_edges("user1", node_b).await?.is_empty());
        assert!(store.get_outgoing_edges("user1", node_b).await?.is_empty());

        Ok(())
    }
}
