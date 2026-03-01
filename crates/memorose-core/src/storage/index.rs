use anyhow::Result;
use tantivy::schema::*;
use tantivy::{Index, IndexWriter, IndexReader, ReloadPolicy};
use std::path::Path;
use std::sync::{Arc, Mutex};
use memorose_common::MemoryUnit;

#[derive(Clone)]
pub struct TextIndex {
    index: Index,
    writer: Arc<Mutex<IndexWriter>>,
    reader: IndexReader,
    _commit_task: Arc<tokio::task::JoinHandle<()>>,
}

impl TextIndex {
    pub fn new<P: AsRef<Path>>(path: P, interval_ms: u64) -> Result<Self> {
        let index_path = path.as_ref();
        std::fs::create_dir_all(index_path)?;

        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("id", STRING | STORED);
        schema_builder.add_text_field("user_id", STRING | STORED);
        schema_builder.add_text_field("app_id", STRING | STORED);
        schema_builder.add_text_field("content", TEXT | STORED);
        schema_builder.add_text_field("stream_id", STRING);
        schema_builder.add_u64_field("level", INDEXED | STORED);
        schema_builder.add_i64_field("transaction_time", INDEXED | STORED | FAST);
        schema_builder.add_i64_field("valid_time", INDEXED | STORED | FAST);
        let schema = schema_builder.build();

        let index = match Index::open_or_create(tantivy::directory::MmapDirectory::open(index_path)?, schema.clone()) {
            Ok(idx) => idx,
            Err(e) => {
                // Schema incompatible - recreate index
                tracing::warn!("Tantivy schema incompatible, recreating index: {}", e);
                std::fs::remove_dir_all(index_path)?;
                std::fs::create_dir_all(index_path)?;
                Index::open_or_create(tantivy::directory::MmapDirectory::open(index_path)?, schema.clone())?
            }
        };

        // 50MB buffer
        let writer = index.writer(50_000_000)?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        let writer_arc = Arc::new(Mutex::new(writer));
        let reader_clone = reader.clone();
        let writer_clone = writer_arc.clone();

        let commit_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(interval_ms));
            interval.tick().await;

            loop {
                interval.tick().await;
                if let Ok(mut w) = writer_clone.try_lock() {
                    if let Err(e) = w.commit() {
                        tracing::error!("Background commit failed: {:?}", e);
                    }
                } else {
                    tracing::debug!("Skipping background commit (lock busy)");
                }
                if let Err(e) = reader_clone.reload() {
                    tracing::error!("Background reload failed: {:?}", e);
                }
            }
        });

        Ok(Self {
            index,
            writer: writer_arc,
            reader,
            _commit_task: Arc::new(commit_task),
        })
    }

    pub fn index_unit(&self, unit: &MemoryUnit) -> Result<()> {
        let schema = self.index.schema();
        let id_field = schema.get_field("id").unwrap();
        let user_id_field = schema.get_field("user_id").unwrap();
        let app_id_field = schema.get_field("app_id").unwrap();
        let content_field = schema.get_field("content").unwrap();
        let stream_field = schema.get_field("stream_id").unwrap();
        let level_field = schema.get_field("level").unwrap();
        let tx_time_field = schema.get_field("transaction_time").unwrap();
        let valid_time_field = schema.get_field("valid_time").unwrap();

        let mut doc = tantivy::TantivyDocument::default();
        doc.add_text(id_field, &unit.id.to_string());
        doc.add_text(user_id_field, &unit.user_id);
        doc.add_text(app_id_field, &unit.app_id);
        doc.add_text(content_field, &unit.content);
        doc.add_text(stream_field, &unit.stream_id.to_string());
        doc.add_u64(level_field, unit.level as u64);
        doc.add_i64(tx_time_field, unit.transaction_time.timestamp_micros());
        if let Some(vt) = unit.valid_time {
            doc.add_i64(valid_time_field, vt.timestamp_micros());
        }

        let writer = self.writer.lock().map_err(|_| anyhow::anyhow!("Lock poison"))?;
        writer.add_document(doc)?;
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let mut writer = self.writer.lock().map_err(|_| anyhow::anyhow!("Lock poison"))?;
        writer.commit()?;
        Ok(())
    }

    pub fn reload(&self) -> Result<()> {
        self.reader.reload()?;
        Ok(())
    }

    pub fn search(&self, query_str: &str, limit: usize, time_range: Option<memorose_common::TimeRange>, user_id: Option<&str>, app_id: Option<&str>) -> Result<Vec<String>> {
        self.search_bitemporal(query_str, limit, time_range, None, user_id, app_id)
    }

    pub fn search_bitemporal(
        &self,
        query_str: &str,
        limit: usize,
        valid_time: Option<memorose_common::TimeRange>,
        transaction_time: Option<memorose_common::TimeRange>,
        user_id: Option<&str>,
        app_id: Option<&str>,
    ) -> Result<Vec<String>> {
        let searcher = self.reader.searcher();
        let schema = self.index.schema();
        let content_field = schema.get_field("content").unwrap();
        let id_field = schema.get_field("id").unwrap();

        let query_parser = tantivy::query::QueryParser::for_index(&self.index, vec![content_field]);
        let user_query = query_parser.parse_query(query_str)?;

        let mut sub_queries: Vec<(tantivy::query::Occur, Box<dyn tantivy::query::Query>)> = vec![
            (tantivy::query::Occur::Must, user_query),
        ];

        // User ID filter
        if let Some(uid) = user_id {
            let user_id_field = schema.get_field("user_id").unwrap();
            let term = tantivy::Term::from_field_text(user_id_field, uid);
            let term_query = tantivy::query::TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);
            sub_queries.push((tantivy::query::Occur::Must, Box::new(term_query)));
        }

        // App ID filter
        if let Some(aid) = app_id {
            let app_id_field = schema.get_field("app_id").unwrap();
            let term = tantivy::Term::from_field_text(app_id_field, aid);
            let term_query = tantivy::query::TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);
            sub_queries.push((tantivy::query::Occur::Must, Box::new(term_query)));
        }

        if let Some(range) = valid_time {
            if range.start.is_some() || range.end.is_some() {
                let start = range.start.map(|t| t.timestamp_micros()).unwrap_or(i64::MIN);
                let end = range.end.map(|t| t.timestamp_micros()).unwrap_or(i64::MAX);
                let time_query = tantivy::query::RangeQuery::new_i64("valid_time".to_string(), start..end + 1);
                sub_queries.push((tantivy::query::Occur::Must, Box::new(time_query)));
            }
        }

        if let Some(range) = transaction_time {
            if range.start.is_some() || range.end.is_some() {
                let start = range.start.map(|t| t.timestamp_micros()).unwrap_or(i64::MIN);
                let end = range.end.map(|t| t.timestamp_micros()).unwrap_or(i64::MAX);
                let time_query = tantivy::query::RangeQuery::new_i64("transaction_time".to_string(), start..end + 1);
                sub_queries.push((tantivy::query::Occur::Must, Box::new(time_query)));
            }
        }

        let combined_query = tantivy::query::BooleanQuery::new(sub_queries);
        let top_docs = searcher.search(&combined_query, &tantivy::collector::TopDocs::with_limit(limit))?;

        let mut results = Vec::new();
        for (_score, doc_address) in top_docs {
            let retrieved_doc: tantivy::TantivyDocument = searcher.doc(doc_address)?;
            if let Some(val) = retrieved_doc.get_first(id_field) {
                 if let Some(s) = val.as_str() {
                     results.push(s.to_string());
                 }
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

    #[test]
    fn test_text_index() -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let temp_dir = tempdir()?;
            let index = TextIndex::new(temp_dir.path(), 1000)?;

            let stream_id = Uuid::new_v4();
            let unit = MemoryUnit::new("u1".into(), None, "a1".into(), stream_id, memorose_common::MemoryType::Factual, "The quick brown fox jumps".to_string(), None);

            index.index_unit(&unit)?;

            index.commit()?;
            index.reload()?;

            let results = index.search("fox", 10, None, None, None)?;

            assert!(!results.is_empty());
            assert_eq!(results[0], unit.id.to_string());
            Ok(())
        })
    }
}
