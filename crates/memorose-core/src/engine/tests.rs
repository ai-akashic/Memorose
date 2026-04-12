use super::*;
use super::helpers::*;
use super::types::*;
use crate::arbitrator::{Arbitrator, MemoryCorrectionAction, MemoryCorrectionKind};
use crate::fact_extraction::{
    self, MemoryFactAttribute, MemoryFactChangeType, MemoryFactDescriptor, MemoryFactSubject,
    MemoryFactValueKind, MemoryFactValuePayload,
};
use crate::llm::LLMClient;
use crate::reranker::Reranker;
use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use memorose_common::{
    Event, EventContent, ForgetMode, ForgetTargetKind, ForgettingTombstone, GraphEdge,
    MemoryDomain, MemoryType, MemoryUnit, RelationType, SharePolicy, ShareTarget, StoredMemoryFact,
    TimeRange,
};
use std::sync::Arc;
use tempfile::tempdir;
use uuid::Uuid;

const TEST_USER: &str = "test_user";
    #[tokio::test]
    async fn test_engine_integration() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        // 1. Test L0 Ingestion
        let stream_id = Uuid::new_v4();
        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Text("L0 Test".to_string()),
        );
        engine.ingest_event(event.clone()).await?;

        let pending = engine.fetch_pending_events().await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, event.id);

        let retrieved_event = engine.get_event(TEST_USER, &event.id.to_string()).await?;
        assert!(retrieved_event.is_some());
        assert_eq!(retrieved_event.unwrap().id, event.id);

        // Mark processed
        engine.mark_event_processed(&event.id.to_string()).await?;
        let pending_after = engine.fetch_pending_events().await?;
        assert!(pending_after.is_empty());

        // 2. Test L1 Storage & Retrieval
        let mut embedding = vec![0.0; 384];
        embedding[10] = 1.0;
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "L1 Insight".to_string(),
            Some(embedding.clone()),
        );

        engine.store_memory_unit(unit.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        // Search by Vector
        let filter = engine.build_user_filter(TEST_USER, None);
        let similar = engine
            .search_similar(TEST_USER, &embedding, 1, filter)
            .await?;
        assert_eq!(similar.len(), 1);
        assert_eq!(similar[0].0.id, unit.id);

        // Search by Text
        let text_hits = engine
            .search_text(TEST_USER, "Insight", 1, true, None)
            .await?;
        assert_eq!(text_hits.len(), 1);
        assert_eq!(text_hits[0].id, unit.id);

        // 3. Test Forgetting Mechanism
        let mut weak_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Weak Memory".to_string(),
            None,
        );
        weak_unit.importance = 0.15;
        engine.store_memory_unit(weak_unit.clone()).await?;

        // Decay: 0.15 * 0.5 = 0.075
        engine.decay_importance(TEST_USER, 0.5).await?;

        // Prune memories below 0.1
        let pruned_count = engine.prune_memories(TEST_USER, 0.1).await?;
        assert!(pruned_count >= 1);

        // Verify it's gone
        let search_gone = engine.search_text(TEST_USER, "Weak", 1, true, None).await?;
        assert!(search_gone.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_auto_linking() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        // 1. Store first memory
        let mut emb1 = vec![0.0; 384];
        emb1[0] = 1.0;
        let unit1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Apple is a fruit".to_string(),
            Some(emb1),
        );
        engine.store_memory_unit(unit1.clone()).await?;

        // 2. Store second similar memory
        let mut emb2 = vec![0.0; 384];
        emb2[0] = 0.99;
        let unit2 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Apples are sweet".to_string(),
            Some(emb2),
        );
        engine.store_memory_unit(unit2.clone()).await?;

        // Verify graph edge exists from unit2 to unit1
        let edges = engine
            .graph()
            .get_outgoing_edges(TEST_USER, unit2.id)
            .await?;
        assert!(!edges.is_empty(), "Edge should be automatically created");
        assert_eq!(edges[0].target_id, unit1.id);

        Ok(())
    }

    #[tokio::test]
    async fn test_conflict_arbitration() -> Result<()> {
        if std::env::var("GOOGLE_API_KEY").is_err() && std::env::var("OPENAI_API_KEY").is_err() {
            return Ok(());
        }

        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut emb1 = vec![0.0; 384];
        emb1[0] = 1.0;
        let mut unit1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I love cats".to_string(),
            Some(emb1.clone()),
        );
        unit1.transaction_time = Utc::now() - chrono::Duration::days(1);
        engine.store_memory_unit(unit1.clone()).await?;

        let mut emb2 = vec![0.0; 384];
        emb2[0] = 0.95;
        let unit2 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I hate cats now".to_string(),
            Some(emb2.clone()),
        );
        engine.store_memory_unit(unit2.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let results = engine
            .search_text(TEST_USER, "cats", 10, true, None)
            .await?;

        println!(
            "Arbitration results: {:?}",
            results.iter().map(|u| &u.content).collect::<Vec<_>>()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_community_flow() -> Result<()> {
        let has_google = std::env::var("GOOGLE_API_KEY")
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        let has_openai = std::env::var("OPENAI_API_KEY")
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        if !has_google && !has_openai {
            return Ok(());
        }

        let temp_dir = tempdir()?;
        let engine =
            match MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
                .await
            {
                Ok(e) => e,
                Err(_) => return Ok(()), // skip if backend fails to initialize
            };
        let stream_id = Uuid::new_v4();

        let mut emb1 = vec![0.0; 768];
        emb1[0] = 1.0;
        let u1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Rust is memory safe".to_string(),
            Some(emb1.clone()),
        );
        engine.store_memory_unit(u1.clone()).await?;

        let mut emb2 = vec![0.0; 768];
        emb2[0] = 0.95;
        let u2 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "The borrow checker prevents data races".to_string(),
            Some(emb2.clone()),
        );
        engine.store_memory_unit(u2.clone()).await?;

        let mut emb3 = vec![0.0; 768];
        emb3[0] = 0.90;
        let u3 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Ownership is key to Rust".to_string(),
            Some(emb3.clone()),
        );
        engine.store_memory_unit(u3.clone()).await?;

        let _ = engine.process_communities(TEST_USER).await;

        let prefix = format!("u:{}:unit:", TEST_USER);
        let kv = engine.kv_store.clone();
        let prefix_bytes = prefix.into_bytes();
        let all_units: Vec<(Vec<u8>, Vec<u8>)> =
            tokio::task::spawn_blocking(move || kv.scan(&prefix_bytes)).await??;

        let l2_units: Vec<MemoryUnit> = all_units
            .into_iter()
            .filter_map(|(_, v): (Vec<u8>, Vec<u8>)| serde_json::from_slice::<MemoryUnit>(&v).ok())
            .filter(|u| u.level == 2)
            .collect();

        if !l2_units.is_empty() {
            let l2 = &l2_units[0];
            println!(
                "Generated L2: {} - {}",
                l2.keywords.first().unwrap_or(&"No Name".to_string()),
                l2.content
            );

            assert!(l2.references.len() >= 3);
            assert!(
                !l2.keywords.is_empty(),
                "L2 unit should have keywords (at least title)"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_feedback_loop() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let u1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Memory A".into(),
            None,
        );
        let u2 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Memory B".into(),
            None,
        );
        engine.store_memory_unit(u1.clone()).await?;
        engine.store_memory_unit(u2.clone()).await?;

        engine
            .apply_reranker_feedback(
                TEST_USER,
                vec![u1.id.to_string(), u2.id.to_string()],
                vec![],
            )
            .await?;

        let edges = engine.graph().get_outgoing_edges(TEST_USER, u1.id).await?;
        let edge = edges
            .iter()
            .find(|e| e.target_id == u2.id)
            .expect("Edge should be created by reinforcement");
        assert!((edge.weight - 0.1).abs() < 0.001);

        engine
            .apply_reranker_feedback(
                TEST_USER,
                vec![u1.id.to_string(), u2.id.to_string()],
                vec![],
            )
            .await?;
        let edges_updated = engine.graph().get_outgoing_edges(TEST_USER, u1.id).await?;
        let edge_updated = edges_updated.iter().find(|e| e.target_id == u2.id).unwrap();
        assert!((edge_updated.weight - 0.2).abs() < 0.001);

        Ok(())
    }

    #[tokio::test]
    async fn test_temporal_text_search() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut u1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Memorose started in 2020".into(),
            None,
        );
        u1.valid_time =
            Some(chrono::TimeZone::with_ymd_and_hms(&Utc, 2020, 1, 1, 0, 0, 0).unwrap());
        engine.store_memory_unit(u1.clone()).await?;

        let mut u2 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Memorose is advanced in 2026".into(),
            None,
        );
        u2.valid_time =
            Some(chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 1, 1, 0, 0, 0).unwrap());
        engine.store_memory_unit(u2.clone()).await?;

        engine.index.commit()?;
        engine.index.reload()?;

        let range = TimeRange {
            start: Some(chrono::TimeZone::with_ymd_and_hms(&Utc, 2025, 1, 1, 0, 0, 0).unwrap()),
            end: Some(chrono::TimeZone::with_ymd_and_hms(&Utc, 2027, 1, 1, 0, 0, 0).unwrap()),
        };

        let hits = engine
            .search_text(TEST_USER, "Memorose", 10, false, Some(range))
            .await?;

        assert_eq!(
            hits.len(),
            1,
            "Should only return 1 hit due to time filtering"
        );
        assert_eq!(hits[0].id, u2.id);

        Ok(())
    }

    #[tokio::test]
    async fn test_search_filters() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut u1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Highly relevant".into(),
            Some(vec![1.0; 768]),
        );
        u1.importance = 1.0;
        let mut u2 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Less relevant".into(),
            Some(vec![0.5; 768]),
        );
        u2.importance = 0.5;

        engine
            .store_memory_units(vec![u1.clone(), u2.clone()])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let results = engine
            .search_hybrid(
                TEST_USER,
                None,
                None,
                "relevant",
                &vec![1.0; 768],
                10,
                false,
                Some(0.3),
                0,
                None,
                None,
            )
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.id, u1.id);

        Ok(())
    }

    #[tokio::test]
    async fn test_search_hybrid_applies_org_filter_before_ranking() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut org_unit = MemoryUnit::new(
            Some("org_alpha".into()),
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Alpha org incident playbook".into(),
            Some(vec![1.0; 768]),
        );
        org_unit.importance = 1.0;

        let mut other_unit = MemoryUnit::new(
            Some("org_beta".into()),
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Beta org incident playbook".into(),
            Some(vec![1.0; 768]),
        );
        other_unit.importance = 1.0;

        engine
            .store_memory_units(vec![org_unit.clone(), other_unit.clone()])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let results = engine
            .search_hybrid(
                TEST_USER,
                Some("org_alpha"),
                None,
                "incident playbook",
                &vec![1.0; 768],
                10,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.id, org_unit.id);

        Ok(())
    }

    struct MockReranker;
    #[async_trait::async_trait]
    impl crate::reranker::Reranker for MockReranker {
        async fn rerank(
            &self,
            _query: &str,
            _store: &KvStore,
            _candidates: Vec<(MemoryUnit, f32)>,
        ) -> Result<Vec<(MemoryUnit, f32)>> {
            Ok(vec![])
        }
        async fn apply_feedback(
            &self,
            _store: &KvStore,
            _c: Vec<String>,
            _r: Vec<String>,
        ) -> Result<()> {
            Ok(())
        }
    }

    struct MockCorrectionLLM {
        response: String,
    }

    #[async_trait::async_trait]
    impl crate::llm::LLMClient for MockCorrectionLLM {
        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 3],
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn generate(&self, _prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: self.response.clone(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<crate::llm::CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: crate::llm::CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn summarize_group(
            &self,
            texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: texts.join("\n"),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn describe_image(
            &self,
            image_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: image_url_or_base64.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn transcribe(
            &self,
            audio_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: audio_url_or_base64.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn describe_video(&self, video_url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: video_url.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }
    }

    struct PromptMatchingCorrectionLLM {
        responses: Vec<(String, String)>,
    }

    struct PanicOnGenerateLLM;

    #[async_trait::async_trait]
    impl crate::llm::LLMClient for PromptMatchingCorrectionLLM {
        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 3],
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn generate(&self, prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            let data = self
                .responses
                .iter()
                .find_map(|(needle, response)| prompt.contains(needle).then(|| response.clone()))
                .unwrap_or_else(|| "null".to_string());

            Ok(crate::llm::LLMResponse {
                data,
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<crate::llm::CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: crate::llm::CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn summarize_group(
            &self,
            texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: texts.join("\n"),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn describe_image(
            &self,
            image_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: image_url_or_base64.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn transcribe(
            &self,
            audio_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: audio_url_or_base64.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn describe_video(&self, video_url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: video_url.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }
    }

    #[async_trait::async_trait]
    impl crate::llm::LLMClient for PanicOnGenerateLLM {
        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 3],
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn generate(&self, _prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            panic!("generate should not be called when persisted extracted facts exist")
        }

        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<crate::llm::CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: crate::llm::CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn summarize_group(
            &self,
            texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: texts.join("\n"),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn describe_image(
            &self,
            image_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: image_url_or_base64.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn transcribe(
            &self,
            audio_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: audio_url_or_base64.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn describe_video(&self, video_url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: video_url.to_string(),
                usage: memorose_common::TokenUsage::default(),
            })
        }
    }

    #[tokio::test]
    async fn test_custom_reranker() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_reranker(std::sync::Arc::new(MockReranker));

        let u1 = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Test".into(),
            Some(vec![1.0; 768]),
        );
        engine.store_memory_unit(u1).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let results = engine
            .search_hybrid(
                TEST_USER,
                None,
                None,
                "Test",
                &vec![1.0; 768],
                10,
                false,
                None,
                0,
                None,
                None,
            )
            .await?;
        assert!(results.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_hard_delete_clears_forgetting_tombstone() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Delete me after logical forgetting".into(),
            Some(vec![1.0; 768]),
        );
        let unit_id = unit.id;
        engine.store_memory_unit(unit).await?;

        let tombstone = ForgettingTombstone {
            user_id: TEST_USER.into(),
            org_id: None,
            target_kind: memorose_common::ForgetTargetKind::MemoryUnit,
            target_id: unit_id.to_string(),
            reason_query: "forget this".into(),
            created_at: Utc::now(),
            preview_id: Some(Uuid::new_v4().to_string()),
            mode: memorose_common::ForgetMode::Logical,
        };
        engine.mark_memory_unit_forgotten(TEST_USER, unit_id, &tombstone)?;
        assert!(engine.is_memory_unit_forgotten(TEST_USER, unit_id)?);

        engine.delete_memory_unit_hard(TEST_USER, unit_id).await?;

        assert!(!engine.is_memory_unit_forgotten(TEST_USER, unit_id)?);
        assert!(engine.get_memory_unit(TEST_USER, unit_id).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_residence_update() -> Result<()> {
        let mut unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing.".into(),
            None,
        );
        unit.keywords = vec!["Profile".into()];

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::User,
                subject_key: "user:self".into(),
                attribute: MemoryFactAttribute::Residence,
                value: "Beijing".into(),
                canonical_value: "beijing".into(),
                value_kind: MemoryFactValueKind::City,
                value_payload: MemoryFactValuePayload::City {
                    name: "beijing".into(),
                },
                change_type: MemoryFactChangeType::Update,
                confidence: 90,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_defaults_local_fact_subject_to_user() -> Result<()>
    {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Home base: Shanghai".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::User,
                subject_key: "user:self".into(),
                attribute: MemoryFactAttribute::Residence,
                value: "Shanghai".into(),
                canonical_value: "shanghai".into(),
                value_kind: MemoryFactValueKind::City,
                value_payload: MemoryFactValuePayload::City {
                    name: "shanghai".into(),
                },
                change_type: MemoryFactChangeType::Reaffirm,
                confidence: 90,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_preference_contradiction() -> Result<()> {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I do not like sushi".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::User,
                subject_key: "user:self".into(),
                attribute: MemoryFactAttribute::Preference,
                value: "sushi".into(),
                canonical_value: "sushi".into(),
                value_kind: MemoryFactValueKind::Freeform,
                value_payload: MemoryFactValuePayload::Freeform {
                    text: "sushi".into(),
                },
                change_type: MemoryFactChangeType::Contradiction,
                confidence: 85,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_contact_with_canonical_value() -> Result<()>
    {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "My email is Dylan@Example.COM.".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::User,
                subject_key: "user:self".into(),
                attribute: MemoryFactAttribute::Contact,
                value: "Dylan@Example.COM".into(),
                canonical_value: "dylan@example.com".into(),
                value_kind: MemoryFactValueKind::Email,
                value_payload: MemoryFactValuePayload::Email {
                    address: "dylan@example.com".into(),
                },
                change_type: MemoryFactChangeType::Reaffirm,
                confidence: 80,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_skill_addition() -> Result<()> {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I also speak Japanese.".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::User,
                subject_key: "user:self".into(),
                attribute: MemoryFactAttribute::Skill,
                value: "Japanese".into(),
                canonical_value: "japanese".into(),
                value_kind: MemoryFactValueKind::SkillName,
                value_payload: MemoryFactValuePayload::SkillName {
                    name: "japanese".into(),
                },
                change_type: MemoryFactChangeType::Addition,
                confidence: 75,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_external_named_subject() -> Result<()> {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Alice lives in Beijing".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::External,
                subject_key: "external:alice".into(),
                attribute: MemoryFactAttribute::Residence,
                value: "Beijing".into(),
                canonical_value: "beijing".into(),
                value_kind: MemoryFactValueKind::City,
                value_payload: MemoryFactValuePayload::City {
                    name: "beijing".into(),
                },
                change_type: MemoryFactChangeType::Reaffirm,
                confidence: 90,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_named_organization_subject() -> Result<()>
    {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Acme Corp is based in Shanghai".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::Organization,
                subject_key: "organization:acme_corp".into(),
                attribute: MemoryFactAttribute::Residence,
                value: "Shanghai".into(),
                canonical_value: "shanghai".into(),
                value_kind: MemoryFactValueKind::City,
                value_payload: MemoryFactValuePayload::City {
                    name: "shanghai".into(),
                },
                change_type: MemoryFactChangeType::Reaffirm,
                confidence: 90,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_phone_payload() -> Result<()> {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "My phone is +1 (415) 555-2671.".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::User,
                subject_key: "user:self".into(),
                attribute: MemoryFactAttribute::Contact,
                value: "+1 (415) 555-2671".into(),
                canonical_value: "14155552671".into(),
                value_kind: MemoryFactValueKind::Phone,
                value_payload: MemoryFactValuePayload::Phone {
                    digits: "14155552671".into(),
                },
                change_type: MemoryFactChangeType::Reaffirm,
                confidence: 80,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_memory_fact_descriptor_extracts_schedule_payload() -> Result<()> {
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "The meeting moved to 2026-05-01 15:00".into(),
            None,
        );

        let fact = MemoroseEngine::detect_memory_fact(&unit);

        assert_eq!(
            fact,
            Some(MemoryFactDescriptor {
                subject: MemoryFactSubject::External,
                subject_key: "external:unknown".into(),
                attribute: MemoryFactAttribute::Schedule,
                value: "2026-05-01 15:00".into(),
                canonical_value: "2026-05-01 15:00".into(),
                value_kind: MemoryFactValueKind::DateTimeLike,
                value_payload: MemoryFactValuePayload::DateTimeLike {
                    text: "2026-05-01 15:00".into(),
                },
                change_type: MemoryFactChangeType::Update,
                confidence: 70,
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_prefers_slot_keyword_matches() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Home base: Shanghai".into(),
            None,
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_id = old_unit.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_unit, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert_eq!(candidates.first().map(|unit| unit.id), Some(old_id));
        assert!(candidates.iter().any(|unit| unit.id == old_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_prefers_persisted_candidate_facts_when_content_is_opaque(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Profile metadata sync completed".into(),
            None,
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(2);
        old_unit.extracted_facts = vec![StoredMemoryFact {
            subject: "user".into(),
            subject_ref: Some("user:self".into()),
            subject_name: None,
            attribute: "residence".into(),
            value: "Shanghai".into(),
            canonical_value: Some("shanghai".into()),
            change_type: "reaffirm".into(),
            temporal_status: Some("current".into()),
            polarity: Some("positive".into()),
            evidence_span: Some("home city is Shanghai".into()),
            confidence: 0.91,
        }];
        let old_id = old_unit.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_unit, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert_eq!(candidates.first().map(|unit| unit.id), Some(old_id));
        assert!(candidates.iter().any(|unit| unit.id == old_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_uses_llm_fact_fallback() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Home base: Shanghai".into(),
            None,
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_id = old_unit.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_unit, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            MockCorrectionLLM {
                response: r#"{"subject":"user","attribute":"residence","value":"Beijing","change_type":"update","confidence":0.92}"#
                    .into(),
            },
        ));

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Current city: Beijing".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert_eq!(candidates.first().map(|unit| unit.id), Some(old_id));
        assert!(candidates.iter().any(|unit| unit.id == old_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_multiple_llm_facts() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_residence = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Home base: Shanghai".into(),
            None,
        );
        old_residence.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_residence_id = old_residence.id;

        let mut old_contact = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "My email is old@example.com".into(),
            None,
        );
        old_contact.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_contact_id = old_contact.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_residence, old_contact, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            MockCorrectionLLM {
                response: r#"{"facts":[{"subject":"user","attribute":"residence","value":"Beijing","change_type":"update","confidence":0.93},{"subject":"user","attribute":"contact","value":"dylan@example.com","change_type":"update","confidence":0.91}]}"#
                    .into(),
            },
        ));

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I now live in Beijing and my email is dylan@example.com".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_residence_id));
        assert!(candidates.iter().any(|unit| unit.id == old_contact_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_multiple_rule_facts_bilingual(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_residence = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "我住在上海".into(),
            None,
        );
        old_residence.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_residence_id = old_residence.id;

        let mut old_contact = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "我的邮箱是 old@example.com".into(),
            None,
        );
        old_contact.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_contact_id = old_contact.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "我喜欢北京烤鸭".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_residence, old_contact, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "我现在住在北京，我的邮箱是 new@example.com".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_residence_id));
        assert!(candidates.iter().any(|unit| unit.id == old_contact_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_multi_clause_history_update(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_employment = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I work at OpenAI".into(),
            None,
        );
        old_employment.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_employment_id = old_employment.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Favorite food is ramen".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_employment, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I used to work at OpenAI, now work at Anthropic".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert_eq!(
            candidates.first().map(|unit| unit.id),
            Some(old_employment_id)
        );
        assert!(candidates.iter().any(|unit| unit.id == old_employment_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_explicit_contact_transition(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_contact = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "My email is old@example.com".into(),
            None,
        );
        old_contact.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_contact_id = old_contact.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Beijing".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_contact, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "My email changed from old@example.com to new@example.com".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert_eq!(candidates.first().map(|unit| unit.id), Some(old_contact_id));
        assert!(candidates.iter().any(|unit| unit.id == old_contact_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_same_sentence_mixed_slot_transitions(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_residence = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            None,
        );
        old_residence.transaction_time = Utc::now() - chrono::Duration::days(4);
        let old_residence_id = old_residence.id;

        let mut old_contact = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "My email is old@example.com".into(),
            None,
        );
        old_contact.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_contact_id = old_contact.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Favorite food is sushi".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_residence, old_contact, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I moved from Shanghai to Beijing and changed my email from old@example.com to new@example.com".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_residence_id));
        assert!(candidates.iter().any(|unit| unit.id == old_contact_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_long_mixed_input_with_noise(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_residence = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            None,
        );
        old_residence.transaction_time = Utc::now() - chrono::Duration::days(4);
        let old_residence_id = old_residence.id;

        let mut old_contact = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "My email is old@example.com".into(),
            None,
        );
        old_contact.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_contact_id = old_contact.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "The call is at 4pm tomorrow".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_residence, old_contact, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Can you remind me where I used to live? btw, quick update: I now live in Beijing, and my email changed from old@example.com to new@example.com. also, the call is at around 3pm tmrw lol".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_residence_id));
        assert!(candidates.iter().any(|unit| unit.id == old_contact_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_long_mixed_forget_and_update_input(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_employment = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I work at OpenAI".into(),
            None,
        );
        old_employment.transaction_time = Utc::now() - chrono::Duration::days(4);
        let old_employment_id = old_employment.id;

        let mut old_contact = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "My email is old@example.com".into(),
            None,
        );
        old_contact.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_contact_id = old_contact.id;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "The call is tomorrow at 3pm".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_employment, old_contact, unrelated_unit])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Actually, quick cleanup: I no longer work at OpenAI, and my email changed from old@example.com to new@example.com. Can you remind me about the call tomorrow?".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_employment_id));
        assert!(candidates.iter().any(|unit| unit.id == old_contact_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_plan_memory_correction_actions_supports_long_mixed_forget_and_update_input(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_employment = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I work at OpenAI".into(),
            Some(vec![1.0; 768]),
        );
        old_employment.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_employment_id = old_employment.id;

        let mut old_contact = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "My email is old@example.com".into(),
            Some(vec![1.0; 768]),
        );
        old_contact.transaction_time = Utc::now() - chrono::Duration::days(1);
        let old_contact_id = old_contact.id;

        engine
            .store_memory_units(vec![old_employment, old_contact])
            .await?;

        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            PromptMatchingCorrectionLLM {
                responses: vec![(
                    "memory correction engine".into(),
                    format!(
                        r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Employment removed","confidence":0.96}},{{"target_id":"{}","action":"OBSOLETE","reason":"Email updated","confidence":0.97}}]"#,
                        old_employment_id, old_contact_id
                    ),
                )],
            },
        ));

        let mut preview_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Actually, quick cleanup: I no longer work at OpenAI, and my email changed from old@example.com to new@example.com. Can you remind me about the call tomorrow?".into(),
            Some(vec![1.0; 768]),
        );
        preview_unit.transaction_time = Utc::now();

        let actions = engine
            .plan_memory_correction_actions(&preview_unit, 8)
            .await?;

        assert!(actions.iter().any(|action| {
            action.target_id == old_employment_id
                && action.kind == MemoryCorrectionKind::Obsolete
                && action.effect == RacDecisionEffect::Tombstone
                && action.relation == Some(RelationType::EvolvedTo)
        }));
        assert!(actions.iter().any(|action| {
            action.target_id == old_contact_id
                && action.kind == MemoryCorrectionKind::Obsolete
                && action.effect == RacDecisionEffect::Tombstone
                && action.relation == Some(RelationType::EvolvedTo)
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_long_mixed_forget_and_addition_input(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_employment = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I work at OpenAI".into(),
            None,
        );
        old_employment.transaction_time = Utc::now() - chrono::Duration::days(4);
        let old_employment_id = old_employment.id;

        let mut unrelated_preference = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I love ramen".into(),
            None,
        );
        unrelated_preference.transaction_time = Utc::now() - chrono::Duration::days(1);

        engine
            .store_memory_units(vec![old_employment, unrelated_preference])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Actually, I no longer work at OpenAI. I also speak Japanese. I also love skiing."
                .into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_employment_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_self_correction_reversal_input(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_residence = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            None,
        );
        old_residence.transaction_time = Utc::now() - chrono::Duration::days(3);
        let old_residence_id = old_residence.id;

        engine.store_memory_unit(old_residence).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Shanghai. Actually, scratch that, I now live in Singapore.".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_residence_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_ignores_non_assertive_hypothetical_input(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_residence = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Beijing".into(),
            None,
        );
        old_residence.transaction_time = Utc::now() - chrono::Duration::days(3);

        engine.store_memory_unit(old_residence).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut hypothetical_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "If I move to Beijing next month, remind me to update my profile.".into(),
            None,
        );
        hypothetical_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&hypothetical_unit, 4)
            .await?;

        assert!(candidates.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_reported_speech_subject_attribution(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_residence = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "John Doe lives in Shanghai".into(),
            None,
        );
        old_residence.transaction_time = Utc::now() - chrono::Duration::days(5);
        let old_residence_id = old_residence.id;

        engine.store_memory_unit(old_residence).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "John Doe said \"I now live in Beijing\"".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_residence_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_memory_correction_candidates_supports_according_to_subject_carry(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_employment = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "John Doe works at Anthropic".into(),
            None,
        );
        old_employment.transaction_time = Utc::now() - chrono::Duration::days(6);
        let old_employment_id = old_employment.id;

        engine.store_memory_unit(old_employment).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "According to John Doe, he now works at OpenAI".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let candidates = engine
            .fetch_memory_correction_candidates(&new_unit, 4)
            .await?;

        assert!(candidates.iter().any(|unit| unit.id == old_employment_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_plan_memory_correction_actions_supports_long_mixed_forget_and_addition_input(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_employment = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I work at OpenAI".into(),
            Some(vec![1.0; 768]),
        );
        old_employment.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_employment_id = old_employment.id;

        engine.store_memory_unit(old_employment).await?;
        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            PromptMatchingCorrectionLLM {
                responses: vec![(
                    "memory correction engine".into(),
                    format!(
                        r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Employment removed","confidence":0.96}}]"#,
                        old_employment_id
                    ),
                )],
            },
        ));

        let mut preview_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Actually, I no longer work at OpenAI. I also speak Japanese. I also love skiing."
                .into(),
            Some(vec![1.0; 768]),
        );
        preview_unit.transaction_time = Utc::now();

        let actions = engine
            .plan_memory_correction_actions(&preview_unit, 8)
            .await?;

        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].target_id, old_employment_id);
        assert_eq!(actions[0].kind, MemoryCorrectionKind::Obsolete);
        assert_eq!(actions[0].effect, RacDecisionEffect::Tombstone);

        Ok(())
    }

    #[tokio::test]
    async fn test_rac_metric_snapshot_tracks_extraction_actions_and_tombstones() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Home base: Shanghai".into(),
            None,
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_id = old_unit.id;
        engine.store_memory_unit(old_unit).await?;

        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            PromptMatchingCorrectionLLM {
                responses: vec![
                    (
                        "memory fact extraction engine".into(),
                        r#"{"subject":"user","attribute":"residence","value":"Beijing","change_type":"update","confidence":0.92}"#
                            .into(),
                    ),
                    (
                        "memory correction engine".into(),
                        format!(
                            r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Residence updated","confidence":0.96}}]"#,
                            old_id
                        ),
                    ),
                ],
            },
        ));

        let before = engine.get_rac_metric_snapshot()?;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Current city: Beijing".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let affected = engine.reconcile_conflicting_memory_unit(&new_unit).await?;
        let after = engine.get_rac_metric_snapshot()?;
        let history = engine.get_rac_metric_history(4)?;

        assert_eq!(affected, vec![old_id]);
        assert!(
            after.fact_extraction_attempt_total > before.fact_extraction_attempt_total,
            "expected extraction attempts to increase"
        );
        assert!(
            after.fact_extraction_success_total > before.fact_extraction_success_total,
            "expected extraction successes to increase"
        );
        assert_eq!(
            after.correction_action_obsolete_total,
            before.correction_action_obsolete_total + 1
        );
        assert_eq!(after.tombstone_total, before.tombstone_total + 1);
        assert!(history.iter().any(|point| {
            point.fact_extraction_attempt_total > 0
                && point.correction_action_obsolete_total > 0
                && point.tombstone_total > 0
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_fact_descriptors_compatible_matches_best_multi_fact_pair() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            PromptMatchingCorrectionLLM {
                responses: vec![
                    (
                        "I now live in Beijing and my email is dylan@example.com".into(),
                        r#"{"facts":[{"subject":"user","attribute":"residence","value":"Beijing","change_type":"update","confidence":0.93},{"subject":"user","attribute":"contact","value":"dylan@example.com","change_type":"update","confidence":0.96}]}"#
                            .into(),
                    ),
                    (
                        "Favorite food is sushi and my email is old@example.com".into(),
                        r#"{"facts":[{"subject":"user","attribute":"preference","value":"sushi","change_type":"reaffirm","confidence":0.87},{"subject":"user","attribute":"contact","value":"old@example.com","change_type":"historical","confidence":0.92}]}"#
                            .into(),
                    ),
                ],
            },
        ));

        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing and my email is dylan@example.com".into(),
            None,
        );
        let target_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Favorite food is sushi and my email is old@example.com".into(),
            None,
        );

        let (source_fact, target_fact) = engine
            .resolve_fact_descriptors_compatible(&new_unit, &target_unit)
            .await
            .expect("expected a compatible fact pair");

        assert_eq!(source_fact.attribute, MemoryFactAttribute::Contact);
        assert_eq!(target_fact.attribute, MemoryFactAttribute::Contact);
        assert_eq!(source_fact.subject, MemoryFactSubject::User);
        assert_eq!(target_fact.subject, MemoryFactSubject::User);

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_memory_fact_descriptors_prefers_persisted_extracted_facts() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        engine.arbitrator =
            crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(PanicOnGenerateLLM));

        let mut unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Acme Corp previously worked with John Doe".into(),
            None,
        );
        unit.extracted_facts = vec![StoredMemoryFact {
            subject: "organization".into(),
            subject_ref: Some("organization:acme_corp".into()),
            subject_name: Some("Acme Corp".into()),
            attribute: "relationship".into(),
            value: "John Doe".into(),
            canonical_value: Some("john doe".into()),
            change_type: "reaffirm".into(),
            temporal_status: Some("current".into()),
            polarity: Some("positive".into()),
            evidence_span: Some("Acme Corp worked with John Doe".into()),
            confidence: 0.89,
        }];

        let descriptors = engine.resolve_memory_fact_descriptors(&unit).await;

        assert_eq!(descriptors.len(), 1);
        assert_eq!(descriptors[0].subject, MemoryFactSubject::Organization);
        assert_eq!(descriptors[0].subject_key, "organization:acme_corp");
        assert_eq!(descriptors[0].attribute, MemoryFactAttribute::Relationship);
        assert_eq!(
            descriptors[0].value_payload,
            MemoryFactValuePayload::PersonName {
                name: "john doe".into()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_memory_fact_descriptors_extracts_multiple_rule_facts() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing and my email is dylan@example.com".into(),
            None,
        );

        let descriptors = engine.resolve_memory_fact_descriptors(&unit).await;

        assert_eq!(descriptors.len(), 2);
        assert!(descriptors
            .iter()
            .any(|fact| fact.attribute == MemoryFactAttribute::Residence));
        assert!(descriptors
            .iter()
            .any(|fact| fact.attribute == MemoryFactAttribute::Contact));

        Ok(())
    }

    #[tokio::test]
    async fn test_hydrate_memory_unit_extracted_facts_populates_rule_facts() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing and my email is dylan@example.com".into(),
            None,
        );

        engine.hydrate_memory_unit_extracted_facts(&mut unit).await;

        assert!(unit.extracted_facts.len() >= 2);
        assert!(unit
            .extracted_facts
            .iter()
            .any(|fact| fact.attribute == "residence"));
        assert!(unit
            .extracted_facts
            .iter()
            .any(|fact| fact.attribute == "contact"));

        Ok(())
    }

    #[tokio::test]
    async fn test_reconcile_conflicting_memory_unit_uses_slot_aware_candidates() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Home base: Shanghai".into(),
            None,
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(2);
        let old_id = old_unit.id;
        engine.store_memory_unit(old_unit).await?;

        let mut unrelated_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            None,
        );
        unrelated_unit.transaction_time = Utc::now() - chrono::Duration::days(1);
        engine.store_memory_unit(unrelated_unit).await?;

        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            MockCorrectionLLM {
                response: format!(
                    r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Residence updated","confidence":0.94}}]"#,
                    old_id
                ),
            },
        ));

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            None,
        );
        new_unit.transaction_time = Utc::now();

        let affected = engine.reconcile_conflicting_memory_unit(&new_unit).await?;

        assert_eq!(affected, vec![old_id]);
        assert!(engine.is_memory_unit_forgotten(TEST_USER, old_id)?);

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_tombstones_target_and_links_relation(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;
        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Address updated".into(),
                    confidence: 0.95,
                }],
            )
            .await?;

        assert_eq!(affected, vec![old_id]);
        assert!(engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine.get_memory_unit(TEST_USER, old_id).await?.is_none());

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::EvolvedTo));

        Ok(())
    }

    #[tokio::test]
    async fn test_plan_memory_correction_actions_returns_validated_preview() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(1);
        let old_id = old_unit.id;

        engine.store_memory_unit(old_unit).await?;
        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            PromptMatchingCorrectionLLM {
                responses: vec![(
                    "memory correction engine".into(),
                    format!(
                        r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Residence updated","confidence":0.96}}]"#,
                        old_id
                    ),
                )],
            },
        ));

        let mut preview_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        preview_unit.transaction_time = Utc::now();

        let actions = engine
            .plan_memory_correction_actions(&preview_unit, 8)
            .await?;

        let action = actions
            .into_iter()
            .find(|action| action.target_id == old_id)
            .expect("expected planned correction action");
        assert_eq!(action.kind, MemoryCorrectionKind::Obsolete);
        assert_eq!(action.effect, RacDecisionEffect::Tombstone);
        assert_eq!(action.relation, Some(RelationType::EvolvedTo));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_keeps_contradicting_target_visible() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I have never lived in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;
        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Contradicts,
                    reason: "Conflicting claim".into(),
                    confidence: 0.82,
                }],
            )
            .await?;

        assert_eq!(affected, vec![old_id]);
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine.get_memory_unit(TEST_USER, old_id).await?.is_some());

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::Contradicts));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_skips_obsolete_for_mismatched_slots() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;
        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Incorrect cross-slot overwrite".into(),
                    confidence: 0.9,
                }],
            )
            .await?;

        assert!(affected.is_empty());
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine.get_memory_unit(TEST_USER, old_id).await?.is_some());

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(!outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::EvolvedTo));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_skips_obsolete_for_different_external_subjects(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Alice lives in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Bob now lives in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;
        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Different person should not overwrite".into(),
                    confidence: 0.9,
                }],
            )
            .await?;

        assert!(affected.is_empty());
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine.get_memory_unit(TEST_USER, old_id).await?.is_some());

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(!outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::EvolvedTo));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_skips_low_confidence_obsolete() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(1);
        let old_id = old_unit.id;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        new_unit.transaction_time = Utc::now();
        let new_id = new_unit.id;

        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Low confidence replacement".into(),
                    confidence: 0.62,
                }],
            )
            .await?;

        assert!(affected.is_empty());
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(!outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::EvolvedTo));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_downgrades_medium_confidence_obsolete_to_relation_only(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(1);
        let old_id = old_unit.id;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        new_unit.transaction_time = Utc::now();
        let new_id = new_unit.id;

        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Needs review before tombstone".into(),
                    confidence: 0.78,
                }],
            )
            .await?;

        assert_eq!(affected, vec![old_id]);
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::EvolvedTo));

        let decisions = engine.list_recent_rac_decisions(8)?;
        let recent = decisions
            .into_iter()
            .find(|decision| {
                decision.source_unit_id == new_id
                    && decision.target_unit_id == Some(old_id)
                    && decision.action == "obsolete"
            })
            .expect("expected rac decision record");
        assert_eq!(recent.effect, RacDecisionEffect::RelationOnly);
        assert_eq!(
            recent.guard_reason.as_deref(),
            Some("obsolete_relation_only_due_to_confidence")
        );

        let reviews =
            engine.list_rac_reviews(Some(RacReviewStatus::Pending), Some(TEST_USER), None, 8)?;
        let review = reviews
            .into_iter()
            .find(|review| {
                review.source_unit_id == new_id
                    && review.target_unit_id == old_id
                    && review.action == "obsolete"
            })
            .expect("expected pending rac review record");
        assert_eq!(review.stage, "post_store");
        assert_eq!(review.status, RacReviewStatus::Pending);
        assert_eq!(
            review.guard_reason.as_deref(),
            Some("obsolete_relation_only_due_to_confidence")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_rac_review_approval_tombstones_target() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(1);
        let old_id = old_unit.id;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        new_unit.transaction_time = Utc::now();
        let new_id = new_unit.id;

        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Needs review before tombstone".into(),
                    confidence: 0.78,
                }],
            )
            .await?;

        let review = engine
            .list_rac_reviews(Some(RacReviewStatus::Pending), Some(TEST_USER), None, 8)?
            .into_iter()
            .find(|review| review.source_unit_id == new_id && review.target_unit_id == old_id)
            .expect("expected pending review");

        let resolved = engine
            .resolve_rac_review(
                &review.review_id,
                true,
                Some("qa-reviewer".into()),
                Some("approved after inspection".into()),
            )
            .await?
            .expect("review should resolve");

        assert_eq!(resolved.status, RacReviewStatus::Approved);
        assert_eq!(resolved.reviewer.as_deref(), Some("qa-reviewer"));
        assert_eq!(
            resolved.reviewer_note.as_deref(),
            Some("approved after inspection")
        );
        assert!(engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine
            .list_rac_reviews(Some(RacReviewStatus::Pending), Some(TEST_USER), None, 8)?
            .is_empty());

        let decisions = engine.list_recent_rac_decisions(16)?;
        let approval_decision = decisions
            .into_iter()
            .find(|decision| {
                decision.stage == "review_approve"
                    && decision.source_unit_id == new_id
                    && decision.target_unit_id == Some(old_id)
                    && decision.action == "obsolete"
            })
            .expect("expected review approval decision");
        assert_eq!(approval_decision.effect, RacDecisionEffect::Tombstone);

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_rac_review_rejection_keeps_target_visible() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        old_unit.transaction_time = Utc::now() - chrono::Duration::days(1);
        let old_id = old_unit.id;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        new_unit.transaction_time = Utc::now();
        let new_id = new_unit.id;

        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Needs review before tombstone".into(),
                    confidence: 0.78,
                }],
            )
            .await?;

        let review = engine
            .list_rac_reviews(Some(RacReviewStatus::Pending), Some(TEST_USER), None, 8)?
            .into_iter()
            .find(|review| review.source_unit_id == new_id && review.target_unit_id == old_id)
            .expect("expected pending review");

        let resolved = engine
            .resolve_rac_review(
                &review.review_id,
                false,
                Some("qa-reviewer".into()),
                Some("rejected".into()),
            )
            .await?
            .expect("review should resolve");

        assert_eq!(resolved.status, RacReviewStatus::Rejected);
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine
            .list_rac_reviews(Some(RacReviewStatus::Pending), Some(TEST_USER), None, 8)?
            .is_empty());
        assert!(engine
            .list_rac_reviews(Some(RacReviewStatus::Rejected), Some(TEST_USER), None, 8)?
            .into_iter()
            .any(|review| review.review_id == resolved.review_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_manual_memory_correction_supports_manual_contradicts() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I have never lived in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;

        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_manual_memory_correction(
                TEST_USER,
                new_id,
                old_id,
                MemoryCorrectionKind::Contradicts,
                "manual contradiction".into(),
                0.86,
                "manual_api",
            )
            .await?;

        assert_eq!(affected, vec![old_id]);
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::Contradicts));

        let decisions = engine.list_recent_rac_decisions(8)?;
        let recent = decisions
            .into_iter()
            .find(|decision| {
                decision.stage == "manual_api"
                    && decision.source_unit_id == new_id
                    && decision.target_unit_id == Some(old_id)
                    && decision.action == "contradicts"
            })
            .expect("expected manual correction decision");
        assert_eq!(recent.effect, RacDecisionEffect::RelationOnly);

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_skips_obsolete_when_target_is_newer() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        old_unit.transaction_time = Utc::now() + chrono::Duration::minutes(5);
        let old_id = old_unit.id;

        let mut new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        new_unit.transaction_time = Utc::now();
        let new_id = new_unit.id;

        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Should not obsolete newer memory".into(),
                    confidence: 0.97,
                }],
            )
            .await?;

        assert!(affected.is_empty());
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(!outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::EvolvedTo));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_uses_llm_fact_fallback_for_obsolete() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let mut engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Current city: Beijing".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;
        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;
        engine.arbitrator = crate::arbitrator::Arbitrator::with_client(std::sync::Arc::new(
            MockCorrectionLLM {
                response: r#"{"subject":"user","attribute":"residence","value":"Beijing","change_type":"update","confidence":0.92}"#
                    .into(),
            },
        ));

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Obsolete,
                    reason: "Residence updated".into(),
                    confidence: 0.95,
                }],
            )
            .await?;

        assert_eq!(affected, vec![old_id]);
        assert!(engine.is_memory_unit_forgotten(TEST_USER, old_id)?);

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(outgoing
            .iter()
            .any(|edge| edge.target_id == old_id && edge.relation == RelationType::EvolvedTo));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_ignores_reaffirm_action() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I still live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;
        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Reaffirm,
                    reason: "Same fact".into(),
                    confidence: 0.7,
                }],
            )
            .await?;

        assert!(affected.is_empty());
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine.get_memory_unit(TEST_USER, old_id).await?.is_some());

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(!outgoing.iter().any(|edge| {
            edge.target_id == old_id
                && matches!(
                    edge.relation,
                    RelationType::EvolvedTo | RelationType::Contradicts
                )
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_memory_correction_actions_ignores_ignore_action() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let old_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            Some(vec![1.0; 768]),
        );
        let old_id = old_unit.id;
        let new_unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            Some(vec![1.0; 768]),
        );
        let new_id = new_unit.id;
        engine
            .store_memory_units(vec![old_unit, new_unit.clone()])
            .await?;

        let affected = engine
            .apply_memory_correction_actions(
                &new_unit,
                vec![MemoryCorrectionAction {
                    target_id: old_id,
                    kind: MemoryCorrectionKind::Ignore,
                    reason: "Unrelated candidate".into(),
                    confidence: 0.4,
                }],
            )
            .await?;

        assert!(affected.is_empty());
        assert!(!engine.is_memory_unit_forgotten(TEST_USER, old_id)?);
        assert!(engine.get_memory_unit(TEST_USER, old_id).await?.is_some());

        let outgoing = engine.graph.get_outgoing_edges(TEST_USER, new_id).await?;
        assert!(!outgoing.iter().any(|edge| {
            edge.target_id == old_id
                && matches!(
                    edge.relation,
                    RelationType::EvolvedTo | RelationType::Contradicts
                )
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrency_progress_update() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        // 1. Create parent L2
        let mut parent = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Parent Task".into(),
            None,
        );
        parent.level = 2;
        parent.task_metadata = Some(memorose_common::TaskMetadata {
            status: memorose_common::TaskStatus::InProgress,
            progress: 0.0,
        });
        let parent_id = parent.id;
        engine.store_memory_unit(parent).await?;

        // 2. Create 10 children L1s and link them
        for i in 0..10 {
            let mut child = MemoryUnit::new(
                None,
                TEST_USER.into(),
                None,
                stream_id,
                memorose_common::MemoryType::Factual,
                format!("Child {}", i),
                None,
            );
            child.level = 1;
            child.task_metadata = Some(memorose_common::TaskMetadata {
                status: memorose_common::TaskStatus::Completed,
                progress: 1.0,
            });
            child.references.push(parent_id);
            engine.store_memory_unit(child).await?;
        }

        // 3. Simulate concurrent updates using the worker logic
        use crate::worker::BackgroundWorker;
        let worker = std::sync::Arc::new(BackgroundWorker::new(engine.clone()));
        let mut handles = Vec::new();

        for _ in 0..20 {
            let worker_clone = worker.clone();
            let pid = parent_id;
            handles.push(tokio::spawn(async move {
                worker_clone.update_parent_progress(TEST_USER, pid).await
            }));
        }

        for h in handles {
            h.await.unwrap().expect("Concurrent update failed");
        }

        // 4. Verify final progress
        let updated_parent = engine.get_memory_unit(TEST_USER, parent_id).await?.unwrap();
        let meta = updated_parent.task_metadata.unwrap();

        assert!((meta.progress - 1.0).abs() < 0.001);
        assert_eq!(meta.status, memorose_common::TaskStatus::Completed);

        Ok(())
    }

    #[tokio::test]
    async fn test_user_isolation() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        // Store memory for user A
        let unit_a = MemoryUnit::new(
            None,
            "user_a".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Secret of user A".into(),
            None,
        );
        engine.store_memory_unit(unit_a.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        // Store memory for user B
        let unit_b = MemoryUnit::new(
            None,
            "user_b".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Secret of user B".into(),
            None,
        );
        engine.store_memory_unit(unit_b.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        // User A should only see their own data
        let results_a = engine
            .search_text("user_a", "Secret", 10, false, None)
            .await?;
        assert_eq!(results_a.len(), 1);
        assert_eq!(results_a[0].user_id, "user_a");

        // User B should only see their own data
        let results_b = engine
            .search_text("user_b", "Secret", 10, false, None)
            .await?;
        assert_eq!(results_b.len(), 1);
        assert_eq!(results_b[0].user_id, "user_b");

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_event_failed_clears_retry_state() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("retry me".into()),
        );
        let event_id = event.id.to_string();
        engine.ingest_event_directly(event).await?;

        assert_eq!(
            engine.increment_retry_count_if_pending(&event_id).await?,
            Some(1)
        );
        assert_eq!(engine.get_retry_count(&event_id).await?, 1);

        engine
            .mark_event_failed(&event_id, "simulated failure")
            .await?;

        assert_eq!(engine.get_retry_count(&event_id).await?, 0);
        assert_eq!(
            engine.increment_retry_count_if_pending(&event_id).await?,
            None
        );
        assert!(engine.fetch_pending_events().await?.is_empty());
        let failed_key = format!("failed:{}", event_id);
        assert!(engine.system_kv().get(failed_key.as_bytes())?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_pending_events_sorts_by_transaction_time() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut later = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("later".into()),
        );
        later.transaction_time = Utc::now() + chrono::Duration::seconds(30);

        let mut earlier = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("earlier".into()),
        );
        earlier.transaction_time = Utc::now() - chrono::Duration::seconds(30);

        engine.ingest_event_directly(later.clone()).await?;
        engine.ingest_event_directly(earlier.clone()).await?;

        let pending = engine.fetch_pending_events_limited(10).await?;
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].id, earlier.id);
        assert_eq!(pending[1].id, later.id);

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_pending_events_marks_orphaned_entries_failed() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let orphan_id = Uuid::new_v4().to_string();
        let pending_key = format!("pending:{}", orphan_id);
        let pending_val = serde_json::to_vec(&serde_json::json!({
            "user_id": TEST_USER
        }))?;
        engine
            .system_kv()
            .put(pending_key.as_bytes(), &pending_val)?;

        let pending = engine.fetch_pending_events_limited(10).await?;
        assert!(pending.is_empty());
        let failed_key = format!("failed:{}", orphan_id);
        assert!(engine.system_kv().get(failed_key.as_bytes())?.is_some());
        assert!(engine.system_kv().get(pending_key.as_bytes())?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_pending_events_limit_zero_short_circuits() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("queued".into()),
        );
        engine.ingest_event_directly(event).await?;

        let pending = engine.fetch_pending_events_limited(0).await?;
        assert!(pending.is_empty());
        assert_eq!(engine.count_pending_events().await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_pending_events_limited_respects_limit() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut first = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("first".into()),
        );
        first.id = Uuid::from_u128(1);

        let mut second = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("second".into()),
        );
        second.id = Uuid::from_u128(2);

        engine.ingest_event_directly(first.clone()).await?;
        engine.ingest_event_directly(second.clone()).await?;

        let pending = engine.fetch_pending_events_limited(1).await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, first.id);
        assert_eq!(engine.count_pending_events().await?, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_pending_events_ignores_nonstandard_pending_keys() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        engine.system_kv().put(b"pending:bad:marker", b"{}")?;

        let pending = engine.fetch_pending_events_limited(10).await?;
        assert!(pending.is_empty());
        assert!(engine.system_kv().get(b"pending:bad:marker")?.is_some());
        assert!(engine.system_kv().get(b"failed:bad")?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_pending_events_marks_malformed_and_missing_user_metadata_failed(
    ) -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let malformed_id = Uuid::new_v4().to_string();
        engine
            .system_kv()
            .put(format!("pending:{malformed_id}").as_bytes(), b"{bad-json")?;

        let missing_user_id = Uuid::new_v4().to_string();
        engine.system_kv().put(
            format!("pending:{missing_user_id}").as_bytes(),
            &serde_json::to_vec(&serde_json::json!({"other":"value"}))?,
        )?;

        let empty_metadata = Uuid::new_v4().to_string();
        engine
            .system_kv()
            .put(format!("pending:{empty_metadata}").as_bytes(), b"")?;

        let pending = engine.fetch_pending_events_limited(10).await?;
        assert!(pending.is_empty());

        for event_id in [&malformed_id, &missing_user_id, &empty_metadata] {
            let failed_key = format!("failed:{event_id}");
            let failed = engine
                .system_kv()
                .get(failed_key.as_bytes())?
                .expect("failed marker should exist");
            let failed_json: serde_json::Value = serde_json::from_slice(&failed)?;
            let error = failed_json["error"].as_str().unwrap_or("");
            assert!(
                error.contains("Pending metadata") || error.contains("Malformed pending metadata")
            );
            assert!(engine
                .system_kv()
                .get(format!("pending:{event_id}").as_bytes())?
                .is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_event_clears_pending_retry_failed_and_forget_markers() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("to delete".into()),
        );
        let event_id = event.id.to_string();
        engine.ingest_event_directly(event.clone()).await?;
        assert_eq!(engine.count_pending_events().await?, 1);
        assert_eq!(
            engine.increment_retry_count_if_pending(&event_id).await?,
            Some(1)
        );

        engine.system_kv().put(
            format!("failed:{event_id}").as_bytes(),
            &serde_json::to_vec(&serde_json::json!({"error":"boom"}))?,
        )?;
        engine.mark_event_forgotten(
            TEST_USER,
            &event_id,
            &ForgettingTombstone {
                user_id: TEST_USER.into(),
                org_id: None,
                target_kind: ForgetTargetKind::Event,
                target_id: event_id.clone(),
                reason_query: "cleanup".into(),
                created_at: Utc::now(),
                preview_id: None,
                mode: ForgetMode::Logical,
            },
        )?;

        assert!(engine.is_event_forgotten(TEST_USER, &event_id)?);
        engine.delete_event(TEST_USER, &event_id).await?;

        assert_eq!(engine.count_pending_events().await?, 0);
        assert_eq!(engine.get_retry_count(&event_id).await?, 0);
        assert!(engine.get_event(TEST_USER, &event_id).await?.is_none());
        assert!(!engine.is_event_forgotten(TEST_USER, &event_id)?);
        assert!(engine
            .system_kv()
            .get(format!("failed:{event_id}").as_bytes())?
            .is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_retry_count_invalid_payload_defaults_to_zero() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        engine.system_kv().put(b"retry_count:broken", &[1, 2])?;
        assert_eq!(engine.get_retry_count("broken").await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_event_hides_and_restores_forgotten_event() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("forgettable".into()),
        );
        let event_id = event.id.to_string();
        engine.ingest_event_directly(event.clone()).await?;

        engine.mark_event_forgotten(
            TEST_USER,
            &event_id,
            &ForgettingTombstone {
                user_id: TEST_USER.into(),
                org_id: None,
                target_kind: ForgetTargetKind::Event,
                target_id: event_id.clone(),
                reason_query: "hide".into(),
                created_at: Utc::now(),
                preview_id: None,
                mode: ForgetMode::Logical,
            },
        )?;

        assert!(engine.get_event(TEST_USER, &event_id).await?.is_none());
        engine.clear_event_forgotten(TEST_USER, &event_id)?;
        assert!(engine.get_event(TEST_USER, &event_id).await?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_org_policy_and_backfill_status_default_invalid_and_valid_payloads() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let org_id = "org-alpha";

        assert_eq!(
            engine.get_org_share_policy(TEST_USER, org_id)?,
            SharePolicy::default()
        );

        let policy_key = MemoroseEngine::org_share_policy_key(TEST_USER, org_id);
        engine.system_kv().put(policy_key.as_bytes(), b"not-json")?;
        assert_eq!(
            engine.get_org_share_policy(TEST_USER, org_id)?,
            SharePolicy::default()
        );

        assert_eq!(engine.get_org_backfill_status(TEST_USER, org_id)?, None);

        let status_key =
            MemoroseEngine::backfill_status_key(&MemoryDomain::Organization, TEST_USER, org_id);
        engine.system_kv().put(status_key.as_bytes(), b"not-json")?;
        assert_eq!(engine.get_org_backfill_status(TEST_USER, org_id)?, None);

        let status = serde_json::json!({"state":"completed","processed":3});
        engine
            .system_kv()
            .put(status_key.as_bytes(), &serde_json::to_vec(&status)?)?;
        assert_eq!(
            engine.get_org_backfill_status(TEST_USER, org_id)?,
            Some(status)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_reflect_on_session_creates_l2_topics_and_graph_edges() -> Result<()> {
        let temp_dir = tempdir()?;
        let stream_id = Uuid::new_v4();
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(Arc::new(
                MockCorrectionLLM {
                    response: format!(
                        r#"[{{"summary":"Residence topic","source_ids":["{}","{}"]}}]"#,
                        Uuid::from_u128(11),
                        Uuid::from_u128(12)
                    ),
                },
            )));

        let mut first = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Beijing".into(),
            None,
        );
        first.id = Uuid::from_u128(11);
        let mut second = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "My office is in Chaoyang".into(),
            None,
        );
        second.id = Uuid::from_u128(12);

        engine
            .store_memory_units(vec![first.clone(), second.clone()])
            .await?;
        engine.reflect_on_session(TEST_USER, stream_id).await?;

        let prefix = format!("u:{}:unit:", TEST_USER);
        let kv = engine.kv_store.clone();
        let prefix_bytes = prefix.into_bytes();
        let all_units: Vec<(Vec<u8>, Vec<u8>)> =
            tokio::task::spawn_blocking(move || kv.scan(&prefix_bytes)).await??;

        let l2s: Vec<MemoryUnit> = all_units
            .into_iter()
            .filter_map(|(_, v)| serde_json::from_slice::<MemoryUnit>(&v).ok())
            .filter(|unit| unit.level == 2)
            .collect();
        assert_eq!(l2s.len(), 1);
        assert_eq!(l2s[0].content, "Residence topic");
        assert_eq!(l2s[0].embedding.as_deref(), Some(&[0.0, 0.0, 0.0][..]));

        let outgoing = engine
            .graph()
            .get_outgoing_edges(TEST_USER, l2s[0].id)
            .await?;
        assert_eq!(outgoing.len(), 2);
        assert!(outgoing.iter().any(|edge| edge.target_id == first.id));
        assert!(outgoing.iter().any(|edge| edge.target_id == second.id));

        Ok(())
    }

    #[tokio::test]
    async fn test_reflect_on_user_window_batches_across_streams() -> Result<()> {
        let temp_dir = tempdir()?;
        let older_stream_id = Uuid::new_v4();
        let newer_stream_id = Uuid::new_v4();
        let now = Utc::now();
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(Arc::new(
                MockCorrectionLLM {
                    response: format!(
                        r#"[{{"summary":"Cross-stream topic","source_ids":["{}","{}"]}}]"#,
                        Uuid::from_u128(21),
                        Uuid::from_u128(22)
                    ),
                },
            )));

        let mut older = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            older_stream_id,
            memorose_common::MemoryType::Factual,
            "I live in Beijing".into(),
            None,
        );
        older.id = Uuid::from_u128(21);
        older.transaction_time = now - chrono::Duration::seconds(10);

        let mut newer = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            newer_stream_id,
            memorose_common::MemoryType::Factual,
            "My office is in Chaoyang".into(),
            None,
        );
        newer.id = Uuid::from_u128(22);
        newer.transaction_time = now;

        engine
            .store_memory_units(vec![older.clone(), newer.clone()])
            .await?;
        let created_topics = engine.reflect_on_user_window(TEST_USER, 10).await?;
        assert_eq!(created_topics, 1);

        let prefix = format!("u:{}:unit:", TEST_USER);
        let kv = engine.kv_store.clone();
        let prefix_bytes = prefix.into_bytes();
        let all_units: Vec<(Vec<u8>, Vec<u8>)> =
            tokio::task::spawn_blocking(move || kv.scan(&prefix_bytes)).await??;

        let l2s: Vec<MemoryUnit> = all_units
            .into_iter()
            .filter_map(|(_, v)| serde_json::from_slice::<MemoryUnit>(&v).ok())
            .filter(|unit| unit.level == 2)
            .collect();
        assert_eq!(l2s.len(), 1);
        assert_eq!(l2s[0].content, "Cross-stream topic");
        assert_eq!(l2s[0].stream_id, newer_stream_id);
        assert_eq!(l2s[0].embedding.as_deref(), Some(&[0.0, 0.0, 0.0][..]));

        let outgoing = engine
            .graph()
            .get_outgoing_edges(TEST_USER, l2s[0].id)
            .await?;
        assert_eq!(outgoing.len(), 2);
        assert!(outgoing.iter().any(|edge| edge.target_id == older.id));
        assert!(outgoing.iter().any(|edge| edge.target_id == newer.id));

        Ok(())
    }

    #[test]
    fn test_reflection_and_community_markers_roundtrip() -> Result<()> {
        let temp_dir = tempdir()?;
        let rt = tokio::runtime::Runtime::new()?;
        let engine = rt.block_on(MemoroseEngine::new_with_default_threshold(
            temp_dir.path(),
            1000,
            true,
            true,
        ))?;

        engine.set_needs_reflect("alice")?;
        engine.set_needs_reflect("bob")?;
        engine.set_needs_community("alice")?;
        engine.set_needs_community("carol")?;

        let mut reflections = engine.get_pending_reflections()?;
        reflections.sort();
        assert_eq!(reflections, vec!["alice".to_string(), "bob".to_string()]);

        let mut communities = engine.get_pending_communities()?;
        communities.sort();
        assert_eq!(communities, vec!["alice".to_string(), "carol".to_string()]);

        engine.clear_reflection_marker("alice")?;
        engine.clear_community_marker("carol")?;

        assert_eq!(engine.get_pending_reflections()?, vec!["bob".to_string()]);
        assert_eq!(engine.get_pending_communities()?, vec!["alice".to_string()]);
        Ok(())
    }

    #[test]
    fn test_reflection_marker_accumulates_pending_units_and_tokens() -> Result<()> {
        let temp_dir = tempdir()?;
        let rt = tokio::runtime::Runtime::new()?;
        let engine = rt.block_on(MemoroseEngine::new_with_default_threshold(
            temp_dir.path(),
            1000,
            true,
            true,
        ))?;

        engine.bump_reflection_marker("alice", 2, 120)?;
        engine.bump_reflection_marker("alice", 3, 80)?;

        let markers = engine.get_pending_reflection_markers()?;
        let alice = markers
            .into_iter()
            .find(|(user_id, _)| user_id == "alice")
            .expect("alice marker should exist");
        assert_eq!(alice.1.pending_units, 5);
        assert_eq!(alice.1.pending_tokens, 200);
        assert_eq!(alice.1.first_event_tx_micros, 0);
        assert_eq!(alice.1.last_event_tx_micros, 0);
        assert!(alice.1.last_event_at_ts >= alice.1.first_event_at_ts);
        Ok(())
    }

    #[tokio::test]
    async fn test_store_l2_units_does_not_schedule_reflection() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Derived topic".into(),
            None,
        );
        unit.level = 2;

        engine.store_memory_units(vec![unit]).await?;

        assert!(engine.get_pending_reflections()?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_event_directly_rejects_empty_variants() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let variants = vec![
            EventContent::Text("   ".into()),
            EventContent::Image(" ".into()),
            EventContent::Audio(" ".into()),
            EventContent::Video(" ".into()),
            EventContent::Json(serde_json::Value::Null),
            EventContent::Json(serde_json::Value::String(" ".into())),
        ];

        for content in variants {
            let err = engine
                .ingest_event_directly(Event::new(None, TEST_USER.into(), None, stream_id, content))
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("Rejected empty event"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_events_directly_batches_pending_writes() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();
        let event_a = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Text("alpha".into()),
        );
        let event_b = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Text("beta".into()),
        );

        engine
            .ingest_events_directly(vec![event_a.clone(), event_b.clone()])
            .await?;

        assert_eq!(engine.count_pending_events().await?, 2);
        let pending = engine.fetch_pending_events_limited(10).await?;
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().any(|event| event.id == event_a.id));
        assert!(pending.iter().any(|event| event.id == event_b.id));
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_recent_l1_units_returns_top_k_without_full_scan_order_loss() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut older = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "older".into(),
            None,
        );
        older.transaction_time = Utc::now() - chrono::Duration::minutes(10);

        let mut newest = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "newest".into(),
            None,
        );
        newest.transaction_time = Utc::now();

        let mut middle = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "middle".into(),
            None,
        );
        middle.transaction_time = Utc::now() - chrono::Duration::minutes(3);

        let newest_id = newest.id;
        let middle_id = middle.id;

        engine
            .store_memory_units(vec![older, newest, middle])
            .await?;

        let recent = engine.fetch_recent_l1_units(TEST_USER, 2).await?;
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].id, newest_id);
        assert_eq!(recent[1].id, middle_id);

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_recent_l1_units_since_filters_to_incremental_window() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let mut older = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "older".into(),
            None,
        );
        older.transaction_time = Utc::now() - chrono::Duration::minutes(10);

        let mut newer = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "newer".into(),
            None,
        );
        newer.transaction_time = Utc::now();

        let newer_id = newer.id;
        let min_tx_micros = newer.transaction_time.timestamp_micros();

        engine.store_memory_units(vec![older, newer]).await?;

        let recent = engine
            .fetch_recent_l1_units_since(TEST_USER, min_tx_micros, 10)
            .await?;
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].id, newer_id);

        Ok(())
    }

    #[tokio::test]
    async fn test_export_snapshot_writes_archive() -> Result<()> {
        let temp_dir = tempdir()?;
        let output_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("snapshot me".into()),
        );
        engine.ingest_event_directly(event).await?;

        let output_path = output_dir.path().join("snapshot.tar.gz");
        engine.export_snapshot(output_path.clone()).await?;

        assert!(output_path.exists());
        assert!(std::fs::metadata(&output_path)?.len() > 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_restore_from_snapshot_replaces_existing_target_dir() -> Result<()> {
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let archive_dir = tempdir()?;
        let target_root = tempdir()?;
        let snapshot_path = archive_dir.path().join("snapshot.tar.gz");
        let target_dir = target_root.path().join("restore-target");

        std::fs::create_dir_all(target_dir.join("stale"))?;
        std::fs::write(target_dir.join("stale/old.txt"), b"old")?;

        let file = std::fs::File::create(&snapshot_path)?;
        let enc = GzEncoder::new(file, Compression::default());
        let mut tar = tar::Builder::new(enc);
        let mut header = tar::Header::new_gnu();
        let payload = b"fresh";
        header.set_size(payload.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append_data(&mut header, "rocksdb/new.txt", &payload[..])?;
        let enc = tar.into_inner()?;
        enc.finish()?;

        MemoroseEngine::restore_from_snapshot(snapshot_path, target_dir.clone()).await?;

        assert!(target_dir.join("rocksdb/new.txt").exists());
        assert!(!target_dir.join("stale/old.txt").exists());
        assert_eq!(std::fs::read(target_dir.join("rocksdb/new.txt"))?, b"fresh");
        Ok(())
    }

    #[tokio::test]
    async fn test_bump_l1_count_tracks_threshold_crossing() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        for i in 0..4 {
            let unit = MemoryUnit::new(
                None,
                TEST_USER.into(),
                None,
                stream_id,
                memorose_common::MemoryType::Factual,
                format!("base {}", i),
                None,
            );
            engine.store_memory_unit(unit).await?;
        }

        for i in 0..2 {
            let unit = MemoryUnit::new(
                None,
                TEST_USER.into(),
                None,
                stream_id,
                memorose_common::MemoryType::Factual,
                format!("delta {}", i),
                None,
            );
            engine.store_memory_unit(unit).await?;
        }

        let (before, after) = engine.bump_l1_count_and_get_range(TEST_USER, 2).await?;
        assert_eq!((before, after), (4, 6));
        assert!(before / 5 < after / 5);

        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "delta 2".into(),
            None,
        );
        engine.store_memory_unit(unit).await?;
        let (before, after) = engine.bump_l1_count_and_get_range(TEST_USER, 1).await?;
        assert_eq!((before, after), (6, 7));
        assert!(!(before / 5 < after / 5));

        Ok(())
    }

    #[tokio::test]
    async fn test_text_search_returns_local_memories() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let primary = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Cross stream retrieval phrase".into(),
            None,
        );
        let secondary = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Cross stream retrieval phrase".into(),
            None,
        );

        engine
            .store_memory_units(vec![primary.clone(), secondary.clone()])
            .await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let results = engine
            .search_text(TEST_USER, "cross stream retrieval", 10, false, None)
            .await?;

        assert!(!results.is_empty());
        assert_eq!(results.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_org_shared_memory_is_visible_across_consumers() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Organization onboarding standard".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        engine.store_memory_unit(source.clone()).await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "onboarding standard",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        assert!(!shared.is_empty());
        assert!(shared
            .iter()
            .any(|(unit, _)| unit.domain == MemoryDomain::Organization
                && unit.user_id == MemoroseEngine::organization_read_view_owner("org_alpha")
                && unit.agent_id.is_none()
                && unit.stream_id.is_nil()
                && unit.references.is_empty()
                && unit.assets.is_empty()));

        let read_view = shared
            .iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit.clone())
            .expect("expected organization read view");
        let record = engine
            .load_organization_knowledge(read_view.id)?
            .expect("expected canonical organization knowledge");
        assert_eq!(
            engine
                .resolve_organization_record_source_ids(&record)
                .await?,
            vec![source.id]
        );
        assert_eq!(record.org_id, "org_alpha");
        assert_eq!(record.content, read_view.content);
        assert_eq!(record.keywords, read_view.keywords);

        Ok(())
    }

    #[tokio::test]
    async fn test_org_read_view_does_not_persist_view_unit() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Organization onboarding standard".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        engine.store_memory_unit(source).await?;

        let read_view = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "onboarding standard",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        let unit_key = format!(
            "u:{}:unit:{}",
            MemoroseEngine::organization_read_view_owner("org_alpha"),
            read_view.id
        );
        assert!(engine.kv().get(unit_key.as_bytes())?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_disabling_org_contribution_removes_org_read_view() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_cleanup",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_cleanup",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_cleanup".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Org cleanup knowledge".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        engine.store_memory_unit(source.clone()).await?;

        let before = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_cleanup"),
                None,
                "cleanup knowledge",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;
        assert!(!before.is_empty());

        let removed = engine
            .disable_org_contribution("author", "org_cleanup")
            .await?;
        assert_eq!(removed, 1);

        let after = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_cleanup"),
                None,
                "cleanup knowledge",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;
        assert!(after.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_disabling_org_contribution_marks_contribution_revoked() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        for user_id in ["author_a", "author_b"] {
            engine.set_org_share_policy(
                user_id,
                "org_cleanup",
                &memorose_common::SharePolicy {
                    contribute: true,
                    consume: false,
                    include_history: false,
                    targets: vec![],
                },
            )?;
        }
        engine.set_org_share_policy(
            "consumer",
            "org_cleanup",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source_a = MemoryUnit::new(
            Some("org_cleanup".into()),
            "author_a".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Cleanup Playbook: restart the cleanup worker.".into(),
            Some(vec![1.0; 768]),
        );
        source_a.level = 2;
        source_a.keywords = vec!["Cleanup Playbook".into(), "Restart".into()];

        let mut source_b = MemoryUnit::new(
            Some("org_cleanup".into()),
            "author_b".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Cleanup Playbook: retry failed cleanup jobs.".into(),
            Some(vec![1.0; 768]),
        );
        source_b.level = 2;
        source_b.keywords = vec!["Cleanup Playbook".into(), "Retry".into()];

        engine
            .store_memory_units(vec![source_a.clone(), source_b.clone()])
            .await?;

        let read_view = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_cleanup"),
                None,
                "cleanup playbook",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        engine
            .disable_org_contribution("author_a", "org_cleanup")
            .await?;

        let contribution = engine
            .load_organization_contribution(read_view.id, source_a.id)?
            .expect("expected contribution record");
        assert!(matches!(
            contribution.status,
            OrganizationKnowledgeContributionStatus::Revoked
        ));
        assert!(contribution.revoked_at.is_some());

        let hydrated = engine
            .get_shared_search_hit_by_index(read_view.id)
            .await?
            .expect("expected rebuilt organization read view")
            .into_memory_unit();
        let hydrated_record = engine
            .load_organization_knowledge(hydrated.id)?
            .expect("expected rebuilt organization knowledge record");
        assert_eq!(
            engine
                .resolve_organization_record_source_ids(&hydrated_record)
                .await?,
            vec![source_b.id]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_contribution_is_activated_from_candidate() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Escalation Playbook: page the incident commander.".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Escalation Playbook".into()];
        engine.store_memory_unit(source.clone()).await?;

        let read_view = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "escalation playbook",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        let contribution = engine
            .load_organization_contribution(read_view.id, source.id)?
            .expect("expected contribution record");
        assert!(matches!(
            contribution.status,
            OrganizationKnowledgeContributionStatus::Active
        ));
        assert_eq!(contribution.contributor_user_id, "author");
        assert!(contribution.candidate_at.is_some());
        assert!(contribution.activated_at.is_some());
        assert!(matches!(
            contribution.approval_mode,
            Some(OrganizationKnowledgeApprovalMode::Auto)
        ));
        assert_eq!(
            contribution.approved_by.as_deref(),
            Some("system:auto_publish")
        );
        assert!(contribution.revoked_at.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_detail_record_exposes_membership_and_history() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Runbook: rotate credentials after incident closure.".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Credential Rotation".into()];
        engine.store_memory_unit(source.clone()).await?;

        let read_view = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "credential rotation",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        let detail = engine
            .get_organization_knowledge_detail_record(read_view.id)
            .await?
            .expect("expected organization knowledge detail");

        assert_eq!(detail.record.id, read_view.id);
        assert_eq!(detail.read_view.id, read_view.id);
        assert_eq!(detail.memberships.len(), 1);
        assert_eq!(detail.contributions.len(), 1);

        let membership = &detail.memberships[0];
        assert_eq!(membership.membership.source_id, source.id);
        assert_eq!(membership.membership.contributor_user_id, "author");
        assert_eq!(membership.source_unit.memory_type, MemoryType::Factual);
        assert_eq!(membership.source_unit.level, 2);
        assert_eq!(membership.source_unit.keywords, vec!["Credential Rotation"]);
        assert!(membership
            .source_unit
            .content
            .contains("rotate credentials"));
        assert!(membership.contribution.is_some());
        assert!(matches!(
            membership
                .contribution
                .as_ref()
                .and_then(|record| record.approval_mode.as_ref()),
            Some(OrganizationKnowledgeApprovalMode::Auto)
        ));
        assert_eq!(
            membership
                .contribution
                .as_ref()
                .and_then(|record| record.approved_by.as_deref()),
            Some("system:auto_publish")
        );

        let contribution = &detail.contributions[0];
        assert_eq!(contribution.contribution.source_id, source.id);
        assert_eq!(contribution.contribution.contributor_user_id, "author");
        assert!(matches!(
            contribution.contribution.status,
            OrganizationKnowledgeContributionStatus::Active
        ));
        assert!(matches!(
            contribution.contribution.approval_mode.as_ref(),
            Some(OrganizationKnowledgeApprovalMode::Auto)
        ));
        assert_eq!(
            contribution.contribution.approved_by.as_deref(),
            Some("system:auto_publish")
        );
        let contribution_source = contribution
            .source_unit
            .as_ref()
            .expect("expected contribution source unit");
        assert_eq!(contribution_source.memory_type, MemoryType::Factual);
        assert_eq!(contribution_source.level, 2);
        assert_eq!(contribution_source.keywords, vec!["Credential Rotation"]);
        assert!(contribution_source.content.contains("rotate credentials"));

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_requires_l2_user_memory() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let l1_source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Raw user note that should stay local".into(),
            Some(vec![1.0; 768]),
        );
        engine.store_memory_unit(l1_source).await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "raw user note",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        assert!(shared.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_excludes_agent_memory() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut procedural = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            Some("agent_writer".into()),
            stream_id,
            memorose_common::MemoryType::Procedural,
            "Agent-specific recovery pattern".into(),
            Some(vec![1.0; 768]),
        );
        procedural.level = 2;
        engine.store_memory_unit(procedural).await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "recovery pattern",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        assert!(shared.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_org_shared_memory_ignores_agent_filter() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Shared organization troubleshooting playbook".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        engine.store_memory_unit(source).await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                Some("consumer_agent"),
                "troubleshooting playbook",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        assert!(shared
            .iter()
            .any(|(unit, _)| unit.domain == MemoryDomain::Organization));

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_canonicalizes_content() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I restart our payment service when my alert fires.".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Incident Recovery".into(), "Incident Recovery".into()];
        engine.store_memory_unit(source).await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "incident recovery payment service",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        let read_view = shared
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        assert_eq!(read_view.keywords, vec!["Incident Recovery".to_string()]);
        assert!(read_view.content.starts_with("Incident Recovery:"));
        assert!(read_view.content.contains("the contributor restart"));
        assert!(read_view
            .content
            .contains("the organization's payment service"));
        assert!(read_view.content.contains("the contributor's alert"));

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_skips_placeholder_l2_content() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "LLM not available".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        engine.store_memory_unit(source).await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "llm not available",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        assert!(shared.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_merges_same_topic_sources() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        for user_id in ["author_a", "author_b"] {
            engine.set_org_share_policy(
                user_id,
                "org_alpha",
                &memorose_common::SharePolicy {
                    contribute: true,
                    consume: false,
                    include_history: false,
                    targets: vec![],
                },
            )?;
        }
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source_a = MemoryUnit::new(
            Some("org_alpha".into()),
            "author_a".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I restart our payment service after alert storms.".into(),
            Some(vec![1.0; 768]),
        );
        source_a.level = 2;
        source_a.keywords = vec!["Incident Recovery".into(), "Restart".into()];

        let mut source_b = MemoryUnit::new(
            Some("org_alpha".into()),
            "author_b".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "We roll back the payment service after failed deploys.".into(),
            Some(vec![0.5; 768]),
        );
        source_b.level = 2;
        source_b.keywords = vec!["Incident Recovery".into(), "Rollback".into()];

        engine
            .store_memory_units(vec![source_a.clone(), source_b.clone()])
            .await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "incident recovery payment service",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        let org_units: Vec<MemoryUnit> = shared
            .into_iter()
            .filter(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit.into_memory_unit())
            .collect();

        assert_eq!(org_units.len(), 1);
        let read_view = &org_units[0];
        assert_eq!(
            read_view.user_id,
            MemoroseEngine::organization_read_view_owner("org_alpha")
        );
        assert_eq!(read_view.keywords.len(), 3);
        assert_eq!(read_view.keywords[0], "Incident Recovery");
        assert!(read_view.keywords.contains(&"Restart".to_string()));
        assert!(read_view.keywords.contains(&"Rollback".to_string()));
        assert!(read_view.agent_id.is_none());
        assert!(read_view.stream_id.is_nil());
        let record = engine
            .load_organization_knowledge(read_view.id)?
            .expect("expected organization knowledge record");
        let source_ids = engine
            .resolve_organization_record_source_ids(&record)
            .await?;
        assert_eq!(source_ids.len(), 2);
        assert!(source_ids.contains(&source_a.id));
        assert!(source_ids.contains(&source_b.id));

        Ok(())
    }

    #[tokio::test]
    async fn test_org_global_read_prefers_canonical_record_over_stored_view() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Organization incident playbook".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Incident Playbook".into()];
        engine.store_memory_unit(source).await?;

        let read_view = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "incident playbook",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        let record = engine
            .load_organization_knowledge(read_view.id)?
            .expect("expected canonical organization knowledge");
        let key = format!(
            "u:{}:unit:{}",
            MemoroseEngine::organization_read_view_owner("org_alpha"),
            read_view.id
        );
        let mut stale_view = read_view.clone();
        stale_view.content = "STALE VIEW".into();
        stale_view.keywords = vec!["STALE".into()];
        engine
            .kv()
            .put(key.as_bytes(), &serde_json::to_vec(&stale_view)?)?;

        let hydrated = engine
            .get_shared_search_hit_by_index(read_view.id)
            .await?
            .expect("expected organization knowledge hit by index")
            .into_memory_unit();

        assert_eq!(hydrated.content, record.content);
        assert_eq!(hydrated.keywords, record.keywords);
        assert_ne!(hydrated.content, "STALE VIEW");

        Ok(())
    }

    #[tokio::test]
    async fn test_org_global_list_prefers_canonical_record_over_stored_view() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Organization onboarding guide".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Onboarding Guide".into()];
        engine.store_memory_unit(source).await?;

        let read_view = engine
            .list_memory_units_global(None)
            .await?
            .into_iter()
            .find(|unit| unit.domain == MemoryDomain::Organization)
            .expect("expected organization knowledge read view in global list");

        let key = format!(
            "u:{}:unit:{}",
            MemoroseEngine::organization_read_view_owner("org_alpha"),
            read_view.id
        );
        let mut stale_view = read_view.clone();
        stale_view.content = "STALE LIST VIEW".into();
        engine
            .kv()
            .put(key.as_bytes(), &serde_json::to_vec(&stale_view)?)?;

        let listed = engine
            .list_memory_units_global(None)
            .await?
            .into_iter()
            .find(|unit| unit.id == read_view.id)
            .expect("expected organization knowledge read view in global list");

        assert_ne!(listed.content, "STALE LIST VIEW");
        assert_eq!(listed.content, read_view.content);

        Ok(())
    }

    #[tokio::test]
    async fn test_org_text_search_prefers_canonical_record_over_stored_view() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_alpha".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Organization troubleshooting playbook".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Troubleshooting Playbook".into()];
        engine.store_memory_unit(source).await?;

        let read_view = engine
            .search_text_with_shared(
                "consumer",
                Some("org_alpha"),
                "troubleshooting",
                5,
                false,
                None,
            )
            .await?
            .into_iter()
            .find(|unit| unit.domain == MemoryDomain::Organization)
            .expect("expected organization result");

        let key = format!(
            "u:{}:unit:{}",
            MemoroseEngine::organization_read_view_owner("org_alpha"),
            read_view.id
        );
        let mut stale_view = read_view.clone();
        stale_view.content = "Completely unrelated cached view".into();
        stale_view.keywords = vec!["Unrelated".into()];
        engine
            .kv()
            .put(key.as_bytes(), &serde_json::to_vec(&stale_view)?)?;

        let results = engine
            .search_text_with_shared(
                "consumer",
                Some("org_alpha"),
                "troubleshooting",
                5,
                false,
                None,
            )
            .await?;

        assert!(results.iter().any(|unit| {
            unit.domain == MemoryDomain::Organization
                && unit.id == read_view.id
                && unit.content.contains("troubleshooting")
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_org_knowledge_merges_by_shared_topic_alias() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        for user_id in ["author_a", "author_b"] {
            engine.set_org_share_policy(
                user_id,
                "org_alpha",
                &memorose_common::SharePolicy {
                    contribute: true,
                    consume: false,
                    include_history: false,
                    targets: vec![],
                },
            )?;
        }
        engine.set_org_share_policy(
            "consumer",
            "org_alpha",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source_a = MemoryUnit::new(
            Some("org_alpha".into()),
            "author_a".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I restart our cleanup worker when alerts fire.".into(),
            Some(vec![1.0; 768]),
        );
        source_a.level = 2;
        source_a.keywords = vec!["Restart Runbook".into(), "Cleanup Playbook".into()];

        let mut source_b = MemoryUnit::new(
            Some("org_alpha".into()),
            "author_b".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "We retry the cleanup worker after failed jobs.".into(),
            Some(vec![1.0; 768]),
        );
        source_b.level = 2;
        source_b.keywords = vec!["Retry Procedure".into(), "Cleanup Playbook".into()];

        engine.store_memory_unit(source_a.clone()).await?;
        engine.store_memory_unit(source_b.clone()).await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_alpha"),
                None,
                "cleanup playbook worker",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        let read_view = shared
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        assert_eq!(read_view.keywords[0], "Cleanup Playbook");
        assert!(read_view.keywords.contains(&"Restart Runbook".to_string()));
        assert!(read_view.keywords.contains(&"Retry Procedure".to_string()));
        let record = engine
            .load_organization_knowledge(read_view.id)?
            .expect("expected organization knowledge record");
        assert_eq!(
            engine
                .resolve_organization_record_source_ids(&record)
                .await?
                .len(),
            2
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_disabling_org_contribution_preserves_other_topic_sources() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        for user_id in ["author_a", "author_b"] {
            engine.set_org_share_policy(
                user_id,
                "org_cleanup",
                &memorose_common::SharePolicy {
                    contribute: true,
                    consume: false,
                    include_history: false,
                    targets: vec![],
                },
            )?;
        }
        engine.set_org_share_policy(
            "consumer",
            "org_cleanup",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source_a = MemoryUnit::new(
            Some("org_cleanup".into()),
            "author_a".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I restart our cleanup worker when alerts fire.".into(),
            Some(vec![1.0; 768]),
        );
        source_a.level = 2;
        source_a.keywords = vec!["Cleanup Playbook".into(), "Restart".into()];

        let mut source_b = MemoryUnit::new(
            Some("org_cleanup".into()),
            "author_b".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "We re-run the cleanup worker after failed jobs.".into(),
            Some(vec![1.0; 768]),
        );
        source_b.level = 2;
        source_b.keywords = vec!["Cleanup Playbook".into(), "Retry".into()];

        engine.store_memory_unit(source_a.clone()).await?;
        engine.store_memory_unit(source_b.clone()).await?;

        let removed = engine
            .disable_org_contribution("author_a", "org_cleanup")
            .await?;
        assert_eq!(removed, 1);
        assert!(engine.load_organization_membership(source_a.id)?.is_none());

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_cleanup"),
                None,
                "cleanup worker",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;

        let read_view = shared
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view to remain");

        assert_eq!(
            read_view.keywords,
            vec!["Cleanup Playbook".to_string(), "Retry".to_string()]
        );
        assert!(read_view.content.contains("the organization"));
        let record = engine
            .load_organization_knowledge(read_view.id)?
            .expect("expected organization knowledge record");
        assert_eq!(
            engine
                .resolve_organization_record_source_ids(&record)
                .await?,
            vec![source_b.id]
        );

        let removed_second = engine
            .disable_org_contribution("author_b", "org_cleanup")
            .await?;
        assert_eq!(removed_second, 1);

        let after = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_cleanup"),
                None,
                "cleanup worker",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;
        assert!(after.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_disabling_org_contribution_rebinds_topic_alias_mappings() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        for user_id in ["author_a", "author_b"] {
            engine.set_org_share_policy(
                user_id,
                "org_cleanup",
                &memorose_common::SharePolicy {
                    contribute: true,
                    consume: false,
                    include_history: false,
                    targets: vec![],
                },
            )?;
        }
        engine.set_org_share_policy(
            "consumer",
            "org_cleanup",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source_a = MemoryUnit::new(
            Some("org_cleanup".into()),
            "author_a".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I restart our cleanup worker when alerts fire.".into(),
            Some(vec![1.0; 768]),
        );
        source_a.level = 2;
        source_a.keywords = vec!["Restart Runbook".into(), "Cleanup Playbook".into()];

        let mut source_b = MemoryUnit::new(
            Some("org_cleanup".into()),
            "author_b".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "We retry the cleanup worker after failed jobs.".into(),
            Some(vec![1.0; 768]),
        );
        source_b.level = 2;
        source_b.keywords = vec!["Retry Procedure".into(), "Cleanup Playbook".into()];

        engine.store_memory_unit(source_a).await?;
        engine.store_memory_unit(source_b.clone()).await?;

        engine
            .disable_org_contribution("author_a", "org_cleanup")
            .await?;

        let shared = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_cleanup"),
                None,
                "retry procedure cleanup",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?;
        let read_view = shared
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        let retry_mapping = MemoroseEngine::organization_topic_relation_key(
            "org_cleanup",
            &MemoroseEngine::build_organization_topic_key("Retry Procedure"),
        );
        let shared_mapping = MemoroseEngine::organization_topic_relation_key(
            "org_cleanup",
            &MemoroseEngine::build_organization_topic_key("Cleanup Playbook"),
        );

        assert_eq!(read_view.keywords[0], "Retry Procedure");
        let record = engine
            .load_organization_knowledge(read_view.id)?
            .expect("expected organization knowledge record");
        assert_eq!(
            engine
                .resolve_organization_record_source_ids(&record)
                .await?,
            vec![source_b.id]
        );
        assert_eq!(
            engine
                .load_organization_topic_relation(
                    "org_cleanup",
                    &MemoroseEngine::build_organization_topic_key("Retry Procedure"),
                )?
                .map(|relation| relation.knowledge_id),
            Some(read_view.id)
        );
        assert_eq!(
            engine
                .load_organization_topic_relation(
                    "org_cleanup",
                    &MemoroseEngine::build_organization_topic_key("Cleanup Playbook"),
                )?
                .map(|relation| relation.knowledge_id),
            Some(read_view.id)
        );
        assert!(engine.system_kv().get(retry_mapping.as_bytes())?.is_some());
        assert!(engine.system_kv().get(shared_mapping.as_bytes())?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_startup_reconcile_removes_persisted_org_views() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let mut stale_view = MemoryUnit::new_with_domain(
            Some("org_stale".into()),
            "stale_owner".into(),
            None,
            Uuid::nil(),
            memorose_common::MemoryType::Factual,
            MemoryDomain::Organization,
            "Stale persisted organization read view".into(),
            Some(vec![1.0; 768]),
        );
        stale_view.level = 2;
        stale_view.keywords = vec!["Stale".into()];

        let unit_key = format!("u:{}:unit:{}", stale_view.user_id, stale_view.id);
        let index_key = format!("idx:unit:{}", stale_view.id);
        engine
            .kv()
            .put(unit_key.as_bytes(), &serde_json::to_vec(&stale_view)?)?;
        engine
            .kv()
            .put(index_key.as_bytes(), stale_view.user_id.as_bytes())?;

        drop(engine);

        let reopened =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        assert!(reopened.kv().get(unit_key.as_bytes())?.is_none());
        assert!(reopened.kv().get(index_key.as_bytes())?.is_none());
        assert!(reopened
            .get_shared_search_hit_by_index(stale_view.id)
            .await?
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_startup_reconcile_removes_org_record_without_live_sources() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_reconcile",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;
        engine.set_org_share_policy(
            "consumer",
            "org_reconcile",
            &memorose_common::SharePolicy {
                contribute: false,
                consume: true,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_reconcile".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Reconcile startup should remove orphaned org records.".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Startup Reconcile".into()];
        engine.store_memory_unit(source.clone()).await?;

        let read_view = engine
            .search_hybrid_with_shared(
                "consumer",
                Some("org_reconcile"),
                None,
                "startup reconcile",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?
            .into_iter()
            .find(|(unit, _)| unit.domain == MemoryDomain::Organization)
            .map(|(unit, _)| unit)
            .expect("expected organization read view");

        let source_key = format!("u:{}:unit:{}", source.user_id, source.id);
        let source_index_key = format!("idx:unit:{}", source.id);
        engine.kv().delete(source_key.as_bytes())?;
        engine.kv().delete(source_index_key.as_bytes())?;

        drop(engine);

        let reopened =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        assert!(reopened
            .load_organization_knowledge(read_view.id)?
            .is_none());
        assert!(reopened
            .search_hybrid_with_shared(
                "consumer",
                Some("org_reconcile"),
                None,
                "startup reconcile",
                &vec![1.0; 768],
                5,
                false,
                Some(0.0),
                0,
                None,
                None,
            )
            .await?
            .into_iter()
            .all(|(unit, _)| unit.id != read_view.id));

        Ok(())
    }

    #[tokio::test]
    async fn test_startup_reconcile_cleans_stale_org_source_relations() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let relation = OrganizationKnowledgeRelationRecord {
            org_id: "org_stale_relation".into(),
            knowledge_id: Uuid::new_v4(),
            relation: OrganizationKnowledgeRelationKind::Source {
                source_id: Uuid::new_v4(),
            },
            updated_at: Utc::now(),
        };
        let primary_key = MemoroseEngine::organization_relation_key(&relation);
        let index_key = MemoroseEngine::organization_knowledge_relation_index_key(&relation);
        let bytes = serde_json::to_vec(&relation)?;

        engine.system_kv().put(primary_key.as_bytes(), &bytes)?;
        engine.system_kv().put(index_key.as_bytes(), &bytes)?;

        drop(engine);

        let reopened =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        assert!(reopened.system_kv().get(primary_key.as_bytes())?.is_none());
        assert!(reopened.system_kv().get(index_key.as_bytes())?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_org_relation_index_is_written_for_knowledge() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_index",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_index".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Index the organization relation structure.".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Relation Index".into(), "Org Index".into()];
        engine.store_memory_unit(source).await?;

        let record = engine
            .list_organization_knowledge_records(Some("org_index"), None)
            .await?
            .into_iter()
            .next()
            .expect("expected organization knowledge record");
        let relations = engine
            .list_organization_relations_for_knowledge(record.id)
            .await?;

        assert!(!relations.is_empty());
        for relation in relations {
            let index_key = MemoroseEngine::organization_knowledge_relation_index_key(&relation);
            assert!(engine.system_kv().get(index_key.as_bytes())?.is_some());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_org_relation_index_is_removed_with_read_view() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        engine.set_org_share_policy(
            "author",
            "org_index_cleanup",
            &memorose_common::SharePolicy {
                contribute: true,
                consume: false,
                include_history: false,
                targets: vec![],
            },
        )?;

        let mut source = MemoryUnit::new(
            Some("org_index_cleanup".into()),
            "author".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Remove relation index when organization read view is deleted.".into(),
            Some(vec![1.0; 768]),
        );
        source.level = 2;
        source.keywords = vec!["Relation Cleanup".into()];
        engine.store_memory_unit(source).await?;

        let record = engine
            .list_organization_knowledge_records(Some("org_index_cleanup"), None)
            .await?
            .into_iter()
            .next()
            .expect("expected organization knowledge record");
        let relation_prefix =
            MemoroseEngine::organization_knowledge_relation_index_prefix(record.id);
        assert!(!engine
            .system_kv()
            .scan(relation_prefix.as_bytes())?
            .is_empty());

        let removed = engine
            .disable_org_contribution("author", "org_index_cleanup")
            .await?;
        assert_eq!(removed, 1);
        assert!(engine.load_organization_knowledge(record.id)?.is_none());
        assert!(engine
            .system_kv()
            .scan(relation_prefix.as_bytes())?
            .is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_local_text_search_excludes_shared_org_read_view() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let stream_id = Uuid::new_v4();

        let source = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Read view should not leak into local text search".into(),
            None,
        );
        engine.store_memory_unit(source.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        let local_results = engine
            .search_text(TEST_USER, "read view leak", 10, false, None)
            .await?;

        assert_eq!(local_results.len(), 1);
        assert!(local_results
            .iter()
            .all(|unit| MemoroseEngine::is_local_domain(&unit.domain)));

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_get_neighbors_and_multi_hop_traverse() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();
        let node_c = Uuid::new_v4();
        let node_d = Uuid::new_v4();

        for edge in [
            memorose_common::GraphEdge::new(
                TEST_USER.into(),
                node_a,
                node_b,
                memorose_common::RelationType::RelatedTo,
                0.9,
            ),
            memorose_common::GraphEdge::new(
                TEST_USER.into(),
                node_b,
                node_c,
                memorose_common::RelationType::RelatedTo,
                0.8,
            ),
            memorose_common::GraphEdge::new(
                TEST_USER.into(),
                node_b,
                node_d,
                memorose_common::RelationType::RelatedTo,
                0.2,
            ),
        ] {
            engine.graph().add_edge(&edge).await?;
        }
        engine.graph().flush().await?;

        let neighbors = engine
            .batch_get_neighbors(TEST_USER, &[node_a, node_b])
            .await?;
        assert_eq!(neighbors.get(&node_a).map(Vec::len), Some(1));
        assert_eq!(neighbors.get(&node_b).map(Vec::len), Some(2));

        let traversed = engine
            .multi_hop_traverse(TEST_USER, vec![node_a], 2, Some(0.5))
            .await?;
        assert!(traversed.contains(&node_a));
        assert!(traversed.contains(&node_b));
        assert!(traversed.contains(&node_c));
        assert!(!traversed.contains(&node_d));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_neighbors_cached_query_cache_stats_and_invalidate() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();
        let edge = memorose_common::GraphEdge::new(
            TEST_USER.into(),
            node_a,
            node_b,
            memorose_common::RelationType::RelatedTo,
            0.9,
        );
        engine.graph().add_edge(&edge).await?;
        engine.graph().flush().await?;

        let first = engine.get_neighbors_cached(TEST_USER, node_a).await?;
        assert_eq!(first.len(), 1);

        let stats = engine.query_cache_stats().await;
        assert_eq!(stats.edge_cache_size, 1);

        engine
            .graph()
            .delete_edges_for_node(TEST_USER, node_a)
            .await?;
        let cached = engine.get_neighbors_cached(TEST_USER, node_a).await?;
        assert_eq!(cached.len(), 1);

        engine.invalidate_query_cache(TEST_USER).await;
        let stats_after_invalidate = engine.query_cache_stats().await;
        assert_eq!(stats_after_invalidate.edge_cache_size, 1);
        engine.query_cache.clear().await;
        let after_invalidate = engine.get_neighbors_cached(TEST_USER, node_a).await?;
        assert!(after_invalidate.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_engine_filter_and_key_helpers() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        assert_eq!(engine.build_time_filter(None), None);
        assert_eq!(
            engine.build_time_filter(Some(TimeRange {
                start: Some(Utc.timestamp_micros(1_700_000_000_000_000).unwrap()),
                end: Some(Utc.timestamp_micros(1_700_000_300_000_000).unwrap()),
            })),
            Some("valid_time >= 1700000000000000 AND valid_time <= 1700000300000000".into())
        );
        assert_eq!(
            engine.build_user_filter("o'hara", Some("importance > 0.7".into())),
            Some("user_id = 'o''hara' AND importance > 0.7".into())
        );
        assert_eq!(
            engine.build_global_filter(
                MemoryDomain::Organization,
                Some("org'o"),
                Some("agent'a"),
                Some("importance > 0.5".into()),
            ),
            Some(
                "domain = 'organization' AND org_id = 'org''o' AND agent_id = 'agent''a' AND importance > 0.5"
                    .into(),
            )
        );

        assert_eq!(
            MemoroseEngine::org_share_policy_key("user_a", "org_b"),
            "share_policy:user:user_a:org:org_b"
        );
        let knowledge_id = Uuid::nil();
        assert_eq!(
            MemoroseEngine::organization_knowledge_key(knowledge_id),
            format!("organization_knowledge:{knowledge_id}")
        );

        Ok(())
    }

    #[test]
    fn test_engine_text_and_topic_helpers() {
        assert_eq!(
            MemoroseEngine::normalize_whitespace("  hello\t there\n   world  "),
            "hello there world"
        );
        assert_eq!(
            MemoroseEngine::neutralize_first_person_language(
                "I moved my project; we changed our plan for me."
            ),
            "the contributor moved the contributor's project; the organization changed the organization's plan for the contributor."
        );
        assert_eq!(
            MemoroseEngine::build_organization_topic_key(" Retry / Procedure! 2026 "),
            "retry-procedure-2026"
        );
        assert_eq!(
            MemoroseEngine::fallback_organization_topic_label(
                "  This   fallback topic uses six words max here  "
            ),
            Some("This fallback topic uses six words".into())
        );
        assert_eq!(
            MemoroseEngine::fallback_organization_topic_label(" \n\t "),
            None
        );

        assert_eq!(
            MemoroseEngine::organization_topic_candidates_from_keywords_and_content(
                &[
                    "Retry Procedure".into(),
                    "retry procedure".into(),
                    "Cleanup Playbook".into(),
                ],
                "ignored fallback content",
            ),
            vec![
                ("Retry Procedure".into(), "retry-procedure".into()),
                ("Cleanup Playbook".into(), "cleanup-playbook".into()),
            ]
        );
        assert_eq!(
            MemoroseEngine::organization_topic_candidates_from_keywords_and_content(
                &[],
                "Incident coordination playbook for regional outages and drills",
            ),
            vec![(
                "Incident coordination playbook for regional outages".into(),
                "incident-coordination-playbook-for-regional-outages".into(),
            )]
        );
    }

    #[test]
    fn test_engine_similarity_policy_and_metric_helpers() {
        let now = Utc.with_ymd_and_hms(2026, 4, 6, 9, 47, 58).unwrap();
        assert_eq!(
            MemoroseEngine::rac_metric_bucket_start(now),
            Utc.with_ymd_and_hms(2026, 4, 6, 9, 0, 0).unwrap()
        );

        let base_record = OrganizationKnowledgeRecord {
            id: Uuid::new_v4(),
            org_id: "org_similarity".into(),
            topic_label: "Cleanup Playbook".into(),
            topic_alias_keys: vec!["cleanup-playbook".into()],
            memory_type: MemoryType::Factual,
            content: "Cleanup worker retry steps for incidents".into(),
            embedding: Some(vec![1.0, 0.0]),
            keywords: vec!["Cleanup Playbook".into(), "Retry".into()],
            importance: 0.9,
            valid_time: None,
            created_at: now,
            updated_at: now,
        };

        let both = MemoroseEngine::organization_similarity_score(
            &base_record,
            "cleanup retry",
            &[1.0, 1.0],
        );
        let semantic_only = MemoroseEngine::organization_similarity_score(
            &base_record,
            "unrelated topic",
            &[1.0, 1.0],
        );
        let lexical_only = MemoroseEngine::organization_similarity_score(
            &OrganizationKnowledgeRecord {
                embedding: None,
                ..base_record.clone()
            },
            "cleanup retry",
            &[0.0, 1.0],
        );
        let none = MemoroseEngine::organization_similarity_score(
            &OrganizationKnowledgeRecord {
                embedding: None,
                keywords: vec!["totally different".into()],
                content: "nothing overlapping".into(),
                ..base_record
            },
            "cleanup retry",
            &[0.0, 1.0],
        );

        assert!(both > semantic_only);
        assert!(both > 0.0);
        assert!(semantic_only > 0.0);
        assert!(lexical_only > 0.0);
        assert_eq!(none, 0.0);

        let normalized = MemoroseEngine::normalize_share_policy(
            SharePolicy {
                contribute: true,
                consume: false,
                include_history: true,
                targets: vec![],
            },
            ShareTarget::Organization,
        );
        assert_eq!(normalized.targets, vec![ShareTarget::Organization]);
        assert!(normalized.contribute);
        assert!(!normalized.consume);
        assert!(normalized.include_history);
    }

    #[tokio::test]
    async fn test_engine_l3_task_helpers_and_auto_plan_goal() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true)
            .await?
            .with_arbitrator(crate::arbitrator::Arbitrator::with_client(Arc::new(
                MockCorrectionLLM {
                    response: r#"[{"summary":"Plan","dependencies":[]},{"summary":"Execute","description":"Ship it","dependencies":["Plan"]}]"#.into(),
                },
            )));

        let user_id = "planner_user";
        let stream_id = Uuid::new_v4();
        let goal_id = Uuid::new_v4();

        engine
            .auto_plan_goal(
                Some("org_plan".into()),
                user_id.into(),
                Some("agent_plan".into()),
                stream_id,
                goal_id,
                "ship release".into(),
                0,
            )
            .await?;

        let tasks = engine.list_l3_tasks(user_id).await?;
        assert_eq!(tasks.len(), 2);

        let plan = tasks.iter().find(|task| task.title == "Plan").unwrap();
        let execute = tasks.iter().find(|task| task.title == "Execute").unwrap();
        assert_eq!(plan.parent_id, Some(goal_id));
        assert_eq!(execute.parent_id, Some(goal_id));
        assert_eq!(execute.description, "Ship it");
        assert_eq!(execute.dependencies, vec![plan.task_id]);
        assert_eq!(
            engine
                .get_l3_task(user_id, plan.task_id)
                .await?
                .map(|task| task.title),
            Some("Plan".into())
        );

        let outgoing = engine
            .graph()
            .get_outgoing_edges(user_id, execute.task_id)
            .await?;
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].target_id, goal_id);
        assert_eq!(outgoing[0].relation, RelationType::IsSubTaskOf);

        let empty_engine = MemoroseEngine::new_with_default_threshold(
            temp_dir.path().join("empty"),
            1000,
            true,
            true,
        )
        .await?
        .with_arbitrator(crate::arbitrator::Arbitrator::with_client(Arc::new(
            MockCorrectionLLM {
                response: "[]".into(),
            },
        )));
        empty_engine
            .auto_plan_goal(
                None,
                "nobody".into(),
                None,
                Uuid::new_v4(),
                Uuid::new_v4(),
                "noop".into(),
                0,
            )
            .await?;
        assert!(empty_engine.list_l3_tasks("nobody").await?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_ready_l3_tasks_filters_blocked_and_missing_dependencies() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let user_id = "task_user";

        let mut completed =
            memorose_common::L3Task::new(None, user_id.into(), None, "Done".into(), "done".into());
        completed.status = memorose_common::TaskStatus::Completed;

        let ready = memorose_common::L3Task::new(
            None,
            user_id.into(),
            None,
            "Ready".into(),
            "ready".into(),
        );

        let mut blocked_by_progress = memorose_common::L3Task::new(
            None,
            user_id.into(),
            None,
            "Waiting".into(),
            "waiting".into(),
        );
        blocked_by_progress.dependencies = vec![completed.task_id];
        blocked_by_progress.status = memorose_common::TaskStatus::InProgress;

        let mut dependent_ready = memorose_common::L3Task::new(
            None,
            user_id.into(),
            None,
            "DependentReady".into(),
            "dep ready".into(),
        );
        dependent_ready.dependencies = vec![completed.task_id];

        let mut blocked_missing_dep = memorose_common::L3Task::new(
            None,
            user_id.into(),
            None,
            "MissingDep".into(),
            "missing dep".into(),
        );
        blocked_missing_dep.dependencies = vec![Uuid::new_v4()];

        let mut blocked_incomplete_dep = memorose_common::L3Task::new(
            None,
            user_id.into(),
            None,
            "BlockedIncomplete".into(),
            "blocked incomplete".into(),
        );
        blocked_incomplete_dep.dependencies = vec![ready.task_id];

        for task in [
            completed.clone(),
            ready.clone(),
            blocked_by_progress,
            dependent_ready.clone(),
            blocked_missing_dep,
            blocked_incomplete_dep,
        ] {
            engine.store_l3_task(&task).await?;
        }

        let mut titles = engine
            .get_ready_l3_tasks(user_id)
            .await?
            .into_iter()
            .map(|task| task.title)
            .collect::<Vec<_>>();
        titles.sort();

        assert_eq!(
            titles,
            vec!["DependentReady".to_string(), "Ready".to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_engine_organization_snapshot_helpers_and_detail_sorting() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let now = Utc.with_ymd_and_hms(2026, 4, 6, 10, 0, 0).unwrap();

        let source_a = MemoryUnit::new(
            Some("org_snapshot".into()),
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "source a".into(),
            None,
        );
        let source_b = MemoryUnit::new(
            Some("org_snapshot".into()),
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "source b".into(),
            None,
        );
        let fallback_source = MemoryUnit::new(
            Some("org_snapshot".into()),
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "fallback source".into(),
            None,
        );
        engine
            .store_memory_units(vec![
                source_a.clone(),
                source_b.clone(),
                fallback_source.clone(),
            ])
            .await?;

        let record = OrganizationKnowledgeRecord {
            id: Uuid::new_v4(),
            org_id: "org_snapshot".into(),
            topic_label: "Release Guide".into(),
            topic_alias_keys: vec!["release-guide".into()],
            memory_type: MemoryType::Factual,
            content: "release guide body".into(),
            embedding: None,
            keywords: vec!["release".into()],
            importance: 0.8,
            valid_time: None,
            created_at: now,
            updated_at: now,
        };
        let read_view = MemoroseEngine::materialize_organization_read_view(&record);

        let contribution_active = OrganizationKnowledgeContributionRecord {
            org_id: "org_snapshot".into(),
            knowledge_id: record.id,
            source_id: source_a.id,
            contributor_user_id: TEST_USER.into(),
            status: OrganizationKnowledgeContributionStatus::Active,
            candidate_at: Some(now),
            activated_at: Some(now),
            approval_mode: Some(OrganizationKnowledgeApprovalMode::Auto),
            approved_by: Some("system".into()),
            updated_at: now,
            revoked_at: None,
        };
        let contribution_candidate = OrganizationKnowledgeContributionRecord {
            org_id: "org_snapshot".into(),
            knowledge_id: record.id,
            source_id: source_b.id,
            contributor_user_id: TEST_USER.into(),
            status: OrganizationKnowledgeContributionStatus::Candidate,
            candidate_at: Some(now),
            activated_at: None,
            approval_mode: None,
            approved_by: None,
            updated_at: now + chrono::Duration::seconds(5),
            revoked_at: None,
        };
        let contribution_revoked = OrganizationKnowledgeContributionRecord {
            org_id: "org_snapshot".into(),
            knowledge_id: record.id,
            source_id: fallback_source.id,
            contributor_user_id: TEST_USER.into(),
            status: OrganizationKnowledgeContributionStatus::Revoked,
            candidate_at: Some(now),
            activated_at: None,
            approval_mode: None,
            approved_by: None,
            updated_at: now + chrono::Duration::seconds(10),
            revoked_at: Some(now + chrono::Duration::seconds(10)),
        };

        let membership_a = OrganizationKnowledgeMembershipRecord {
            org_id: "org_snapshot".into(),
            knowledge_id: record.id,
            source_id: source_a.id,
            contributor_user_id: TEST_USER.into(),
            updated_at: now,
        };
        let membership_b = OrganizationKnowledgeMembershipRecord {
            org_id: "org_snapshot".into(),
            knowledge_id: record.id,
            source_id: source_b.id,
            contributor_user_id: TEST_USER.into(),
            updated_at: now + chrono::Duration::seconds(1),
        };

        let detail = engine
            .build_organization_knowledge_detail_record_from_snapshot(
                OrganizationKnowledgeSnapshot {
                    record: record.clone(),
                    read_view: read_view.clone(),
                    membership_sources: vec![
                        (membership_b.clone(), source_b.clone()),
                        (membership_a.clone(), source_a.clone()),
                    ],
                    contributions: vec![
                        contribution_revoked.clone(),
                        contribution_candidate.clone(),
                        contribution_active.clone(),
                    ],
                },
            )
            .await;

        assert_eq!(detail.record.id, record.id);
        assert_eq!(detail.read_view.id, read_view.id);
        assert_eq!(detail.memberships.len(), 2);
        assert_eq!(detail.memberships[0].membership.source_id, source_a.id);
        assert_eq!(detail.memberships[1].membership.source_id, source_b.id);
        assert_eq!(detail.contributions.len(), 3);
        assert_eq!(detail.contributions[0].contribution.source_id, source_a.id);
        assert_eq!(detail.contributions[1].contribution.source_id, source_b.id);
        assert_eq!(
            detail.contributions[2].contribution.source_id,
            fallback_source.id
        );
        assert_eq!(
            detail.contributions[2]
                .source_unit
                .as_ref()
                .map(|unit| unit.id),
            Some(fallback_source.id)
        );

        let active_memberships = MemoroseEngine::organization_memberships_from_contributions(&[
            contribution_active.clone(),
            contribution_candidate,
            contribution_revoked,
        ]);
        assert_eq!(active_memberships.len(), 1);
        assert_eq!(active_memberships[0].source_id, source_a.id);

        Ok(())
    }

    #[tokio::test]
    async fn test_engine_list_organization_knowledge_snapshots_orders_and_filters() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let now = Utc.with_ymd_and_hms(2026, 4, 6, 11, 0, 0).unwrap();

        let record_old = OrganizationKnowledgeRecord {
            id: Uuid::new_v4(),
            org_id: "org_a".into(),
            topic_label: "Alpha".into(),
            topic_alias_keys: vec!["alpha".into()],
            memory_type: MemoryType::Factual,
            content: "alpha".into(),
            embedding: None,
            keywords: vec!["alpha".into()],
            importance: 0.5,
            valid_time: None,
            created_at: now,
            updated_at: now,
        };
        let record_new = OrganizationKnowledgeRecord {
            id: Uuid::new_v4(),
            org_id: "org_a".into(),
            topic_label: "Beta".into(),
            topic_alias_keys: vec!["beta".into()],
            memory_type: MemoryType::Factual,
            content: "beta".into(),
            embedding: None,
            keywords: vec!["beta".into()],
            importance: 0.7,
            valid_time: None,
            created_at: now,
            updated_at: now + chrono::Duration::seconds(30),
        };
        let other_org = OrganizationKnowledgeRecord {
            id: Uuid::new_v4(),
            org_id: "org_b".into(),
            topic_label: "Gamma".into(),
            topic_alias_keys: vec!["gamma".into()],
            memory_type: MemoryType::Factual,
            content: "gamma".into(),
            embedding: None,
            keywords: vec!["gamma".into()],
            importance: 0.4,
            valid_time: None,
            created_at: now,
            updated_at: now + chrono::Duration::seconds(10),
        };

        engine.store_organization_knowledge(&record_old)?;
        engine.store_organization_knowledge(&record_new)?;
        engine.store_organization_knowledge(&other_org)?;

        let filtered = engine
            .list_organization_knowledge_snapshots(Some("org_a"))
            .await?;
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].record.id, record_new.id);
        assert_eq!(filtered[1].record.id, record_old.id);

        let all = engine.list_organization_knowledge_snapshots(None).await?;
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].record.id, record_new.id);

        assert!(engine
            .get_organization_knowledge_detail_record(Uuid::new_v4())
            .await?
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_engine_native_hit_and_metric_counter_helpers() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;

        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "native hit".into(),
            None,
        );
        let mut hit = SharedSearchHit::native(unit.clone());
        hit.keywords.push("tag".into());
        assert_eq!(hit.memory_unit().id, unit.id);
        assert_eq!(
            hit.clone().into_memory_unit().keywords,
            vec!["tag".to_string()]
        );

        let mut metrics = RacMetricSnapshot {
            fact_extraction_attempt_total: 1,
            fact_extraction_success_total: 2,
            correction_action_obsolete_total: 3,
            correction_action_contradicts_total: 4,
            correction_action_reaffirm_total: 5,
            correction_action_ignore_total: 6,
            tombstone_total: 7,
        };
        metrics.merge(&RacMetricSnapshot {
            fact_extraction_attempt_total: 10,
            fact_extraction_success_total: 20,
            correction_action_obsolete_total: 30,
            correction_action_contradicts_total: 40,
            correction_action_reaffirm_total: 50,
            correction_action_ignore_total: 60,
            tombstone_total: 70,
        });
        assert_eq!(metrics.fact_extraction_attempt_total, 11);
        assert_eq!(metrics.tombstone_total, 77);

        assert!(matches!(
            OrganizationKnowledgeContributionStatus::default(),
            OrganizationKnowledgeContributionStatus::Active
        ));

        engine.increment_organization_metric_counter("org_metrics", "auto_approved_total", 0)?;
        engine.increment_organization_metric_counter("org_metrics", "auto_approved_total", 2)?;
        engine.increment_organization_metric_counter("org_metrics", "revoke_total", 1)?;
        let org_snapshot = engine.get_organization_automation_counter_snapshot("org_metrics")?;
        assert_eq!(org_snapshot.auto_approved_total, 2);
        assert_eq!(org_snapshot.revoke_total, 1);
        assert_eq!(
            engine.get_organization_metric_counter("org_metrics", "missing")?,
            0
        );

        engine.increment_rac_metric_counter("fact_extraction_attempt_total", 0)?;
        engine.increment_rac_metric_counter("fact_extraction_attempt_total", 3)?;
        engine.increment_rac_metric_counter("tombstone_total", 2)?;
        let rac_snapshot = engine.get_rac_metric_snapshot()?;
        assert_eq!(rac_snapshot.fact_extraction_attempt_total, 3);
        assert_eq!(rac_snapshot.tombstone_total, 2);

        Ok(())
    }

    #[test]
    fn test_apply_token_budget_to_scored_memory_units_truncates_ranked_results() {
        let first = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "alpha beta gamma delta".into(),
            None,
        );
        let second = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "epsilon zeta eta theta iota kappa".into(),
            None,
        );

        let first_cost = MemoroseEngine::memory_unit_token_cost(&first);
        let second_cost = MemoroseEngine::memory_unit_token_cost(&second);
        let results = vec![(first.clone(), 0.9), (second.clone(), 0.8)];

        let budgeted =
            MemoroseEngine::apply_token_budget_to_scored_memory_units(results, Some(first_cost));
        assert_eq!(budgeted.len(), 1);
        assert_eq!(budgeted[0].0.id, first.id);

        let unbounded = MemoroseEngine::apply_token_budget_to_scored_memory_units(
            vec![(first.clone(), 0.9), (second.clone(), 0.8)],
            Some(first_cost + second_cost),
        );
        assert_eq!(unbounded.len(), 2);
    }

    #[test]
    fn test_apply_token_budget_to_scored_shared_hits_truncates_ranked_results() {
        let first = SharedSearchHit::native(MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "shared alpha beta gamma".into(),
            None,
        ));
        let second = SharedSearchHit::native(MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "shared epsilon zeta eta theta".into(),
            None,
        ));

        let first_cost = MemoroseEngine::memory_unit_token_cost(first.memory_unit());
        let budgeted = MemoroseEngine::apply_token_budget_to_scored_shared_hits(
            vec![(first.clone(), 0.9), (second, 0.8)],
            Some(first_cost),
        );
        assert_eq!(budgeted.len(), 1);
        assert_eq!(budgeted[0].0.id, first.id);
    }

    #[test]
    fn test_apply_token_budget_skips_oversized_item_and_keeps_later_fit() {
        let oversized = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu".into(),
            None,
        );
        let small = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "tiny fit".into(),
            None,
        );

        let budget = MemoroseEngine::memory_unit_token_cost(&small);
        let budgeted = MemoroseEngine::apply_token_budget_to_scored_memory_units(
            vec![(oversized, 0.95), (small.clone(), 0.80)],
            Some(budget),
        );

        assert_eq!(budgeted.len(), 1);
        assert_eq!(budgeted[0].0.id, small.id);
    }

#[cfg(test)]
mod missing_coverage_tests {
    use crate::engine::*;
    use crate::engine::helpers::*;
    use crate::engine::types::*;
    use crate::fact_extraction::{
        MemoryFactAttribute, MemoryFactSubject, MemoryFactValueKind, MemoryFactValuePayload,
        MemoryFactChangeType, MemoryFactDescriptor,
    };
    use crate::arbitrator::{MemoryCorrectionAction, MemoryCorrectionKind};
    use chrono::{TimeZone, Utc};
    use memorose_common::{
        Event, EventContent, ForgetMode, ForgetTargetKind, ForgettingTombstone, GraphEdge,
        MemoryDomain, MemoryType, MemoryUnit, RelationType, SharePolicy, StoredMemoryFact,
        TimeRange,
    };
    use std::sync::Arc;
    use tempfile::tempdir;
    use uuid::Uuid;

    const TEST_USER: &str = "test_user";

    #[test]
    fn test_rac_metric_history_point_merge() {
        let mut p1 = RacMetricHistoryPoint {
            bucket_start: "100".into(),
            fact_extraction_attempt_total: 1,
            fact_extraction_success_total: 2,
            correction_action_obsolete_total: 3,
            correction_action_contradicts_total: 4,
            correction_action_reaffirm_total: 5,
            correction_action_ignore_total: 6,
            tombstone_total: 7,
        };
        let p2 = RacMetricHistoryPoint {
            bucket_start: "200".into(),
            fact_extraction_attempt_total: 10,
            fact_extraction_success_total: 20,
            correction_action_obsolete_total: 30,
            correction_action_contradicts_total: 40,
            correction_action_reaffirm_total: 50,
            correction_action_ignore_total: 60,
            tombstone_total: 70,
        };
        p1.merge(&p2);
        assert_eq!(p1.fact_extraction_attempt_total, 11);
        assert_eq!(p1.fact_extraction_success_total, 22);
        assert_eq!(p1.correction_action_obsolete_total, 33);
        assert_eq!(p1.correction_action_contradicts_total, 44);
        assert_eq!(p1.correction_action_reaffirm_total, 55);
        assert_eq!(p1.correction_action_ignore_total, 66);
        assert_eq!(p1.tombstone_total, 77);
    }

    #[test]
    fn test_matches_valid_time_filter() {
        use chrono::TimeZone;
        let t1 = Utc.timestamp_opt(1000, 0).unwrap();
        let t2 = Utc.timestamp_opt(2000, 0).unwrap();
        let t3 = Utc.timestamp_opt(3000, 0).unwrap();

        // No range
        assert!(MemoroseEngine::matches_valid_time_filter(Some(t2), None));

        // No valid_time but range exists
        let range = TimeRange {
            start: Some(t1),
            end: Some(t3),
        };
        assert!(!MemoroseEngine::matches_valid_time_filter(
            None,
            Some(&range)
        ));

        // valid_time < start
        assert!(!MemoroseEngine::matches_valid_time_filter(
            Some(t1 - chrono::Duration::seconds(1)),
            Some(&range)
        ));

        // valid_time > end
        assert!(!MemoroseEngine::matches_valid_time_filter(
            Some(t3 + chrono::Duration::seconds(1)),
            Some(&range)
        ));

        // Inside range
        assert!(MemoroseEngine::matches_valid_time_filter(
            Some(t2),
            Some(&range)
        ));
    }

    #[test]
    fn test_memory_unit_token_cost() {
        let mut unit = MemoryUnit::new(
            None,
            "user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Hello world".into(),
            None,
        );
        unit.keywords = vec!["keyword1".into(), "keyword2".into()];
        unit.assets.push(memorose_common::Asset {
            asset_type: "image".into(),
            storage_key: "http://example.com/image.png".into(),
            original_name: "image.png".into(),
            description: Some("A nice image".into()),
            metadata: std::collections::HashMap::new(),
        });

        let cost = MemoroseEngine::memory_unit_token_cost(&unit);
        assert!(cost > 0);
    }

    #[tokio::test]
    async fn test_memorose_engine_new_helper() {
        let temp = tempfile::tempdir().unwrap();
        let engine = MemoroseEngine::new(temp.path(), 1000, false, false, 0.5, 128)
            .await
            .unwrap();
        assert_eq!(engine.auto_planner, false);
    }

    #[tokio::test]
    async fn test_search_hybrid_empty_index() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, false, false).await?;

        // Search without ingesting anything — should return empty vec, not error
        let results = engine
            .search_hybrid(
                TEST_USER,
                None,
                None,
                "anything at all",
                &vec![0.1; 768],
                10,
                false,
                None,
                0,
                None,
                None,
            )
            .await?;

        assert!(results.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_search_hybrid_min_score_filtering() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, false, false).await?;
        let stream_id = Uuid::new_v4();

        // Ingest an event so the engine has data
        let event = Event::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            EventContent::Text("The quick brown fox jumps over the lazy dog".to_string()),
        );
        engine.ingest_event(event).await?;

        // Store a memory unit with a known embedding
        let mut embedding = vec![0.0; 768];
        embedding[0] = 1.0;
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "The quick brown fox".to_string(),
            Some(embedding),
        );
        engine.store_memory_unit(unit).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        // Search with min_score = 5.0 (impossibly high — reranker adds importance +
        // recency on top of the normalized [0,1] RRF score, so we need to exceed
        // even those bonuses)
        let results = engine
            .search_hybrid(
                TEST_USER,
                None,
                None,
                "fox",
                &vec![0.5; 768],
                10,
                false,
                Some(5.0),
                0,
                None,
                None,
            )
            .await?;

        assert!(results.is_empty(), "No result should meet a 5.0 min_score threshold");
        Ok(())
    }

    #[tokio::test]
    async fn test_search_text_basic() -> Result<()> {
        let temp_dir = tempdir()?;
        let engine =
            MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, false, false).await?;
        let stream_id = Uuid::new_v4();

        // Store a memory unit with known content
        let unit = MemoryUnit::new(
            None,
            TEST_USER.into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "Rust programming language is fast and safe".to_string(),
            Some(vec![0.0; 768]),
        );
        engine.store_memory_unit(unit.clone()).await?;
        engine.index.commit()?;
        engine.index.reload()?;

        // Search by text — should find the unit
        let text_hits = engine
            .search_text(TEST_USER, "Rust programming", 10, false, None)
            .await?;

        assert!(!text_hits.is_empty(), "Text search should find the stored unit");
        assert_eq!(text_hits[0].id, unit.id);
        Ok(())
    }
}
