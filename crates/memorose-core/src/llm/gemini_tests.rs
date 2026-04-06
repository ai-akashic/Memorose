#[cfg(test)]
mod tests {
    use super::super::{
        embed_input_to_parts, map_usage_metadata, parse_batch_embed_response,
        parse_embed_response, parse_generate_response, trim_json_fence, GeminiClient,
        GeminiUsageMetadata, Part,
    };
    use crate::llm::{EmbedInput, EmbedPart, LLMClient};
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const TEST_MODEL: &str = "test-model";
    const TEST_EMBEDDING_MODEL: &str = "test-embedding-model";

    #[tokio::test]
    async fn test_generate_success() {
        let mock_server = MockServer::start().await;

        let expected_response = json!({
            "candidates": [{
                "content": {
                    "parts": [{ "text": "Hello form Mock Gemini!" }]
                }
            }]
        });

        Mock::given(method("POST"))
            .and(path(format!(
                "/v1beta/models/{}:generateContent",
                TEST_MODEL
            )))
            .and(query_param("key", "test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(expected_response))
            .mount(&mock_server)
            .await;

        let client = GeminiClient::with_base_url(
            "test-key".to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            mock_server.uri(),
            None,
            None,
        );

        let result = client.generate("Hello").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().data, "Hello form Mock Gemini!");
    }

    #[tokio::test]
    async fn test_generate_403_error() {
        let mock_server = MockServer::start().await;

        let error_response = json!({
            "error": {
                "code": 403,
                "message": "Method doesn't allow unregistered callers",
                "status": "PERMISSION_DENIED"
            }
        });

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(403).set_body_json(error_response))
            .mount(&mock_server)
            .await;

        let client = GeminiClient::with_base_url(
            "bad-key".to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            mock_server.uri(),
            None,
            None,
        );

        let result = client.generate("Hello").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        // Our client wraps the error
        assert!(err_msg.contains("Gemini API error (403 Forbidden)"));
    }

    #[tokio::test]
    async fn test_embed_success() {
        let mock_server = MockServer::start().await;

        let expected_response = json!({
            "embedding": {
                "values": [0.1, 0.2, 0.3]
            }
        });

        Mock::given(method("POST"))
            .and(path(format!(
                "/v1beta/models/{}:embedContent",
                TEST_EMBEDDING_MODEL
            )))
            .and(query_param("key", "test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(expected_response))
            .mount(&mock_server)
            .await;

        let client = GeminiClient::with_base_url(
            "test-key".to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            mock_server.uri(),
            None,
            None,
        );

        let result = client.embed("test text").await;
        assert!(result.is_ok());
        let vec = result.unwrap().data;
        assert_eq!(vec.len(), 3);
        assert_eq!(vec[0], 0.1);
    }

    #[tokio::test]
    async fn test_embed_batch_success() {
        let mock_server = MockServer::start().await;

        let expected_response = json!({
            "embeddings": [
                { "values": [0.1, 0.2, 0.3] },
                { "values": [0.4, 0.5, 0.6] }
            ]
        });

        Mock::given(method("POST"))
            .and(path(format!(
                "/v1beta/models/{}:batchEmbedContents",
                TEST_EMBEDDING_MODEL
            )))
            .and(query_param("key", "test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(expected_response))
            .mount(&mock_server)
            .await;

        let client = GeminiClient::with_base_url(
            "test-key".to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            mock_server.uri(),
            None,
            None,
        );

        let result = client
            .embed_batch(vec!["one".to_string(), "two".to_string()])
            .await;
        assert!(result.is_ok());

        let embeddings = result.unwrap().data;
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0], vec![0.1, 0.2, 0.3]);
        assert_eq!(embeddings[1], vec![0.4, 0.5, 0.6]);
    }

    #[tokio::test]
    async fn test_embed_batch_count_mismatch() {
        let mock_server = MockServer::start().await;

        let mismatch_response = json!({
            "embeddings": [
                { "values": [0.1, 0.2, 0.3] }
            ]
        });

        Mock::given(method("POST"))
            .and(path(format!(
                "/v1beta/models/{}:batchEmbedContents",
                TEST_EMBEDDING_MODEL
            )))
            .and(query_param("key", "test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(mismatch_response))
            .mount(&mock_server)
            .await;

        let client = GeminiClient::with_base_url(
            "test-key".to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            mock_server.uri(),
            None,
            None,
        );

        let result = client
            .embed_batch(vec!["one".to_string(), "two".to_string()])
            .await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("count mismatch"));
    }

    #[tokio::test]
    async fn test_describe_image_invalid_url() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/image.jpg"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let client = GeminiClient::with_base_url(
            "test-key".to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            "http://localhost:1234".to_string(), // Dummy LLM base
            None,
            None,
        );

        let image_url = format!("{}/image.jpg", mock_server.uri());
        let result = client.describe_image(&image_url).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_transcribe_invalid_key() {
        let client = GeminiClient::new(
            "test-key".into(),
            TEST_MODEL.into(),
            TEST_EMBEDDING_MODEL.into(),
        );
        let result = client.transcribe("some-data").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_l2_normalize() {
        use crate::llm::gemini::l2_normalize;
        let mut v = vec![3.0, 4.0];
        l2_normalize(&mut v);
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6);
        assert!((v[0] - 0.6).abs() < 1e-6);
        assert!((v[1] - 0.8).abs() < 1e-6);
    }

    #[test]
    fn test_l2_normalize_zero_vector() {
        use crate::llm::gemini::l2_normalize;
        let mut v = vec![0.0, 0.0, 0.0];
        l2_normalize(&mut v);
        assert_eq!(v, vec![0.0, 0.0, 0.0]);
    }

    #[test]
    fn test_embed_input_text_conversion() {
        let input = EmbedInput::Text("hello".to_string());
        assert!(!input.has_multimodal_parts());
        assert_eq!(input.as_text(), "hello");
    }

    #[test]
    fn test_embed_input_multimodal_detection() {
        let input = EmbedInput::Multimodal {
            parts: vec![
                EmbedPart::Text("caption".to_string()),
                EmbedPart::InlineData {
                    mime_type: "image/jpeg".to_string(),
                    data: "base64data".to_string(),
                },
            ],
        };
        assert!(input.has_multimodal_parts());
        assert_eq!(input.as_text(), "caption");
    }

    #[test]
    fn test_map_usage_metadata_defaults_missing_fields() {
        let usage = map_usage_metadata(Some(GeminiUsageMetadata {
            prompt_token_count: Some(5),
            candidates_token_count: None,
            total_token_count: Some(7),
        }));

        assert_eq!(usage.prompt_tokens, 5);
        assert_eq!(usage.completion_tokens, 0);
        assert_eq!(usage.total_tokens, 7);
        assert_eq!(map_usage_metadata(None).total_tokens, 0);
    }

    #[test]
    fn test_parse_generate_response_extracts_text_and_usage() {
        let response = parse_generate_response(
            r#"{
                "candidates": [{
                    "content": {"parts": [{"text": "Hello from parser"}]}
                }],
                "usageMetadata": {
                    "promptTokenCount": 3,
                    "candidatesTokenCount": 4,
                    "totalTokenCount": 7
                }
            }"#,
        )
        .expect("generate response should parse");

        assert_eq!(response.data, "Hello from parser");
        assert_eq!(response.usage.prompt_tokens, 3);
        assert_eq!(response.usage.completion_tokens, 4);
        assert_eq!(response.usage.total_tokens, 7);
    }

    #[test]
    fn test_parse_generate_response_handles_inline_part_as_empty_text() {
        let response = parse_generate_response(
            r#"{
                "candidates": [{
                    "content": {
                        "parts": [{
                            "inline_data": {"mime_type":"image/png","data":"abc"}
                        }]
                    }
                }]
            }"#,
        )
        .expect("inline-only response should still parse");

        assert_eq!(response.data, "");
    }

    #[test]
    fn test_parse_generate_response_rejects_missing_content() {
        let err = parse_generate_response(r#"{"candidates":[]}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("No content in Gemini response"));
    }

    #[test]
    fn test_parse_embed_response_normalizes_when_output_dimensionality_present() {
        let response = parse_embed_response(
            r#"{"embedding":{"values":[3.0,4.0]}}"#,
            Some(2),
        )
        .expect("embed response should parse");

        assert!((response.data[0] - 0.6).abs() < 1e-6);
        assert!((response.data[1] - 0.8).abs() < 1e-6);
    }

    #[test]
    fn test_parse_embed_response_surfaces_api_error() {
        let err = parse_embed_response(
            r#"{"error":{"message":"bad request"}}"#,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("Gemini Embedding API error: bad request"));
    }

    #[test]
    fn test_parse_batch_embed_response_checks_count_and_normalizes() {
        let response = parse_batch_embed_response(
            r#"{
                "embeddings":[
                    {"values":[3.0,4.0]},
                    {"values":[5.0,12.0]}
                ]
            }"#,
            2,
            Some(2),
        )
        .expect("batch response should parse");

        assert_eq!(response.data.len(), 2);
        let norm0: f32 = response.data[0].iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm1: f32 = response.data[1].iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm0 - 1.0).abs() < 1e-6);
        assert!((norm1 - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_parse_batch_embed_response_rejects_count_mismatch() {
        let err = parse_batch_embed_response(
            r#"{"embeddings":[{"values":[1.0,2.0]}]}"#,
            2,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("count mismatch"));
    }

    #[test]
    fn test_trim_json_fence_removes_markdown_wrappers() {
        assert_eq!(
            trim_json_fence("```json\n{\"content\":\"x\"}\n```"),
            "{\"content\":\"x\"}"
        );
        assert_eq!(trim_json_fence("  plain  "), "plain");
    }

    #[test]
    fn test_embed_input_to_parts_preserves_text_and_inline_parts() {
        let parts = embed_input_to_parts(EmbedInput::Multimodal {
            parts: vec![
                EmbedPart::Text("caption".to_string()),
                EmbedPart::InlineData {
                    mime_type: "image/png".to_string(),
                    data: "abc".to_string(),
                },
            ],
        });

        assert!(matches!(&parts[0], Part::Text { text } if text == "caption"));
        assert!(matches!(&parts[1], Part::Inline { inline_data } if inline_data.mime_type == "image/png" && inline_data.data == "abc"));
    }

    #[test]
    fn test_new_uses_default_base_url() {
        let client = GeminiClient::new(
            " test-key ".to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
        );

        assert_eq!(client.api_key, "test-key");
        assert_eq!(client.base_url, "https://generativelanguage.googleapis.com");
        assert_eq!(client.model, TEST_MODEL);
        assert_eq!(client.embedding_model, TEST_EMBEDDING_MODEL);
    }

    #[tokio::test]
    async fn test_generate_invalid_base_url_errors_without_server() {
        let client = GeminiClient::with_base_url(
            "test-key".to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            "not-a-valid-url".to_string(),
            None,
            None,
        );

        assert!(client.generate("Hello").await.is_err());
    }

    #[tokio::test]
    async fn test_embed_batch_empty_returns_empty_without_network() {
        let client = GeminiClient::new(
            "test-key".into(),
            TEST_MODEL.into(),
            TEST_EMBEDDING_MODEL.into(),
        );

        let response = client.embed_batch(Vec::new()).await.expect("empty batch");
        assert!(response.data.is_empty());
        assert_eq!(response.usage.total_tokens, 0);
    }

    #[tokio::test]
    async fn test_embed_content_batch_empty_returns_empty_without_network() {
        let client = GeminiClient::new(
            "test-key".into(),
            TEST_MODEL.into(),
            TEST_EMBEDDING_MODEL.into(),
        );

        let response = client
            .embed_content_batch(Vec::new())
            .await
            .expect("empty input batch");
        assert!(response.data.is_empty());
        assert_eq!(response.usage.total_tokens, 0);
    }

    #[tokio::test]
    async fn test_embed_content_multimodal_invalid_base_url_errors() {
        let client = GeminiClient::with_base_url(
            "test-key".to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            "not-a-valid-url".to_string(),
            None,
            None,
        );

        let input = EmbedInput::Multimodal {
            parts: vec![
                EmbedPart::Text("caption".to_string()),
                EmbedPart::InlineData {
                    mime_type: "image/png".to_string(),
                    data: "abc".to_string(),
                },
            ],
        };

        assert!(client.embed_content(input).await.is_err());
    }

    #[tokio::test]
    async fn test_describe_image_base64_invalid_base_url_errors() {
        let client = GeminiClient::with_base_url(
            "test-key".to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            "not-a-valid-url".to_string(),
            None,
            None,
        );

        assert!(client.describe_image("ZmFrZQ==").await.is_err());
    }

    #[tokio::test]
    async fn test_describe_video_base64_invalid_base_url_errors() {
        let client = GeminiClient::with_base_url(
            "test-key".to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            "not-a-valid-url".to_string(),
            None,
            None,
        );

        assert!(client.describe_video("ZmFrZQ==").await.is_err());
    }
}
