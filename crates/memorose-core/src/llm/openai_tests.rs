#[cfg(test)]
mod tests {
    use super::super::{map_usage, parse_chat_response, parse_embed_response};
    use crate::llm::openai::OpenAIClient;
    use crate::llm::{EmbedInput, EmbedPart, LLMClient};
    use memorose_common::TokenUsage;
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const TEST_MODEL: &str = "test-model";
    const TEST_EMBEDDING_MODEL: &str = "test-embedding-model";
    const TEST_API_KEY: &str = "sk-test-key";

    #[tokio::test]
    async fn test_generate_success() {
        let mock_server = MockServer::start().await;

        let expected_response = json!({
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1677652288,
            "model": TEST_MODEL,
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello from Mock OpenAI!"
                },
                "finish_reason": "stop"
            }]
        });

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(header(
                "Authorization",
                format!("Bearer {}", TEST_API_KEY).as_str(),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(expected_response))
            .mount(&mock_server)
            .await;

        let client = OpenAIClient::new(
            TEST_API_KEY.to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            Some(mock_server.uri()),
        );

        let result = client.generate("Say hello").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().data, "Hello from Mock OpenAI!");
    }

    #[tokio::test]
    async fn test_generate_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(401).set_body_string(
                r#"{
  "error": {
    "message": "Invalid Authentication",
    "type": "server_error",
    "param": null,
    "code": null
  }
}"#,
            ))
            .mount(&mock_server)
            .await;

        let client = OpenAIClient::new(
            TEST_API_KEY.to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            Some(mock_server.uri()),
        );

        let result = client.generate("Say hello").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("OpenAI API error (401 Unauthorized)"));
    }

    #[tokio::test]
    async fn test_embed_success() {
        let mock_server = MockServer::start().await;

        let expected_response = json!({
            "object": "list",
            "data": [
                {
                    "object": "embedding",
                    "embedding": [0.1, 0.2, 0.3],
                    "index": 0
                }
            ],
            "model": TEST_EMBEDDING_MODEL,
            "usage": {
                "prompt_tokens": 8,
                "total_tokens": 8
            }
        });

        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .and(header(
                "Authorization",
                format!("Bearer {}", TEST_API_KEY).as_str(),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(expected_response))
            .mount(&mock_server)
            .await;

        let client = OpenAIClient::new(
            TEST_API_KEY.to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            Some(mock_server.uri()),
        );

        let result = client.embed("test text").await;
        assert!(result.is_ok());
        let embedding = result.unwrap();
        assert_eq!(embedding.data.len(), 3);
        assert_eq!(embedding.data, vec![0.1, 0.2, 0.3]);
    }

    #[tokio::test]
    async fn test_compress_success() {
        let mock_server = MockServer::start().await;

        let expected_response = json!({
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1677652288,
            "model": TEST_MODEL,
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "{\"content\": \"Summarized fact\", \"valid_at\": null}"
                },
                "finish_reason": "stop"
            }]
        });

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(expected_response))
            .mount(&mock_server)
            .await;

        let client = OpenAIClient::new(
            TEST_API_KEY.to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            Some(mock_server.uri()),
        );

        let result = client.compress("Long text to compress", false).await;
        assert!(result.is_ok());
        let output = result.unwrap();
        assert_eq!(output.data.content, "Summarized fact");
        assert!(output.data.valid_at.is_none());
    }

    #[test]
    fn test_map_usage_defaults_completion_tokens() {
        let usage = map_usage(Some(super::super::Usage {
            prompt_tokens: 12,
            completion_tokens: None,
            total_tokens: 12,
        }));

        assert_eq!(usage.prompt_tokens, 12);
        assert_eq!(usage.completion_tokens, 0);
        assert_eq!(usage.total_tokens, 12);

        let default_usage = map_usage(None);
        assert_eq!(
            default_usage.prompt_tokens,
            TokenUsage::default().prompt_tokens
        );
        assert_eq!(
            default_usage.completion_tokens,
            TokenUsage::default().completion_tokens
        );
        assert_eq!(
            default_usage.total_tokens,
            TokenUsage::default().total_tokens
        );
    }

    #[test]
    fn test_parse_chat_response_extracts_content_and_usage() {
        let response = parse_chat_response(
            r#"{
                "choices": [{"message": {"content": "hello"}}],
                "usage": {"prompt_tokens": 5, "completion_tokens": 7, "total_tokens": 12}
            }"#,
            "OpenAI response",
        )
        .expect("chat response should parse");

        assert_eq!(response.data, "hello");
        assert_eq!(response.usage.prompt_tokens, 5);
        assert_eq!(response.usage.completion_tokens, 7);
        assert_eq!(response.usage.total_tokens, 12);
    }

    #[test]
    fn test_parse_chat_response_requires_message_content() {
        let err = parse_chat_response(
            r#"{"choices": [{"message": {"content": null}}]}"#,
            "OpenAI response",
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("No content in OpenAI response"));
    }

    #[test]
    fn test_parse_embed_response_extracts_embeddings_and_usage() {
        let response = parse_embed_response(
            r#"{
                "data": [
                    {"embedding": [0.1, 0.2]},
                    {"embedding": [0.3, 0.4]}
                ],
                "usage": {"prompt_tokens": 8, "total_tokens": 8}
            }"#,
        )
        .expect("embedding response should parse");

        assert_eq!(response.data, vec![vec![0.1, 0.2], vec![0.3, 0.4]]);
        assert_eq!(response.usage.prompt_tokens, 8);
        assert_eq!(response.usage.completion_tokens, 0);
        assert_eq!(response.usage.total_tokens, 8);
    }

    #[test]
    fn test_parse_embed_response_rejects_invalid_json() {
        let err = parse_embed_response(r#"{"data":"oops"}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Failed to parse OpenAI embedding response"));
    }

    #[test]
    fn test_new_uses_default_base_url() {
        let client = OpenAIClient::new(
            TEST_API_KEY.to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            None,
        );

        assert_eq!(client.base_url, "https://api.openai.com/v1");
        assert_eq!(client.model, TEST_MODEL);
        assert_eq!(client.embedding_model, TEST_EMBEDDING_MODEL);
    }

    #[tokio::test]
    async fn test_call_chat_completion_invalid_base_url_errors_without_server() {
        let client = OpenAIClient::new(
            TEST_API_KEY.to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            Some("not-a-valid-url".to_string()),
        );

        assert!(client
            .call_chat_completion(Some("system"), "hello", true)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_embed_batch_large_input_errors_after_chunk_path() {
        let client = OpenAIClient::new(
            TEST_API_KEY.to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            Some("not-a-valid-url".to_string()),
        );

        let texts = (0..2049).map(|i| format!("text-{i}")).collect();
        assert!(client.embed_batch(texts).await.is_err());
    }

    #[tokio::test]
    async fn test_embed_content_multimodal_hits_media_fallback_branches() {
        let client = OpenAIClient::new(
            TEST_API_KEY.to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            Some("not-a-valid-url".to_string()),
        );

        let input = EmbedInput::Multimodal {
            parts: vec![
                EmbedPart::Text("prefix".to_string()),
                EmbedPart::InlineData {
                    mime_type: "image/png".to_string(),
                    data: "abc".to_string(),
                },
                EmbedPart::InlineData {
                    mime_type: "audio/mpeg".to_string(),
                    data: "def".to_string(),
                },
                EmbedPart::InlineData {
                    mime_type: "video/mp4".to_string(),
                    data: "ghi".to_string(),
                },
                EmbedPart::InlineData {
                    mime_type: "application/octet-stream".to_string(),
                    data: "jkl".to_string(),
                },
            ],
        };

        assert!(client.embed_content(input).await.is_err());
    }

    #[tokio::test]
    async fn test_embed_content_batch_mixed_inputs_errors_without_server() {
        let client = OpenAIClient::new(
            TEST_API_KEY.to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            Some("not-a-valid-url".to_string()),
        );

        let inputs = vec![
            EmbedInput::Text("hello".to_string()),
            EmbedInput::Multimodal {
                parts: vec![
                    EmbedPart::Text("world".to_string()),
                    EmbedPart::InlineData {
                        mime_type: "audio/wav".to_string(),
                        data: "123".to_string(),
                    },
                ],
            },
        ];

        assert!(client.embed_content_batch(inputs).await.is_err());
    }

    #[tokio::test]
    async fn test_describe_image_base64_invalid_base_url_errors() {
        let client = OpenAIClient::new(
            TEST_API_KEY.to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            Some("not-a-valid-url".to_string()),
        );

        assert!(client.describe_image("ZmFrZQ==").await.is_err());
    }

    #[tokio::test]
    async fn test_transcribe_and_describe_video_are_not_implemented() {
        let client = OpenAIClient::new(
            TEST_API_KEY.to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            Some("not-a-valid-url".to_string()),
        );

        let transcribe_err = client.transcribe("audio").await.unwrap_err().to_string();
        assert!(transcribe_err.contains("transcribe not fully implemented"));

        let describe_video_err = client
            .describe_video("video")
            .await
            .unwrap_err()
            .to_string();
        assert!(describe_video_err.contains("describe_video not fully implemented"));
    }
}
