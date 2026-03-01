#[cfg(test)]
mod tests {
    use crate::llm::openai::OpenAIClient;
    use crate::llm::LLMClient;
    use wiremock::matchers::{method, path, header};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use serde_json::json;

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
            .and(header("Authorization", format!("Bearer {}", TEST_API_KEY).as_str()))
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
            .respond_with(ResponseTemplate::new(401).set_body_string(r#"{
  "error": {
    "message": "Invalid Authentication",
    "type": "server_error",
    "param": null,
    "code": null
  }
}"#))
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
            .and(header("Authorization", format!("Bearer {}", TEST_API_KEY).as_str()))
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
}
