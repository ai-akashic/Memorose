#[cfg(test)]
mod tests {
    use crate::llm::gemini::GeminiClient;
    use crate::llm::LLMClient;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use serde_json::json;

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
            .and(path(format!("/v1beta/models/{}:generateContent", TEST_MODEL)))
            .and(query_param("key", "test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(expected_response))
            .mount(&mock_server)
            .await;

        let client = GeminiClient::with_base_url(
            "test-key".to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            mock_server.uri(),
        );

        let result = client.generate("Hello").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Hello form Mock Gemini!");
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
            .and(path(format!("/v1beta/models/{}:embedContent", TEST_EMBEDDING_MODEL)))
            .and(query_param("key", "test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(expected_response))
            .mount(&mock_server)
            .await;

        let client = GeminiClient::with_base_url(
            "test-key".to_string(),
            TEST_MODEL.to_string(),
            TEST_EMBEDDING_MODEL.to_string(),
            mock_server.uri(),
        );

        let result = client.embed("test text").await;
        assert!(result.is_ok());
        let vec = result.unwrap();
        assert_eq!(vec.len(), 3);
        assert_eq!(vec[0], 0.1);
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
}