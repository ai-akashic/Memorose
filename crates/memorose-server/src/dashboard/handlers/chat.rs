use axum::{
    extract::State,
    response::sse::{Event, Sse},
    Json,
};
use futures_util::stream::Stream;
use serde::Deserialize;
use std::sync::Arc;

use super::types::{append_context_with_budget, format_memory_unit_context};

#[derive(Deserialize)]
pub struct ChatRequest {
    message: String,
    user_id: String,
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default = "default_chat_limit")]
    context_limit: usize,
}

fn default_chat_limit() -> usize {
    5
}

pub async fn chat(
    State(state): State<Arc<crate::AppState>>,
    Json(payload): Json<ChatRequest>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let user_id = payload.user_id.clone();
    let org_id = payload.org_id.clone();
    let message = payload.message.clone();
    let context_limit = payload.context_limit;

    let stream = async_stream::stream! {
        // Step 1: Search for relevant context using hybrid search
        let shard = state.shard_manager.shard_for_user(&user_id);

        let context_results = match state.llm_client.embed(&message).await {
            Ok(embedding) => {
                match shard.engine.search_hybrid_with_shared(
                    &user_id,
                    org_id.as_deref(),
                    None,
                    &message,
                    &embedding.data,
                    context_limit,
                    false,
                    None,
                    1,
                    None,
                    None,
                ).await {
                    Ok(results) => results,
                    Err(e) => {
                        yield Ok(Event::default().event("error").data(format!("Search failed: {}", e)));
                        return;
                    }
                }
            }
            Err(e) => {
                yield Ok(Event::default().event("error").data(format!("Embedding failed: {}", e)));
                return;
            }
        };

        // Step 2: Build context from search results
        let mut context_text = String::new();
        let context_budget = context_limit.clamp(1, 10) * 500;
        if !context_results.is_empty() {
            context_text.push_str("## Relevant Context from Memory:\n");
            for (unit, _score) in &context_results {
                if !append_context_with_budget(
                    &mut context_text,
                    &format_memory_unit_context(unit.memory_unit()),
                    context_budget,
                ) {
                    break;
                }
            }
            context_text.push_str("\n");
        }

        // Step 3: Build prompt
        let system_prompt = format!(
            "You are a helpful AI assistant with access to the user's memory system. \
    Use the provided memory context when it is relevant, especially multimodal descriptions and source references. \
    If the memory context is insufficient, answer honestly and do not invent remembered facts.\n\n{}",
            context_text
        );

        // Step 4: Generate response using LLM
        let full_prompt = format!("{}\nUser: {}", system_prompt, message);
        match state.llm_client.generate(&full_prompt).await {
            Ok(response) => {
                // Stream the response word by word for better UX
                let words: Vec<&str> = response.data.split_whitespace().collect();
                for (i, word) in words.iter().enumerate() {
                    let text = if i == words.len() - 1 {
                        word.to_string()
                    } else {
                        format!("{} ", word)
                    };
                    yield Ok(Event::default().event("message").data(text));
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                }

                yield Ok(Event::default().event("done").data(""));
            }
            Err(e) => {
                yield Ok(Event::default().event("error").data(format!("Generation failed: {}", e)));
            }
        }
    };

    Sse::new(stream)
}
