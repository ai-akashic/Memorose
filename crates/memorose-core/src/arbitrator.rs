use memorose_common::{MemoryUnit, GraphEdge, RelationType};
use memorose_common::config::AppConfig;
use crate::llm::LLMClient;
use anyhow::Result;
use std::sync::Arc;

/// Approximate character budget for LLM prompts (~25k tokens at ~4 chars/token).
/// Keeps batches within context window limits for all supported models.
const MAX_CONTEXT_CHARS: usize = 100_000;

/// Build a memory context string from an iterator of formatted entries,
/// stopping before exceeding MAX_CONTEXT_CHARS.
fn build_bounded_context<'a>(entries: impl Iterator<Item = String>, separator: &str) -> (String, usize, usize) {
    let mut context = String::new();
    let mut included = 0;
    let mut total = 0;
    for entry in entries {
        total += 1;
        let needed = if context.is_empty() { entry.len() } else { separator.len() + entry.len() };
        if context.len() + needed > MAX_CONTEXT_CHARS {
            break; // budget exhausted; subsequent entries won't fit either
        }
        if !context.is_empty() {
            context.push_str(separator);
        }
        context.push_str(&entry);
        included += 1;
    }
    (context, included, total)
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct CommunityInsight {
    pub name: String,
    pub summary: String,
    #[serde(default)]
    pub keywords: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct MilestoneDTO {
    pub summary: String,
    pub dependencies: Vec<String>,
}

#[derive(Clone)]
pub struct Arbitrator {
    llm_client: Option<Arc<dyn LLMClient>>,
}

impl Arbitrator {
    pub async fn decompose_goal(&self, user_id: &str, app_id: &str, stream_id: uuid::Uuid, goal: &str) -> Result<Vec<MemoryUnit>> {
        let client = match &self.llm_client {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

        let system_prompt = "You are a strategic AI planner. \
            Decompose the following high-level Goal (L3) into a set of 3-5 actionable Milestones (L2). \
            For each milestone, identify its dependencies (which other milestones must be completed first). \
            \
            Output format (JSON): \
            [{\"summary\": \"milestone summary\", \"dependencies\": [\"milestone_name_x\"]}]";

        let combined_prompt = format!("{}\n\nGoal: {}", system_prompt, goal);
        let result = client.generate(&combined_prompt).await?;

        let clean_json = result.data.trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();

        let milestones: Vec<MilestoneDTO> = serde_json::from_str(clean_json).unwrap_or_default();

        let mut units = Vec::new();
        for m in milestones {
            let mut unit = MemoryUnit::new(
                user_id.to_string(), 
                None, // agent_id (topics are factual derivations)
                app_id.to_string(), 
                stream_id, 
                memorose_common::MemoryType::Factual,
                m.summary, 
                None
            );
            unit.level = 2;
            unit.task_metadata = Some(memorose_common::TaskMetadata {
                status: memorose_common::TaskStatus::Pending,
                progress: 0.0,
            });
            units.push(unit);
        }

        Ok(units)
    }

    pub fn new() -> Self {
        let config = AppConfig::load().unwrap_or_else(|e| {
            tracing::warn!("Failed to load config for Arbitrator ({}), using defaults", e);
            AppConfig::default()
        });
        
        let llm_client = crate::llm::create_llm_client(&config.llm);
        
        if llm_client.is_none() {
            tracing::warn!("Arbitrator initialized without API Key or provider. Conflict resolution will be disabled (Pass-through mode).");
        }
        Self { llm_client }
    }

    pub fn with_client(client: Arc<dyn LLMClient>) -> Self {
        Self { llm_client: Some(client) }
    }

    pub fn get_llm_client(&self) -> Option<Arc<dyn LLMClient>> {
        self.llm_client.clone()
    }

    // ... (existing arbitrate, consolidate, extract_topics, analyze_relations methods)

    pub async fn arbitrate(&self, memories: Vec<MemoryUnit>, query: Option<&str>) -> Result<Vec<MemoryUnit>> {
        // ... (existing implementation)
        let client = match &self.llm_client {
            Some(c) => c,
            None => return Ok(memories),
        };

        if memories.len() <= 1 {
            return Ok(memories);
        }

        // Prepare prompt with memories, IDs and timestamps
        let (memory_context, included, total) = build_bounded_context(
            memories.iter().map(|m| format!("ID: {}\nTimestamp: {}\nContent: {}", m.id, m.transaction_time, m.content)),
            "\n---\n",
        );
        if included < total {
            tracing::warn!("Arbitrator: truncated context to {}/{} memories to stay within token budget", included, total);
        }

        let query_str = query.map(|q| format!("User Query: {}\n", q)).unwrap_or_else(|| "No specific query, just identify latest facts.".to_string());

        let system_prompt = "You are a conflict resolution system for an AI memory database. \
            Analyze the following retrieved memories. Identify any factual conflicts. \
            \
            CRITICAL INSTRUCTION ON CONFLICTS: \
            1. If the User Query asks for 'history', 'changes', 'evolution', or 'steps', YOU MUST RETAIN ALL CONFLICTING VERSIONS (old and new) to show the timeline. \
            2. If the User Query asks for 'initial', 'first', 'earliest', 'original', 'start', or 'oldest', YOU MUST RETAIN ONLY THE OLDEST VERSION (based on Timestamp). \
            3. If the User Query asks for a SPECIFIC fact/date/detail (e.g., 'When did I say X?', 'What about the 18th?'), YOU MUST RETAIN ONLY MEMORIES MATCHING THAT SPECIFIC DETAIL. Filter out other versions (even if newer) unless they directly reference the specific detail requested. \
            4. If the User Query asks for 'current', 'latest', 'now', 'final', or is neutral, FAVOR THE MOST RECENT INFORMATION (based on Timestamp) and filter out obsolete facts. \
            \
            If no conflicts exist, keep all memories. \
            Return ONLY the IDs of the memories that should be RETAINED, separated by commas. \
            Do not explain.";
        
        let user_prompt = format!("{}\nMemories:\n{}", query_str, memory_context);

        let combined_prompt = format!("{}\n\n{}", system_prompt, user_prompt);
        let result = match client.generate(&combined_prompt).await {
            Ok(r) => r.data,
            Err(e) => {
                tracing::warn!("Arbitrator LLM call failed: {:?}. Falling back to pass-through.", e);
                return Ok(memories);
            }
        };

        // Parse IDs from result
        let retained_ids: Vec<String> = result
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        // Filter original memories
        let filtered: Vec<MemoryUnit> = memories.into_iter()
            .filter(|m| retained_ids.contains(&m.id.to_string()))
            .collect();

        Ok(filtered)
    }

    /// Synthesize a single coherent narrative from a set of memories, resolving conflicts and preserving history.
    pub async fn consolidate(&self, memories: Vec<MemoryUnit>) -> Result<String> {
        if memories.is_empty() {
            return Ok(String::new());
        }

        // Fallback if no LLM
        let client = match &self.llm_client {
            Some(c) => c,
            None => {
                // Simple concatenation fallback
                return Ok(memories.iter()
                    .map(|m| m.content.clone())
                    .collect::<Vec<_>>()
                    .join("\n"));
            }
        };

        let (memory_context, included, total) = build_bounded_context(
            memories.iter().map(|m| format!("Timestamp: {}\nContent: {}", m.transaction_time, m.content)),
            "\n---\n",
        );
        if included < total {
            tracing::warn!("Consolidate: truncated context to {}/{} memories to stay within token budget", included, total);
        }

        let system_prompt = "You are a memory consolidation engine. \
            Analyze the following memories which may contain updates, corrections, or evolution of facts. \
            Synthesize a SINGLE coherent narrative paragraph that answers the user's intent based on these memories. \
            Crucially, if there are changes (e.g., plans changed from date A to date B), EXPLICITLY mention the history of the change (e.g., 'Initially A, but changed to B'). \
            Do not omit the history if it helps context. \
            Return ONLY the consolidated text.";

        let user_prompt = format!("Memories:\n{}", memory_context);

        let combined_prompt = format!("{}\n\n{}", system_prompt, user_prompt);
        match client.generate(&combined_prompt).await {
            Ok(res) => Ok(res.data),
            Err(e) => {
                tracing::warn!("Consolidation failed: {:?}. Returning concatenation.", e);
                Ok(memories.iter().map(|m| m.content.clone()).collect::<Vec<_>>().join("\n"))
            }
        }
    }

    /// Prospective Reflection: Analyze a set of memories (usually from a single session)
    /// and extract/summarize them into topic-based MemoryUnits (Level 2).
    pub async fn extract_topics(&self, user_id: &str, app_id: &str, stream_id: uuid::Uuid, memories: Vec<MemoryUnit>) -> Result<Vec<MemoryUnit>> {
        let client = match &self.llm_client {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

        if memories.is_empty() {
            return Ok(Vec::new());
        }

        let (memories_str, included, total) = build_bounded_context(
            memories.iter().map(|m| format!("ID: {}\nContent: {}", m.id, m.content)),
            "\n---\n",
        );
        if included < total {
            tracing::warn!("extract_topics: truncated context to {}/{} memories to stay within token budget", included, total);
        }

        let system_prompt = "You are a Memory Management System (Prospective Reflection). \
            Analyze the following dialogue segments/memories from a recent session. \
            Your goal is to extract 'Topics' that summarize the key information. \
            \
            For each topic identify: \
            1. A concise summary of the topic (e.g., 'User is allergic to penicillin'). \
            2. The original memory IDs that belong to this topic. \
            \
            Output format (JSON): \
            [{\"summary\": \"topic summary\", \"source_ids\": [\"uuid1\", \"uuid2\"]}] \
            \
            Focus on extracting facts, preferences, and long-term insights. Skip trivial chitchat.";

        let combined_prompt = format!("{}\n\n{}", system_prompt, memories_str);
        let result = match client.generate(&combined_prompt).await {
            Ok(res) => res.data,
            Err(e) => {
                tracing::error!("LLM generate failed for extract_topics: {:?}", e);
                return Ok(Vec::new());
            }
        };

        let clean_json = result.trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();

        #[derive(serde::Deserialize)]
        struct TopicDTO {
            summary: String,
            source_ids: Vec<String>,
        }

        let dtos: Vec<TopicDTO> = match serde_json::from_str(clean_json) {
            Ok(d) => d,
            Err(e) => {
                tracing::error!("Failed to parse topics JSON: {:?}. Raw response: {}", e, clean_json);
                Vec::new()
            }
        };
        
        let mut topic_units = Vec::new();
        for dto in dtos {
            let mut unit = MemoryUnit::new(
                user_id.to_string(), 
                None, // agent_id
                app_id.to_string(), 
                stream_id, 
                memorose_common::MemoryType::Factual,
                dto.summary, 
                None
            );
            unit.level = 2; // Level 2: Topic/Insight

            // Map source IDs to references
            for id_str in dto.source_ids {
                if let Ok(id) = uuid::Uuid::parse_str(&id_str) {
                    unit.references.push(id);
                }
            }
            topic_units.push(unit);
        }

        if !topic_units.is_empty() {
            tracing::info!("Generated {} L2 topics for user {} stream {}", topic_units.len(), user_id, stream_id);
        }

        Ok(topic_units)
    }

    /// Analyze a new memory against context memories to find semantic relationships (Edge creation).
    pub async fn analyze_relations(&self, new_memory: &MemoryUnit, context_memories: &[MemoryUnit]) -> Result<Vec<GraphEdge>> {
        let client = match &self.llm_client {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

        if context_memories.is_empty() {
            return Ok(Vec::new());
        }

        let (context_str, included, total) = build_bounded_context(
            context_memories.iter().map(|m| format!("ID: {}\nContent: {}", m.id, m.content)),
            "\n---\n",
        );
        if included < total {
            tracing::warn!("analyze_relations: truncated context to {}/{} memories to stay within token budget", included, total);
        }

        let system_prompt = "You are a Knowledge Graph builder. \
            Analyze the 'New Memory' against the 'Context Memories'. \
            Identify relationships between the New Memory and any Context Memory. \
            Output valid relationships in JSON format: \
            [{\"target_id\": \"UUID\", \"relation\": \"RelatedTo|CausedBy|EvolvedTo|DerivedFrom\", \"weight\": 0.0-1.0}] \
            'EvolvedTo': Use when the new memory updates or replaces the old one. \
            'RelatedTo': Use when they share the same subject (e.g., 'I am X' and 'I go home'). \
            If no strong relation, return empty list []. Return ONLY JSON.";

        let user_prompt = format!("Context Memories:\n{}\n\nNew Memory:\nContent: {}", context_str, new_memory.content);

        let combined_prompt = format!("{}\n\n{}", system_prompt, user_prompt);
        let result = match client.generate(&combined_prompt).await {
            Ok(res) => res.data,
            Err(_) => return Ok(Vec::new()),
        };

        // Parse JSON (Naive parsing for MVP)
        // Clean markdown code blocks if present
        let clean_json = result.trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();

        #[derive(serde::Deserialize)]
        struct RelationDTO {
            target_id: String,
            relation: String,
            weight: f32,
        }

        let dtos: Vec<RelationDTO> = serde_json::from_str(clean_json).unwrap_or_default();
        
        let mut edges = Vec::new();
        for dto in dtos {
            if let Ok(target_uuid) = uuid::Uuid::parse_str(&dto.target_id) {
                let rel = match dto.relation.as_str() {
                    "DerivedFrom" => RelationType::DerivedFrom,
                    "CausedBy" => RelationType::CausedBy,
                    "EvolvedTo" => RelationType::EvolvedTo,
                    _ => RelationType::RelatedTo,
                };
                edges.push(GraphEdge::new(new_memory.user_id.clone(), new_memory.id, target_uuid, rel, dto.weight));
            }
        }

        Ok(edges)
    }

    /// Summarize a detected community of memories into a high-level insight.
    pub async fn summarize_community(&self, memories: Vec<String>) -> Result<CommunityInsight> {
        let client = match &self.llm_client {
            Some(c) => c,
            None => return Ok(CommunityInsight { 
                name: "Unknown Community".to_string(), 
                summary: "LLM not available".to_string(),
                keywords: Vec::new(),
            }),
        };

        if memories.is_empty() {
            return Ok(CommunityInsight {
                name: "Empty Community".to_string(),
                summary: "No memories provided.".to_string(),
                keywords: Vec::new(),
            });
        }

        let (memory_block, included, total) = build_bounded_context(memories.into_iter(), "\n---\n");
        if included < total {
            tracing::warn!("summarize_community: truncated context to {}/{} memories to stay within token budget", included, total);
        }
        let system_prompt = "You are a Community Insight Generator. \
            Analyze the following group of related memories (a 'Community'). \
            Identify the common theme that binds them together. \
            \
            Output ONLY valid JSON: \
            {\"name\": \"Short Title (3-5 words)\", \"summary\": \"Comprehensive summary (1-2 paragraphs)\", \"keywords\": [\"k1\", \"k2\", \"k3\"]}";

        let user_prompt = format!("Community Memories:\n{}", memory_block);

        let combined_prompt = format!("{}\n\n{}", system_prompt, user_prompt);
        let result = client.generate(&combined_prompt).await?;

        let clean_json = result.data.trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();

        let insight: CommunityInsight = serde_json::from_str(clean_json)
            .unwrap_or_else(|_| CommunityInsight {
                name: "Parsing Error".to_string(),
                summary: result.data,
                keywords: Vec::new(),
            });

        Ok(insight)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use crate::llm::{LLMClient, CompressionOutput}; // Import CompressionOutput

    struct MockLLM {
        response: String,
    }

    #[async_trait]
    impl LLMClient for MockLLM {
        async fn generate(&self, _prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse { data: self.response.clone(), usage: Default::default() })
        }
        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse { data: vec![0.0; 384], usage: Default::default() })
        }
        async fn compress(&self, text: &str, _is_agent: bool) -> Result<crate::llm::LLMResponse<CompressionOutput>> {
            Ok(crate::llm::LLMResponse { data: CompressionOutput { content: text.to_string(), valid_at: None }, usage: Default::default() })
        }
        async fn summarize_group(&self, _texts: Vec<String>) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse { data: self.response.clone(), usage: Default::default() })
        }
        async fn describe_image(&self, _image_url_or_base64: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse { data: "Description of image".to_string(), usage: Default::default() })
        }
        async fn describe_video(&self, _video_url: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse { data: "Description of video".to_string(), usage: Default::default() })
        }
        async fn transcribe(&self, _audio_url_or_base64: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse { data: "Transcription of audio".to_string(), usage: Default::default() })
        }
    }

    #[tokio::test]
    async fn test_community_summarization() {
        let mock_json = r#"
        {
            "name": "Rust Programming",
            "summary": "The user is learning Rust and enjoys its memory safety features.",
            "keywords": ["Rust", "Memory Safety"]
        }
        "#;
        
        let client = Arc::new(MockLLM { response: mock_json.to_string() });
        let arbitrator = Arbitrator::with_client(client);

        let memories = vec![
            "I started learning Rust yesterday.".to_string(),
            "The borrow checker is tough but useful.".to_string(),
        ];

        let insight = arbitrator.summarize_community(memories).await.unwrap();
        
        assert_eq!(insight.name, "Rust Programming");
        assert!(insight.summary.contains("memory safety"));
    }
}
