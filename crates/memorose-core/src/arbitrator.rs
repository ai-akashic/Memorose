use crate::fact_extraction::{self, MemoryFactDescriptor};
use crate::llm::LLMClient;
use anyhow::Result;
use memorose_common::config::AppConfig;
use memorose_common::{GraphEdge, MemoryUnit, RelationType};
use std::sync::Arc;

/// Approximate character budget for LLM prompts (~25k tokens at ~4 chars/token).
/// Keeps batches within context window limits for all supported models.
const MAX_CONTEXT_CHARS: usize = 100_000;

/// Build a memory context string from an iterator of formatted entries,
/// stopping before exceeding MAX_CONTEXT_CHARS.
fn build_bounded_context<'a>(
    entries: impl Iterator<Item = String>,
    separator: &str,
) -> (String, usize, usize) {
    let mut context = String::new();
    let mut included = 0;
    let mut total = 0;
    for entry in entries {
        total += 1;
        let needed = if context.is_empty() {
            entry.len()
        } else {
            separator.len() + entry.len()
        };
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

#[derive(serde::Serialize)]
struct PromptMemoryFact {
    subject: &'static str,
    subject_key: String,
    attribute: &'static str,
    value: String,
    canonical_value: String,
    value_kind: &'static str,
    comparison_key: String,
    change_type: &'static str,
    confidence: f32,
}

fn prompt_fact_subject(subject: crate::fact_extraction::MemoryFactSubject) -> &'static str {
    match subject {
        crate::fact_extraction::MemoryFactSubject::User => "user",
        crate::fact_extraction::MemoryFactSubject::Organization => "organization",
        crate::fact_extraction::MemoryFactSubject::Agent => "agent",
        crate::fact_extraction::MemoryFactSubject::External => "external",
    }
}

fn prompt_fact_attribute(attribute: crate::fact_extraction::MemoryFactAttribute) -> &'static str {
    match attribute {
        crate::fact_extraction::MemoryFactAttribute::Residence => "residence",
        crate::fact_extraction::MemoryFactAttribute::Preference => "preference",
        crate::fact_extraction::MemoryFactAttribute::Employment => "employment",
        crate::fact_extraction::MemoryFactAttribute::Relationship => "relationship",
        crate::fact_extraction::MemoryFactAttribute::Status => "status",
        crate::fact_extraction::MemoryFactAttribute::Contact => "contact",
        crate::fact_extraction::MemoryFactAttribute::Ownership => "ownership",
        crate::fact_extraction::MemoryFactAttribute::Skill => "skill",
        crate::fact_extraction::MemoryFactAttribute::Schedule => "schedule",
    }
}

fn prompt_fact_value_kind(value_kind: crate::fact_extraction::MemoryFactValueKind) -> &'static str {
    match value_kind {
        crate::fact_extraction::MemoryFactValueKind::Freeform => "freeform",
        crate::fact_extraction::MemoryFactValueKind::Email => "email",
        crate::fact_extraction::MemoryFactValueKind::Phone => "phone",
        crate::fact_extraction::MemoryFactValueKind::City => "city",
        crate::fact_extraction::MemoryFactValueKind::OrganizationName => "organization_name",
        crate::fact_extraction::MemoryFactValueKind::PersonName => "person_name",
        crate::fact_extraction::MemoryFactValueKind::Title => "title",
        crate::fact_extraction::MemoryFactValueKind::SkillName => "skill_name",
        crate::fact_extraction::MemoryFactValueKind::DateTimeLike => "datetime_like",
        crate::fact_extraction::MemoryFactValueKind::AssetName => "asset_name",
    }
}

fn prompt_fact_change_type(
    change_type: crate::fact_extraction::MemoryFactChangeType,
) -> &'static str {
    match change_type {
        crate::fact_extraction::MemoryFactChangeType::Update => "update",
        crate::fact_extraction::MemoryFactChangeType::Contradiction => "contradiction",
        crate::fact_extraction::MemoryFactChangeType::Negation => "negation",
        crate::fact_extraction::MemoryFactChangeType::Historical => "historical",
        crate::fact_extraction::MemoryFactChangeType::Reaffirm => "reaffirm",
        crate::fact_extraction::MemoryFactChangeType::Addition => "addition",
    }
}

fn prompt_facts_for_memory(memory: &MemoryUnit) -> Vec<PromptMemoryFact> {
    fact_extraction::detect_memory_facts(memory)
        .into_iter()
        .map(|fact: MemoryFactDescriptor| PromptMemoryFact {
            subject: prompt_fact_subject(fact.subject),
            subject_key: fact.subject_key,
            attribute: prompt_fact_attribute(fact.attribute),
            value: fact.value,
            canonical_value: fact.canonical_value,
            value_kind: prompt_fact_value_kind(fact.value_kind),
            comparison_key: fact.value_payload.comparison_key().to_string(),
            change_type: prompt_fact_change_type(fact.change_type),
            confidence: fact.confidence as f32 / 100.0,
        })
        .collect()
}

fn format_memory_correction_prompt_entry(memory: &MemoryUnit) -> String {
    let facts =
        serde_json::to_string(&prompt_facts_for_memory(memory)).unwrap_or_else(|_| "[]".into());
    format!(
        "ID: {}\nTimestamp: {}\nContent: {}\nFacts: {}",
        memory.id, memory.transaction_time, memory.content, facts
    )
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryCorrectionKind {
    Obsolete,
    Contradicts,
    Reaffirm,
    Ignore,
}

impl MemoryCorrectionKind {
    pub fn relation(self) -> Option<RelationType> {
        match self {
            Self::Obsolete => Some(RelationType::EvolvedTo),
            Self::Contradicts => Some(RelationType::Contradicts),
            Self::Reaffirm | Self::Ignore => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryCorrectionAction {
    pub target_id: uuid::Uuid,
    pub kind: MemoryCorrectionKind,
    pub reason: String,
    pub confidence: f32,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct ExtractedMemoryFact {
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_name: Option<String>,
    pub attribute: String,
    pub value: String,
    pub change_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub polarity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_span: Option<String>,
    #[serde(default = "default_extracted_fact_confidence")]
    pub confidence: f32,
}

fn default_extracted_fact_confidence() -> f32 {
    0.5
}

#[derive(Clone)]
pub struct Arbitrator {
    llm_client: Option<Arc<dyn LLMClient>>,
}

impl Arbitrator {
    pub async fn extract_memory_facts(
        &self,
        memory: &MemoryUnit,
    ) -> Result<Vec<ExtractedMemoryFact>> {
        let client = match &self.llm_client {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

        let system_prompt = "You are a memory fact extraction engine. \
            Extract zero or more structured facts from the memory when possible. \
            Return ONLY valid JSON in one of these formats: \
            {\"facts\":[{\"subject\":\"user|organization|agent|external\",\"subject_ref\":\"canonical subject ref like user:self|organization:acme or null\",\"subject_name\":\"surface subject text or null\",\"attribute\":\"residence|preference|employment|relationship|status|contact|ownership|skill|schedule\",\"value\":\"canonical value\",\"change_type\":\"update|contradiction|negation|historical|reaffirm|addition\",\"temporal_status\":\"current|historical|negated|null\",\"polarity\":\"positive|negative|null\",\"evidence_span\":\"short supporting phrase|null\",\"confidence\":0.0-1.0}]} \
            OR [{\"subject\":\"user|organization|agent|external\",\"subject_ref\":\"canonical subject ref like user:self|organization:acme or null\",\"subject_name\":\"surface subject text or null\",\"attribute\":\"residence|preference|employment|relationship|status|contact|ownership|skill|schedule\",\"value\":\"canonical value\",\"change_type\":\"update|contradiction|negation|historical|reaffirm|addition\",\"temporal_status\":\"current|historical|negated|null\",\"polarity\":\"positive|negative|null\",\"evidence_span\":\"short supporting phrase|null\",\"confidence\":0.0-1.0}] \
            OR null when no stable fact is present.";

        let user_prompt = format!(
            "Memory:\nTimestamp: {}\nContent: {}\nKeywords: {}",
            memory.transaction_time,
            memory.content,
            memory.keywords.join(", ")
        );

        let result = match client
            .generate(&format!("{}\n\n{}", system_prompt, user_prompt))
            .await
        {
            Ok(response) => response.data,
            Err(error) => {
                tracing::warn!(
                    "Memory fact extraction LLM call failed: {:?}. Skipping extraction fallback.",
                    error
                );
                return Ok(Vec::new());
            }
        };

        let clean_json = result
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();

        if clean_json.eq_ignore_ascii_case("null") || clean_json.is_empty() {
            return Ok(Vec::new());
        }

        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum ExtractedFactsEnvelope {
            Facts { facts: Vec<ExtractedMemoryFact> },
            List(Vec<ExtractedMemoryFact>),
            Single(ExtractedMemoryFact),
        }

        let extracted = match serde_json::from_str::<ExtractedFactsEnvelope>(clean_json) {
            Ok(ExtractedFactsEnvelope::Facts { facts }) => facts,
            Ok(ExtractedFactsEnvelope::List(facts)) => facts,
            Ok(ExtractedFactsEnvelope::Single(fact)) => vec![fact],
            Err(_) => Vec::new(),
        };

        Ok(extracted
            .into_iter()
            .filter(|fact| {
                !fact.subject.trim().is_empty()
                    && !fact.attribute.trim().is_empty()
                    && !fact.value.trim().is_empty()
                    && !fact.change_type.trim().is_empty()
            })
            .collect())
    }

    pub async fn extract_memory_fact(
        &self,
        memory: &MemoryUnit,
    ) -> Result<Option<ExtractedMemoryFact>> {
        Ok(self.extract_memory_facts(memory).await?.into_iter().next())
    }

    pub async fn decompose_goal(
        &self,
        org_id: Option<&str>,
        user_id: &str,
        agent_id: Option<&str>,
        _stream_id: uuid::Uuid,
        goal: &str,
    ) -> Result<Vec<memorose_common::L3Task>> {
        let client = match &self.llm_client {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

        let system_prompt = "You are a strategic AI planner. \
            Decompose the following high-level Goal (L3) into a set of 3-5 actionable Milestones (L3Tasks). \
            For each milestone, identify its dependencies (which other milestones must be completed first). \
            \
            Output format (JSON): \
            [{\"summary\": \"milestone title\", \"description\": \"Detailed action plan for this milestone\", \"dependencies\": [\"milestone_title_x\"]}]";

        let combined_prompt = format!("{}\n\nGoal: {}", system_prompt, goal);
        let result = client.generate(&combined_prompt).await?;

        let clean_json = result
            .data
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();

        #[derive(serde::Deserialize)]
        struct MilestoneDTO {
            summary: String,
            #[serde(default)]
            description: String,
            #[serde(default)]
            dependencies: Vec<String>,
        }

        let milestones: Vec<MilestoneDTO> = serde_json::from_str(clean_json).unwrap_or_default();

        let mut tasks = Vec::new();
        // Create tasks first to get their UUIDs
        let mut title_to_id = std::collections::HashMap::new();

        for m in &milestones {
            let task = memorose_common::L3Task::new(
                org_id.map(|s| s.to_string()),
                user_id.to_string(),
                agent_id.map(|s| s.to_string()),
                m.summary.clone(),
                if m.description.is_empty() {
                    m.summary.clone()
                } else {
                    m.description.clone()
                },
            );
            title_to_id.insert(m.summary.clone(), task.task_id);
            tasks.push((task, m.dependencies.clone()));
        }

        // Second pass: wire dependencies
        let mut final_tasks = Vec::new();
        for (mut task, deps) in tasks {
            for dep_title in deps {
                if let Some(dep_id) = title_to_id.get(&dep_title) {
                    task.dependencies.push(*dep_id);
                }
            }
            final_tasks.push(task);
        }

        Ok(final_tasks)
    }

    pub fn new() -> Self {
        let config = AppConfig::load().unwrap_or_else(|e| {
            tracing::warn!(
                "Failed to load config for Arbitrator ({}), using defaults",
                e
            );
            AppConfig::default()
        });

        let llm_client = crate::llm::create_llm_client(&config.llm);

        if llm_client.is_none() {
            tracing::warn!("Arbitrator initialized without API Key or provider. Conflict resolution will be disabled (Pass-through mode).");
        }
        Self { llm_client }
    }

    pub fn with_client(client: Arc<dyn LLMClient>) -> Self {
        Self {
            llm_client: Some(client),
        }
    }

    pub fn get_llm_client(&self) -> Option<Arc<dyn LLMClient>> {
        self.llm_client.clone()
    }

    // ... (existing arbitrate, consolidate, extract_topics, analyze_relations methods)

    pub async fn arbitrate(
        &self,
        memories: Vec<MemoryUnit>,
        query: Option<&str>,
    ) -> Result<Vec<MemoryUnit>> {
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
            memories.iter().map(|m| {
                format!(
                    "ID: {}\nTimestamp: {}\nContent: {}",
                    m.id, m.transaction_time, m.content
                )
            }),
            "\n---\n",
        );
        if included < total {
            tracing::warn!(
                "Arbitrator: truncated context to {}/{} memories to stay within token budget",
                included,
                total
            );
        }

        let query_str = query
            .map(|q| format!("User Query: {}\n", q))
            .unwrap_or_else(|| "No specific query, just identify latest facts.".to_string());

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
                tracing::warn!(
                    "Arbitrator LLM call failed: {:?}. Falling back to pass-through.",
                    e
                );
                return Ok(memories);
            }
        };

        // Parse IDs from result
        let retained_ids: Vec<String> = result.split(',').map(|s| s.trim().to_string()).collect();

        // Filter original memories
        let filtered: Vec<MemoryUnit> = memories
            .into_iter()
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
                return Ok(memories
                    .iter()
                    .map(|m| m.content.clone())
                    .collect::<Vec<_>>()
                    .join("\n"));
            }
        };

        let (memory_context, included, total) = build_bounded_context(
            memories
                .iter()
                .map(|m| format!("Timestamp: {}\nContent: {}", m.transaction_time, m.content)),
            "\n---\n",
        );
        if included < total {
            tracing::warn!(
                "Consolidate: truncated context to {}/{} memories to stay within token budget",
                included,
                total
            );
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
                Ok(memories
                    .iter()
                    .map(|m| m.content.clone())
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
        }
    }

    /// Prospective Reflection: Analyze a set of memories (usually from a single session)
    /// and extract/summarize them into topic-based MemoryUnits (Level 2).
    pub async fn extract_topics(
        &self,
        user_id: &str,
        stream_id: uuid::Uuid,
        memories: Vec<MemoryUnit>,
    ) -> Result<Vec<MemoryUnit>> {
        let client = match &self.llm_client {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

        if memories.is_empty() {
            return Ok(Vec::new());
        }

        let (memories_str, included, total) = build_bounded_context(
            memories
                .iter()
                .map(|m| format!("ID: {}\nContent: {}", m.id, m.content)),
            "\n---\n",
        );
        if included < total {
            tracing::warn!(
                "extract_topics: truncated context to {}/{} memories to stay within token budget",
                included,
                total
            );
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

        let clean_json = result
            .trim()
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
                tracing::error!(
                    "Failed to parse topics JSON: {:?}. Raw response: {}",
                    e,
                    clean_json
                );
                Vec::new()
            }
        };

        let mut topic_units = Vec::new();
        for dto in dtos {
            let mut unit = MemoryUnit::new(
                None,
                user_id.to_string(),
                None, // agent_id
                stream_id,
                memorose_common::MemoryType::Factual,
                dto.summary,
                None,
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
            tracing::info!(
                "Generated {} L2 topics for user {} stream {}",
                topic_units.len(),
                user_id,
                stream_id
            );
        }

        Ok(topic_units)
    }

    /// Analyze a new memory against context memories to find semantic relationships (Edge creation).
    pub async fn analyze_relations(
        &self,
        new_memory: &MemoryUnit,
        context_memories: &[MemoryUnit],
    ) -> Result<Vec<GraphEdge>> {
        let client = match &self.llm_client {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

        if context_memories.is_empty() {
            return Ok(Vec::new());
        }

        let (context_str, included, total) = build_bounded_context(
            context_memories
                .iter()
                .map(|m| format!("ID: {}\nContent: {}", m.id, m.content)),
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

        let user_prompt = format!(
            "Context Memories:\n{}\n\nNew Memory:\nContent: {}",
            context_str, new_memory.content
        );

        let combined_prompt = format!("{}\n\n{}", system_prompt, user_prompt);
        let result = match client.generate(&combined_prompt).await {
            Ok(res) => res.data,
            Err(_) => return Ok(Vec::new()),
        };

        // Parse JSON (Naive parsing for MVP)
        // Clean markdown code blocks if present
        let clean_json = result
            .trim()
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
                edges.push(GraphEdge::new(
                    new_memory.user_id.clone(),
                    new_memory.id,
                    target_uuid,
                    rel,
                    dto.weight,
                ));
            }
        }

        Ok(edges)
    }

    /// Compare a new factual memory against existing user memories and return
    /// structured correction actions for obsolete or contradictory historical facts.
    pub async fn detect_memory_corrections(
        &self,
        new_memory: &MemoryUnit,
        context_memories: &[MemoryUnit],
    ) -> Result<Vec<MemoryCorrectionAction>> {
        let client = match &self.llm_client {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

        if context_memories.is_empty() {
            return Ok(Vec::new());
        }

        let (context_str, included, total) = build_bounded_context(
            context_memories
                .iter()
                .map(format_memory_correction_prompt_entry),
            "\n---\n",
        );
        if included < total {
            tracing::warn!(
                "detect_memory_corrections: truncated context to {}/{} memories to stay within token budget",
                included,
                total
            );
        }

        let system_prompt = "You are a memory correction engine. \
            Compare the New Memory against Existing Memories and identify old facts that are now obsolete or contradicted. \
            Return ONLY valid JSON in this format: \
            [{\"target_id\":\"UUID\",\"action\":\"OBSOLETE|CONTRADICTS|REAFFIRM|IGNORE\",\"reason\":\"short reason\",\"confidence\":0.0-1.0}] \
            Use OBSOLETE when the new memory updates/replaces an older fact. \
            Use CONTRADICTS only when both versions should remain linked as conflicting claims. \
            Use REAFFIRM when the new memory simply confirms the existing fact and no mutation is needed. \
            Use IGNORE when the candidate is irrelevant or no correction should be applied. \
            Return [] when no correction is needed.";

        let user_prompt = format!(
            "Existing Memories:\n{}\n\nNew Memory:\n{}",
            context_str,
            format_memory_correction_prompt_entry(new_memory)
        );

        let result = match client
            .generate(&format!("{}\n\n{}", system_prompt, user_prompt))
            .await
        {
            Ok(response) => response.data,
            Err(error) => {
                tracing::warn!(
                    "Memory correction LLM call failed: {:?}. Skipping correction pass.",
                    error
                );
                return Ok(Vec::new());
            }
        };

        let clean_json = result
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();

        #[derive(serde::Deserialize)]
        struct MemoryCorrectionDTO {
            target_id: String,
            action: String,
            #[serde(default)]
            reason: String,
            #[serde(default = "default_correction_confidence")]
            confidence: f32,
        }

        fn default_correction_confidence() -> f32 {
            1.0
        }

        let dtos: Vec<MemoryCorrectionDTO> = serde_json::from_str(clean_json).unwrap_or_default();
        let mut actions = Vec::new();
        for dto in dtos {
            let Ok(target_id) = uuid::Uuid::parse_str(&dto.target_id) else {
                continue;
            };
            let kind = match dto.action.as_str() {
                "OBSOLETE" => MemoryCorrectionKind::Obsolete,
                "CONTRADICTS" => MemoryCorrectionKind::Contradicts,
                "REAFFIRM" => MemoryCorrectionKind::Reaffirm,
                "IGNORE" => MemoryCorrectionKind::Ignore,
                _ => continue,
            };
            actions.push(MemoryCorrectionAction {
                target_id,
                kind,
                reason: dto.reason,
                confidence: dto.confidence.clamp(0.0, 1.0),
            });
        }

        Ok(actions)
    }

    /// Summarize a detected community of memories into a high-level insight.
    pub async fn summarize_community(&self, memories: Vec<String>) -> Result<CommunityInsight> {
        let client = match &self.llm_client {
            Some(c) => c,
            None => {
                return Ok(CommunityInsight {
                    name: "Unknown Community".to_string(),
                    summary: "LLM not available".to_string(),
                    keywords: Vec::new(),
                })
            }
        };

        if memories.is_empty() {
            return Ok(CommunityInsight {
                name: "Empty Community".to_string(),
                summary: "No memories provided.".to_string(),
                keywords: Vec::new(),
            });
        }

        let (memory_block, included, total) =
            build_bounded_context(memories.into_iter(), "\n---\n");
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

        let clean_json = result
            .data
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();

        let insight: CommunityInsight =
            serde_json::from_str(clean_json).unwrap_or_else(|_| CommunityInsight {
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
    use crate::fact_extraction::{
        MemoryFactAttribute, MemoryFactChangeType, MemoryFactSubject, MemoryFactValueKind,
    };
    use crate::llm::{CompressionOutput, LLMClient};
    use async_trait::async_trait; // Import CompressionOutput
    use std::sync::Mutex;

    struct MockLLM {
        response: String,
    }

    struct PromptCaptureLLM {
        response: String,
        prompts: Arc<Mutex<Vec<String>>>,
    }

    struct ErrorLLM;

    #[async_trait]
    impl LLMClient for MockLLM {
        async fn generate(&self, _prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: self.response.clone(),
                usage: Default::default(),
            })
        }
        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 384],
                usage: Default::default(),
            })
        }
        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: Default::default(),
            })
        }
        async fn summarize_group(
            &self,
            _texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: self.response.clone(),
                usage: Default::default(),
            })
        }
        async fn describe_image(
            &self,
            _image_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "Description of image".to_string(),
                usage: Default::default(),
            })
        }
        async fn describe_video(
            &self,
            _video_url: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "Description of video".to_string(),
                usage: Default::default(),
            })
        }
        async fn transcribe(
            &self,
            _audio_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "Transcription of audio".to_string(),
                usage: Default::default(),
            })
        }
    }

    #[async_trait]
    impl LLMClient for PromptCaptureLLM {
        async fn generate(&self, prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            self.prompts.lock().unwrap().push(prompt.to_string());
            Ok(crate::llm::LLMResponse {
                data: self.response.clone(),
                usage: Default::default(),
            })
        }
        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 384],
                usage: Default::default(),
            })
        }
        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: Default::default(),
            })
        }
        async fn summarize_group(
            &self,
            _texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: self.response.clone(),
                usage: Default::default(),
            })
        }
        async fn describe_image(
            &self,
            _image_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "Description of image".to_string(),
                usage: Default::default(),
            })
        }
        async fn describe_video(
            &self,
            _video_url: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "Description of video".to_string(),
                usage: Default::default(),
            })
        }
        async fn transcribe(
            &self,
            _audio_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "Transcription of audio".to_string(),
                usage: Default::default(),
            })
        }
    }

    #[async_trait]
    impl LLMClient for ErrorLLM {
        async fn generate(&self, _prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            Err(anyhow::anyhow!("mock llm failure"))
        }
        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse {
                data: vec![0.0; 384],
                usage: Default::default(),
            })
        }
        async fn compress(
            &self,
            text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<CompressionOutput>> {
            Ok(crate::llm::LLMResponse {
                data: CompressionOutput {
                    content: text.to_string(),
                    valid_at: None,
                },
                usage: Default::default(),
            })
        }
        async fn summarize_group(
            &self,
            _texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Err(anyhow::anyhow!("mock llm failure"))
        }
        async fn describe_image(
            &self,
            _image_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "Description of image".to_string(),
                usage: Default::default(),
            })
        }
        async fn describe_video(
            &self,
            _video_url: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "Description of video".to_string(),
                usage: Default::default(),
            })
        }
        async fn transcribe(
            &self,
            _audio_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            Ok(crate::llm::LLMResponse {
                data: "Transcription of audio".to_string(),
                usage: Default::default(),
            })
        }
    }

    #[test]
    fn test_arbitrator_helper_mappings_and_budget() {
        assert_eq!(prompt_fact_subject(MemoryFactSubject::User), "user");
        assert_eq!(prompt_fact_subject(MemoryFactSubject::Organization), "organization");
        assert_eq!(prompt_fact_subject(MemoryFactSubject::Agent), "agent");
        assert_eq!(prompt_fact_subject(MemoryFactSubject::External), "external");

        assert_eq!(prompt_fact_attribute(MemoryFactAttribute::Residence), "residence");
        assert_eq!(prompt_fact_attribute(MemoryFactAttribute::Employment), "employment");
        assert_eq!(prompt_fact_attribute(MemoryFactAttribute::Relationship), "relationship");
        assert_eq!(prompt_fact_attribute(MemoryFactAttribute::Status), "status");
        assert_eq!(prompt_fact_attribute(MemoryFactAttribute::Contact), "contact");
        assert_eq!(prompt_fact_attribute(MemoryFactAttribute::Ownership), "ownership");
        assert_eq!(prompt_fact_attribute(MemoryFactAttribute::Skill), "skill");
        assert_eq!(prompt_fact_attribute(MemoryFactAttribute::Schedule), "schedule");

        assert_eq!(prompt_fact_value_kind(MemoryFactValueKind::Email), "email");
        assert_eq!(prompt_fact_value_kind(MemoryFactValueKind::Phone), "phone");
        assert_eq!(
            prompt_fact_value_kind(MemoryFactValueKind::OrganizationName),
            "organization_name"
        );
        assert_eq!(prompt_fact_value_kind(MemoryFactValueKind::PersonName), "person_name");
        assert_eq!(prompt_fact_value_kind(MemoryFactValueKind::Title), "title");
        assert_eq!(prompt_fact_value_kind(MemoryFactValueKind::SkillName), "skill_name");
        assert_eq!(prompt_fact_value_kind(MemoryFactValueKind::DateTimeLike), "datetime_like");
        assert_eq!(prompt_fact_value_kind(MemoryFactValueKind::AssetName), "asset_name");

        assert_eq!(
            prompt_fact_change_type(MemoryFactChangeType::Contradiction),
            "contradiction"
        );
        assert_eq!(prompt_fact_change_type(MemoryFactChangeType::Historical), "historical");
        assert_eq!(prompt_fact_change_type(MemoryFactChangeType::Addition), "addition");

        assert_eq!(MemoryCorrectionKind::Obsolete.relation(), Some(RelationType::EvolvedTo));
        assert_eq!(
            MemoryCorrectionKind::Contradicts.relation(),
            Some(RelationType::Contradicts)
        );
        assert_eq!(MemoryCorrectionKind::Reaffirm.relation(), None);
        assert_eq!(MemoryCorrectionKind::Ignore.relation(), None);

        assert_eq!(default_extracted_fact_confidence(), 0.5);

        let huge = "x".repeat(MAX_CONTEXT_CHARS + 1);
        let (context, included, total) =
            build_bounded_context(vec!["small".to_string(), huge].into_iter(), "\n---\n");
        assert_eq!(context, "small");
        assert_eq!(included, 1);
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_extract_memory_fact_parses_structured_json() {
        let client = Arc::new(MockLLM {
            response: r#"{"subject":"user","attribute":"residence","value":"Beijing","change_type":"update","confidence":0.92}"#.to_string(),
        });
        let arbitrator = Arbitrator::with_client(client);

        let memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Current city: Beijing".into(),
            None,
        );

        let fact = arbitrator.extract_memory_fact(&memory).await.unwrap();

        assert_eq!(
            fact,
            Some(ExtractedMemoryFact {
                subject: "user".into(),
                subject_ref: None,
                subject_name: None,
                attribute: "residence".into(),
                value: "Beijing".into(),
                change_type: "update".into(),
                temporal_status: None,
                polarity: None,
                evidence_span: None,
                confidence: 0.92,
            })
        );
    }

    #[tokio::test]
    async fn test_extract_memory_facts_parses_facts_envelope() {
        let client = Arc::new(MockLLM {
            response: r#"{"facts":[{"subject":"user","attribute":"residence","value":"Beijing","change_type":"update","confidence":0.92},{"subject":"user","attribute":"contact","value":"dylan@example.com","change_type":"reaffirm","confidence":0.81}]}"#.to_string(),
        });
        let arbitrator = Arbitrator::with_client(client);

        let memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing and my email is dylan@example.com".into(),
            None,
        );

        let facts = arbitrator.extract_memory_facts(&memory).await.unwrap();

        assert_eq!(
            facts,
            vec![
                ExtractedMemoryFact {
                    subject: "user".into(),
                    subject_ref: None,
                    subject_name: None,
                    attribute: "residence".into(),
                    value: "Beijing".into(),
                    change_type: "update".into(),
                    temporal_status: None,
                    polarity: None,
                    evidence_span: None,
                    confidence: 0.92,
                },
                ExtractedMemoryFact {
                    subject: "user".into(),
                    subject_ref: None,
                    subject_name: None,
                    attribute: "contact".into(),
                    value: "dylan@example.com".into(),
                    change_type: "reaffirm".into(),
                    temporal_status: None,
                    polarity: None,
                    evidence_span: None,
                    confidence: 0.81,
                }
            ]
        );
    }

    #[tokio::test]
    async fn test_extract_memory_facts_accepts_single_object_for_backward_compat() {
        let client = Arc::new(MockLLM {
            response: r#"{"subject":"user","attribute":"residence","value":"Beijing","change_type":"update","confidence":0.92}"#.to_string(),
        });
        let arbitrator = Arbitrator::with_client(client);

        let memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Current city: Beijing".into(),
            None,
        );

        let facts = arbitrator.extract_memory_facts(&memory).await.unwrap();

        assert_eq!(
            facts,
            vec![ExtractedMemoryFact {
                subject: "user".into(),
                subject_ref: None,
                subject_name: None,
                attribute: "residence".into(),
                value: "Beijing".into(),
                change_type: "update".into(),
                temporal_status: None,
                polarity: None,
                evidence_span: None,
                confidence: 0.92,
            }]
        );
    }

    #[tokio::test]
    async fn test_extract_memory_facts_parses_richer_schema_fields() {
        let client = Arc::new(MockLLM {
            response: r#"{"facts":[{"subject":"organization","subject_ref":"organization:openai","subject_name":"OpenAI","attribute":"status","value":"research partner","change_type":"reaffirm","temporal_status":"current","polarity":"positive","evidence_span":"OpenAI is a research partner","confidence":0.88}]}"#.to_string(),
        });
        let arbitrator = Arbitrator::with_client(client);

        let memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "OpenAI is a research partner".into(),
            None,
        );

        let facts = arbitrator.extract_memory_facts(&memory).await.unwrap();

        assert_eq!(
            facts,
            vec![ExtractedMemoryFact {
                subject: "organization".into(),
                subject_ref: Some("organization:openai".into()),
                subject_name: Some("OpenAI".into()),
                attribute: "status".into(),
                value: "research partner".into(),
                change_type: "reaffirm".into(),
                temporal_status: Some("current".into()),
                polarity: Some("positive".into()),
                evidence_span: Some("OpenAI is a research partner".into()),
                confidence: 0.88,
            }]
        );
    }

    #[tokio::test]
    async fn test_extract_memory_facts_handles_null_markdown_and_invalid_entries() {
        let memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "No stable fact".into(),
            None,
        );

        let null_arbitrator = Arbitrator::with_client(Arc::new(MockLLM {
            response: "null".into(),
        }));
        assert!(null_arbitrator.extract_memory_facts(&memory).await.unwrap().is_empty());

        let fenced_arbitrator = Arbitrator::with_client(Arc::new(MockLLM {
            response: "```json\n[{\"subject\":\"user\",\"attribute\":\"status\",\"value\":\"active\",\"change_type\":\"reaffirm\"}]\n```".into(),
        }));
        let fenced = fenced_arbitrator.extract_memory_facts(&memory).await.unwrap();
        assert_eq!(fenced.len(), 1);
        assert_eq!(fenced[0].confidence, 0.5);

        let invalid_arbitrator = Arbitrator::with_client(Arc::new(MockLLM {
            response: r#"{"facts":[{"subject":"","attribute":"residence","value":"Beijing","change_type":"update"},{"subject":"user","attribute":"residence","value":"","change_type":"update"}]}"#.into(),
        }));
        assert!(invalid_arbitrator
            .extract_memory_facts(&memory)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn test_arbitrator_fallbacks_without_llm() {
        let arbitrator = Arbitrator { llm_client: None };
        let stream_id = uuid::Uuid::new_v4();

        let memories = vec![
            MemoryUnit::new(
                None,
                "test-user".into(),
                None,
                stream_id,
                memorose_common::MemoryType::Factual,
                "first memory".into(),
                None,
            ),
            MemoryUnit::new(
                None,
                "test-user".into(),
                None,
                stream_id,
                memorose_common::MemoryType::Factual,
                "second memory".into(),
                None,
            ),
        ];

        let retained = arbitrator.arbitrate(memories.clone(), Some("latest")).await.unwrap();
        assert_eq!(retained.len(), 2);

        let consolidated = arbitrator.consolidate(memories.clone()).await.unwrap();
        assert_eq!(consolidated, "first memory\nsecond memory");

        let tasks = arbitrator
            .decompose_goal(None, "test-user", None, stream_id, "ship release")
            .await
            .unwrap();
        assert!(tasks.is_empty());

        let topics = arbitrator
            .extract_topics("test-user", stream_id, memories.clone())
            .await
            .unwrap();
        assert!(topics.is_empty());

        let relations = arbitrator
            .analyze_relations(&memories[0], &memories[1..])
            .await
            .unwrap();
        assert!(relations.is_empty());

        let corrections = arbitrator
            .detect_memory_corrections(&memories[0], &memories[1..])
            .await
            .unwrap();
        assert!(corrections.is_empty());

        let insight = arbitrator.summarize_community(vec![]).await.unwrap();
        assert_eq!(insight.name, "Unknown Community");
        assert_eq!(insight.summary, "LLM not available");
    }

    #[tokio::test]
    async fn test_arbitrate_with_llm_filters_retained_ids_and_short_circuits_singleton() {
        let stream_id = uuid::Uuid::new_v4();
        let oldest = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I used to live in Shanghai".into(),
            None,
        );
        let latest = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            None,
        );

        let singleton = Arbitrator::with_client(Arc::new(MockLLM {
            response: latest.id.to_string(),
        }))
        .arbitrate(vec![latest.clone()], Some("latest"))
        .await
        .unwrap();
        assert_eq!(singleton.len(), 1);
        assert_eq!(singleton[0].id, latest.id);

        let retained = Arbitrator::with_client(Arc::new(MockLLM {
            response: latest.id.to_string(),
        }))
        .arbitrate(vec![oldest, latest.clone()], Some("latest"))
        .await
        .unwrap();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].id, latest.id);
    }

    #[tokio::test]
    async fn test_arbitrate_llm_error_falls_back_to_passthrough() {
        let stream_id = uuid::Uuid::new_v4();
        let memories = vec![
            MemoryUnit::new(
                None,
                "test-user".into(),
                None,
                stream_id,
                memorose_common::MemoryType::Factual,
                "first".into(),
                None,
            ),
            MemoryUnit::new(
                None,
                "test-user".into(),
                None,
                stream_id,
                memorose_common::MemoryType::Factual,
                "second".into(),
                None,
            ),
        ];

        let retained = Arbitrator::with_client(Arc::new(ErrorLLM))
            .arbitrate(memories.clone(), Some("latest"))
            .await
            .unwrap();

        assert_eq!(retained.len(), memories.len());
        assert_eq!(retained[0].id, memories[0].id);
        assert_eq!(retained[1].id, memories[1].id);
    }

    #[tokio::test]
    async fn test_consolidate_with_llm_and_error_fallback() {
        let memories = vec![
            MemoryUnit::new(
                None,
                "test-user".into(),
                None,
                uuid::Uuid::new_v4(),
                memorose_common::MemoryType::Factual,
                "first memory".into(),
                None,
            ),
            MemoryUnit::new(
                None,
                "test-user".into(),
                None,
                uuid::Uuid::new_v4(),
                memorose_common::MemoryType::Factual,
                "second memory".into(),
                None,
            ),
        ];

        let consolidated = Arbitrator::with_client(Arc::new(MockLLM {
            response: "Initially first, then second.".into(),
        }))
        .consolidate(memories.clone())
        .await
        .unwrap();
        assert_eq!(consolidated, "Initially first, then second.");

        let empty = Arbitrator::with_client(Arc::new(MockLLM {
            response: "unused".into(),
        }))
        .consolidate(Vec::new())
        .await
        .unwrap();
        assert!(empty.is_empty());

        let fallback = Arbitrator::with_client(Arc::new(ErrorLLM))
            .consolidate(memories)
            .await
            .unwrap();
        assert_eq!(fallback, "first memory\nsecond memory");
    }

    #[tokio::test]
    async fn test_decompose_goal_parses_dependencies_and_defaults_description() {
        let client = Arc::new(MockLLM {
            response: r#"[{"summary":"Plan","dependencies":[]},{"summary":"Execute","description":"Run it","dependencies":["Plan"]}]"#.into(),
        });
        let arbitrator = Arbitrator::with_client(client);
        let tasks = arbitrator
            .decompose_goal(
                Some("org_demo"),
                "test-user",
                Some("agent_x"),
                uuid::Uuid::new_v4(),
                "ship release",
            )
            .await
            .unwrap();

        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].title, "Plan");
        assert_eq!(tasks[0].description, "Plan");
        assert_eq!(tasks[0].org_id.as_deref(), Some("org_demo"));
        assert_eq!(tasks[0].agent_id.as_deref(), Some("agent_x"));
        assert_eq!(tasks[1].title, "Execute");
        assert_eq!(tasks[1].description, "Run it");
        assert_eq!(tasks[1].dependencies, vec![tasks[0].task_id]);
    }

    #[tokio::test]
    async fn test_extract_topics_handles_success_invalid_json_errors_and_empty_inputs() {
        let stream_id = uuid::Uuid::new_v4();
        let source_a = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I moved to Beijing".into(),
            None,
        );
        let source_b = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I work at OpenAI".into(),
            None,
        );

        let success = Arbitrator::with_client(Arc::new(MockLLM {
            response: format!(
                r#"[{{"summary":"User profile","source_ids":["{}","{}","not-a-uuid"]}}]"#,
                source_a.id, source_b.id
            ),
        }))
        .extract_topics("test-user", stream_id, vec![source_a.clone(), source_b.clone()])
        .await
        .unwrap();
        assert_eq!(success.len(), 1);
        assert_eq!(success[0].level, 2);
        assert_eq!(success[0].content, "User profile");
        assert_eq!(success[0].references, vec![source_a.id, source_b.id]);

        let invalid_json = Arbitrator::with_client(Arc::new(MockLLM {
            response: "not-json".into(),
        }))
        .extract_topics("test-user", stream_id, vec![source_a.clone()])
        .await
        .unwrap();
        assert!(invalid_json.is_empty());

        let empty_memories = Arbitrator::with_client(Arc::new(MockLLM {
            response: "[]".into(),
        }))
        .extract_topics("test-user", stream_id, Vec::new())
        .await
        .unwrap();
        assert!(empty_memories.is_empty());

        let llm_error = Arbitrator::with_client(Arc::new(ErrorLLM))
            .extract_topics("test-user", stream_id, vec![source_b])
            .await
            .unwrap();
        assert!(llm_error.is_empty());
    }

    #[tokio::test]
    async fn test_analyze_relations_parses_edges_and_defaults_unknown_relation() {
        let stream_id = uuid::Uuid::new_v4();
        let new_memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I moved to Beijing and changed jobs".into(),
            None,
        );
        let context_a = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I used to live in Shanghai".into(),
            None,
        );
        let context_b = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            stream_id,
            memorose_common::MemoryType::Factual,
            "I started a new role".into(),
            None,
        );

        let edges = Arbitrator::with_client(Arc::new(MockLLM {
            response: format!(
                "```json\n[\n  {{\"target_id\":\"{}\",\"relation\":\"DerivedFrom\",\"weight\":0.8}},\n  {{\"target_id\":\"{}\",\"relation\":\"SomethingElse\",\"weight\":0.4}},\n  {{\"target_id\":\"not-a-uuid\",\"relation\":\"CausedBy\",\"weight\":0.2}}\n]\n```",
                context_a.id, context_b.id
            ),
        }))
        .analyze_relations(&new_memory, &[context_a.clone(), context_b.clone()])
        .await
        .unwrap();

        assert_eq!(edges.len(), 2);
        assert_eq!(edges[0].target_id, context_a.id);
        assert_eq!(edges[0].relation, RelationType::DerivedFrom);
        assert_eq!(edges[1].target_id, context_b.id);
        assert_eq!(edges[1].relation, RelationType::RelatedTo);

        let empty_context = Arbitrator::with_client(Arc::new(MockLLM {
            response: "[]".into(),
        }))
        .analyze_relations(&new_memory, &[])
        .await
        .unwrap();
        assert!(empty_context.is_empty());

        let llm_error = Arbitrator::with_client(Arc::new(ErrorLLM))
            .analyze_relations(&new_memory, &[context_a])
            .await
            .unwrap();
        assert!(llm_error.is_empty());
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

        let client = Arc::new(MockLLM {
            response: mock_json.to_string(),
        });
        let arbitrator = Arbitrator::with_client(client);

        let memories = vec![
            "I started learning Rust yesterday.".to_string(),
            "The borrow checker is tough but useful.".to_string(),
        ];

        let insight = arbitrator.summarize_community(memories).await.unwrap();

        assert_eq!(insight.name, "Rust Programming");
        assert!(insight.summary.contains("memory safety"));
    }

    #[tokio::test]
    async fn test_detect_memory_corrections_parses_obsolete_action() {
        let target_id = uuid::Uuid::new_v4();
        let client = Arc::new(MockLLM {
            response: format!(
                r#"[{{"target_id":"{}","action":"OBSOLETE","reason":"Address updated","confidence":0.91}}]"#,
                target_id
            ),
        });
        let arbitrator = Arbitrator::with_client(client);

        let new_memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            None,
        );
        let old_memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            None,
        );

        let actions = arbitrator
            .detect_memory_corrections(&new_memory, &[old_memory])
            .await
            .unwrap();

        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].target_id, target_id);
        assert_eq!(actions[0].kind, MemoryCorrectionKind::Obsolete);
        assert_eq!(actions[0].reason, "Address updated");
        assert_eq!(actions[0].confidence, 0.91);
    }

    #[tokio::test]
    async fn test_detect_memory_corrections_parses_contradicts_action() {
        let target_id = uuid::Uuid::new_v4();
        let client = Arc::new(MockLLM {
            response: format!(
                r#"[{{"target_id":"{}","action":"CONTRADICTS","reason":"Claims conflict","confidence":0.73}}]"#,
                target_id
            ),
        });
        let arbitrator = Arbitrator::with_client(client);

        let new_memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I have never lived in Shanghai".into(),
            None,
        );
        let old_memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            None,
        );

        let actions = arbitrator
            .detect_memory_corrections(&new_memory, &[old_memory])
            .await
            .unwrap();

        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].target_id, target_id);
        assert_eq!(actions[0].kind, MemoryCorrectionKind::Contradicts);
        assert_eq!(actions[0].reason, "Claims conflict");
        assert_eq!(actions[0].confidence, 0.73);
    }

    #[tokio::test]
    async fn test_detect_memory_corrections_parses_reaffirm_action() {
        let target_id = uuid::Uuid::new_v4();
        let client = Arc::new(MockLLM {
            response: format!(
                r#"[{{"target_id":"{}","action":"REAFFIRM","reason":"Same current fact","confidence":0.67}}]"#,
                target_id
            ),
        });
        let arbitrator = Arbitrator::with_client(client);

        let new_memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I still live in Beijing".into(),
            None,
        );
        let old_memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Beijing".into(),
            None,
        );

        let actions = arbitrator
            .detect_memory_corrections(&new_memory, &[old_memory])
            .await
            .unwrap();

        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].target_id, target_id);
        assert_eq!(actions[0].kind, MemoryCorrectionKind::Reaffirm);
        assert_eq!(actions[0].reason, "Same current fact");
        assert_eq!(actions[0].confidence, 0.67);
    }

    #[tokio::test]
    async fn test_detect_memory_corrections_parses_ignore_action() {
        let target_id = uuid::Uuid::new_v4();
        let client = Arc::new(MockLLM {
            response: format!(
                r#"[{{"target_id":"{}","action":"IGNORE","reason":"Unrelated candidate","confidence":0.41}}]"#,
                target_id
            ),
        });
        let arbitrator = Arbitrator::with_client(client);

        let new_memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            None,
        );
        let old_memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Favorite food is Beijing duck".into(),
            None,
        );

        let actions = arbitrator
            .detect_memory_corrections(&new_memory, &[old_memory])
            .await
            .unwrap();

        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].target_id, target_id);
        assert_eq!(actions[0].kind, MemoryCorrectionKind::Ignore);
        assert_eq!(actions[0].reason, "Unrelated candidate");
        assert_eq!(actions[0].confidence, 0.41);
    }

    #[tokio::test]
    async fn test_detect_memory_corrections_prompt_includes_normalized_facts() {
        let prompts = Arc::new(Mutex::new(Vec::new()));
        let client = Arc::new(PromptCaptureLLM {
            response: "[]".into(),
            prompts: prompts.clone(),
        });
        let arbitrator = Arbitrator::with_client(client);

        let new_memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            None,
        );
        let old_memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "Home base: Shanghai".into(),
            None,
        );

        let _ = arbitrator
            .detect_memory_corrections(&new_memory, &[old_memory])
            .await
            .unwrap();

        let prompt = prompts
            .lock()
            .unwrap()
            .last()
            .cloned()
            .expect("expected generated prompt");

        assert!(prompt.contains("Facts: ["));
        assert!(prompt.contains(r#""attribute":"residence""#));
        assert!(prompt.contains(r#""canonical_value":"beijing""#));
        assert!(prompt.contains(r#""canonical_value":"shanghai""#));
        assert!(prompt.contains(r#""comparison_key":"beijing""#));
    }

    #[tokio::test]
    async fn test_detect_memory_corrections_defaults_confidence_and_skips_invalid_entries() {
        let target_id = uuid::Uuid::new_v4();
        let new_memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I now live in Beijing".into(),
            None,
        );
        let old_memory = MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            uuid::Uuid::new_v4(),
            memorose_common::MemoryType::Factual,
            "I live in Shanghai".into(),
            None,
        );

        let actions = Arbitrator::with_client(Arc::new(MockLLM {
            response: format!(
                "```json\n[\n  {{\"target_id\":\"{}\",\"action\":\"OBSOLETE\",\"reason\":\"updated address\"}},\n  {{\"target_id\":\"{}\",\"action\":\"REAFFIRM\",\"reason\":\"still valid\",\"confidence\":1.7}},\n  {{\"target_id\":\"bad-uuid\",\"action\":\"IGNORE\",\"reason\":\"skip me\"}},\n  {{\"target_id\":\"{}\",\"action\":\"UNKNOWN\",\"reason\":\"skip me too\"}}\n]\n```",
                target_id, old_memory.id, old_memory.id
            ),
        }))
        .detect_memory_corrections(&new_memory, std::slice::from_ref(&old_memory))
        .await
        .unwrap();

        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].target_id, target_id);
        assert_eq!(actions[0].kind, MemoryCorrectionKind::Obsolete);
        assert_eq!(actions[0].confidence, 1.0);
        assert_eq!(actions[1].target_id, old_memory.id);
        assert_eq!(actions[1].kind, MemoryCorrectionKind::Reaffirm);
        assert_eq!(actions[1].confidence, 1.0);

        let empty_context = Arbitrator::with_client(Arc::new(MockLLM {
            response: "[]".into(),
        }))
        .detect_memory_corrections(&new_memory, &[])
        .await
        .unwrap();
        assert!(empty_context.is_empty());

        let llm_error = Arbitrator::with_client(Arc::new(ErrorLLM))
            .detect_memory_corrections(&new_memory, &[old_memory])
            .await
            .unwrap();
        assert!(llm_error.is_empty());
    }

    #[tokio::test]
    async fn test_summarize_community_fallbacks_and_parsing_error_wrapper() {
        let without_llm = Arbitrator { llm_client: None }
            .summarize_community(vec!["memory".into()])
            .await
            .unwrap();
        assert_eq!(without_llm.name, "Unknown Community");
        assert_eq!(without_llm.summary, "LLM not available");

        let empty = Arbitrator::with_client(Arc::new(MockLLM {
            response: "unused".into(),
        }))
        .summarize_community(Vec::new())
        .await
        .unwrap();
        assert_eq!(empty.name, "Empty Community");
        assert_eq!(empty.summary, "No memories provided.");

        let parse_error = Arbitrator::with_client(Arc::new(MockLLM {
            response: "plain text summary".into(),
        }))
        .summarize_community(vec!["one".into(), "two".into()])
        .await
        .unwrap();
        assert_eq!(parse_error.name, "Parsing Error");
        assert_eq!(parse_error.summary, "plain text summary");
        assert!(parse_error.keywords.is_empty());
    }
}
