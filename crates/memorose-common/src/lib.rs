use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

pub mod config;
pub mod sharding;
pub mod video;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStream {
    pub id: Uuid,
    pub transaction_time: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}

impl MemoryStream {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            transaction_time: Utc::now(),
            metadata: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum EventContent {
    Text(String),
    Image(String), // URL
    Audio(String), // URL
    Video(String), // URL
    Json(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub org_id: Option<String>,
    pub user_id: String,
    pub agent_id: Option<String>,
    pub stream_id: Uuid,
    pub content: EventContent,
    pub transaction_time: DateTime<Utc>,
    pub valid_time: Option<DateTime<Utc>>,
    pub metadata: serde_json::Value,
}

impl Event {
    pub fn new(
        org_id: Option<String>,
        user_id: String,
        agent_id: Option<String>,
        stream_id: Uuid,
        content: EventContent,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            org_id,
            user_id,
            agent_id,
            stream_id,
            content,
            transaction_time: Utc::now(),
            valid_time: None,
            metadata: serde_json::json!({}),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RelationType {
    Next, // Temporal sequence
    RelatedTo,
    Contradicts,
    Supports,
    Abstracts,
    DerivedFrom,
    CausedBy,
    EvolvedTo,
    IsSubTaskOf,  // Vertical hierarchy
    Blocks,       // Horizontal dependency
    Accomplishes, // Goal fulfillment
}

impl RelationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            RelationType::Next => "Next",
            RelationType::RelatedTo => "RelatedTo",
            RelationType::Contradicts => "Contradicts",
            RelationType::Supports => "Supports",
            RelationType::Abstracts => "Abstracts",
            RelationType::DerivedFrom => "DerivedFrom",
            RelationType::CausedBy => "CausedBy",
            RelationType::EvolvedTo => "EvolvedTo",
            RelationType::IsSubTaskOf => "IsSubTaskOf",
            RelationType::Blocks => "Blocks",
            RelationType::Accomplishes => "Accomplishes",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
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
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MemoryDomain {
    Agent,
    #[default]
    User,
    Organization,
}

impl MemoryDomain {
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryDomain::Agent => "agent",
            MemoryDomain::User => "user",
            MemoryDomain::Organization => "organization",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    #[default]
    Native,
    Projected,
    Derived,
}

impl EdgeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            EdgeKind::Native => "native",
            EdgeKind::Projected => "projected",
            EdgeKind::Derived => "derived",
        }
    }

    pub fn from_str(value: &str) -> Self {
        match value {
            "projected" => EdgeKind::Projected,
            "derived" => EdgeKind::Derived,
            _ => EdgeKind::Native,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShareTarget {
    Organization,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharePolicy {
    #[serde(default)]
    pub contribute: bool,
    #[serde(default)]
    pub consume: bool,
    #[serde(default)]
    pub include_history: bool,
    #[serde(default)]
    pub targets: Vec<ShareTarget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    Pending,
    InProgress,
    Blocked(String), // Reason for being blocked
    Completed,
    Failed(String), // Reason for failure
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L3Task {
    pub task_id: Uuid,
    pub org_id: Option<String>,
    pub user_id: String,
    pub agent_id: Option<String>,
    pub parent_id: Option<Uuid>, // Hierarchy support

    pub title: String,
    pub description: String,
    pub status: TaskStatus,
    pub progress: f32, // 0.0 - 1.0

    pub dependencies: Vec<Uuid>, // Pre-requisites
    pub context_refs: Vec<Uuid>, // Links to L1/L2 MemoryUnit IDs

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_summary: Option<String>,
}

impl L3Task {
    pub fn new(
        org_id: Option<String>,
        user_id: String,
        agent_id: Option<String>,
        title: String,
        description: String,
    ) -> Self {
        let now = Utc::now();
        Self {
            task_id: Uuid::new_v4(),
            org_id,
            user_id,
            agent_id,
            parent_id: None,
            title,
            description,
            status: TaskStatus::Pending,
            progress: 0.0,
            dependencies: Vec::new(),
            context_refs: Vec::new(),
            created_at: now,
            updated_at: now,
            result_summary: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskMetadata {
    pub status: TaskStatus,
    pub progress: f32, // 0.0 - 1.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphEdge {
    pub source_id: Uuid,
    pub target_id: Uuid,
    pub user_id: String,
    pub namespace_key: String,
    pub source_namespace_key: String,
    pub target_namespace_key: String,
    pub edge_kind: EdgeKind,
    pub relation: RelationType,
    pub weight: f32,
    pub transaction_time: DateTime<Utc>,
}

impl GraphEdge {
    pub fn new(
        user_id: String,
        source: Uuid,
        target: Uuid,
        relation: RelationType,
        weight: f32,
    ) -> Self {
        let namespace_key =
            MemoryUnit::build_namespace_key(&MemoryDomain::User, None, Some(&user_id), None);
        Self::new_scoped(
            user_id,
            source,
            target,
            relation,
            weight,
            namespace_key,
            None,
            None,
            EdgeKind::Native,
        )
    }

    pub fn new_scoped(
        user_id: String,
        source: Uuid,
        target: Uuid,
        relation: RelationType,
        weight: f32,
        namespace_key: String,
        source_namespace_key: Option<String>,
        target_namespace_key: Option<String>,
        edge_kind: EdgeKind,
    ) -> Self {
        Self {
            source_id: source,
            target_id: target,
            user_id,
            namespace_key: namespace_key.clone(),
            source_namespace_key: source_namespace_key.unwrap_or_else(|| namespace_key.clone()),
            target_namespace_key: target_namespace_key.unwrap_or(namespace_key.clone()),
            edge_kind,
            relation,
            weight,
            transaction_time: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
}

/// Metadata for multimodal assets (images, audio, etc.)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Asset {
    pub storage_key: String, // The internal key (e.g., s3://bucket/uuid.png or local://uuid.png)
    pub original_name: String,
    pub asset_type: String, // e.g., "image/png", "audio/mpeg"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ForgetTargetKind {
    MemoryUnit,
    Event,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ForgetMode {
    Logical,
    Hard,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForgettingTombstone {
    pub user_id: String,
    pub org_id: Option<String>,
    pub target_kind: ForgetTargetKind,
    pub target_id: String,
    pub reason_query: String,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_id: Option<String>,
    pub mode: ForgetMode,
}

/// Represents a consolidated memory unit (L1/L2).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryType {
    Factual,    // User facts and preferences
    Procedural, // Agent experiences, reflections, and tool usage paths
}

impl Default for MemoryType {
    fn default() -> Self {
        Self::Factual
    }
}

fn default_stored_fact_confidence() -> f32 {
    0.5
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoredMemoryFact {
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_name: Option<String>,
    pub attribute: String,
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_value: Option<String>,
    pub change_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub polarity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_span: Option<String>,
    #[serde(default = "default_stored_fact_confidence")]
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryUnit {
    pub id: Uuid,
    pub org_id: Option<String>,
    pub user_id: String,
    pub agent_id: Option<String>,
    pub stream_id: Uuid,
    pub memory_type: MemoryType,
    pub domain: MemoryDomain,
    pub namespace_key: String,
    #[serde(default)]
    pub share_policy: SharePolicy,

    /// Semantic content (compressed/summarized text)
    pub content: String,

    /// Vector embedding for retrieval
    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding: Option<Vec<f32>>,

    /// Keywords for text indexing
    pub keywords: Vec<String>,

    /// Importance score (0.0 - 1.0) for forgetting mechanism
    pub importance: f32,

    /// Memory level (1: L1 Consolidated, 2: L2 Insight, etc.)
    pub level: u8,

    pub transaction_time: DateTime<Utc>,
    pub valid_time: Option<DateTime<Utc>>,
    pub last_accessed_at: DateTime<Utc>,
    pub access_count: u64,

    /// Links to source Events or other MemoryUnits (Graph edges)
    pub references: Vec<Uuid>,

    /// Multimodal assets associated with this memory
    #[serde(default)]
    pub assets: Vec<Asset>,

    /// Cached structured facts extracted from this memory
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extracted_facts: Vec<StoredMemoryFact>,

    /// Task-specific metadata (status, progress)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_metadata: Option<TaskMetadata>,
}

impl MemoryUnit {
    pub fn new(
        org_id: Option<String>,
        user_id: String,
        agent_id: Option<String>,
        stream_id: Uuid,
        memory_type: MemoryType,
        content: String,
        embedding: Option<Vec<f32>>,
    ) -> Self {
        let domain = Self::infer_domain(agent_id.as_deref(), &memory_type);
        Self::new_with_domain(
            org_id,
            user_id,
            agent_id,
            stream_id,
            memory_type,
            domain,
            content,
            embedding,
        )
    }

    pub fn new_with_domain(
        org_id: Option<String>,
        user_id: String,
        agent_id: Option<String>,
        stream_id: Uuid,
        memory_type: MemoryType,
        domain: MemoryDomain,
        content: String,
        embedding: Option<Vec<f32>>,
    ) -> Self {
        let now = Utc::now();
        let namespace_key = Self::build_namespace_key(
            &domain,
            org_id.as_deref(),
            Some(&user_id),
            agent_id.as_deref(),
        );
        Self {
            id: Uuid::new_v4(),
            org_id,
            user_id,
            agent_id,
            stream_id,
            memory_type,
            domain,
            namespace_key,
            share_policy: SharePolicy::default(),
            content,
            embedding,
            keywords: Vec::new(),
            importance: 1.0, // Start with high importance
            level: 1,        // Default to L1
            transaction_time: now,
            valid_time: None,
            last_accessed_at: now,
            access_count: 0,
            references: Vec::new(),
            assets: Vec::new(),
            extracted_facts: Vec::new(),
            task_metadata: None,
        }
    }

    pub fn infer_domain(agent_id: Option<&str>, memory_type: &MemoryType) -> MemoryDomain {
        if matches!(memory_type, MemoryType::Procedural) && agent_id.is_some() {
            MemoryDomain::Agent
        } else {
            MemoryDomain::User
        }
    }

    pub fn build_namespace_key(
        domain: &MemoryDomain,
        org_id: Option<&str>,
        user_id: Option<&str>,
        agent_id: Option<&str>,
    ) -> String {
        match domain {
            // Three-domain direction:
            // - Agent memory is scoped to the agent itself (optionally under an org)
            // - User memory is scoped to the user itself (optionally under an org)
            MemoryDomain::Agent => format!(
                "agent:{}:{}",
                org_id.unwrap_or("_global"),
                agent_id.unwrap_or("_agent")
            ),
            MemoryDomain::User => format!(
                "user:{}:{}",
                org_id.unwrap_or("_global"),
                user_id.unwrap_or("_anonymous")
            ),
            MemoryDomain::Organization => {
                format!("org:{}", org_id.unwrap_or("_global"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_serialization() {
        let stream_id = Uuid::new_v4();
        let content = EventContent::Text("Hello World".to_string());
        let event = Event::new(None, "user1".into(), None, stream_id, content);

        let json = serde_json::to_string(&event).expect("Failed to serialize");
        let deserialized: Event = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(event.id, deserialized.id);
        assert_eq!(event.stream_id, deserialized.stream_id);
        assert_eq!(deserialized.user_id, "user1");
    }

    #[test]
    fn test_bitemporal_fields() {
        let now = Utc::now();
        let valid_time = now - chrono::Duration::days(7);

        let mut unit = MemoryUnit::new(
            None,
            "u1".into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            "text".into(),
            None,
        );
        unit.valid_time = Some(valid_time);

        assert_eq!(unit.valid_time, Some(valid_time));

        let mut event = Event::new(
            None,
            "u1".into(),
            None,
            Uuid::new_v4(),
            EventContent::Text("test".into()),
        );
        event.valid_time = Some(valid_time);
        assert_eq!(event.valid_time, Some(valid_time));
    }

    #[test]
    fn test_memory_unit_new_infers_agent_domain() {
        let unit = MemoryUnit::new(
            Some("org1".into()),
            "u1".into(),
            Some("agent1".into()),
            Uuid::new_v4(),
            MemoryType::Procedural,
            "tool trace".into(),
            None,
        );

        assert_eq!(unit.domain, MemoryDomain::Agent);
        assert_eq!(unit.namespace_key, "agent:org1:agent1");
    }

    #[test]
    fn test_relation_type_to_str() {
        assert_eq!(RelationType::Next.as_str(), "Next");
        assert_eq!(RelationType::IsSubTaskOf.as_str(), "IsSubTaskOf");
        assert_eq!(RelationType::Contradicts.as_str(), "Contradicts");
        assert_eq!(RelationType::DerivedFrom.as_str(), "DerivedFrom");
        assert_eq!(RelationType::EvolvedTo.as_str(), "EvolvedTo");
        assert_eq!(RelationType::Supports.as_str(), "Supports");
        assert_eq!(RelationType::Abstracts.as_str(), "Abstracts");
        assert_eq!(RelationType::CausedBy.as_str(), "CausedBy");
        assert_eq!(RelationType::Blocks.as_str(), "Blocks");
        assert_eq!(RelationType::Accomplishes.as_str(), "Accomplishes");
        assert_eq!(RelationType::RelatedTo.as_str(), "RelatedTo");
    }

    #[test]
    fn test_relation_type_from_str() {
        assert_eq!(RelationType::from_str("Next"), RelationType::Next);
        assert_eq!(RelationType::from_str("IsSubTaskOf"), RelationType::IsSubTaskOf);
        assert_eq!(RelationType::from_str("Contradicts"), RelationType::Contradicts);
        assert_eq!(RelationType::from_str("DerivedFrom"), RelationType::DerivedFrom);
        assert_eq!(RelationType::from_str("EvolvedTo"), RelationType::EvolvedTo);
        assert_eq!(RelationType::from_str("Supports"), RelationType::Supports);
        assert_eq!(RelationType::from_str("Abstracts"), RelationType::Abstracts);
        assert_eq!(RelationType::from_str("CausedBy"), RelationType::CausedBy);
        assert_eq!(RelationType::from_str("Blocks"), RelationType::Blocks);
        assert_eq!(RelationType::from_str("Accomplishes"), RelationType::Accomplishes);
        assert_eq!(RelationType::from_str("RelatedTo"), RelationType::RelatedTo);
        assert_eq!(RelationType::from_str("UnknownString"), RelationType::RelatedTo);
    }

    #[test]
    fn test_memory_type_default() {
        assert_eq!(MemoryType::default(), MemoryType::Factual);
    }
}
