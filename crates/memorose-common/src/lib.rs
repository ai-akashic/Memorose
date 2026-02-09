use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use std::collections::HashMap;

pub mod config;
pub mod sharding;
pub mod video;

fn default_legacy_id() -> String { "_legacy".to_string() }

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
    #[serde(default = "default_legacy_id")]
    pub user_id: String,
    #[serde(default = "default_legacy_id")]
    pub app_id: String,
    pub stream_id: Uuid,
    pub content: EventContent,
    pub transaction_time: DateTime<Utc>,
    pub valid_time: Option<DateTime<Utc>>,
    pub metadata: serde_json::Value,
}

impl Event {
    pub fn new(user_id: String, app_id: String, stream_id: Uuid, content: EventContent) -> Self {
        Self {
            id: Uuid::new_v4(),
            user_id,
            app_id,
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
    Next,         // Temporal sequence
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    Pending,
    Active,
    Completed,
    Failed,
    Blocked,
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
    #[serde(default = "default_legacy_id")]
    pub user_id: String,
    pub relation: RelationType,
    pub weight: f32,
    pub transaction_time: DateTime<Utc>,
}

impl GraphEdge {
    pub fn new(user_id: String, source: Uuid, target: Uuid, relation: RelationType, weight: f32) -> Self {
        Self {
            source_id: source,
            target_id: target,
            user_id,
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
    pub metadata: HashMap<String, String>,
}

/// Represents a consolidated memory unit (L1/L2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryUnit {
    pub id: Uuid,
    #[serde(default = "default_legacy_id")]
    pub user_id: String,
    #[serde(default = "default_legacy_id")]
    pub app_id: String,
    pub stream_id: Uuid,
    
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

    /// Task-specific metadata (status, progress)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_metadata: Option<TaskMetadata>,
}

impl MemoryUnit {
    pub fn new(user_id: String, app_id: String, stream_id: Uuid, content: String, embedding: Option<Vec<f32>>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            user_id,
            app_id,
            stream_id,
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
            task_metadata: None,
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
        let event = Event::new("user1".into(), "app1".into(), stream_id, content);

        let json = serde_json::to_string(&event).expect("Failed to serialize");
        let deserialized: Event = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(event.id, deserialized.id);
        assert_eq!(event.stream_id, deserialized.stream_id);
        assert_eq!(deserialized.user_id, "user1");
        assert_eq!(deserialized.app_id, "app1");
    }

    #[test]
    fn test_event_backward_compat() {
        // Old format without user_id/app_id should deserialize with defaults
        let json = r#"{"id":"00000000-0000-0000-0000-000000000001","stream_id":"00000000-0000-0000-0000-000000000002","content":{"type":"Text","data":"test"},"transaction_time":"2026-01-01T00:00:00Z","valid_time":null,"metadata":{}}"#;
        let event: Event = serde_json::from_str(json).expect("Failed to deserialize legacy event");
        assert_eq!(event.user_id, "_legacy");
        assert_eq!(event.app_id, "_legacy");
    }

    #[test]
    fn test_bitemporal_fields() {
        let now = Utc::now();
        let valid_time = now - chrono::Duration::days(7);
        
        let mut unit = MemoryUnit::new("u1".into(), "a1".into(), Uuid::new_v4(), "text".into(), None);
        unit.valid_time = Some(valid_time);
        
        assert_eq!(unit.valid_time, Some(valid_time));

        let mut event = Event::new("u1".into(), "a1".into(), Uuid::new_v4(), EventContent::Text("test".into()));
        event.valid_time = Some(valid_time);
        assert_eq!(event.valid_time, Some(valid_time));
    }
}
