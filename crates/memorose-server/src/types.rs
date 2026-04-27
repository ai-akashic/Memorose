use chrono::{DateTime, Utc};
use memorose_common::{Asset, MemoryType, MemoryUnit, RelationType};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers (moved alongside the types that reference them)
// ---------------------------------------------------------------------------

pub fn default_content_type() -> String {
    "text".to_string()
}

pub fn default_graph_depth() -> usize {
    1
}

pub fn default_retrieve_limit() -> usize {
    10
}

pub fn default_context_limit() -> usize {
    12
}

pub fn default_context_token_budget() -> usize {
    800
}

// ---------------------------------------------------------------------------
// Asset / Unit views
// ---------------------------------------------------------------------------

pub fn public_asset_storage_key(asset: &Asset) -> String {
    use std::hash::{Hash, Hasher};

    let key = asset.storage_key.trim();
    if key.starts_with("http://")
        || key.starts_with("https://")
        || key.starts_with("s3://")
        || key.starts_with("local://")
        || key.starts_with("inline://")
    {
        return key.to_string();
    }

    if key.is_empty() {
        return "inline://asset".to_string();
    }

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    asset.asset_type.hash(&mut hasher);
    key.hash(&mut hasher);
    format!("inline://{}/{:016x}", asset.asset_type, hasher.finish())
}

#[derive(Clone, Serialize)]
pub struct RetrievalAssetView {
    pub storage_key: String,
    pub original_name: String,
    pub asset_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl From<&Asset> for RetrievalAssetView {
    fn from(asset: &Asset) -> Self {
        Self {
            storage_key: public_asset_storage_key(asset),
            original_name: asset.original_name.clone(),
            asset_type: asset.asset_type.clone(),
            description: asset.description.clone(),
        }
    }
}

#[derive(Clone, Serialize)]
pub struct RetrievalMemoryUnitView {
    pub id: Uuid,
    pub memory_type: MemoryType,
    pub content: String,
    pub keywords: Vec<String>,
    pub level: u8,
    pub assets: Vec<RetrievalAssetView>,
}

impl From<&MemoryUnit> for RetrievalMemoryUnitView {
    fn from(unit: &MemoryUnit) -> Self {
        Self {
            id: unit.id,
            memory_type: unit.memory_type.clone(),
            content: unit.content.clone(),
            keywords: unit.keywords.clone(),
            level: unit.level,
            assets: unit.assets.iter().map(RetrievalAssetView::from).collect(),
        }
    }
}
// PLACEHOLDER_CHUNK2

#[derive(Clone, Serialize)]
pub struct GoalMemoryUnitView {
    pub id: Uuid,
    pub content: String,
    pub transaction_time: DateTime<Utc>,
}

impl From<&MemoryUnit> for GoalMemoryUnitView {
    fn from(unit: &MemoryUnit) -> Self {
        Self {
            id: unit.id,
            content: unit.content.clone(),
            transaction_time: unit.transaction_time,
        }
    }
}

// ---------------------------------------------------------------------------
// Graph
// ---------------------------------------------------------------------------

#[derive(Deserialize, Serialize)]
pub struct AddEdgeRequest {
    pub source_id: Uuid,
    pub target_id: Uuid,
    pub relation: RelationType,
    pub weight: Option<f32>,
}

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------

#[derive(Deserialize, Serialize)]
pub struct IngestRequest {
    pub content: String,
    #[serde(default = "default_content_type")]
    pub content_type: String,
    #[serde(default)]
    pub org_id: Option<String>,
    #[serde(default)]
    pub level: Option<u8>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub task_status: Option<String>,
    #[serde(default)]
    pub task_progress: Option<f32>,
}
// PLACEHOLDER_CHUNK3

#[derive(Deserialize, Serialize)]
pub struct BatchIngestRequest {
    pub events: Vec<IngestRequest>,
}

// ---------------------------------------------------------------------------
// Retrieve
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct RetrieveRequest {
    pub query: String,
    #[serde(default = "default_retrieve_limit")]
    pub limit: usize,
    #[serde(default)]
    pub enable_arbitration: bool,
    #[serde(default)]
    pub min_score: Option<f32>,
    #[serde(default)]
    pub token_budget: Option<usize>,
    #[serde(default = "default_graph_depth")]
    pub graph_depth: usize,
    #[serde(default)]
    pub start_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub end_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub as_of: Option<DateTime<Utc>>,
    #[serde(default)]
    pub org_id: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    /// Base64-encoded image for cross-modal retrieval
    #[serde(default)]
    pub image: Option<String>,
    /// Base64-encoded audio for cross-modal retrieval
    #[serde(default)]
    pub audio: Option<String>,
    /// Base64-encoded video for cross-modal retrieval
    #[serde(default)]
    pub video: Option<String>,
}

#[derive(Serialize)]
pub struct RetrieveResultItem {
    pub unit: RetrievalMemoryUnitView,
    pub score: f32,
}

#[derive(Serialize)]
pub struct RetrieveResponse {
    pub stream_id: Uuid,
    pub query: String,
    pub results: Vec<RetrieveResultItem>,
    pub query_time_ms: u128,
}
// PLACEHOLDER_CHUNK4

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct MemoryContextRequest {
    pub user_id: String,
    pub query: String,
    #[serde(default = "default_context_limit")]
    pub limit: usize,
    #[serde(default)]
    pub enable_arbitration: bool,
    #[serde(default)]
    pub min_score: Option<f32>,
    #[serde(default)]
    pub token_budget: Option<usize>,
    #[serde(default = "default_graph_depth")]
    pub graph_depth: usize,
    #[serde(default)]
    pub start_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub end_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub as_of: Option<DateTime<Utc>>,
    #[serde(default)]
    pub org_id: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub audio: Option<String>,
    #[serde(default)]
    pub video: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ContextFormat {
    Text,
    Xml,
}

impl ContextFormat {
    pub fn from_raw(raw: Option<&str>) -> Self {
        match raw.map(str::trim) {
            Some(value) if value.eq_ignore_ascii_case("xml") => Self::Xml,
            _ => Self::Text,
        }
    }
    // PLACEHOLDER_CHUNK5

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Xml => "xml",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ContextCompressionTier {
    Tiny,
    Compact,
    Detailed,
}

impl ContextCompressionTier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tiny => "dense_l2_l3",
            Self::Compact => "adaptive_compact",
            Self::Detailed => "detailed_l1_first",
        }
    }
}

#[derive(Clone, Serialize)]
pub struct MemoryContextHitView {
    pub id: Uuid,
    pub level: u8,
    pub memory_type: MemoryType,
    pub domain: String,
    pub score: f32,
}

#[derive(Serialize)]
pub struct MemoryContextResponse {
    pub query: String,
    pub format: String,
    pub strategy: String,
    pub token_budget: usize,
    pub used_token_estimate: usize,
    pub matched_count: usize,
    pub included_count: usize,
    pub truncated: bool,
    pub context: String,
    pub hits: Vec<MemoryContextHitView>,
    pub query_time_ms: u128,
}

pub struct RenderedMemoryContext {
    pub context: String,
    pub used_token_estimate: usize,
    pub matched_count: usize,
    pub included_count: usize,
    pub truncated: bool,
    pub strategy: &'static str,
    pub hits: Vec<MemoryContextHitView>,
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct JoinRequest {
    pub node_id: u32,
    #[serde(default)]
    pub address: String,
}

// ---------------------------------------------------------------------------
// Goals / Tasks
// ---------------------------------------------------------------------------

#[derive(serde::Serialize)]
pub struct GoalTree {
    pub goal: GoalMemoryUnitView,
    pub tasks: Vec<L3TaskTree>,
}

#[derive(serde::Serialize)]
pub struct L3TaskTree {
    pub task: memorose_common::L3Task,
    pub children: Vec<L3TaskTree>,
}

#[derive(serde::Deserialize)]
pub struct UpdateTaskStatusRequest {
    pub status: memorose_common::TaskStatus,
    pub progress: Option<f32>,
    pub result_summary: Option<String>,
}
