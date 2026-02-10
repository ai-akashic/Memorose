use serde::{Deserialize, Serialize};
use std::env;
use config::{Config, ConfigError, File, Environment};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LLMProvider {
    OpenAI,
    Gemini,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LLMConfig {
    pub provider: LLMProvider,
    pub openai_api_key: Option<String>,
    pub google_api_key: Option<String>,
    pub model: String,
    pub embedding_model: String,
    pub stt_provider: Option<LLMProvider>,
    pub stt_model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub root_dir: String,
    #[serde(default = "default_commit_interval")]
    pub index_commit_interval_ms: u64,
}

fn default_commit_interval() -> u64 {
    5000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    pub node_id: u64,
    pub raft_addr: String,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub snapshot_logs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub llm_concurrency: usize,
    pub consolidation_interval_ms: u64,
    pub community_interval_ms: u64,
    pub insight_interval_ms: u64,
    pub enable_auto_planner: bool,
    pub enable_task_reflection: bool,
}

fn default_shard_count() -> u32 { 1 }
fn default_physical_node_id() -> u32 { 1 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardingConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_shard_count")]
    pub shard_count: u32,
    #[serde(default = "default_physical_node_id")]
    pub physical_node_id: u32,
    #[serde(default)]
    pub nodes: Vec<ShardNodeConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardNodeConfig {
    pub id: u32,
    pub http_addr: String,
    pub raft_base_port: u16,
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            shard_count: 1,
            physical_node_id: 1,
            nodes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub llm: LLMConfig,
    pub storage: StorageConfig,
    pub raft: RaftConfig,
    pub worker: WorkerConfig,
    #[serde(default)]
    pub sharding: Option<ShardingConfig>,
}

impl Default for LLMConfig {
    fn default() -> Self {
        Self {
            provider: LLMProvider::Gemini,
            openai_api_key: None,
            google_api_key: None,
            model: String::new(),
            embedding_model: String::new(),
            stt_provider: None,
            stt_model: None,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            root_dir: "./data".into(),
            index_commit_interval_ms: 5000,
        }
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            raft_addr: "127.0.0.1:5001".into(),
            heartbeat_interval_ms: 500,
            election_timeout_min_ms: 1500,
            election_timeout_max_ms: 3000,
            snapshot_logs: 100,
        }
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            llm_concurrency: 5,
            consolidation_interval_ms: 1000,
            community_interval_ms: 60000,
            insight_interval_ms: 30000,
            enable_auto_planner: true,
            enable_task_reflection: true,
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            llm: LLMConfig::default(),
            storage: StorageConfig::default(),
            raft: RaftConfig::default(),
            worker: WorkerConfig::default(),
            sharding: None,
        }
    }
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let node_id = env::var("NODE_ID").unwrap_or_else(|_| "1".to_string());
        
        let s = Config::builder()
            // Default settings
            .set_default("storage.root_dir", format!("./data/node-{}", node_id))?
            .set_default("storage.index_commit_interval_ms", 5000)?
            .set_default("llm.provider", "gemini")?
            .set_default("llm.model", "")?
            .set_default("llm.embedding_model", "")?
            .set_default("raft.node_id", node_id)?
            .set_default("raft.raft_addr", "127.0.0.1:5001")?
            .set_default("raft.heartbeat_interval_ms", 500)?
            .set_default("raft.election_timeout_min_ms", 1500)?
            .set_default("raft.election_timeout_max_ms", 3000)?
            .set_default("raft.snapshot_logs", 1000000)?
            .set_default("worker.llm_concurrency", 5)?
            .set_default("worker.consolidation_interval_ms", 1000)?
            .set_default("worker.community_interval_ms", 60000)?
            .set_default("worker.insight_interval_ms", 30000)?
            .set_default("worker.enable_auto_planner", true)?
            .set_default("worker.enable_task_reflection", true)?
            
            // File: config.toml
            .add_source(File::with_name("config").required(false))
            
            // Environment: MEMOROSE_LLM__PROVIDER=openai -> llm.provider=openai
            .add_source(Environment::with_prefix("MEMOROSE").separator("__"))
            
            // Legacy ENV overrides (for backward compatibility during migration)
            .set_override_option("llm.openai_api_key", env::var("OPENAI_API_KEY").ok())?
            .set_override_option("llm.google_api_key", env::var("GOOGLE_API_KEY").ok())?
            .set_override_option("llm.model", env::var("LLM_MODEL").ok())?
            .set_override_option("llm.embedding_model", env::var("EMBEDDING_MODEL").ok())?
            .set_override_option("raft.raft_addr", env::var("RAFT_ADDR").ok())?
            
            .build()?;

        s.try_deserialize()
    }

    pub fn get_active_key(&self) -> Option<String> {
        match self.llm.provider {
            LLMProvider::OpenAI => self.llm.openai_api_key.clone(),
            LLMProvider::Gemini => self.llm.google_api_key.clone(),
        }
    }

    pub fn get_model_name(&self) -> String {
        self.llm.model.clone()
    }

    pub fn get_embedding_model_name(&self) -> String {
        self.llm.embedding_model.clone()
    }

    pub fn get_base_url(&self) -> Option<String> {
        match self.llm.provider {
            LLMProvider::OpenAI => None,
            LLMProvider::Gemini => Some("https://generativelanguage.googleapis.com/v1beta/openai/".to_string()),
        }
    }

    /// Returns true if sharding is enabled with more than 1 shard.
    pub fn is_sharded(&self) -> bool {
        self.sharding.as_ref().map_or(false, |s| s.enabled && s.shard_count > 1)
    }

    /// Returns the number of shards (1 if not sharded).
    pub fn shard_count(&self) -> u32 {
        self.sharding.as_ref()
            .filter(|s| s.enabled)
            .map_or(1, |s| s.shard_count.max(1))
    }

    /// Returns the physical node ID, falling back to raft.node_id.
    pub fn physical_node_id(&self) -> u32 {
        self.sharding.as_ref()
            .filter(|s| s.enabled)
            .map_or(self.raft.node_id as u32, |s| s.physical_node_id)
    }
}