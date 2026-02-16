use serde::{Deserialize, Serialize};
use std::env;
use config::{Config, ConfigError, File, Environment};

// --- Constants for Default Configuration ---
pub const DEFAULT_STORAGE_COMMIT_INTERVAL_MS: u64 = 5000;

pub const DEFAULT_RAFT_HEARTBEAT_INTERVAL_MS: u64 = 500;
pub const DEFAULT_RAFT_ELECTION_TIMEOUT_MIN_MS: u64 = 1500;
pub const DEFAULT_RAFT_ELECTION_TIMEOUT_MAX_MS: u64 = 3000;
pub const DEFAULT_RAFT_SNAPSHOT_LOGS: u64 = 1000000;

pub const DEFAULT_WORKER_LLM_CONCURRENCY: usize = 5;
pub const DEFAULT_WORKER_DECAY_INTERVAL_SECS: u64 = 60;
pub const DEFAULT_WORKER_DECAY_FACTOR: f32 = 0.9;
pub const DEFAULT_WORKER_PRUNE_THRESHOLD: f32 = 0.1;
pub const DEFAULT_WORKER_CONSOLIDATION_INTERVAL_MS: u64 = 1000;
pub const DEFAULT_WORKER_CONSOLIDATION_BATCH_SIZE: usize = 200;
pub const DEFAULT_WORKER_CONSOLIDATION_MAX_RETRIES: u32 = 3;
pub const DEFAULT_WORKER_COMPACTION_INTERVAL_SECS: u64 = 3600;
pub const DEFAULT_WORKER_COMMUNITY_INTERVAL_MS: u64 = 1000;
pub const DEFAULT_WORKER_COMMUNITY_MIN_MEMBERS: usize = 3;
pub const DEFAULT_WORKER_COMMUNITY_MAX_USERS_PER_CYCLE: usize = 100000;
pub const DEFAULT_WORKER_COMMUNITY_MAX_GROUPS_PER_USER: usize = 100000;
pub const DEFAULT_COMMUNITY_TRIGGER_L1_STEP: usize = 5;
pub const DEFAULT_WORKER_INSIGHT_INTERVAL_MS: u64 = 30000;
pub const DEFAULT_WORKER_INSIGHT_RECENT_L1_LIMIT: usize = 20;
pub const DEFAULT_AUTO_LINK_SIMILARITY_THRESHOLD: f32 = 0.6;
pub const DEFAULT_WORKER_TICK_INTERVAL_MS: u64 = 100;

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
    DEFAULT_STORAGE_COMMIT_INTERVAL_MS
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
    pub decay_interval_secs: u64,
    pub decay_factor: f32,
    pub prune_threshold: f32,
    pub consolidation_interval_ms: u64,
    pub consolidation_batch_size: usize,
    pub consolidation_max_retries: u32,
    pub compaction_interval_secs: u64,
    pub community_interval_ms: u64,
    pub community_min_members: usize,
    pub community_max_users_per_cycle: usize,
    pub community_max_groups_per_user: usize,
    pub community_trigger_l1_step: usize,
    pub insight_interval_ms: u64,
    pub insight_recent_l1_limit: usize,
    pub enable_auto_planner: bool,
    pub enable_task_reflection: bool,
    pub auto_link_similarity_threshold: f32,
    pub tick_interval_ms: u64,
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
            index_commit_interval_ms: DEFAULT_STORAGE_COMMIT_INTERVAL_MS,
        }
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            raft_addr: "127.0.0.1:5001".into(),
            heartbeat_interval_ms: DEFAULT_RAFT_HEARTBEAT_INTERVAL_MS,
            election_timeout_min_ms: DEFAULT_RAFT_ELECTION_TIMEOUT_MIN_MS,
            election_timeout_max_ms: DEFAULT_RAFT_ELECTION_TIMEOUT_MAX_MS,
            snapshot_logs: DEFAULT_RAFT_SNAPSHOT_LOGS,
        }
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            llm_concurrency: DEFAULT_WORKER_LLM_CONCURRENCY,
            decay_interval_secs: DEFAULT_WORKER_DECAY_INTERVAL_SECS,
            decay_factor: DEFAULT_WORKER_DECAY_FACTOR,
            prune_threshold: DEFAULT_WORKER_PRUNE_THRESHOLD,
            consolidation_interval_ms: DEFAULT_WORKER_CONSOLIDATION_INTERVAL_MS,
            consolidation_batch_size: DEFAULT_WORKER_CONSOLIDATION_BATCH_SIZE,
            consolidation_max_retries: DEFAULT_WORKER_CONSOLIDATION_MAX_RETRIES,
            compaction_interval_secs: DEFAULT_WORKER_COMPACTION_INTERVAL_SECS,
            community_interval_ms: DEFAULT_WORKER_COMMUNITY_INTERVAL_MS,
            community_min_members: DEFAULT_WORKER_COMMUNITY_MIN_MEMBERS,
            community_max_users_per_cycle: DEFAULT_WORKER_COMMUNITY_MAX_USERS_PER_CYCLE,
            community_max_groups_per_user: DEFAULT_WORKER_COMMUNITY_MAX_GROUPS_PER_USER,
            community_trigger_l1_step: DEFAULT_COMMUNITY_TRIGGER_L1_STEP,
            insight_interval_ms: DEFAULT_WORKER_INSIGHT_INTERVAL_MS,
            insight_recent_l1_limit: DEFAULT_WORKER_INSIGHT_RECENT_L1_LIMIT,
            enable_auto_planner: true,
            enable_task_reflection: true,
            auto_link_similarity_threshold: DEFAULT_AUTO_LINK_SIMILARITY_THRESHOLD,
            tick_interval_ms: DEFAULT_WORKER_TICK_INTERVAL_MS,
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
            .set_default("storage.index_commit_interval_ms", DEFAULT_STORAGE_COMMIT_INTERVAL_MS)?
            .set_default("llm.provider", "gemini")?
            .set_default("llm.model", "")?
            .set_default("llm.embedding_model", "")?
            .set_default("raft.node_id", node_id)?
            .set_default("raft.raft_addr", "127.0.0.1:5001")?
            .set_default("raft.heartbeat_interval_ms", DEFAULT_RAFT_HEARTBEAT_INTERVAL_MS)?
            .set_default("raft.election_timeout_min_ms", DEFAULT_RAFT_ELECTION_TIMEOUT_MIN_MS)?
            .set_default("raft.election_timeout_max_ms", DEFAULT_RAFT_ELECTION_TIMEOUT_MAX_MS)?
            .set_default("raft.snapshot_logs", DEFAULT_RAFT_SNAPSHOT_LOGS)?
            .set_default("worker.llm_concurrency", DEFAULT_WORKER_LLM_CONCURRENCY as i64)?
            .set_default("worker.decay_interval_secs", DEFAULT_WORKER_DECAY_INTERVAL_SECS)?
            .set_default("worker.decay_factor", DEFAULT_WORKER_DECAY_FACTOR as f64)?
            .set_default("worker.prune_threshold", DEFAULT_WORKER_PRUNE_THRESHOLD as f64)?
            .set_default("worker.consolidation_interval_ms", DEFAULT_WORKER_CONSOLIDATION_INTERVAL_MS)?
            .set_default("worker.consolidation_batch_size", DEFAULT_WORKER_CONSOLIDATION_BATCH_SIZE as i64)?
            .set_default("worker.consolidation_max_retries", DEFAULT_WORKER_CONSOLIDATION_MAX_RETRIES)?
            .set_default("worker.compaction_interval_secs", DEFAULT_WORKER_COMPACTION_INTERVAL_SECS)?
            .set_default("worker.community_interval_ms", DEFAULT_WORKER_COMMUNITY_INTERVAL_MS)?
            .set_default("worker.community_min_members", DEFAULT_WORKER_COMMUNITY_MIN_MEMBERS as i64)?
            .set_default("worker.community_max_users_per_cycle", DEFAULT_WORKER_COMMUNITY_MAX_USERS_PER_CYCLE as i64)?
            .set_default("worker.community_max_groups_per_user", DEFAULT_WORKER_COMMUNITY_MAX_GROUPS_PER_USER as i64)?
            .set_default("worker.community_trigger_l1_step", DEFAULT_COMMUNITY_TRIGGER_L1_STEP as i64)?
            .set_default("worker.insight_interval_ms", DEFAULT_WORKER_INSIGHT_INTERVAL_MS)?
            .set_default("worker.insight_recent_l1_limit", DEFAULT_WORKER_INSIGHT_RECENT_L1_LIMIT as i64)?
            .set_default("worker.enable_auto_planner", true)?
            .set_default("worker.enable_task_reflection", true)?
            .set_default("worker.auto_link_similarity_threshold", DEFAULT_AUTO_LINK_SIMILARITY_THRESHOLD as f64)?
            .set_default("worker.tick_interval_ms", DEFAULT_WORKER_TICK_INTERVAL_MS)?


            // File: config.toml
            .add_source(File::with_name("config").required(false))
            
            // Environment: MEMOROSE_LLM__PROVIDER=openai -> llm.provider=openai
            .add_source(Environment::with_prefix("MEMOROSE").separator("__"))
            
            // Legacy ENV overrides (for backward compatibility during migration)
            .set_override_option("llm.openai_api_key", env::var("OPENAI_API_KEY").ok())?
            .set_override_option("llm.google_api_key", env::var("GOOGLE_API_KEY").ok())?
            .set_override_option("llm.model", env::var("LLM_MODEL").ok())?
            .set_override_option("llm.embedding_model", env::var("EMBEDDING_MODEL").ok())?
            .set_override_option("raft.node_id", env::var("NODE_ID").ok().and_then(|v| v.parse::<u64>().ok()))?
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
