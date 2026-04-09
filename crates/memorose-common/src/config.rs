use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};
use std::env;

// --- Constants for Default Configuration ---
pub const DEFAULT_STORAGE_COMMIT_INTERVAL_MS: u64 = 5000;
pub const DEFAULT_STORAGE_COMMIT_MIN_INTERVAL_MS: u64 = 5000;
pub const DEFAULT_STORAGE_COMMIT_MAX_INTERVAL_MS: u64 = 30000;
pub const DEFAULT_STORAGE_COMMIT_DOCS_THRESHOLD: usize = 2000;
pub const DEFAULT_STORAGE_COMMIT_BYTES_THRESHOLD: u64 = 32_000_000;
pub const DEFAULT_STORAGE_RECENT_OVERLAY_ENABLED: bool = true;
pub const DEFAULT_STORAGE_RECENT_OVERLAY_TTL_SECS: u64 = 120;
pub const DEFAULT_STORAGE_RECENT_OVERLAY_PER_USER_MAX_DOCS: usize = 1000;
pub const DEFAULT_STORAGE_RECENT_OVERLAY_PER_USER_MAX_BYTES: usize = 8_388_608;
pub const DEFAULT_STORAGE_RECENT_OVERLAY_GLOBAL_MAX_BYTES: usize = 134_217_728;
pub const DEFAULT_STORAGE_RECENT_OVERLAY_QUERY_LIMIT: usize = 200;

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
pub const DEFAULT_WORKER_CONSOLIDATION_FETCH_MULTIPLIER: usize = 20;
pub const DEFAULT_WORKER_CONSOLIDATION_TARGET_TOKENS: usize = 4096;
pub const DEFAULT_WORKER_CONSOLIDATION_MAX_EVENTS_PER_PACK: usize = 128;
pub const DEFAULT_WORKER_CONSOLIDATION_STORE_BATCH_SIZE: usize = 32;
pub const DEFAULT_WORKER_CONSOLIDATION_MAX_RETRIES: u32 = 3;
pub const DEFAULT_WORKER_COMPACTION_INTERVAL_SECS: u64 = 3600;
pub const DEFAULT_WORKER_COMMUNITY_INTERVAL_MS: u64 = 1000;
pub const DEFAULT_WORKER_COMMUNITY_MIN_MEMBERS: usize = 3;
pub const DEFAULT_WORKER_COMMUNITY_MAX_USERS_PER_CYCLE: usize = 100000;
pub const DEFAULT_WORKER_COMMUNITY_MAX_GROUPS_PER_USER: usize = 100000;
pub const DEFAULT_COMMUNITY_TRIGGER_L1_STEP: usize = 5;
pub const DEFAULT_WORKER_INSIGHT_INTERVAL_MS: u64 = 30000;
pub const DEFAULT_WORKER_INSIGHT_RECENT_L1_LIMIT: usize = 20;
pub const DEFAULT_WORKER_INSIGHT_MIN_PENDING_TOKENS: usize = 2000;
pub const DEFAULT_WORKER_INSIGHT_MIN_PENDING_L1: usize = 8;
pub const DEFAULT_WORKER_INSIGHT_MAX_DELAY_MS: u64 = 21_600_000;
pub const DEFAULT_WORKER_INSIGHT_BATCH_TARGET_TOKENS: usize = 4096;
pub const DEFAULT_WORKER_INSIGHT_MAX_L1_PER_BATCH: usize = 32;
pub const DEFAULT_WORKER_INSIGHT_MAX_BATCHES_PER_CYCLE: usize = 4;
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
    pub base_url: Option<String>,
    pub model: String,
    pub embedding_model: String,
    #[serde(default = "default_embedding_dim")]
    pub embedding_dim: i32,
    #[serde(default)]
    pub embedding_output_dim: Option<i32>,
    #[serde(default)]
    pub embedding_task_type: Option<String>,
    pub stt_provider: Option<LLMProvider>,
    pub stt_model: Option<String>,
}

fn default_embedding_dim() -> i32 {
    3072 // default for gemini-embedding-2
}

impl LLMConfig {
    pub fn get_base_url(&self) -> Option<String> {
        if self.base_url.is_some() {
            return self.base_url.clone();
        }
        match self.provider {
            LLMProvider::OpenAI => Some("https://api.openai.com/v1".to_string()),
            LLMProvider::Gemini => Some("https://generativelanguage.googleapis.com".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub root_dir: String,
    #[serde(default = "default_commit_interval")]
    pub index_commit_interval_ms: u64,
    #[serde(default = "default_commit_min_interval")]
    pub index_commit_min_interval_ms: u64,
    #[serde(default = "default_commit_max_interval")]
    pub index_commit_max_interval_ms: u64,
    #[serde(default = "default_commit_docs_threshold")]
    pub index_commit_docs_threshold: usize,
    #[serde(default = "default_commit_bytes_threshold")]
    pub index_commit_bytes_threshold: u64,
    #[serde(default = "default_recent_overlay_enabled")]
    pub recent_overlay_enabled: bool,
    #[serde(default = "default_recent_overlay_ttl_secs")]
    pub recent_overlay_ttl_secs: u64,
    #[serde(default = "default_recent_overlay_per_user_max_docs")]
    pub recent_overlay_per_user_max_docs: usize,
    #[serde(default = "default_recent_overlay_per_user_max_bytes")]
    pub recent_overlay_per_user_max_bytes: usize,
    #[serde(default = "default_recent_overlay_global_max_bytes")]
    pub recent_overlay_global_max_bytes: usize,
    #[serde(default = "default_recent_overlay_query_limit")]
    pub recent_overlay_query_limit: usize,
}

fn default_commit_interval() -> u64 {
    DEFAULT_STORAGE_COMMIT_INTERVAL_MS
}

fn default_commit_min_interval() -> u64 {
    DEFAULT_STORAGE_COMMIT_MIN_INTERVAL_MS
}

fn default_commit_max_interval() -> u64 {
    DEFAULT_STORAGE_COMMIT_MAX_INTERVAL_MS
}

fn default_commit_docs_threshold() -> usize {
    DEFAULT_STORAGE_COMMIT_DOCS_THRESHOLD
}

fn default_commit_bytes_threshold() -> u64 {
    DEFAULT_STORAGE_COMMIT_BYTES_THRESHOLD
}

fn default_recent_overlay_enabled() -> bool {
    DEFAULT_STORAGE_RECENT_OVERLAY_ENABLED
}

fn default_recent_overlay_ttl_secs() -> u64 {
    DEFAULT_STORAGE_RECENT_OVERLAY_TTL_SECS
}

fn default_recent_overlay_per_user_max_docs() -> usize {
    DEFAULT_STORAGE_RECENT_OVERLAY_PER_USER_MAX_DOCS
}

fn default_recent_overlay_per_user_max_bytes() -> usize {
    DEFAULT_STORAGE_RECENT_OVERLAY_PER_USER_MAX_BYTES
}

fn default_recent_overlay_global_max_bytes() -> usize {
    DEFAULT_STORAGE_RECENT_OVERLAY_GLOBAL_MAX_BYTES
}

fn default_recent_overlay_query_limit() -> usize {
    DEFAULT_STORAGE_RECENT_OVERLAY_QUERY_LIMIT
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    pub node_id: u64,
    pub raft_addr: String,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub snapshot_logs: u64,
    #[serde(default = "default_auto_initialize")]
    pub auto_initialize: bool,
    #[serde(default)]
    pub bootstrap_seed_node_id: Option<u32>,
}

fn default_auto_initialize() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub llm_concurrency: usize,
    pub decay_interval_secs: u64,
    pub decay_factor: f32,
    pub prune_threshold: f32,
    pub consolidation_interval_ms: u64,
    pub consolidation_batch_size: usize,
    pub consolidation_fetch_multiplier: usize,
    pub consolidation_target_tokens: usize,
    pub consolidation_max_events_per_pack: usize,
    pub consolidation_store_batch_size: usize,
    pub consolidation_max_retries: u32,
    pub compaction_interval_secs: u64,
    pub community_interval_ms: u64,
    pub community_min_members: usize,
    pub community_max_users_per_cycle: usize,
    pub community_max_groups_per_user: usize,
    pub community_trigger_l1_step: usize,
    pub insight_interval_ms: u64,
    pub insight_recent_l1_limit: usize,
    pub insight_min_pending_tokens: usize,
    pub insight_min_pending_l1: usize,
    pub insight_max_delay_ms: u64,
    pub insight_batch_target_tokens: usize,
    pub insight_max_l1_per_batch: usize,
    pub insight_max_batches_per_cycle: usize,
    pub enable_auto_planner: bool,
    pub enable_task_reflection: bool,
    pub auto_link_similarity_threshold: f32,
    pub tick_interval_ms: u64,
}

fn default_shard_count() -> u32 {
    1
}
fn default_physical_node_id() -> u32 {
    1
}

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
    #[serde(default)]
    pub reranker: RerankerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RerankerType {
    Weighted,
    Http,
}

impl Default for RerankerType {
    fn default() -> Self {
        RerankerType::Weighted
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RerankerConfig {
    #[serde(default)]
    pub r#type: RerankerType,
    #[serde(default)]
    pub endpoint: Option<String>,
}

impl Default for LLMConfig {
    fn default() -> Self {
        Self {
            provider: LLMProvider::Gemini,
            openai_api_key: None,
            google_api_key: None,
            base_url: None,
            model: String::new(),
            embedding_model: String::new(),
            embedding_dim: 3072,
            embedding_output_dim: None,
            embedding_task_type: None,
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
            index_commit_min_interval_ms: DEFAULT_STORAGE_COMMIT_MIN_INTERVAL_MS,
            index_commit_max_interval_ms: DEFAULT_STORAGE_COMMIT_MAX_INTERVAL_MS,
            index_commit_docs_threshold: DEFAULT_STORAGE_COMMIT_DOCS_THRESHOLD,
            index_commit_bytes_threshold: DEFAULT_STORAGE_COMMIT_BYTES_THRESHOLD,
            recent_overlay_enabled: DEFAULT_STORAGE_RECENT_OVERLAY_ENABLED,
            recent_overlay_ttl_secs: DEFAULT_STORAGE_RECENT_OVERLAY_TTL_SECS,
            recent_overlay_per_user_max_docs: DEFAULT_STORAGE_RECENT_OVERLAY_PER_USER_MAX_DOCS,
            recent_overlay_per_user_max_bytes: DEFAULT_STORAGE_RECENT_OVERLAY_PER_USER_MAX_BYTES,
            recent_overlay_global_max_bytes: DEFAULT_STORAGE_RECENT_OVERLAY_GLOBAL_MAX_BYTES,
            recent_overlay_query_limit: DEFAULT_STORAGE_RECENT_OVERLAY_QUERY_LIMIT,
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
            auto_initialize: true,
            bootstrap_seed_node_id: None,
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
            consolidation_fetch_multiplier: DEFAULT_WORKER_CONSOLIDATION_FETCH_MULTIPLIER,
            consolidation_target_tokens: DEFAULT_WORKER_CONSOLIDATION_TARGET_TOKENS,
            consolidation_max_events_per_pack: DEFAULT_WORKER_CONSOLIDATION_MAX_EVENTS_PER_PACK,
            consolidation_store_batch_size: DEFAULT_WORKER_CONSOLIDATION_STORE_BATCH_SIZE,
            consolidation_max_retries: DEFAULT_WORKER_CONSOLIDATION_MAX_RETRIES,
            compaction_interval_secs: DEFAULT_WORKER_COMPACTION_INTERVAL_SECS,
            community_interval_ms: DEFAULT_WORKER_COMMUNITY_INTERVAL_MS,
            community_min_members: DEFAULT_WORKER_COMMUNITY_MIN_MEMBERS,
            community_max_users_per_cycle: DEFAULT_WORKER_COMMUNITY_MAX_USERS_PER_CYCLE,
            community_max_groups_per_user: DEFAULT_WORKER_COMMUNITY_MAX_GROUPS_PER_USER,
            community_trigger_l1_step: DEFAULT_COMMUNITY_TRIGGER_L1_STEP,
            insight_interval_ms: DEFAULT_WORKER_INSIGHT_INTERVAL_MS,
            insight_recent_l1_limit: DEFAULT_WORKER_INSIGHT_RECENT_L1_LIMIT,
            insight_min_pending_tokens: DEFAULT_WORKER_INSIGHT_MIN_PENDING_TOKENS,
            insight_min_pending_l1: DEFAULT_WORKER_INSIGHT_MIN_PENDING_L1,
            insight_max_delay_ms: DEFAULT_WORKER_INSIGHT_MAX_DELAY_MS,
            insight_batch_target_tokens: DEFAULT_WORKER_INSIGHT_BATCH_TARGET_TOKENS,
            insight_max_l1_per_batch: DEFAULT_WORKER_INSIGHT_MAX_L1_PER_BATCH,
            insight_max_batches_per_cycle: DEFAULT_WORKER_INSIGHT_MAX_BATCHES_PER_CYCLE,
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
            reranker: RerankerConfig::default(),
        }
    }
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let node_id = env::var("NODE_ID").unwrap_or_else(|_| "1".to_string());

        let s = Config::builder()
            // Default settings
            .set_default("storage.root_dir", format!("./data/node-{}", node_id))?
            .set_default(
                "storage.index_commit_interval_ms",
                DEFAULT_STORAGE_COMMIT_INTERVAL_MS,
            )?
            .set_default(
                "storage.index_commit_min_interval_ms",
                DEFAULT_STORAGE_COMMIT_MIN_INTERVAL_MS,
            )?
            .set_default(
                "storage.index_commit_max_interval_ms",
                DEFAULT_STORAGE_COMMIT_MAX_INTERVAL_MS,
            )?
            .set_default(
                "storage.index_commit_docs_threshold",
                DEFAULT_STORAGE_COMMIT_DOCS_THRESHOLD as i64,
            )?
            .set_default(
                "storage.index_commit_bytes_threshold",
                DEFAULT_STORAGE_COMMIT_BYTES_THRESHOLD as i64,
            )?
            .set_default(
                "storage.recent_overlay_enabled",
                DEFAULT_STORAGE_RECENT_OVERLAY_ENABLED,
            )?
            .set_default(
                "storage.recent_overlay_ttl_secs",
                DEFAULT_STORAGE_RECENT_OVERLAY_TTL_SECS,
            )?
            .set_default(
                "storage.recent_overlay_per_user_max_docs",
                DEFAULT_STORAGE_RECENT_OVERLAY_PER_USER_MAX_DOCS as i64,
            )?
            .set_default(
                "storage.recent_overlay_per_user_max_bytes",
                DEFAULT_STORAGE_RECENT_OVERLAY_PER_USER_MAX_BYTES as i64,
            )?
            .set_default(
                "storage.recent_overlay_global_max_bytes",
                DEFAULT_STORAGE_RECENT_OVERLAY_GLOBAL_MAX_BYTES as i64,
            )?
            .set_default(
                "storage.recent_overlay_query_limit",
                DEFAULT_STORAGE_RECENT_OVERLAY_QUERY_LIMIT as i64,
            )?
            .set_default("llm.provider", "gemini")?
            .set_default("llm.model", "")?
            .set_default("llm.embedding_model", "")?
            .set_default("raft.node_id", node_id)?
            .set_default("raft.raft_addr", "127.0.0.1:5001")?
            .set_default(
                "raft.heartbeat_interval_ms",
                DEFAULT_RAFT_HEARTBEAT_INTERVAL_MS,
            )?
            .set_default(
                "raft.election_timeout_min_ms",
                DEFAULT_RAFT_ELECTION_TIMEOUT_MIN_MS,
            )?
            .set_default(
                "raft.election_timeout_max_ms",
                DEFAULT_RAFT_ELECTION_TIMEOUT_MAX_MS,
            )?
            .set_default("raft.snapshot_logs", DEFAULT_RAFT_SNAPSHOT_LOGS)?
            .set_default("raft.auto_initialize", true)?
            .set_default(
                "worker.llm_concurrency",
                DEFAULT_WORKER_LLM_CONCURRENCY as i64,
            )?
            .set_default(
                "worker.decay_interval_secs",
                DEFAULT_WORKER_DECAY_INTERVAL_SECS,
            )?
            .set_default("worker.decay_factor", DEFAULT_WORKER_DECAY_FACTOR as f64)?
            .set_default(
                "worker.prune_threshold",
                DEFAULT_WORKER_PRUNE_THRESHOLD as f64,
            )?
            .set_default(
                "worker.consolidation_interval_ms",
                DEFAULT_WORKER_CONSOLIDATION_INTERVAL_MS,
            )?
            .set_default(
                "worker.consolidation_batch_size",
                DEFAULT_WORKER_CONSOLIDATION_BATCH_SIZE as i64,
            )?
            .set_default(
                "worker.consolidation_fetch_multiplier",
                DEFAULT_WORKER_CONSOLIDATION_FETCH_MULTIPLIER as i64,
            )?
            .set_default(
                "worker.consolidation_target_tokens",
                DEFAULT_WORKER_CONSOLIDATION_TARGET_TOKENS as i64,
            )?
            .set_default(
                "worker.consolidation_max_events_per_pack",
                DEFAULT_WORKER_CONSOLIDATION_MAX_EVENTS_PER_PACK as i64,
            )?
            .set_default(
                "worker.consolidation_store_batch_size",
                DEFAULT_WORKER_CONSOLIDATION_STORE_BATCH_SIZE as i64,
            )?
            .set_default(
                "worker.consolidation_max_retries",
                DEFAULT_WORKER_CONSOLIDATION_MAX_RETRIES,
            )?
            .set_default(
                "worker.compaction_interval_secs",
                DEFAULT_WORKER_COMPACTION_INTERVAL_SECS,
            )?
            .set_default(
                "worker.community_interval_ms",
                DEFAULT_WORKER_COMMUNITY_INTERVAL_MS,
            )?
            .set_default(
                "worker.community_min_members",
                DEFAULT_WORKER_COMMUNITY_MIN_MEMBERS as i64,
            )?
            .set_default(
                "worker.community_max_users_per_cycle",
                DEFAULT_WORKER_COMMUNITY_MAX_USERS_PER_CYCLE as i64,
            )?
            .set_default(
                "worker.community_max_groups_per_user",
                DEFAULT_WORKER_COMMUNITY_MAX_GROUPS_PER_USER as i64,
            )?
            .set_default(
                "worker.community_trigger_l1_step",
                DEFAULT_COMMUNITY_TRIGGER_L1_STEP as i64,
            )?
            .set_default(
                "worker.insight_interval_ms",
                DEFAULT_WORKER_INSIGHT_INTERVAL_MS,
            )?
            .set_default(
                "worker.insight_recent_l1_limit",
                DEFAULT_WORKER_INSIGHT_RECENT_L1_LIMIT as i64,
            )?
            .set_default(
                "worker.insight_min_pending_tokens",
                DEFAULT_WORKER_INSIGHT_MIN_PENDING_TOKENS as i64,
            )?
            .set_default(
                "worker.insight_min_pending_l1",
                DEFAULT_WORKER_INSIGHT_MIN_PENDING_L1 as i64,
            )?
            .set_default(
                "worker.insight_max_delay_ms",
                DEFAULT_WORKER_INSIGHT_MAX_DELAY_MS,
            )?
            .set_default(
                "worker.insight_batch_target_tokens",
                DEFAULT_WORKER_INSIGHT_BATCH_TARGET_TOKENS as i64,
            )?
            .set_default(
                "worker.insight_max_l1_per_batch",
                DEFAULT_WORKER_INSIGHT_MAX_L1_PER_BATCH as i64,
            )?
            .set_default(
                "worker.insight_max_batches_per_cycle",
                DEFAULT_WORKER_INSIGHT_MAX_BATCHES_PER_CYCLE as i64,
            )?
            .set_default("worker.enable_auto_planner", true)?
            .set_default("worker.enable_task_reflection", true)?
            .set_default(
                "worker.auto_link_similarity_threshold",
                DEFAULT_AUTO_LINK_SIMILARITY_THRESHOLD as f64,
            )?
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
            .set_override_option(
                "raft.node_id",
                env::var("NODE_ID").ok().and_then(|v| v.parse::<u64>().ok()),
            )?
            .set_override_option("raft.raft_addr", env::var("RAFT_ADDR").ok())?
            .build()?;

        s.try_deserialize().and_then(|config: AppConfig| {
            // Validate Raft timing invariants to prevent permanent leader-election loops.
            if config.raft.heartbeat_interval_ms >= config.raft.election_timeout_min_ms {
                return Err(ConfigError::Message(format!(
                    "raft.heartbeat_interval_ms ({}) must be strictly less than \
                     raft.election_timeout_min_ms ({})",
                    config.raft.heartbeat_interval_ms, config.raft.election_timeout_min_ms
                )));
            }
            if config.raft.election_timeout_min_ms >= config.raft.election_timeout_max_ms {
                return Err(ConfigError::Message(format!(
                    "raft.election_timeout_min_ms ({}) must be strictly less than \
                     raft.election_timeout_max_ms ({})",
                    config.raft.election_timeout_min_ms, config.raft.election_timeout_max_ms
                )));
            }
            Ok(config)
        })
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
            LLMProvider::Gemini => {
                Some("https://generativelanguage.googleapis.com/v1beta/openai/".to_string())
            }
        }
    }

    /// Returns true if sharding is enabled with more than 1 shard.
    pub fn is_sharded(&self) -> bool {
        self.sharding
            .as_ref()
            .map_or(false, |s| s.enabled && s.shard_count > 1)
    }

    /// Returns the number of shards (1 if not sharded).
    pub fn shard_count(&self) -> u32 {
        self.sharding
            .as_ref()
            .filter(|s| s.enabled)
            .map_or(1, |s| s.shard_count.max(1))
    }

    /// Returns the physical node ID, falling back to raft.node_id.
    pub fn physical_node_id(&self) -> u32 {
        self.sharding
            .as_ref()
            .filter(|s| s.enabled)
            .map_or(self.raft.node_id as u32, |s| s.physical_node_id)
    }

    /// Returns the number of physical nodes described by topology config.
    pub fn cluster_node_count(&self) -> u32 {
        self.sharding
            .as_ref()
            .filter(|s| s.enabled)
            .map_or(1, |s| s.nodes.len().max(1) as u32)
    }

    /// Returns true when startup topology implies distributed/cluster operation.
    pub fn is_cluster_mode(&self) -> bool {
        self.is_sharded() || self.cluster_node_count() > 1
    }

    /// Returns true when startup topology implies single-node standalone operation.
    pub fn is_standalone_mode(&self) -> bool {
        !self.is_cluster_mode()
    }

    /// Returns true when this node is the configured bootstrap seed node.
    ///
    /// When no explicit seed is configured, only a single-node topology is
    /// allowed to self-bootstrap automatically.
    pub fn is_bootstrap_seed_node(&self) -> bool {
        match self.raft.bootstrap_seed_node_id {
            Some(seed_node_id) => self.physical_node_id() == seed_node_id,
            None => self.cluster_node_count() <= 1,
        }
    }

    /// Returns true when startup should auto-bootstrap local raft groups.
    pub fn should_auto_initialize_raft(&self) -> bool {
        self.raft.auto_initialize && self.is_bootstrap_seed_node()
    }

    /// Returns true when auto-bootstrap is enabled but multi-node topology
    /// lacks an explicit seed selection.
    pub fn needs_explicit_bootstrap_seed(&self) -> bool {
        self.raft.auto_initialize
            && self.cluster_node_count() > 1
            && self.raft.bootstrap_seed_node_id.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_node_topology_auto_bootstraps_by_default() {
        let config = AppConfig::default();
        assert_eq!(config.cluster_node_count(), 1);
        assert!(config.is_standalone_mode());
        assert!(!config.is_cluster_mode());
        assert!(config.is_bootstrap_seed_node());
        assert!(config.should_auto_initialize_raft());
        assert!(!config.needs_explicit_bootstrap_seed());
    }

    #[test]
    fn test_multi_node_topology_requires_explicit_seed() {
        let mut config = AppConfig::default();
        config.sharding = Some(ShardingConfig {
            enabled: true,
            shard_count: 4,
            physical_node_id: 1,
            nodes: vec![
                ShardNodeConfig {
                    id: 1,
                    http_addr: "10.0.0.1:3000".into(),
                    raft_base_port: 5001,
                },
                ShardNodeConfig {
                    id: 2,
                    http_addr: "10.0.0.2:3000".into(),
                    raft_base_port: 5001,
                },
            ],
        });

        assert_eq!(config.cluster_node_count(), 2);
        assert!(config.is_cluster_mode());
        assert!(!config.is_standalone_mode());
        assert!(!config.is_bootstrap_seed_node());
        assert!(!config.should_auto_initialize_raft());
        assert!(config.needs_explicit_bootstrap_seed());
    }

    #[test]
    fn test_explicit_seed_enables_only_matching_node() {
        let mut config = AppConfig::default();
        config.sharding = Some(ShardingConfig {
            enabled: true,
            shard_count: 4,
            physical_node_id: 2,
            nodes: vec![
                ShardNodeConfig {
                    id: 1,
                    http_addr: "10.0.0.1:3000".into(),
                    raft_base_port: 5001,
                },
                ShardNodeConfig {
                    id: 2,
                    http_addr: "10.0.0.2:3000".into(),
                    raft_base_port: 5001,
                },
            ],
        });
        config.raft.bootstrap_seed_node_id = Some(1);

        assert!(!config.is_bootstrap_seed_node());
        assert!(!config.should_auto_initialize_raft());

        config.sharding.as_mut().unwrap().physical_node_id = 1;
        assert!(config.is_bootstrap_seed_node());
        assert!(config.should_auto_initialize_raft());
        assert!(!config.needs_explicit_bootstrap_seed());
    }

    #[test]
    fn test_explicit_disable_overrides_single_node_default() {
        let mut config = AppConfig::default();
        config.raft.auto_initialize = false;
        assert!(!config.should_auto_initialize_raft());
    }

    #[test]
    fn test_config_accessors() {
        let mut config = AppConfig::default();
        config.llm.provider = LLMProvider::Gemini;
        config.llm.google_api_key = Some("gemini_key".to_string());
        config.llm.model = "gemini-model".to_string();
        config.llm.embedding_model = "gemini-embed".to_string();

        assert_eq!(config.get_active_key(), Some("gemini_key".to_string()));
        assert_eq!(config.get_model_name(), "gemini-model");
        assert_eq!(config.get_embedding_model_name(), "gemini-embed");
        assert_eq!(
            config.get_base_url(),
            Some("https://generativelanguage.googleapis.com/v1beta/openai/".to_string())
        );

        config.llm.provider = LLMProvider::OpenAI;
        config.llm.openai_api_key = Some("openai_key".to_string());
        assert_eq!(config.get_active_key(), Some("openai_key".to_string()));
        assert_eq!(config.get_base_url(), None);
    }

    #[test]
    fn test_is_sharded() {
        let mut config = AppConfig::default();
        assert!(!config.is_sharded());

        config.sharding = Some(ShardingConfig {
            enabled: true,
            shard_count: 1,
            physical_node_id: 1,
            nodes: vec![],
        });
        assert!(!config.is_sharded());

        config.sharding.as_mut().unwrap().shard_count = 2;
        assert!(config.is_sharded());
        assert_eq!(config.shard_count(), 2);
    }

    #[test]
    fn test_llm_config_get_base_url() {
        let mut config = LLMConfig {
            provider: LLMProvider::OpenAI,
            openai_api_key: None,
            google_api_key: None,
            model: "".into(),
            base_url: None,
            embedding_model: "".into(),
            embedding_dim: 128,
            embedding_output_dim: None,
            embedding_task_type: None,
            stt_provider: None,
            stt_model: None,
        };
        assert_eq!(
            config.get_base_url(),
            Some("https://api.openai.com/v1".to_string())
        );

        config.provider = LLMProvider::Gemini;
        assert_eq!(
            config.get_base_url(),
            Some("https://generativelanguage.googleapis.com".to_string())
        );

        config.base_url = Some("http://localhost:8080".into());
        assert_eq!(
            config.get_base_url(),
            Some("http://localhost:8080".to_string())
        );
    }

    #[test]
    fn test_storage_config_defaults() {
        assert_eq!(
            default_commit_interval(),
            DEFAULT_STORAGE_COMMIT_INTERVAL_MS
        );
        assert_eq!(
            default_commit_min_interval(),
            DEFAULT_STORAGE_COMMIT_MIN_INTERVAL_MS
        );
        assert_eq!(
            default_commit_max_interval(),
            DEFAULT_STORAGE_COMMIT_MAX_INTERVAL_MS
        );
        assert_eq!(
            default_commit_docs_threshold(),
            DEFAULT_STORAGE_COMMIT_DOCS_THRESHOLD
        );
        assert_eq!(
            default_commit_bytes_threshold(),
            DEFAULT_STORAGE_COMMIT_BYTES_THRESHOLD
        );
        assert_eq!(
            default_recent_overlay_enabled(),
            DEFAULT_STORAGE_RECENT_OVERLAY_ENABLED
        );
        assert_eq!(
            default_recent_overlay_ttl_secs(),
            DEFAULT_STORAGE_RECENT_OVERLAY_TTL_SECS
        );
        assert_eq!(
            default_recent_overlay_per_user_max_docs(),
            DEFAULT_STORAGE_RECENT_OVERLAY_PER_USER_MAX_DOCS
        );
        assert_eq!(
            default_recent_overlay_per_user_max_bytes(),
            DEFAULT_STORAGE_RECENT_OVERLAY_PER_USER_MAX_BYTES
        );
        assert_eq!(
            default_recent_overlay_global_max_bytes(),
            DEFAULT_STORAGE_RECENT_OVERLAY_GLOBAL_MAX_BYTES
        );
        assert_eq!(
            default_recent_overlay_query_limit(),
            DEFAULT_STORAGE_RECENT_OVERLAY_QUERY_LIMIT
        );
    }

    #[test]
    fn test_more_config_defaults() {
        assert_eq!(default_auto_initialize(), true);
        assert_eq!(default_shard_count(), 1);
        assert_eq!(default_physical_node_id(), 1);
    }

    #[test]
    fn test_app_config_accessors() {
        let mut config = AppConfig::default();
        config.llm.provider = LLMProvider::Gemini;
        config.llm.google_api_key = Some("gemini-key".into());
        config.llm.model = "gemini-model".into();
        config.llm.embedding_model = "gemini-embedding".into();

        assert_eq!(config.get_active_key(), Some("gemini-key".into()));
        assert_eq!(config.get_model_name(), "gemini-model");
        assert_eq!(config.get_embedding_model_name(), "gemini-embedding");
        assert_eq!(
            config.get_base_url(),
            Some("https://generativelanguage.googleapis.com/v1beta/openai/".into())
        );

        config.llm.provider = LLMProvider::OpenAI;
        config.llm.openai_api_key = Some("openai-key".into());
        assert_eq!(config.get_active_key(), Some("openai-key".into()));
        assert_eq!(config.get_base_url(), None);
    }

    #[test]
    fn test_sharding_accessors() {
        let mut config = AppConfig::default();
        config.raft.node_id = 42;

        assert_eq!(config.is_sharded(), false);
        assert_eq!(config.shard_count(), 1);
        assert_eq!(config.physical_node_id(), 42);
        assert_eq!(config.cluster_node_count(), 1);
        assert_eq!(config.is_cluster_mode(), false);

        config.sharding = Some(ShardingConfig {
            enabled: true,
            shard_count: 5,
            physical_node_id: 10,
            nodes: vec![
                ShardNodeConfig {
                    id: 1,
                    http_addr: "".into(),
                    raft_base_port: 0,
                },
                ShardNodeConfig {
                    id: 2,
                    http_addr: "".into(),
                    raft_base_port: 0,
                },
            ],
        });

        assert_eq!(config.is_sharded(), true);
        assert_eq!(config.shard_count(), 5);
        assert_eq!(config.physical_node_id(), 10);
        assert_eq!(config.cluster_node_count(), 2);
        assert_eq!(config.is_cluster_mode(), true);
    }
}
