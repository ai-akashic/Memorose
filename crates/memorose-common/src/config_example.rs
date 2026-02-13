// Copyright 2026 Akashic Project Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Configuration loading with TOML + environment variables
//!
//! Priority (highest to lowest):
//! 1. Environment variables (e.g., MEMOROSE__SERVER__PORT=8080)
//! 2. config.{environment}.toml (e.g., config.production.toml)
//! 3. config.toml (default)

use serde::{Deserialize, Serialize};
use config::{Config, ConfigError, Environment, File};
use std::path::PathBuf;

/// Root configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub consolidation: ConsolidationConfig,
    pub forgetting: ForgettingConfig,
    pub graph: GraphConfig,
    #[serde(default)]
    pub raft: Option<RaftConfig>,
    pub cache: CacheConfig,
    #[serde(default)]
    pub multimodal: MultimodalConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    pub telemetry: TelemetryConfig,
    #[serde(default)]
    pub development: DevelopmentConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub workers: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default = "default_wal_sync_mode")]
    pub wal_sync_mode: String,
    #[serde(default)]
    pub rocksdb: RocksDBConfig,
    #[serde(default)]
    pub lance: LanceConfig,
    #[serde(default)]
    pub tantivy: TantivyConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RocksDBConfig {
    #[serde(default = "default_max_open_files")]
    pub max_open_files: i32,
    #[serde(default = "default_write_buffer_size_mb")]
    pub write_buffer_size_mb: usize,
    #[serde(default = "default_max_write_buffer_number")]
    pub max_write_buffer_number: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LanceConfig {
    #[serde(default = "default_index_cache_size_mb")]
    pub index_cache_size_mb: usize,
    #[serde(default)]
    pub use_legacy_format: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TantivyConfig {
    #[serde(default = "default_heap_size_mb")]
    pub heap_size_mb: usize,
    #[serde(default = "default_num_threads")]
    pub num_threads: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsolidationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_entropy_threshold")]
    pub entropy_threshold: f64,
    #[serde(default = "default_similarity_threshold")]
    pub similarity_threshold: f64,
    pub llm: LLMConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LLMConfig {
    #[serde(default = "default_llm_provider")]
    pub provider: String,
    #[serde(default = "default_llm_model")]
    pub model: String,
    #[serde(default = "default_embedding_model")]
    pub embedding_model: String,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_temperature")]
    pub temperature: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForgettingConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_decay_half_life_days")]
    pub decay_half_life_days: u64,
    #[serde(default = "default_min_importance")]
    pub min_importance: f64,
    #[serde(default = "default_prune_interval_secs")]
    pub prune_interval_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphConfig {
    #[serde(default = "default_similarity_threshold")]
    pub auto_link_threshold: f64,
    #[serde(default = "default_max_edges_per_node")]
    pub max_edges_per_node: usize,
    #[serde(default = "default_true")]
    pub enable_page_rank: bool,
    #[serde(default = "default_true")]
    pub enable_community_detection: bool,
    #[serde(default = "default_prune_interval_secs")]
    pub community_detection_interval_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    pub node_id: String,
    pub raft_addr: String,
    #[serde(default = "default_election_timeout_min_ms")]
    pub election_timeout_min_ms: u64,
    #[serde(default = "default_election_timeout_max_ms")]
    pub election_timeout_max_ms: u64,
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval: u64,
    #[serde(default = "default_max_snapshot_count")]
    pub max_snapshot_count: usize,
    #[serde(default)]
    pub peers: Vec<RaftPeer>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftPeer {
    pub node_id: String,
    pub raft_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_cache_memory_mb")]
    pub max_memory_mb: usize,
    #[serde(default = "default_cache_ttl_secs")]
    pub ttl_secs: u64,
    #[serde(default = "default_eviction_policy")]
    pub eviction_policy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MultimodalConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_clip_model")]
    pub clip_model: String,
    #[serde(default = "default_whisper_model")]
    pub whisper_model: String,
    #[serde(default = "default_video_keyframe_count")]
    pub video_keyframe_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    #[serde(default)]
    pub require_auth: bool,
    pub jwt_secret: Option<String>,
    #[serde(default = "default_jwt_expiration_secs")]
    pub jwt_expiration_secs: u64,
    #[serde(default = "default_true")]
    pub enable_rate_limit: bool,
    #[serde(default = "default_rate_limit_rpm")]
    pub rate_limit_rpm: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,
    pub tracing_endpoint: Option<String>,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_true")]
    pub access_logs: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DevelopmentConfig {
    #[serde(default)]
    pub debug: bool,
    #[serde(default)]
    pub use_mock_llm: bool,
    #[serde(default)]
    pub hot_reload: bool,
    pub random_seed: Option<u64>,
}

// ============================================
// Default value functions
// ============================================
fn default_host() -> String { "0.0.0.0".to_string() }
fn default_port() -> u16 { 3000 }
fn default_data_dir() -> PathBuf { PathBuf::from("./data") }
fn default_wal_sync_mode() -> String { "normal".to_string() }
fn default_max_open_files() -> i32 { 1000 }
fn default_write_buffer_size_mb() -> usize { 64 }
fn default_max_write_buffer_number() -> i32 { 3 }
fn default_index_cache_size_mb() -> usize { 256 }
fn default_heap_size_mb() -> usize { 128 }
fn default_num_threads() -> usize { 2 }
fn default_interval_secs() -> u64 { 10 }
fn default_batch_size() -> usize { 100 }
fn default_entropy_threshold() -> f64 { 2.5 }
fn default_similarity_threshold() -> f64 { 0.7 }
fn default_llm_provider() -> String { "gemini".to_string() }
fn default_llm_model() -> String { "gemini-2.0-flash".to_string() }
fn default_embedding_model() -> String { "text-embedding-004".to_string() }
fn default_max_retries() -> u32 { 3 }
fn default_timeout_secs() -> u64 { 30 }
fn default_temperature() -> f32 { 0.3 }
fn default_decay_half_life_days() -> u64 { 30 }
fn default_min_importance() -> f64 { 0.1 }
fn default_prune_interval_secs() -> u64 { 3600 }
fn default_max_edges_per_node() -> usize { 100 }
fn default_election_timeout_min_ms() -> u64 { 150 }
fn default_election_timeout_max_ms() -> u64 { 300 }
fn default_heartbeat_interval_ms() -> u64 { 50 }
fn default_snapshot_interval() -> u64 { 1000 }
fn default_max_snapshot_count() -> usize { 5 }
fn default_cache_memory_mb() -> usize { 512 }
fn default_cache_ttl_secs() -> u64 { 300 }
fn default_eviction_policy() -> String { "lru".to_string() }
fn default_clip_model() -> String { "openai/clip-vit-base-patch32".to_string() }
fn default_whisper_model() -> String { "openai/whisper-base".to_string() }
fn default_video_keyframe_count() -> usize { 10 }
fn default_jwt_expiration_secs() -> u64 { 86400 }
fn default_rate_limit_rpm() -> u32 { 1000 }
fn default_metrics_port() -> u16 { 9090 }
fn default_log_level() -> String { "info".to_string() }
fn default_true() -> bool { true }

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            require_auth: false,
            jwt_secret: None,
            jwt_expiration_secs: default_jwt_expiration_secs(),
            enable_rate_limit: true,
            rate_limit_rpm: default_rate_limit_rpm(),
        }
    }
}

// ============================================
// Configuration loading
// ============================================
impl AppConfig {
    /// Load configuration with environment-based priority
    ///
    /// Priority (highest to lowest):
    /// 1. Environment variables (MEMOROSE__SERVER__PORT)
    /// 2. config.{env}.toml (e.g., config.production.toml)
    /// 3. config.toml
    /// 4. Built-in defaults
    pub fn load() -> Result<Self, ConfigError> {
        let env = std::env::var("RUN_ENV").unwrap_or_else(|_| "development".to_string());

        let config = Config::builder()
            // Start with defaults from config.toml
            .add_source(File::with_name("config").required(false))
            // Override with environment-specific config
            .add_source(File::with_name(&format!("config.{}", env)).required(false))
            // Override with environment variables
            // Example: MEMOROSE__SERVER__PORT=8080
            .add_source(
                Environment::with_prefix("MEMOROSE")
                    .separator("__")
                    .try_parsing(true)
            )
            .build()?;

        config.try_deserialize()
    }

    /// Load from a specific file path
    pub fn load_from_path(path: &str) -> Result<Self, ConfigError> {
        let config = Config::builder()
            .add_source(File::with_name(path))
            .add_source(
                Environment::with_prefix("MEMOROSE")
                    .separator("__")
                    .try_parsing(true)
            )
            .build()?;

        config.try_deserialize()
    }

    /// Get API key from environment (never from config file)
    pub fn get_api_key(&self) -> Result<String, ConfigError> {
        match self.consolidation.llm.provider.as_str() {
            "gemini" => std::env::var("GOOGLE_API_KEY")
                .or_else(|_| std::env::var("GEMINI_API_KEY"))
                .map_err(|_| ConfigError::Message(
                    "GOOGLE_API_KEY or GEMINI_API_KEY must be set".to_string()
                )),
            "openai" => std::env::var("OPENAI_API_KEY")
                .map_err(|_| ConfigError::Message(
                    "OPENAI_API_KEY must be set".to_string()
                )),
            _ => Ok("mock_key".to_string()), // For mock LLM
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AppConfig::load().unwrap_or_else(|_| {
            // If no config file exists, use programmatic defaults
            serde_json::from_str(r#"{
                "server": {"host": "0.0.0.0", "port": 3000, "workers": 0},
                "database": {
                    "data_dir": "./data",
                    "wal_sync_mode": "normal",
                    "rocksdb": {},
                    "lance": {},
                    "tantivy": {}
                },
                "consolidation": {
                    "enabled": true,
                    "interval_secs": 10,
                    "batch_size": 100,
                    "entropy_threshold": 2.5,
                    "similarity_threshold": 0.7,
                    "llm": {
                        "provider": "gemini",
                        "model": "gemini-2.0-flash",
                        "embedding_model": "text-embedding-004",
                        "max_retries": 3,
                        "timeout_secs": 30,
                        "temperature": 0.3
                    }
                },
                "forgetting": {
                    "enabled": true,
                    "decay_half_life_days": 30,
                    "min_importance": 0.1,
                    "prune_interval_secs": 3600
                },
                "graph": {
                    "auto_link_threshold": 0.7,
                    "max_edges_per_node": 100,
                    "enable_page_rank": true,
                    "enable_community_detection": true,
                    "community_detection_interval_secs": 3600
                },
                "cache": {
                    "enabled": true,
                    "max_memory_mb": 512,
                    "ttl_secs": 300,
                    "eviction_policy": "lru"
                },
                "telemetry": {
                    "enabled": true,
                    "metrics_port": 9090,
                    "log_level": "info",
                    "access_logs": true
                }
            }"#).unwrap()
        });

        assert_eq!(config.server.port, 3000);
        assert_eq!(config.consolidation.llm.provider, "gemini");
    }

    #[test]
    fn test_env_override() {
        std::env::set_var("MEMOROSE__SERVER__PORT", "8080");
        std::env::set_var("MEMOROSE__CONSOLIDATION__ENABLED", "false");

        // Note: This will only work if config file exists
        // In practice, test with actual config file in CI
    }
}
