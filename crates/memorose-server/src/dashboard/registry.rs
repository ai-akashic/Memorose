use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;

const REGISTRY_FILE: &str = "dashboard_registry.json";
pub const DEFAULT_ORG_ID: &str = "default";
const DEFAULT_ORG_NAME: &str = "Default";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrganizationRecord {
    pub org_id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiKeyRecord {
    key_id: String,
    org_id: String,
    name: String,
    key_prefix: String,
    key_hash: String,
    created_at: DateTime<Utc>,
    revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct AuthenticatedApiKey;

#[derive(Debug, Serialize, Deserialize, Default)]
struct RegistryData {
    #[serde(default)]
    organizations: Vec<OrganizationRecord>,
    #[serde(default)]
    api_keys: Vec<ApiKeyRecord>,
}

pub struct ManagementRegistry {
    path: PathBuf,
    file_lock: Mutex<()>,
}

impl ManagementRegistry {
    pub fn new(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join(REGISTRY_FILE);
        let registry = Self {
            path,
            file_lock: Mutex::new(()),
        };

        if !registry.path.exists() {
            registry.write_data(&RegistryData {
                organizations: vec![default_org()],
                ..RegistryData::default()
            })?;
            return Ok(registry);
        }

        let mut data = registry.read_data()?;
        if ensure_default_org(&mut data) {
            registry.write_data(&data)?;
        }

        Ok(registry)
    }

    pub async fn list_organizations(&self) -> Result<Vec<OrganizationRecord>> {
        let _lock = self.file_lock.lock().await;
        let mut data = self.read_data()?;
        ensure_default_org(&mut data);
        data.organizations.sort_by(|a, b| a.org_id.cmp(&b.org_id));
        Ok(data.organizations)
    }

    pub async fn create_organization(
        &self,
        org_id: &str,
        name: Option<String>,
    ) -> Result<OrganizationRecord> {
        let _lock = self.file_lock.lock().await;
        let mut data = self.read_data()?;
        ensure_default_org(&mut data);

        if data.organizations.iter().any(|org| org.org_id == org_id) {
            return Err(anyhow!("organization already exists"));
        }

        let record = OrganizationRecord {
            org_id: org_id.to_string(),
            name: normalize_name(name, org_id),
            created_at: Utc::now(),
        };
        data.organizations.push(record.clone());
        data.organizations.sort_by(|a, b| a.org_id.cmp(&b.org_id));
        self.write_data(&data)?;
        Ok(record)
    }

    pub async fn authenticate_api_key(&self, raw_key: &str) -> Result<Option<AuthenticatedApiKey>> {
        let _lock = self.file_lock.lock().await;
        let data = self.read_data()?;
        let hashed = hash_api_key(raw_key);

        Ok(data.api_keys.into_iter().find_map(|record| {
            if record.revoked_at.is_none() && record.key_hash == hashed {
                Some(AuthenticatedApiKey)
            } else {
                None
            }
        }))
    }

    fn read_data(&self) -> Result<RegistryData> {
        let raw = std::fs::read_to_string(&self.path)?;
        Ok(serde_json::from_str(&raw)?)
    }

    fn write_data(&self, data: &RegistryData) -> Result<()> {
        let json = serde_json::to_string_pretty(data)?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.path)?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }
}

fn normalize_name(name: Option<String>, fallback: &str) -> String {
    name.map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn ensure_default_org(data: &mut RegistryData) -> bool {
    if data
        .organizations
        .iter()
        .any(|org| org.org_id == DEFAULT_ORG_ID)
    {
        return false;
    }

    data.organizations.push(default_org());
    true
}

fn default_org() -> OrganizationRecord {
    OrganizationRecord {
        org_id: DEFAULT_ORG_ID.to_string(),
        name: DEFAULT_ORG_NAME.to_string(),
        created_at: Utc::now(),
    }
}

fn hash_api_key(raw_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_key.as_bytes());
    format!("{:x}", hasher.finalize())
}
