use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use uuid::Uuid;

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
pub struct AppRecord {
    pub app_id: String,
    pub org_id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiKeyRecord {
    key_id: String,
    app_id: String,
    org_id: String,
    name: String,
    key_prefix: String,
    key_hash: String,
    created_at: DateTime<Utc>,
    revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiKeySummary {
    pub key_id: String,
    pub app_id: String,
    pub org_id: String,
    pub name: String,
    pub key_prefix: String,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct AuthenticatedApiKey {
    pub app_id: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct RegistryData {
    #[serde(default)]
    organizations: Vec<OrganizationRecord>,
    #[serde(default)]
    apps: Vec<AppRecord>,
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

    pub async fn list_apps(&self, org_id: Option<&str>) -> Result<Vec<AppRecord>> {
        let _lock = self.file_lock.lock().await;
        let mut data = self.read_data()?;
        ensure_default_org(&mut data);
        let mut apps: Vec<_> = data
            .apps
            .into_iter()
            .filter(|app| org_id.map(|value| app.org_id == value).unwrap_or(true))
            .collect();
        apps.sort_by(|a, b| a.app_id.cmp(&b.app_id));
        Ok(apps)
    }

    pub async fn get_app(&self, app_id: &str) -> Result<Option<AppRecord>> {
        let _lock = self.file_lock.lock().await;
        let data = self.read_data()?;
        Ok(data.apps.into_iter().find(|app| app.app_id == app_id))
    }

    pub async fn create_app(
        &self,
        app_id: &str,
        org_id: &str,
        name: Option<String>,
    ) -> Result<AppRecord> {
        let _lock = self.file_lock.lock().await;
        let mut data = self.read_data()?;
        ensure_default_org(&mut data);

        if data.apps.iter().any(|app| app.app_id == app_id) {
            return Err(anyhow!("application already exists"));
        }
        if !data.organizations.iter().any(|org| org.org_id == org_id) {
            return Err(anyhow!("organization not found"));
        }

        let record = AppRecord {
            app_id: app_id.to_string(),
            org_id: org_id.to_string(),
            name: normalize_name(name, app_id),
            created_at: Utc::now(),
        };
        data.apps.push(record.clone());
        data.apps.sort_by(|a, b| a.app_id.cmp(&b.app_id));
        self.write_data(&data)?;
        Ok(record)
    }

    pub async fn list_api_keys(&self, app_id: &str) -> Result<Vec<ApiKeySummary>> {
        let _lock = self.file_lock.lock().await;
        let data = self.read_data()?;
        Ok(data
            .api_keys
            .into_iter()
            .filter(|key| key.app_id == app_id)
            .map(ApiKeySummary::from)
            .collect())
    }

    pub async fn create_api_key(
        &self,
        app_id: &str,
        name: Option<String>,
    ) -> Result<(ApiKeySummary, String)> {
        let _lock = self.file_lock.lock().await;
        let mut data = self.read_data()?;

        let app = data
            .apps
            .iter()
            .find(|candidate| candidate.app_id == app_id)
            .cloned()
            .ok_or_else(|| anyhow!("application not found"))?;

        let raw_key = generate_api_key();
        let key_record = ApiKeyRecord {
            key_id: Uuid::new_v4().to_string(),
            app_id: app.app_id.clone(),
            org_id: app.org_id.clone(),
            name: normalize_name(name, "Primary API Key"),
            key_prefix: raw_key.chars().take(12).collect(),
            key_hash: hash_api_key(&raw_key),
            created_at: Utc::now(),
            revoked_at: None,
        };

        let summary = ApiKeySummary::from(key_record.clone());
        data.api_keys.push(key_record);
        data.api_keys
            .sort_by(|a, b| b.created_at.cmp(&a.created_at).then(a.key_id.cmp(&b.key_id)));
        self.write_data(&data)?;
        Ok((summary, raw_key))
    }

    pub async fn revoke_api_key(&self, app_id: &str, key_id: &str) -> Result<bool> {
        let _lock = self.file_lock.lock().await;
        let mut data = self.read_data()?;

        let Some(record) = data
            .api_keys
            .iter_mut()
            .find(|candidate| candidate.app_id == app_id && candidate.key_id == key_id)
        else {
            return Ok(false);
        };

        if record.revoked_at.is_some() {
            return Ok(true);
        }

        record.revoked_at = Some(Utc::now());
        self.write_data(&data)?;
        Ok(true)
    }

    pub async fn authenticate_api_key(&self, raw_key: &str) -> Result<Option<AuthenticatedApiKey>> {
        let _lock = self.file_lock.lock().await;
        let data = self.read_data()?;
        let hashed = hash_api_key(raw_key);

        Ok(data.api_keys.into_iter().find_map(|record| {
            if record.revoked_at.is_none() && record.key_hash == hashed {
                Some(AuthenticatedApiKey {
                    app_id: record.app_id,
                })
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

impl From<ApiKeyRecord> for ApiKeySummary {
    fn from(value: ApiKeyRecord) -> Self {
        Self {
            key_id: value.key_id,
            app_id: value.app_id,
            org_id: value.org_id,
            name: value.name,
            key_prefix: value.key_prefix,
            created_at: value.created_at,
            revoked_at: value.revoked_at,
        }
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

fn generate_api_key() -> String {
    let mut bytes = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut bytes);
    let suffix: String = bytes.iter().map(|byte| format!("{:02x}", byte)).collect();
    format!("mrs_{}", suffix)
}

fn hash_api_key(raw_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_key.as_bytes());
    format!("{:x}", hasher.finalize())
}
