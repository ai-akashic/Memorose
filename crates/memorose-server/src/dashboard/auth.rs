use anyhow::Result;
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use bcrypt::{hash, DEFAULT_COST};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

const AUTH_FILE: &str = "dashboard_auth.json";
const SECRET_FILE: &str = "dashboard_secret.key";

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthData {
    pub username: String,
    pub password_hash: String,
    #[serde(default)]
    pub must_change_password: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
}

pub struct DashboardAuth {
    pub auth_path: std::path::PathBuf,
    pub secret: Vec<u8>,
    pub file_lock: Mutex<()>,
    /// Pre-computed bcrypt hash for timing-attack-safe comparison on invalid usernames
    pub dummy_hash: String,
}

impl DashboardAuth {
    pub fn new(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let auth_path = data_dir.join(AUTH_FILE);
        let secret_path = data_dir.join(SECRET_FILE);

        // Generate or load JWT secret
        let secret = if secret_path.exists() {
            std::fs::read(&secret_path)?
        } else {
            let secret: Vec<u8> = (0..64).map(|_| rand::random::<u8>()).collect();
            std::fs::write(&secret_path, &secret)?;
            // Restrict file permissions on Unix
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&secret_path, std::fs::Permissions::from_mode(0o600))?;
            }
            secret
        };

        // Use create_new(true) for an atomic create -- prevents TOCTOU race between
        // check and write when multiple processes start simultaneously.
        let initial_password = std::env::var("DASHBOARD_ADMIN_PASSWORD")
            .unwrap_or_else(|_| "admin".to_string());
        let must_change = initial_password == "admin";
        let password_hash = hash(&initial_password, DEFAULT_COST)?;
        let auth_data = AuthData {
            username: "admin".to_string(),
            password_hash,
            must_change_password: must_change,
        };
        let json = serde_json::to_string_pretty(&auth_data)?;

        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&auth_path)
        {
            Ok(mut file) => {
                file.write_all(json.as_bytes())?;
                if must_change {
                    tracing::warn!("Dashboard using default credentials (admin/admin). Set DASHBOARD_ADMIN_PASSWORD or change via Settings.");
                } else {
                    tracing::info!("Dashboard admin user created with password from DASHBOARD_ADMIN_PASSWORD env var.");
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Another process created the file first — that's fine, use theirs.
            }
            Err(e) => return Err(e.into()),
        }

        // Generate a real bcrypt dummy hash for timing-attack prevention
        let dummy_hash = hash("__dummy_timing_pad__", DEFAULT_COST)?;

        Ok(Self {
            auth_path,
            secret,
            file_lock: Mutex::new(()),
            dummy_hash,
        })
    }

    pub fn create_token(&self, username: &str) -> Result<String> {
        let expiration = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::hours(24))
            .unwrap()
            .timestamp() as usize;

        let claims = Claims {
            sub: username.to_string(),
            exp: expiration,
        };

        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(&self.secret),
        )?;

        Ok(token)
    }

    pub fn verify_token(&self, token: &str) -> Result<Claims> {
        let token_data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(&self.secret),
            &Validation::default(),
        )?;

        Ok(token_data.claims)
    }
}

pub async fn auth_middleware(
    State(state): State<Arc<crate::AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let auth_header = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    let token = match auth_header {
        Some(header) if header.starts_with("Bearer ") => &header[7..],
        _ => {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Missing or invalid Authorization header" })),
            )
                .into_response();
        }
    };

    match state.dashboard_auth.verify_token(token) {
        Ok(_claims) => next.run(request).await,
        Err(_) => (
            axum::http::StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Invalid or expired token" })),
        )
            .into_response(),
    }
}
