use axum::{extract::State, response::IntoResponse, Json};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct LoginRequest {
    username: String,
    password: String,
}

pub async fn login(
    State(state): State<Arc<crate::AppState>>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<LoginRequest>,
) -> axum::response::Response {
    let client_ip = headers
        .get("x-forwarded-for")
        .or_else(|| headers.get("x-real-ip"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .split(',')
        .next()
        .unwrap_or("unknown")
        .trim()
        .to_string();

    let attempts = state.login_limiter.get(&client_ip).await.unwrap_or(0);
    if attempts >= 5 {
        return (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({ "error": "Too many login attempts. Try again later." })),
        )
            .into_response();
    }

    let username = payload.username.clone();

    let _lock = state.dashboard_auth.file_lock.lock().await;

    let auth_path = state.dashboard_auth.auth_path.clone();
    let password = payload.password.clone();
    let u = username.clone();
    let dummy_hash = state.dashboard_auth.dummy_hash.clone();
    let verify_result = tokio::task::spawn_blocking(move || -> anyhow::Result<(bool, bool)> {
        let data = std::fs::read_to_string(&auth_path)?;
        let auth_data: crate::dashboard::auth::AuthData = serde_json::from_str(&data)?;
        let allow_default_admin = auth_data.username == "admin"
            && u == "admin"
            && password == "admin"
            && auth_data.must_change_password;

        let hash_to_check = if auth_data.username == u {
            auth_data.password_hash.clone()
        } else {
            dummy_hash
        };
        let valid = bcrypt::verify(&password, &hash_to_check).unwrap_or(false);
        let is_valid = allow_default_admin || (valid && auth_data.username == u);
        Ok((is_valid, auth_data.must_change_password))
    })
    .await;

    match verify_result {
        Ok(Ok((true, must_change))) => {
            state.login_limiter.invalidate(&client_ip).await;
            match state.dashboard_auth.create_token(&username) {
                Ok(token) => Json(serde_json::json!({
                    "token": token,
                    "expires_in": 86400,
                    "must_change_password": must_change,
                }))
                .into_response(),
                Err(e) => {
                    tracing::error!("Token creation failed: {}", e);
                    (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": "Internal server error" })),
                    )
                        .into_response()
                }
            }
        }
        Ok(Ok((false, _))) => {
            state.login_limiter.insert(client_ip, attempts + 1).await;
            (
                axum::http::StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Invalid credentials" })),
            )
                .into_response()
        }
        Ok(Err(e)) => {
            tracing::error!("Auth error: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Internal server error" })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Auth task error: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Internal server error" })),
            )
                .into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct ChangePasswordRequest {
    current_password: String,
    new_password: String,
}

pub async fn change_password(
    State(state): State<Arc<crate::AppState>>,
    Json(payload): Json<ChangePasswordRequest>,
) -> axum::response::Response {
    let auth_path = state.dashboard_auth.auth_path.clone();
    let current = payload.current_password.clone();
    let new_pw = payload.new_password.clone();
    let _lock = state.dashboard_auth.file_lock.lock().await;

    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<bool> {
        let data = std::fs::read_to_string(&auth_path)?;
        let auth_data: crate::dashboard::auth::AuthData = serde_json::from_str(&data)?;

        if !bcrypt::verify(&current, &auth_data.password_hash)? {
            return Ok(false);
        }

        if new_pw.len() < 8 {
            anyhow::bail!("Password must be at least 8 characters");
        }

        let new_hash = bcrypt::hash(&new_pw, bcrypt::DEFAULT_COST)?;
        let new_auth = serde_json::json!({
            "username": auth_data.username,
            "password_hash": new_hash,
            "must_change_password": false,
        });
        let json = serde_json::to_string_pretty(&new_auth)?;
        std::fs::write(&auth_path, json)?;
        Ok(true)
    })
    .await;

    match result {
        Ok(Ok(true)) => Json(serde_json::json!({ "status": "updated" })).into_response(),
        Ok(Ok(false)) => (
            axum::http::StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Current password is incorrect" })),
        )
            .into_response(),
        Ok(Err(e)) => {
            let msg = e.to_string();
            if msg.contains("at least") {
                (
                    axum::http::StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": msg })),
                )
                    .into_response()
            } else {
                tracing::error!("Password change error: {}", e);
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": "Internal server error" })),
                )
                    .into_response()
            }
        }
        Err(e) => {
            tracing::error!("Password change task error: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Internal server error" })),
            )
                .into_response()
        }
    }
}
