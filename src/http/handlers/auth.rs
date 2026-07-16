use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{extract::State, http::StatusCode, Json};
use chrono::{Duration, Utc};
use jsonwebtoken::{encode, EncodingKey, Header};

use crate::database::domain::models::User;
use crate::http::middlewares::auth::Claims;
use crate::http::models::{AuthResponse, LoginRequest, RegisterRequest};
use super::AppState;

/// Register a new user account.
#[utoipa::path(
    post,
    path = "/api/auth/register",
    tag = "Auth",
    request_body = RegisterRequest,
    responses(
        (status = 201, description = "User registered successfully", body = AuthResponse),
        (status = 409, description = "Username already exists",
            body = String, example = json!({"error": "Username already exists"})),
        (status = 500, description = "Internal server error",
            body = String, example = json!({"error": "Hashing error: ..."})),
    )
)]
pub async fn register(
    State(state): State<AppState>,
    Json(payload): Json<RegisterRequest>,
) -> Result<(StatusCode, Json<AuthResponse>), (StatusCode, Json<serde_json::Value>)> {
    // Check if user already exists (PostgreSQL)
    if let Ok(Some(_)) = state.user_repo.get_user_by_username(&payload.username).await {
        return Err((
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "Username already exists" })),
        ));
    }

    // Hash password with Argon2
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let password_hash = argon2
        .hash_password(payload.password.as_bytes(), &salt)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Hashing error: {}", e) })),
            )
        })?
        .to_string();

    let user = User {
        username: payload.username.clone(),
        password_hash,
        created_at: Utc::now(),
    };

    // Persist to PostgreSQL
    state.user_repo.create_user(&user).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("DB error: {}", e) })),
        )
    })?;

    Ok((
        StatusCode::CREATED,
        Json(AuthResponse {
            token: "".to_string(),
            message: "User registered successfully".to_string(),
        }),
    ))
}

/// Authenticate and receive a JWT Bearer token.
#[utoipa::path(
    post,
    path = "/api/auth/login",
    tag = "Auth",
    request_body = LoginRequest,
    responses(
        (status = 200, description = "Login successful – JWT token returned", body = AuthResponse),
        (status = 401, description = "Invalid credentials",
            body = String, example = json!({"error": "Invalid username or password"})),
        (status = 500, description = "Internal server error",
            body = String, example = json!({"error": "DB error: ..."})),
    )
)]
pub async fn login(
    State(state): State<AppState>,
    Json(payload): Json<LoginRequest>,
) -> Result<(StatusCode, Json<AuthResponse>), (StatusCode, Json<serde_json::Value>)> {
    // Lookup in PostgreSQL
    let user = state
        .user_repo
        .get_user_by_username(&payload.username)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("DB error: {}", e) })),
            )
        })?
        .ok_or((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Invalid username or password" })),
        ))?;

    let parsed_hash = PasswordHash::new(&user.password_hash).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("Hash parse error: {}", e) })),
        )
    })?;

    if Argon2::default()
        .verify_password(payload.password.as_bytes(), &parsed_hash)
        .is_err()
    {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Invalid username or password" })),
        ));
    }

    let expiration = Utc::now()
        .checked_add_signed(Duration::hours(24))
        .expect("valid timestamp")
        .timestamp() as usize;

    let claims = Claims {
        sub: user.username.clone(),
        exp: expiration,
    };

    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(crate::http::jwt::secret()),
    )
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("Token creation error: {}", e) })),
        )
    })?;

    Ok((
        StatusCode::OK,
        Json(AuthResponse {
            token,
            message: "Login successful".to_string(),
        }),
    ))
}
