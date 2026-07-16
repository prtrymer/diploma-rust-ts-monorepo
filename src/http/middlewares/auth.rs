use axum::{
    async_trait,
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
}

pub struct AuthError(pub StatusCode, pub String);

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({
            "error": self.1,
        }));
        (self.0, body).into_response()
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for Claims
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let auth_header = parts
            .headers
            .get("Authorization")
            .and_then(|value| value.to_str().ok());

        let token = match auth_header {
            Some(header) if header.starts_with("Bearer ") => header[7..].to_string(),
            _ => {
                // For SSE, also try to get the token from the query parameters
                let query = parts.uri.query().unwrap_or("");
                let token_param = query.split('&').find(|p| p.starts_with("token="));
                match token_param {
                    Some(param) => param[6..].to_string(),
                    None => return Err(AuthError(StatusCode::UNAUTHORIZED, "Missing or invalid token".to_string())),
                }
            }
        };

        let token_data = decode::<Claims>(
            &token,
            &DecodingKey::from_secret(crate::http::jwt::secret()),
            &Validation::default(),
        )
        .map_err(|e| AuthError(StatusCode::UNAUTHORIZED, format!("Invalid token: {}", e)))?;

        Ok(token_data.claims)
    }
}
