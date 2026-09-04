use axum::Router;
use axum::body::Body;
use axum::extract::{Json as JsonRequest, Path, Query, State};
use axum::response::{IntoResponse, Json as JsonResponse};
use axum::routing::{get, post};

use chrono::{DateTime, Utc};
use http::{header, Response, StatusCode};
use serde::{Deserialize, Serialize};
use tracing::error;

use opsense_model::cache::Cache;
use opsense_model::entities::admin::{AuthConfig, UserTokenInfo};

use crate::api::AppState;
use crate::api::admin::AdminHeaders;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct AdminResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    auth: Option<AuthConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Deserialize)]
pub struct PutTokenPayload {
    name: String,
    token: String,
}

#[derive(Deserialize)]
pub struct IssueUserTokenRequest {
    user_id: String,

    /// Hạn dùng dạng RFC 3339 (VD: 2026-12-31T23:59:59Z), bỏ trống = không hết hạn
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expires_at: Option<String>,
}

#[derive(Serialize, Default)]
pub struct UserTokenResponse {
    /// Plaintext token — chỉ trả về khi issue/reveal
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    users: Option<Vec<UserTokenInfo>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Deserialize)]
pub struct IntrospectTokenRequest {
    token: String,
}

#[derive(Serialize)]
pub struct IntrospectTokenResponse {
    active: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    tenant_id: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    user_id: Option<String>,
}

#[derive(Deserialize)]
pub struct OidcQuery {
    #[serde(default = "default_oidc_name")]
    pub name: String,
}

fn default_oidc_name() -> String {
    "default".to_string()
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/tenant/{host}/id", get(get_tenant_id))
        .route(
            "/tenant/{host}/auth-config",
            get(get_tenant_auth_config),
        )
        .route("/files/{*path}", get(fetch_file))
        .route("/tokens/generics/{name}", get(get_token).post(put_token))
        .route(
            "/tokens/users",
            post(issue_user_token).get(list_user_tokens),
        )
        .route(
            "/tokens/users/{user_id}",
            get(reveal_user_token).delete(revoke_user_token),
        )
        .route("/token/introspect", post(introspect_token))
}

async fn get_token(
    State(app_state): State<AppState>,
    Path(name): Path<String>,
    AdminHeaders { tenant_id, .. }: AdminHeaders,
) -> impl IntoResponse {
    match app_state
        .admin_entity
        .get_unencrypted_token(tenant_id.into(), &name)
        .await
    {
        Ok(token) => (StatusCode::OK, token),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Fail getting token: {error}"),
        ),
    }
}

async fn put_token(
    State(app_state): State<AppState>,
    AdminHeaders { tenant_id, .. }: AdminHeaders,
    JsonRequest(PutTokenPayload { name, token }): JsonRequest<PutTokenPayload>,
) -> impl IntoResponse {
    match app_state
        .admin_entity
        .put_unencrypted_token(tenant_id.into(), &name, &token)
        .await
    {
        Ok(_) => Ok(StatusCode::OK),
        Err(error) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Fail put new token {name}: {error}"),
        )),
    }
}

async fn issue_user_token(
    State(app_state): State<AppState>,
    AdminHeaders { tenant_id, .. }: AdminHeaders,
    JsonRequest(payload): JsonRequest<IssueUserTokenRequest>,
) -> impl IntoResponse {
    let expires_at = match payload
        .expires_at
        .as_deref()
        .map(DateTime::parse_from_rfc3339)
    {
        None => None,
        Some(Ok(datetime)) => Some(datetime.with_timezone(&Utc)),
        Some(Err(error)) => {
            return (
                StatusCode::BAD_REQUEST,
                JsonResponse(UserTokenResponse {
                    error: Some(format!("Invalid expires_at (expect RFC 3339): {error}")),
                    ..Default::default()
                }),
            );
        }
    };

    match app_state
        .admin_entity
        .issue_user_token(tenant_id.into(), &payload.user_id, expires_at)
        .await
    {
        Ok(token) => (
            StatusCode::CREATED,
            JsonResponse(UserTokenResponse {
                token: Some(token),
                ..Default::default()
            }),
        ),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            JsonResponse(UserTokenResponse {
                error: Some(format!(
                    "Fail issuing token for {}: {error}",
                    payload.user_id
                )),
                ..Default::default()
            }),
        ),
    }
}

async fn list_user_tokens(
    State(app_state): State<AppState>,
    AdminHeaders { tenant_id, .. }: AdminHeaders,
) -> impl IntoResponse {
    match app_state
        .admin_entity
        .list_user_tokens(tenant_id.into())
        .await
    {
        Ok(users) => (
            StatusCode::OK,
            JsonResponse(UserTokenResponse {
                users: Some(users),
                ..Default::default()
            }),
        ),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            JsonResponse(UserTokenResponse {
                error: Some(format!("Fail listing user tokens: {error}")),
                ..Default::default()
            }),
        ),
    }
}

async fn reveal_user_token(
    State(app_state): State<AppState>,
    Path(user_id): Path<String>,
    AdminHeaders { tenant_id, .. }: AdminHeaders,
) -> impl IntoResponse {
    match app_state
        .admin_entity
        .reveal_user_token(tenant_id.into(), &user_id)
        .await
    {
        Ok(token) => (
            StatusCode::OK,
            JsonResponse(UserTokenResponse {
                token: Some(token),
                ..Default::default()
            }),
        ),
        Err(error) => (
            StatusCode::NOT_FOUND,
            JsonResponse(UserTokenResponse {
                error: Some(format!("Fail revealing token of {user_id}: {error}")),
                ..Default::default()
            }),
        ),
    }
}

async fn revoke_user_token(
    State(app_state): State<AppState>,
    Path(user_id): Path<String>,
    AdminHeaders { tenant_id, .. }: AdminHeaders,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    match app_state
        .admin_entity
        .revoke_user_token(tenant_id.into(), &user_id)
        .await
    {
        Ok(_) => Ok(StatusCode::OK),
        Err(error) => Err((
            StatusCode::NOT_FOUND,
            format!("Fail revoking token of {user_id}: {error}"),
        )),
    }
}

async fn introspect_token(
    State(app_state): State<AppState>,
    AdminHeaders { tenant_id, .. }: AdminHeaders,
    JsonRequest(payload): JsonRequest<IntrospectTokenRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant = i64::from(tenant_id);
    match app_state
        .admin_entity
        .verify_user_token(tenant, &payload.token)
        .await
    {
        Ok(Some(user_id)) => Ok((
            StatusCode::OK,
            JsonResponse(IntrospectTokenResponse {
                active: true,
                tenant_id: Some(tenant),
                user_id: Some(user_id),
            }),
        )),
        Ok(None) => Ok((
            StatusCode::OK,
            JsonResponse(IntrospectTokenResponse {
                active: false,
                tenant_id: None,
                user_id: None,
            }),
        )),
        Err(error) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Fail introspecting token: {error}"),
        )),
    }
}

async fn get_tenant_id(
    State(app_state): State<AppState>,
    Path(host): Path<String>,
) -> impl IntoResponse {
    match app_state.admin_entity.get_tenant_id(&host).await {
        Ok(response) => (StatusCode::OK, format!("{}", response)),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Fail to get tenant of {}: {:?}", host, error),
        ),
    }
}

async fn get_tenant_auth_config(
    State(app_state): State<AppState>,
    Path(host): Path<String>,
    Query(query): Query<OidcQuery>,
) -> Result<impl IntoResponse, (StatusCode, JsonResponse<AdminResponse>)> {
    match app_state
        .admin_entity
        .get_tenant_auth_config(&host, &query.name)
        .await
    {
        Ok(auth_config) => Ok((
            StatusCode::OK,
            JsonResponse(AdminResponse {
                auth: Some(auth_config),
                ..Default::default()
            }),
        )),
        Err(error) => {
            error!(
                error = ?error,
                host = ?host,
                "Failed to get tenant configuration from database/service"
            );

            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                JsonResponse(AdminResponse {
                    error: Some(format!("Fail to get tenant of {}: {:?}", host, error)),
                    ..Default::default()
                }),
            ))
        }
    }
}

async fn fetch_file(
    State(app_state): State<AppState>,
    Path(path): Path<String>,
    AdminHeaders { tenant_id, host }: AdminHeaders,
) -> Result<Response<Body>, (StatusCode, JsonResponse<AdminResponse>)> {
    let tenant_id = tenant_id.into();
    let cache = Cache::new(app_state.connector.clone(), tenant_id);
    let host = host.hostname();
    let key = format!("seo_file:{}:{}", host, path);

    // Fallback chỉ dùng path người dùng truyền — Admin chưa có `get_full_path`.
    // Lưu path đã resolve vào cache để lần sau khỏi gọi lại DB.
    let path_in_str = match cache.get(&key).await {
        Ok(value) => value,
        Err(_) => format!("{}/{}", host, path),
    };

    let bucket = app_state.secret.get("S3_BUCKET", "/").await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            JsonResponse(AdminResponse {
                error: Some("S3_BUCKET not set".into()),
                ..Default::default()
            }),
        )
    })?;

    let response = app_state
        .s3
        .get_object()
        .bucket(&bucket)
        .key(&path_in_str)
        .send()
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                JsonResponse(AdminResponse {
                    error: Some(format!("S3 error {path_in_str}: {error}")),
                    ..Default::default()
                }),
            )
        })?;

    let content_type = response
        .content_type()
        .unwrap_or("application/octet-stream")
        .to_string();

    let content_length = response.content_length().unwrap_or(0);

    if let Err(error) = cache.set(&key, &path_in_str, 86400).await {
        log::warn!("Failed to cache response for key {}: {}", key, error);
    }

    let body_bytes = response.body.collect().await.map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            JsonResponse(AdminResponse {
                error: Some(format!("Stream error: {}", error)),
                ..Default::default()
            }),
        )
    })?;

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, content_length)
        .body(axum::body::Body::from(body_bytes.into_bytes()))
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                JsonResponse(AdminResponse {
                    error: Some(format!("Stream error: {}", error)),
                    ..Default::default()
                }),
            )
        })
}
