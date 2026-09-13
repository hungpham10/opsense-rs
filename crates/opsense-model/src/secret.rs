use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use reqwest::Client as HttpClient;
use reqwest::header::{ACCEPT, AUTHORIZATION};
use serde::Deserialize;

const INFISICAL_BASE_URL: &str = "https://app.infisical.com";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoginResponse {
    access_token: String,
}

#[derive(Deserialize)]
struct GetSecretResponse {
    secret: SecretPayload,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SecretPayload {
    secret_value: String,
}

#[derive(Clone)]
struct InfisicalClient {
    http: HttpClient,
    base_url: String,
}

impl InfisicalClient {
    async fn new(client_id: String, client_secret: String) -> Result<Self, Error> {
        let base_url =
            std::env::var("INFISICAL_API_URL").unwrap_or_else(|_| INFISICAL_BASE_URL.to_string());

        let http = HttpClient::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("infisical-rs")
            .build()
            .map_err(|error| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("Fail to build infisical client: {:?}", error),
                )
            })?;

        let token = Self::login(&http, &base_url, &client_id, &client_secret).await?;

        let mut headers = reqwest::header::HeaderMap::new();
        let mut auth_value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|error| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("Fail to login to infisical: {:?}", error),
                )
            })?;
        auth_value.set_sensitive(true);
        headers.insert(AUTHORIZATION, auth_value);

        let http = HttpClient::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("infisical-rs")
            .default_headers(headers)
            .build()
            .map_err(|error| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("Fail to build infisical client: {:?}", error),
                )
            })?;

        Ok(Self { http, base_url })
    }

    async fn login(
        http: &HttpClient,
        base_url: &str,
        client_id: &str,
        client_secret: &str,
    ) -> Result<String, Error> {
        let url = format!("{base_url}/api/v1/auth/universal-auth/login");

        let response = http
            .post(&url)
            .json(&serde_json::json!({
                "clientId": client_id,
                "clientSecret": client_secret,
            }))
            .send()
            .await
            .map_err(|error| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("Fail to login to infisical: {:?}", error),
                )
            })?
            .error_for_status()
            .map_err(|error| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("Fail to login to infisical: {:?}", error),
                )
            })?;

        let payload: LoginResponse = response.json().await.map_err(|error| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("Fail to login to infisical: {:?}", error),
            )
        })?;

        Ok(payload.access_token)
    }

    async fn get_secret(&self, key: &str, path: &str) -> Result<String, Error> {
        let project_id = std::env::var("INFISICAL_PROJECT_ID")
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "Invalid INFISICAL_PROJECT_ID"))?;
        let environment = std::env::var("ENVIRONMENT").unwrap_or_else(|_| "dev".to_string());

        let url = format!("{}/api/v3/secrets/raw/{key}", self.base_url);
        let response = self
            .http
            .get(&url)
            .query(&[
                ("workspaceId", project_id.as_str()),
                ("environment", environment.as_str()),
                ("secretPath", if path.is_empty() { "/" } else { path }),
                ("expandSecretReferences", "true"),
                ("type", "shared"),
                ("include_imports", "true"),
            ])
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(|error| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("Fail fetching secret: {:?}", error),
                )
            })?
            .error_for_status()
            .map_err(|error| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("Fail fetching secret: {:?}", error),
                )
            })?;

        let payload: GetSecretResponse = response.json().await.map_err(|error| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("Fail fetching secret: {:?}", error),
            )
        })?;

        Ok(payload.secret.secret_value)
    }
}

#[derive(Clone)]
pub struct Secret {
    client: Option<Arc<InfisicalClient>>,
}

impl Secret {
    pub async fn new() -> Result<Self, Error> {
        let client_id = std::env::var("INFISICAL_CLIENT_ID");
        let client_secret = std::env::var("INFISICAL_CLIENT_SECRET");

        let client = match (client_id, client_secret) {
            (Ok(id), Ok(secret)) => Some(Arc::new(InfisicalClient::new(id, secret).await?)),
            _ => None,
        };

        Ok(Self { client })
    }

    pub async fn get(&self, key: &str, path: &str) -> Result<String, Error> {
        if let Ok(value) = std::env::var(key) {
            Ok(value)
        } else {
            self.force(key, path).await
        }
    }

    pub async fn force(&self, key: &str, path: &str) -> Result<String, Error> {
        match &self.client {
            Some(client) => {
                let secret_value = client.get_secret(key, path).await?;

                unsafe {
                    std::env::set_var(key, secret_value.clone());
                }
                Ok(secret_value)
            }
            None => {
                // No Infisical client available — check env var directly
                std::env::var(key).map_err(|_| {
                    Error::new(
                        ErrorKind::NotFound,
                        format!(
                            "Secret '{}' not found and no Infisical client available",
                            key
                        ),
                    )
                })
            }
        }
    }
}
