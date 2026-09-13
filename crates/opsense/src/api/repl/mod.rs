mod v1;
use v1::{MutationRoot, QueryRoot};

use std::sync::Arc;

use async_graphql::{EmptySubscription, Schema};
use axum::Extension;
use axum::Router;
use axum::routing::post;
use axum_extra::TypedHeader;
use axum_macros::FromRequestParts;
use headers::Header;
use http::{HeaderName, HeaderValue};

use super::{AppState, XTenantId};

#[derive(Debug, Clone)]
pub struct XUserId(pub Option<String>);

impl Header for XUserId {
    fn name() -> &'static HeaderName {
        static NAME: HeaderName = HeaderName::from_static("x-user-id");
        &NAME
    }

    fn decode<'i, I>(values: &mut I) -> std::result::Result<Self, headers::Error>
    where
        Self: Sized,
        I: Iterator<Item = &'i HeaderValue>,
    {
        // Lấy giá trị đầu tiên từ iterator
        let value = values.next();

        match value {
            Some(v) => {
                let value_str = v.to_str().map_err(|_| headers::Error::invalid())?;
                if value_str.is_empty() {
                    Ok(XUserId(None))
                } else {
                    Ok(XUserId(Some(value_str.to_string())))
                }
            }
            None => Ok(XUserId(None)),
        }
    }

    fn encode<E>(&self, values: &mut E)
    where
        E: Extend<HeaderValue>,
    {
        if let Some(ref id) = self.0
            && let Ok(value) = HeaderValue::from_str(id)
        {
            values.extend(std::iter::once(value));
        }
    }
}

#[derive(FromRequestParts)]
pub struct ReplHeaders {
    #[from_request(via(TypedHeader))]
    pub tenant_id: XTenantId,

    #[from_request(via(TypedHeader))]
    pub user_id: XUserId,
}

pub fn routes(state: AppState) -> Router<AppState> {
    let schema = Arc::new(Schema::build(
        QueryRoot,
        MutationRoot,
        EmptySubscription,
    )
    .finish());
    Router::new()
        .route("/graphql", post(v1::graphql))
        .layer(Extension(schema))
        .with_state(state)
}
