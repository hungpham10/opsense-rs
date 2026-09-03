mod v1;

use axum::Router;
use axum_extra::TypedHeader;
use axum_macros::FromRequestParts;
use headers::{Header, HeaderName, HeaderValue, Host};

use super::{AppState, XTenantId};

pub struct XRequestId(pub i64);

impl Header for XRequestId {
    fn name() -> &'static HeaderName {
        static NAME: HeaderName = HeaderName::from_static("x-request-id");
        &NAME
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
    where
        I: Iterator<Item = &'i HeaderValue>,
    {
        if let Some(value) = values.next() {
            let s = value.to_str().map_err(|_| headers::Error::invalid())?;
            let id = s.parse().map_err(|_| headers::Error::invalid())?;

            Ok(XRequestId(id))
        } else {
            Err(headers::Error::invalid())
        }
    }

    fn encode<E>(&self, values: &mut E)
    where
        E: Extend<HeaderValue>,
    {
        let v = HeaderValue::from_str(&self.0.to_string()).unwrap();
        values.extend(std::iter::once(v));
    }
}

impl From<XRequestId> for i64 {
    fn from(request: XRequestId) -> Self {
        request.0
    }
}

#[derive(FromRequestParts)]
pub struct AdminHeaders {
    #[from_request(via(TypedHeader))]
    pub tenant_id: XTenantId,

    #[from_request(via(TypedHeader))]
    pub host: Host,
}

pub fn routes() -> Router<AppState> {
    Router::new().nest("/v1", v1::routes())
}
