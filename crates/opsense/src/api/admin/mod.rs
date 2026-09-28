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

// =========================================================================
// Cổng kiểm tra "chỉ chính mình" (self-only)
// =========================================================================

/// Role được phép vượt qua cổng self-only.
///
/// **Để trống = không ai vượt qua.** Hiện chưa có hạ tầng role nào ở tầng app:
/// `grep -rn "X-User-Role" crates/` rỗng, và claim `role` không tồn tại trong
/// `id_token` của Dex (`conf/dex/config.dev.yaml:37-39` chỉ khai
/// `scopeMappings: [openid, email, profile]`). Nên tạm để rỗng và coi mọi role
/// là bình thường.
///
/// Khi cần admin thao tác token của user khác: điền role ở đây, và định nghĩa
/// nó ở provider (Dex `scopeMappings`/custom claim, hoặc Auth0). Khi đó
/// Nginx phải set `X-User-Role` — nó **đã** set ở `04-api.conf:203` từ
/// `jwt_obj.payload.role`, nên chỉ thiếu phía provider phát claim.
pub const ADMIN_ROLE: &str = "";

/// Chặn truy cập vào tài nguyên của user khác.
///
/// Cổng này **có thật trong app**, không phải ở Nginx. Lý do: Nginx chỉ verify
/// token rồi chuyển tiếp, danh tính người gọi đến app là *một header không ai
/// kiểm chứng* (`X-User-Id`, do Nginx set từ claim `sub`). Không kiểm ở đây thì
/// bất kỳ user đăng nhập hợp lệ nào cũng thao tác được token của user khác.
///
/// Áp cho: `GET/DELETE /tokens/users/{user_id}`, `POST /tokens/users`.
///
/// # Đo được trước khi viết
///
/// Tạo user giả trong DB rồi gọi bằng token của `dev-user`:
///
/// ```text
/// GET    /api/admin/v1/tokens/users/VICTIM-user-id  → 200 + plaintext token
/// DELETE /api/admin/v1/tokens/users/VICTIM-user-id  → 200 qua UDS ⇒ revoked_at set
/// ```
///
/// `reveal_user_token` và `revoke_user_token` lấy `user_id` từ **URL** mà
/// `AdminHeaders` chỉ có `tenant_id` + `host` — không có gì để so sánh, nên
/// "chỉ thao tác token của chính mình" chưa từng được thực thi.
pub fn require_self_or_admin(caller_id: &str, target_id: &str) -> Result<(), AdminForbidden> {
    if caller_id == target_id {
        return Ok(());
    }
    // Chỗ dành cho admin: so role của người gọi với `ADMIN_ROLE`. Cố ý chưa viết
    // — không có nguồn role nào đáng tin ở tầng app, và viết một nhánh so sánh
    // rỗng rồi tưởng đã bảo mật thì tệ hơn là chưa có.
    Err(AdminForbidden::NotSelf {
        caller: caller_id.to_string(),
        target: target_id.to_string(),
    })
}

/// 403 khi người gọi không phải chủ tài nguyên.
#[derive(Debug)]
pub enum AdminForbidden {
    NotSelf { caller: String, target: String },
}

impl axum::response::IntoResponse for AdminForbidden {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        use axum::response::Json;
        let (msg, err) = match self {
            AdminForbidden::NotSelf { caller, target } => (
                format!("caller {caller} may not act on {target}"),
                "forbidden",
            ),
        };
        (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": err, "message": msg })),
        )
            .into_response()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    /// Hàm thuần, không cần DB ⇒ test được trực tiếp.
    ///
    /// Cặp test này là **hồi quy** cho lỗ hổng đã đo: tạo user giả trong DB rồi
    /// gọi bằng token `dev-user` thì `GET /tokens/users/{id}` trả 200 kèm
    /// plaintext token của user đó. Không có cổng này thì lặp lại đúng như vậy.
    #[test]
    fn self_is_allowed() {
        let me = "CghkZXYtdXNlchIFbG9jYWw";
        assert!(require_self_or_admin(me, me).is_ok());
    }

    #[test]
    fn other_user_is_forbidden() {
        let err = require_self_or_admin("CghkZXYtdXNlchIFbG9jYWw", "VICTIM-user-id");
        assert!(
            err.is_err(),
            "user khác phải bị chặn — nếu test này đỏ thì cổng self-only đã bị gỡ"
        );
    }

    /// `ADMIN_ROLE` phải **rỗng** cho tới khi có provider phát claim `role`.
    /// Điền sớm mà chưa có nguồn role ⇒ nhánh so sánh là đồ trang, tệ hơn là
    /// không có. Test này ghim lại để đổi hằng số phải cố ý.
    #[test]
    fn admin_role_is_empty_until_provider_supplies_it() {
        assert!(
            ADMIN_ROLE.is_empty(),
            "chỉ điền ADMIN_ROLE khi provider thật sự phát claim `role`              (Dex scopeMappings / Auth0), nếu không thì nhánh admin là đồ trang"
        );
    }
}
