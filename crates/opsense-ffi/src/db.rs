//! Truy vấn DB qua FFI — module viết SQL, host làm transport.
//!
//! Contract này **không** kéo `sqlx` vào crate SDK (mọi module `.so` sẽ phình
//! theo). Host nhận SQL text + params JSON, chạy prepared statement trên pool
//! của tenant, trả rows JSON.
//!
//! Quy ước:
//! - Placeholder: `$1..$n` (Postgres-native). Host tự đổi sang `?` cho MySQL.
//! - Param JSON → kiểu SQL: `null` → NULL, bool → BOOLEAN, số nguyên → BIGINT,
//!   số thực → DOUBLE, string → TEXT, mảng số (0..=255) → BLOB, object → TEXT
//!   (JSON đã serialize).
//! - Row → `serde_json::Value::Object`, key là tên cột. `BLOB` được encode
//!   base64 (JSON không có kiểu nhị phân).
//!
//! **Giới hạn của `sqlx::Any`** (host dùng `AnyPool`): chỉ 9 kiểu được map —
//! BOOLEAN, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, TEXT, BLOB, NULL. Cột
//! `TIMESTAMPTZ`/`NUMERIC`/`JSON`/`UUID` (Postgres) hoặc `BOOL` (SQLite) sẽ
//! làm **hỏng cả row**. Module tự viết SQL nên hãy chủ động `CAST(col AS TEXT)`
//! khi đọc, và tạo bảng bằng các kiểu nằm trong danh sách trên.

use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::cache::BoxFuture;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbError {
    /// Lỗi từ database: cú pháp SQL, vi phạm constraint, mất kết nối…
    Backend(String),
    /// Kết nối được nhưng không đọc nổi kết quả (kiểu cột không hỗ trợ,
    /// JSON không map được sang struct…).
    Query(String),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::Backend(e) => write!(f, "db backend error: {e}"),
            DbError::Query(e) => write!(f, "db query error: {e}"),
        }
    }
}

impl std::error::Error for DbError {}

/// Nguồn DB thật đằng sau [`Db`]. Host dựng per-tenant từ `Resolver`.
///
/// `fn` trả `BoxFuture` (không `async fn`) vì trait phải dyn-compatible.
pub trait DbBackend: Send + Sync + 'static {
    /// `SELECT` → danh sách row, mỗi row là một JSON object.
    fn query(&self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<Vec<Value>, DbError>>;

    /// `INSERT`/`UPDATE`/`DELETE` → số row bị ảnh hưởng.
    fn execute(&self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<u64, DbError>>;
}

/// Wrapper tiện dụng quanh [`DbBackend`] — thêm giải mã row → struct.
pub struct Db {
    backend: Arc<dyn DbBackend>,
}

impl Db {
    pub fn new(backend: Arc<dyn DbBackend>) -> Self {
        Self { backend }
    }

    /// Trả rows thô dưới dạng JSON.
    pub async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Value>, DbError> {
        self.backend.query(sql, params.to_vec()).await
    }

    /// Trả rows đã giải mã thẳng sang struct của module.
    pub async fn query_as<T: DeserializeOwned>(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<T>, DbError> {
        let rows = self.backend.query(sql, params.to_vec()).await?;
        rows.into_iter()
            .map(|row| serde_json::from_value(row).map_err(|e| DbError::Query(e.to_string())))
            .collect()
    }

    /// Như [`Db::query_as`] nhưng lấy row đầu tiên.
    pub async fn query_one<T: DeserializeOwned>(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Option<T>, DbError> {
        let mut rows = self.query_as(sql, params).await?;
        Ok(if rows.is_empty() {
            None
        } else {
            Some(rows.swap_remove(0))
        })
    }

    /// `INSERT`/`UPDATE`/`DELETE` → số row bị ảnh hưởng.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        self.backend.execute(sql, params.to_vec()).await
    }
}

/// Đổi `$1..$n` → `?` cho MySQL và xếp lại params theo đúng thứ tự xuất hiện.
///
/// MySQL không có placeholder đánh số, còn `?` bind theo vị trí. Nếu một
/// placeholder được dùng lại (`$1 … $1`) thì param tương ứng được nhân bản.
///
/// Trả `(sql_mới, params_mới)`. Bỏ qua mọi ký tự nằm trong `'…'`, `"…"` hoặc
/// `` `…` ``. Param không được tham chiếu sẽ bị bỏ đi.
pub fn rewrite_placeholders_mysql(
    sql: &str,
    params: &[Value],
) -> Result<(String, Vec<Value>), DbError> {
    let mut out = String::with_capacity(sql.len());
    let mut ordered = Vec::with_capacity(params.len());
    let mut quote: Option<char> = None;
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        match quote {
            Some(q) => {
                out.push(c);
                if c == q {
                    // `''` / `""` escape bằng cách lặp dấu nháy.
                    if chars.peek() == Some(&q) {
                        out.extend(chars.next());
                    } else {
                        quote = None;
                    }
                }
            }
            None => match c {
                '\'' | '"' | '`' => {
                    quote = Some(c);
                    out.push(c);
                }
                '$' => {
                    let mut digits = String::new();
                    while let Some(d) = chars.peek().copied().filter(|d| d.is_ascii_digit()) {
                        digits.push(d);
                        chars.next();
                    }
                    if digits.is_empty() {
                        out.push('$');
                    } else {
                        let idx: usize = digits.parse().map_err(|_| {
                            DbError::Query(format!("placeholder ${digits} không hợp lệ"))
                        })?;
                        let value = params.get(idx.wrapping_sub(1)).ok_or_else(|| {
                            DbError::Query(format!(
                                "placeholder ${idx} nhưng chỉ có {} param",
                                params.len()
                            ))
                        })?;
                        ordered.push(value.clone());
                        out.push('?');
                    }
                }
                _ => out.push(c),
            },
        }
    }

    Ok((out, ordered))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn mysql_rewrites_positional_placeholders() {
        let sql = "SELECT * FROM t WHERE a = $1 AND b = $2";
        let (out, params) = rewrite_placeholders_mysql(sql, &[json!(1), json!("x")]).unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE a = ? AND b = ?");
        assert_eq!(params, vec![json!(1), json!("x")]);
    }

    #[test]
    fn mysql_duplicates_reused_placeholder() {
        let sql = "SELECT * FROM t WHERE a = $1 OR b = $1";
        let (out, params) = rewrite_placeholders_mysql(sql, &[json!(7)]).unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE a = ? OR b = ?");
        assert_eq!(params, vec![json!(7), json!(7)]);
    }

    #[test]
    fn mysql_ignores_dollar_inside_string_literal() {
        let sql = "SELECT '$1' AS lit, a FROM t WHERE a = $1";
        let (out, params) = rewrite_placeholders_mysql(sql, &[json!(3)]).unwrap();
        assert_eq!(out, "SELECT '$1' AS lit, a FROM t WHERE a = ?");
        assert_eq!(params, vec![json!(3)]);
    }

    #[test]
    fn mysql_rejects_missing_param() {
        assert!(rewrite_placeholders_mysql("SELECT $2", &[json!(1)]).is_err());
    }

    #[test]
    fn mysql_drops_unused_param() {
        let (out, params) = rewrite_placeholders_mysql("SELECT $1", &[json!(1), json!(2)]).unwrap();
        assert_eq!(out, "SELECT ?");
        assert_eq!(params, vec![json!(1)]);
    }

    #[test]
    fn db_error_variants_are_distinct() {
        assert_ne!(
            std::mem::discriminant(&DbError::Backend(String::new())),
            std::mem::discriminant(&DbError::Query(String::new()))
        );
    }
}
