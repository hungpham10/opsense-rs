//! Impl [`DbBackend`] trên `sqlx::AnyPool` — transport DB cho module động.
//!
//! Module gửi SQL text (`$1..$n`) + params JSON; ở đây bind thành prepared
//! statement của đúng dialect, chạy, rồi đọc row về JSON.
//!
//! **Vì sao dùng `try_get_raw` thay `try_get`:** `AnyRow::try_get` kiểm tra
//! `T::compatible(&type_info)` trước khi decode, và `f64` không "compatible"
//! với `REAL` — trong khi ta biết chắc mình muốn gì. `try_get_raw` +
//! `Decode::decode` bỏ qua bước kiểm tra đó.
//!
//! **Giới hạn `sqlx::Any`:** chỉ 9 kiểu được map (xem `opsense_ffi::db`).
//! Cột kiểu `TIMESTAMPTZ`/`NUMERIC`/`JSON`/`UUID` (Postgres) hay `BOOL`
//! (SQLite) làm hỏng cả row ngay tại `AnyRow::map_from`, trước cả khi tới đây.

use std::sync::Arc;

use base64::Engine as _;
use opsense_ffi::cache::BoxFuture;
use opsense_ffi::db::{DbBackend, DbError};
use opsense_model::resolver::DbKind;
use serde_json::{Map, Value};
use sqlx::{Any, AnyPool, Column, Decode, Row, TypeInfo, ValueRef};

/// `DbBackend` dùng chung một `AnyPool` của tenant.
pub struct AnyDbBackend {
    pool: AnyPool,
    kind: DbKind,
}

impl AnyDbBackend {
    pub fn new(pool: AnyPool, kind: DbKind) -> Self {
        Self { pool, kind }
    }

    /// Arc cho `TenantServices`. Giữ luôn `AnyDbBackend` để host có thể downcast.
    pub fn arc(pool: AnyPool, kind: DbKind) -> Arc<dyn DbBackend> {
        Arc::new(Self::new(pool, kind))
    }
}

/// JSON array → BLOB, chỉ chấp nhận byte 0..=255.
fn bytes_from_array(values: &[Value]) -> Result<Vec<u8>, DbError> {
    values
        .iter()
        .map(|v| {
            v.as_u64()
                .filter(|n| *n <= u8::MAX as u64)
                .map(|n| n as u8)
                .ok_or_else(|| DbError::Query(format!("BLOB chỉ nhận mảng byte 0..=255, gặp {v}")))
        })
        .collect()
}

/// Một row → JSON object (key = tên cột). BLOB encode base64.
fn row_to_json(row: &sqlx::any::AnyRow) -> Result<Value, DbError> {
    let mut map = Map::new();

    for col in row.columns() {
        let raw = row
            .try_get_raw(col.ordinal())
            .map_err(|e| DbError::Query(e.to_string()))?;

        let value = if raw.is_null() {
            Value::Null
        } else {
            // `type_info().name()` đã được host chuẩn hoá về 9 tên canonical.
            match raw.type_info().name() {
                "BOOLEAN" => Value::Bool(
                    <bool as Decode<Any>>::decode(raw)
                        .map_err(|e| DbError::Query(e.to_string()))?,
                ),
                "SMALLINT" => Value::from(
                    <i16 as Decode<Any>>::decode(raw).map_err(|e| DbError::Query(e.to_string()))?,
                ),
                "INTEGER" => Value::from(
                    <i32 as Decode<Any>>::decode(raw).map_err(|e| DbError::Query(e.to_string()))?,
                ),
                "BIGINT" => Value::from(
                    <i64 as Decode<Any>>::decode(raw).map_err(|e| DbError::Query(e.to_string()))?,
                ),
                "REAL" => Value::from(
                    <f32 as Decode<Any>>::decode(raw).map_err(|e| DbError::Query(e.to_string()))?,
                ),
                "DOUBLE" => Value::from(
                    <f64 as Decode<Any>>::decode(raw).map_err(|e| DbError::Query(e.to_string()))?,
                ),
                "TEXT" => Value::String(
                    <String as Decode<Any>>::decode(raw)
                        .map_err(|e| DbError::Query(e.to_string()))?,
                ),
                "BLOB" => {
                    let bytes = <Vec<u8> as Decode<Any>>::decode(raw)
                        .map_err(|e| DbError::Query(e.to_string()))?;
                    Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
                }
                other => {
                    return Err(DbError::Query(format!(
                        "cột `{}` có kiểu `{other}` không hỗ trợ",
                        col.name()
                    )));
                }
            }
        };

        map.insert(col.name().to_string(), value);
    }

    Ok(Value::Object(map))
}

impl DbBackend for AnyDbBackend {
    fn query(&self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<Vec<Value>, DbError>> {
        let pool = self.pool.clone();
        let kind = self.kind;
        let sql = sql.to_string();

        Box::pin(async move {
            let (sql, params) = match kind {
                DbKind::MySql => opsense_ffi::db::rewrite_placeholders_mysql(&sql, &params)?,
                _ => (sql, params),
            };

            let mut q = sqlx::query::<Any>(&sql);
            for p in &params {
                q = bind_value(q, p)?;
            }

            let rows = q
                .fetch_all(&pool)
                .await
                .map_err(|e| DbError::Backend(e.to_string()))?;

            rows.iter().map(row_to_json).collect()
        })
    }

    fn execute(&self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<u64, DbError>> {
        let pool = self.pool.clone();
        let kind = self.kind;
        let sql = sql.to_string();

        Box::pin(async move {
            let (sql, params) = match kind {
                DbKind::MySql => opsense_ffi::db::rewrite_placeholders_mysql(&sql, &params)?,
                _ => (sql, params),
            };

            let mut q = sqlx::query::<Any>(&sql);
            for p in &params {
                q = bind_value(q, p)?;
            }

            let result = q
                .execute(&pool)
                .await
                .map_err(|e| DbError::Backend(e.to_string()))?;

            Ok(result.rows_affected())
        })
    }
}

/// Bind một param JSON vào query, giữ nguyên kiểu `Query` để reassign.
fn bind_value<'q>(
    q: sqlx::query::Query<'q, Any, sqlx::any::AnyArguments<'q>>,
    value: &Value,
) -> Result<sqlx::query::Query<'q, Any, sqlx::any::AnyArguments<'q>>, DbError> {
    Ok(match value {
        // `Option<T>::None` mang theo kind của `T`; chọn TEXT vì nó là kiểu
        // "trung tính" nhất. NULL cho cột số nên viết `CAST($n AS BIGINT)`.
        Value::Null => q.bind(Option::<String>::None),
        Value::Bool(b) => q.bind(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                q.bind(i)
            } else if let Some(f) = n.as_f64() {
                q.bind(f)
            } else {
                return Err(DbError::Query(format!("số không hợp lệ: {n}")));
            }
        }
        Value::String(s) => q.bind(s.clone()),
        Value::Array(a) => q.bind(bytes_from_array(a)?),
        // Object → TEXT (JSON đã serialize). Cột JSON của Postgres không đọc
        // lại được qua `Any`, nên chỉ dùng khi cột là TEXT.
        Value::Object(_) => q.bind(value.to_string()),
    })
}
