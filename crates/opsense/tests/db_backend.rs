//! `AnyDbBackend` — transport DB cho module động, chạy trên SQLite thật.
//!
//! Dùng DB **file** (không `sqlite::memory:`): pool nhiều connection, mỗi
//! connection `:memory:` riêng biệt nên CREATE TABLE sẽ "biến mất" ở kết nối
//! khác. Chỉ cần SQLite — không cần Postgres/MySQL như suite integration khác.

use opsense::api::modules::AnyDbBackend;
use opsense_ffi::db::{Db, DbBackend, DbError};
use opsense_model::resolver::DbKind;
use serde::Deserialize;
use serde_json::json;
use sqlx::any::AnyPoolOptions;

/// Pool SQLite file dùng chung cho test.
async fn pool(dir: &std::path::Path) -> sqlx::AnyPool {
    sqlx::any::install_default_drivers();

    let file = dir.join("module-db.sqlite");
    let dsn = format!("sqlite://{}?mode=rwc", file.display());
    AnyPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("mở pool SQLite")
}

#[derive(Debug, PartialEq, Deserialize)]
struct Trade {
    id: i64,
    price: f64,
    symbol: String,
}

#[tokio::test]
async fn db_backend_roundtrips_typed_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool(dir.path()).await;
    let backend = AnyDbBackend::new(pool, DbKind::Sqlite);
    let db = Db::new(std::sync::Arc::new(AnyDbBackend::new(
        // pool thứ hai trỏ cùng file — chứng minh dữ liệu persist qua connection.
        pool_again(dir.path()).await,
        DbKind::Sqlite,
    )));

    backend
        .execute(
            "CREATE TABLE trades (id INTEGER, price DOUBLE, symbol TEXT, payload BLOB)",
            vec![],
        )
        .await
        .expect("CREATE TABLE");

    let affected = backend
        .execute(
            "INSERT INTO trades (id, price, symbol, payload) VALUES ($1, $2, $3, $4)",
            vec![json!(1), json!(105.5), json!("BTC"), json!([1, 2, 3])],
        )
        .await
        .expect("INSERT");
    assert_eq!(affected, 1);

    // INSERT thứ hai kèm NULL — cột payload để trống.
    backend
        .execute(
            "INSERT INTO trades (id, price, symbol, payload) VALUES ($1, $2, $3, $4)",
            vec![json!(2), json!(200.25), json!("ETH"), json!(null)],
        )
        .await
        .expect("INSERT NULL");

    // ── query thô: kiểu được giữ đúng, BLOB → base64 ──────────────────────
    let rows = backend
        .query(
            "SELECT id, price, symbol, payload FROM trades ORDER BY id",
            vec![],
        )
        .await
        .expect("SELECT");
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0]["id"], json!(1));
    assert_eq!(rows[0]["price"], json!(105.5));
    assert_eq!(rows[0]["symbol"], json!("BTC"));
    assert_eq!(rows[0]["payload"], json!("AQID")); // [1,2,3] → base64
    assert_eq!(rows[1]["payload"], json!(null));

    // ── query_as: JSON row → struct của module ────────────────────────────
    let trades: Vec<Trade> = db
        .query_as("SELECT id, price, symbol FROM trades ORDER BY id", &[])
        .await
        .expect("query_as");
    assert_eq!(
        trades,
        vec![
            Trade {
                id: 1,
                price: 105.5,
                symbol: "BTC".into()
            },
            Trade {
                id: 2,
                price: 200.25,
                symbol: "ETH".into()
            },
        ]
    );

    // ── query_one + param đánh số $1 ─────────────────────────────────────
    let one: Option<Trade> = db
        .query_one(
            "SELECT id, price, symbol FROM trades WHERE id = $1",
            &[json!(2)],
        )
        .await
        .expect("query_one");
    assert_eq!(
        one,
        Some(Trade {
            id: 2,
            price: 200.25,
            symbol: "ETH".into()
        })
    );

    let none: Option<Trade> = db
        .query_one(
            "SELECT id, price, symbol FROM trades WHERE id = $1",
            &[json!(99)],
        )
        .await
        .expect("query_one miss");
    assert_eq!(none, None);

    // ── execute UPDATE trả rows_affected ─────────────────────────────────
    let updated = db
        .execute(
            "UPDATE trades SET price = $1 WHERE symbol = $2",
            &[json!(999.0), json!("BTC")],
        )
        .await
        .expect("UPDATE");
    assert_eq!(updated, 1);

    // ── lỗi backend (SQL sai) phân biệt được với lỗi query ───────────────
    let err = backend
        .query("SELECT * FROM khong_ton_tai", vec![])
        .await
        .expect_err("bảng không tồn tại phải lỗi");
    assert!(matches!(err, DbError::Backend(_)), "gặp: {err:?}");
}

async fn pool_again(dir: &std::path::Path) -> sqlx::AnyPool {
    let file = dir.join("module-db.sqlite");
    let dsn = format!("sqlite://{}?mode=rwc", file.display());
    AnyPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
        .expect("mở pool SQLite thứ hai")
}
