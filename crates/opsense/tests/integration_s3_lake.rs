//! S3 lakehouse integration test.
//!
//! Verify rằng opsense-mlib ghi parquet time-partitioned (`ts/blk=<id>/batch-*.parquet`)
//! vào RustFS rồi đọc lại đúng. Yêu cầu `docker compose up rustfs rustfs-bucket`.
//! Skip graceful khi RustFS không reachable (dev local không cần stack đầy).
//!
//! Chạy:
//!   docker compose up -d rustfs rustfs-bucket
//!   cargo test --test integration_s3_lake -- --nocapture

mod common;

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use object_store::{aws::AmazonS3Builder, ObjectStore};
use opsense_mlib::storage::parquet::{LakehouseStorage, S3Config};
use opsense_mlib::storage::TimeseriesStorage;

const LAKE_KEY: &str = "integration-test-station";

fn s3_endpoint() -> String {
    std::env::var("OPSENSE_S3_ENDPOINT").unwrap_or_else(|_| "http://rustfs:9000".into())
}
fn s3_user() -> String {
    std::env::var("OPSENSE_S3_ACCESS_KEY_ID").unwrap_or_else(|_| "opsense".into())
}
fn s3_pass() -> String {
    std::env::var("OPSENSE_S3_SECRET_ACCESS_KEY").unwrap_or_else(|_| "opsense123".into())
}

/// Đợi RustFS sẵn sàng (tối đa 30s). Trả `true` nếu ready.
async fn wait_rustfs() -> bool {
    let client = reqwest::Client::new();
    for _ in 0..30 {
        if client.get(format!("{}/health", s3_endpoint()))
            .send()
            .await
            .is_ok()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

fn s3_store() -> Arc<dyn ObjectStore> {
    let cfg = S3Config {
        bucket: "opsense-lake".into(),
        // `prefix` là namespace **chung** của cả lake; station nằm ở segment
        // (`lake_key`) → key thật = `{prefix}/{lake_key}/ts/…`. Đặt prefix =
        // lake_key sẽ lặp đôi (`x/x/ts/…`) và list theo `x/ts/` rỗng.
        prefix: String::new(),
        access_key_id: Some(s3_user()),
        secret_access_key: Some(s3_pass()),
        region: None,
        endpoint: Some(s3_endpoint()),
        session_token: None,
    };
    // Dùng cùng logic với parquet.rs (path-style cho RustFS).
    let mut b = AmazonS3Builder::new().with_bucket_name(&cfg.bucket);
    if let Some(k) = &cfg.access_key_id { b = b.with_access_key_id(k); }
    if let Some(s) = &cfg.secret_access_key { b = b.with_secret_access_key(s); }
    if let Some(r) = &cfg.region { b = b.with_region(r); }
    if let Some(ep) = &cfg.endpoint {
        b = b.with_endpoint(ep)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false);
    }
    Arc::new(b.build().expect("build S3 client"))
}

/// Liệt kê keys trong S3 prefix.
async fn list_keys(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut stream = store.list(Some(&object_store::path::Path::from(prefix)));
    while let Some(result) = stream.next().await {
        match result {
            Ok(obj) => { out.push(obj.location.to_string()); }
            Err(_) => break,
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn s3_lake_write_flush_snapshot_retain() {
    if !wait_rustfs().await {
        eprintln!("skipping: RustFS không reachable — `docker compose up rustfs rustfs-bucket`");
        return;
    }

    let td = tempfile::tempdir().unwrap();
    let local = td.path().to_string_lossy().into_owned();
    let store = s3_store();

    // Mở lake với S3 mirror. flush_threshold=0 → chỉ flush khi gọi tường minh.
    // Layout: `s3://opsense-lake/{lake_key}/ts/blk=<id>/batch-*.parquet`.
    let s3 = S3Config {
        bucket: "opsense-lake".into(),
        prefix: String::new(),
        access_key_id: Some(s3_user()),
        secret_access_key: Some(s3_pass()),
        region: None,
        endpoint: Some(s3_endpoint()),
        session_token: None,
    };
    let lake = LakehouseStorage::open_with_s3(&local, s3, 0usize, 3600, LAKE_KEY.to_string())
        .await
        .expect("open_with_s3");

    // 1. Ghi 2 block.
    lake.append(b"blk:1", 100, b"v1").await.unwrap();
    lake.append(b"blk:2", 3600, b"v2").await.unwrap();

    // 2. Flush → file parquet trên local + S3.
    lake.flush_timeseries().unwrap();

    let ts_keys = list_keys(&store, &format!("{}/ts/", LAKE_KEY)).await;
    assert!(ts_keys.iter().any(|k| k.contains("blk=1")), "phải có blk=1 trên S3: {ts_keys:?}");
    assert!(ts_keys.iter().any(|k| k.contains("blk=2")), "phải có blk=2 trên S3: {ts_keys:?}");
    assert!(ts_keys.iter().any(|k| k.ends_with("manifest.json")), "phải có manifest.json");

    // 3. Đọc lại qua range (từ local + S3 merged).
    let r = lake.range(b"blk:1", 0, 100).await.unwrap();
    assert_eq!(r, vec![(100, b"v1".to_vec())]);
    let r = lake.range(b"blk:2", 3600, 3600).await.unwrap();
    assert_eq!(r, vec![(3600, b"v2".to_vec())]);

    // 4. Snapshot → state files trên S3.
    lake.snapshot().unwrap();
    let state_keys = list_keys(&store, &format!("{}/state/", LAKE_KEY)).await;
    assert!(!state_keys.is_empty(), "phải có state/ trên S3");
    assert!(state_keys.iter().any(|k| k.ends_with("manifest.json")), "state/manifest.json");

    // 5. Retention: blk:1→partition blk=1, blk:2→partition blk=2
    //    (series có prefix blk: → dùng id trong series).
    //    keep_after_ts=7200 → keep_block=2 → giữ blk=2, xoá blk=1.
    lake.retain_block_partitions(7200).unwrap();
    let ts_keys_after = list_keys(&store, &format!("{}/ts/", LAKE_KEY)).await;
    assert!(ts_keys_after.iter().any(|k| k.contains("blk=2")), "blk=2 phải còn sau retain(7200)");
    assert!(
        !ts_keys_after.iter().any(|k| k.contains("blk=1")),
        "blk=1 phải bị xoá sau retain(7200): {ts_keys_after:?}"
    );
}
