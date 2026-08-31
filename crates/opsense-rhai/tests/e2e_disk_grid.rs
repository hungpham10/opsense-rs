//! End-to-end: HttpSource thật (Prometheus demo) -> RhaiTransform(disk_grid_report)
//! để bắt warn message khi disk-grid kẹt trong pipeline thật.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use opsense_components::vector::runtime::{Component, Runtime};
use opsense_components::{
    new_station_registry, ClockSource, CollectorSink, HttpSource, OpsenseContext, StationKind,
};
use opsense_core::collector::Collector;
use opsense_components::FieldSpec;
use opsense_libs::cast::CastType;
use opsense_core::{Watermarks};
use opsense_rhai::RhaiTransform;

#[tokio::test]
async fn e2e_disk_grid() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    let collector = Arc::new(Collector::new(vec![]));
    let ctx = Arc::new(OpsenseContext::new(
        collector,
        Watermarks::new(),
        Arc::new(std::collections::BTreeMap::new()),
        new_station_registry(),
    ));

    let mut src = HttpSource::new("disk-usage", &["clock"], "https://prometheus.demo.prometheus.io/api/v1/query_range");
    src.initial_lookback_secs = 900;
    src.timeout_secs = 30;
    src.station = true;
    src.station_kind = StationKind::Timeseries;
    src.items = "data.result[].values[]".into();
    let mut params = BTreeMap::new();
    params.insert("query".to_string(), "100 * (1 - node_filesystem_avail_bytes / node_filesystem_size_bytes)".to_string());
    params.insert("start".to_string(), "{{from_ts}}".to_string());
    params.insert("end".to_string(), "{{to_ts}}".to_string());
    params.insert("step".to_string(), "60".to_string());
    src.params = params;
    src.fields.insert("ts".into(), FieldSpec { query: "0".into(), cast_to: Some(CastType::I64) });
    src.fields.insert("value".into(), FieldSpec { query: "1".into(), cast_to: Some(CastType::F64) });
    src.fields.insert("labels".into(), FieldSpec { query: "^.^.metric".into(), cast_to: None });
    src.fields.insert("metric_id".into(), FieldSpec { query: "^.^.metric.mountpoint".into(), cast_to: None });

    let script_path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/disk_grid_report.rhai").to_string();
    let grid = RhaiTransform::new_file("disk-grid", &["disk-usage"], &script_path);

    let mut drain = CollectorSink::new();
    drain.id = "drain".into();
    drain.inputs = vec!["disk-grid".into()];

    let mut runtime = Runtime::new();
    runtime.set_context(ctx.clone());
    let components: Vec<Arc<dyn Component>> = vec![
        Arc::new(ClockSource::new(Duration::from_millis(300))),
        Arc::new(src),
        Arc::new(grid),
        Arc::new(drain),
    ];
    runtime.reload(components).expect("reload");
    let _handle = runtime.start(|_event: Event| async {}).expect("start");
    tokio::time::sleep(Duration::from_secs(10)).await;
    runtime.stop().expect("stop");
    let _ = runtime.wait_for_shutdown().await;

    use opsense_components::vector::runtime::Event;
    let now = opsense_components::signal::now_secs();
    use opsense_core::Context as _;
    eprintln!("station ids: {:?}", opsense_core::registry::station_ids_snapshot());
    if let Some(st) = opsense_core::registry::station("disk-usage").await {
        let g = st.read().await;
        eprintln!("disk-usage station describe: {:?}", g.describe());
    } else {
        eprintln!("disk-usage station CHƯA được register!");
    }

    // 1) batch từ station disk-usage có dữ liệu?
    let up = ctx.read_window(&["disk-usage".to_string()], Some("disk-usage"), 0, now, None).await;
    eprintln!("disk-usage window: {} obs", up.len());
    assert!(!up.is_empty(), "disk-usage phải có dữ liệu");

    // 2) chạy script + deserialize như process_window
    let input_json = serde_json::to_value(&up).unwrap();
    let items = opsense_rhai::call_process_with(
        opsense_rhai::ScriptSource::File(std::path::PathBuf::from(&script_path)),
        input_json,
        Default::default(),
        Default::default(),
    )
    .await
    .expect("script chạy ok");
    eprintln!("script out items: {}", items.len());
    for item in &items {
        match serde_json::from_value::<opsense_model::Observation>(item.clone()) {
            Ok(o) => eprintln!("ok: {} v={}", o.metric_id, o.value),
            Err(e) => eprintln!("DESER ERR: {e} item={item}"),
        }
    }
}
