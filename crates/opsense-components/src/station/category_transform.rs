//! `CategoryStationTransform` — trạm đứng giữa pipeline: forward signal gốc
//! cho downstream (như mọi transform), đồng thời index cặp key/value từ cửa
//! sổ window vào `Station::Category(Search<u8>)` đăng ký trong
//! `OpsenseContext.stations` (substring search trên key).
//!
//! Key lấy từ label `key_field` (default `"metric_id"`), fallback về trường
//! `metric_id`. Value lấy từ label `value_field` (default `"value"`), fallback
//! về trường `value`. (Mapping key/value → record: tạm chỉ feed key vào
//! `Search`; lưu value vào `Search` là follow-up theo plan.)
//!
//! Component được đăng ký thủ công (thay vì qua `#[transform]`) vì proc-macro
//! sinh `#[derive]` với span khiến `default = "default_key_field"` không resolve
//! được; viết rõ ràng giúp `Deserialize` derive hoạt động đáng tin cậy.

use std::any::Any;
use std::io::Error;
use std::sync::Arc;

use opsense_core::registry;
use opsense_core::station::Station;

use crate::signal;
use crate::vector::runtime::{Component, ComponentType, Identify, Message, Outbound};
use crate::OpsenseContext;
use opsense_core::Context;
use tokio::sync::mpsc;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CategoryStationTransform {
    pub id: String,
    pub inputs: Vec<String>,

    /// Label chứa key; default `"metric_id"`. Fallback về trường `metric_id`.
    #[serde(default = "default_key_field")]
    pub key_field: String,

    /// Label chứa value; default `"value"`. Fallback về trường `value`.
    #[serde(default = "default_value_field")]
    pub value_field: String,
}

impl CategoryStationTransform {
    #[must_use]
    pub fn new(id: &str, inputs: &[&str]) -> Self {
        Self {
            id: id.to_string(),
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
            key_field: default_key_field(),
            value_field: default_value_field(),
        }
    }
}

fn default_key_field() -> String {
    "metric_id".to_string()
}

fn default_value_field() -> String {
    "value".to_string()
}

impl Identify for CategoryStationTransform {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_inputs(&self) -> Option<&Vec<String>> {
        Some(&self.inputs)
    }

    fn component_type(&self) -> ComponentType {
        ComponentType::Transform
    }

    fn compare(&self, other: &dyn Component) -> bool {
        if let Some(other_concrete) = other.as_any().downcast_ref::<Self>() {
            self == other_concrete
        } else {
            false
        }
    }

    fn clone_arc(&self) -> Arc<dyn Component> {
        Arc::new(self.clone())
    }
}

#[typetag::serde(name = "category_station_transform")]
#[async_trait::async_trait]
impl Component for CategoryStationTransform {
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let ctx = tx
            .ctx
            .as_ref()
            .and_then(|c| c.as_any().downcast_ref::<OpsenseContext>())
            .ok_or_else(|| Error::other("OpsenseContext not injected into Runtime"))?;

        // Đảm bảo station tồn tại (first-wins). `Search` đã là async (`Send`),
        // nên gọi trực tiếp `.await` trong task tokio.
        {
            let st = registry::ensure_search(&self.id).await;
            let mut g = st.write().await;
            if let Station::Category(_search) = &mut *g {
                // Khởi tạo rỗng; entries được feed trong loop bên dưới.
            }
        }

        while let Some(msg) = rx.recv().await {
            // Forward trước tiên: downstream không bao giờ bị trễ vì trạm.
            for stream in &tx.streams {
                let _ = stream.send(msg.clone()).await;
            }

            let event = signal::event(&msg);
            if event != Some(signal::DATA_READY) && event != Some(signal::PROCESSED) {
                continue;
            }
            let Some(ts) = signal::ts(&msg) else {
                continue;
            };

            let from = ctx.get_node_watermark(&self.id);
            if ts <= from {
                continue;
            }

            let batch = ctx
                .read_window(&self.inputs, signal::src(&msg), from, ts, None)
                .await;
            for obs in &batch {
                let key = obs
                    .labels
                    .get(&self.key_field)
                    .cloned()
                    .unwrap_or_else(|| obs.metric_id.clone());
                let value = obs
                    .labels
                    .get(&self.value_field)
                    .cloned()
                    .unwrap_or_else(|| obs.value.to_string());

                // Index qua wrapper `CategoryStation` (không đụng `Search`
                // bên trong): wrapper giữ bookkeeping `entries` (record idx →
                // key/value) mà `search_entries` của HTTP/MCP/Rhai dùng để
                // trả kết quả; insert trực tiếp vào `Search` sẽ khiến search
                // hit record-id nào không có mapping nào.
                let st = registry::ensure_search(&self.id).await;
                let mut g = st.write().await;
                g.insert_entry(key.as_bytes(), &value).await;
            }
            ctx.set_node_watermark(&self.id, ts);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use opsense_core::collector::Collector;
    use opsense_core::registry;
    use opsense_core::station::Station;
    use opsense_core::{Stage, Watermarks};
    use opsense_model::{Observation, Signal, TelemetryKind};

    use crate::vector::runtime::Event;

    /// Mỗi test dùng node id riêng vì `registry::REGISTRY` là process-global
    /// và first-wins — id trùng sẽ nhìn thấy station của test chạy trước.
    fn unique_id(base: &str) -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        format!("{base}-{}", N.fetch_add(1, Ordering::Relaxed))
    }

    async fn make_ctx() -> Arc<OpsenseContext> {
        Arc::new(OpsenseContext::new(
            Arc::new(Collector::new(vec![])),
            Watermarks::new(),
            Arc::new(BTreeMap::new()),
            OpsenseContext::new_stations(),
        ))
    }

    /// Đăng ký station timeseries `src_id` (vai node upstream) với dữ liệu
    /// metric `up` — mô phỏng những gì `http_source` ghi vào trạm của nó.
    async fn seed_upstream(src_id: &str, obs: Vec<Observation>) {
        let st = Arc::new(tokio::sync::RwLock::new(Station::timeseries(1024)));
        st.write().await.append(Stage::Processed, &obs).await;
        registry::register_station(src_id, st).await;
    }

    async fn drive(
        transform: CategoryStationTransform,
        ctx: Arc<OpsenseContext>,
        msgs: Vec<Message>,
    ) {
        let (_in_tx, mut in_rx) = mpsc::channel::<Message>(8);
        let (evt_tx, _evt_rx) = mpsc::channel::<Event>(8);
        let outbound = Outbound {
            streams: vec![],
            broadcast: None,
            event: evt_tx,
            ctx: Some(ctx),
        };
        let handle = tokio::spawn(async move { transform.run(0, &mut in_rx, outbound).await });
        // Channel bị drop ngay khi task chạy → gửi từ thread riêng.
        let sender = tokio::spawn(async move {
            for m in msgs {
                _ = _in_tx.send(m).await;
            }
        });
        sender.await.unwrap();
        handle.await.unwrap().unwrap();
    }

    fn up_obs(ts: i64, value: f64) -> Observation {
        Observation::new(ts, "up".into(), TelemetryKind::Metric, Signal::Raw, value)
    }

    #[tokio::test]
    async fn indexes_window_keys_into_category_station() {
        let src_id = unique_id("prom-src");
        let cat_id = unique_id("cat");
        let ctx = make_ctx().await;
        seed_upstream(
            &src_id,
            vec![up_obs(1_000, 1.0).with_label("source", "demo")],
        )
        .await;

        drive(
            CategoryStationTransform::new(&cat_id, &[&src_id]),
            ctx,
            vec![signal::tagged(signal::data_ready(2_000), &src_id)],
        )
        .await;

        let st = registry::station(&cat_id).await.expect("category station");
        let entries = st.read().await.search_entries("u", None).await;
        assert_eq!(
            entries,
            vec![("up".to_string(), "1".to_string())],
            "key `up` phải tìm thấy qua substring `u` kèm value từ observation"
        );
    }

    #[tokio::test]
    async fn prefers_value_label_over_raw_value() {
        let src_id = unique_id("prom-src");
        let cat_id = unique_id("cat");
        let ctx = make_ctx().await;
        seed_upstream(&src_id, vec![up_obs(1_000, 1.0).with_label("value", "OK")]).await;

        drive(
            CategoryStationTransform::new(&cat_id, &[&src_id]),
            ctx,
            vec![signal::tagged(signal::data_ready(2_000), &src_id)],
        )
        .await;

        let st = registry::station(&cat_id).await.unwrap();
        assert_eq!(
            st.read().await.search_entries("up", None).await,
            vec![("up".to_string(), "OK".to_string())],
        );
    }

    #[tokio::test]
    async fn duplicate_keys_are_deduplicated_in_search_results() {
        let src_id = unique_id("prom-src");
        let cat_id = unique_id("cat");
        let ctx = make_ctx().await;
        seed_upstream(
            &src_id,
            vec![up_obs(1_000, 1.0), up_obs(1_060, 1.0)],
        )
        .await;

        drive(
            CategoryStationTransform::new(&cat_id, &[&src_id]),
            ctx,
            vec![signal::tagged(signal::data_ready(2_000), &src_id)],
        )
        .await;

        let st = registry::station(&cat_id).await.unwrap();
        assert_eq!(
            st.read().await.search_entries("up", None).await.len(),
            1,
            "cùng key ở nhiều điểm dữ liệu vẫn chỉ index một entry"
        );
    }

    #[tokio::test]
    async fn ignores_tick_and_stale_signals() {
        let src_id = unique_id("prom-src");
        let cat_id = unique_id("cat");
        let ctx = make_ctx().await;
        seed_upstream(&src_id, vec![up_obs(1_000, 1.0)]).await;

        // `tick` không phải data_ready/processed → bị bỏ qua, không index gì.
        drive(
            CategoryStationTransform::new(&cat_id, &[&src_id]),
            ctx.clone(),
            vec![signal::tick(1_500)],
        )
        .await;
        let st = registry::station(&cat_id).await.unwrap();
        assert!(
            st.read().await.search_entries("up", None).await.is_empty(),
            "tick không được index gì"
        );

        // Sau tick, data_ready vẫn index bình thường.
        drive(
            CategoryStationTransform::new(&cat_id, &[&src_id]),
            ctx,
            vec![signal::tagged(signal::data_ready(2_000), &src_id)],
        )
        .await;
        let st = registry::station(&cat_id).await.unwrap();
        assert_eq!(st.read().await.search_entries("up", None).await.len(), 1);
    }

    #[tokio::test]
    async fn uses_custom_key_field_label_when_present() {
        let src_id = unique_id("prom-src");
        let cat_id = unique_id("cat");
        let ctx = make_ctx().await;
        seed_upstream(
            &src_id,
            vec![up_obs(1_000, 1.0).with_label("name", "service.up")],
        )
        .await;

        let mut t = CategoryStationTransform::new(&cat_id, &[&src_id]);
        t.key_field = "name".into();
        drive(
            t,
            ctx,
            vec![signal::tagged(signal::data_ready(2_000), &src_id)],
        )
        .await;

        let st = registry::station(&cat_id).await.unwrap();
        assert_eq!(
            st.read().await.search_entries("service", None).await,
            vec![("service.up".to_string(), "1".to_string())],
        );
    }

    #[tokio::test]
    async fn no_upstream_data_means_no_entries() {
        let src_id = unique_id("prom-src");
        let cat_id = unique_id("cat");
        let ctx = make_ctx().await;
        seed_upstream(&src_id, vec![]).await;

        drive(
            CategoryStationTransform::new(&cat_id, &[&src_id]),
            ctx,
            vec![signal::tagged(signal::data_ready(2_000), &src_id)],
        )
        .await;

        let st = registry::station(&cat_id).await.unwrap();
        assert!(st.read().await.search_entries("up", None).await.is_empty());
    }
}
