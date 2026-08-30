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

        // Record index tăng dần cho mỗi key được feed (duplicate key trả
        // `Err(Duplicated)` — bỏ qua, key đã có trong index).
        let mut next_idx: usize = 1;

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
                let _value = obs
                    .labels
                    .get(&self.value_field)
                    .cloned()
                    .unwrap_or_else(|| obs.value.to_string());

                let metas: Vec<Option<&[u8]>> = vec![None; key.len()];

                let st = registry::ensure_search(&self.id).await;
                let mut g = st.write().await;
                if let Station::Category(search) = &mut *g {
                    let _ = search.insert_chain(next_idx, key.as_bytes(), &metas).await;
                }
                next_idx += 1;
            }
            ctx.set_node_watermark(&self.id, ts);
        }
        Ok(())
    }
}
