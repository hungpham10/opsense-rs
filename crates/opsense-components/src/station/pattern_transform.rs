//! `PatternStationTransform` — trạm đứng giữa pipeline: forward signal gốc
//! cho downstream (như mọi transform), đồng thời feed text từ cửa sổ window
//! vào `Station::Pattern(AhoCorasick)` đăng ký trong `OpsenseContext.stations`
//! để đếm hit/miss (matching substring).
//!
//! Pattern được đăng ký trước (config `patterns`, idempotent) hoặc sau (Rhai
//! `pattern_add` / API). Mỗi observation đóng góp một lượt match: text lấy từ
//! label `text_field` (default `"log"`), fallback về trường `metric_id`.
//!
//! Component được đăng ký thủ công (thay vì qua `#[transform]`) vì proc-macro
//! sinh `#[derive]` với span khiến `default = "default_text_field"` không resolve
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
pub struct PatternStationTransform {
    pub id: String,
    pub inputs: Vec<String>,
    /// Label chứa text cần match; default `"log"`. Fallback về `metric_id`.
    #[serde(default = "default_text_field")]
    pub text_field: String,
    /// Pattern đăng ký sẵn khi node khởi động (idempotent theo giá trị).
    #[serde(default)]
    pub patterns: Vec<String>,
}

impl PatternStationTransform {
    #[must_use]
    pub fn new(id: &str, inputs: &[&str]) -> Self {
        Self {
            id: id.to_string(),
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
            text_field: default_text_field(),
            patterns: Vec::new(),
        }
    }
}

fn default_text_field() -> String {
    "log".to_string()
}

impl Identify for PatternStationTransform {
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

#[typetag::serde(name = "pattern_station_transform")]
#[async_trait::async_trait]
impl Component for PatternStationTransform {
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

        // Đăng ký station (first-wins) + register pattern sẵn (idempotent)
        // trước khi xử lý window. `AhoCorasick` đã là async (`Send`),
        // nên gọi trực tiếp `.await` trong task tokio.
        {
            let st = registry::ensure_pattern(&self.id).await;
            let mut g = st.write().await;
            if let Station::Pattern(auto) = &mut *g {
                for pattern in &self.patterns {
                    auto.add(pattern.clone());
                }
                auto.optimize().await;
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
                let text = obs
                    .labels
                    .get(&self.text_field)
                    .cloned()
                    .unwrap_or_else(|| obs.metric_id.clone());
                // `similar` vừa update hit/miss counter, vừa trả về kết quả
                // khớp — ta chỉ cần side-effect đếm, nên bỏ qua giá trị trả về.
                let st = registry::ensure_pattern(&self.id).await;
                let g = st.read().await;
                if let Station::Pattern(auto) = &*g {
                    let _ = auto.similar(&text).await;
                }
            }
            ctx.set_node_watermark(&self.id, ts);
        }
        Ok(())
    }
}
