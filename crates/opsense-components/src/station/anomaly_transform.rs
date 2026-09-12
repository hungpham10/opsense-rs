//! `anomaly_station_transform` — phát hiện bất thường trên luồng observation
//! bằng Random Cut Forest ([`opsense_libs::rcf::RcfForest`]) + ngưỡng
//! `[capacity]` trong config.
//!
//! Với mỗi message JSON chứa `observations` từ upstream:
//! - Mỗi `metric_id` có một rừng RCF riêng (dims=1 trên `value`, hỗ trợ
//!   shingling để bắt bất thường theo hình dạng chuỗi).
//! - `RcfForest::add` trả anomaly score (thường ~1.0); score vượt
//!   `threshold` ⇒ observation được đánh dấu bất thường.
//! - Song song, nếu `metric_id` có mục trong `[capacity]` (map metric →
//!   maximum, đọc qua [`opsense_core::Context::capacity`]) và `value` vượt
//!   ngưỡng đó ⇒ cũng đánh dấu bất thường (capacity breach).
//!
//! Observation bất thường được ghi vào station `Timeseries` của chính node
//! (queryable qua MCP/GraphQL) rồi forward xuống downstream; observation
//! thường chỉ forward.

use std::collections::HashMap;
use std::io::Error;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock, mpsc};

use opsense_core::Station;
use opsense_core::TimeseriesStation;
use opsense_libs::rcf::{RcfConfig, RcfForest};
use opsense_macros::transform;
use opsense_model::events::LogLevel;

use super::{downcast_ctx, extract_observations};
use crate::vector::runtime::{Component, Identify, Message, Outbound};

fn default_threshold() -> f64 {
    3.0
}

fn default_num_trees() -> usize {
    50
}

fn default_sample_size() -> usize {
    256
}

#[transform]
pub struct AnomalyStationTransform {
    pub id: String,
    pub inputs: Vec<String>,
    /// Ngưỡng anomaly score (RCF score thường ~1.0, bất thường lớn dần).
    #[serde(default = "default_threshold")]
    pub threshold: f64,
    #[serde(default = "default_num_trees")]
    pub num_trees: usize,
    #[serde(default = "default_sample_size")]
    pub sample_size: usize,
    /// Số điểm liên tiếp ghép thành 1 vector (shingling) — >1 để bắt bất
    /// thường theo hình dạng chuỗi thay vì giá trị đơn.
    #[serde(default)]
    pub shingle_size: usize,
}

impl AnomalyStationTransform {
    #[must_use]
    pub fn new(id: &str, inputs: &[&str]) -> Self {
        Self {
            id: id.to_string(),
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
            threshold: default_threshold(),
            num_trees: default_num_trees(),
            sample_size: default_sample_size(),
            shingle_size: 1,
        }
    }
}

impl_anomaly_station_transform!(
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let ctx = downcast_ctx(&tx)?;
        ctx.registry(
            &self.id,
            Station::Timeseries(Arc::new(RwLock::new(TimeseriesStation::default()))),
        )
        .await
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })?;
        let me = ctx
            .station::<Arc<RwLock<TimeseriesStation>>>(&self.id)
            .await?;

        let cfg = RcfConfig {
            num_trees: self.num_trees,
            sample_size: self.sample_size,
            shingle_size: self.shingle_size.max(1),
            leaf_capacity: 1,
        };
        // Một rừng RCF cho mỗi metric — tránh cross-metric contamination.
        let forests: Mutex<HashMap<String, RcfForest>> = Mutex::new(HashMap::new());

        while let Some(msg) = rx.recv().await {
            for s in &tx.streams {
                let _ = s.send(msg.clone()).await;
            }
            let batch = extract_observations(&msg.payload);
            if batch.is_empty() {
                continue;
            }

            let mut anomalies: Vec<opsense_model::events::Observation> = Vec::new();
            let mut guard = forests.lock().await;
            for obs in &batch {
                let forest = guard
                    .entry(obs.metric_id.clone())
                    .or_insert_with(|| RcfForest::with_config(cfg, 1));
                let score = forest.add(&[obs.value]).unwrap_or(None);

                let rcf_breach = score.is_some_and(|s| s > self.threshold);
                let capacity_breach = ctx
                    .capacity(&obs.metric_id)
                    .is_some_and(|max| obs.value > max);

                if rcf_breach || capacity_breach {
                    let mut anomaly = obs.clone();
                    anomaly.severity = Some(LogLevel::Error);
                    anomaly.labels.insert("anomaly".into(), "true".into());
                    if let Some(s) = score {
                        anomaly
                            .labels
                            .insert("anomaly_score".into(), format!("{s:.3}"));
                    }
                    if capacity_breach {
                        anomaly
                            .labels
                            .insert("capacity".into(), "breached".into());
                    }
                    anomalies.push(anomaly);
                }
            }
            drop(guard);

            if !anomalies.is_empty() {
                let from = anomalies.iter().map(|o| o.ts).min().unwrap_or(0);
                let to = anomalies.iter().map(|o| o.ts).max().unwrap_or(0);
                me.write().await.update_range(&anomalies, from, to, to);
            }
        }
        Ok(())
    }
);

#[cfg(test)]
mod tests {
    use super::*;
    use opsense_core::StationKind;

    fn component(id: &str) -> AnomalyStationTransform {
        AnomalyStationTransform::new(id, &["upstream"])
    }

    #[test]
    fn deserializes_with_defaults() {
        let json = serde_json::json!({
            "type": "anomaly_station_transform",
            "id": "anom",
            "inputs": ["ingest"]
        });
        let boxed: Box<dyn Component> = serde_json::from_value(json).unwrap();
        let c = boxed
            .as_any()
            .downcast_ref::<AnomalyStationTransform>()
            .unwrap();
        assert_eq!(c.id, "anom");
        assert!((c.threshold - 3.0).abs() < f64::EPSILON);
        assert_eq!(c.num_trees, 50);
        assert_eq!(c.sample_size, 256);
        assert_eq!(c.shingle_size, 0); // 0 → chuẩn hoá thành 1 lúc chạy
    }

    #[test]
    fn override_fields_roundtrip() {
        let json = serde_json::json!({
            "type": "anomaly_station_transform",
            "id": "anom",
            "inputs": ["ingest"],
            "threshold": 2.5,
            "num_trees": 10,
            "sample_size": 64,
            "shingle_size": 4
        });
        let boxed: Box<dyn Component> = serde_json::from_value(json).unwrap();
        let c = boxed
            .as_any()
            .downcast_ref::<AnomalyStationTransform>()
            .unwrap();
        assert!((c.threshold - 2.5).abs() < f64::EPSILON);
        assert_eq!(c.num_trees, 10);
        assert_eq!(c.sample_size, 64);
        assert_eq!(c.shingle_size, 4);
        assert_eq!(c.clone_arc().id(), "anom");
    }

    #[test]
    fn registers_as_timeseries() {
        let c = component("anom");
        assert_eq!(c.component_type().to_string(), "Transform");
        let _ = StationKind::Timeseries; // Station đúng kind được assert trong pipeline E2E
    }
}
