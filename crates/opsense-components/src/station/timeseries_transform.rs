//! `timeseries_station_transform` — nhận message JSON từ upstream, trích
//! mảng `observations` từ `payload`, append vào station `Timeseries` của
//! chính node, rồi forward message gốc xuống downstream. Component chỉ
//! tương tác với [`TimeseriesStation`] — không đọc station khác.
//!
//! JSON payload dạng `{ "observations": [...] }`; mỗi observation có các
//! field của [`opsense_core::Observation`] (ts, metric_id, kind, signal, value,
//! labels, severity).

use std::io::Error;
use std::sync::Arc;

use tokio::sync::{RwLock, mpsc};

use opsense_core::Station;
use opsense_core::TimeseriesStation;
use opsense_macros::transform;

use super::{downcast_ctx, extract_observations};
use crate::vector::runtime::{Component, Identify, Message, Outbound};

#[transform]
pub struct TimeseriesStationTransform {
    pub id: String,
    pub inputs: Vec<String>,
}

impl TimeseriesStationTransform {
    #[must_use]
    pub fn new(id: &str, inputs: &[&str]) -> Self {
        Self {
            id: id.to_string(),
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
        }
    }
}

impl_timeseries_station_transform!(
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

        while let Some(msg) = rx.recv().await {
            for s in &tx.streams {
                let _ = s.send(msg.clone()).await;
            }
            let batch = extract_observations(&msg.payload);
            if batch.is_empty() {
                continue;
            }
            let from = batch.iter().map(|o| o.ts).min().unwrap_or(0);
            let to = batch.iter().map(|o| o.ts).max().unwrap_or(0);
            me.write().await.update_range(&batch, from, to, to);
        }
        Ok(())
    }
);
