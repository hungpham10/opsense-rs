//! `station_transform` — station đứng giữa pipeline: nhận
//! `data_ready|processed(ts)`, snapshot cửa sổ `(cursor, ts]` vào station store
//! riêng (giống `station_sink`), rồi **forward signal gốc** cho downstream —
//! khác duy nhất so với sink ở bước cuối này.

use std::io::Error;

use crate::station::{
    default_bind, default_block_secs, default_max_hot_blocks, default_max_hot_mb, default_stage,
    ensure_station, stage_of, StationOptions, StationStorage,
};
use crate::vector::runtime::{Component, Identify, Message, Outbound};
use crate::{signal, OpsenseContext};
use opsense_core::Context;
use opsense_macros::transform;
use tokio::sync::mpsc;

#[transform]
pub struct TimeseriesStationTransform {
    pub id: String,
    pub inputs: Vec<String>,
    /// Which working-store stage this station snapshots: `raw` | `processed`.
    #[serde(default = "default_stage")]
    pub stage: String,
    /// Bind address of the query endpoint.
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_block_secs")]
    pub block_secs: i64,
    #[serde(default = "default_max_hot_blocks")]
    pub max_hot_blocks: usize,
    /// Soft cap on approximate hot bytes per stage.
    #[serde(default = "default_max_hot_mb")]
    pub max_hot_mb: usize,
    /// Cold-tier directory (LMDB); empty keeps it RAM-only.
    #[serde(default)]
    pub data_dir: String,
    /// Delete cold observations older than this many seconds (0 = forever).
    #[serde(default)]
    pub cold_retention_secs: i64,
}

impl TimeseriesStationTransform {
    #[must_use]
    pub fn new(id: &str, inputs: &[&str]) -> Self {
        Self {
            id: id.to_string(),
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
            stage: default_stage(),
            bind: default_bind(),
            block_secs: default_block_secs(),
            max_hot_blocks: default_max_hot_blocks(),
            max_hot_mb: default_max_hot_mb(),
            data_dir: String::new(),
            cold_retention_secs: 0,
        }
    }

    fn options(&self) -> StationOptions {
        StationOptions {
            id: self.id.clone(),
            inputs: self.inputs.clone(),
            bind: self.bind.clone(),
            block_secs: self.block_secs,
            max_hot_blocks: self.max_hot_blocks,
            max_hot_mb: self.max_hot_mb,
            data_dir: self.data_dir.clone(),
            cold_retention_secs: self.cold_retention_secs,
            origin_enabled: false,
            stages: vec![stage_of(&self.stage)],
            storage: StationStorage::None,
        }
    }
}

impl_timeseries_station_transform!(
    async fn pre_run(&self) -> Result<(), Error> {
        let _cache = ensure_station(&self.options()).await;
        Ok(())
    }

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
        let stage = stage_of(&self.stage);
        let cache = ensure_station(&self.options()).await;

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
                .read_window(&self.inputs, signal::src(&msg), from, ts, Some(stage))
                .await;
            if !batch.is_empty() {
                let mut g = cache.write().await;
                g.append(stage, &batch).await;
            }
            ctx.set_node_watermark(&self.id, ts);
        }
        Ok(())
    }
);
