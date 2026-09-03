//! `category_station_transform` — nhận message JSON từ upstream, trích
//! cặp key/value từ `payload` rồi `insert` vào station `Category` của chính
//! node. Component chỉ tương tác với [`CategoryStation`] — không đọc
//! station khác. Forward message gốc xuống downstream.
//!
//! JSON payload dạng `{ "key": "...", "value": "..." }`. Field key/value có
//! thể chỉnh qua config (mặc định `"key"` / `"value"`).

use std::io::Error;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{RwLock, mpsc};

use opsense_core::CategoryStation;
use opsense_core::Station;
use opsense_macros::transform;

use super::downcast_ctx;
use crate::vector::runtime::{Component, Identify, Message, Outbound};

#[transform]
pub struct CategoryStationTransform {
    pub id: String,
    pub inputs: Vec<String>,

    /// Tên field JSON làm key khi insert. Mặc định `"key"`.
    #[serde(default = "default_key_field")]
    pub key_field: String,

    /// Tên field JSON làm value khi insert. Mặc định `"value"`.
    #[serde(default = "default_value_field")]
    pub value_field: String,
}

fn default_key_field() -> String {
    "key".to_string()
}

fn default_value_field() -> String {
    "value".to_string()
}

fn field<'a>(payload: &'a Value, name: &str) -> Option<&'a str> {
    payload.get(name).and_then(Value::as_str)
}

impl_category_station_transform!(
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let ctx = downcast_ctx(&tx)?;
        ctx.registry(
            &self.id,
            Station::Category(Arc::new(RwLock::new(CategoryStation::new()))),
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
            .station::<Arc<RwLock<CategoryStation>>>(&self.id)
            .await?;

        while let Some(msg) = rx.recv().await {
            for s in &tx.streams {
                let _ = s.send(msg.clone()).await;
            }
            if let (Some(key), Some(value)) = (
                field(&msg.payload, &self.key_field),
                field(&msg.payload, &self.value_field),
            ) {
                me.write().await.insert(key, value).await?;
            }
        }
        Ok(())
    }
);
