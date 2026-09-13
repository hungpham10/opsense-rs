//! `pattern_station_transform` — nhận message JSON từ upstream, feed text
//! từ field `text_field` vào station `Pattern` (Aho-Corasick) rồi forward
//! message xuống downstream với field `matched` (true/false) được thêm vào
//! payload. Component chỉ tương tác với [`PatternStation`] — không đọc
//! station khác.
//!
//! Pattern đăng ký sẵn qua config `patterns`. Mỗi message có field
//! `text_field` (mặc định `"text"`) chứa log line cần match.

use std::io::Error;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{RwLock, mpsc};

use opsense_core::PatternStation;
use opsense_core::Station;
use opsense_macros::transform;

use super::downcast_ctx;
use crate::vector::runtime::{Component, Identify, Message, Outbound};

#[transform]
pub struct PatternStationTransform {
    pub id: String,
    pub inputs: Vec<String>,

    /// Field JSON chứa log line cần match. Mặc định `"text"`.
    #[serde(default = "default_text_field")]
    pub text_field: String,

    /// Pattern đăng ký sẵn khi node khởi động.
    #[serde(default)]
    pub patterns: Vec<String>,

    /// Tên field ghi kết quả match (`true`/`false`) vào payload trước khi
    /// forward. Mặc định `"matched"`.
    #[serde(default = "default_matched_field")]
    pub matched_field: String,
}

fn default_text_field() -> String {
    "text".to_string()
}

fn default_matched_field() -> String {
    "matched".to_string()
}

impl PatternStationTransform {
    #[must_use]
    pub fn new(id: &str, inputs: &[&str]) -> Self {
        Self {
            id: id.to_string(),
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
            text_field: default_text_field(),
            patterns: Vec::new(),
            matched_field: default_matched_field(),
        }
    }
}

impl_pattern_station_transform!(
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let ctx = downcast_ctx(&tx)?;
        ctx.registry(
            &self.id,
            Station::Pattern(Arc::new(RwLock::new(PatternStation::new()))),
        )
        .await
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })?;
        let me = ctx.station::<Arc<RwLock<PatternStation>>>(&self.id).await?;

        // Seed các pattern cấu hình sẵn + commit automaton.
        {
            let g = me.read().await;
            for p in &self.patterns {
                g.set(p).await;
            }
            g.commit().await;
        }

        while let Some(msg) = rx.recv().await {
            // Forward bản gốc trước.
            for s in &tx.streams {
                let _ = s.send(msg.clone()).await;
            }
            let Some(text) = msg.payload.get(&self.text_field).and_then(Value::as_str) else {
                continue;
            };
            let matched = me.read().await.lookup(text).await;
            // Forward thêm bản đã gắn `matched_field` để consumer dễ xử lý.
            let mut stamped = msg.clone();
            if let Some(obj) = stamped.payload.as_object_mut() {
                obj.insert(self.matched_field.clone(), Value::Bool(matched));
            }
            for s in &tx.streams {
                let _ = s.send(stamped.clone()).await;
            }
        }
        Ok(())
    }
);
