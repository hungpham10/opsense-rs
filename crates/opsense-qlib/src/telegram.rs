//! # Telegram Components
//!
//! Các component liên quan đến gửi tin nhắn Telegram.
//! - `TelegramMessage`: Message type cho inter-component communication
//! - `TelegramSink`: Sink component nhận TelegramMessage và gửi đến Telegram API

use std::fmt;
use std::io::Error;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{error, warn};
use vector_config_macro::sink;
use vector_runtime::{Component, Identify, Message as VectorMessage, Outbound};

/// Telegram message được truyền giữa các components qua Vector runtime.
/// Component producer (như StrategySandboxies) output message này,
/// TelegramSink consume và gửi đến Telegram API.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TelegramMessage {
    /// Bot token (có thể override per-message hoặc dùng default từ config)
    pub token: Option<String>,
    /// Chat ID đích (có thể override per-message hoặc dùng default từ config)
    pub chat_id: Option<i64>,
    /// Nội dung tin nhắn (Markdown format)
    pub text: String,
    /// Parse mode: "Markdown", "HTML", hoặc None
    #[serde(default)]
    pub parse_mode: Option<String>,
    /// Disable web page preview
    #[serde(default)]
    pub disable_web_page_preview: bool,
    /// Disable notification
    #[serde(default)]
    pub disable_notification: bool,
}

impl TelegramMessage {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            ..Default::default()
        }
    }

    pub fn with_token(mut self, token: String) -> Self {
        self.token = Some(token);
        self
    }

    pub fn with_chat_id(mut self, chat_id: i64) -> Self {
        self.chat_id = Some(chat_id);
        self
    }

    pub fn with_parse_mode(mut self, parse_mode: impl Into<String>) -> Self {
        self.parse_mode = Some(parse_mode.into());
        self
    }
}

/// TelegramSink — Sink component nhận TelegramMessage và gửi đến Telegram Bot API.
///
/// Config:
/// - `token`: Default bot token (có thể override per-message)
/// - `chat_id`: Default chat ID (có thể override per-message)
/// - `inputs`: Input stream names (tự động derive từ macro)
#[sink(exclude(Debug))]
pub struct TelegramSink {
    pub id: String,
    #[serde(default)]
    pub inputs: Vec<String>,

    /// Default bot token (dùng khi message không có token)
    pub token: Option<String>,
    /// Default chat ID (dùng khi message không có chat_id)
    pub chat_id: Option<i64>,

    /// HTTP client timeout (seconds)
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

fn default_timeout() -> u64 {
    30
}

impl fmt::Debug for TelegramSink {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TelegramSink")
            .field("id", &self.id)
            .field("token", &self.token.as_ref().map(|_| "***"))
            .field("chat_id", &self.chat_id)
            .field("timeout_secs", &self.timeout_secs)
            .finish()
    }
}

impl TelegramSink {
    /// Gửi một TelegramMessage đến Telegram Bot API.
    async fn send_message(&self, msg: &TelegramMessage) -> Result<(), Error> {
        let token = msg.token.as_ref().or(self.token.as_ref()).ok_or_else(|| {
            Error::other(
                "Telegram bot token not configured (neither in message nor in sink config)",
            )
        })?;
        let chat_id = msg.chat_id.or(self.chat_id).ok_or_else(|| {
            Error::other("Telegram chat_id not configured (neither in message nor in sink config)")
        })?;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(self.timeout_secs))
            .build()
            .map_err(|e| Error::other(format!("Failed to create HTTP client: {e}")))?;

        let mut body = serde_json::json!({
            "chat_id": chat_id,
            "text": msg.text,
        });

        if let Some(pm) = &msg.parse_mode {
            body["parse_mode"] = serde_json::Value::String(pm.clone());
        }
        if msg.disable_web_page_preview {
            body["disable_web_page_preview"] = serde_json::Value::Bool(true);
        }
        if msg.disable_notification {
            body["disable_notification"] = serde_json::Value::Bool(true);
        }

        let url = format!("https://api.telegram.org/bot{}/sendMessage", token);
        let resp = client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::other(format!("Telegram request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            warn!("Telegram API error: {} - {}", status, text);
            return Err(Error::other(format!(
                "Telegram API error: {} - {}",
                status, text
            )));
        }

        Ok(())
    }
}

impl_telegram_sink!(
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<VectorMessage>,
        _tx: Outbound,
    ) -> Result<(), Error> {
        while let Some(msg) = rx.recv().await {
            // Parse payload as TelegramMessage
            let tg_msg: TelegramMessage = match serde_json::from_value(msg.payload) {
                Ok(m) => m,
                Err(e) => {
                    warn!("[TelegramSink] Failed to parse message: {e}");
                    continue;
                }
            };

            // Send to Telegram (fire-and-forget with error logging)
            if let Err(e) = self.send_message(&tg_msg).await {
                error!("[TelegramSink] Failed to send message: {e}");
            }
        }
        Ok(())
    }
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telegram_message_builder() {
        let msg = TelegramMessage::new("Hello")
            .with_token("bot123".into())
            .with_chat_id(456)
            .with_parse_mode("Markdown");

        assert_eq!(msg.text, "Hello");
        assert_eq!(msg.token, Some("bot123".into()));
        assert_eq!(msg.chat_id, Some(456));
        assert_eq!(msg.parse_mode, Some("Markdown".into()));
    }

    #[test]
    fn test_telegram_message_default() {
        let msg = TelegramMessage::default();
        assert_eq!(msg.text, "");
        assert!(msg.token.is_none());
        assert!(msg.chat_id.is_none());
        assert!(!msg.disable_web_page_preview);
        assert!(!msg.disable_notification);
    }

    #[test]
    #[ignore]
    fn test_deserialize_telegram_sink() {
        let json = serde_json::json!({
            "id": "tg_sink",
            "type": "telegram_sink",
            "token": "bot_token_123",
            "chat_id": 123456,
            "timeout_secs": 10
        });

        let sink: TelegramSink = serde_json::from_value(json).unwrap();
        assert_eq!(sink.id, "tg_sink");
        assert_eq!(sink.token, Some("bot_token_123".into()));
        assert_eq!(sink.chat_id, Some(123456));
        assert_eq!(sink.timeout_secs, 10);
    }

    #[test]
    #[ignore]
    fn test_deserialize_telegram_sink_defaults() {
        let json = serde_json::json!({
            "id": "tg_sink_minimal",
            "type": "telegram_sink"
        });

        let sink: TelegramSink = serde_json::from_value(json).unwrap();
        assert_eq!(sink.id, "tg_sink_minimal");
        assert!(sink.token.is_none());
        assert!(sink.chat_id.is_none());
        assert_eq!(sink.timeout_secs, 30); // default
    }

    #[test]
    #[ignore]
    fn test_deserialize_telegram_message() {
        let json = serde_json::json!({
            "text": "Test message",
            "token": "bot123",
            "chat_id": 456,
            "parse_mode": "HTML"
        });

        let msg: TelegramMessage = serde_json::from_value(json).unwrap();
        assert_eq!(msg.text, "Test message");
        assert_eq!(msg.token, Some("bot123".into()));
        assert_eq!(msg.chat_id, Some(456));
        assert_eq!(msg.parse_mode, Some("HTML".into()));
    }
}
