use std::io::{Error, ErrorKind};
use std::time::Duration;

use opsense_macros::sink;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::vector::runtime::{Component, Identify, Message, Outbound};

#[sink]
pub struct TelegramSink {
    pub id: String,
    pub inputs: Vec<String>,
    pub token_env: String,
    pub chat_id: String,
}

impl TelegramSink {
    fn validate(&self) -> Result<(), Error> {
        if self.token_env.trim().is_empty() || self.chat_id.trim().is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "token_env and chat_id are required",
            ));
        }
        Ok(())
    }

    pub fn body(&self, payload: &Value) -> Result<Value, Error> {
        let text = serde_json::to_string(payload).map_err(Error::other)?;
        if text.encode_utf16().count() > 4096 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Telegram event exceeds 4096 UTF-16 units",
            ));
        }
        Ok(json!({"chat_id": self.chat_id, "text": text}))
    }

    fn validate_response(status: reqwest::StatusCode, payload: &Value) -> Result<(), Error> {
        if !status.is_success() || payload.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err(Error::other(format!(
                "Telegram rejected notification (HTTP {})",
                status.as_u16()
            )));
        }
        Ok(())
    }
}

impl_telegram_sink!(
    async fn run(
        &self,
        _: usize,
        rx: &mut mpsc::Receiver<Message>,
        _: Outbound,
    ) -> Result<(), Error> {
        self.validate()?;
        let token = std::env::var(&self.token_env).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "Telegram token environment variable unavailable",
            )
        })?;
        if !token.contains(':')
            || !token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b':' || b == b'_' || b == b'-')
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Invalid Telegram token",
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| Error::other("Failed to build Telegram HTTP client"))?;
        let url = format!("https://api.telegram.org/bot{token}/sendMessage");
        while let Some(msg) = rx.recv().await {
            let body = self.body(&msg.payload)?;
            let response =
                client.post(&url).json(&body).send().await.map_err(|_| {
                    Error::other("Telegram request failed; delivery status unknown")
                })?;
            let status = response.status();
            let payload: Value = response
                .json()
                .await
                .map_err(|_| Error::other("Invalid Telegram response; delivery status unknown"))?;
            Self::validate_response(status, &payload)?;
        }
        Ok(())
    }
);

#[cfg(test)]
mod tests {
    use super::*;

    fn sink() -> TelegramSink {
        TelegramSink {
            id: "notify".into(),
            inputs: vec!["trades".into()],
            token_env: "TELEGRAM_BOT_TOKEN".into(),
            chat_id: "123".into(),
        }
    }

    #[test]
    fn component_roundtrips_without_secret() {
        let value = json!({"type":"telegram_sink", "id":"notify", "inputs":["trades"], "token_env":"TELEGRAM_BOT_TOKEN", "chat_id":"123"});
        let component: Box<dyn Component> = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(component).unwrap(), value);
    }

    #[test]
    fn sends_plain_json_without_markup() {
        let payload = json!({"event": "<Placed>", "symbol": "BTC_USDT"});
        let body = sink().body(&payload).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(body["text"].as_str().unwrap()).unwrap(),
            payload
        );
        assert!(body.get("parse_mode").is_none());
    }

    #[test]
    fn rejects_oversized_events_without_truncation() {
        assert!(sink().body(&json!("x".repeat(4094))).is_ok());
        assert!(sink().body(&json!("x".repeat(4095))).is_err());
        assert!(sink().body(&json!("𝄞".repeat(2048))).is_err());
    }

    #[test]
    fn requires_successful_api_response() {
        assert!(
            TelegramSink::validate_response(reqwest::StatusCode::OK, &json!({"ok":true})).is_ok()
        );
        for (status, body) in [
            (reqwest::StatusCode::OK, json!({"ok":false})),
            (reqwest::StatusCode::TOO_MANY_REQUESTS, json!({"ok":true})),
            (reqwest::StatusCode::OK, json!({})),
        ] {
            assert!(TelegramSink::validate_response(status, &body).is_err());
        }
    }

    #[test]
    fn requires_destination_and_token_variable() {
        let mut config = sink();
        config.chat_id.clear();
        assert!(config.validate().is_err());
        config = sink();
        config.token_env.clear();
        assert!(config.validate().is_err());
    }
}
