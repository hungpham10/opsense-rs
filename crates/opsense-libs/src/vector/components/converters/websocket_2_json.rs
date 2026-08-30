use std::io::{Error, ErrorKind};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::Value;

use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};

use crate::vector::runtime::{Component, Event, Identify, Message as VectorMessage, Outbound};
use opsense_macros::source;

#[async_trait]
pub trait WebSocketPolling {
    async fn on_start(&self) -> Result<Option<String>, Error>;
    async fn on_send(&self) -> Result<Option<String>, Error>;
    async fn on_receive(
        &self,
        message: WsMessage,
        txs: &[mpsc::Sender<VectorMessage>],
    ) -> Result<(), Error>;
}

pub struct WebSocketClient {
    pub url: String,
    pub reconnect_interval_sec: u64,
}

impl WebSocketClient {
    pub fn new(url: String, reconnect_interval_sec: u64) -> Self {
        Self {
            url,
            reconnect_interval_sec,
        }
    }

    pub async fn run<Handler: WebSocketPolling + Send + Sync + 'static>(
        &self,
        handler: Handler,
        id: usize,
        tx: &Outbound,
    ) -> Result<(), Error> {
        loop {
            match connect_async(&self.url).await {
                Ok((ws_stream, _)) => {
                    let (mut write, mut read) = ws_stream.split();

                    let msg_to_send = if let Ok(Some(msg)) = handler.on_start().await {
                        Ok(Some(msg))
                    } else {
                        handler.on_send().await
                    };

                    if let Ok(Some(msg)) = msg_to_send {
                        use futures_util::SinkExt;

                        if let Err(error) = write.send(WsMessage::Text(msg.into())).await {
                            let _ = tx
                                .event
                                .send(Event::Minor((
                                    id,
                                    Error::other(format!(
                                        "Failed to send msg to websocket: {}",
                                        error
                                    )),
                                )))
                                .await;
                            continue;
                        }
                    }

                    while let Some(message) = read.next().await {
                        match message {
                            Ok(msg) => {
                                if let Err(error) =
                                    handler.on_receive(msg, tx.streams.as_slice()).await
                                {
                                    let _ = tx.event.send(Event::Minor((id, error))).await;
                                }
                            }
                            Err(error) => {
                                let _ = tx
                                    .event
                                    .send(Event::Minor((
                                        id,
                                        Error::other(format!(
                                            "Failed to read data from websocket: {}",
                                            error
                                        )),
                                    )))
                                    .await;
                                break;
                            }
                        }

                        if let Ok(Some(msg)) = handler.on_send().await {
                            use futures_util::SinkExt;

                            if let Err(error) = write.send(WsMessage::Text(msg.into())).await {
                                let _ = tx
                                    .event
                                    .send(Event::Minor((
                                        id,
                                        Error::other(format!(
                                            "Failed sending msg to websocket: {}",
                                            error,
                                        )),
                                    )))
                                    .await;
                                break;
                            }

                            sleep(Duration::from_secs(self.reconnect_interval_sec)).await;
                        }
                    }
                }
                Err(error) => {
                    let _ = tx
                        .event
                        .send(Event::Minor((
                            id,
                            Error::other(format!(
                                "Failed when setup websocket connection: {}",
                                error
                            )),
                        )))
                        .await;

                    sleep(Duration::from_secs(self.reconnect_interval_sec)).await;
                }
            }
        }
    }
}

#[source]
pub struct Websocket2Json {
    pub id: String,
    pub uri: String,
    pub start: Option<Value>,
    pub send: Option<Value>,
}

#[async_trait]
impl WebSocketPolling for Websocket2Json {
    async fn on_start(&self) -> Result<Option<String>, Error> {
        match &self.start {
            Some(value) if !value.is_null() => {
                serde_json::to_string(value).map(Some).map_err(|error| {
                    Error::new(
                        ErrorKind::InvalidData,
                        format!("Failed to serialize on_start: {error}"),
                    )
                })
            }
            _ => Ok(None),
        }
    }

    async fn on_send(&self) -> Result<Option<String>, Error> {
        // Sửa lỗi check rỗng cho `Value`: Kiểm tra nếu `Some` và không phải là `Null`
        match &self.send {
            Some(value) if !value.is_null() => {
                serde_json::to_string(value).map(Some).map_err(|e| {
                    Error::new(
                        ErrorKind::InvalidData,
                        format!("Failed to serialize on_send: {e}"),
                    )
                })
            }
            _ => Ok(None),
        }
    }

    async fn on_receive(
        &self,
        message: WsMessage,
        txs: &[mpsc::Sender<VectorMessage>],
    ) -> Result<(), Error> {
        let raw_text = match message {
            WsMessage::Text(text) => text.to_string(),
            WsMessage::Binary(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            _ => return Ok(()),
        };

        if raw_text.is_empty() {
            return Ok(());
        }

        let json_payload: serde_json::Value = serde_json::from_str(&raw_text).map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!("Malformed JSON from WS: {e}"),
            )
        })?;

        let vector_msg = VectorMessage {
            payload: json_payload,
        };

        for tx in txs {
            let _ = tx.send(vector_msg.clone()).await;
        }

        Ok(())
    }
}

impl_websocket_2_json!(
    async fn run(
        &self,
        id: usize,
        _: &mut mpsc::Receiver<VectorMessage>,
        tx: Outbound,
    ) -> Result<(), Error> {
        WebSocketClient::new(self.uri.clone(), 3)
            .run(self.clone(), id, &tx)
            .await
    }
);
