use std::io::{Error, ErrorKind};

use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use crate::vector::runtime::{Component, Identify, Message, Outbound};
use opsense_macros::sink;

#[sink(derive(PartialEq))]
pub struct FileSink {
    pub id: String,
    pub path: String,
    pub inputs: Vec<String>,
}

impl_file_sink!(
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<Message>,
        _: Outbound,
    ) -> Result<(), std::io::Error> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;

        while let Some(message) = rx.recv().await {
            let payload_json = serde_json::to_string(&message.payload)
                .unwrap_or_else(|_| format!("{:?}", message.payload));

            let mut line = payload_json;

            line.push('\n');
            file.write_all(line.as_bytes()).await.map_err(|e| {
                Error::new(
                    ErrorKind::WriteZero,
                    format!("Failed to write to file {}: {}", self.path, e),
                )
            })?;

            file.flush().await?;
        }

        Err(Error::new(
            ErrorKind::BrokenPipe,
            format!("FileSink {}: Input channel closed", self.id),
        ))
    }
);
