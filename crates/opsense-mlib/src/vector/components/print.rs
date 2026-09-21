use opsense_macros::sink;
use tokio::sync::mpsc;

use crate::vector::runtime::{Component, Identify, Message, Outbound};

#[sink]
pub struct Print {
    pub id: String,
    pub inputs: Vec<String>,

    #[serde(default = "default_prefix")]
    pub prefix: String,
}

fn default_prefix() -> String {
    "PRINT-SINK".to_string()
}

impl_print!(
    async fn run(
        &self,
        _: usize,
        rx: &mut mpsc::Receiver<Message>,
        _: Outbound,
    ) -> Result<(), std::io::Error> {
        while let Some(message) = rx.recv().await {
            println!("{}: Received data: {}", self.prefix, message.payload);
        }

        Ok(())
    }
);
