use std::io::Error;

use crate::vector::runtime::{Component, Identify, Message, Outbound};
use opsense_macros::output;
use tokio::sync::mpsc;

#[output(derive(PartialEq))]
pub struct Output {
    pub id: String,
    pub inputs: Vec<String>,
}

impl_output!(
    async fn run(
        &self,
        _: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        while let Some(msg) = rx.recv().await {
            if let Some(ref broadcast) = tx.broadcast {
                let _ = broadcast.send(msg);
            }
        }
        Ok(())
    }
);
