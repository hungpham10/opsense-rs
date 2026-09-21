use crate::vector::runtime::{Component, Identify, Message, Outbound};
use opsense_macros::sink;
use tokio::sync::mpsc;

#[sink(derive(PartialEq, Default))]
pub struct Null {
    pub id: String,
    pub inputs: Vec<String>,
}

impl_null!(
    async fn run(
        &self,
        _: usize,
        rx: &mut mpsc::Receiver<Message>,
        _: Outbound,
    ) -> Result<(), std::io::Error> {
        while rx.recv().await.is_some() {}
        Ok(())
    }
);
