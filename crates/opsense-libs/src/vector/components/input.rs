use crate::vector::runtime::{Component, Event, Identify, Message, Outbound};
use opsense_macros::input;
use std::io::{Error, ErrorKind};
use tokio::sync::mpsc;

#[input(derive(PartialEq))]
pub struct Input {
    pub id: String,
}

impl_input!(
    async fn run(
        &self,
        id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), std::io::Error> {
        while let Some(msg) = rx.recv().await {
            let mut failed = true;

            for stream in &tx.streams {
                if let Err(error) = stream.send(msg.clone()).await {
                    tx.event
                        .send(Event::Minor((
                            id,
                            Error::other(format!(
                                "Failed to send data to one specific output: {error}"
                            )),
                        )))
                        .await
                        .map_err(|error| {
                            Error::new(
                                ErrorKind::BrokenPipe,
                                format!("Failed to report error: {error}"),
                            )
                        })?;
                } else {
                    failed = false;
                }
            }

            if failed {
                tx.event
                    .send(Event::Major((
                        id,
                        Error::other("Failed to send data to every node"),
                    )))
                    .await
                    .map_err(|error| {
                        Error::new(
                            ErrorKind::BrokenPipe,
                            format!("Failed to report error: {error}"),
                        )
                    })?;
            }
        }

        Ok(())
    }
);
