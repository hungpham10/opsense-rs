use std::collections::HashMap;
use std::io::{Error, ErrorKind};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::mpsc;

use crate::cast::{CastType, cast_value};
use crate::jq::{JsonQuery, Operator};
use crate::vector::runtime::{Component, Identify, Message, Outbound};
use opsense_macros::transform;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransformConfig {
    query: Vec<Operator>,
    cast_to: Option<CastType>,
}

#[transform]
pub struct Json2Json {
    pub id: String,
    pub inputs: Vec<String>,
    pub constants: Option<HashMap<String, Value>>,

    pub transforms: HashMap<String, TransformConfig>,
}

impl_json_2_json!(
    async fn run(
        &self,
        _: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let pipelines = self
            .transforms
            .iter()
            .map(|(output, config)| {
                (
                    output,
                    (JsonQuery::new(config.query.to_vec()), &config.cast_to),
                )
            })
            .collect::<HashMap<_, _>>();

        while let Some(message) = rx.recv().await {
            let bytes = message.payload.to_string().into_bytes();

            let raw_json: Value = match serde_json::from_slice(&bytes) {
                Ok(val) => val,
                Err(_) => {
                    continue;
                }
            };

            let mut output_map = Map::new();
            let mut skip_message = false;

            for (&output, (query, cast_to)) in &pipelines {
                if let Some(&node) = query.pick(&raw_json).first() {
                    let mut final_node = node.clone();

                    if let Some(target_type) = cast_to {
                        if let Some(casted_value) = cast_value(final_node, target_type) {
                            final_node = casted_value;
                        } else {
                            skip_message = true;
                            break;
                        }
                    }

                    output_map.insert(output.clone(), final_node);
                } else {
                    skip_message = true;
                    break;
                }
            }

            if skip_message {
                continue;
            }

            if let Some(constants) = &self.constants {
                for (key, value) in constants {
                    output_map.insert(key.clone(), value.clone());
                }
            }

            let output_payload = Value::Object(output_map);

            for stream in &tx.streams {
                if let Err(error) = stream
                    .send(Message {
                        payload: output_payload.clone(),
                    })
                    .await
                {
                    return Err(Error::new(
                        ErrorKind::BrokenPipe,
                        format!("Failed to forward dynamic json downstream: {error}"),
                    ));
                }
            }
        }

        Ok(())
    }
);
