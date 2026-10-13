//! Host services implementation for the serve binary.

use opsense_ffi::{BoxFut, HostServices, ModuleError, Value};

use crate::api::AppState;
use opsense_core::Observation;

pub struct ServeHostServices {
    pub state: AppState,
}

impl HostServices for ServeHostServices {
    fn call(&self, op: &str, req: Value) -> BoxFut<'static, Result<Value, ModuleError>> {
        let state = self.state.clone();
        let op = op.to_string();
        
        Box::pin(async move {
            match op.as_str() {
                "status" => {
                    let status = state.status().await;
                    let json = serde_json::json!({
                        "nodes": status.nodes.iter().map(|n| serde_json::json!({
                            "id": n.id,
                            "type": n.kind,
                            "inputs": n.inputs,
                            "description": n.description,
                            "running": n.running,
                            "faultCount": n.fault_count,
                            "lastError": n.last_error,
                        })).collect::<Vec<_>>(),
                        "stations": status.stations.iter().map(|s| serde_json::json!({
                            "id": s.id,
                            "kind": format!("{:?}", s.kind),
                        })).collect::<Vec<_>>(),
                    });
                    Ok(json)
                }
                "components" => {
                    let id = req.get("id").and_then(|v| v.as_str());
                    let comps = state.components(id).await;
                    Ok(serde_json::to_value(&comps).unwrap_or_default())
                }
                "attributes" => {
                    let attrs = state.attributes().await;
                    Ok(serde_json::to_value(&attrs).unwrap_or_default())
                }
                "attr_set" => {
                    let name = req.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    let value = req.get("value").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    state.set_attribute(name.into(), value).await;
                    Ok(serde_json::json!({ "ok": true, "env_override_active": false }))
                }
                "attr_remove" => {
                    let name = req.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    let removed = state.remove_attribute(name).await;
                    Ok(serde_json::json!({ "removed": removed }))
                }
                "query_station" => {
                    Err(ModuleError::Internal("query_station not yet implemented for dynamic modules".into()))
                }
                "orders" => {
                    Err(ModuleError::Internal("orders not yet implemented for dynamic modules".into()))
                }
                "clear_station" => {
                    Err(ModuleError::Internal("clear_station not yet implemented for dynamic modules".into()))
                }
                "clear_all_stations" => {
                    Err(ModuleError::Internal("clear_all_stations not yet implemented for dynamic modules".into()))
                }
                "config_read" => {
                    let comps = state.components(None).await;
                    Ok(serde_json::to_value(&comps).unwrap_or_default())
                }
                "audit_write" => {
                    // Parse observation from req
                    let obs: Observation = match serde_json::from_value(req) {
                        Ok(o) => o,
                        Err(e) => return Err(ModuleError::BadRequest(format!("invalid observation: {e}"))),
                    };
                    state.audit(obs).await;
                    Ok(serde_json::json!({ "ok": true }))
                }
                "station_query" => {
                    Err(ModuleError::Internal("station_query not yet implemented for dynamic modules".into()))
                }
                _ => Err(ModuleError::BadRequest(format!("unknown op: {op}"))),
            }
        })
    }

    fn log(&self, level: u32, _target: &str, msg: &str) {
        match level {
            0 => tracing::error!("{}", msg),
            1 => tracing::warn!("{}", msg),
            2 => tracing::info!("{}", msg),
            3 => tracing::debug!("{}", msg),
            _ => tracing::trace!("{}", msg),
        }
    }
}