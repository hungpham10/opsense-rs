//! Host services available to modules via the FFI `call` function.

use std::future::Future;
use std::pin::Pin;
use serde_json::Value;

use super::BoxFut;

/// Operations the host supports. Called via `HostServices::call(op, req)`.
///
/// Adding new operations does not require ABI changes — just a new `op` string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostOp {
    /// Get pipeline status (nodes + stations).
    /// req: {} → res: { nodes: [...], stations: [...] }
    Status,

    /// Get live component configs.
    /// req: { id?: string } → res: [ComponentConfig, ...]
    Components,

    /// Get all in-memory attributes.
    /// req: {} → res: { key: value, ... }
    Attributes,

    /// Set an attribute.
    /// req: { name: string, value: string } → res: { ok: bool, env_override_active: bool }
    AttrSet,

    /// Remove an attribute.
    /// req: { name: string } → res: { removed: bool }
    AttrRemove,

    /// Query a timeseries station.
    /// req: { node, from_ts?, to_ts?, limit?, signal?, label_kind?, status?, order? }
    /// res: { observations: [...], truncated: bool, scanned: usize }
    QueryStation,

    /// Get trading orders with optional PnL aggregation.
    /// req: { node, status?, from_ts?, to_ts?, limit?, order?, interval?, mark_price? }
    /// res: { observations: [...], pnl?: PnlSummary, ... }
    Orders,

    /// Clear a single station (RAM + storage).
    /// req: { node: string } → res: { cleared: [...], failed: [...] }
    ClearStation,

    /// Clear ALL stations (requires confirm=true).
    /// req: { confirm: true } → res: { cleared: [...], failed: [...] }
    ClearAllStations,

    /// Read current pipeline config (typetag JSON).
    /// req: {} → res: [ComponentConfig, ...]
    ConfigRead,

    /// Write an audit observation.
    /// req: Observation JSON → res: { ok: true }
    AuditWrite,

    /// Raw station query (for advanced use cases).
    /// req: { node, from_ts, to_ts, limit, signal, label_kind, status, order }
    /// res: { observations: [...], truncated, scanned }
    StationQuery,
}

impl HostOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            HostOp::Status => "status",
            HostOp::Components => "components",
            HostOp::Attributes => "attributes",
            HostOp::AttrSet => "attr_set",
            HostOp::AttrRemove => "attr_remove",
            HostOp::QueryStation => "query_station",
            HostOp::Orders => "orders",
            HostOp::ClearStation => "clear_station",
            HostOp::ClearAllStations => "clear_all_stations",
            HostOp::ConfigRead => "config_read",
            HostOp::AuditWrite => "audit_write",
            HostOp::StationQuery => "station_query",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "status" => Some(HostOp::Status),
            "components" => Some(HostOp::Components),
            "attributes" => Some(HostOp::Attributes),
            "attr_set" => Some(HostOp::AttrSet),
            "attr_remove" => Some(HostOp::AttrRemove),
            "query_station" => Some(HostOp::QueryStation),
            "orders" => Some(HostOp::Orders),
            "clear_station" => Some(HostOp::ClearStation),
            "clear_all_stations" => Some(HostOp::ClearAllStations),
            "config_read" => Some(HostOp::ConfigRead),
            "audit_write" => Some(HostOp::AuditWrite),
            "station_query" => Some(HostOp::StationQuery),
            _ => None,
        }
    }
}

/// Trait for host services. Implemented by the gateway, passed to plugins.
///
/// The plugin calls `host.call(op, req_json)` and receives `res_json`.
/// All communication is JSON-based to maintain ABI stability.
pub trait HostServices: Send + Sync + 'static {
    /// Call a host operation.
    ///
    /// This is the single escape hatch for module → host communication.
    /// The host dispatches on `op` and returns a JSON response.
    fn call(&self, op: &str, req: Value) -> BoxFut<'static, Result<Value, super::ModuleError>>;

    /// Log a message through the host's tracing infrastructure.
    fn log(&self, level: u32, target: &str, msg: &str);
}

/// Builder for host call requests — helps avoid manual JSON construction.
pub struct HostCall<'a> {
    host: &'a dyn HostServices,
    op: HostOp,
    req: serde_json::Map<String, Value>,
}

impl<'a> HostCall<'a> {
    pub fn new(host: &'a dyn HostServices, op: HostOp) -> Self {
        Self { host, op, req: serde_json::Map::new() }
    }

    pub fn arg<T: serde::Serialize + ?Sized>(mut self, key: &str, value: &T) -> Result<Self, serde_json::Error> {
        self.req.insert(key.into(), serde_json::to_value(value)?);
        Ok(self)
    }

    pub fn arg_opt<T: serde::Serialize + ?Sized>(mut self, key: &str, value: Option<&T>) -> Result<Self, serde_json::Error> {
        if let Some(v) = value {
            self.req.insert(key.into(), serde_json::to_value(v)?);
        }
        Ok(self)
    }

    pub async fn execute(self) -> Result<Value, super::ModuleError> {
        self.host.call(self.op.as_str(), Value::Object(self.req)).await
    }
}

/// Convenience methods on `HostServices` for common operations.
impl dyn HostServices + '_ {
    pub async fn status(&self) -> Result<Value, super::ModuleError> {
        HostCall::new(self, HostOp::Status).execute().await
    }

    pub async fn components(&self, id: Option<&str>) -> Result<Value, super::ModuleError> {
        HostCall::new(self, HostOp::Components).arg_opt("id", id)?.execute().await
    }

    pub async fn attributes(&self) -> Result<Value, super::ModuleError> {
        HostCall::new(self, HostOp::Attributes).execute().await
    }

    pub async fn set_attribute(&self, name: &str, value: &str) -> Result<Value, super::ModuleError> {
        HostCall::new(self, HostOp::AttrSet).arg("name", name)?.arg("value", value)?.execute().await
    }

    pub async fn remove_attribute(&self, name: &str) -> Result<Value, super::ModuleError> {
        HostCall::new(self, HostOp::AttrRemove).arg("name", name)?.execute().await
    }

    pub async fn query_station(
        &self,
        node: &str,
        from_ts: Option<Value>,
        to_ts: Option<Value>,
        limit: Option<i64>,
        signal: Option<&str>,
        label_kind: Option<&str>,
        status: Option<&str>,
        order: Option<&str>,
    ) -> Result<Value, super::ModuleError> {
        let mut call = HostCall::new(self, HostOp::QueryStation);
        call = call.arg("node", node)?;
        if let Some(v) = from_ts { call = call.arg("from_ts", &v)?; }
        if let Some(v) = to_ts { call = call.arg("to_ts", &v)?; }
        if let Some(v) = limit { call = call.arg("limit", &v)?; }
        if let Some(v) = signal { call = call.arg("signal", v)?; }
        if let Some(v) = label_kind { call = call.arg("label_kind", v)?; }
        if let Some(v) = status { call = call.arg("status", v)?; }
        if let Some(v) = order { call = call.arg("order", v)?; }
        call.execute().await
    }

    pub async fn orders(
        &self,
        node: &str,
        status: Option<&str>,
        from_ts: Option<Value>,
        to_ts: Option<Value>,
        limit: Option<i64>,
        order: Option<&str>,
        interval: Option<&str>,
        mark_price: Option<f64>,
    ) -> Result<Value, super::ModuleError> {
        let mut call = HostCall::new(self, HostOp::Orders);
        call = call.arg("node", node)?;
        if let Some(v) = status { call = call.arg("status", v)?; }
        if let Some(v) = from_ts { call = call.arg("from_ts", &v)?; }
        if let Some(v) = to_ts { call = call.arg("to_ts", &v)?; }
        if let Some(v) = limit { call = call.arg("limit", &v)?; }
        if let Some(v) = order { call = call.arg("order", v)?; }
        if let Some(v) = interval { call = call.arg("interval", v)?; }
        if let Some(v) = mark_price { call = call.arg("mark_price", &v)?; }
        call.execute().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_op_str_roundtrip() {
        for op in [
            HostOp::Status, HostOp::Components, HostOp::Attributes,
            HostOp::AttrSet, HostOp::AttrRemove, HostOp::QueryStation,
            HostOp::Orders, HostOp::ClearStation, HostOp::ClearAllStations,
            HostOp::ConfigRead, HostOp::AuditWrite, HostOp::StationQuery,
        ] {
            assert_eq!(HostOp::from_str(op.as_str()), Some(op));
        }
        assert_eq!(HostOp::from_str("unknown"), None);
    }
}