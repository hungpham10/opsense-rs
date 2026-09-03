//! MCP thin client — speaks to `opsense serve` via GraphQL.
//!
//! Every MCP tool is a single GraphQL round-trip via [`OpsenseClient`]. No
//! local state, no caching (the server holds the truth). 10 tools:
//!   - opsense_status            → Query.status
//!   - opsense_attributes        → Query.attributes
//!   - opsense_set_attribute     → Mutation.setAttribute
//!   - opsense_remove_attribute  → Mutation.removeAttribute
//!   - opsense_query_timeseries  → Query.queryTimeseries
//!   - opsense_reload            → Mutation.reload (full component list)
//!   - opsense_stations          → Query.stations (subset of status)

pub mod server;
pub mod tools;

use std::io;

use crate::client::OpsenseClient;

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:8080/graphql";

/// Spin up the MCP server on stdio. Connects to `opsense serve` GraphQL
/// at `endpoint` (env `OPSENSE_GRAPHQL_URL` overrides default).
pub async fn run(endpoint: Option<String>) -> io::Result<()> {
    let endpoint = endpoint
        .or_else(|| std::env::var("OPSENSE_GRAPHQL_URL").ok())
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());

    let client = OpsenseClient::new(&endpoint)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

    server::serve(client).await
}
