//! Pretty-printing helpers for REPL output. Backed by `comfy-table`.

use comfy_table::{modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL, ContentArrangement, Table};

use crate::client::{EditResult, Observation, Status};
use crate::client::graphql::StationSummary;

/// Table-rendering extension trait for `OpsenseClient`. Keeps the formatting
/// glue next to the client and off the dispatch hot path.
pub trait TableDisplay {
    fn status_table(&self, status: &Status) -> Table;
    fn stations_table(&self, stations: &[StationSummary]) -> Table;
}

pub fn format_status_table(status: &Status) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["id", "type", "inputs"]);
    for n in &status.nodes {
        table.add_row(vec![n.id.clone(), n.kind.clone(), n.inputs.join(", ")]);
    }
    table
}

pub fn format_stations_table(stations: &[StationSummary]) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(vec!["id", "kind"]);
    for s in stations {
        table.add_row(vec![s.id.clone(), s.kind.clone()]);
    }
    table
}

pub fn format_attributes(attrs: &std::collections::BTreeMap<String, String>) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(vec!["name", "value"]);
    for (k, v) in attrs {
        table.add_row(vec![k.clone(), v.clone()]);
    }
    table
}

pub fn format_edit_result(label: &str, result: &EditResult) -> String {
    let mut out = String::new();
    out.push_str(&format!("{label}: reloaded={}\n", result.reloaded));
    for n in &result.nodes {
        out.push_str(&format!("  {} ({}) <- {}\n", n.id, n.kind, n.inputs.join(", ")));
    }
    out
}

pub fn format_observations(observations: &[Observation]) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(vec!["ts", "metric", "value", "labels"]);
    for o in observations {
        let labels = o
            .labels
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        table.add_row(vec![
            o.ts.to_string(),
            o.metric_id.clone(),
            o.value.to_string(),
            labels,
        ]);
    }
    table
}
