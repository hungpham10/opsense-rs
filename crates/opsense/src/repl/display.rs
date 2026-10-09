//! Pretty-printing helpers for REPL output. Backed by `comfy-table`.

use comfy_table::{ContentArrangement, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};

use crate::client::graphql::StationSummary;
use crate::client::{EditResult, Observation, Status};

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
        .set_header(vec!["id", "type", "inputs", "state", "error"]);
    for n in &status.nodes {
        table.add_row(vec![
            n.id.clone(),
            n.kind.clone(),
            n.inputs.join(", "),
            if n.running { "running" } else { "stopped" }.to_string(),
            n.last_error
                .as_ref()
                .map(|f| clip(&format!("{}: {}: {}", f.severity, f.code, f.message), 40))
                .unwrap_or_else(|| "-".to_string()),
        ]);
    }
    table
}

/// Cắt cột cho bảng. `cmd_status` in thêm mục "Nodes in fault" **không cắt** —
/// đó là chỗ người ta đọc để debug, nên chỗ cắt ở đây là bảng tổng quan.
fn clip(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((idx, _)) => format!("{}…", &s[..idx]),
        None => s.to_string(),
    }
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
        out.push_str(&format!(
            "  {} ({}) <- {}\n",
            n.id,
            n.kind,
            n.inputs.join(", ")
        ));
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

/// Pretty-print PnL summary (from `OrdersResult.pnl`).
/// Same preset as `format_observations` for consistent REPL look.
pub fn format_pnl_summary(pnl: &crate::client::graphql::PnlSummary) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(vec!["metric", "value"]);

    // Header info
    table.add_row(vec!["interval", &pnl.interval]);
    table.add_row(vec!["window", &format!("{} → {}", pnl.from_ts, pnl.to_ts)]);
    table.add_row(vec!["complete", &pnl.complete.to_string()]);

    // Overall totals
    table.add_row(vec!["", ""]);
    table.add_row(vec!["total.trades", &pnl.trades.to_string()]);
    table.add_row(vec!["total.wins", &pnl.wins.to_string()]);
    table.add_row(vec!["total.losses", &pnl.losses.to_string()]);
    table.add_row(vec!["total.win_rate", &format!("{:.2}%", pnl.win_rate * 100.0)]);
    table.add_row(vec!["total.net_pnl_abs", &format!("{:.2}", pnl.net_pnl_abs)]);
    table.add_row(vec!["total.net_pnl_pct", &format!("{:.4}%", pnl.net_pnl_pct * 100.0)]);
    table.add_row(vec!["total.gross_profit", &format!("{:.2}", pnl.gross_profit_abs)]);
    table.add_row(vec!["total.gross_loss", &format!("{:.2}", pnl.gross_loss_abs)]);
    table.add_row(vec!["total.notional", &format!("{:.2}", pnl.notional)]);
    table.add_row(vec!["total.avg_win_pct", &format!("{:.4}%", pnl.avg_win_pct * 100.0)]);
    table.add_row(vec!["total.avg_loss_pct", &format!("{:.4}%", pnl.avg_loss_pct * 100.0)]);
    table.add_row(vec!["total.long_trades", &pnl.long_trades.to_string()]);
    table.add_row(vec!["total.short_trades", &pnl.short_trades.to_string()]);
    table.add_row(vec!["total.open_count", &pnl.open_count.to_string()]);
    table.add_row(vec!["total.open_notional", &format!("{:.2}", pnl.open_notional)]);

    // Unrealized
    if let Some(mark) = pnl.mark_price {
        table.add_row(vec!["total.unrealized_abs", &format!("{:.2}", pnl.unrealized_abs)]);
        table.add_row(vec!["total.mark_price", &format!("{:.2}", mark)]);
    }

    // Zero-size rows
    if pnl.zero_size_rows > 0 {
        table.add_row(vec!["total.zero_size_rows", &pnl.zero_size_rows.to_string()]);
    }

    // Per-bucket breakdown (only if interval > 0)
    if !pnl.buckets.is_empty() {
        table.add_row(vec!["", ""]);
        table.add_row(vec!["bucket_ts", "trades", "wins", "losses", "win%", "net_abs", "net_pct", "notional"]);
        for b in &pnl.buckets {
            table.add_row(vec![
                &b.bucket_ts.to_string(),
                &b.trades.to_string(),
                &b.wins.to_string(),
                &b.losses.to_string(),
                &format!("{:.1}%", b.win_rate * 100.0),
                &format!("{:.2}", b.net_pnl_abs),
                &format!("{:.4}%", b.net_pnl_pct * 100.0),
                &format!("{:.2}", b.notional),
            ]);
        }
    }

    table
}
