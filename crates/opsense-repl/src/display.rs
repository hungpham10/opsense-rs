//! Terminal rendering: comfy-table grids and RecordBatch previews.

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use comfy_table::{Cell, Color, Table};
use opsense_core::registry;

/// Render rows as an ASCII table with the given headers.
#[must_use]
pub fn table(headers: &[&str], rows: Vec<Vec<String>>) -> String {
    let mut t = Table::new();
    t.set_header(headers.iter().map(|h| Cell::new(*h)));
    for row in rows {
        t.add_row(row);
    }
    t.to_string()
}

/// Render a station list (`id`, backend, latest raw/processed).
pub async fn station_table(ids: &[String]) -> String {
    let mut rows = Vec::with_capacity(ids.len());
    for id in ids {
        let described = registry::describe_station(id).await;
        let row = match described {
            Some(info) => vec![
                id.clone(),
                info["backend"].as_str().unwrap_or("?").to_string(),
                info["latest_raw"].to_string(),
                info["latest_processed"].to_string(),
            ],
            None => vec![id.clone(), "?".into(), "?".into(), "?".into()],
        };
        rows.push(row);
    }
    table(
        &["station", "backend", "latest_raw", "latest_processed"],
        rows,
    )
}

/// Format one cell of a batch by column type; unsupported types become a
/// placeholder instead of failing the whole preview.
#[must_use]
pub fn cell(batch: &RecordBatch, col: usize, row: usize) -> String {
    let column = batch.column(col);
    if column.is_null(row) {
        return "∅".into();
    }
    if let Some(v) = column.as_any().downcast_ref::<Int64Array>() {
        return v.value(row).to_string();
    }
    if let Some(v) = column.as_any().downcast_ref::<Float64Array>() {
        return format!("{:.6}", v.value(row));
    }
    if let Some(v) = column.as_any().downcast_ref::<BooleanArray>() {
        return v.value(row).to_string();
    }
    if let Some(v) = column.as_any().downcast_ref::<StringArray>() {
        return v.value(row).to_string();
    }
    "<opaque>".into()
}

/// First-rows preview used after queries and Python DataFrames.
#[must_use]
pub fn dataframe_preview(batch: &RecordBatch, max_rows: usize) -> String {
    let schema = batch.schema();
    let headers: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let shown = batch.num_rows().min(max_rows);
    let rows: Vec<Vec<String>> = (0..shown)
        .map(|r| {
            (0..batch.num_columns())
                .map(|c| cell(batch, c, r))
                .collect()
        })
        .collect();
    let mut out = table(&headers, rows);
    if batch.num_rows() > shown {
        out.push_str(&format!("\n… {} more rows", batch.num_rows() - shown));
    }
    out
}

/// Colored status line for command results.
#[must_use]
pub fn ok(msg: impl Into<String>) -> String {
    let _ = Color::Green; // color support lands with the styled prompt
    msg.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let ids = Int64Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec!["alice", "bob", "carol"]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(ids), Arc::new(names)],
        )
        .unwrap()
    }

    #[test]
    fn table_renders_headers_and_rows() {
        let headers = &["col_a", "col_b"];
        let rows = vec![
            vec!["1".into(), "hello".into()],
            vec!["2".into(), "world".into()],
        ];
        let out = table(headers, rows);
        assert!(out.contains("col_a"));
        assert!(out.contains("col_b"));
        assert!(out.contains("1"));
        assert!(out.contains("hello"));
    }

    #[test]
    fn table_handles_empty_rows() {
        let headers = &["x"];
        let rows: Vec<Vec<String>> = vec![];
        let out = table(headers, rows);
        assert!(out.contains("x"));
    }

    #[test]
    fn cell_int64() {
        let batch = make_batch();
        assert_eq!(cell(&batch, 0, 0), "1");
        assert_eq!(cell(&batch, 0, 1), "2");
        assert_eq!(cell(&batch, 0, 2), "3");
    }

    #[test]
    fn cell_string() {
        let batch = make_batch();
        assert_eq!(cell(&batch, 1, 0), "alice");
        assert_eq!(cell(&batch, 1, 1), "bob");
        assert_eq!(cell(&batch, 1, 2), "carol");
    }

    #[test]
    fn cell_null_returns_nul() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Schema::new(vec![Field::new("n", DataType::Int64, true)]);
        let nulls = Int64Array::from(vec![None]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(nulls)]).unwrap();
        assert_eq!(cell(&batch, 0, 0), "∅");
    }

    #[test]
    fn dataframe_preview_renders_table() {
        let batch = make_batch();
        let out = dataframe_preview(&batch, 10);
        assert!(out.contains("id"));
        assert!(out.contains("name"));
        assert!(out.contains("alice"));
    }

    #[test]
    fn dataframe_preview_respects_max_rows() {
        let batch = make_batch(); // 3 rows
        let out = dataframe_preview(&batch, 2);
        assert!(out.contains("alice"));
        assert!(out.contains("bob"));
        // should not contain carol
        assert!(!out.contains("carol"));
        // should show truncation notice
        assert!(out.contains("more rows"));
    }

    #[test]
    fn dataframe_preview_all_rows_no_truncation() {
        let batch = make_batch(); // 3 rows
        let out = dataframe_preview(&batch, 100);
        assert!(out.contains("carol"));
        // no truncation message when all rows shown
        assert!(!out.contains("more rows"));
    }

    #[test]
    fn ok_passes_through_message() {
        assert_eq!(ok("done"), "done");
        assert_eq!(ok("ok"), "ok");
        assert_eq!(ok(String::from("hello")), "hello");
    }
}
