//! Arrow segment helpers — moved from `opsense-session::backend`.

use anyhow::{Result, anyhow};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_array::RecordBatch;
use bytes::Bytes;

/// Rows per ARROW frame when streaming a dataset into a kernel (~0.5 MB of
/// f64 columns); keeps any single frame well under wire limits without going
/// row-by-row.
pub const DATASET_CHUNK_ROWS: usize = 64_000;

/// Split a RecordBatch into N complete Arrow IPC stream segments of at most
/// [`DATASET_CHUNK_ROWS`] rows each.
///
/// # Errors
/// Arrow encode failures.
pub fn chunk_record_batch(rb: &RecordBatch) -> Result<Vec<Bytes>> {
    let total = rb.num_rows();
    let mut segments = Vec::with_capacity(total.div_ceil(DATASET_CHUNK_ROWS).max(1));
    let mut offset = 0;
    while offset < total {
        let len = DATASET_CHUNK_ROWS.min(total - offset);
        segments.push(record_batch_to_segment(&rb.slice(offset, len))?);
        offset += len;
    }
    if segments.is_empty() {
        segments.push(record_batch_to_segment(rb)?);
    }
    Ok(segments)
}

/// Encode one RecordBatch as a complete Arrow IPC stream segment.
///
/// # Errors
/// Arrow encode failures.
pub fn record_batch_to_segment(rb: &RecordBatch) -> Result<Bytes> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &rb.schema())?;
        writer.write(rb)?;
        writer.finish()?;
    }
    Ok(Bytes::from(buf))
}

/// Decode an Arrow IPC stream segment back to a RecordBatch.
///
/// # Errors
/// Arrow decode / empty-stream failures.
pub fn segment_to_record_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)?;
    let schema = reader.schema().clone();
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    match batches.len() {
        0 => Err(anyhow!("empty arrow stream segment")),
        1 => Ok(batches.remove(0)),
        _ => Ok(arrow::compute::concat_batches(&schema, &batches)?),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(
                    (0..rows).map(|i| i as f64).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn record_batch_segment_roundtrip_preserves_rows() {
        let batch = make_batch(5);
        let bytes = record_batch_to_segment(&batch).unwrap();
        let back = segment_to_record_batch(&bytes).unwrap();
        assert_eq!(back.num_rows(), 5);
    }

    #[test]
    fn chunk_record_batch_small_batch_is_one_segment() {
        let batch = make_batch(100);
        let segments = chunk_record_batch(&batch).unwrap();
        assert_eq!(segments.len(), 1);
    }

    #[test]
    fn chunk_record_batch_splits_at_boundary() {
        let rows = DATASET_CHUNK_ROWS + 1;
        let batch = make_batch(rows);
        let segments = chunk_record_batch(&batch).unwrap();
        assert_eq!(segments.len(), 2);
    }
}