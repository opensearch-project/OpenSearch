/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Corpus generator: one parquet file + in-memory ground-truth view.
//!
//! Write-once, read-many. The same `Corpus` is shared across many
//! iterations of a fuzz test; each iteration constructs a random tree
//! and evaluates it against the same data.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder, TimestampNanosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::NamedTempFile;

use super::config::{ColumnKind, FixtureConfig};

/// One row of the corpus, kept in memory for the oracle. Mirrors the
/// arrow schema column-by-column. `None` = null.
#[derive(Debug, Clone)]
pub(in crate::indexed_table::tests_e2e) enum CellValue {
    Utf8(Option<String>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float64(Option<f64>),
    Boolean(Option<bool>),
    /// Days since epoch.
    Date32(Option<i32>),
    /// Nanoseconds since epoch.
    TimestampNanos(Option<i64>),
}

/// Corpus = the parquet fixture + everything we need to evaluate
/// queries against it with a row-by-row oracle.
pub(in crate::indexed_table::tests_e2e) struct Corpus {
    pub schema: SchemaRef,
    /// Parquet file(s) on disk, one per segment. Segment 0 holds rows
    /// `[0, segment_row_counts[0])`, segment 1 holds the next slice, etc.
    /// Kept alive for the corpus lifetime.
    pub parquet_files: Vec<NamedTempFile>,
    /// Row count per segment. `segment_row_counts.iter().sum() == num_rows`.
    pub segment_row_counts: Vec<usize>,
    /// Column-major ground-truth data, **global** indexing:
    /// `cells[col_idx][global_row_idx]`.
    pub cells: Vec<Vec<CellValue>>,
    /// Column name → column index. Populated from `schema`.
    pub col_idx: std::collections::HashMap<String, usize>,
    pub config: FixtureConfig,
}

impl Corpus {
    pub fn num_rows(&self) -> usize {
        self.config.num_rows
    }
}

/// Build a corpus from the given config. Deterministic given
/// `config.seed`.
pub(in crate::indexed_table::tests_e2e) fn build_corpus(config: FixtureConfig) -> Corpus {
    let mut rng = StdRng::seed_from_u64(config.seed);
    let schema = build_schema(&config);

    // Column-major: generate cells first (one Vec<CellValue> per column),
    // then build arrow arrays from them so the parquet file and the
    // oracle see *exactly* the same data.
    let mut cells: Vec<Vec<CellValue>> = Vec::with_capacity(config.columns.len());
    // Reserve column 0 for the synthetic __doc_id.
    let doc_id_col: Vec<CellValue> = (0..config.num_rows)
        .map(|i| CellValue::Int32(Some(i as i32)))
        .collect();
    cells.push(doc_id_col);
    for (_, kind) in &config.columns {
        let col = gen_column_cells(&mut rng, *kind, config.num_rows, config.null_pct);
        cells.push(col);
    }

    // Arrow arrays: __doc_id first, then user columns.
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cells.len());
    let doc_id_array = {
        let mut b = Int32Builder::new();
        for v in &cells[0] {
            match v {
                CellValue::Int32(Some(x)) => b.append_value(*x),
                _ => unreachable!("__doc_id must be non-null Int32"),
            }
        }
        Arc::new(b.finish()) as ArrayRef
    };
    arrays.push(doc_id_array);
    for (col, (_, kind)) in cells.iter().skip(1).zip(config.columns.iter()) {
        arrays.push(cells_to_array(col, *kind));
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays).expect("record batch");

    // Split rows across `num_segments` parquet files. Row ranges are
    // contiguous slices of the single big `batch`; the oracle's view
    // stays `cells` in global indexing.
    let num_segments = config.num_segments.max(1);
    let mut segment_row_counts: Vec<usize> = Vec::with_capacity(num_segments);
    let base = config.num_rows / num_segments;
    let rem = config.num_rows % num_segments;
    for i in 0..num_segments {
        segment_row_counts.push(base + if i < rem { 1 } else { 0 });
    }

    let mut parquet_files: Vec<NamedTempFile> = Vec::with_capacity(num_segments);
    let mut start = 0usize;
    for &count in &segment_row_counts {
        let slice = batch.slice(start, count);
        parquet_files.push(write_parquet(
            &slice,
            config.rows_per_row_group,
            config.rows_per_page,
        ));
        start += count;
    }
    debug_assert_eq!(start, config.num_rows);

    let col_idx = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().clone(), i))
        .collect();

    Corpus {
        schema,
        parquet_files,
        segment_row_counts,
        cells,
        col_idx,
        config,
    }
}

fn build_schema(config: &FixtureConfig) -> SchemaRef {
    let mut fields: Vec<Field> = Vec::with_capacity(config.columns.len() + 1);
    // Synthetic doc-id column, always non-null Int32 at column 0. Lets the
    // harness recover row identity from returned record batches.
    fields.push(Field::new("__doc_id", DataType::Int32, false));
    for (name, kind) in &config.columns {
        let dt = match kind {
            ColumnKind::Utf8 { .. } => DataType::Utf8,
            ColumnKind::Int32 { .. } => DataType::Int32,
            ColumnKind::Int64 { .. } => DataType::Int64,
            ColumnKind::Float64 { .. } => DataType::Float64,
            ColumnKind::Boolean => DataType::Boolean,
            ColumnKind::Date32 { .. } => DataType::Date32,
            ColumnKind::TimestampNanos { .. } => {
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None)
            }
        };
        fields.push(Field::new(name, dt, config.null_pct > 0.0));
    }
    Arc::new(Schema::new(fields))
}

/// Generate one column's worth of `CellValue`s. Follows the
/// distinct-pool + take-indices pattern from DataFusion's test-utils:
/// build a pool of distinct values once, pick from it per row. Null
/// probability gates on each pick.
fn gen_column_cells(
    rng: &mut StdRng,
    kind: ColumnKind,
    num_rows: usize,
    null_pct: f64,
) -> Vec<CellValue> {
    match kind {
        ColumnKind::Utf8 {
            num_distinct,
            max_len,
        } => {
            let pool: Vec<String> = (0..num_distinct.max(1))
                .map(|_| random_alphanumeric(rng, max_len))
                .collect();
            (0..num_rows)
                .map(|_| {
                    if rng.gen::<f64>() < null_pct {
                        CellValue::Utf8(None)
                    } else {
                        let i = rng.gen_range(0..pool.len());
                        CellValue::Utf8(Some(pool[i].clone()))
                    }
                })
                .collect()
        }
        ColumnKind::Int32 { min, max } => (0..num_rows)
            .map(|_| {
                if rng.gen::<f64>() < null_pct {
                    CellValue::Int32(None)
                } else {
                    CellValue::Int32(Some(rng.gen_range(min..max)))
                }
            })
            .collect(),
        ColumnKind::Int64 { min, max } => (0..num_rows)
            .map(|_| {
                if rng.gen::<f64>() < null_pct {
                    CellValue::Int64(None)
                } else {
                    CellValue::Int64(Some(rng.gen_range(min..max)))
                }
            })
            .collect(),
        ColumnKind::Float64 { min, max } => (0..num_rows)
            .map(|_| {
                if rng.gen::<f64>() < null_pct {
                    CellValue::Float64(None)
                } else {
                    CellValue::Float64(Some(rng.gen_range(min..max)))
                }
            })
            .collect(),
        ColumnKind::Boolean => (0..num_rows)
            .map(|_| {
                if rng.gen::<f64>() < null_pct {
                    CellValue::Boolean(None)
                } else {
                    CellValue::Boolean(Some(rng.gen()))
                }
            })
            .collect(),
        ColumnKind::Date32 { min, max } => (0..num_rows)
            .map(|_| {
                if rng.gen::<f64>() < null_pct {
                    CellValue::Date32(None)
                } else {
                    CellValue::Date32(Some(rng.gen_range(min..max)))
                }
            })
            .collect(),
        ColumnKind::TimestampNanos { min, max } => (0..num_rows)
            .map(|_| {
                if rng.gen::<f64>() < null_pct {
                    CellValue::TimestampNanos(None)
                } else {
                    CellValue::TimestampNanos(Some(rng.gen_range(min..max)))
                }
            })
            .collect(),
    }
}

fn cells_to_array(col: &[CellValue], kind: ColumnKind) -> ArrayRef {
    match kind {
        ColumnKind::Utf8 { .. } => {
            let mut b = StringBuilder::new();
            for v in col {
                match v {
                    CellValue::Utf8(Some(s)) => b.append_value(s),
                    CellValue::Utf8(None) => b.append_null(),
                    _ => unreachable!("kind/cell mismatch"),
                }
            }
            Arc::new(b.finish())
        }
        ColumnKind::Int32 { .. } => {
            let mut b = Int32Builder::new();
            for v in col {
                match v {
                    CellValue::Int32(Some(x)) => b.append_value(*x),
                    CellValue::Int32(None) => b.append_null(),
                    _ => unreachable!("kind/cell mismatch"),
                }
            }
            Arc::new(b.finish())
        }
        ColumnKind::Int64 { .. } => {
            let mut b = Int64Builder::new();
            for v in col {
                match v {
                    CellValue::Int64(Some(x)) => b.append_value(*x),
                    CellValue::Int64(None) => b.append_null(),
                    _ => unreachable!("kind/cell mismatch"),
                }
            }
            Arc::new(b.finish())
        }
        ColumnKind::Float64 { .. } => {
            let mut b = Float64Builder::new();
            for v in col {
                match v {
                    CellValue::Float64(Some(x)) => b.append_value(*x),
                    CellValue::Float64(None) => b.append_null(),
                    _ => unreachable!("kind/cell mismatch"),
                }
            }
            Arc::new(b.finish())
        }
        ColumnKind::Boolean => {
            let mut b = BooleanBuilder::new();
            for v in col {
                match v {
                    CellValue::Boolean(Some(x)) => b.append_value(*x),
                    CellValue::Boolean(None) => b.append_null(),
                    _ => unreachable!("kind/cell mismatch"),
                }
            }
            Arc::new(b.finish())
        }
        ColumnKind::Date32 { .. } => {
            let mut b = Date32Builder::new();
            for v in col {
                match v {
                    CellValue::Date32(Some(x)) => b.append_value(*x),
                    CellValue::Date32(None) => b.append_null(),
                    _ => unreachable!("kind/cell mismatch"),
                }
            }
            Arc::new(b.finish())
        }
        ColumnKind::TimestampNanos { .. } => {
            let mut b = TimestampNanosecondBuilder::new();
            for v in col {
                match v {
                    CellValue::TimestampNanos(Some(x)) => b.append_value(*x),
                    CellValue::TimestampNanos(None) => b.append_null(),
                    _ => unreachable!("kind/cell mismatch"),
                }
            }
            Arc::new(b.finish())
        }
    }
}

fn write_parquet(
    batch: &RecordBatch,
    rows_per_row_group: usize,
    rows_per_page: usize,
) -> NamedTempFile {
    let tmp = NamedTempFile::new().expect("tempfile");
    let props = WriterProperties::builder()
        .set_max_row_group_size(rows_per_row_group)
        .set_data_page_row_count_limit(rows_per_page)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();
    let mut w = ArrowWriter::try_new(tmp.reopen().expect("reopen"), batch.schema(), Some(props))
        .expect("arrow writer");
    w.write(batch).expect("write");
    w.close().expect("close");
    tmp
}

fn random_alphanumeric(rng: &mut StdRng, max_len: usize) -> String {
    let len = rng.gen_range(1..=max_len.max(1));
    (0..len)
        .map(|_| {
            let c: u32 = rng.gen_range(0..62);
            match c {
                0..=9 => (b'0' + c as u8) as char,
                10..=35 => (b'a' + (c as u8 - 10)) as char,
                _ => (b'A' + (c as u8 - 36)) as char,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corpus_is_deterministic() {
        let cfg = FixtureConfig::small(0xdeadbeef);
        let a = build_corpus(cfg.clone());
        let b = build_corpus(cfg);
        assert_eq!(a.cells.len(), b.cells.len());
        for (col_a, col_b) in a.cells.iter().zip(b.cells.iter()) {
            assert_eq!(col_a.len(), col_b.len());
            // Debug-format match — thorough and works for every variant.
            assert_eq!(format!("{:?}", col_a[0]), format!("{:?}", col_b[0]));
            assert_eq!(
                format!("{:?}", col_a[col_a.len() - 1]),
                format!("{:?}", col_b[col_b.len() - 1])
            );
        }
    }

    #[test]
    fn corpus_produces_multi_rg_parquet() {
        let cfg = FixtureConfig::small(0x1234);
        let corpus = build_corpus(cfg);
        // Load parquet metadata and assert > 1 RG for the small preset
        // (10_000 / 2_048 ≈ 5 RGs).
        let file = std::fs::File::open(corpus.parquet_files[0].path()).unwrap();
        let meta = datafusion::parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
            &file,
            datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions::new()
                .with_page_index(true),
        )
        .unwrap();
        assert!(meta.metadata().num_row_groups() > 1);
    }

    #[test]
    fn corpus_respects_schema() {
        let cfg = FixtureConfig::small(0xfeed);
        let corpus = build_corpus(cfg);
        assert_eq!(corpus.cells.len(), corpus.schema.fields().len());
        assert_eq!(corpus.cells[0].len(), corpus.num_rows());
    }
}
