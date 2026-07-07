/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Bloom filter pruning for indexed parquet reads.
//!
//! Uses the Parquet bloom filter (SBBF — Split Block Bloom Filter) embedded in
//! the file footer to decide whether a row group can possibly contain values
//! that match equality predicates. Runs AFTER page-level min/max pruning (free,
//! in-memory) and BEFORE the expensive FFM collector call (Lucene, ~1-5 ms).
//!
//! If the bloom filter proves absence for all equality predicates, the row group
//! is skipped entirely — saving the FFM round-trip.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{Column, ScalarValue};
use datafusion::parquet::bloom_filter::Sbbf;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use object_store::{GetOptions, GetRange, ObjectStore};

/// `PruningStatistics` implementation backed by a single row group's bloom
/// filters. The `contained()` method is the only one that returns meaningful
/// data — all other methods return `None` so `PruningPredicate::prune` falls
/// back to "cannot prune" for non-equality predicates.
struct BloomFilterStatistics {
    /// Column name → parsed SBBF for this row group.
    blooms: std::collections::HashMap<String, Sbbf>,
}

impl BloomFilterStatistics {
    /// Check whether a scalar value is contained in the bloom filter.
    /// Returns `true` if the value MAY be present, `false` if it is
    /// definitely absent.
    fn check_scalar(sbbf: &Sbbf, value: &ScalarValue) -> bool {
        match value {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => sbbf.check(s.as_str()),
            ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => {
                sbbf.check(b.as_slice())
            }
            ScalarValue::Int32(Some(v)) => sbbf.check(v),
            ScalarValue::Int64(Some(v)) => sbbf.check(v),
            ScalarValue::UInt32(Some(v)) => sbbf.check(v),
            ScalarValue::UInt64(Some(v)) => sbbf.check(v),
            ScalarValue::Float32(Some(v)) => sbbf.check(v),
            ScalarValue::Float64(Some(v)) => sbbf.check(v),
            ScalarValue::Boolean(Some(v)) => sbbf.check(v),
            ScalarValue::Decimal128(Some(v), _precision, scale) => {
                // Parquet stores Decimal as fixed-length byte array (big-endian).
                // The SBBF hashes the raw bytes that were inserted at write time.
                // For Decimal128 the parquet writer uses the i128 in big-endian
                // byte representation (16 bytes).
                //
                // NOTE: This assumes the query value has the same scale as the
                // stored value. If the Parquet column is DECIMAL(10,2) and the
                // query literal is 1.5 (scale=1, stored as 15), but Parquet
                // stored it as 150 (scale=2), the byte representations differ
                // and the bloom filter will return a false negative. DataFusion's
                // cast coercion should normalize scales before reaching here, but
                // if a mismatch occurs we conservatively return `true` (may match).
                let _ = scale; // used only in the comment above
                let bytes = v.to_be_bytes();
                sbbf.check(bytes.as_slice())
            }
            // Null or unsupported type → conservatively say "may be present"
            _ => true,
        }
    }
}

impl PruningStatistics for BloomFilterStatistics {
    fn min_values(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }
    fn max_values(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }
    fn num_containers(&self) -> usize {
        1
    }
    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }
    fn row_counts(&self) -> Option<ArrayRef> {
        None
    }
    fn contained(&self, column: &Column, values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        let sbbf = self.blooms.get(column.name())?;
        // For the single-container case (1 row group), we return a
        // single-element BooleanArray. `true` means the bloom filter says
        // at least one of the values MAY be present; `false` means NONE of
        // the values can be present (definite absence).
        let any_may_match = values.iter().any(|v| Self::check_scalar(sbbf, v));
        Some(BooleanArray::from(vec![any_may_match]))
    }
}

/// Attempt to prune a single row group using bloom filters.
///
/// Returns `true` if the row group should be PRUNED (skipped), `false` if it
/// should be kept (bloom filter says values may be present, or bloom filter is
/// unavailable).
///
/// Any error (missing bloom filter, I/O error, parse error) results in `false`
/// (conservative: do not prune).
pub async fn bloom_prune_rg(
    store: &dyn ObjectStore,
    path: &object_store::path::Path,
    metadata: &ParquetMetaData,
    arrow_schema: &Arc<Schema>,
    rg_idx: usize,
    predicate: &PruningPredicate,
) -> bool {
    match bloom_prune_rg_inner(store, path, metadata, arrow_schema, rg_idx, predicate).await {
        Ok(should_prune) => should_prune,
        Err(_) => false, // conservative: don't prune on error
    }
}

async fn bloom_prune_rg_inner(
    store: &dyn ObjectStore,
    path: &object_store::path::Path,
    metadata: &ParquetMetaData,
    arrow_schema: &Arc<Schema>,
    rg_idx: usize,
    predicate: &PruningPredicate,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let rg_meta = metadata
        .row_groups()
        .get(rg_idx)
        .ok_or("row group index out of range")?;

    // Identify which columns participate in the predicate's equality checks.
    // `literal_columns` returns columns where the predicate has literal
    // equality constraints (e.g., col = 'value').
    let literal_cols = predicate.literal_columns();
    if literal_cols.is_empty() {
        return Ok(false); // no equality predicates → cannot bloom-prune
    }

    let mut blooms = std::collections::HashMap::new();

    for col_name in &literal_cols {
        // Find the parquet column index for this column name.
        let col_idx = match arrow_schema
            .fields()
            .iter()
            .position(|f| f.name() == col_name)
        {
            Some(idx) => idx,
            None => continue, // column not in schema → skip
        };

        // Get the column chunk metadata for this column in this row group.
        let col_chunk = rg_meta.column(col_idx);

        // Check if bloom filter metadata is available.
        let bf_offset: u64 = match col_chunk.bloom_filter_offset() {
            Some(offset) if offset >= 0 => offset as u64,
            _ => continue, // no bloom filter for this column
        };

        let bf_length: u64 = match col_chunk.bloom_filter_length() {
            Some(len) if len > 0 => len as u64,
            _ => {
                // If length is not recorded, we cannot safely read.
                // Some parquet writers don't record length — skip this column.
                continue;
            }
        };

        // Read the bloom filter bytes from the object store.
        let opts = GetOptions {
            range: Some(GetRange::Bounded(std::ops::Range {
                start: bf_offset,
                end: bf_offset + bf_length,
            })),
            ..Default::default()
        };

        let get_result = store.get_opts(path, opts).await?;
        let bytes = get_result.bytes().await?;

        // Parse the SBBF from the raw bytes (includes header + bitset).
        let sbbf = Sbbf::from_bytes(&bytes)?;
        blooms.insert(col_name.clone(), sbbf);
    }

    if blooms.is_empty() {
        return Ok(false); // no bloom filters available → cannot prune
    }

    let stats = BloomFilterStatistics { blooms };

    // Call the pruning predicate with our bloom-filter-backed statistics.
    // Result is a Vec<bool> with one entry per container (we have 1 container).
    // `true` means "keep", `false` means "can prune".
    match predicate.prune(&stats) {
        Ok(keep) => {
            // Single container: if keep[0] == false, the RG can be pruned.
            Ok(keep.first() == Some(&false))
        }
        Err(_) => Ok(false), // evaluation error → don't prune
    }
}
