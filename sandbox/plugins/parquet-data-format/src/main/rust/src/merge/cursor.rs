/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::fs::File;
use std::sync::{Arc, Mutex};

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::schema::types::SchemaDescriptor;

use super::error::{MergeError, MergeResult};
use super::heap::{get_sort_values, SortKey};
use super::io_task::get_merge_pool;
use super::schema::projection_indices_excluding_row_id;

/// A cursor over a single sorted Parquet input file.
///
/// Each cursor reads batches sequentially and prefetches the next batch on the
/// shared Rayon pool to overlap IO with merge computation. When `live_bits` is
/// `Some`, dead rows are skipped so `row_idx` always points at a live row (or the
/// cursor is exhausted).
pub struct FileCursor {
    reader: Arc<Mutex<parquet::arrow::arrow_reader::ParquetRecordBatchReader>>,
    prefetch_rx: std::sync::mpsc::Receiver<Option<MergeResult<RecordBatch>>>,
    prefetch_tx: std::sync::mpsc::SyncSender<Option<MergeResult<RecordBatch>>>,
    prefetch_pending: bool,
    pub current_batch: Option<RecordBatch>,
    pub row_idx: usize,
    pub file_id: usize,
    pub sort_col_indices: Vec<usize>,
    pub sort_col_types: Vec<ArrowDataType>,
    pub nulls_first: Vec<bool>,
    /// Packed live-docs bitset in Lucene `FixedBitSet#getBits()` layout. `None` = all alive.
    live_bits: Option<Arc<Vec<u64>>>,
    /// Total source rows. Bits at indices `>= num_rows` are ignored.
    num_rows: u64,
    /// Source row-id offset at the start of the current batch.
    pub base_row_id: u64,
}

/// Returns `true` if `abs_row_id` is alive given the optional `live_bits` packed bitset.
/// `None` ⇒ all alive; rows beyond `num_rows` or beyond the bitset's length are treated
/// as alive (defensive — Java-side contract is that absent bits mean "all alive").
#[inline]
pub fn is_row_id_alive(live_bits: Option<&[u64]>, num_rows: u64, abs_row_id: u64) -> bool {
    match live_bits {
        None => true,
        Some(bits) => {
            if abs_row_id >= num_rows {
                return true;
            }
            let word = (abs_row_id / 64) as usize;
            let bit = (abs_row_id % 64) as u64;
            match bits.get(word) {
                Some(&w) => (w & (1u64 << bit)) != 0,
                None => true,
            }
        }
    }
}

/// One past the highest live-row index within `[0, batch_upper)` for the batch starting
/// at `base_row_id`. `None` live_bits ⇒ all alive ⇒ returns `batch_upper`. If no row in
/// the range is alive, returns 0.
#[inline]
pub fn last_live_index_plus_one(
    live_bits: Option<&[u64]>,
    num_rows: u64,
    base_row_id: u64,
    batch_upper: usize,
) -> usize {
    match live_bits {
        None => batch_upper,
        Some(_) => {
            let mut i = batch_upper;
            while i > 0 {
                let idx = i - 1;
                if is_row_id_alive(live_bits, num_rows, base_row_id + idx as u64) {
                    return i;
                }
                i -= 1;
            }
            0
        }
    }
}

impl FileCursor {
    /// Returns `true` if the given absolute source row id is alive.
    #[inline]
    pub fn is_row_id_alive(&self, abs_row_id: u64) -> bool {
        is_row_id_alive(self.live_bits.as_deref().map(|v| v.as_slice()), self.num_rows, abs_row_id)
    }

    /// Opens a Parquet file and creates a cursor positioned at the first live row.
    ///
    /// Returns `(cursor, projected_arrow_schema, parquet_schema_descriptor, writer_generation, total_row_count)`
    /// so the caller can build union schemas and row-ID mappings without re-opening the file.
    /// `live_bits` is an optional packed bitset of live rows (Lucene layout).
    pub fn new(
        path: &str,
        file_id: usize,
        sort_columns: &[String],
        nulls_first: &[bool],
        batch_size: usize,
        live_bits: Option<Arc<Vec<u64>>>,
    ) -> MergeResult<(Self, Arc<ArrowSchema>, SchemaDescriptor, i64, usize)> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();
        let writer_generation = crate::writer_properties_builder::read_writer_generation(builder.metadata().file_metadata(), file_id);
        let total_row_count = builder.metadata().file_metadata().num_rows() as usize;

        let mut sort_col_types = Vec::with_capacity(sort_columns.len());
        for col_name in sort_columns {
            let dt = schema
                .fields()
                .iter()
                .find(|f| f.name() == col_name.as_str())
                .map(|f| f.data_type().clone())
                .ok_or_else(|| {
                    MergeError::Logic(format!(
                        "Sort column '{}' not found in file '{}' (cursor {})",
                        col_name, path, file_id
                    ))
                })?;
            sort_col_types.push(dt);
        }

        let parquet_schema_descr = builder.parquet_schema().clone();
        let projection_indices = projection_indices_excluding_row_id(&schema);

        let projection =
            parquet::arrow::ProjectionMask::roots(&parquet_schema_descr, projection_indices);

        let mut reader = builder
            .with_batch_size(batch_size)
            .with_projection(projection)
            .build()?;

        let first_batch = match reader.next() {
            Some(Ok(b)) if b.num_rows() > 0 => b,
            Some(Err(e)) => return Err(e.into()),
            _ => {
                return Err(MergeError::Logic(format!(
                    "File '{}' (cursor {}) yielded no rows despite passing validation",
                    path, file_id
                )));
            }
        };

        let projected_schema = first_batch.schema();

        let mut sort_col_indices = Vec::with_capacity(sort_columns.len());
        for col_name in sort_columns {
            let idx = projected_schema
                .fields()
                .iter()
                .position(|f| f.name() == col_name.as_str())
                .ok_or_else(|| {
                    MergeError::Logic(format!(
                        "Sort column '{}' not found after projection in file '{}'",
                        col_name, path
                    ))
                })?;
            sort_col_indices.push(idx);
        }

        let (prefetch_tx, prefetch_rx) =
            std::sync::mpsc::sync_channel::<Option<MergeResult<RecordBatch>>>(1);

        let reader = Arc::new(Mutex::new(reader));

        let mut cursor = Self {
            reader,
            prefetch_rx,
            prefetch_tx,
            prefetch_pending: false,
            current_batch: Some(first_batch),
            row_idx: 0,
            file_id,
            sort_col_indices,
            sort_col_types,
            nulls_first: nulls_first.to_vec(),
            live_bits,
            num_rows: total_row_count as u64,
            base_row_id: 0,
        };

        cursor.start_prefetch();
        cursor.skip_to_next_live_row()?;

        Ok((cursor, projected_schema, parquet_schema_descr, writer_generation, total_row_count))
    }

    /// Returns `true` if the given absolute source row id is alive.
    #[inline]
    fn is_current_row_alive(&self) -> bool {
        self.is_row_id_alive(self.base_row_id + self.row_idx as u64)
    }

    /// Advances `row_idx` past dead rows, loading successor batches as needed.
    /// After this, either `row_idx` points at a live row or the cursor is exhausted.
    fn skip_to_next_live_row(&mut self) -> MergeResult<()> {
        if self.live_bits.is_none() {
            return Ok(());
        }
        loop {
            let Some(batch) = self.current_batch.as_ref() else {
                return Ok(());
            };
            while self.row_idx < batch.num_rows() && !self.is_current_row_alive() {
                self.row_idx += 1;
            }
            if self.row_idx < batch.num_rows() {
                return Ok(());
            }
            if !self.load_next_batch()? {
                return Ok(());
            }
        }
    }

    fn start_prefetch(&mut self) {
        if self.prefetch_pending {
            return;
        }
        self.prefetch_pending = true;

        let reader = Arc::clone(&self.reader);
        let tx = self.prefetch_tx.clone();

        get_merge_pool(None).spawn(move || {
            let mut reader = reader.lock().unwrap();
            let result = match reader.next() {
                Some(Ok(batch)) if batch.num_rows() > 0 => Some(Ok(batch)),
                Some(Err(e)) => Some(Err(MergeError::Arrow(e))),
                _ => None,
            };
            let _ = tx.send(result);
        });
    }

    pub fn load_next_batch(&mut self) -> MergeResult<bool> {
        let prev_rows = self.current_batch.as_ref().map(|b| b.num_rows() as u64).unwrap_or(0);
        self.current_batch = None;

        match self.prefetch_rx.recv() {
            Ok(Some(Ok(batch))) => {
                self.base_row_id += prev_rows;
                self.current_batch = Some(batch);
                self.row_idx = 0;
                self.prefetch_pending = false;
                self.start_prefetch();
                Ok(true)
            }
            Ok(Some(Err(e))) => {
                self.prefetch_pending = false;
                Err(e)
            }
            Ok(None) | Err(_) => {
                self.prefetch_pending = false;
                Ok(false)
            }
        }
    }

    #[inline]
    pub fn current_sort_values(&self) -> MergeResult<Vec<SortKey>> {
        let batch = self
            .current_batch
            .as_ref()
            .ok_or_else(|| MergeError::Logic("Cursor exhausted".into()))?;
        get_sort_values(batch, self.row_idx, &self.sort_col_indices, &self.sort_col_types, &self.nulls_first)
    }

    /// Returns sort values at the cursor's last live row in the current batch.
    pub fn last_sort_values(&self) -> MergeResult<Vec<SortKey>> {
        let batch = self
            .current_batch
            .as_ref()
            .ok_or_else(|| MergeError::Logic("Cursor exhausted".into()))?;
        let end_excl = self.last_live_index_plus_one(batch.num_rows());
        assert!(end_excl > self.row_idx, "last_sort_values called on cursor with no live row");
        get_sort_values(
            batch,
            end_excl - 1,
            &self.sort_col_indices,
            &self.sort_col_types,
            &self.nulls_first,
        )
    }

    /// One past the highest live-row index within `[0, batch_upper)`.
    #[inline]
    fn last_live_index_plus_one(&self, batch_upper: usize) -> usize {
        last_live_index_plus_one(
            self.live_bits.as_deref().map(|v| v.as_slice()),
            self.num_rows,
            self.base_row_id,
            batch_upper,
        )
    }

    #[inline]
    pub fn batch_height(&self) -> usize {
        self.current_batch.as_ref().map_or(0, |b| b.num_rows())
    }

    /// Returns the sub-batch `[start, start+len)` with dead rows filtered out.
    /// Zero-copy when `live_bits` is `None`.
    pub fn take_live_slice(&self, start: usize, len: usize) -> MergeResult<RecordBatch> {
        let batch = self.current_batch.as_ref().unwrap();
        let slice = batch.slice(start, len);
        match &self.live_bits {
            None => Ok(slice),
            Some(_) => {
                let mut mask_values: Vec<bool> = Vec::with_capacity(len);
                let start_abs = self.base_row_id + start as u64;
                for i in 0..len {
                    mask_values.push(self.is_row_id_alive(start_abs + i as u64));
                }
                let mask = BooleanArray::from(mask_values);
                Ok(filter_record_batch(&slice, &mask)?)
            }
        }
    }

    pub fn advance(&mut self) -> MergeResult<bool> {
        if self.current_batch.is_none() {
            return Ok(false);
        }
        self.row_idx += 1;
        if self.row_idx >= self.current_batch.as_ref().unwrap().num_rows() {
            if !self.load_next_batch()? {
                return Ok(false);
            }
        }
        if self.live_bits.is_some() {
            self.skip_to_next_live_row()?;
            return Ok(self.current_batch.is_some());
        }
        Ok(true)
    }

    pub fn advance_past_batch(&mut self) -> MergeResult<bool> {
        if !self.load_next_batch()? {
            return Ok(false);
        }
        if self.live_bits.is_some() {
            self.skip_to_next_live_row()?;
            return Ok(self.current_batch.is_some());
        }
        Ok(true)
    }
}


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod is_row_id_alive_tests {
    use super::is_row_id_alive;

    #[test]
    fn none_bits_returns_true_for_any_row() {
        assert!(is_row_id_alive(None, 0, 0));
        assert!(is_row_id_alive(None, 100, 0));
        assert!(is_row_id_alive(None, 100, 99));
        assert!(is_row_id_alive(None, 100, 1_000_000));
    }

    #[test]
    fn bit_zero_alive_and_dead() {
        let all_alive: Vec<u64> = vec![!0u64];
        assert!(is_row_id_alive(Some(&all_alive), 64, 0));
        let row_zero_dead: Vec<u64> = vec![!0u64 & !1u64];
        assert!(!is_row_id_alive(Some(&row_zero_dead), 64, 0));
    }

    #[test]
    fn bit_at_word_boundary_63() {
        let bits: Vec<u64> = vec![1u64 << 63];
        assert!(is_row_id_alive(Some(&bits), 64, 63));
        for r in 0..63 {
            assert!(!is_row_id_alive(Some(&bits), 64, r), "row {} should be dead", r);
        }
    }

    #[test]
    fn bit_crosses_word_boundary_64() {
        let bits: Vec<u64> = vec![0u64, 1u64];
        assert!(is_row_id_alive(Some(&bits), 128, 64));
        assert!(!is_row_id_alive(Some(&bits), 128, 63));
        assert!(!is_row_id_alive(Some(&bits), 128, 65));
    }

    #[test]
    fn bit_at_word_boundary_127() {
        let bits: Vec<u64> = vec![0u64, 1u64 << 63];
        assert!(is_row_id_alive(Some(&bits), 128, 127));
        assert!(!is_row_id_alive(Some(&bits), 128, 126));
    }

    #[test]
    fn out_of_bounds_returns_alive_defensively() {
        let bits: Vec<u64> = vec![0u64];
        // num_rows=10 — anything >= 10 must come back alive even though all bits are 0.
        assert!(is_row_id_alive(Some(&bits), 10, 10));
        assert!(is_row_id_alive(Some(&bits), 10, 100));
    }

    #[test]
    fn short_bitmap_treats_missing_words_as_alive() {
        // Bitmap covers only 64 bits, but num_rows says 128.
        let bits: Vec<u64> = vec![0u64];
        // Row 64 lives in word 1, which is missing.
        assert!(is_row_id_alive(Some(&bits), 128, 64));
        // Row 0 is in word 0 and explicitly dead.
        assert!(!is_row_id_alive(Some(&bits), 128, 0));
    }

    #[test]
    fn empty_bitmap_returns_alive_for_all() {
        let bits: Vec<u64> = vec![];
        assert!(is_row_id_alive(Some(&bits), 0, 0));
        assert!(is_row_id_alive(Some(&bits), 100, 0));
    }
}

#[cfg(test)]
mod last_live_index_plus_one_tests {
    use super::last_live_index_plus_one;

    #[test]
    fn none_bits_returns_batch_upper() {
        assert_eq!(last_live_index_plus_one(None, 100, 0, 50), 50);
        assert_eq!(last_live_index_plus_one(None, 100, 25, 25), 25);
    }

    #[test]
    fn empty_batch_returns_zero() {
        let bits: Vec<u64> = vec![!0u64];
        assert_eq!(last_live_index_plus_one(Some(&bits), 64, 0, 0), 0);
    }

    #[test]
    fn all_rows_alive_returns_batch_upper() {
        let bits: Vec<u64> = vec![!0u64];
        assert_eq!(last_live_index_plus_one(Some(&bits), 64, 0, 10), 10);
    }

    #[test]
    fn last_row_dead_returns_index_of_previous_alive() {
        // Rows 0..9 alive, row 10 dead.
        let bits: Vec<u64> = vec![0x3FFu64];
        assert_eq!(last_live_index_plus_one(Some(&bits), 64, 0, 11), 10);
    }

    #[test]
    fn trailing_dead_rows_skipped() {
        // Rows 0..4 alive, rows 5..15 dead.
        let bits: Vec<u64> = vec![0x1Fu64];
        assert_eq!(last_live_index_plus_one(Some(&bits), 64, 0, 16), 5);
    }

    #[test]
    fn all_rows_dead_returns_zero() {
        let bits: Vec<u64> = vec![0u64];
        assert_eq!(last_live_index_plus_one(Some(&bits), 64, 0, 10), 0);
    }

    #[test]
    fn respects_base_row_id_offset() {
        // bit at abs row 10 alive, all others dead.
        let bits: Vec<u64> = vec![1u64 << 10];
        // Cursor is at base_row_id=5, asking about a 10-row batch ⇒ checks abs rows 5..14.
        // Last alive in that range is abs 10 (= local idx 5 in this batch). Result is 5+1=6.
        assert_eq!(last_live_index_plus_one(Some(&bits), 64, 5, 10), 6);
    }
}
