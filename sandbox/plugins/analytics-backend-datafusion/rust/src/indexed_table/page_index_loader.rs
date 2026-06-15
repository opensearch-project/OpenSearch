/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Scoped, lazy page-index loading for the indexed table's page pruner.
//!
//! # Why this exists
//!
//! Footer-only metadata loading (see [`super::parquet_bridge::load_parquet_metadata`])
//! deliberately drops the parquet page index (`ColumnIndex` + `OffsetIndex`)
//! from the cached `ParquetMetaData`, because decoding it for *every* column of
//! *every* row group dominates the native heap on wide schemas (~82% in
//! production profiles) while only a handful of predicate columns are ever
//! pruned on.
//!
//! This module restores page-level pruning while keeping the heavy
//! `ColumnIndex` **scoped to the predicate columns only**. Given a footer-only
//! `ParquetMetaData` and the parquet column indices the query's predicate
//! references, it:
//!
//!   1. Range-reads (per row group) the predicate columns' `ColumnIndex` bytes
//!      plus *every* column's `OffsetIndex` bytes over the object store, on the
//!      IO runtime.
//!   2. Decodes them with arrow-rs's public per-column readers
//!      ([`read_columns_indexes`] / [`read_offset_indexes`], which take a
//!      `&[ColumnChunkMetaData]` slice).
//!   3. Builds full-width per-RG vectors:
//!      - `ColumnIndex`: real entries at predicate-column positions,
//!        [`ColumnIndexMetaData::NONE`] placeholders elsewhere. The page pruner's
//!        `StatisticsConverter` only dereferences predicate positions, so the
//!        placeholders keep absolute `index[rg][parquet_col]` indexing valid and
//!        cost almost nothing.
//!      - `OffsetIndex`: real entries for *all* columns (no placeholders).
//!   4. Rebuilds a `ParquetMetaData` carrying that page index.
//!
//! ## Why the OffsetIndex is kept for all columns (not just predicate columns)
//!
//! The `ColumnIndex` is read only at *prune* time, and only for predicate
//! columns — so scoping it is safe and is where the heap savings come from. The
//! `OffsetIndex` is different: it is also read at *scan* time. When the pruner
//! produces a `RowSelection`, arrow-rs's `InMemoryRowGroup::fetch_ranges`
//! dereferences `offset_index[col].page_locations` for every **projected**
//! column (by absolute index) to compute which page byte ranges to fetch. If a
//! projected column had only an empty placeholder there, the reader would fetch
//! zero page ranges for it and fail with "failed to skip rows, expected N, got
//! 0". This loader runs before the projection is known, so it keeps a real
//! `OffsetIndex` for every column. That is cheap relative to the `ColumnIndex`:
//! fixed-width page offsets/sizes with no per-page string min/max stats.
//!
//! The result is cached by `(file path, predicate-column-set)` so repeated
//! queries over the same columns skip the re-decode.
//!
//! # Memory
//!
//! The augmented metadata retains the (heavy) `ColumnIndex` for the predicate
//! columns only — a few columns instead of all ~hundreds — plus the (light)
//! `OffsetIndex` for all columns. Resident footprint stays close to the
//! footer-only baseline; the scoped cache is bounded (see
//! [`MAX_SCOPED_ENTRIES`]).
//!
//! # Correctness / fallback
//!
//! Any failure (file has no page index, a predicate column lacks an index
//! range, a decode/range error) makes augmentation return `None`. The caller
//! then keeps the footer-only metadata and the page pruner conservatively
//! no-ops (scans the whole RG) — never a wrong result.

use std::ops::Range;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use once_cell::sync::Lazy;

use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::{
    ColumnChunkMetaData, ParquetColumnIndex, ParquetMetaData, ParquetOffsetIndex,
};
use datafusion::parquet::file::page_index::column_index::ColumnIndexMetaData;
use datafusion::parquet::file::page_index::index_reader::{
    read_columns_indexes, read_offset_indexes,
};
use datafusion::parquet::file::reader::{ChunkReader, Length};
use object_store::ObjectStore;
use prost::bytes::{Buf, Bytes};

use arrow::datatypes::SchemaRef;

/// Default byte budget for the scoped page-index cache, used until the caller
/// sets one from the runtime's metadata-cache limit (see
/// [`set_scoped_cache_limit`]). 64 MiB is generous for predicate-column-only
/// page index (a few columns × row groups per file) yet a tiny fraction of the
/// footer-only baseline the page-index strip already buys us.
const DEFAULT_SCOPED_CACHE_LIMIT: usize = 64 * 1024 * 1024;

/// Cache key: object-store path + the sorted set of parquet column indices the
/// page index was built for.
#[derive(Clone, PartialEq, Eq, Hash)]
struct ScopedKey {
    path: String,
    parquet_cols: Vec<usize>,
}

/// One cached entry: the augmented metadata, its decoded size, and a
/// last-used tick for LRU ordering.
struct ScopedEntry {
    meta: Arc<ParquetMetaData>,
    size: usize,
    last_used: u64,
}

/// Byte-bounded LRU over scoped page-index metadata, keyed by
/// `(file, predicate-column-set)`.
///
/// Unlike the previous count-capped map (which silently stopped caching when
/// full, degrading to decode-every-query), this evicts the least-recently-used
/// entries once `used > limit`, so it always serves cached data within a memory
/// budget — mirroring DataFusion's `DefaultFilesMetadataCache`. Entry size is
/// `ParquetMetaData::memory_size()` (dominated by the decoded page index for the
/// predicate columns).
struct ScopedLru {
    map: HashMap<ScopedKey, ScopedEntry>,
    used: usize,
    limit: usize,
    /// Monotonic clock for LRU ordering. Avoids `Instant` (which the workflow
    /// harness forbids) and is fine single-process.
    tick: u64,
    /// Observability counters (cumulative since process start / last reset).
    hits: u64,
    misses: u64,
    evictions: u64,
}

/// Snapshot of scoped page-index cache counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScopedCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
    pub used_bytes: usize,
    pub limit_bytes: usize,
}

impl ScopedLru {
    fn new(limit: usize) -> Self {
        Self {
            map: HashMap::new(),
            used: 0,
            limit,
            tick: 0,
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    fn next_tick(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }

    fn get(&mut self, key: &ScopedKey) -> Option<Arc<ParquetMetaData>> {
        let t = self.next_tick();
        match self.map.get_mut(key) {
            Some(entry) => {
                entry.last_used = t;
                self.hits += 1;
                Some(Arc::clone(&entry.meta))
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }

    fn stats(&self) -> ScopedCacheStats {
        ScopedCacheStats {
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
            entries: self.map.len(),
            used_bytes: self.used,
            limit_bytes: self.limit,
        }
    }

    fn insert(&mut self, key: ScopedKey, meta: Arc<ParquetMetaData>) {
        let size = meta.memory_size();
        // An entry larger than the whole budget can never be retained; skip it
        // rather than evicting everything else for something we'd drop anyway.
        if size > self.limit {
            return;
        }
        let t = self.next_tick();
        if let Some(old) = self.map.insert(
            key,
            ScopedEntry { meta, size, last_used: t },
        ) {
            self.used -= old.size;
        }
        self.used += size;
        self.evict();
    }

    fn evict(&mut self) {
        while self.used > self.limit {
            // Find the least-recently-used key.
            let Some(victim) = self
                .map
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            if let Some(removed) = self.map.remove(&victim) {
                self.used -= removed.size;
                self.evictions += 1;
            }
        }
    }

    fn set_limit(&mut self, limit: usize) {
        self.limit = limit;
        self.evict();
    }
}

/// Process-wide scoped page-index cache.
static SCOPED_CACHE: Lazy<Mutex<ScopedLru>> =
    Lazy::new(|| Mutex::new(ScopedLru::new(DEFAULT_SCOPED_CACHE_LIMIT)));

/// Set the scoped cache's byte budget. Called once per query from the executor
/// with the runtime's configured metadata-cache limit, so the scoped cache
/// tracks the same tuning as the footer metadata cache. Idempotent and cheap;
/// shrinking the limit evicts immediately.
pub fn set_scoped_cache_limit(limit: usize) {
    if limit == 0 {
        return; // ignore unset/zero; keep the existing budget
    }
    if let Ok(mut c) = SCOPED_CACHE.lock() {
        c.set_limit(limit);
    }
}

/// Snapshot of the scoped page-index cache's hit/miss/eviction counters plus
/// current occupancy. For node-stats / observability surfaces and tests.
pub fn scoped_cache_stats() -> ScopedCacheStats {
    SCOPED_CACHE.lock().map(|c| c.stats()).unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn clear_scoped_cache_for_test() {
    if let Ok(mut c) = SCOPED_CACHE.lock() {
        c.map.clear();
        c.used = 0;
        c.tick = 0;
        c.limit = DEFAULT_SCOPED_CACHE_LIMIT;
        c.hits = 0;
        c.misses = 0;
        c.evictions = 0;
    }
}

#[cfg(test)]
pub(crate) fn scoped_cache_len_for_test() -> usize {
    SCOPED_CACHE.lock().map(|c| c.map.len()).unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn set_scoped_cache_limit_for_test(limit: usize) {
    if let Ok(mut c) = SCOPED_CACHE.lock() {
        c.set_limit(limit);
    }
}

#[cfg(test)]
pub(crate) fn scoped_cache_bytes_for_test() -> usize {
    SCOPED_CACHE.lock().map(|c| c.used).unwrap_or(0)
}

/// Reset entries + counters but KEEP the configured limit — for eviction tests
/// that set a tight budget then want clean counters from an empty cache.
#[cfg(test)]
pub(crate) fn clear_counters_keep_limit() {
    if let Ok(mut c) = SCOPED_CACHE.lock() {
        c.map.clear();
        c.used = 0;
        c.tick = 0;
        c.hits = 0;
        c.misses = 0;
        c.evictions = 0;
    }
}

/// A [`ChunkReader`] over an in-memory byte buffer that represents the file
/// region `[base, base + bytes.len())`. The arrow-rs page-index readers call
/// `get_bytes(absolute_offset, len)`; we translate the absolute file offset
/// into the buffer.
struct BufferChunkReader {
    /// Absolute file offset of `bytes[0]`.
    base: u64,
    bytes: Bytes,
}

impl Length for BufferChunkReader {
    fn len(&self) -> u64 {
        self.base + self.bytes.len() as u64
    }
}

impl ChunkReader for BufferChunkReader {
    type T = prost::bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        let rel = self.rel(start, 0)?;
        Ok(self.bytes.slice(rel..).reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let rel = self.rel(start, length)?;
        Ok(self.bytes.slice(rel..rel + length))
    }
}

impl BufferChunkReader {
    /// Translate an absolute file offset (and a length to validate) into a
    /// buffer-relative start, erroring if it falls outside the prefetched span.
    fn rel(&self, start: u64, length: usize) -> ParquetResult<usize> {
        let rel = start.checked_sub(self.base).ok_or_else(|| {
            ParquetError::General(format!(
                "page-index read offset {} precedes buffer base {}",
                start, self.base
            ))
        })?;
        let rel = usize::try_from(rel)
            .map_err(|e| ParquetError::General(format!("offset overflow: {}", e)))?;
        if rel + length > self.bytes.len() {
            return Err(ParquetError::General(format!(
                "page-index read [{}..{}) exceeds buffer of len {}",
                rel,
                rel + length,
                self.bytes.len()
            )));
        }
        Ok(rel)
    }
}

/// Union of `column_index` byte ranges across the given column chunks. `None`
/// if any chunk lacks a column index (we require all predicate columns to have
/// one, else we fall back to footer-only).
fn column_index_union(chunks: &[ColumnChunkMetaData]) -> Option<Range<u64>> {
    range_union(chunks, |c| {
        let off = u64::try_from(c.column_index_offset()?).ok()?;
        let len = u64::try_from(c.column_index_length()?).ok()?;
        Some(off..off + len)
    })
}

/// Union of `offset_index` byte ranges across the given column chunks.
fn offset_index_union(chunks: &[ColumnChunkMetaData]) -> Option<Range<u64>> {
    range_union(chunks, |c| {
        let off = u64::try_from(c.offset_index_offset()?).ok()?;
        let len = u64::try_from(c.offset_index_length()?).ok()?;
        Some(off..off + len)
    })
}

fn range_union(
    chunks: &[ColumnChunkMetaData],
    f: impl Fn(&ColumnChunkMetaData) -> Option<Range<u64>>,
) -> Option<Range<u64>> {
    let mut acc: Option<Range<u64>> = None;
    for c in chunks {
        let r = f(c)?; // any missing range → bail (caller falls back)
        acc = Some(match acc {
            None => r,
            Some(a) => a.start.min(r.start)..a.end.max(r.end),
        });
    }
    acc
}

/// Map the query's arrow predicate-column names to this file's parquet column
/// indices, using the same resolution `PagePruner` uses
/// (`StatisticsConverter::parquet_column_index`). Columns absent from the
/// parquet file (schema evolution) are skipped. Returns a sorted, deduped set.
pub fn resolve_predicate_parquet_columns(
    arrow_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    predicate_column_names: &[String],
) -> Vec<usize> {
    let parquet_schema = metadata.file_metadata().schema_descr();
    let mut set = std::collections::BTreeSet::new();
    for name in predicate_column_names {
        if let Ok(conv) = StatisticsConverter::try_new(name, arrow_schema, parquet_schema) {
            if let Some(idx) = conv.parquet_column_index() {
                set.insert(idx);
            }
        }
    }
    set.into_iter().collect()
}

/// Build metadata carrying page index for `parquet_cols` only, fetching their
/// `ColumnIndex`/`OffsetIndex` bytes over `store`. Returns `None` (caller keeps
/// footer-only metadata) on any condition that would make page pruning unsafe
/// or impossible: empty column set, file without a page index, or a decode/IO
/// error.
///
/// Consults and populates the scoped `(file, column-set)` cache.
pub async fn load_scoped_page_index(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    parquet_cols: &[usize],
) -> Option<Arc<ParquetMetaData>> {
    if parquet_cols.is_empty() {
        return None;
    }

    let key = ScopedKey {
        path: location.as_ref().to_string(),
        parquet_cols: parquet_cols.to_vec(),
    };
    // Cache hit: return the already-decoded scoped page index, no I/O, no decode.
    if let Ok(mut cache) = SCOPED_CACHE.lock() {
        if let Some(hit) = cache.get(&key) {
            return Some(hit);
        }
    }

    let augmented = build_augmented_metadata(store, location, footer_meta, parquet_cols).await?;

    // Publish to the byte-bounded LRU (evicts least-recently-used over budget).
    if let Ok(mut cache) = SCOPED_CACHE.lock() {
        cache.insert(key, Arc::clone(&augmented));
    }
    Some(augmented)
}

async fn build_augmented_metadata(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    parquet_cols: &[usize],
) -> Option<Arc<ParquetMetaData>> {
    let num_rgs = footer_meta.num_row_groups();
    if num_rgs == 0 {
        return None;
    }
    let num_cols = footer_meta.file_metadata().schema_descr().num_columns();
    // Predicate indices must be in range for every RG (they index into the
    // file schema, shared across RGs).
    if parquet_cols.iter().any(|&i| i >= num_cols) {
        return None;
    }

    // Phase 1: per RG, gather two things and compute their union byte ranges for
    // a single vectored fetch. Bail to footer-only if any required index range is
    // missing.
    //
    //  - ColumnIndex for the PREDICATE columns only. This is the heavy, string
    //    min/max-laden structure the page pruner reads, and the pruner only ever
    //    dereferences predicate positions (placeholders fill the rest), so
    //    scoping it to predicate columns is exactly what bounds the heap.
    //
    //  - OffsetIndex for ALL columns. Unlike the ColumnIndex, the OffsetIndex is
    //    consumed at READ time, not just at prune time: when a RowSelection is
    //    active, arrow-rs's `InMemoryRowGroup::fetch_ranges` dereferences
    //    `offset_index[col].page_locations` for every *projected* column by
    //    absolute index to locate the pages to fetch. A missing/empty entry for a
    //    projected column yields zero fetched page ranges and the read fails
    //    ("failed to skip rows, expected N, got 0"). Since this loader runs before
    //    we know the query's projection, we keep a real OffsetIndex for every
    //    column. It is cheap relative to the ColumnIndex — fixed-width page
    //    offsets/sizes, no per-page string stats (~a third of the page index, and
    //    a small fraction of the predicate-column ColumnIndex on wide string
    //    schemas).
    struct RgPlan {
        pred_chunks: Vec<ColumnChunkMetaData>,
        all_chunks: Vec<ColumnChunkMetaData>,
        col_range: Range<u64>,
        off_range: Range<u64>,
    }
    let mut plans: Vec<RgPlan> = Vec::with_capacity(num_rgs);
    // Flat list of ranges for a single vectored fetch: [rg0_col, rg0_off, rg1_col, ...].
    let mut fetch_ranges: Vec<Range<u64>> = Vec::with_capacity(num_rgs * 2);
    for rg_idx in 0..num_rgs {
        let rg = footer_meta.row_group(rg_idx);
        let pred_chunks: Vec<ColumnChunkMetaData> =
            parquet_cols.iter().map(|&i| rg.column(i).clone()).collect();
        let all_chunks: Vec<ColumnChunkMetaData> =
            (0..num_cols).map(|i| rg.column(i).clone()).collect();
        let col_range = column_index_union(&pred_chunks)?;
        // OffsetIndex range spans every column (see note above). If any column
        // lacks an offset index we bail to footer-only: the reader would
        // otherwise take the offset-index branch and fail on the missing column.
        let off_range = offset_index_union(&all_chunks)?;
        fetch_ranges.push(col_range.clone());
        fetch_ranges.push(off_range.clone());
        plans.push(RgPlan {
            pred_chunks,
            all_chunks,
            col_range,
            off_range,
        });
    }

    // Phase 2: one vectored fetch of all index byte ranges (on the IO runtime
    // via the SpawnIoStore-wrapped object store).
    let buffers = store.get_ranges(location, &fetch_ranges).await.ok()?;
    if buffers.len() != fetch_ranges.len() {
        return None;
    }

    // Phase 3: per RG, decode the predicate columns and scatter into full-width
    // vectors.
    let mut column_index: ParquetColumnIndex = Vec::with_capacity(num_rgs);
    let mut offset_index: ParquetOffsetIndex = Vec::with_capacity(num_rgs);

    for (rg_idx, plan) in plans.iter().enumerate() {
        let col_buf = buffers[rg_idx * 2].clone();
        let off_buf = buffers[rg_idx * 2 + 1].clone();

        let col_reader = BufferChunkReader {
            base: plan.col_range.start,
            bytes: col_buf,
        };
        let off_reader = BufferChunkReader {
            base: plan.off_range.start,
            bytes: off_buf,
        };

        // `read_columns_indexes` / `read_offset_indexes` are deprecated in
        // arrow-rs (slated for removal) but are the only PUBLIC API that decodes
        // a *column subset*; the per-column primitives are `pub(crate)`. They
        // return a Vec aligned to the chunk slice we pass.
        //
        // ColumnIndex: predicate columns only (the part we scope to bound the heap).
        // OffsetIndex: every column (needed by the reader for projected columns —
        // see the Phase 1 note), so decode against the full chunk list.
        #[allow(deprecated)]
        let decoded_cols = read_columns_indexes(&col_reader, &plan.pred_chunks).ok()??;
        #[allow(deprecated)]
        let decoded_offs = read_offset_indexes(&off_reader, &plan.all_chunks).ok()??;
        if decoded_cols.len() != parquet_cols.len() || decoded_offs.len() != num_cols {
            return None;
        }

        // ColumnIndex row: NONE placeholders everywhere, real entries at predicate
        // positions. `StatisticsConverter` only ever dereferences the predicate
        // positions, so placeholders keep absolute indexing valid and are cheap.
        let mut col_row: Vec<ColumnIndexMetaData> =
            (0..num_cols).map(|_| ColumnIndexMetaData::NONE).collect();
        for (k, &parquet_col) in parquet_cols.iter().enumerate() {
            col_row[parquet_col] = decoded_cols[k].clone();
        }
        column_index.push(col_row);
        // OffsetIndex row: real entries for every column, already aligned to
        // absolute column order by `all_chunks`.
        offset_index.push(decoded_offs);
    }

    // Phase 4: rebuild metadata with the sparse page index grafted on.
    let base = ParquetMetaData::clone(footer_meta);
    let rebuilt = base
        .into_builder()
        .set_column_index(Some(column_index))
        .set_offset_index(Some(offset_index))
        .build();
    Some(Arc::new(rebuilt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::page_pruner::{build_pruning_predicate, PagePruner};
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::arrow::arrow_reader::RowSelection;
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::common::ScalarValue;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjPath;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    /// Two int columns (`price`, `qty`), one row group, four 8-row data pages.
    /// Returns the raw parquet bytes.
    fn two_col_parquet() -> (Bytes, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int32, false),
            Field::new("qty", DataType::Int32, false),
        ]));
        let prices: Vec<i32> = (0..32).collect();
        let qtys: Vec<i32> = (100..132).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(prices)),
                Arc::new(Int32Array::from(qtys)),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(32)
            .set_data_page_row_count_limit(8)
            .set_write_batch_size(8)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut buf: Vec<u8> = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        (Bytes::from(buf), schema)
    }

    async fn stage(bytes: Bytes) -> (Arc<dyn ObjectStore>, ObjPath) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let loc = ObjPath::from("data.parquet");
        store.put(&loc, PutPayload::from_bytes(bytes)).await.unwrap();
        (store, loc)
    }

    /// Decode footer-only metadata (no page index) from the staged bytes.
    fn footer_only(bytes: &Bytes) -> Arc<ParquetMetaData> {
        let meta = ArrowReaderMetadata::load(
            &bytes.clone(),
            ArrowReaderOptions::new().with_page_index(false),
        )
        .unwrap();
        meta.metadata().clone()
    }

    /// Full page index baseline (what we want the scoped path to match).
    fn full_index(bytes: &Bytes) -> Arc<ParquetMetaData> {
        let meta = ArrowReaderMetadata::load(
            &bytes.clone(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
        meta.metadata().clone()
    }

    fn col(name: &str, idx: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(PhysColumn::new(name, idx))
    }
    fn lit_int(v: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Int32(Some(v))))
    }
    fn pred(name: &str, idx: usize, op: Operator, v: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(col(name, idx), op, lit_int(v)))
    }
    fn kept(sel: &RowSelection) -> usize {
        sel.iter().filter(|s| !s.skip).map(|s| s.row_count).sum()
    }

    #[tokio::test]
    async fn footer_only_has_no_page_index() {
        let (bytes, _schema) = two_col_parquet();
        let fo = footer_only(&bytes);
        assert!(
            fo.column_index().is_none() && fo.offset_index().is_none(),
            "fixture footer-only metadata must lack a page index"
        );
        // And the full baseline DOES have one (else the test is vacuous).
        let full = full_index(&bytes);
        assert!(full.column_index().is_some() && full.offset_index().is_some());
    }

    /// The crux: scoped augmentation of footer-only metadata must reproduce the
    /// EXACT pruning result of the full page index, for the predicate column.
    #[tokio::test]
    async fn scoped_augmentation_matches_full_index_pruning() {
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);

        // Predicate references `price` (parquet col 0). price page ranges are
        // 0..8, 8..16, 16..24, 24..32 — `price >= 20` keeps pages 3 and 4.
        let parquet_cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        assert_eq!(parquet_cols, vec![0]);

        let augmented = load_scoped_page_index(&store, &loc, &fo, &parquet_cols)
            .await
            .expect("augmentation must succeed for a file with a page index");
        assert!(
            augmented.column_index().is_some() && augmented.offset_index().is_some(),
            "augmented metadata must carry a page index"
        );

        let predicate = pred("price", 0, Operator::GtEq, 20);
        let pp = build_pruning_predicate(&predicate, schema.clone()).unwrap();

        let scoped_pruner = PagePruner::new(&schema, Arc::clone(&augmented));
        let full_pruner = PagePruner::new(&schema, full_index(&bytes));

        let scoped_sel = scoped_pruner.prune_rg(&pp, 0, None).unwrap();
        let full_sel = full_pruner.prune_rg(&pp, 0, None).unwrap();

        assert_eq!(
            kept(&scoped_sel),
            kept(&full_sel),
            "scoped page index must prune identically to the full index"
        );
        // Sanity: `price >= 20` keeps the last two 8-row pages (rows 16..32).
        assert_eq!(kept(&full_sel), 16);
        assert_eq!(kept(&scoped_sel), 16);
    }

    /// Read the rows kept by a `RowSelection` for a single projected leaf column,
    /// using the given metadata. Returns the decoded i32 values (in order) or the
    /// reader error. This drives arrow-rs's `InMemoryRowGroup::fetch_ranges` down
    /// the offset-index branch (selection present), which dereferences
    /// `offset_index[projected_col].page_locations` by absolute index — exactly
    /// the path that breaks when a projected column carries only an empty
    /// placeholder offset index.
    fn read_selected_column(
        bytes: &Bytes,
        meta: &Arc<ParquetMetaData>,
        leaf_col: usize,
        selection: RowSelection,
    ) -> std::result::Result<Vec<i32>, String> {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use datafusion::parquet::arrow::ProjectionMask;

        let arm = ArrowReaderMetadata::try_new(Arc::clone(meta), ArrowReaderOptions::new())
            .map_err(|e| format!("try_new metadata: {e}"))?;
        let builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(bytes.clone(), arm);
        let proj = ProjectionMask::leaves(builder.parquet_schema(), [leaf_col]);
        let mut reader = builder
            .with_row_groups(vec![0])
            .with_projection(proj)
            .with_row_selection(selection)
            .build()
            .map_err(|e| format!("build reader: {e}"))?;

        let mut out = Vec::new();
        while let Some(next) = reader.next() {
            let batch = next.map_err(|e| format!("read batch: {e}"))?;
            let a = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("projected column was not Int32")?;
            for i in 0..a.len() {
                out.push(a.value(i));
            }
        }
        Ok(out)
    }

    /// Regression: the augmented metadata is fed not only to the page pruner but
    /// to the actual parquet data reader (see `parquet_bridge::bridge_config` →
    /// `CachedMetadataReader`). When the pruner yields a `RowSelection` and the
    /// query projects a column that is NOT a predicate column, arrow-rs locates
    /// the pages to fetch via that column's `OffsetIndex`. If we had scoped the
    /// OffsetIndex to predicate columns only (empty placeholder elsewhere), the
    /// reader would fetch zero page ranges for the projected column and fail
    /// ("failed to skip rows, expected N, got 0"). Keeping a real OffsetIndex for
    /// every column makes this read succeed and match the full-index read.
    ///
    /// Predicate is on `price` (col 0); projection is `qty` (col 1) — the
    /// non-predicate column. The selection keeps rows 16..32.
    #[tokio::test]
    async fn scoped_index_reads_non_predicate_projected_column() {
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);

        // Scope to the predicate column `price` (col 0) only — `qty` (col 1) is
        // NOT in the predicate set, so its ColumnIndex is a placeholder. Its
        // OffsetIndex, however, must be real for the read below to work.
        let parquet_cols =
            resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        assert_eq!(parquet_cols, vec![0]);

        let augmented = load_scoped_page_index(&store, &loc, &fo, &parquet_cols)
            .await
            .expect("augmentation must succeed");

        // The augmented OffsetIndex must carry real page locations for the
        // non-predicate column too, not an empty placeholder.
        let oi = augmented.offset_index().expect("augmented has offset index");
        assert!(
            !oi[0][1].page_locations.is_empty(),
            "non-predicate column (qty) must keep a real offset index, not an empty placeholder"
        );

        // `price >= 20` keeps the last two 8-row pages → rows 16..32.
        let selection = RowSelection::from(vec![
            datafusion::parquet::arrow::arrow_reader::RowSelector::skip(16),
            datafusion::parquet::arrow::arrow_reader::RowSelector::select(16),
        ]);

        // Read `qty` (col 1) through the scoped/augmented metadata.
        let scoped_vals = read_selected_column(&bytes, &augmented, 1, selection.clone())
            .expect("reading a non-predicate projected column must succeed with scoped metadata");

        // Ground truth: the same read through the full page index.
        let full = full_index(&bytes);
        let full_vals = read_selected_column(&bytes, &full, 1, selection)
            .expect("full-index read must succeed");

        // qty values are 100..132, so rows 16..32 are 116..132.
        let expected: Vec<i32> = (116..132).collect();
        assert_eq!(scoped_vals, expected, "scoped read returned wrong qty values");
        assert_eq!(
            scoped_vals, full_vals,
            "scoped read must match the full-index read exactly"
        );
    }

    #[tokio::test]
    async fn empty_column_set_returns_none() {
        clear_scoped_cache_for_test();
        let (bytes, _schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        assert!(load_scoped_page_index(&store, &loc, &fo, &[]).await.is_none());
    }

    // The scoped cache is process-global; the lib test binary runs tests in
    // parallel, so cache-population tests serialize on this mutex (and clear
    // under it) to keep their (file, column-set) keys from colliding / racing
    // with siblings. Distinct fixtures alone aren't enough because the InMemory
    // path is always "data.parquet".
    static CACHE_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[tokio::test]
    async fn second_load_is_cache_hit() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);

        let first = load_scoped_page_index(&store, &loc, &fo, &cols).await.unwrap();
        assert_eq!(scoped_cache_len_for_test(), 1);
        let second = load_scoped_page_index(&store, &loc, &fo, &cols).await.unwrap();
        assert!(
            Arc::ptr_eq(&first, &second),
            "second load with same (file, column-set) must hit the scoped cache"
        );
        assert_eq!(scoped_cache_len_for_test(), 1);
    }

    /// Different predicate-column sets over the same file are cached
    /// independently, and each scoped index prunes correctly for ITS OWN
    /// predicate — which is the only way the pruner uses them in production
    /// (one cache entry per query's predicate-column set).
    #[tokio::test]
    async fn distinct_column_sets_cached_independently() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);

        let c_price = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let c_qty = resolve_predicate_parquet_columns(&schema, &fo, &["qty".to_string()]);
        assert_eq!(c_price, vec![0]);
        assert_eq!(c_qty, vec![1]);

        let a_price = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let a_qty = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap();
        assert_eq!(scoped_cache_len_for_test(), 2);
        assert!(
            !Arc::ptr_eq(&a_price, &a_qty),
            "distinct column-sets must produce distinct cache entries"
        );

        // qty-scoped index prunes a qty predicate: `qty >= 120` (qty pages
        // 100..108,108..116,116..124,124..132) keeps the last two pages → 16.
        let pp_qty =
            build_pruning_predicate(&pred("qty", 1, Operator::GtEq, 120), schema.clone()).unwrap();
        let sel_qty = PagePruner::new(&schema, Arc::clone(&a_qty))
            .prune_rg(&pp_qty, 0, None)
            .unwrap();
        assert_eq!(kept(&sel_qty), 16);

        // price-scoped index prunes a price predicate: `price >= 20` keeps the
        // last two 8-row pages → 16.
        let pp_price =
            build_pruning_predicate(&pred("price", 0, Operator::GtEq, 20), schema.clone()).unwrap();
        let sel_price = PagePruner::new(&schema, Arc::clone(&a_price))
            .prune_rg(&pp_price, 0, None)
            .unwrap();
        assert_eq!(kept(&sel_price), 16);
    }

    /// Byte-bounded LRU: with a tiny limit the cache holds at most a couple of
    /// entries and evicts the least-recently-used, never exceeding the budget —
    /// it does NOT silently stop caching (the old count-cap bug).
    #[tokio::test]
    async fn lru_evicts_over_byte_budget() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();

        // Three distinct column-sets over the same file → three would-be entries.
        // (price), (qty), (price,qty).
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let c_price = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let c_qty = resolve_predicate_parquet_columns(&schema, &fo, &["qty".to_string()]);
        let c_both =
            resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string(), "qty".to_string()]);

        // Size one entry, then set the budget to ~1.5 entries so only one fits.
        let one = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let one_size = one.memory_size();
        set_scoped_cache_limit_for_test(one_size + one_size / 2);

        // Re-clear so the budget applies from empty, keeping the new limit.
        if let Ok(mut c) = SCOPED_CACHE.lock() {
            c.map.clear();
            c.used = 0;
        }

        // Insert three distinct entries; each is ~one_size, budget holds ~1.
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap();
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_both).await.unwrap();

        // Never exceeds the budget, and didn't degrade to "cache nothing".
        assert!(
            scoped_cache_bytes_for_test() <= one_size + one_size / 2,
            "cache bytes {} must stay within budget {}",
            scoped_cache_bytes_for_test(),
            one_size + one_size / 2
        );
        assert!(
            scoped_cache_len_for_test() >= 1,
            "LRU must retain at least the most-recent entry, not stop caching"
        );

        // The most-recently-used (c_both) must still be a hit (same Arc on reload).
        let again = load_scoped_page_index(&store, &loc, &fo, &c_both).await.unwrap();
        let again2 = load_scoped_page_index(&store, &loc, &fo, &c_both).await.unwrap();
        assert!(
            Arc::ptr_eq(&again, &again2),
            "most-recently-used entry must remain cached"
        );

        clear_scoped_cache_for_test(); // restore default limit for other tests
    }

    /// Hit/miss accounting: first load of a (file, column-set) is a miss; the
    /// next is a hit. A different column-set is another miss. Counters reflect
    /// exactly that.
    #[tokio::test]
    async fn stats_count_hits_and_misses() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();

        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let c_price = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let c_qty = resolve_predicate_parquet_columns(&schema, &fo, &["qty".to_string()]);

        // 1st price load → miss (and populates).
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let s = scoped_cache_stats();
        assert_eq!((s.hits, s.misses), (0, 1), "first load must be a miss");

        // 2nd price load → hit.
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let s = scoped_cache_stats();
        assert_eq!((s.hits, s.misses), (1, 1), "second same-key load must be a hit");

        // qty load → another miss (distinct column-set).
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap();
        let s = scoped_cache_stats();
        assert_eq!((s.hits, s.misses), (1, 2));

        // Two more price + qty loads → two more hits.
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap();
        let s = scoped_cache_stats();
        assert_eq!((s.hits, s.misses), (3, 2));
        assert_eq!(s.entries, 2);
        assert_eq!(s.evictions, 0, "budget is ample; nothing should evict");

        clear_scoped_cache_for_test();
    }

    /// Eviction counter increments when entries are pushed out under a tight
    /// budget, and a subsequent reload of an evicted key is a miss again.
    #[tokio::test]
    async fn stats_count_evictions() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();

        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let c_price = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let c_qty = resolve_predicate_parquet_columns(&schema, &fo, &["qty".to_string()]);

        let one = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let one_size = one.memory_size();
        // Budget that fits only ~1 entry.
        set_scoped_cache_limit_for_test(one_size + one_size / 2);
        clear_counters_keep_limit();

        // price then qty: inserting qty over budget evicts price.
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap();
        let s = scoped_cache_stats();
        assert!(s.evictions >= 1, "tight budget must have evicted at least once");
        assert!(s.used_bytes <= s.limit_bytes, "must stay within budget");

        clear_scoped_cache_for_test();
    }

    /// A warm cache hit must be dramatically faster than a cold decode (which
    /// does range I/O + thrift decode + scatter). Asserts a hit is at least 5×
    /// faster than the miss on the same key — a loose bound that still catches a
    /// regression where the "hit" path accidentally re-decodes.
    #[tokio::test]
    async fn cache_hit_is_faster_than_cold_decode() {
        use std::time::Instant;
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();

        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);

        // Cold: miss → range-read + decode + scatter.
        let t0 = Instant::now();
        let _ = load_scoped_page_index(&store, &loc, &fo, &cols).await.unwrap();
        let cold = t0.elapsed();
        assert_eq!(scoped_cache_stats().misses, 1);

        // Warm: average several hits to reduce timer noise on a fast op.
        const N: u32 = 50;
        let t1 = Instant::now();
        for _ in 0..N {
            let _ = load_scoped_page_index(&store, &loc, &fo, &cols).await.unwrap();
        }
        let warm_avg = t1.elapsed() / N;
        let s = scoped_cache_stats();
        assert_eq!(s.misses, 1, "no further misses");
        assert_eq!(s.hits, N as u64);

        eprintln!(
            "scoped cache: cold decode = {:?}, warm hit (avg of {}) = {:?} ({}x faster)",
            cold, N, warm_avg,
            if warm_avg.as_nanos() > 0 { cold.as_nanos() / warm_avg.as_nanos() } else { u128::MAX }
        );
        // Loose bound: a hit must be at least 5× faster than the cold path.
        // (In practice it's 100s–1000s×; 5× tolerates CI jitter while still
        // failing loudly if a "hit" secretly re-decodes.)
        assert!(
            warm_avg * 5 < cold,
            "cache hit ({:?}) not materially faster than cold decode ({:?}) — is the hit path re-decoding?",
            warm_avg, cold
        );

        clear_scoped_cache_for_test();
    }
}

/// Tests that run against **real production parquet files**, gated behind the
/// `OPENSEARCH_REAL_PARQUET_DIR` env var so they never run in CI or the normal
/// `cargo test` sweep. Point the var at a directory of `.parquet` files:
///
/// ```sh
/// OPENSEARCH_REAL_PARQUET_DIR=/path/to/parquet \
///   cargo test -p opensearch-datafusion --lib real_parquet -- --nocapture --test-threads=1
/// ```
///
/// These verify the scoped page-index logic against ground truth: for each file
/// and a chosen predicate column, our scoped (single-column) page index must
/// drive `PagePruner` to the EXACT same per-row-group selection as a full
/// page-index decode — on real wide schemas with real page layouts.
#[cfg(test)]
mod real_parquet {
    use super::*;
    use crate::indexed_table::page_pruner::{build_pruning_predicate, PagePruner};
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
    use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
    use datafusion::parquet::arrow::parquet_to_arrow_schema;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
    use datafusion::physical_expr::PhysicalExpr;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjPath;
    use object_store::{ObjectStore, ObjectStoreExt};

    fn dir_from_env() -> Option<std::path::PathBuf> {
        std::env::var_os("OPENSEARCH_REAL_PARQUET_DIR").map(std::path::PathBuf::from)
    }

    fn list_parquet(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut v: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
            .collect();
        // Smallest first — keeps the default run fast; the big 3 GB files are
        // exercised only if you let it run to completion.
        v.sort_by_key(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(u64::MAX));
        v
    }

    /// Load full-page-index metadata for a local file (ground truth).
    fn full_index_meta(path: &std::path::Path) -> Arc<ParquetMetaData> {
        let file = std::fs::File::open(path).unwrap();
        let meta = ArrowReaderMetadata::load(
            &file,
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
        meta.metadata().clone()
    }

    /// Load footer-only metadata for a local file.
    fn footer_only_meta(path: &std::path::Path) -> Arc<ParquetMetaData> {
        let file = std::fs::File::open(path).unwrap();
        let meta = ArrowReaderMetadata::load(
            &file,
            ArrowReaderOptions::new().with_page_index(false),
        )
        .unwrap();
        meta.metadata().clone()
    }

    fn arrow_schema(meta: &ParquetMetaData) -> SchemaRef {
        let fm = meta.file_metadata();
        Arc::new(parquet_to_arrow_schema(fm.schema_descr(), fm.key_value_metadata()).unwrap())
    }

    /// Pick a predicate column that's actually prunable AND derive a threshold
    /// near the column's real maximum so the predicate forces genuine page
    /// skips (otherwise a trivial `>= 0` would keep every page and the
    /// equivalence check wouldn't exercise the decode/scatter path).
    ///
    /// Uses the FULL page index's per-page stats to find a high value, so the
    /// `>= threshold` predicate prunes most pages. Returns
    /// (arrow_field_name, parquet_col_idx, threshold).
    fn pick_prunable_column(
        full_meta: &ParquetMetaData,
        schema: &SchemaRef,
    ) -> Option<(String, usize, ScalarValue)> {
        pick_prunable_columns(full_meta, schema, 1).into_iter().next()
    }

    /// Find up to `n` distinct prunable numeric columns, each with a threshold
    /// near its real per-page max (so `>= threshold` forces genuine page skips).
    /// Returns `(arrow_field_name, parquet_col_idx, threshold)` per column.
    fn pick_prunable_columns(
        full_meta: &ParquetMetaData,
        schema: &SchemaRef,
        n: usize,
    ) -> Vec<(String, usize, ScalarValue)> {
        let Some(rg0) = full_meta.row_groups().first() else { return vec![] };
        let (Some(col_idx), Some(off_idx)) = (full_meta.column_index(), full_meta.offset_index())
        else {
            return vec![];
        };
        let mut out = Vec::new();
        for field in schema.fields() {
            if out.len() >= n {
                break;
            }
            let name = field.name();
            let Ok(conv) = StatisticsConverter::try_new(
                name,
                schema,
                full_meta.file_metadata().schema_descr(),
            ) else { continue };
            let Some(pq_idx) = conv.parquet_column_index() else { continue };
            let col = rg0.column(pq_idx);
            if col.column_index_offset().is_none() || col.offset_index_offset().is_none() {
                continue;
            }
            // Per-page maxes for RG0; pick the overall max so `>= max` keeps only
            // the page(s) reaching it and skips the rest — a real, selective prune.
            let Ok(maxes) = conv.data_page_maxes(col_idx, off_idx, [&0usize]) else { continue };
            let threshold = match field.data_type() {
                DataType::Int8 | DataType::Int16 | DataType::Int32 => {
                    let Some(a) = maxes.as_any().downcast_ref::<datafusion::arrow::array::Int32Array>() else { continue };
                    let Some(m) = (0..a.len()).filter(|&i| a.is_valid(i)).map(|i| a.value(i)).max() else { continue };
                    ScalarValue::Int32(Some(m))
                }
                DataType::Int64 => {
                    let Some(a) = maxes.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() else { continue };
                    let Some(m) = (0..a.len()).filter(|&i| a.is_valid(i)).map(|i| a.value(i)).max() else { continue };
                    ScalarValue::Int64(Some(m))
                }
                DataType::UInt32 => {
                    let Some(a) = maxes.as_any().downcast_ref::<datafusion::arrow::array::UInt32Array>() else { continue };
                    let Some(m) = (0..a.len()).filter(|&i| a.is_valid(i)).map(|i| a.value(i)).max() else { continue };
                    ScalarValue::UInt32(Some(m))
                }
                DataType::UInt64 => {
                    let Some(a) = maxes.as_any().downcast_ref::<datafusion::arrow::array::UInt64Array>() else { continue };
                    let Some(m) = (0..a.len()).filter(|&i| a.is_valid(i)).map(|i| a.value(i)).max() else { continue };
                    ScalarValue::UInt64(Some(m))
                }
                _ => continue, // numeric int columns only — simplest reliable threshold
            };
            out.push((name.clone(), pq_idx, threshold));
        }
        out
    }

    fn col_expr(name: &str, idx: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(PhysColumn::new(name, idx))
    }
    fn ge(name: &str, idx: usize, v: ScalarValue) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(col_expr(name, idx), Operator::GtEq, Arc::new(Literal::new(v))))
    }
    fn lt(name: &str, idx: usize, v: ScalarValue) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(col_expr(name, idx), Operator::Lt, Arc::new(Literal::new(v))))
    }
    fn and(l: Arc<dyn PhysicalExpr>, r: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(l, Operator::And, r))
    }
    fn or(l: Arc<dyn PhysicalExpr>, r: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(l, Operator::Or, r))
    }
    fn not(e: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(datafusion::physical_expr::expressions::NotExpr::new(e))
    }

    fn selection_skips(sel: &datafusion::parquet::arrow::arrow_reader::RowSelection) -> Vec<(bool, usize)> {
        sel.iter().map(|s| (s.skip, s.row_count)).collect()
    }

    #[tokio::test]
    async fn scoped_matches_full_index_on_real_files() {
        let Some(dir) = dir_from_env() else {
            eprintln!("OPENSEARCH_REAL_PARQUET_DIR not set — skipping real-file test");
            return;
        };
        clear_scoped_cache_for_test();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());

        let files = list_parquet(&dir);
        assert!(!files.is_empty(), "no .parquet files in {}", dir.display());

        let mut checked = 0usize;
        for path in &files {
            let footer = footer_only_meta(path);
            let schema = arrow_schema(&footer);
            let num_cols = footer.file_metadata().schema_descr().num_columns();
            let num_rgs = footer.num_row_groups();

            // Footer-only load must NOT carry a page index.
            assert!(
                footer.column_index().is_none() && footer.offset_index().is_none(),
                "{}: footer-only load unexpectedly has a page index",
                path.display()
            );

            // Ground truth: full page index (also used to derive a selective
            // threshold from real per-page stats).
            let full = full_index_meta(path);

            let Some((col_name, pq_idx, threshold)) = pick_prunable_column(&full, &schema) else {
                eprintln!(
                    "{}: no prunable numeric column with a page index — skipping (cols={}, rgs={})",
                    path.display(), num_cols, num_rgs
                );
                continue;
            };
            let arrow_idx = schema.index_of(&col_name).unwrap();

            eprintln!(
                "{}: cols={} rgs={} rows={} → testing column '{}' (parquet idx {}), threshold {:?}",
                path.file_name().unwrap().to_string_lossy(),
                num_cols, num_rgs, footer.file_metadata().num_rows(), col_name, pq_idx, threshold
            );

            // Build our scoped index for just this column.
            let loc = ObjPath::from(path.to_string_lossy().as_ref());
            let augmented = load_scoped_page_index(&store, &loc, &footer, &[pq_idx])
                .await
                .unwrap_or_else(|| panic!("{}: scoped page-index load returned None", path.display()));
            assert!(
                augmented.column_index().is_some() && augmented.offset_index().is_some(),
                "{}: augmented metadata missing page index", path.display()
            );

            let predicate = ge(&col_name, arrow_idx, threshold);
            let pp = match build_pruning_predicate(&predicate, schema.clone()) {
                Some(pp) => pp,
                None => {
                    eprintln!("  predicate not prunable, skipping file");
                    continue;
                }
            };

            let scoped_pruner = PagePruner::new(&schema, Arc::clone(&augmented));
            let full_pruner = PagePruner::new(&schema, Arc::clone(&full));

            // Compare per-RG selections across EVERY row group.
            let mut total_rows = 0usize;
            let mut kept_rows = 0usize;
            for rg in 0..num_rgs {
                let s = scoped_pruner.prune_rg(&pp, rg, None);
                let f = full_pruner.prune_rg(&pp, rg, None);
                match (s, f) {
                    (Some(s), Some(f)) => {
                        assert_eq!(
                            selection_skips(&s),
                            selection_skips(&f),
                            "{} rg {}: scoped selection differs from full-index selection",
                            path.display(), rg
                        );
                        for sel in s.iter() {
                            total_rows += sel.row_count;
                            if !sel.skip { kept_rows += sel.row_count; }
                        }
                    }
                    (None, None) => {}
                    (s, f) => panic!(
                        "{} rg {}: scoped/full disagree on prunability: scoped.is_some={}, full.is_some={}",
                        path.display(), rg, s.is_some(), f.is_some()
                    ),
                }
            }
            checked += 1;
            if total_rows > 0 {
                eprintln!(
                    "  ✓ {} row groups matched full-index pruning; predicate kept {}/{} rows ({:.1}% pruned)",
                    num_rgs, kept_rows, total_rows,
                    100.0 * (total_rows - kept_rows) as f64 / total_rows as f64
                );
            } else {
                eprintln!("  ✓ {} row groups matched full-index pruning", num_rgs);
            }
        }
        assert!(checked > 0, "no file had a prunable numeric column to verify");
        eprintln!("verified scoped==full pruning on {}/{} files", checked, files.len());
    }

    /// Complex multi-column boolean predicates (`AND` / `OR` / `NOT` / nested)
    /// on real files. This is the case single-column tests can't cover: the
    /// scoped index must carry real entries for *several* columns scattered into
    /// their distinct absolute parquet slots, and `CommonGridPageStats` must
    /// build a correct union page-grid across columns whose page layouts differ.
    /// For each boolean shape, the scoped multi-column index must drive
    /// `prune_rg` to the exact same selection as a full page-index decode.
    #[tokio::test]
    async fn complex_boolean_matches_full_index_on_real_files() {
        let Some(dir) = dir_from_env() else {
            eprintln!("OPENSEARCH_REAL_PARQUET_DIR not set — skipping real-file boolean test");
            return;
        };
        clear_scoped_cache_for_test();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let files = list_parquet(&dir);
        assert!(!files.is_empty(), "no .parquet files in {}", dir.display());

        let mut checked = 0usize;
        for path in &files {
            let footer = footer_only_meta(path);
            let schema = arrow_schema(&footer);
            let num_rgs = footer.num_row_groups();
            let full = full_index_meta(path);

            // Need at least 3 prunable columns to build interesting trees.
            let cols = pick_prunable_columns(&full, &schema, 3);
            if cols.len() < 3 {
                eprintln!(
                    "{}: only {} prunable numeric columns — need 3, skipping",
                    path.file_name().unwrap().to_string_lossy(), cols.len()
                );
                continue;
            }
            let (n0, p0, t0) = cols[0].clone();
            let (n1, p1, t1) = cols[1].clone();
            let (n2, p2, t2) = cols[2].clone();
            let a0 = schema.index_of(&n0).unwrap();
            let a1 = schema.index_of(&n1).unwrap();
            let a2 = schema.index_of(&n2).unwrap();

            // Scope the index to ALL three columns (sorted/deduped as production does).
            let mut parquet_cols = vec![p0, p1, p2];
            parquet_cols.sort_unstable();
            parquet_cols.dedup();
            let loc = ObjPath::from(path.to_string_lossy().as_ref());
            let augmented = load_scoped_page_index(&store, &loc, &footer, &parquet_cols)
                .await
                .unwrap_or_else(|| panic!("{}: scoped multi-col load returned None", path.display()));

            let scoped_pruner = PagePruner::new(&schema, Arc::clone(&augmented));
            let full_pruner = PagePruner::new(&schema, Arc::clone(&full));

            // A range of boolean shapes. `>= max` is selective; `< max` is its
            // complement; combine with AND/OR/NOT and nest.
            // Note: `NOT(col >= v)` is conservatively non-translatable by
            // DataFusion's `PruningPredicate` (collapses to always-true), so we
            // also include `NOT(col < v)` which IS prunable (≡ `col >= v`) to
            // exercise negation through the scoped index, not just skip it.
            let shapes: Vec<(&str, Arc<dyn PhysicalExpr>)> = vec![
                ("c0 AND c1", and(ge(&n0, a0, t0.clone()), ge(&n1, a1, t1.clone()))),
                ("c0 OR c1", or(ge(&n0, a0, t0.clone()), ge(&n1, a1, t1.clone()))),
                ("NOT(c0 < t0)  [≡ c0 >= t0, prunable]", not(lt(&n0, a0, t0.clone()))),
                ("c0 AND (c1 OR c2)", and(ge(&n0, a0, t0.clone()), or(ge(&n1, a1, t1.clone()), ge(&n2, a2, t2.clone())))),
                ("(c0 AND c1) OR NOT(c2 < t2)", or(and(ge(&n0, a0, t0.clone()), ge(&n1, a1, t1.clone())), not(lt(&n2, a2, t2.clone())))),
                ("c0 AND NOT(c1 < t1 AND c2 < t2)", and(ge(&n0, a0, t0.clone()), not(and(lt(&n1, a1, t1.clone()), lt(&n2, a2, t2.clone()))))),
            ];

            eprintln!(
                "{}: rgs={} cols=[{}({}), {}({}), {}({})]",
                path.file_name().unwrap().to_string_lossy(),
                num_rgs, n0, a0, n1, a1, n2, a2
            );

            for (label, expr) in &shapes {
                let Some(pp) = build_pruning_predicate(expr, schema.clone()) else {
                    eprintln!("  [{}] not prunable (always-true) — skipping shape", label);
                    continue;
                };
                let mut total = 0usize;
                let mut kept = 0usize;
                for rg in 0..num_rgs {
                    let s = scoped_pruner.prune_rg(&pp, rg, None);
                    let f = full_pruner.prune_rg(&pp, rg, None);
                    match (s, f) {
                        (Some(s), Some(f)) => {
                            assert_eq!(
                                selection_skips(&s),
                                selection_skips(&f),
                                "{} [{}] rg {}: scoped multi-col selection differs from full-index",
                                path.display(), label, rg
                            );
                            for sel in s.iter() {
                                total += sel.row_count;
                                if !sel.skip { kept += sel.row_count; }
                            }
                        }
                        (None, None) => {}
                        (s, f) => panic!(
                            "{} [{}] rg {}: scoped/full disagree on prunability: scoped={} full={}",
                            path.display(), label, rg, s.is_some(), f.is_some()
                        ),
                    }
                }
                let pruned_pct = if total > 0 {
                    100.0 * (total - kept) as f64 / total as f64
                } else { 0.0 };
                eprintln!("  ✓ [{}] matched across {} RGs ({:.1}% pruned)", label, num_rgs, pruned_pct);
            }
            checked += 1;
        }
        assert!(checked > 0, "no file had >=3 prunable numeric columns for boolean tests");
        eprintln!("verified complex-boolean scoped==full on {}/{} files", checked, files.len());
    }
}

/// Truth-table tests: quantify page-index bytes scanned and scoped-cache bytes
/// filled, **with** scoping (this change) vs **without** (the full-page-index
/// baseline DataFusion/the listing path would read). These are the reliable,
/// deterministic measurements behind the design.
///
/// Method:
///  - A `CountingStore` wraps `InMemory` and sums the bytes of every range read
///    (`get_opts`/`get_ranges` both funnel through `get_opts` in object_store
///    0.13). This is the exact "parquet bytes scanned for the page index".
///  - WITHOUT change (full page index): the bytes are the union of every
///    column's `ColumnIndex` + `OffsetIndex` ranges across all row groups —
///    computed directly from footer metadata offsets/lengths (what a
///    `PageIndexPolicy::Optional` decode reads). Cache fill = full
///    `ParquetMetaData::memory_size()`.
///  - WITH change (scoped): drive `load_scoped_page_index` through the
///    CountingStore and read the actual bytes scanned; cache fill =
///    `scoped_cache_bytes_for_test()`.
#[cfg(test)]
mod truth_table {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjPath;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex as StdMutex;

    /// ObjectStore wrapper that counts page-index range-read bytes two ways:
    ///  - `requested`: the raw bytes the caller asks for (sum of the ranges
    ///    passed to `get_ranges`). This is the loader's *intent* — what page
    ///    index it scopes to — independent of transport coalescing.
    ///  - `fetched`: the bytes actually pulled from storage after object_store's
    ///    `get_ranges` coalesces adjacent ranges (`get_opts` bounded reads). On
    ///    small/contiguous files this can exceed `requested` because coalescing
    ///    merges the predicate-CI and all-OI ranges (plus any gap) into one read.
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<InMemory>,
        requested: Arc<AtomicU64>,
        fetched: Arc<AtomicU64>,
        reads: Arc<AtomicU64>,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            // `fetched`: bytes actually pulled from storage (post-coalescing).
            if let Some(range) = options.range.as_ref() {
                if let object_store::GetRange::Bounded(r) = range {
                    self.fetched.fetch_add(r.end - r.start, Ordering::Relaxed);
                    self.reads.fetch_add(1, Ordering::Relaxed);
                }
            }
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &ObjPath,
            ranges: &[std::ops::Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            // `requested`: the loader's true intent (sum of the exact ranges it
            // asks for), before object_store coalesces adjacent ranges.
            let req: u64 = ranges.iter().map(|r| r.end - r.start).sum();
            self.requested.fetch_add(req, Ordering::Relaxed);
            self.inner.get_ranges(location, ranges).await
        }
        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(locations)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// 10 columns: 4 numeric (`n0..n3`, candidate filter columns) + 6 wide string
    /// columns (`s0..s5`, the ColumnIndex hogs). One row group, multiple pages.
    fn wide_parquet() -> (Bytes, SchemaRef) {
        let mut fields: Vec<Field> = Vec::new();
        for i in 0..4 {
            fields.push(Field::new(format!("n{i}"), DataType::Int32, false));
        }
        for i in 0..6 {
            fields.push(Field::new(format!("s{i}"), DataType::Utf8, false));
        }
        let schema = Arc::new(Schema::new(fields));

        const ROWS: usize = 4096;
        let mut cols: Vec<arrow::array::ArrayRef> = Vec::new();
        for c in 0..4 {
            let v: Vec<i32> = (0..ROWS as i32).map(|r| r + c * 1000).collect();
            cols.push(Arc::new(Int32Array::from(v)));
        }
        for c in 0..6 {
            // Long-ish, distinct strings → fat per-page min/max in the ColumnIndex.
            let v: Vec<String> = (0..ROWS)
                .map(|r| format!("col{c}_row{r:06}_padding_padding_padding"))
                .collect();
            cols.push(Arc::new(StringArray::from(v)));
        }
        let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();

        let props = WriterProperties::builder()
            .set_max_row_group_size(ROWS)
            .set_data_page_row_count_limit(256) // many pages → real page index
            .set_write_batch_size(256)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut buf: Vec<u8> = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        (Bytes::from(buf), schema)
    }

    struct Counters {
        requested: Arc<AtomicU64>,
        #[allow(dead_code)]
        fetched: Arc<AtomicU64>,
        #[allow(dead_code)]
        reads: Arc<AtomicU64>,
    }
    impl Counters {
        fn reset(&self) {
            self.requested.store(0, Ordering::Relaxed);
            self.fetched.store(0, Ordering::Relaxed);
            self.reads.store(0, Ordering::Relaxed);
        }
        fn requested(&self) -> u64 { self.requested.load(Ordering::Relaxed) }
    }

    async fn stage_counting(bytes: Bytes) -> (Arc<dyn ObjectStore>, Counters, ObjPath) {
        let inner = Arc::new(InMemory::new());
        let loc = ObjPath::from("data.parquet");
        inner.put(&loc, PutPayload::from_bytes(bytes)).await.unwrap();
        let requested = Arc::new(AtomicU64::new(0));
        let fetched = Arc::new(AtomicU64::new(0));
        let reads = Arc::new(AtomicU64::new(0));
        let store: Arc<dyn ObjectStore> = Arc::new(CountingStore {
            inner,
            requested: Arc::clone(&requested),
            fetched: Arc::clone(&fetched),
            reads: Arc::clone(&reads),
        });
        (store, Counters { requested, fetched, reads }, loc)
    }

    fn footer_only(bytes: &Bytes) -> Arc<ParquetMetaData> {
        ArrowReaderMetadata::load(&bytes.clone(), ArrowReaderOptions::new().with_page_index(false))
            .unwrap()
            .metadata()
            .clone()
    }

    fn full_index(bytes: &Bytes) -> Arc<ParquetMetaData> {
        ArrowReaderMetadata::load(&bytes.clone(), ArrowReaderOptions::new().with_page_index(true))
            .unwrap()
            .metadata()
            .clone()
    }

    /// Bytes the WITHOUT-change (full page index) path reads off disk: the sum of
    /// every column's ColumnIndex + OffsetIndex lengths across all row groups.
    fn full_page_index_disk_bytes(meta: &ParquetMetaData) -> u64 {
        let mut total = 0u64;
        for rg in meta.row_groups() {
            for c in rg.columns() {
                total += c.column_index_length().unwrap_or(0).max(0) as u64;
                total += c.offset_index_length().unwrap_or(0).max(0) as u64;
            }
        }
        total
    }

    /// Bytes the scoped path *should* read off disk: ColumnIndex for predicate
    /// columns + OffsetIndex for all columns, across all row groups.
    fn scoped_page_index_disk_bytes(meta: &ParquetMetaData, predicate_cols: &[usize]) -> u64 {
        let mut total = 0u64;
        for rg in meta.row_groups() {
            for &pc in predicate_cols {
                total += rg.column(pc).column_index_length().unwrap_or(0).max(0) as u64;
            }
            for c in rg.columns() {
                total += c.offset_index_length().unwrap_or(0).max(0) as u64;
            }
        }
        total
    }

    // Serialize against the process-global scoped cache.
    static GUARD: StdMutex<()> = StdMutex::new(());

    /// The headline truth table. Prints a table and asserts the invariants:
    ///  - scoped page-index bytes scanned < full page-index bytes (CI scoped)
    ///  - scoped cache fill < full metadata size
    ///  - scoped cache holds exactly one entry after one file/predicate load
    ///  - a second identical load is a pure cache hit (zero additional bytes)
    #[tokio::test]
    async fn truth_table_bytes_and_cache() {
        let _g = GUARD.lock().unwrap();
        clear_scoped_cache_for_test();

        let (bytes, schema) = wide_parquet();
        let full = full_index(&bytes);
        let total_cols = full.row_group(0).columns().len();

        // Predicate touches a single numeric column n1 (parquet col 1).
        let (store, ctr, loc) = stage_counting(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let predicate_cols = resolve_predicate_parquet_columns(&schema, &fo, &["n1".to_string()]);
        assert_eq!(predicate_cols, vec![1]);

        // --- Expected page-index bytes, computed from metadata (independent of
        //     the loader): "WITHOUT" = full all-column CI+OI; "WITH" = scoped. ---
        let full_bytes = full_page_index_disk_bytes(&full);
        let scoped_bytes_expected = scoped_page_index_disk_bytes(&full, &predicate_cols);
        let full_mem = full.memory_size();

        // --- WITH change: drive the scoped loader through the counting store ---
        ctr.reset();
        let augmented = load_scoped_page_index(&store, &loc, &fo, &predicate_cols)
            .await
            .expect("scoped load");
        let scoped_requested = ctr.requested(); // loader intent: exact scoped ranges
        let scoped_cache_fill = scoped_cache_bytes_for_test();
        let entries_after_first = scoped_cache_len_for_test();

        // --- Second identical load = pure cache hit, zero extra bytes ---
        ctr.reset();
        let _hit = load_scoped_page_index(&store, &loc, &fo, &predicate_cols)
            .await
            .expect("scoped load (hit)");
        let requested_on_hit = ctr.requested();
        let stats = scoped_cache_stats();

        eprintln!("\n=== PAGE-INDEX TRUTH TABLE ({total_cols} cols, 1 RG, predicate=[n1]) ===");
        eprintln!("{:<46} {:>12} {:>12}", "metric", "WITHOUT", "WITH");
        eprintln!("{:<46} {:>12} {:>12}", "page-index bytes (expected, disk)", full_bytes, scoped_bytes_expected);
        eprintln!("{:<46} {:>12} {:>12}", "page-index bytes REQUESTED by loader", full_bytes, scoped_requested);
        eprintln!("{:<46} {:>12} {:>12}", "cache fill (bytes)", full_mem, scoped_cache_fill);
        eprintln!("{:<46} {:>12} {:>12}", "cache entries", "n/a", entries_after_first);
        eprintln!("{:<46} {:>12} {:>12}", "bytes requested on 2nd (cache hit)", "n/a", requested_on_hit);
        eprintln!("cache stats after 2 loads: {stats:?}");
        eprintln!(
            "scoped REQUESTED {:.1}% of full page-index bytes; cache fill {:.1}% of full metadata\n",
            100.0 * scoped_requested as f64 / full_bytes as f64,
            100.0 * scoped_cache_fill as f64 / full_mem as f64,
        );

        // Invariants (robust across coalescing — assert on REQUESTED intent and
        // cache memory, not on post-coalesce fetched bytes which depend on file
        // layout):
        //
        // 1. The loader REQUESTS materially fewer page-index bytes than the full
        //    decode: predicate-column ColumnIndex (1 of 10 cols) + all-column
        //    OffsetIndex, vs every column's CI+OI. The 6 fat string ColumnIndexes
        //    are excluded.
        assert!(
            scoped_requested < full_bytes,
            "scoped REQUESTED page-index bytes ({scoped_requested}) must be < full ({full_bytes})"
        );
        // 2. Requested bytes equal what metadata says the scoped set is (the
        //    loader reads exactly the predicate-CI + all-OI ranges).
        assert_eq!(
            scoped_requested, scoped_bytes_expected,
            "loader must request exactly the scoped page-index ranges"
        );
        // 3. Cache fill is smaller than retaining full metadata.
        assert!(
            scoped_cache_fill < full_mem,
            "scoped cache fill ({scoped_cache_fill}) must be < full metadata size ({full_mem})"
        );
        // 4. One entry after one (file,predicate) load.
        assert_eq!(entries_after_first, 1, "exactly one scoped cache entry");
        // 5. Second identical load is a pure cache hit: zero page-index bytes.
        assert_eq!(requested_on_hit, 0, "cache hit must request zero page-index bytes");
        assert_eq!(stats.hits, 1, "second load must register one cache hit");
        assert_eq!(stats.misses, 1, "first load must register one cache miss");

        // The augmented metadata must carry a real OffsetIndex for ALL columns
        // (read-path correctness) and a real ColumnIndex only for the predicate
        // column.
        let ci = augmented.column_index().unwrap();
        let oi = augmented.offset_index().unwrap();
        for c in 0..total_cols {
            assert!(!oi[0][c].page_locations.is_empty(), "col {c} must keep real OffsetIndex");
        }
        assert!(!matches!(ci[0][1], ColumnIndexMetaData::NONE), "predicate col has real ColumnIndex");
        assert!(matches!(ci[0][0], ColumnIndexMetaData::NONE), "non-predicate col ColumnIndex is NONE");

        clear_scoped_cache_for_test();
    }

    /// Distinct predicate-column sets over the same file are independent cache
    /// entries; widening the predicate scans more ColumnIndex bytes.
    #[tokio::test]
    async fn truth_table_wider_predicate_scans_more() {
        let _g = GUARD.lock().unwrap();
        clear_scoped_cache_for_test();

        let (bytes, schema) = wide_parquet();
        let full = full_index(&bytes);
        let (store, ctr, loc) = stage_counting(bytes.clone()).await;
        let fo = footer_only(&bytes);

        // 1-column predicate.
        let one = resolve_predicate_parquet_columns(&schema, &fo, &["n0".to_string()]);
        ctr.reset();
        let _ = load_scoped_page_index(&store, &loc, &fo, &one).await.unwrap();
        let req_one = ctr.requested();

        // 3-column predicate (distinct key → separate entry, fresh decode).
        let three =
            resolve_predicate_parquet_columns(&schema, &fo, &["n0".into(), "n1".into(), "n2".into()]);
        ctr.reset();
        let _ = load_scoped_page_index(&store, &loc, &fo, &three).await.unwrap();
        let req_three = ctr.requested();

        let full_bytes = full_page_index_disk_bytes(&full);
        eprintln!(
            "\nwider-predicate REQUESTED bytes: 1-col {req_one}, 3-col {req_three}, full {full_bytes}"
        );
        // More predicate columns → more ColumnIndex bytes requested (the OffsetIndex
        // part is the same all-column set in both).
        assert!(req_three > req_one, "3-col predicate must request more CI bytes than 1-col");
        // Both still less than the full all-column ColumnIndex+OffsetIndex.
        assert!(req_three < full_bytes, "even 3-col scoped < full page index");
        // Two distinct (file, predicate-cols) keys → two cache entries.
        assert_eq!(scoped_cache_len_for_test(), 2, "distinct predicate sets are distinct entries");

        clear_scoped_cache_for_test();
    }
}
