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
//! This module restores page-level pruning **scoped to the predicate columns
//! only**. Given a footer-only `ParquetMetaData` and the parquet column indices
//! the query's predicate references, it:
//!
//!   1. Range-reads just those columns' `ColumnIndex` / `OffsetIndex` bytes
//!      (per row group) over the object store, on the IO runtime.
//!   2. Decodes them with arrow-rs's public per-column readers
//!      ([`read_columns_indexes`] / [`read_offset_indexes`], which take a
//!      `&[ColumnChunkMetaData]` slice).
//!   3. Scatters the decoded entries into full-width per-RG vectors — real
//!      entries at predicate-column positions, cheap placeholders elsewhere
//!      ([`ColumnIndexMetaData::NONE`] / an empty `OffsetIndexMetaData`) — so
//!      that `StatisticsConverter`'s absolute `index[rg][parquet_col]` indexing
//!      stays valid.
//!   4. Rebuilds a `ParquetMetaData` carrying that sparse page index.
//!
//! The result is cached by `(file path, predicate-column-set)` so repeated
//! queries over the same columns skip the re-decode.
//!
//! # Memory
//!
//! The augmented metadata retains page index for the predicate columns only —
//! a few columns instead of all ~hundreds. Resident footprint stays close to
//! the footer-only baseline; the scoped cache is bounded (see
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
use datafusion::parquet::file::page_index::offset_index::OffsetIndexMetaData;
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

    // Phase 1: per RG, gather the predicate columns' chunks and compute the
    // union byte ranges for the column index and offset index. Bail to
    // footer-only if any predicate column in any RG lacks a page index.
    struct RgPlan {
        pred_chunks: Vec<ColumnChunkMetaData>,
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
        let col_range = column_index_union(&pred_chunks)?;
        let off_range = offset_index_union(&pred_chunks)?;
        fetch_ranges.push(col_range.clone());
        fetch_ranges.push(off_range.clone());
        plans.push(RgPlan {
            pred_chunks,
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
        // return a Vec aligned to the `pred_chunks` slice we pass.
        #[allow(deprecated)]
        let decoded_cols = read_columns_indexes(&col_reader, &plan.pred_chunks).ok()??;
        #[allow(deprecated)]
        let decoded_offs = read_offset_indexes(&off_reader, &plan.pred_chunks).ok()??;
        if decoded_cols.len() != parquet_cols.len() || decoded_offs.len() != parquet_cols.len() {
            return None;
        }

        // Full-width rows: NONE / empty placeholders everywhere, real entries at
        // predicate positions. `StatisticsConverter` only ever dereferences the
        // predicate positions; placeholders keep absolute indexing valid and are
        // zero-allocation.
        let mut col_row: Vec<ColumnIndexMetaData> =
            (0..num_cols).map(|_| ColumnIndexMetaData::NONE).collect();
        let mut off_row: Vec<OffsetIndexMetaData> = (0..num_cols)
            .map(|_| OffsetIndexMetaData {
                page_locations: Vec::new(),
                unencoded_byte_array_data_bytes: None,
            })
            .collect();
        for (k, &parquet_col) in parquet_cols.iter().enumerate() {
            col_row[parquet_col] = decoded_cols[k].clone();
            off_row[parquet_col] = decoded_offs[k].clone();
        }
        column_index.push(col_row);
        offset_index.push(off_row);
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
