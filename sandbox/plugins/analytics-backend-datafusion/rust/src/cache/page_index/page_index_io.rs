/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Page-index load entry points and their supporting internals.
//!
//! [`load_scoped_page_index_cols`] is the single public entry point — it loads
//! the ColumnIndex for predicate columns (all RGs) and the OffsetIndex for
//! projection columns, using the two process-global caches.

use std::collections::{HashMap, HashSet};
use std::mem;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use arrow::datatypes::SchemaRef;
use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::{
    ColumnChunkMetaData, OffsetIndexBuilder, ParquetColumnIndex, ParquetMetaData,
    ParquetOffsetIndex,
};
use datafusion::parquet::file::page_index::column_index::ColumnIndexMetaData;
use datafusion::parquet::file::page_index::index_reader::{
    read_columns_indexes, read_offset_indexes,
};
use datafusion::parquet::file::reader::{ChunkReader, Length};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion::scalar::ScalarValue;
use object_store::ObjectStore;
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use prost::bytes::{buf, Buf, Bytes};

use super::cache_keys::{CiCellKey, OiCellKey, OiColumn};
use super::{COLUMN_INDEX_CACHE, OFFSET_INDEX_CACHE};

/// Load + graft a scoped page index: ColumnIndex for `predicate_cols` (all RGs),
/// OffsetIndex for `projection_cols` (∪ predicate ∪ col 0). This is the single
/// production entry point used by both the listing path (`ScopedPageIndexReaderFactory`)
/// and the indexed executor augmentation loop.
pub async fn load_scoped_page_index_cols(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    predicate_cols: &[usize],
    projection_cols: &[usize],
) -> Option<Arc<ParquetMetaData>> {
    attach_scoped_page_index_to_metadata(
        store,
        location,
        footer_meta,
        predicate_cols,
        Some(projection_cols),
    )
    .await
}

async fn attach_scoped_page_index_to_metadata(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    predicate_cols: &[usize],
    projection_cols: Option<&[usize]>,
) -> Option<Arc<ParquetMetaData>> {
    // Nothing to build when both predicate_cols and projection_cols are empty/absent.
    // An empty projection slice means "no specific projection" → treat as None (all columns).
    let oi_proj = match projection_cols {
        Some(p) if !p.is_empty() => Some(p),
        _ => None, // empty or absent → OI covers all columns
    };
    if predicate_cols.is_empty() && oi_proj.is_none() && projection_cols.is_some() {
        // All empty — nothing to build.
        return None;
    }
    // CI and OI IO run concurrently — wall time = max(ci, oi) not their sum.
    let (ci_result, oi_result) = tokio::join!(
        async {
            if predicate_cols.is_empty() {
                Some(None)
            } else {
                Some(Some(
                    get_or_build_column_index(store, location, footer_meta, predicate_cols).await?,
                ))
            }
        },
        get_or_build_offset_index(store, location, footer_meta, predicate_cols, oi_proj),
    );
    let column_index = ci_result?;
    let offset_index = oi_result?;
    Some(graft(footer_meta, column_index, offset_index))
}

/// Build a fresh `ParquetMetaData` = `footer` with the page-index pair grafted
/// on. Clones the footer to get an owned value for the builder — but with
/// `ParquetMetaData.row_groups` held behind an `Arc` (see the arrow-rs change),
/// that clone is a refcount bump, not a deep copy of every row group's
/// column-chunk metadata. On wide / many-row-group files (e.g. textbench's
/// ~403-col, ~64-RG footers) that deep copy was ~60ms/query; sharing makes the
/// graft effectively free.
fn graft(
    footer_meta: &Arc<ParquetMetaData>,
    column_index: Option<ParquetColumnIndex>,
    offset_index: ParquetOffsetIndex,
) -> Arc<ParquetMetaData> {
    let base = ParquetMetaData::clone(footer_meta);
    let rebuilt = base
        .into_builder()
        .set_column_index(column_index)
        .set_offset_index(Some(offset_index))
        .build();
    Arc::new(rebuilt)
}

// ── ColumnIndex cache lookup + build (per `(file, col, rg)` cell) ────────────

/// Assemble the full-width `[rg][col]` `ColumnIndex` matrix (real cells only at
/// `predicate_cols` × all RGs; `NONE` everywhere else) by looking up each
/// `(file, col, rg)` cell in the cache and decoding only the cells that miss.
///
/// Each cell is keyed on `(file, col, rg)` and decoded once per file — reused
/// across every query that filters on the same column regardless of predicates.
async fn get_or_build_column_index(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    predicate_cols: &[usize],
) -> Option<ParquetColumnIndex> {
    let num_rgs = footer_meta.num_row_groups();
    if num_rgs == 0 {
        return None;
    }
    let num_cols = footer_meta.file_metadata().schema_descr().num_columns();

    debug_assert!(
        predicate_cols.iter().all(|&i| i < num_cols),
        "predicate_cols contains out-of-bounds index (num_cols={num_cols}): {predicate_cols:?}"
    );
    if predicate_cols.iter().any(|&i| i >= num_cols) {
        return None;
    }

    let build_rgs: Vec<usize> = (0..num_rgs).collect();
    let path: Arc<str> = Arc::from(location.as_ref());

    // Initially filled NONE for all RGs and all cols - as placeholders
    let mut col_index_matrix: ParquetColumnIndex = (0..num_rgs)
        .map(|_| (0..num_cols).map(|_| ColumnIndexMetaData::NONE).collect())
        .collect();

    // Phase 1: serve every needed cell that is already cached; collect misses.
    let mut missing_col_rg_matrix: Vec<(usize, usize)> = Vec::new(); // (col, rg)
    for &rg in &build_rgs {
        for &col in predicate_cols {
            let key = CiCellKey {
                path: path.clone(),
                col,
                rg,
            };
            match COLUMN_INDEX_CACHE.get(&key) {
                Some(cell) => col_index_matrix[rg][col] = cell,
                None => missing_col_rg_matrix.push((col, rg)),
            }
        }
    }
    // Phase 2: decode the missing cells (vectored fetch grouped by RG), place
    // them in the matrix, and populate the cache.
    // Use insert_batch so all cells from this query are inserted under a single
    // write-lock acquisition — avoids per-cell lock overhead and runs eviction
    // exactly once after the batch rather than once per cell.
    if !missing_col_rg_matrix.is_empty() {
        let built =
            build_column_index_cells(store, location, footer_meta, &missing_col_rg_matrix).await?;

        let batch = built.iter().map(|cell| {
            debug_assert!(
                cell.rg < col_index_matrix.len() && cell.col < col_index_matrix[cell.rg].len(),
                "cell ({}, {}) out of matrix bounds ({num_rgs} rgs, {num_cols} cols)",
                cell.col,
                cell.rg,
            );
            (
                CiCellKey {
                    path: path.clone(),
                    col: cell.col,
                    rg: cell.rg,
                },
                cell.data.clone(),
                cell.size,
            )
        });
        COLUMN_INDEX_CACHE.insert_batch(batch);

        for cell in built {
            col_index_matrix[cell.rg][cell.col] = cell.data;
        }
    }

    Some(col_index_matrix)
}

/// One row group's decode unit: the scoped columns and their precomputed chunk
/// metadata. Built once per RG and used by BOTH the fetch (to union the chunks'
/// byte extents) and the decode (to pass to `read_*_indexes`), so `chunks` is
/// cloned from the footer exactly once.
struct RgPlan {
    rg: usize,
    cols: Vec<usize>,
    chunks: Vec<ColumnChunkMetaData>,
}

impl RgPlan {
    fn new(footer_meta: &Arc<ParquetMetaData>, rg: usize, cols: Vec<usize>) -> Self {
        let rgm = footer_meta.row_group(rg);
        let chunks = cols.iter().map(|&i| rgm.column(i).clone()).collect();
        Self { rg, cols, chunks }
    }
}

struct CiCell {
    col: usize,
    rg: usize,
    data: ColumnIndexMetaData,
    size: usize,
}

struct OiCell {
    col: usize,
    data: OiColumn,
    size: usize,
}

/// Range-read + decode the requested `(col, rg)` ColumnIndex cells, grouping by
/// row group so each RG's columns share one vectored fetch + decode. `None` if
/// any requested column lacks a column-index range (→ footer-only fallback).
async fn build_column_index_cells(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    col_rg_matrix: &[(usize, usize)],
) -> Option<Vec<CiCell>> {
    let mut by_rg: HashMap<usize, Vec<usize>> = HashMap::new();
    for &(col, rg) in col_rg_matrix {
        by_rg.entry(rg).or_default().push(col);
    }
    let plan: Vec<RgPlan> = by_rg
        .into_iter()
        .map(|(rg, cols)| RgPlan::new(footer_meta, rg, cols))
        .collect();

    // Fetch (wide whole-region on remote, narrow per-RG on local). Decode below is
    // always scoped to the requested columns.
    let buffers = fetch_index_buffers(store, location, footer_meta, &plan, ci_extent).await?;

    let mut out: Vec<CiCell> = Vec::with_capacity(col_rg_matrix.len());
    for (i, p) in plan.iter().enumerate() {
        // Deprecated but the only PUBLIC column-subset decoder (arrow-rs#8643).
        #[allow(deprecated)]
        let decoded = read_columns_indexes(&buffers.reader(i), &p.chunks).ok()??;
        if decoded.len() != p.cols.len() {
            return None;
        }
        let rgm = footer_meta.row_group(p.rg);
        for (entry, &col) in decoded.into_iter().zip(p.cols.iter()) {
            let size = rgm.column(col).column_index_length().unwrap_or(0).max(0) as usize;
            out.push(CiCell {
                col,
                rg: p.rg,
                data: entry,
                size,
            });
        }
    }
    Some(out)
}

// ── OffsetIndex cache lookup + build (per `(file, col)` cell, all RGs) ───────

/// Assemble the full-width `[rg][col]` `OffsetIndex` matrix (real entries only at
/// the resolved offset columns; empty placeholders elsewhere) from per-`(file,
/// col)` cells, decoding only the columns that miss.
///
/// The resolved offset-column set is `predicate ∪ projection ∪ {0}` (`projection_cols
/// == None` → all columns); see [`OiCellKey`] for why each must be real. Each
/// cached cell is a column's OffsetIndex across **all** row groups, keyed only on
/// `(file, col)`, so it is decoded once per file and reused across every query
/// that reads that column irrespective of projection or predicate.
async fn get_or_build_offset_index(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    predicate_cols: &[usize],
    projection_cols: Option<&[usize]>,
) -> Option<ParquetOffsetIndex> {
    let num_rgs = footer_meta.num_row_groups();
    if num_rgs == 0 {
        return None;
    }
    let num_cols = footer_meta.file_metadata().schema_descr().num_columns();

    // Resolve which columns need a real OffsetIndex:
    //   None   → no explicit projection, read everything → all columns get a real entry.
    //   Some   → build {col 0} ∪ predicate_cols ∪ proj_cols, clamped to num_cols.
    //            Col 0 is always included because the page-skip metric reads it
    //            regardless of what the query projects or filters on.
    //            Predicate-only queries (empty proj_cols) still get col 0 + predicate
    //            columns; projection-only queries get col 0 + projected columns.
    let off_cols: Vec<usize> = match projection_cols {
        None => (0..num_cols).collect(),
        Some(proj_cols) => {
            let mut set: HashSet<usize> = HashSet::new();
            set.insert(0); // page-skip metric always reads col 0
            for &c in predicate_cols {
                set.insert(c);
            }
            for &c in proj_cols {
                set.insert(c);
            }
            debug_assert!(
                set.iter().all(|&c| c < num_cols),
                "column index out of bounds (num_cols={num_cols}): {set:?}"
            );
            set.into_iter().filter(|&c| c < num_cols).collect()
        }
    };
    if off_cols.is_empty() {
        return None;
    }

    let path: Arc<str> = Arc::from(location.as_ref());
    // Placeholder for columns we don't build: a SINGLE page spanning the whole row
    // group, NOT an empty page-locations list. A scoped OffsetIndex is grafted as a
    // full-width `[rg][col]` matrix; consumers (DataFusion's page pruner, arrow's
    // reader, our indexed pruner) index it by absolute column and dereference
    // `page_locations` (`.last()`, `[0]`, `windows(2)`). An EMPTY placeholder
    // panics those (`page_locations.last().unwrap()` etc.) if any path touches a
    // column we scoped out — which is hard to predict across every query shape
    // (count/agg, SingleCollector prefetch, schema-evolved files). A one-page
    // placeholder is always safe to dereference and makes pruning conservatively
    // keep the whole RG (1 page = all rows → can't prune), never a wrong result.
    //
    // The single page MUST carry the column chunk's REAL byte offset+size, not
    // (0, 0). When a row selection actually READS a placeholdered column (e.g. a
    // `match() | sort | head` that page-selects the projected columns), arrow
    // derives the fetch byte range from this page location. A (0, 0) page makes
    // arrow compute a range whose `end < start`, and the read path's
    // `range.end - range.start` then underflows (`parquet_bridge.rs` get_byte_ranges
    // → "subtract with overflow" / release "capacity overflow"). Spanning the real
    // chunk extent (`byte_range()` = dictionary/data page start + compressed size)
    // makes any derived range a valid full-chunk read — the one-page semantics are
    // preserved (still "can't prune"), just with coordinates arrow can subtract.
    let placeholder_for = |rg_idx: usize, col_idx: usize| -> OffsetIndexMetaData {
        let rg = footer_meta.row_group(rg_idx);
        let (col_start, col_len) = rg.column(col_idx).byte_range();
        let mut b = OffsetIndexBuilder::new();
        // compressed_page_size is i32; clamp the (already-bounded) chunk length so a
        // pathologically large column can't wrap the cast.
        let size = i32::try_from(col_len).unwrap_or(i32::MAX);
        b.append_offset_and_size(col_start as i64, size);
        b.append_row_count(rg.num_rows());
        b.build()
    };
    // Each column chunk has its OWN byte offset, so the placeholder is per-(rg, col):
    // unlike the earlier (0, 0) placeholder it cannot be shared across columns. The
    // scoped columns get their real OffsetIndex scattered in afterward, overwriting
    // these placeholders.
    let mut matrix: ParquetOffsetIndex = (0..num_rgs)
        .map(|rg| (0..num_cols).map(|col| placeholder_for(rg, col)).collect())
        .collect();

    // Phase 1: serve cached columns; collect misses.
    let mut missing: Vec<usize> = Vec::new();
    for &col in &off_cols {
        let key = OiCellKey {
            path: path.clone(),
            col,
        };
        match OFFSET_INDEX_CACHE.get(&key) {
            Some(column) => scatter_offset_column(&mut matrix, col, &column),
            None => missing.push(col),
        }
    }

    // Phase 2: decode the missing columns (each spanning all RGs), scatter into
    // the matrix, and populate the cache.
    // Use insert_batch: all missing OI columns for this query in one lock acquisition.
    if !missing.is_empty() {
        let built =
            build_offset_index_columns(store, location, footer_meta, &missing, num_rgs).await?;

        let batch = built.iter().map(|cell| {
            (
                OiCellKey {
                    path: path.clone(),
                    col: cell.col,
                },
                cell.data.clone(),
                cell.size,
            )
        });
        OFFSET_INDEX_CACHE.insert_batch(batch);

        for cell in built {
            scatter_offset_column_owned(&mut matrix, cell.col, cell.data);
        }
    }

    Some(matrix)
}

/// Place a column's all-RG OffsetIndex (indexed by RG) into the matrix at `col`.
fn scatter_offset_column(matrix: &mut ParquetOffsetIndex, col: usize, column: &OiColumn) {
    for (rg, entry) in column.iter().enumerate() {
        if rg < matrix.len() {
            matrix[rg][col] = entry.clone();
        }
    }
}

/// Consuming version of [`scatter_offset_column`] — used after inserting into
/// the cache so we move rather than clone the per-RG entries.
fn scatter_offset_column_owned(matrix: &mut ParquetOffsetIndex, col: usize, column: OiColumn) {
    for (rg, entry) in column.into_iter().enumerate() {
        if rg < matrix.len() {
            matrix[rg][col] = entry;
        }
    }
}

/// Range-read + decode the OffsetIndex for each requested column across **every**
/// row group (read-time safety — see [`OiCellKey`]). `None` if any column lacks
/// an offset-index range (→ footer-only fallback).
async fn build_offset_index_columns(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    cols: &[usize],
    num_rgs: usize,
) -> Option<Vec<OiCell>> {
    // Same `cols` on every RG (read-time safety). Fetch, then decode scoped per RG.
    let plan: Vec<RgPlan> = (0..num_rgs)
        .map(|rg| RgPlan::new(footer_meta, rg, cols.to_vec()))
        .collect();
    let buffers = fetch_index_buffers(store, location, footer_meta, &plan, oi_extent).await?;

    // Per-column accumulator: one OiColumn slot per requested col, filled RG by RG.
    let mut columns: Vec<OiColumn> = cols.iter().map(|_| Vec::with_capacity(num_rgs)).collect();
    for (i, p) in plan.iter().enumerate() {
        #[allow(deprecated)]
        let decoded = read_offset_indexes(&buffers.reader(i), &p.chunks).ok()??;
        if decoded.len() != cols.len() {
            return None;
        }
        for (k, entry) in decoded.into_iter().enumerate() {
            columns[k].push(entry);
        }
    }

    let mut out: Vec<OiCell> = Vec::with_capacity(cols.len());
    for (k, &col) in cols.iter().enumerate() {
        let size = footer_meta
            .row_groups()
            .iter()
            .map(|rg| rg.column(col).offset_index_length().unwrap_or(0).max(0) as usize)
            .sum();
        out.push(OiCell {
            col,
            data: mem::take(&mut columns[k]),
            size,
        });
    }
    Some(out)
}

/// The page-index extent (byte offset..end) of one column chunk, for one index kind.
/// The only thing that differs between the CI and OI fetch paths.
type ChunkExtent = fn(&ColumnChunkMetaData) -> Option<Range<u64>>;

fn ci_extent(c: &ColumnChunkMetaData) -> Option<Range<u64>> {
    let off = u64::try_from(c.column_index_offset()?).ok()?;
    let len = u64::try_from(c.column_index_length()?).ok()?;
    Some(off..off + len)
}

fn oi_extent(c: &ColumnChunkMetaData) -> Option<Range<u64>> {
    let off = u64::try_from(c.offset_index_offset()?).ok()?;
    let len = u64::try_from(c.offset_index_length()?).ok()?;
    Some(off..off + len)
}

/// Fetch the page-index bytes for a decode plan and return one [`BufferChunkReader`]
/// The fetched page-index bytes for a decode plan. The caller builds a transient
/// [`BufferChunkReader`] per decode iteration via [`IndexBuffers::reader`] — readers
/// are never stored across the loop.
enum IndexBuffers {
    /// Wide (remote): every plan entry decodes from the ONE whole-region buffer.
    Shared { base: u64, bytes: Bytes },
    /// Narrow (local): one scoped buffer per plan entry, aligned to `plan` order.
    PerEntry(Vec<(u64, Bytes)>),
}

impl IndexBuffers {
    /// Reader for the `i`-th plan entry. A `BufferChunkReader` is a `(u64, Bytes)`
    /// where `Bytes::clone` is a refcount bump, so this is cheap and short-lived.
    fn reader(&self, i: usize) -> BufferChunkReader {
        match self {
            IndexBuffers::Shared { base, bytes } => BufferChunkReader {
                base: *base,
                bytes: bytes.clone(),
            },
            IndexBuffers::PerEntry(v) => BufferChunkReader {
                base: v[i].0,
                bytes: v[i].1.clone(),
            },
        }
    }
}

/// Fetch the page-index bytes for a decode plan. The single place the fetch strategy
/// is decided:
///
/// - **Remote/warm store** ([`is_whole_region_fetch_enabled`]) — ONE `get_ranges`
///   over the file's whole index region (all cols × all RGs; lenient — missing
///   columns skipped). Matches the range key eager warm-population writes, so a
///   warmed file is a hit instead of remote IO. All entries share that one buffer;
///   only the scoped columns are decoded out, so heap is unchanged — just the bytes
///   fetched widen.
/// - **Local store** — the original narrow per-entry fetch of just the scoped
///   columns' extents (fewer bytes; no warm tier to match).
///
/// `extent` selects the index kind. `None` (→ footer-only fallback) if there is no
/// such index region, or — narrow path — any scoped column lacks an extent.
async fn fetch_index_buffers(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    plan: &[RgPlan],
    extent: ChunkExtent,
) -> Option<IndexBuffers> {
    if super::is_whole_region_fetch_enabled() {
        // Whole region spans ALL cols × ALL RGs, so it's derived from the footer,
        // not the (scoped) plan.
        let region = whole_index_region(footer_meta, extent)?;
        let buffers = store
            .get_ranges(location, std::slice::from_ref(&region))
            .await
            .ok()?;
        return Some(IndexBuffers::Shared {
            base: region.start,
            bytes: buffers.first()?.clone(),
        });
    }
    // Narrow: one scoped fetch per plan entry, reusing each entry's precomputed chunks.
    let mut fetch_ranges: Vec<Range<u64>> = Vec::with_capacity(plan.len());
    for p in plan {
        fetch_ranges.push(union_extent(&p.chunks, extent)?);
    }
    let buffers = store.get_ranges(location, &fetch_ranges).await.ok()?;
    if buffers.len() != fetch_ranges.len() {
        return None;
    }
    Some(IndexBuffers::PerEntry(
        fetch_ranges.iter().map(|r| r.start).zip(buffers).collect(),
    ))
}

/// `min(start)..max(end)` over EVERY `(col, rg)` chunk's `extent`, skipping columns
/// without one (lenient). The whole-file index region for the wide fetch — the
/// single source of truth for the range key warm-population must also write.
fn whole_index_region(
    footer_meta: &Arc<ParquetMetaData>,
    extent: ChunkExtent,
) -> Option<Range<u64>> {
    let num_cols = footer_meta.file_metadata().schema_descr().num_columns();
    let mut acc: Option<Range<u64>> = None;
    for rg in 0..footer_meta.num_row_groups() {
        let rgm = footer_meta.row_group(rg);
        for col in 0..num_cols {
            if let Some(r) = extent(rgm.column(col)) {
                acc = Some(merge(acc, r));
            }
        }
    }
    acc
}

/// Union of `extent` across `chunks`; any missing extent bails to `None` (the narrow
/// path requires all requested columns to have an index, else footer-only fallback).
fn union_extent(chunks: &[ColumnChunkMetaData], extent: ChunkExtent) -> Option<Range<u64>> {
    let mut acc: Option<Range<u64>> = None;
    for c in chunks {
        acc = Some(merge(acc, extent(c)?));
    }
    acc
}

fn merge(acc: Option<Range<u64>>, r: Range<u64>) -> Range<u64> {
    match acc {
        None => r,
        Some(a) => a.start.min(r.start)..a.end.max(r.end),
    }
}

/// A [`ChunkReader`] over an in-memory byte buffer representing the file region
/// `[base, base + bytes.len())`. The arrow-rs page-index readers call
/// `get_bytes(absolute_offset, len)`; we translate into the buffer.
struct BufferChunkReader {
    base: u64,
    bytes: Bytes,
}

impl Length for BufferChunkReader {
    fn len(&self) -> u64 {
        self.base + self.bytes.len() as u64
    }
}

impl ChunkReader for BufferChunkReader {
    type T = buf::Reader<Bytes>;

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
    /// Translate an absolute file offset `start` to a buffer-relative index.
    /// The fork's `read_columns_indexes`/`read_offset_indexes` call `get_bytes`
    /// with absolute file offsets (from chunk metadata); `self.base` is the
    /// absolute start of the fetched buffer, so `start - base` gives the
    /// position within `self.bytes`.
    fn rel(&self, start: u64, length: usize) -> ParquetResult<usize> {
        let rel = start.checked_sub(self.base).ok_or_else(|| {
            ParquetError::General(format!(
                "page-index read offset {start} precedes buffer base {}",
                self.base
            ))
        })?;
        let rel = usize::try_from(rel)
            .map_err(|e| ParquetError::General(format!("offset overflow: {e}")))?;
        if rel + length > self.bytes.len() {
            return Err(ParquetError::General(format!(
                "page-index read [{rel}..{}) exceeds buffer of len {}",
                rel + length,
                self.bytes.len()
            )));
        }
        Ok(rel)
    }
}
#[cfg(test)]
mod tests {
    use super::super::column_schema_resolver::{
        resolve_predicate_parquet_columns, resolve_predicate_parquet_columns_pair,
    };
    use super::super::{
        clear_scoped_cache_for_test, column_index_cache_stats, offset_index_cache_stats,
        scoped_cache_stats, set_column_index_cache_limit_for_test, set_whole_region_fetch_enabled,
        ScopedCacheStats, SCOPED_CACHE_TEST_GUARD,
    };
    use super::*;
    use crate::indexed_table::page_pruner::{build_pruning_predicate, PagePruner};
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::parquet::arrow::arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
    };
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
    use datafusion::physical_expr::PhysicalExpr;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjPath;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::super::SCOPED_CACHE_TEST_GUARD as CACHE_TEST_GUARD;

    // ── fixtures + expr helpers ──────────────────────────────────────────

    /// 2 columns (`price`, `qty`), 32 rows, 1 row group, 4 pages of 8 rows.
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

    /// 4 row groups of 10 rows (`id` 0..40, `v` = id*2), page size 5.
    fn four_rg_parquet() -> (Bytes, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let ids: Vec<i32> = (0..40).collect();
        let vs: Vec<i32> = (0..40).map(|x| x * 2).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(vs)),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(10)
            .set_data_page_row_count_limit(5)
            .set_write_batch_size(5)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut buf: Vec<u8> = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        (Bytes::from(buf), schema)
    }

    /// 4 columns (2 int `n0`,`n1` + 2 wide string `s0`,`s1`), 1 RG, multiple pages.
    fn wide4_parquet() -> (Bytes, SchemaRef) {
        use arrow::array::StringArray;
        let schema = Arc::new(Schema::new(vec![
            Field::new("n0", DataType::Int32, false),
            Field::new("n1", DataType::Int32, false),
            Field::new("s0", DataType::Utf8, false),
            Field::new("s1", DataType::Utf8, false),
        ]));
        const ROWS: i32 = 256;
        let n0: Vec<i32> = (0..ROWS).collect();
        let n1: Vec<i32> = (0..ROWS).collect();
        let s0: Vec<String> = (0..ROWS).map(|r| format!("s0_{r:05}_padpadpad")).collect();
        let s1: Vec<String> = (0..ROWS).map(|r| format!("s1_{r:05}_padpadpad")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(n0)),
                Arc::new(Int32Array::from(n1)),
                Arc::new(StringArray::from(s0)),
                Arc::new(StringArray::from(s1)),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(ROWS as usize)
            .set_data_page_row_count_limit(32)
            .set_write_batch_size(32)
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
        store
            .put(&loc, PutPayload::from_bytes(bytes))
            .await
            .unwrap();
        (store, loc)
    }

    fn footer_only(bytes: &Bytes) -> Arc<ParquetMetaData> {
        ArrowReaderMetadata::load(
            &bytes.clone(),
            ArrowReaderOptions::new().with_page_index(false),
        )
        .unwrap()
        .metadata()
        .clone()
    }

    fn full_index(bytes: &Bytes) -> Arc<ParquetMetaData> {
        ArrowReaderMetadata::load(
            &bytes.clone(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap()
        .metadata()
        .clone()
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
    fn ci() -> ScopedCacheStats {
        column_index_cache_stats()
    }
    fn oi() -> ScopedCacheStats {
        offset_index_cache_stats()
    }

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
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(bytes.clone(), arm);
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

    // ── baseline / correctness ────────────────────────────────────────────

    #[tokio::test]
    async fn footer_only_has_no_page_index() {
        let (bytes, _schema) = two_col_parquet();
        let fo = footer_only(&bytes);
        assert!(fo.column_index().is_none());
        assert!(fo.offset_index().is_none());
    }

    #[tokio::test]
    async fn empty_column_set_returns_none() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, _schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        assert!(load_scoped_page_index_cols(&store, &loc, &fo, &[], &[])
            .await
            .is_none());
        assert_eq!(ci().entries, 0);
        assert_eq!(oi().entries, 0);
    }

    #[tokio::test]
    async fn scoped_index_is_predicate_scoped_for_column_index() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);

        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        assert_eq!(cols, vec![0]);

        let aug = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[])
            .await
            .unwrap();
        let c = aug.column_index().unwrap();
        let o = aug.offset_index().unwrap();
        assert!(
            !matches!(c[0][0], ColumnIndexMetaData::NONE),
            "predicate col has real CI"
        );
        assert!(
            matches!(c[0][1], ColumnIndexMetaData::NONE),
            "non-predicate col CI is NONE"
        );
        assert!(
            !o[0][0].page_locations().is_empty() && !o[0][1].page_locations().is_empty(),
            "OffsetIndex real for every column (all-col default)"
        );
    }

    // The pair-resolve must return exactly what two separate single-name resolves
    // return — it only shares the per-file arrow-schema derivation, nothing else.
    #[tokio::test]
    async fn resolve_pair_equals_two_single_resolves() {
        let (bytes, schema) = two_col_parquet();
        let fo = footer_only(&bytes);
        let names_a = vec!["price".to_string()];
        let names_b = vec!["qty".to_string(), "price".to_string()];
        let mut single_a = resolve_predicate_parquet_columns(&schema, &fo, &names_a);
        let mut single_b = resolve_predicate_parquet_columns(&schema, &fo, &names_b);
        let (mut pair_a, mut pair_b) =
            resolve_predicate_parquet_columns_pair(&schema, &fo, &names_a, &names_b);
        single_a.sort_unstable();
        single_b.sort_unstable();
        pair_a.sort_unstable();
        pair_b.sort_unstable();
        assert_eq!(pair_a, single_a, "pair predicate result must match single");
        assert_eq!(pair_b, single_b, "pair projection result must match single");
        assert_eq!(pair_a, vec![0]);
        assert_eq!(pair_b, vec![0, 1]);
    }

    /// Regression: a match()-only query has NO residual predicate columns
    /// (`parquet_cols` empty) but DOES project columns (`offset_cols` non-empty).
    /// The scoped load must still build the OffsetIndex for the projected column
    /// so the parquet reader fetches only matched-row pages, not whole chunks.
    /// (Bug: the load short-circuited on empty `parquet_cols`, skipping the
    /// OffsetIndex → reader over-read ~2.5× the bytes on `... | stats ... by URL`.)
    #[tokio::test]
    async fn projection_only_builds_offset_index_without_predicate() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, _schema) = two_col_parquet(); // price=col0, qty=col1
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);

        // No predicate columns; project qty (col 1).
        let parquet_cols: Vec<usize> = vec![];
        let offset_cols: Vec<usize> = vec![1];
        let aug = load_scoped_page_index_cols(&store, &loc, &fo, &parquet_cols, &offset_cols)
            .await
            .expect("projection-only load must produce grafted metadata (not None)");

        // No predicate → no ColumnIndex grafted.
        assert!(
            aug.column_index().is_none(),
            "no predicate → ColumnIndex absent"
        );

        // OffsetIndex must be real for the projected col (1) AND col 0 (loader
        // always unions in {0}); other behavior unchanged.
        let o = aug.offset_index().expect("OffsetIndex must be grafted");
        assert!(
            !o[0][1].page_locations().is_empty(),
            "projected col qty has real OffsetIndex"
        );
        assert!(
            !o[0][0].page_locations().is_empty(),
            "col 0 OffsetIndex real (always unioned)"
        );
    }

    #[tokio::test]
    async fn scoped_pruning_matches_full_index() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let aug = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[])
            .await
            .unwrap();
        let full = full_index(&bytes);
        let pp =
            build_pruning_predicate(&pred("price", 0, Operator::GtEq, 20), schema.clone()).unwrap();
        let s = PagePruner::new(&schema, Arc::clone(&aug)).prune_rg(&pp, 0, None);
        let f = PagePruner::new(&schema, full).prune_rg(&pp, 0, None);
        assert_eq!(s.as_ref().map(kept), f.as_ref().map(kept));
        assert_eq!(s.as_ref().map(kept), Some(16));
    }

    /// Schema-evolution fixture: a file whose physical layout is `[extra, price]`
    /// (so `price` is parquet leaf **1**), 32 rows / 4 pages. Used to prove the
    /// predicate→leaf resolution does NOT depend on a column's position in a wider
    /// *union* schema.
    fn evolved_extra_price_parquet() -> Bytes {
        let schema = Arc::new(Schema::new(vec![
            Field::new("extra", DataType::Int32, false),
            Field::new("price", DataType::Int32, false),
        ]));
        let extra: Vec<i32> = (1000..1032).collect();
        let prices: Vec<i32> = (0..32).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(extra)),
                Arc::new(Int32Array::from(prices)),
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
        Bytes::from(buf)
    }

    /// Regression for the schema-evolution wrong-count bug: when the query's
    /// (union) schema lists `price` at a DIFFERENT position than the file's
    /// physical layout, the predicate must still resolve to the file's TRUE leaf
    /// and scoped pruning must match the full-index pruning. Previously the
    /// resolver used the union-schema position, scoped the page index at the wrong
    /// leaf, and the residual mis-pruned → over-count.
    #[tokio::test]
    async fn scoped_resolution_is_per_file_under_schema_evolution() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let bytes = evolved_extra_price_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);

        // The UNION/table schema the query carries: `price` at position 0 — but in
        // THIS file `price` is physically leaf 1 (after `extra`).
        let union_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int32, false),
            Field::new("qty", DataType::Int32, false),
            Field::new("extra", DataType::Int32, false),
        ]));

        // Must resolve to the file's TRUE leaf for `price` = 1, NOT the union
        // position 0 (which is `extra` in this file).
        let cols = resolve_predicate_parquet_columns(&union_schema, &fo, &["price".to_string()]);
        assert_eq!(
            cols,
            vec![1],
            "price must resolve to its per-file leaf (1), not union pos 0"
        );

        let aug = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[])
            .await
            .unwrap();
        let full = full_index(&bytes);
        // `price` pages: 0..8,8..16,16..24,24..32; `price >= 20` keeps the last two
        // pages (rows 16..32 = 16 rows). Build the pruning predicate against the
        // FILE schema (price at index 1) so the converter matches the data.
        let file_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("extra", DataType::Int32, false),
            Field::new("price", DataType::Int32, false),
        ]));
        let pp =
            build_pruning_predicate(&pred("price", 1, Operator::GtEq, 20), file_schema.clone())
                .unwrap();
        let s = PagePruner::new(&file_schema, Arc::clone(&aug)).prune_rg(&pp, 0, None);
        let f = PagePruner::new(&file_schema, full).prune_rg(&pp, 0, None);
        assert_eq!(
            s.as_ref().map(kept),
            f.as_ref().map(kept),
            "scoped pruning must match full index"
        );
        assert_eq!(s.as_ref().map(kept), Some(16));
        clear_scoped_cache_for_test();
    }

    /// Page pruning over a column-scoped index must be a SAFE SUPERSET of the
    /// full-index pruning — never drop a row the full index would keep (that
    /// would be an under-count / lost result). It MAY keep extra rows (the
    /// residual mask drops them post-decode), so equality is NOT required and is
    /// the wrong invariant.
    ///
    /// The hazard this guards: with the page index scoped to `price` only, `qty`
    /// is a one-page non-panicking OffsetIndex placeholder with a `NONE`
    /// ColumnIndex. If the pruner TRUSTED that placeholder's (absent) stats it
    /// would build a bogus single-page grid for `qty` and could mis-prune. The
    /// `page_pruner` fix treats a `NONE`-ColumnIndex column as "no usable stats"
    /// (like a schema-evolution-absent column) → it contributes "unknown" and
    /// never prunes on `qty`, so the scoped result stays a conservative superset.
    #[tokio::test]
    async fn scoped_pruning_is_safe_superset_with_placeholdered_residual_col() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        // price 0..32 (pages 0..8,8..16,16..24,24..32); qty 100..132 (pages
        // 100..108,108..116,116..124,124..132). 1 RG, 4 pages each.
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);

        // Scope the page index to `price` ONLY (mimics the predicate-scoped indexed
        // path). `qty` therefore gets the one-page placeholder + NONE ColumnIndex.
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        assert_eq!(cols, vec![0]);
        let aug = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[])
            .await
            .unwrap();
        let full = full_index(&bytes);

        // Residual references BOTH columns: price >= 16 (full keeps pages 2,3) AND
        // qty <= 115 (full keeps pages 0,1) → full intersection prunes to 0 rows.
        let price_ge = pred("price", 0, Operator::GtEq, 16);
        let qty_le = pred("qty", 1, Operator::LtEq, 115);
        let residual: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(price_ge, Operator::And, qty_le));
        let pp = build_pruning_predicate(&residual, schema.clone()).unwrap();

        let s_kept = PagePruner::new(&schema, Arc::clone(&aug))
            .prune_rg(&pp, 0, None)
            .map(|s| kept(&s));
        let f_kept = PagePruner::new(&schema, full)
            .prune_rg(&pp, 0, None)
            .map(|s| kept(&s));
        // Superset invariant: scoped must keep AT LEAST what full keeps (never
        // fewer). It keeps more here (16 vs 0) because it correctly cannot prune
        // the placeholdered `qty` — that's safe; the residual mask removes the
        // extras post-decode. A scoped result SMALLER than full would be the real
        // bug (lost rows). `None` = "kept everything" (no pruning) = the maximal
        // superset, also safe.
        let s = s_kept.unwrap_or(usize::MAX);
        let f = f_kept.unwrap_or(usize::MAX);
        assert!(
            s >= f,
            "scoped page pruning must be a safe superset of full ({} kept) but kept fewer ({})",
            f,
            s
        );
        clear_scoped_cache_for_test();
    }

    #[tokio::test]
    async fn scoped_index_reads_non_predicate_projected_column() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let aug = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[])
            .await
            .unwrap();
        let selection = RowSelection::from(vec![RowSelector::skip(16), RowSelector::select(16)]);
        let scoped_vals = read_selected_column(&bytes, &aug, 1, selection.clone()).unwrap();
        let full = full_index(&bytes);
        let full_vals = read_selected_column(&bytes, &full, 1, selection).unwrap();
        let expected: Vec<i32> = (116..132).collect();
        assert_eq!(scoped_vals, expected);
        assert_eq!(scoped_vals, full_vals);
    }

    // ── cache behavior: hits, independence, eviction ──────────────────────

    /// Second identical load is a pure hit in BOTH caches; no new cells/bytes.
    /// Cells: predicate `price` → 1 CI cell `(col0,rg0)`; all-column OffsetIndex
    /// (the default) → 2 OI cells (one per column).
    #[tokio::test]
    async fn second_load_is_cache_hit() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);

        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[])
            .await
            .unwrap();
        let (c1, o1) = (ci(), oi());
        assert_eq!(
            (c1.hits, c1.misses, c1.entries),
            (0, 1, 1),
            "1 CI cell (price,rg0)"
        );
        assert_eq!(
            (o1.hits, o1.misses, o1.entries),
            (0, 2, 2),
            "2 OI cells (col0,col1)"
        );
        assert!(c1.used_bytes > 0 && o1.used_bytes > 0);

        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[])
            .await
            .unwrap();
        let (c2, o2) = (ci(), oi());
        assert_eq!(
            (c2.hits, c2.misses, c2.entries, c2.used_bytes),
            (1, 1, 1, c1.used_bytes)
        );
        assert_eq!(
            (o2.hits, o2.misses, o2.entries, o2.used_bytes),
            (2, 2, 2, o1.used_bytes)
        );
    }

    /// Distinct predicate columns → distinct CI cells, but the OffsetIndex column
    /// cells are SHARED. Both loads default to the all-column OffsetIndex, so the
    /// second load re-reads the SAME 2 OI cells from cache (no new cells). This is
    /// the whole point of cell-keying: a column's index is stored once per file.
    #[tokio::test]
    async fn distinct_predicates_share_offset_index() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let c_price = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let c_qty = resolve_predicate_parquet_columns(&schema, &fo, &["qty".to_string()]);

        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_price, &[])
            .await
            .unwrap();
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_qty, &[])
            .await
            .unwrap();

        assert_eq!(
            ci().entries,
            2,
            "distinct predicate cells: (price,rg0) + (qty,rg0)"
        );
        assert_eq!(
            oi().entries,
            2,
            "all-column OffsetIndex: 2 column cells, shared"
        );
        // Second (qty) load re-read the same 2 OI cells from cache.
        assert_eq!(oi().hits, 2);
    }

    /// The cell-keying payoff: a predicate that ADDS a column reuses the cell the
    /// first predicate already decoded, instead of re-decoding it inside a new
    /// set-keyed entry. `price` then `{price, qty}` → `price`'s cell is a HIT; only
    /// `qty`'s cell is freshly decoded. (Under the old set-keyed cache this was a
    /// full miss that re-decoded `price`.)
    #[tokio::test]
    async fn adding_predicate_column_reuses_existing_cell() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let c_price = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let c_both = resolve_predicate_parquet_columns(
            &schema,
            &fo,
            &["price".to_string(), "qty".to_string()],
        );

        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_price, &[])
            .await
            .unwrap();
        assert_eq!(
            (ci().hits, ci().misses, ci().entries),
            (0, 1, 1),
            "price cell decoded"
        );

        // Predicate now covers {price, qty}: price's cell hits, qty's cell misses.
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_both, &[])
            .await
            .unwrap();
        assert_eq!(
            (ci().hits, ci().misses, ci().entries),
            (1, 2, 2),
            "price cell reused (hit); only qty cell freshly decoded"
        );
        clear_scoped_cache_for_test();
    }

    /// Two predicates on the SAME column with DIFFERENT literals resolve to the
    /// same `(file, col)` parquet column, so they share the one CI cell — predicate
    /// *value* never multiplies cache entries. (`status>=400` vs `status>=100`.)
    #[tokio::test]
    async fn different_literals_same_column_share_cell() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        // Both predicates are on `price` (col 0) — only the literal differs, which
        // never enters the cache key.
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);

        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[])
            .await
            .unwrap();
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[])
            .await
            .unwrap();
        assert_eq!(
            ci().entries,
            1,
            "same column → one cell regardless of literal"
        );
        assert_eq!(ci().hits, 1);
        clear_scoped_cache_for_test();
    }

    /// CI hit/miss accounting across two predicate-column sets.
    #[tokio::test]
    async fn stats_count_hits_and_misses() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let c_price = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let c_qty = resolve_predicate_parquet_columns(&schema, &fo, &["qty".to_string()]);

        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_price, &[])
            .await
            .unwrap();
        assert_eq!((ci().hits, ci().misses), (0, 1));
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_price, &[])
            .await
            .unwrap();
        assert_eq!((ci().hits, ci().misses), (1, 1));
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_qty, &[])
            .await
            .unwrap();
        assert_eq!((ci().hits, ci().misses), (1, 2));
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_price, &[])
            .await
            .unwrap();
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_qty, &[])
            .await
            .unwrap();
        let s = ci();
        assert_eq!((s.hits, s.misses, s.entries, s.evictions), (3, 2, 2, 0));
    }

    /// Byte-bounded LRU on the (now cell-keyed) ColumnIndex cache: with the budget
    /// sized to hold ~1.5 cells, loading two distinct column cells evicts the LRU
    /// one; the cache never exceeds its limit and never degrades to "cache
    /// nothing"; the most-recently-used cell survives.
    #[tokio::test]
    async fn lru_evicts_over_byte_budget() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let c_price = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let c_qty = resolve_predicate_parquet_columns(&schema, &fo, &["qty".to_string()]);

        // Measure one CI cell (predicate `price` = col0 at the single RG), then set
        // a budget of ~1.5 cells so a second distinct cell forces an eviction.
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_price, &[])
            .await
            .unwrap();
        let one_cell = ci().used_bytes;
        assert!(one_cell > 0);
        let budget = one_cell + one_cell / 2;
        clear_scoped_cache_for_test();
        set_column_index_cache_limit_for_test(budget);

        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_price, &[])
            .await
            .unwrap(); // cell (col0)
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_qty, &[])
            .await
            .unwrap(); // cell (col1) → evicts col0

        assert!(
            ci().used_bytes <= budget,
            "CI bytes {} must stay within {}",
            ci().used_bytes,
            budget
        );
        assert_eq!(ci().entries, 1, "only the most-recent cell fits");
        assert!(ci().evictions >= 1, "the LRU cell must have evicted");

        // The most-recently-used cell (qty/col1) must still be a hit.
        let hits_before = ci().hits;
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_qty, &[])
            .await
            .unwrap();
        assert_eq!(ci().hits, hits_before + 1, "MRU cell must remain cached");

        clear_scoped_cache_for_test();
    }

    /// CI cells are keyed per `(file, col, rg)`. Loading a predicate column on a
    /// 4-RG file caches 4 cells (one per RG); a repeat query hits all 4.
    #[tokio::test]
    async fn rg_scoped_key_includes_surviving_rgs() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = four_rg_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["id".to_string()]);

        // Cold: 4 RGs × 1 predicate col → 4 CI misses, 4 CI entries.
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[])
            .await
            .unwrap();
        assert_eq!(
            (ci().misses, ci().entries),
            (4, 4),
            "4 RGs × col → 4 CI cells"
        );

        // Warm: all 4 cells hit, entry count unchanged.
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[])
            .await
            .unwrap();
        assert_eq!((ci().hits, ci().entries), (4, 4), "warm: all 4 cells hit");

        // OI: empty projection → all 2 columns × 1 file → 2 OI entries.
        assert_eq!(oi().entries, 2, "OI: 2 cols (all cols) × 1 file");
        clear_scoped_cache_for_test();
    }

    /// The combined payoff across BOTH axes: a second query that adds a new
    /// predicate column AND scans a wider RG set decodes only the genuinely new
    /// `(col, rg)` cells. Uses `wide4` (1 RG) for the column axis and asserts CI
    /// cell-level hit/miss deltas.
    #[tokio::test]
    async fn new_column_combination_caches_only_new_column_cells() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = wide4_parquet(); // n0,n1,s0,s1 — 1 RG
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let c_n0 = resolve_predicate_parquet_columns(&schema, &fo, &["n0".to_string()]);
        let c_n0_n1 =
            resolve_predicate_parquet_columns(&schema, &fo, &["n0".to_string(), "n1".to_string()]);
        let c_n1_s0 =
            resolve_predicate_parquet_columns(&schema, &fo, &["n1".to_string(), "s0".to_string()]);

        // {n0}: 1 new cell.
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_n0, &[])
            .await
            .unwrap();
        assert_eq!((ci().hits, ci().misses, ci().entries), (0, 1, 1));
        // {n0,n1}: n0 hits, n1 new.
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_n0_n1, &[])
            .await
            .unwrap();
        assert_eq!(
            (ci().hits, ci().misses, ci().entries),
            (1, 2, 2),
            "n0 reused; n1 new"
        );
        // {n1,s0}: n1 hits, s0 new.
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &c_n1_s0, &[])
            .await
            .unwrap();
        assert_eq!(
            (ci().hits, ci().misses, ci().entries),
            (2, 3, 3),
            "n1 reused; s0 new"
        );
        clear_scoped_cache_for_test();
    }

    /// OffsetIndex equivalent: different projections cache only the new column
    /// cells. Project {s0} (offset cols n1∪s0∪{0}), then {s1} (offset cols
    /// n1∪s1∪{0}) — the shared cols (0, n1) hit; only the genuinely new projected
    /// column is decoded.
    #[tokio::test]
    async fn different_projections_cache_only_new_offset_columns() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = wide4_parquet(); // n0=0,n1=1,s0=2,s1=3 — 1 RG
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let pred_cols = resolve_predicate_parquet_columns(&schema, &fo, &["n1".to_string()]);

        // Project s0 (col 2): offset cols = {0, 1(n1), 2(s0)} → 3 new cells.
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[2])
            .await
            .unwrap();
        assert_eq!(
            (oi().hits, oi().misses, oi().entries),
            (0, 3, 3),
            "cols 0,1,2"
        );

        // Project s1 (col 3): offset cols = {0, 1, 3}. Cols 0 & 1 hit; col 3 new.
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[3])
            .await
            .unwrap();
        assert_eq!(
            (oi().hits, oi().misses, oi().entries),
            (2, 4, 4),
            "cols 0,1 reused (2 hits); only col 3 freshly decoded"
        );
        clear_scoped_cache_for_test();
    }

    // ── Step 2: column-scoping the OffsetIndex ────────────────────────────

    #[tokio::test]
    async fn col_scoped_offset_index_only_for_requested_columns() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = wide4_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let pred_cols = resolve_predicate_parquet_columns(&schema, &fo, &["n1".to_string()]);
        assert_eq!(pred_cols, vec![1]);

        let aug = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[2])
            .await
            .unwrap();
        let o = aug.offset_index().unwrap();
        // wide4 has 256 rows / page-size 32 → real columns have multiple pages.
        let full = full_index(&bytes);
        let real_pages = full.offset_index().unwrap()[0][0].page_locations().len();
        assert!(real_pages > 1, "fixture should have multi-page columns");
        // Scoped columns (predicate 1 ∪ projection 2 ∪ metric 0) carry the REAL
        // page index; the rest carry a single whole-RG placeholder page (non-empty
        // so any consumer dereference is safe — never empty, which would panic).
        for &c in &[0usize, 1, 2] {
            assert_eq!(
                o[0][c].page_locations().len(),
                real_pages,
                "col {c} (pred/proj/metric) real OI"
            );
        }
        assert_eq!(
            o[0][3].page_locations().len(),
            1,
            "col 3 (scoped out) OI is a single-page placeholder, not real and not empty"
        );
    }

    /// Regression: the single-page placeholder for a scoped-OUT column must carry
    /// the column chunk's REAL byte offset + size — not (0, 0). A (0, 0) placeholder
    /// makes arrow derive a fetch byte range with `end < start` when a row selection
    /// actually reads that column (e.g. `match() | sort | head`), and the read path's
    /// `range.end - range.start` then underflows ("subtract with overflow" in debug /
    /// "capacity overflow" in release; see indexed_table/parquet_bridge.rs get_byte_ranges).
    /// The placeholder page must span `[col_start, col_start + col_len)` so any range
    /// derived from it is a valid full-chunk read.
    #[tokio::test]
    async fn placeholder_offset_index_spans_real_chunk_byte_range() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = wide4_parquet(); // n0,n1,s0,s1 — 1 RG, multi-page columns
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        // Scope to n1 (pred) + s0 (proj). Col 3 (s1) is scoped out → placeholder.
        let pred_cols = resolve_predicate_parquet_columns(&schema, &fo, &["n1".to_string()]);
        let aug = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[2])
            .await
            .unwrap();
        let o = aug.offset_index().unwrap();

        // The scoped-out column's placeholder is a single page...
        let ph_pages = o[0][3].page_locations();
        assert_eq!(
            ph_pages.len(),
            1,
            "scoped-out col 3 must have a single placeholder page"
        );
        let ph = &ph_pages[0];

        // ...whose coordinates equal the real column chunk byte range from the footer.
        let (col_start, col_len) = fo.row_group(0).column(3).byte_range();
        assert_eq!(
            ph.offset as u64, col_start,
            "placeholder page offset must be the real chunk start (not 0)"
        );
        assert_eq!(
            ph.compressed_page_size as u64, col_len,
            "placeholder page size must be the real chunk compressed size (not 0)"
        );
        assert!(
            col_start > 0 && col_len > 0,
            "fixture sanity: real chunk has non-zero offset+size"
        );

        // The byte range a reader derives from this page must NOT underflow:
        // end (offset+size) >= start (offset).
        let end = ph.offset as u64 + ph.compressed_page_size as u64;
        assert!(
            end >= ph.offset as u64,
            "derived range must not underflow (end >= start)"
        );
        assert_eq!(
            ph.first_row_index, 0,
            "single placeholder page starts at row 0"
        );
        clear_scoped_cache_for_test();
    }

    #[tokio::test]
    async fn col_scoped_reads_projected_non_predicate_column() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let pred_cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let aug = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[1])
            .await
            .unwrap();
        let selection = RowSelection::from(vec![RowSelector::skip(16), RowSelector::select(16)]);
        let scoped_vals = read_selected_column(&bytes, &aug, 1, selection.clone()).unwrap();
        let full = full_index(&bytes);
        let full_vals = read_selected_column(&bytes, &full, 1, selection).unwrap();
        let expected: Vec<i32> = (116..132).collect();
        assert_eq!(scoped_vals, expected);
        assert_eq!(scoped_vals, full_vals);
    }

    #[tokio::test]
    async fn col_scoping_reduces_offset_index_bytes() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = wide4_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let pred_cols = resolve_predicate_parquet_columns(&schema, &fo, &["n1".to_string()]);

        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[])
            .await
            .unwrap();
        let all_cols = oi().used_bytes;
        clear_scoped_cache_for_test();
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[2])
            .await
            .unwrap();
        assert!(
            oi().used_bytes < all_cols,
            "col-scoped OI {} < all-col {}",
            oi().used_bytes,
            all_cols
        );
        clear_scoped_cache_for_test();
    }

    /// Cell-keying makes OffsetIndex reuse automatic: an all-columns load caches
    /// per-column cells, and a later column-scoped load whose set is covered by
    /// those cells hits them — no new entries, no special "collapse to all-columns
    /// sentinel" needed (the prior set-keyed design's mechanism).
    #[tokio::test]
    async fn col_scoping_full_coverage_collapses_to_all_columns_entry() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let pred_cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);

        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[])
            .await
            .unwrap();
        assert_eq!(oi().entries, 2, "all-columns load caches 2 column cells");
        // Project {1}; union {0,1} = both columns, both already cached → 2 hits.
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[1])
            .await
            .unwrap();
        assert_eq!(
            oi().entries,
            2,
            "covered columns reuse their cells, no new entries"
        );
        assert_eq!(oi().hits, 2);
        clear_scoped_cache_for_test();
    }

    /// CI (all RGs) + OI column-scoped: both axes populated in one call.
    #[tokio::test]
    async fn fully_scoped_load_combines_both_axes() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = four_rg_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let pred_cols = resolve_predicate_parquet_columns(&schema, &fo, &["id".to_string()]);
        let proj_cols = resolve_predicate_parquet_columns(&schema, &fo, &["val".to_string()]);

        let aug = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &proj_cols)
            .await
            .unwrap();
        let c = aug.column_index().unwrap();
        // All 4 RGs built for the predicate column (id = col 0).
        assert!(!matches!(c[0][0], ColumnIndexMetaData::NONE), "RG0 real CI");
        assert!(!matches!(c[2][0], ColumnIndexMetaData::NONE), "RG2 real CI");
        // CI: 4 RGs × 1 pred col = 4 cells; OI: {id(0), val(1)} × 1 file = 2 cells.
        assert_eq!(ci().entries, 4, "4 RGs × 1 pred col");
        assert_eq!(oi().entries, 2, "2 proj cols × 1 file");
        clear_scoped_cache_for_test();
    }

    // ── Eviction on file deletion ─────────────────────────────────────────────

    /// Evicting a file removes ALL its CI and OI cells from the caches —
    /// a subsequent load for the same path is a miss, not a stale hit.
    #[tokio::test]
    async fn evict_file_clears_all_cells_for_that_path() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);

        // Warm: 1 CI cell (price,rg0) + 2 OI cells (col0, col1).
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[0, 1])
            .await
            .unwrap();
        assert_eq!(ci().entries, 1);
        assert_eq!(oi().entries, 2);
        assert_eq!(ci().misses, 1);

        // Evict the file.
        super::super::evict_file_from_scoped_cache(loc.as_ref());
        assert_eq!(ci().entries, 0, "CI cells must be gone after eviction");
        assert_eq!(oi().entries, 0, "OI cells must be gone after eviction");

        // Reload — must be a miss, not a hit from stale cache.
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[0, 1])
            .await
            .unwrap();
        assert_eq!(
            ci().misses,
            2,
            "second load after eviction must be a cache miss"
        );
        assert_eq!(
            ci().hits,
            0,
            "no hits — eviction prevented serving stale data"
        );

        clear_scoped_cache_for_test();
    }

    /// Evicting file A does not remove cells for file B. Cross-file isolation.
    #[tokio::test]
    async fn evict_file_does_not_affect_other_files() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let cols = resolve_predicate_parquet_columns(
            &schema,
            &footer_only(&bytes),
            &["price".to_string()],
        );

        // Stage two identical files at different paths.
        let store_a: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let loc_a = object_store::path::Path::from("file_a.parquet");
        let loc_b = object_store::path::Path::from("file_b.parquet");
        store_a
            .put(&loc_a, object_store::PutPayload::from_bytes(bytes.clone()))
            .await
            .unwrap();
        store_a
            .put(&loc_b, object_store::PutPayload::from_bytes(bytes.clone()))
            .await
            .unwrap();

        let fo = footer_only(&bytes);
        let _ = load_scoped_page_index_cols(&store_a, &loc_a, &fo, &cols, &[0, 1])
            .await
            .unwrap();
        let _ = load_scoped_page_index_cols(&store_a, &loc_b, &fo, &cols, &[0, 1])
            .await
            .unwrap();
        assert_eq!(ci().entries, 2, "one CI cell per file");
        assert_eq!(oi().entries, 4, "two OI cells per file");

        // Evict only file_a.
        super::super::evict_file_from_scoped_cache(loc_a.as_ref());
        assert_eq!(ci().entries, 1, "only file_a's CI cell removed");
        assert_eq!(oi().entries, 2, "only file_a's OI cells removed");

        // file_b's cells are still hits.
        let hits_before = ci().hits;
        let _ = load_scoped_page_index_cols(&store_a, &loc_b, &fo, &cols, &[0, 1])
            .await
            .unwrap();
        assert_eq!(
            ci().hits,
            hits_before + 1,
            "file_b CI cell must still be cached"
        );

        clear_scoped_cache_for_test();
    }

    // ── Whole-region fetch (remote/warm stores) ───────────────────────────────

    /// With whole-region fetch on, the grafted index decodes IDENTICALLY to the
    /// narrow path: scoped pruning still matches the full index, and the decode is
    /// still scoped (one CI cell per (col, rg), not one per column). Only the bytes
    /// fetched widen. `whole_index_region` is a superset of the scoped union, so the
    /// shared buffer must contain every scoped column's absolute offsets.
    #[tokio::test]
    async fn whole_region_fetch_matches_full_index_and_stays_scoped() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        set_whole_region_fetch_enabled(true);

        let (bytes, schema) = four_rg_parquet(); // id, v — 4 RGs, multi-page
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["id".to_string()]);

        // Whole CI region is a strict superset of the scoped `id`-only union.
        let region = whole_index_region(&fo, ci_extent).unwrap();
        let scoped = union_extent(&[fo.row_group(0).column(0).clone()], ci_extent).unwrap();
        assert!(
            region.start <= scoped.start && region.end >= scoped.end,
            "region superset of scoped"
        );

        let aug = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[])
            .await
            .unwrap();

        // Decode stayed scoped: 4 RGs × 1 predicate col = 4 CI cells (not all columns).
        assert_eq!(
            ci().entries,
            4,
            "decode scoped to predicate col despite wide fetch"
        );
        let c = aug.column_index().unwrap();
        assert!(
            matches!(c[0][1], ColumnIndexMetaData::NONE),
            "non-predicate col stays NONE"
        );

        // Pruning identical to the full index.
        let full = full_index(&bytes);
        let pp =
            build_pruning_predicate(&pred("id", 0, Operator::GtEq, 20), schema.clone()).unwrap();
        for rg in 0..4 {
            let s = PagePruner::new(&schema, Arc::clone(&aug)).prune_rg(&pp, rg, None);
            let f = PagePruner::new(&schema, Arc::clone(&full)).prune_rg(&pp, rg, None);
            assert_eq!(
                s.as_ref().map(kept),
                f.as_ref().map(kept),
                "RG{rg} pruning matches full"
            );
        }

        set_whole_region_fetch_enabled(false);
        clear_scoped_cache_for_test();
    }

    /// The OffsetIndex path under whole-region fetch reads the projected column
    /// correctly (same bytes as the full index), proving the shared whole-region
    /// buffer resolves every scoped column's absolute offsets.
    #[tokio::test]
    async fn whole_region_fetch_offset_index_reads_match_full() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        set_whole_region_fetch_enabled(true);

        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let aug = load_scoped_page_index_cols(&store, &loc, &fo, &cols, &[1])
            .await
            .unwrap();

        let selection = RowSelection::from(vec![RowSelector::skip(16), RowSelector::select(16)]);
        let scoped_vals = read_selected_column(&bytes, &aug, 1, selection.clone()).unwrap();
        let full_vals = read_selected_column(&bytes, &full_index(&bytes), 1, selection).unwrap();
        assert_eq!(scoped_vals, (116..132).collect::<Vec<i32>>());
        assert_eq!(scoped_vals, full_vals);

        set_whole_region_fetch_enabled(false);
        clear_scoped_cache_for_test();
    }
}
