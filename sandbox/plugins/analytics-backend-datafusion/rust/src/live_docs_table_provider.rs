/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! TableProvider that wraps a standard parquet scan with per-file liveDocs RowSelection.
//! When deleted docs exist, builds a `ParquetAccessPlan` per file from the liveDocs bitset
//! so parquet physically skips deleted rows during I/O.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, Statistics};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::table_schema::TableSchema;
use datafusion_datasource::PartitionedFile;
use object_store::ObjectMeta;

use datafusion::datasource::physical_plan::parquet::{ParquetAccessPlan, RowGroupAccess};

use crate::indexed_table::ffm_callbacks::get_live_docs;
use crate::indexed_table::parquet_bridge;
use datafusion::execution::cache::cache_manager::FileMetadataCache;

/// Per-file info needed to build the liveDocs RowSelection.
pub struct LiveDocsFileInfo {
    pub object_meta: ObjectMeta,
    pub writer_generation: i64,
    pub num_rows: u64,
    pub row_group_row_counts: Vec<u64>,
}

pub struct LiveDocsTableProvider {
    schema: SchemaRef,
    files: Vec<LiveDocsFileInfo>,
    store_url: ObjectStoreUrl,
    store: Arc<dyn object_store::ObjectStore>,
    metadata_cache: Arc<dyn FileMetadataCache>,
    context_id: i64,
}

impl LiveDocsTableProvider {
    pub fn new(
        schema: SchemaRef,
        files: Vec<LiveDocsFileInfo>,
        store_url: ObjectStoreUrl,
        store: Arc<dyn object_store::ObjectStore>,
        metadata_cache: Arc<dyn FileMetadataCache>,
        context_id: i64,
    ) -> Self {
        Self {
            schema,
            files,
            store_url,
            store,
            metadata_cache,
            context_id,
        }
    }

    /// Build a ParquetAccessPlan from a liveDocs bitset for one file.
    /// Converts the per-doc bitset into per-row-group RowSelections.
    ///
    /// Word-level scan: for the common mostly-alive case (few deletions in a large shard)
    /// almost every 64-bit word is all-ones, so we advance 64 rows at a time for all-ones
    /// (and all-zeros) words and only drop to per-bit for "mixed" words that straddle a
    /// deletion boundary. This is O(total_words + deletions) instead of O(total_rows) — the
    /// per-bit version dominated query latency on large deletion-bearing segments.
    fn build_access_plan(live_docs: &[u64], row_group_row_counts: &[u64]) -> ParquetAccessPlan {
        let num_rgs = row_group_row_counts.len();
        let mut access_plan = ParquetAccessPlan::new_all(num_rgs);

        let mut doc_offset: usize = 0;
        for (rg_idx, &rg_rows) in row_group_row_counts.iter().enumerate() {
            let rg_rows = rg_rows as usize;
            let rg_start = doc_offset;
            doc_offset += rg_rows;
            if rg_rows == 0 {
                continue;
            }

            let (selectors, any_dead) = Self::rg_selectors(live_docs, rg_start, rg_rows);
            // If no deleted rows, leave the row group as Scan (no selection needed) so the
            // parquet reader keeps its efficient full-RG decode path.
            if any_dead {
                access_plan.set(rg_idx, RowGroupAccess::Selection(RowSelection::from(selectors)));
            }
        }
        access_plan
    }

    /// Compute the select/skip run selectors for one row group's slice
    /// `[rg_start, rg_start + rg_rows)` of the doc-level liveDocs bitset. Returns the selectors
    /// and whether any row in the range was deleted (`false` → the caller keeps the RG as Scan).
    ///
    /// Words that are entirely alive (all-ones) or entirely deleted (all-zeros) advance 64 rows
    /// at once; only "mixed" words straddling a deletion boundary are walked bit-by-bit. `cur_len`
    /// starts at 0 on a (nominal) live run so the initial empty run is never emitted.
    /// Count deleted rows (cleared bits) in `[rg_start, rg_start + rg_rows)` of the liveDocs bitset.
    /// Word-level popcount: mask off the partial leading/trailing words and count set (alive) bits,
    /// subtracting from the chunk width to get deletions. Used only to pre-size the selector Vec.
    fn count_deleted(live_docs: &[u64], rg_start: usize, rg_rows: usize) -> usize {
        let mut deleted = 0usize;
        let mut i = 0usize;
        while i < rg_rows {
            let abs = rg_start + i;
            let word_idx = abs >> 6;
            let bit = abs & 63;
            let word = if word_idx < live_docs.len() {
                live_docs[word_idx]
            } else {
                0
            };
            let chunk = (64 - bit).min(rg_rows - i);
            let mask: u64 = if chunk == 64 { u64::MAX } else { (1u64 << chunk) - 1 };
            let seg = (word >> bit) & mask;
            deleted += chunk - seg.count_ones() as usize;
            i += chunk;
        }
        deleted
    }

    fn rg_selectors(live_docs: &[u64], rg_start: usize, rg_rows: usize) -> (Vec<RowSelector>, bool) {
        // Pre-size the selector Vec so scattered deletes don't trigger repeated reallocation.
        // Each deleted row contributes at most two runs (a skip and the following select), so
        // `2 * deleted + 2` bounds the selector count. Cap the pre-allocation at 64Ki entries so a
        // single large contiguous deletion (few runs, but a huge deleted count) can't over-allocate;
        // pathological groups beyond the cap simply grow as before. The popcount scan is O(words)
        // and autovectorizes, so it's cheap next to the bit-walk and the RowSelection::from below.
        let deleted = Self::count_deleted(live_docs, rg_start, rg_rows);
        let cap = (2 * deleted + 2).min(1 << 16);
        let mut selectors: Vec<RowSelector> = Vec::with_capacity(cap);
        let mut cur_live = true;
        let mut cur_len: usize = 0;
        let mut any_dead = false;

        let flush = |sel: &mut Vec<RowSelector>, live: bool, len: usize| {
            if len == 0 {
                return;
            }
            if live {
                sel.push(RowSelector::select(len));
            } else {
                sel.push(RowSelector::skip(len));
            }
        };

        let mut i = 0usize;
        while i < rg_rows {
            let abs = rg_start + i;
            let word_idx = abs >> 6;
            let bit = abs & 63;
            let word = if word_idx < live_docs.len() {
                live_docs[word_idx]
            } else {
                0
            };
            // Bits to consider from this word: bounded by the word boundary and the RG end.
            let chunk = (64 - bit).min(rg_rows - i);
            let mask: u64 = if chunk == 64 { u64::MAX } else { (1u64 << chunk) - 1 };
            let seg = (word >> bit) & mask;

            if seg == mask {
                // All `chunk` rows alive.
                if cur_live {
                    cur_len += chunk;
                } else {
                    flush(&mut selectors, false, cur_len);
                    cur_live = true;
                    cur_len = chunk;
                }
            } else if seg == 0 {
                // All `chunk` rows deleted.
                any_dead = true;
                if !cur_live {
                    cur_len += chunk;
                } else {
                    flush(&mut selectors, true, cur_len);
                    cur_live = false;
                    cur_len = chunk;
                }
            } else {
                // Mixed word — walk the `chunk` bits individually.
                for k in 0..chunk {
                    let live = (seg >> k) & 1 == 1;
                    if !live {
                        any_dead = true;
                    }
                    if live == cur_live {
                        cur_len += 1;
                    } else {
                        flush(&mut selectors, cur_live, cur_len);
                        cur_live = live;
                        cur_len = 1;
                    }
                }
            }
            i += chunk;
        }
        flush(&mut selectors, cur_live, cur_len);
        (selectors, any_dead)
    }
}

impl std::fmt::Debug for LiveDocsTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveDocsTableProvider")
            .field("files", &self.files.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for LiveDocsTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut partitioned_files: Vec<PartitionedFile> = Vec::with_capacity(self.files.len());

        for file_info in &self.files {
            let mut pf = PartitionedFile::from(file_info.object_meta.clone());

            // Load parquet metadata from cache to get RG row counts and actual num_rows.
            let pq_meta_result = parquet_bridge::load_parquet_metadata_with_meta(
                Arc::clone(&self.store),
                &file_info.object_meta.location,
                file_info.object_meta.clone(),
                Arc::clone(&self.metadata_cache),
            )
            .await;

            if let Ok((_schema, _size, pq_meta)) = pq_meta_result {
                let num_rows: i64 = pq_meta
                    .row_groups()
                    .iter()
                    .map(|rg| rg.num_rows())
                    .sum();
                let rg_row_counts: Vec<u64> = pq_meta
                    .row_groups()
                    .iter()
                    .map(|rg| rg.num_rows() as u64)
                    .collect();

                // Fetch liveDocs for this file's segment.
                let live_docs_result = get_live_docs(
                    self.context_id,
                    file_info.writer_generation,
                    0,
                    num_rows as i32,
                );

                match live_docs_result {
                    Ok(Some(bitset)) => {
                        let access_plan = Self::build_access_plan(&bitset, &rg_row_counts);
                        pf = pf.with_extensions(Arc::new(access_plan));
                    }
                    Ok(None) => {
                        // All alive — no access plan needed
                    }
                    Err(_) => {
                        // Error fetching liveDocs — fall back to reading all rows
                    }
                }
            }

            partitioned_files.push(pf);
        }

        let file_groups = vec![FileGroup::new(partitioned_files)];
        let table_schema = TableSchema::new(self.schema.clone(), vec![]);
        let parquet_source = ParquetSource::new(table_schema);

        let mut builder =
            FileScanConfigBuilder::new(self.store_url.clone(), Arc::new(parquet_source))
                .with_file_groups(file_groups);

        if let Some(proj) = projection {
            builder = builder
                .with_projection_indices(Some(proj.clone()))
                .map_err(|e| datafusion::error::DataFusionError::Internal(format!("{}", e)))?;
        }

        let file_scan_config = builder.build();
        Ok(DataSourceExec::from_data_source(file_scan_config))
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Naive reference: per-bit run builder (the pre-optimization logic).
    fn rg_selectors_naive(live_docs: &[u64], rg_start: usize, rg_rows: usize) -> (Vec<RowSelector>, bool) {
        let mut selectors: Vec<RowSelector> = Vec::new();
        let mut any_dead = false;
        let mut i = 0usize;
        while i < rg_rows {
            let abs0 = rg_start + i;
            let is_live = (live_docs[abs0 / 64] >> (abs0 % 64)) & 1 == 1;
            let run_start = i;
            while i < rg_rows {
                let abs = rg_start + i;
                let live = (live_docs[abs / 64] >> (abs % 64)) & 1 == 1;
                if live != is_live {
                    break;
                }
                i += 1;
            }
            let run_len = i - run_start;
            if is_live {
                selectors.push(RowSelector::select(run_len));
            } else {
                any_dead = true;
                selectors.push(RowSelector::skip(run_len));
            }
        }
        (selectors, any_dead)
    }

    /// Expand selectors to a per-row alive/dead vector for comparison.
    fn expand(selectors: &[RowSelector]) -> Vec<bool> {
        let mut out = Vec::new();
        for s in selectors {
            for _ in 0..s.row_count {
                out.push(!s.skip);
            }
        }
        out
    }

    /// The alive vector implied directly by the bitset over [rg_start, rg_start+rg_rows).
    fn alive_slice(live_docs: &[u64], rg_start: usize, rg_rows: usize) -> Vec<bool> {
        (0..rg_rows)
            .map(|i| {
                let abs = rg_start + i;
                (live_docs[abs / 64] >> (abs % 64)) & 1 == 1
            })
            .collect()
    }

    // Deterministic xorshift so the test needs no rng dependency (Math.random-style is banned).
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
    }

    #[test]
    fn rg_selectors_matches_naive_and_bitset() {
        let mut rng = Rng(0x9E3779B97F4A7C15);
        for _ in 0..2000 {
            let rg_rows = (rng.next() % 600) as usize + 1; // 1..=600
            let rg_start = (rng.next() % 200) as usize; // exercise unaligned starts
            let total_bits = rg_start + rg_rows;
            let words = total_bits.div_ceil(64);
            // Mostly-alive bitset with a sprinkling of deletions (the real-world shape).
            let mut live: Vec<u64> = vec![u64::MAX; words + 1];
            let deletes = (rng.next() % 40) as usize;
            for _ in 0..deletes {
                let pos = rg_start + (rng.next() as usize % rg_rows);
                live[pos / 64] &= !(1u64 << (pos % 64));
            }

            let (opt, opt_dead) = LiveDocsTableProvider::rg_selectors(&live, rg_start, rg_rows);
            let (nai, nai_dead) = rg_selectors_naive(&live, rg_start, rg_rows);
            let expected = alive_slice(&live, rg_start, rg_rows);

            // count_deleted (used to pre-size the selector Vec) must match the true deletion count.
            let expected_deleted = expected.iter().filter(|&&alive| alive == false).count();
            assert_eq!(
                LiveDocsTableProvider::count_deleted(&live, rg_start, rg_rows),
                expected_deleted,
                "count_deleted mismatch (rg_start={rg_start}, rg_rows={rg_rows})"
            );

            assert_eq!(expand(&opt), expected, "optimized selectors mismatch bitset");
            assert_eq!(expand(&nai), expected, "naive selectors mismatch bitset");
            assert_eq!(opt_dead, nai_dead, "any_dead mismatch");
            assert_eq!(
                expand(&opt),
                expand(&nai),
                "optimized vs naive selectors differ (rg_start={rg_start}, rg_rows={rg_rows})"
            );
        }
    }

    #[test]
    fn rg_selectors_all_alive_and_all_dead() {
        let live_all = vec![u64::MAX; 4];
        let (sel, dead) = LiveDocsTableProvider::rg_selectors(&live_all, 0, 200);
        assert!(!dead);
        assert_eq!(expand(&sel), vec![true; 200]);

        let dead_all = vec![0u64; 4];
        let (sel, dead) = LiveDocsTableProvider::rg_selectors(&dead_all, 0, 200);
        assert!(dead);
        assert_eq!(expand(&sel), vec![false; 200]);
    }
}
