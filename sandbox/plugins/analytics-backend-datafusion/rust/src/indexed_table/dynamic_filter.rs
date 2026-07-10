/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Runtime dynamic-filter pruning for the indexed scan.
//!
//! A parent `SortExec`-TopK (or join) pushes a `DynamicFilterPhysicalExpr` into
//! `QueryShardExec` via physical filter pushdown. The filter starts as the
//! `true` placeholder and is *tightened* at runtime (e.g. `ts > <heap-min>`) as
//! the TopK heap fills. [`DynamicRgPruner`] watches that filter and, per row
//! group, decides whether the RG's parquet statistics prove it cannot satisfy
//! the current predicate — in which case the RG is skipped entirely.
//!
//! # Correctness
//!
//! The filter references only the SORT columns. RG statistics span *all* rows in
//! the RG (a superset of the WHERE-passing candidates), so `RG.max <= threshold`
//! implies no candidate in that RG can reach the top-k. Pruning is therefore
//! conservative: we may miss a skip, but never drop a qualifying row. This holds
//! regardless of how the WHERE clause was split between Lucene and parquet. See
//! `docs/dynamic-filters-indexed-table-impl.md` §4b.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{Column, ScalarValue};
use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use datafusion::parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use datafusion::parquet::schema::types::SchemaDescriptor;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::{
    snapshot_generation, snapshot_physical_expr,
};
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};

/// `PruningStatistics` over a single parquet row group (one container). Mirrors
/// DataFusion's private `RowGroupPruningStatistics`
/// (`datasource-parquet/src/row_group_filter.rs`) but scoped to exactly one RG.
struct SingleRowGroupStatistics<'a> {
    parquet_schema: &'a SchemaDescriptor,
    rg_meta: &'a RowGroupMetaData,
    arrow_schema: &'a Schema,
}

impl<'a> SingleRowGroupStatistics<'a> {
    fn converter<'b>(
        &'a self,
        column: &'b Column,
    ) -> datafusion::common::Result<StatisticsConverter<'a>> {
        Ok(StatisticsConverter::try_new(
            &column.name,
            self.arrow_schema,
            self.parquet_schema,
        )?)
    }

    fn iter(&'a self) -> impl Iterator<Item = &'a RowGroupMetaData> + 'a {
        std::iter::once(self.rg_meta)
    }
}

impl PruningStatistics for SingleRowGroupStatistics<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.converter(column)
            .and_then(|c| Ok(c.row_group_mins(self.iter())?))
            .ok()
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.converter(column)
            .and_then(|c| Ok(c.row_group_maxes(self.iter())?))
            .ok()
    }

    fn num_containers(&self) -> usize {
        1
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.converter(column)
            .and_then(|c| Ok(c.row_group_null_counts(self.iter())?))
            .ok()
            .map(|counts| Arc::new(counts) as ArrayRef)
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        let counts: UInt64Array = std::iter::once(Some(self.rg_meta.num_rows() as u64)).collect();
        Some(Arc::new(counts) as ArrayRef)
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

/// Watches a pushed-down dynamic filter and prunes row groups whose parquet
/// statistics cannot satisfy the current (tightening) predicate.
///
/// Cheap in steady state: [`should_prune_rg`] only rebuilds the
/// `PruningPredicate` when the filter's generation actually changes (an atomic
/// read of the snapshot generation), matching DataFusion's `FilePruner`
/// discipline.
pub struct DynamicRgPruner {
    /// The conjoined dynamic filter (still containing `DynamicFilterPhysicalExpr`
    /// nodes). Snapshotted on demand to a concrete predicate.
    filter: Arc<dyn PhysicalExpr>,
    /// Full (parquet) schema — used to build the `PruningPredicate`.
    full_schema: SchemaRef,
    /// Generation of the snapshot currently cached in `pruning_predicate`.
    /// `u64::MAX` means "nothing cached yet" (forces a build on first use).
    cached_generation: u64,
    /// Cached pruning predicate for the current generation. `None` when the
    /// snapshot couldn't be turned into a pruning predicate (e.g. an expression
    /// `PruningPredicate` can't handle) — in which case we never prune.
    pruning_predicate: Option<Arc<PruningPredicate>>,
}

impl DynamicRgPruner {
    /// Build a pruner over `filter`. Returns `None` if `filter` is `None`
    /// (no dynamic filter was accepted), so the hot path can skip all work.
    pub fn new(filter: Option<Arc<dyn PhysicalExpr>>, full_schema: SchemaRef) -> Option<Self> {
        filter.map(|filter| Self {
            filter,
            full_schema,
            cached_generation: u64::MAX,
            pruning_predicate: None,
        })
    }

    /// Refresh the cached `PruningPredicate` if the dynamic filter has changed
    /// since the last call. Cheap when unchanged (just an atomic generation
    /// read). Returns the current generation.
    fn refresh(&mut self) -> u64 {
        let generation = snapshot_generation(&self.filter);
        if generation == self.cached_generation && self.pruning_predicate.is_some() {
            return generation;
        }
        if generation == self.cached_generation {
            // Same generation, but we have no predicate cached (build failed or
            // first call with a non-prunable snapshot) — nothing to redo.
            return generation;
        }
        self.cached_generation = generation;
        // Flatten any DynamicFilterPhysicalExpr to its current concrete value.
        self.pruning_predicate = snapshot_physical_expr(Arc::clone(&self.filter))
            .ok()
            .and_then(|snap| {
                PruningPredicate::try_new(snap, Arc::clone(&self.full_schema))
                    .ok()
                    .map(Arc::new)
            });
        generation
    }

    /// The current concrete pruning predicate (snapshotting the dynamic filter
    /// at its tightening so far), plus the schema needed to evaluate it against
    /// RG stats. Generation-gated, so cheap when the filter hasn't moved.
    ///
    /// Handed to the prefetch closure so an RG can be excluded *before* the
    /// (expensive) Lucene/FFM eval runs. `None` when no prunable predicate is
    /// available yet.
    pub fn current_pruning_predicate(&mut self) -> Option<RgPruningContext> {
        self.refresh();
        self.pruning_predicate
            .clone()
            .map(|predicate| RgPruningContext {
                predicate,
                schema: Arc::clone(&self.full_schema),
            })
    }

    /// True if row group `rg_idx` can be skipped: the current predicate proves
    /// that *no* row in the RG (and hence no candidate) can satisfy it.
    ///
    /// Conservative — returns `false` (scan the RG) on any uncertainty: missing
    /// stats, non-prunable predicate, or evaluation error. Never skips on doubt.
    pub fn should_prune_rg(&mut self, metadata: &Arc<ParquetMetaData>, rg_idx: usize) -> bool {
        let Some(ctx) = self.current_pruning_predicate() else {
            return false;
        };
        ctx.rg_provably_excluded(metadata, rg_idx)
    }
}

/// A snapshotted pruning predicate plus the schema to evaluate it. Cheaply
/// cloneable (`Arc` inside) and `Send`, so it can be moved into the prefetch
/// `spawn_blocking` closure to gate the Lucene eval.
#[derive(Clone)]
pub struct RgPruningContext {
    predicate: Arc<PruningPredicate>,
    schema: SchemaRef,
}

impl RgPruningContext {
    /// True if row group `rg_idx`'s parquet statistics prove it cannot satisfy
    /// the predicate. Conservative: `false` on any missing stats / error.
    pub fn rg_provably_excluded(&self, metadata: &Arc<ParquetMetaData>, rg_idx: usize) -> bool {
        let Some(rg_meta) = metadata.row_groups().get(rg_idx) else {
            return false;
        };
        // Resolve stats against the segment's OWN parquet schema, not the full table
        // schema: StatisticsConverter maps name→parquet-column positionally, so the full
        // schema reads the wrong/no column under dynamic-mapping schema drift.
        let descr = metadata.file_metadata().schema_descr();
        let seg_schema = datafusion::parquet::arrow::parquet_to_arrow_schema(
            descr,
            metadata.file_metadata().key_value_metadata(),
        )
        .unwrap_or_else(|_| self.schema.as_ref().clone());
        let stats = SingleRowGroupStatistics {
            parquet_schema: descr,
            rg_meta,
            arrow_schema: &seg_schema,
        };
        // `prune` returns one bool per container (we have exactly one). `false`
        // means "provably cannot match" → safe to skip. Any error => keep.
        match self.predicate.prune(&stats) {
            Ok(keep) => keep.first().is_some_and(|k| !*k),
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::logical_expr::Operator;
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
    use tempfile::NamedTempFile;

    // Schema drift: the table schema orders columns differently from the segment's own
    // parquet file. StatisticsConverter maps a column name to a parquet index positionally,
    // so resolving `severity` against the table schema lands on a DIFFERENT real file column
    // (here `neg`, all-negative). The always-true dynamic filter `severity >= 0` then reads
    // neg's stats (max < 0) and wrongly prunes the whole RG. Resolving against the segment's
    // own schema reads the real `severity` stats and keeps the RG.
    #[test]
    fn dynamic_rg_prune_resolves_stats_against_segment_schema_under_drift() {
        // File order: name, neg, severity.
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Int32, false),
            Field::new("neg", DataType::Int32, false),
            Field::new("severity", DataType::Int32, false),
        ]));
        // Table order puts `severity` at the position the file holds `neg`.
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Int32, false),
            Field::new("severity", DataType::Int32, false),
            Field::new("neg", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int32Array::from(vec![-9, -8, -7, -6])),
                Arc::new(Int32Array::from(vec![0, 5, 10, 17])),
            ],
        )
        .unwrap();
        let tmp = NamedTempFile::new().unwrap();
        let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), file_schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let md = ArrowReaderMetadata::load(&tmp.reopen().unwrap(), ArrowReaderOptions::new())
            .unwrap()
            .metadata()
            .clone();

        let sev: Arc<dyn PhysicalExpr> = Arc::new(PhysColumn::new("severity", 1));
        let zero: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(0))));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(sev, Operator::GtEq, zero));
        let predicate = Arc::new(PruningPredicate::try_new(expr, table_schema.clone()).unwrap());
        let ctx = RgPruningContext {
            predicate,
            schema: table_schema,
        };

        assert!(
            !ctx.rg_provably_excluded(&md, 0),
            "severity >= 0 is always true for this segment; it must not be pruned under schema drift"
        );
    }
}
