// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Utilities to push down of DataFusion filter predicates (any DataFusion
//! `PhysicalExpr` that evaluates to a [`BooleanArray`]) to the parquet decoder
//! level in `arrow-rs`.
//!
//! DataFusion will use a `ParquetRecordBatchStream` to read data from parquet
//! into [`RecordBatch`]es.
//!
//! The `ParquetRecordBatchStream` takes an optional `RowFilter` which is itself
//! a Vec of `Box<dyn ArrowPredicate>`. During decoding, the predicates are
//! evaluated in order, to generate a mask which is used to avoid decoding rows
//! in projected columns which do not pass the filter which can significantly
//! reduce the amount of compute required for decoding and thus improve query
//! performance.
//!
//! Since the predicates are applied serially in the order defined in the
//! `RowFilter`, the optimal ordering depends on the exact filters. The best
//! filters to execute first have two properties:
//!
//! 1. They are relatively inexpensive to evaluate (e.g. they read
//!    column chunks which are relatively small)
//!
//! 2. They filter many (contiguous) rows, reducing the amount of decoding
//!    required for subsequent filters and projected columns
//!
//! If requested, this code will reorder the filters based on heuristics try and
//! reduce the evaluation cost.
//!
//! The basic algorithm for constructing the `RowFilter` is as follows
//!
//! 1. Break conjunctions into separate predicates. An expression
//!    like `a = 1 AND (b = 2 AND c = 3)` would be
//!    separated into the expressions `a = 1`, `b = 2`, and `c = 3`.
//! 2. Determine whether each predicate can be evaluated as an `ArrowPredicate`.
//! 3. Determine, for each predicate, the total compressed size of all
//!    columns required to evaluate the predicate.
//! 4. Determine, for each predicate, whether all columns required to
//!    evaluate the expression are sorted.
//! 5. Re-order the predicate by total size (from step 3).
//! 6. Partition the predicates according to whether they are sorted (from step 4)
//! 7. "Compile" each predicate `Expr` to a `DatafusionArrowPredicate`.
//! 8. Build the `RowFilter` with the sorted predicates followed by
//!    the unsorted predicates. Within each partition, predicates are
//!    still be sorted by size.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::datatypes::{DataType, Schema};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::physical_plan::ParquetFileMetrics;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::utils::reassign_expr_columns;
use datafusion::physical_plan::expressions::{BinaryExpr, LikeExpr};
use datafusion::physical_plan::metrics;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowPredicate;
use parquet::file::metadata::ParquetMetaData;

use datafusion::common::Result;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, split_conjunction};

/// A row filter that can be used to filter rows from a parquet file.
pub struct LiquidRowFilter {
    predicates: Vec<LiquidPredicate>,
}

impl LiquidRowFilter {
    /// Create a new `LiquidRowFilter` from a vector of `LiquidPredicate`s.
    pub fn new(predicates: Vec<LiquidPredicate>) -> Self {
        Self { predicates }
    }

    /// Get the predicates of the `LiquidRowFilter`.
    pub fn predicates(&self) -> &[LiquidPredicate] {
        &self.predicates
    }

    /// Get the predicates of the `LiquidRowFilter` as mutable.
    pub fn predicates_mut(&mut self) -> &mut [LiquidPredicate] {
        &mut self.predicates
    }
}

pub(crate) fn get_predicate_column_id(projection: &parquet::arrow::ProjectionMask) -> Vec<usize> {
    #[derive(Debug, Clone)]
    struct ProjectionMaskLiquid {
        mask: Option<Vec<bool>>,
    }
    let project_inner: &ProjectionMaskLiquid = unsafe { std::mem::transmute(projection) };
    project_inner
        .mask
        .as_ref()
        .map(|m| {
            m.iter()
                .enumerate()
                .filter_map(|(pos, &x)| if x { Some(pos) } else { None })
                .collect::<Vec<usize>>()
        })
        .unwrap_or_default()
}

/// A "compiled" predicate passed to `ParquetRecordBatchStream` to perform
/// row-level filtering during parquet decoding.
///
/// See the module level documentation for more information.
///
/// Implements the `ArrowPredicate` trait used by the parquet decoder
///
/// An expression can be evaluated as a `DatafusionArrowPredicate` if it:
/// * Does not reference any projected columns
/// * Does not reference columns with non-primitive types (e.g. structs / lists)
#[derive(Debug, Clone)]
pub struct LiquidPredicate {
    /// the filter expression
    physical_expr: Arc<dyn PhysicalExpr>,
    /// the filter expression without reassigned index
    physical_expr_physical_column_index: Arc<dyn PhysicalExpr>,
    /// Path to the columns in the parquet schema required to evaluate the
    /// expression
    projection_mask: ProjectionMask,
    /// how many rows were filtered out by this predicate
    rows_pruned: metrics::Count,
    /// how many rows passed this predicate
    rows_matched: metrics::Count,
    /// how long was spent evaluating this predicate
    time: metrics::Time,
}

impl std::fmt::Display for LiquidPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.physical_expr.as_ref())
    }
}

impl LiquidPredicate {
    /// Create a new `LiquidPredicate` from a `FilterCandidate`
    pub fn try_new_with_metrics(
        candidate: FilterCandidate,
        projection: ProjectionMask,
        rows_pruned: metrics::Count,
        rows_matched: metrics::Count,
        time: metrics::Time,
    ) -> Result<Self> {
        let physical_expr =
            reassign_expr_columns(candidate.expr.clone(), &candidate.filter_schema)?;

        Ok(Self {
            physical_expr,
            physical_expr_physical_column_index: candidate.expr,
            projection_mask: projection,
            rows_pruned,
            rows_matched,
            time,
        })
    }

    /// Create a new `LiquidPredicate` from a `FilterCandidate`
    pub fn try_new(candidate: FilterCandidate, projection: ProjectionMask) -> Result<Self> {
        Self::try_new_with_metrics(
            candidate,
            projection,
            metrics::Count::new(),
            metrics::Count::new(),
            metrics::Time::new(),
        )
    }

    /// Get the physical expression with physical column index.
    pub fn physical_expr_physical_column_index(&self) -> &Arc<dyn PhysicalExpr> {
        &self.physical_expr_physical_column_index
    }

    /// Get the physical expression rewritten to the projected batch schema.
    pub fn physical_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.physical_expr
    }

    /// Get the column ids of the predicate.
    pub fn predicate_column_ids(&self) -> Vec<usize> {
        let projection = self.projection();
        get_predicate_column_id(projection)
    }
}

impl ArrowPredicate for LiquidPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection_mask
    }

    fn evaluate(&mut self, batch: RecordBatch) -> ArrowResult<BooleanArray> {
        // scoped timer updates on drop
        let mut timer = self.time.timer();

        self.physical_expr
            .evaluate(&batch)
            .and_then(|v| v.into_array(batch.num_rows()))
            .and_then(|array| {
                let bool_arr = as_boolean_array(&array)?.clone();
                let num_matched = bool_arr.true_count();
                let num_pruned = bool_arr.len() - num_matched;
                self.rows_pruned.add(num_pruned);
                self.rows_matched.add(num_matched);
                timer.stop();
                Ok(bool_arr)
            })
            .map_err(|e| {
                ArrowError::ComputeError(format!("Error evaluating filter predicate: {e:?}"))
            })
    }
}

/// A candidate expression for creating a `RowFilter`.
///
/// Each candidate contains the expression as well as data to estimate the cost
/// of evaluating the resulting expression.
///
/// See the module level documentation for more information.
pub struct FilterCandidate {
    expr: Arc<dyn PhysicalExpr>,
    required_bytes: usize,
    can_use_index: bool,
    projection: Vec<usize>,
    /// The projected file schema that this filter references
    filter_schema: SchemaRef,
}

impl FilterCandidate {
    pub fn projection(&self, metadata: &ParquetMetaData) -> ProjectionMask {
        ProjectionMask::roots(
            metadata.file_metadata().schema_descr(),
            self.projection.iter().copied(),
        )
    }
}

/// Helper to build a `FilterCandidate`.
///
/// This will do several things
/// 1. Determine the columns required to evaluate the expression
/// 2. Calculate data required to estimate the cost of evaluating the filter
pub struct FilterCandidateBuilder {
    expr: Arc<dyn PhysicalExpr>,
    /// The schema of this parquet file.
    /// Expressions are already adapted to this schema before row-filter construction.
    file_schema: SchemaRef,
}

impl FilterCandidateBuilder {
    /// Create a new `FilterCandidateBuilder`
    pub fn new(expr: Arc<dyn PhysicalExpr>, file_schema: SchemaRef) -> Self {
        Self { expr, file_schema }
    }

    /// Attempt to build a `FilterCandidate` from the expression
    ///
    /// # Return values
    ///
    /// * `Ok(Some(candidate))` if the expression can be used as an ArrowFilter
    /// * `Ok(None)` if the expression cannot be used as an ArrowFilter
    /// * `Err(e)` if an error occurs while building the candidate
    pub fn build(self, metadata: &ParquetMetaData) -> Result<Option<FilterCandidate>> {
        let Some(required_indices_into_file_schema) =
            pushdown_columns(&self.expr, &self.file_schema)?
        else {
            return Ok(None);
        };

        if required_indices_into_file_schema.is_empty() {
            return Ok(None);
        }

        let projected_file_schema = Arc::new(
            self.file_schema
                .project(&required_indices_into_file_schema)?,
        );

        let required_bytes = size_of_columns(&required_indices_into_file_schema, metadata)?;
        let can_use_index = columns_sorted(&required_indices_into_file_schema, metadata)?;

        Ok(Some(FilterCandidate {
            expr: self.expr,
            required_bytes,
            can_use_index,
            projection: required_indices_into_file_schema,
            filter_schema: Arc::clone(&projected_file_schema),
        }))
    }
}

// a struct that implements TreeNodeRewriter to traverse a PhysicalExpr tree structure to determine
// if any column references in the expression would prevent it from being predicate-pushed-down.
// if non_primitive_columns || projected_columns, it can't be pushed down.
// can't be reused between calls to `rewrite`; each construction must be used only once.
struct PushdownChecker<'schema> {
    /// Does the expression require any non-primitive columns (like structs)?
    non_primitive_columns: bool,
    /// Does the expression reference any columns that are not in the file schema?
    projected_columns: bool,
    // Indices into the file schema of the columns required to evaluate the expression
    required_columns: BTreeSet<usize>,
    file_schema: &'schema Schema,
}

impl<'schema> PushdownChecker<'schema> {
    fn new(file_schema: &'schema Schema) -> Self {
        Self {
            non_primitive_columns: false,
            projected_columns: false,
            required_columns: BTreeSet::default(),
            file_schema,
        }
    }

    fn check_single_column(&mut self, column_name: &str) -> Option<TreeNodeRecursion> {
        if let Ok(idx) = self.file_schema.index_of(column_name) {
            self.required_columns.insert(idx);
            if DataType::is_nested(self.file_schema.field(idx).data_type()) {
                self.non_primitive_columns = true;
                return Some(TreeNodeRecursion::Jump);
            }
        } else {
            // If the column does not exist in the file schema then it cannot be pushed down.
            self.projected_columns = true;
            return Some(TreeNodeRecursion::Jump);
        }

        None
    }

    #[inline]
    fn prevents_pushdown(&self) -> bool {
        self.non_primitive_columns || self.projected_columns
    }
}

impl TreeNodeVisitor<'_> for PushdownChecker<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        if let Some(column) = node.downcast_ref::<Column>()
            && let Some(recursion) = self.check_single_column(column.name())
        {
            return Ok(recursion);
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

// Checks if a given expression can be pushed down into `DataSourceExec` as opposed to being evaluated
// post-parquet-scan in a `FilterExec`. If it can be pushed down, this returns all the
// columns in the given expression so that they can be used in the parquet scanning, along with the
// expression rewritten as defined in [`PushdownChecker::f_up`]
fn pushdown_columns(
    expr: &Arc<dyn PhysicalExpr>,
    file_schema: &Schema,
) -> Result<Option<Vec<usize>>> {
    let mut checker = PushdownChecker::new(file_schema);
    expr.visit(&mut checker)?;
    Ok((!checker.prevents_pushdown()).then_some(checker.required_columns.into_iter().collect()))
}

/// Calculate the total compressed size of all `Column`'s required for
/// predicate `Expr`.
///
/// This value represents the total amount of IO required to evaluate the
/// predicate.
fn size_of_columns(columns: &[usize], metadata: &ParquetMetaData) -> Result<usize> {
    let mut total_size = 0;
    let row_groups = metadata.row_groups();
    for idx in columns {
        for rg in row_groups.iter() {
            total_size += rg.column(*idx).compressed_size() as usize;
        }
    }

    Ok(total_size)
}

/// For a given set of `Column`s required for predicate `Expr` determine whether
/// all columns are sorted.
///
/// Sorted columns may be queried more efficiently in the presence of
/// a PageIndex.
fn columns_sorted(_columns: &[usize], _metadata: &ParquetMetaData) -> Result<bool> {
    // TODO How do we know this?
    Ok(false)
}

/// Build a [`LiquidRowFilter`] from the given predicate `Expr` if possible
///
/// # returns
/// * `Ok(Some(row_filter))` if the expression can be used as RowFilter
/// * `Ok(None)` if the expression cannot be used as an RowFilter
/// * `Err(e)` if an error occurs while building the filter
///
/// Note that the returned `RowFilter` may not contains all conjuncts in the
/// original expression. This is because some conjuncts may not be able to be
/// evaluated as an `ArrowPredicate` and will be ignored.
///
/// For example, if the expression is `a = 1 AND b = 2 AND c = 3` and `b = 2`
/// can not be evaluated for some reason, the returned `RowFilter` will contain
/// `a = 1` and `c = 3`.
pub fn build_row_filter(
    expr: &Arc<dyn PhysicalExpr>,
    physical_file_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    reorder_predicates: bool,
    file_metrics: &ParquetFileMetrics,
) -> Result<Option<LiquidRowFilter>> {
    let rows_pruned = &file_metrics.pushdown_rows_pruned;
    let rows_matched = &file_metrics.pushdown_rows_matched;
    let time = &file_metrics.row_pushdown_eval_time;

    // Split into conjuncts:
    // `a = 1 AND b = 2 AND c = 3` -> [`a = 1`, `b = 2`, `c = 3`]
    let predicates = split_conjunction(expr);

    // Determine which conjuncts can be evaluated as ArrowPredicates, if any
    let mut candidates: Vec<FilterCandidate> = predicates
        .into_iter()
        .map(|expr| {
            FilterCandidateBuilder::new(Arc::clone(expr), physical_file_schema.clone())
                .build(metadata)
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();

    // no candidates
    if candidates.is_empty() {
        return Ok(None);
    }

    if reorder_predicates {
        candidates.sort_unstable_by(|c1, c2| match c1.can_use_index.cmp(&c2.can_use_index) {
            Ordering::Equal => c1.required_bytes.cmp(&c2.required_bytes),
            ord => ord,
        });
    }

    candidates.sort_unstable_by(|c1, c2| {
        let p1 = get_priority(&c1.expr);
        let p2 = get_priority(&c2.expr);
        p1.cmp(&p2)
    });

    log::debug!(
        "Predicate eval order: {}",
        candidates
            .iter()
            .map(|c| c.expr.as_ref().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    candidates
        .into_iter()
        .map(|candidate| {
            let projection = ProjectionMask::roots(
                metadata.file_metadata().schema_descr(),
                candidate.projection.iter().copied(),
            );
            LiquidPredicate::try_new_with_metrics(
                candidate,
                projection,
                rows_pruned.clone(),
                rows_matched.clone(),
                time.clone(),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|filters| Some(LiquidRowFilter::new(filters)))
}

fn get_priority(expr: &Arc<dyn PhysicalExpr>) -> u8 {
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        match binary.op() {
            Operator::Eq | Operator::NotEq => 0, // Highest priority
            Operator::LikeMatch | Operator::ILikeMatch => 1,
            Operator::NotLikeMatch | Operator::NotILikeMatch => 2,
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => 3,
            _ => 4,
        }
    } else if expr.is::<LikeExpr>() {
        1 // LIKE expressions
    } else {
        5 // All other expression types
    }
}
