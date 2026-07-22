use arrow::{
    array::{Array, ArrayRef, BooleanArray},
    buffer::BooleanBuffer,
    compute::prep_null_mask_filter,
    record_batch::RecordBatch,
};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use liquid_cache::cache::{CacheFull, LiquidCache, LiquidExpr};
use parquet::arrow::arrow_reader::ArrowPredicate;

use crate::{
    LiquidPredicate,
    cache::{BatchID, ColumnAccessPath, ParquetArrayID},
};
use std::sync::Arc;

/// A column in the cache.
#[derive(Debug)]
pub struct CachedColumn {
    cache_store: Arc<LiquidCache>,
    field: Arc<Field>,
    column_path: ColumnAccessPath,
    /// Whether this column is used in a predicate (WHERE clause).
    /// In predicate-only mode, only predicate columns are cached.
    is_predicate_column: bool,
}

/// A reference to a cached column.
pub type CachedColumnRef = Arc<CachedColumn>;

/// Error type for inserting an arrow array into the cache.
#[derive(Debug)]
pub enum InsertArrowArrayError {
    /// The array is already cached.
    AlreadyCached,
    /// The cache does not have enough memory budget to accept the array.
    CacheFull,
}

impl From<CacheFull> for InsertArrowArrayError {
    fn from(_: CacheFull) -> Self {
        Self::CacheFull
    }
}

impl CachedColumn {
    pub(crate) fn new(
        field: Arc<Field>,
        cache_store: Arc<LiquidCache>,
        column_access_path: ColumnAccessPath,
        is_predicate_column: bool,
    ) -> Self {
        Self {
            field,
            cache_store,
            column_path: column_access_path,
            is_predicate_column,
        }
    }

    /// row_id must be on a batch boundary.
    pub(crate) fn entry_id(&self, batch_id: BatchID) -> ParquetArrayID {
        self.column_path.entry_id(batch_id)
    }

    pub(crate) fn is_cached(&self, batch_id: BatchID) -> bool {
        self.cache_store.is_cached(&self.entry_id(batch_id).into())
    }

    /// Returns the Arrow field metadata for this cached column.
    pub fn field(&self) -> Arc<Field> {
        self.field.clone()
    }

    /// Returns whether this column is a predicate column (used in WHERE clause).
    pub fn is_predicate_column(&self) -> bool {
        self.is_predicate_column
    }

    fn array_to_record_batch(&self, array: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![self.field.clone()]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    /// Evaluates a predicate on a cached column.
    pub fn eval_predicate_with_filter(
        &self,
        batch_id: BatchID,
        filter: &BooleanBuffer,
        predicate: &mut LiquidPredicate,
    ) -> Option<Result<BooleanArray, ArrowError>> {
        let entry_id = self.entry_id(batch_id).into();
        let liquid_expr = LiquidExpr::try_new(
            Arc::clone(predicate.physical_expr()),
            self.field.data_type(),
        );

        if let Some(liquid_expr) = liquid_expr
            && let Some(boolean_array) = self
                .cache_store
                .eval_predicate(&entry_id, &liquid_expr)
                .with_selection(filter)
                .read()
        {
            let predicate_filter = match boolean_array.null_count() {
                0 => boolean_array,
                _ => prep_null_mask_filter(&boolean_array),
            };
            return Some(Ok(predicate_filter));
        }

        let array = self.get_arrow_array_with_filter(batch_id, filter)?;
        let record_batch = self.array_to_record_batch(array);
        let boolean_array = match predicate.evaluate(record_batch) {
            Ok(arr) => arr,
            Err(err) => return Some(Err(err)),
        };
        let predicate_filter = match boolean_array.null_count() {
            0 => boolean_array,
            _ => prep_null_mask_filter(&boolean_array),
        };
        Some(Ok(predicate_filter))
    }

    pub(crate) fn liquid_expr_for_predicate(
        &self,
        expr: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    ) -> Option<LiquidExpr> {
        LiquidExpr::try_new(expr, self.field.data_type())
    }

    /// Get an arrow array with a filter applied.
    /// Returns None for non-predicate columns or string predicate columns
    /// (only numeric predicate columns are cached and served from cache).
    pub fn get_arrow_array_with_filter(
        &self,
        batch_id: BatchID,
        filter: &BooleanBuffer,
    ) -> Option<ArrayRef> {
        if !self.is_predicate_column || is_string_type(self.field.data_type()) {
            return None;
        }
        let entry_id = self.entry_id(batch_id).into();
        let result = self
            .cache_store
            .get(&entry_id)
            .with_selection(filter)
            .read();
        if result.is_some() {
            self.cache_store.observer().runtime_stats().incr_cache_hit();
        } else {
            self.cache_store
                .observer()
                .runtime_stats()
                .incr_cache_miss();
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn get_arrow_array_test_only(&self, batch_id: BatchID) -> Option<ArrayRef> {
        let entry_id = self.entry_id(batch_id).into();
        self.cache_store.get(&entry_id).read()
    }

    /// Insert an array into the cache.
    /// Only numeric predicate columns are cached; string predicates and
    /// non-predicate columns return CacheFull.
    pub fn insert(
        self: &Arc<Self>,
        batch_id: BatchID,
        array: ArrayRef,
    ) -> Result<(), InsertArrowArrayError> {
        if !self.is_predicate_column || is_string_type(self.field.data_type()) {
            return Err(InsertArrowArrayError::CacheFull);
        }

        if self.is_cached(batch_id) {
            return Err(InsertArrowArrayError::AlreadyCached);
        }

        self.cache_store
            .insert(self.entry_id(batch_id).into(), array)
            .execute()?;
        Ok(())
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => true,
        DataType::Binary | DataType::BinaryView | DataType::LargeBinary => true,
        DataType::Dictionary(_, value_type) => is_string_type(value_type.as_ref()),
        _ => false,
    }
}
