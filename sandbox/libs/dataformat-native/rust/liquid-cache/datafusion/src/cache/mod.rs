//! This module contains the cache implementation for the Parquet reader.
//!

use crate::reader::{LiquidPredicate, extract_multi_column_or};
use crate::sync::Mutex;
use ahash::AHashMap;
use arrow::array::{BooleanArray, RecordBatch};
use arrow::buffer::BooleanBuffer;
use arrow_schema::{ArrowError, Field, Schema, SchemaRef};
use liquid_cache::cache::{
    CachePolicy, CacheStats, LiquidCache, LiquidCacheBuilder, SqueezePolicy,
};
use parquet::arrow::arrow_reader::ArrowPredicate;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

mod column;
mod id;
mod stats;

pub use column::{CachedColumn, CachedColumnRef, InsertArrowArrayError};
pub(crate) use id::ColumnAccessPath;
pub use id::{BatchID, ParquetArrayID};

#[derive(Default, Debug)]
struct ColumnMaps {
    // invariant: Arc::ptr_eq(map[field.name()], map[field.id()])
    by_id: AHashMap<u64, CachedColumnRef>,
    by_name: AHashMap<String, CachedColumnRef>,
}

/// A row group in the cache.
#[derive(Debug)]
pub struct CachedRowGroup {
    columns: ColumnMaps,
    cache_store: Arc<LiquidCache>,
}

impl CachedRowGroup {
    /// Create a new row group.
    /// The column_ids are the indices of the columns in the file schema.
    /// So they may not start from 0.
    fn new(
        cache_store: Arc<LiquidCache>,
        row_group_idx: u64,
        file_idx: u64,
        columns: &[(u64, Arc<Field>, bool)],
    ) -> Self {
        let mut column_maps = ColumnMaps::default();
        for (column_id, field, is_predicate_column) in columns {
            let column_access_path = ColumnAccessPath::new(file_idx, row_group_idx, *column_id);
            let column = Arc::new(CachedColumn::new(
                Arc::clone(field),
                Arc::clone(&cache_store),
                column_access_path,
                *is_predicate_column,
            ));
            column_maps.by_id.insert(*column_id, column.clone());
            column_maps.by_name.insert(field.name().to_string(), column);
        }

        Self {
            columns: column_maps,
            cache_store,
        }
    }

    /// Returns the batch size configured for this cached row group.
    pub fn batch_size(&self) -> usize {
        self.cache_store.config().batch_size()
    }

    /// Get a column from the row group.
    pub fn get_column(&self, column_id: u64) -> Option<CachedColumnRef> {
        self.columns.by_id.get(&column_id).cloned()
    }

    /// Get a column from the row group by its field name.
    pub fn get_column_by_name(&self, column_name: &str) -> Option<CachedColumnRef> {
        if let Some(column) = self.columns.by_name.get(column_name) {
            return Some(column.clone());
        }

        // DataFusion may carry qualified names in physical expressions
        // (e.g. "table.col"), while cache fields are keyed by file schema names.
        let unqualified = column_name.rsplit('.').next().unwrap_or(column_name);
        self.columns.by_name.get(unqualified).cloned()
    }

    /// Evaluate a predicate on a row group.
    pub fn evaluate_selection_with_predicate(
        &self,
        batch_id: BatchID,
        selection: &BooleanBuffer,
        predicate: &mut LiquidPredicate,
    ) -> Option<Result<BooleanArray, ArrowError>> {
        let column_ids = predicate.predicate_column_ids();

        if column_ids.len() == 1 {
            // If we only have one column, we can short-circuit and try to evaluate the predicate on encoded data.
            let column_id = column_ids[0];
            let cache = self.get_column(column_id as u64)?;
            return cache.eval_predicate_with_filter(batch_id, selection, predicate);
        } else if column_ids.len() >= 2 {
            // Try to extract multiple column-literal expressions from OR structure
            if let Some(column_exprs) =
                extract_multi_column_or(predicate.physical_expr_physical_column_index())
            {
                let mut combined_buffer: Option<BooleanArray> = None;

                for (col_name, expr) in column_exprs {
                    let column = self.get_column_by_name(col_name)?;
                    let liquid_expr = column.liquid_expr_for_predicate(Arc::clone(&expr));
                    let liquid_expr = match liquid_expr {
                        Some(expr) => expr,
                        None => {
                            combined_buffer = None;
                            break;
                        }
                    };
                    let entry_id = column.entry_id(batch_id).into();
                    let liquid_array = self.cache_store.try_read_liquid(&entry_id);
                    let liquid_array = match liquid_array {
                        None => {
                            combined_buffer = None;
                            break;
                        }
                        Some(array) => array,
                    };
                    let buffer = liquid_array.try_eval_predicate(&liquid_expr, selection);

                    combined_buffer = Some(match combined_buffer {
                        None => buffer,
                        Some(existing) => {
                            arrow::compute::kernels::boolean::or_kleene(&existing, &buffer).ok()?
                        }
                    });
                }

                if let Some(result) = combined_buffer {
                    return Some(Ok(result));
                }
            }
        }
        // Otherwise, we need to first convert the data into arrow arrays.
        let mut arrays = Vec::new();
        let mut fields = Vec::new();
        for column_id in column_ids {
            let column = self.get_column(column_id as u64)?;
            let array = column.get_arrow_array_with_filter(batch_id, selection)?;
            arrays.push(array);
            fields.push(column.field());
        }
        let schema = Arc::new(Schema::new(fields));
        let record_batch = RecordBatch::try_new(schema, arrays).unwrap();
        let boolean_array = predicate.evaluate(record_batch).unwrap();
        Some(Ok(boolean_array))
    }
}

pub(crate) type CachedRowGroupRef = Arc<CachedRowGroup>;

/// A file in the cache.
#[derive(Debug)]
pub struct CachedFile {
    cache_store: Arc<LiquidCache>,
    file_id: u64,
    file_schema: SchemaRef,
}

impl CachedFile {
    fn new(cache_store: Arc<LiquidCache>, file_id: u64, file_schema: SchemaRef) -> Self {
        Self {
            cache_store,
            file_id,
            file_schema,
        }
    }

    /// Create a row group handle scoped to the current query context.
    pub fn create_row_group(
        &self,
        row_group_id: u64,
        predicate_column_ids: Vec<usize>,
    ) -> CachedRowGroupRef {
        let columns: Vec<(u64, Arc<Field>, bool)> = self
            .file_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let is_predicate_column = predicate_column_ids.contains(&idx);
                (idx as u64, Arc::clone(field), is_predicate_column)
            })
            .collect();

        Arc::new(CachedRowGroup::new(
            self.cache_store.clone(),
            row_group_id,
            self.file_id,
            &columns,
        ))
    }

    /// Return the configured cache batch size.
    pub fn batch_size(&self) -> usize {
        self.cache_store.config().batch_size()
    }

    /// Return the full file schema tracked by the cache entry.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.file_schema)
    }
}

/// A reference to a cached file.
pub(crate) type CachedFileRef = Arc<CachedFile>;

/// The main cache structure.
#[derive(Debug)]
pub struct LiquidCacheParquet {
    /// Map file path to file id.
    files: Mutex<AHashMap<String, u64>>,

    cache_store: Arc<LiquidCache>,

    current_file_id: AtomicU64,
}

/// A reference to the main cache structure.
pub type LiquidCacheParquetRef = Arc<LiquidCacheParquet>;

impl LiquidCacheParquet {
    /// Create a new in-memory cache for parquet files.
    pub fn new(
        batch_size: usize,
        max_memory_bytes: usize,
        cache_policy: Box<dyn CachePolicy>,
        squeeze_policy: Box<dyn SqueezePolicy>,
    ) -> Self {
        assert!(batch_size.is_power_of_two());
        let cache_storage = LiquidCacheBuilder::new()
            .with_batch_size(batch_size)
            .with_max_memory_bytes(max_memory_bytes)
            .with_squeeze_policy(squeeze_policy)
            .with_cache_policy(cache_policy)
            .build();

        LiquidCacheParquet {
            files: Mutex::new(AHashMap::new()),
            cache_store: cache_storage,
            current_file_id: AtomicU64::new(0),
        }
    }

    /// Register a file in the cache.
    pub fn register_or_get_file(
        &self,
        file_path: String,
        full_file_schema: SchemaRef,
    ) -> CachedFileRef {
        let mut files = self.files.lock().unwrap();
        let file_id = *files
            .entry(file_path.clone())
            .or_insert_with(|| self.current_file_id.fetch_add(1, Ordering::Relaxed));
        drop(files);

        Arc::new(CachedFile::new(
            self.cache_store.clone(),
            file_id,
            full_file_schema,
        ))
    }

    /// Get the batch size of the cache.
    pub fn batch_size(&self) -> usize {
        self.cache_store.config().batch_size()
    }

    /// Get the max memory bytes of the cache.
    pub fn max_memory_bytes(&self) -> usize {
        self.cache_store.config().max_memory_bytes()
    }

    /// Get the memory usage of the cache in bytes.
    pub fn memory_usage_bytes(&self) -> usize {
        self.cache_store.budget().memory_usage_bytes()
    }

    /// Get a snapshot of the cache statistics.
    pub fn stats(&self) -> CacheStats {
        self.cache_store.stats()
    }

    /// Reset the cache.
    ///
    /// # Safety
    /// This is unsafe because resetting the cache while other threads are using the cache may cause undefined behavior.
    /// You should only call this when no one else is using the cache.
    pub unsafe fn reset(&self) {
        let mut files = self.files.lock().unwrap();
        files.clear();
        self.cache_store.reset();
    }

    /// Get the storage of the cache.
    pub fn storage(&self) -> &Arc<LiquidCache> {
        &self.cache_store
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{CachedRowGroupRef, LiquidCacheParquet};
    use crate::reader::FilterCandidateBuilder;
    use arrow::array::Int32Array;
    use arrow::buffer::BooleanBuffer;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
    use datafusion::physical_plan::expressions::Column;
    use liquid_cache::cache::TranscodeEvict;
    use liquid_cache::cache_policies::LiquidPolicy;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use std::sync::Arc;

    fn setup_cache(batch_size: usize, schema: SchemaRef) -> CachedRowGroupRef {
        let cache = LiquidCacheParquet::new(
            batch_size,
            usize::MAX,
            Box::new(LiquidPolicy::new()),
            Box::new(TranscodeEvict),
        );
        let file = cache.register_or_get_file("test".to_string(), schema);
        // Mark all columns as predicate columns so they are cacheable in tests.
        let all_columns: Vec<usize> = (0..file.schema().fields().len()).collect();
        file.create_row_group(0, all_columns)
    }

    #[test]
    fn evaluate_or_on_cached_columns() {
        let batch_size = 4;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let row_group = setup_cache(batch_size, schema.clone());

        let col_a = row_group.get_column(0).unwrap();
        let col_b = row_group.get_column(1).unwrap();

        let batch_id = BatchID::from_row_id(0, batch_size);

        let array_a = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let array_b = Arc::new(Int32Array::from(vec![10, 20, 30, 40]));

        assert!(col_a.insert(batch_id, array_a.clone()).is_ok());
        assert!(col_b.insert(batch_id, array_b.clone()).is_ok());

        // build parquet metadata for predicate construction
        let tmp_meta = tempfile::NamedTempFile::new().unwrap();
        let mut writer =
            ArrowWriter::try_new(tmp_meta.reopen().unwrap(), Arc::clone(&schema), None).unwrap();
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array_a, array_b]).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file_reader = std::fs::File::open(tmp_meta.path()).unwrap();
        let metadata = ArrowReaderMetadata::load(&file_reader, ArrowReaderOptions::new()).unwrap();

        // expression a = 3 OR b = 20
        let expr_a: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(3)))),
        ));
        let expr_b: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("b", 1)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(20)))),
        ));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(expr_a, Operator::Or, expr_b));

        let builder = FilterCandidateBuilder::new(expr, Arc::clone(&schema));
        let candidate = builder.build(metadata.metadata()).unwrap().unwrap();
        let projection = candidate.projection(metadata.metadata());
        let mut predicate = LiquidPredicate::try_new(candidate, projection).unwrap();

        let selection = BooleanBuffer::new_set(batch_size);
        let result = row_group
            .evaluate_selection_with_predicate(batch_id, &selection, &mut predicate)
            .unwrap()
            .unwrap();

        let expected = BooleanBuffer::collect_bool(batch_size, |i| i == 1 || i == 2).into();
        assert_eq!(result, expected);
    }

    #[test]
    fn evaluate_three_column_or() {
        let batch_size = 8;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let row_group = setup_cache(batch_size, schema.clone());

        let col_a = row_group.get_column(0).unwrap();
        let col_b = row_group.get_column(1).unwrap();
        let col_c = row_group.get_column(2).unwrap();

        let batch_id = BatchID::from_row_id(0, batch_size);

        let array_a = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]));
        let array_b = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80]));
        let array_c = Arc::new(Int32Array::from(vec![
            100, 200, 300, 400, 500, 600, 700, 800,
        ]));

        assert!(col_a.insert(batch_id, array_a.clone()).is_ok());
        assert!(col_b.insert(batch_id, array_b.clone()).is_ok());
        assert!(col_c.insert(batch_id, array_c.clone()).is_ok());

        // build parquet metadata for predicate construction
        let tmp_meta = tempfile::NamedTempFile::new().unwrap();
        let mut writer =
            ArrowWriter::try_new(tmp_meta.reopen().unwrap(), Arc::clone(&schema), None).unwrap();
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![array_a, array_b, array_c]).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file_reader = std::fs::File::open(tmp_meta.path()).unwrap();
        let metadata = ArrowReaderMetadata::load(&file_reader, ArrowReaderOptions::new()).unwrap();

        // expression: a = 2 OR b = 40 OR c = 600
        let expr_a: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
        ));
        let expr_b: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("b", 1)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(40)))),
        ));
        let expr_c: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c", 2)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(600)))),
        ));

        // Build nested OR: (a = 2 OR b = 40) OR c = 600
        let expr_ab = Arc::new(BinaryExpr::new(expr_a, Operator::Or, expr_b));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(expr_ab, Operator::Or, expr_c));

        let builder = FilterCandidateBuilder::new(expr, Arc::clone(&schema));
        let candidate = builder.build(metadata.metadata()).unwrap().unwrap();
        let projection = candidate.projection(metadata.metadata());
        let mut predicate = LiquidPredicate::try_new(candidate, projection).unwrap();

        let selection = BooleanBuffer::new_set(batch_size);
        let result = row_group
            .evaluate_selection_with_predicate(batch_id, &selection, &mut predicate)
            .unwrap()
            .unwrap();

        // Expected: row 1 (a=2), row 3 (b=40), row 5 (c=600) -> indices 1, 3, 5
        let expected =
            BooleanBuffer::collect_bool(batch_size, |i| i == 1 || i == 3 || i == 5).into();
        assert_eq!(result, expected);
    }
}
