//! Optimizers for the Parquet module

use std::sync::Arc;

use datafusion::{
    catalog::memory::DataSourceExec,
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    config::ConfigOptions,
    datasource::{
        physical_plan::{FileSource, ParquetSource},
        source::DataSource,
    },
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::ExecutionPlan,
};

use crate::{LiquidCacheParquetRef, LiquidParquetSource};

/// Physical optimizer rule for local mode liquid cache
///
/// This optimizer rewrites DataSourceExec nodes that read Parquet files
/// to use LiquidParquetSource instead of the default ParquetSource
#[derive(Debug)]
pub struct LocalModeOptimizer {
    cache: LiquidCacheParquetRef,
}

impl LocalModeOptimizer {
    /// Create an optimizer with an existing cache instance
    pub fn new(cache: LiquidCacheParquetRef) -> Self {
        Self { cache }
    }

    /// Create an optimizer with an existing cache instance
    pub fn with_cache(cache: LiquidCacheParquetRef) -> Self {
        Self { cache }
    }
}

impl PhysicalOptimizerRule for LocalModeOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, datafusion::error::DataFusionError> {
        Ok(rewrite_data_source_plan(plan, &self.cache))
    }

    fn name(&self) -> &str {
        "LocalModeLiquidCacheOptimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Rewrite the data source plan to use liquid cache.
pub fn rewrite_data_source_plan(
    plan: Arc<dyn ExecutionPlan>,
    cache: &LiquidCacheParquetRef,
) -> Arc<dyn ExecutionPlan> {
    let rewritten = plan
        .transform_up(|node| try_optimize_parquet_source(node, cache))
        .unwrap();
    rewritten.data
}

/// Returns true if a data type is uncacheable by LC (string/binary).
fn is_uncacheable_type(dt: &arrow_schema::DataType) -> bool {
    use arrow_schema::DataType;
    matches!(
        dt,
        DataType::Utf8
            | DataType::Utf8View
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::BinaryView
            | DataType::LargeBinary
    ) || matches!(dt, DataType::Dictionary(_, v) if is_uncacheable_type(v))
}

/// Max number of output columns for which LC wrapping is worthwhile.
/// Per-column cache overhead exceeds decode savings for wide projections.
const MAX_LC_COLUMNS: usize = 4;

fn try_optimize_parquet_source(
    plan: Arc<dyn ExecutionPlan>,
    cache: &LiquidCacheParquetRef,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>, datafusion::error::DataFusionError> {
    if let Some(data_source_exec) = plan.downcast_ref::<DataSourceExec>()
        && let Some((file_scan_config, parquet_source)) =
            data_source_exec.downcast_to_file_source::<ParquetSource>()
    {
        // Skip LC wrapping if:
        //   - Output has zero columns (COUNT(*) — just needs row count from metadata)
        //   - Too many output columns (cache overhead exceeds decode savings)
        //   - ANY output column is string/binary (LC can't cache, fallback negates hits)
        //   - Predicate references a string column
        let output_schema = plan.schema();
        if output_schema.fields().is_empty() {
            log::debug!("[LC-Optimizer] SKIP: empty projection (COUNT(*))");
            return Ok(Transformed::no(plan));
        }

        if output_schema.fields().len() > MAX_LC_COLUMNS {
            log::debug!(
                "[LC-Optimizer] SKIP: too many columns ({} > {})",
                output_schema.fields().len(),
                MAX_LC_COLUMNS
            );
            return Ok(Transformed::no(plan));
        }

        let has_string_output = output_schema
            .fields()
            .iter()
            .any(|f| is_uncacheable_type(f.data_type()));

        let predicate_has_string = parquet_source.filter().is_some_and(|pred| {
            use datafusion::physical_expr::utils::collect_columns;
            let file_schema = file_scan_config.file_schema();
            let cols = collect_columns(&pred);
            cols.iter().any(|col| {
                file_schema
                    .fields()
                    .get(col.index())
                    .is_some_and(|f| is_uncacheable_type(f.data_type()))
            })
        });

        if has_string_output || predicate_has_string {
            log::debug!(
                "[LC-Optimizer] SKIP: string_in_output={}, string_in_predicate={}, output_cols={}",
                has_string_output,
                predicate_has_string,
                output_schema.fields().len()
            );
            return Ok(Transformed::no(plan));
        }

        let num_fields = output_schema.fields().len();
        let has_predicate = parquet_source.filter().is_some();
        log::debug!(
            "[LC-Optimizer] WRAP: all {} output columns cacheable, predicate={}",
            num_fields,
            has_predicate
        );

        let mut new_config = file_scan_config.clone();
        let new_source =
            LiquidParquetSource::from_parquet_source(parquet_source.clone(), cache.clone());

        new_config.file_source = Arc::new(new_source);
        let new_file_source: Arc<dyn DataSource> = Arc::new(new_config);
        let new_plan = Arc::new(DataSourceExec::new(new_file_source));

        return Ok(Transformed::new(
            new_plan,
            true,
            TreeNodeRecursion::Continue,
        ));
    }
    Ok(Transformed::no(plan))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{datasource::physical_plan::FileScanConfig, prelude::SessionContext};
    use liquid_cache::{cache::TranscodeEvict, cache_policies::LiquidPolicy};
    use parquet::arrow::ArrowWriter;

    use crate::LiquidCacheParquet;

    use super::*;

    fn test_cache() -> LiquidCacheParquetRef {
        Arc::new(LiquidCacheParquet::new(
            8192,
            1_000_000,
            Box::new(LiquidPolicy::new()),
            Box::new(TranscodeEvict),
        ))
    }

    /// Write a small parquet file with numeric and string columns.
    fn write_test_parquet(dir: &std::path::Path) -> String {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int32, false),
            Field::new("e", DataType::Int32, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
                Arc::new(Int32Array::from(vec![100, 200, 300, 400])),
                Arc::new(Int32Array::from(vec![5, 6, 7, 8])),
                Arc::new(Int32Array::from(vec![50, 60, 70, 80])),
                Arc::new(StringArray::from(vec!["w", "x", "y", "z"])),
            ],
        )
        .unwrap();
        let path = dir.join("data.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path.to_string_lossy().into_owned()
    }

    fn count_sources(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize) {
        let mut liquid = 0;
        let mut parquet = 0;
        plan.apply(|node| {
            if let Some(exec) = node.downcast_ref::<DataSourceExec>() {
                let data_source = exec.data_source();
                if let Some(config) = data_source.downcast_ref::<FileScanConfig>() {
                    let file_source = config.file_source();
                    if file_source.downcast_ref::<LiquidParquetSource>().is_some() {
                        liquid += 1;
                    } else if file_source.downcast_ref::<ParquetSource>().is_some() {
                        parquet += 1;
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        (liquid, parquet)
    }

    async fn plan_for_sql(sql: &str, path: &str) -> Arc<dyn ExecutionPlan> {
        let ctx = SessionContext::new();
        ctx.register_parquet("t", path, Default::default())
            .await
            .unwrap();
        let df = ctx.sql(sql).await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    #[tokio::test]
    async fn test_plan_rewrite_wraps_numeric_scan() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(tmp_dir.path());
        let plan = plan_for_sql("SELECT a, b FROM t WHERE a > 1", &path).await;
        let expected_schema = plan.schema();

        let rewritten = rewrite_data_source_plan(plan, &test_cache());

        let (liquid, parquet) = count_sources(&rewritten);
        assert_eq!(liquid, 1);
        assert_eq!(parquet, 0);
        assert_eq!(rewritten.schema(), expected_schema);
    }

    #[tokio::test]
    async fn test_plan_rewrite_skips_string_output() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(tmp_dir.path());
        let plan = plan_for_sql("SELECT a, s FROM t WHERE a > 1", &path).await;

        let rewritten = rewrite_data_source_plan(plan, &test_cache());

        let (liquid, parquet) = count_sources(&rewritten);
        assert_eq!(liquid, 0);
        assert_eq!(parquet, 1);
    }

    #[tokio::test]
    async fn test_plan_rewrite_skips_string_predicate() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(tmp_dir.path());
        let plan = plan_for_sql("SELECT a, b FROM t WHERE s = 'x'", &path).await;

        let rewritten = rewrite_data_source_plan(plan, &test_cache());

        let (liquid, parquet) = count_sources(&rewritten);
        assert_eq!(liquid, 0);
        assert_eq!(parquet, 1);
    }

    #[tokio::test]
    async fn test_plan_rewrite_skips_wide_projection() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(tmp_dir.path());
        // 5 numeric columns > MAX_LC_COLUMNS (4)
        let plan = plan_for_sql("SELECT a, b, c, d, e FROM t", &path).await;

        let rewritten = rewrite_data_source_plan(plan, &test_cache());

        let (liquid, parquet) = count_sources(&rewritten);
        assert_eq!(liquid, 0);
        assert_eq!(parquet, 1);
    }

    #[tokio::test]
    async fn test_plan_rewrite_skips_empty_projection() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(tmp_dir.path());
        let plan = plan_for_sql("SELECT COUNT(*) FROM t", &path).await;

        let rewritten = rewrite_data_source_plan(plan, &test_cache());

        let (liquid, _parquet) = count_sources(&rewritten);
        assert_eq!(liquid, 0);
    }
}
