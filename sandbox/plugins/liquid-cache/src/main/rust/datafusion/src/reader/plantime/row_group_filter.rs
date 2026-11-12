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

use arrow::{array::ArrayRef, array::BooleanArray, array::UInt64Array, datatypes::Schema};
use datafusion::common::{Column, Result, ScalarValue};
use datafusion::datasource::listing::FileRange;
use datafusion::datasource::physical_plan::ParquetFileMetrics;
use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::parquet_column;
use parquet::basic::Type;
use parquet::data_type::Decimal;
use parquet::schema::types::SchemaDescriptor;
use parquet::{
    arrow::{ParquetRecordBatchStreamBuilder, async_reader::AsyncFileReader},
    bloom_filter::Sbbf,
    file::metadata::RowGroupMetaData,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Reduces the [`ParquetAccessPlan`] based on row group level metadata.
///
/// This struct implements the various types of pruning that are applied to a
/// set of row groups within a parquet file, progressively narrowing down the
/// set of row groups (and ranges/selections within those row groups) that
/// should be scanned, based on the available metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct RowGroupAccessPlanFilter {
    /// which row groups should be accessed
    access_plan: ParquetAccessPlan,
}

impl RowGroupAccessPlanFilter {
    /// Create a new `RowGroupPlanBuilder` for pruning out the groups to scan
    /// based on metadata and statistics
    pub fn new(access_plan: ParquetAccessPlan) -> Self {
        Self { access_plan }
    }

    /// Return true if there are no row groups
    pub fn is_empty(&self) -> bool {
        self.access_plan.is_empty()
    }

    /// Returns the inner access plan
    pub fn build(self) -> ParquetAccessPlan {
        self.access_plan
    }

    /// Prune remaining row groups to only those  within the specified range.
    ///
    /// Updates this set to mark row groups that should not be scanned
    ///
    /// # Panics
    /// if `groups.len() != self.len()`
    pub fn prune_by_range(&mut self, groups: &[RowGroupMetaData], range: &FileRange) {
        assert_eq!(groups.len(), self.access_plan.len());
        for (idx, metadata) in groups.iter().enumerate() {
            if !self.access_plan.should_scan(idx) {
                continue;
            }

            // Skip the row group if the first dictionary/data page are not
            // within the range.
            //
            // note don't use the location of metadata
            // <https://github.com/apache/datafusion/issues/5995>
            let col = metadata.column(0);
            let offset = col
                .dictionary_page_offset()
                .unwrap_or_else(|| col.data_page_offset());
            if !range.contains(offset) {
                self.access_plan.skip(idx);
            }
        }
    }
    /// Prune remaining row groups using min/max/null_count statistics and
    /// the [`PruningPredicate`] to determine if the predicate can not be true.
    ///
    /// Updates this set to mark row groups that should not be scanned
    ///
    /// Note: This method currently ignores ColumnOrder
    /// <https://github.com/apache/datafusion/issues/8335>
    ///
    /// # Panics
    /// if `groups.len() != self.len()`
    pub fn prune_by_statistics(
        &mut self,
        arrow_schema: &Schema,
        parquet_schema: &SchemaDescriptor,
        groups: &[RowGroupMetaData],
        predicate: &PruningPredicate,
        metrics: &ParquetFileMetrics,
    ) {
        // scoped timer updates on drop
        let _timer_guard = metrics.statistics_eval_time.timer();

        assert_eq!(groups.len(), self.access_plan.len());
        // Indexes of row groups still to scan
        let row_group_indexes = self.access_plan.row_group_indexes();
        let row_group_metadatas = row_group_indexes
            .iter()
            .map(|&i| &groups[i])
            .collect::<Vec<_>>();

        let pruning_stats = RowGroupPruningStatistics {
            parquet_schema,
            row_group_metadatas,
            arrow_schema,
        };

        // try to prune the row groups in a single call
        match predicate.prune(&pruning_stats) {
            Ok(values) => {
                // values[i] is false means the predicate could not be true for row group i
                for (idx, &value) in row_group_indexes.iter().zip(values.iter()) {
                    if !value {
                        self.access_plan.skip(*idx);
                        metrics.row_groups_pruned_statistics.add_pruned(1);
                    } else {
                        metrics.row_groups_pruned_statistics.add_matched(1);
                    }
                }
            }
            // stats filter array could not be built, so we can't prune
            Err(e) => {
                log::debug!("Error evaluating row group predicate values {e}");
                metrics.predicate_evaluation_errors.add(1);
            }
        }
    }

    /// Prune remaining row groups using available bloom filters and the
    /// [`PruningPredicate`].
    ///
    /// Updates this set with row groups that should not be scanned
    ///
    /// # Panics
    /// if the builder does not have the same number of row groups as this set
    pub async fn prune_by_bloom_filters<T: AsyncFileReader + Send + 'static>(
        &mut self,
        arrow_schema: &Schema,
        builder: &mut ParquetRecordBatchStreamBuilder<T>,
        predicate: &PruningPredicate,
        metrics: &ParquetFileMetrics,
    ) {
        // scoped timer updates on drop
        let _timer_guard = metrics.bloom_filter_eval_time.timer();

        assert_eq!(builder.metadata().num_row_groups(), self.access_plan.len());
        for idx in 0..self.access_plan.len() {
            if !self.access_plan.should_scan(idx) {
                continue;
            }

            // Attempt to find bloom filters for filtering this row group
            let literal_columns = predicate.literal_columns();
            let mut column_sbbf = HashMap::with_capacity(literal_columns.len());

            for column_name in literal_columns {
                let Some((column_idx, _field)) =
                    parquet_column(builder.parquet_schema(), arrow_schema, &column_name)
                else {
                    continue;
                };

                let bf = match builder
                    .get_row_group_column_bloom_filter(idx, column_idx)
                    .await
                {
                    Ok(Some(bf)) => bf,
                    Ok(None) => continue, // no bloom filter for this column
                    Err(e) => {
                        log::debug!("Ignoring error reading bloom filter: {e}");
                        metrics.predicate_evaluation_errors.add(1);
                        continue;
                    }
                };
                let physical_type = builder.parquet_schema().column(column_idx).physical_type();

                column_sbbf.insert(column_name.to_string(), (bf, physical_type));
            }

            let stats = BloomFilterStatistics { column_sbbf };

            // Can this group be pruned?
            let prune_group = match predicate.prune(&stats) {
                Ok(values) => !values[0],
                Err(e) => {
                    log::debug!("Error evaluating row group predicate on bloom filter: {e}");
                    metrics.predicate_evaluation_errors.add(1);
                    false
                }
            };

            if prune_group {
                metrics.row_groups_pruned_bloom_filter.add_pruned(1);
                self.access_plan.skip(idx)
            } else if !stats.column_sbbf.is_empty() {
                metrics.row_groups_pruned_bloom_filter.add_matched(1);
            }
        }
    }
}
/// Implements [`PruningStatistics`] for Parquet Split Block Bloom Filters (SBBF)
struct BloomFilterStatistics {
    /// Maps column name to the parquet bloom filter and parquet physical type
    column_sbbf: HashMap<String, (Sbbf, Type)>,
}

impl BloomFilterStatistics {
    /// Helper function for checking if [`Sbbf`] filter contains [`ScalarValue`].
    ///
    /// In case the type of scalar is not supported, returns `true`, assuming that the
    /// value may be present.
    fn check_scalar(sbbf: &Sbbf, value: &ScalarValue, parquet_type: &Type) -> bool {
        match value {
            ScalarValue::Utf8(Some(v))
            | ScalarValue::Utf8View(Some(v))
            | ScalarValue::LargeUtf8(Some(v)) => sbbf.check(&v.as_str()),
            ScalarValue::Binary(Some(v))
            | ScalarValue::BinaryView(Some(v))
            | ScalarValue::LargeBinary(Some(v)) => sbbf.check(v),
            ScalarValue::FixedSizeBinary(_size, Some(v)) => sbbf.check(v),
            ScalarValue::Boolean(Some(v)) => sbbf.check(v),
            ScalarValue::Float64(Some(v)) => sbbf.check(v),
            ScalarValue::Float32(Some(v)) => sbbf.check(v),
            ScalarValue::Int64(Some(v)) => sbbf.check(v),
            ScalarValue::Int32(Some(v)) => sbbf.check(v),
            ScalarValue::UInt64(Some(v)) => sbbf.check(v),
            ScalarValue::UInt32(Some(v)) => sbbf.check(v),
            ScalarValue::Decimal128(Some(v), p, s) => match parquet_type {
                Type::INT32 => {
                    //https://github.com/apache/parquet-format/blob/eb4b31c1d64a01088d02a2f9aefc6c17c54cc6fc/Encodings.md?plain=1#L35-L42
                    // All physical type  are little-endian
                    if *p > 9 {
                        //DECIMAL can be used to annotate the following types:
                        //
                        // int32: for 1 <= precision <= 9
                        // int64: for 1 <= precision <= 18
                        return true;
                    }
                    let b = (*v as i32).to_le_bytes();
                    // Use Decimal constructor after https://github.com/apache/arrow-rs/issues/5325
                    let decimal = Decimal::Int32 {
                        value: b,
                        precision: *p as i32,
                        scale: *s as i32,
                    };
                    sbbf.check(&decimal)
                }
                Type::INT64 => {
                    if *p > 18 {
                        return true;
                    }
                    let b = (*v as i64).to_le_bytes();
                    let decimal = Decimal::Int64 {
                        value: b,
                        precision: *p as i32,
                        scale: *s as i32,
                    };
                    sbbf.check(&decimal)
                }
                Type::FIXED_LEN_BYTE_ARRAY => {
                    // keep with from_bytes_to_i128
                    let b = v.to_be_bytes().to_vec();
                    // Use Decimal constructor after https://github.com/apache/arrow-rs/issues/5325
                    let decimal = Decimal::Bytes {
                        value: b.into(),
                        precision: *p as i32,
                        scale: *s as i32,
                    };
                    sbbf.check(&decimal)
                }
                _ => true,
            },
            // One more pattern matching since not all data types are supported
            // inside of a Dictionary
            ScalarValue::Dictionary(_, inner) => match inner.as_ref() {
                ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
                | ScalarValue::Float32(_)
                | ScalarValue::Float64(_)
                | ScalarValue::Utf8(_)
                | ScalarValue::LargeUtf8(_)
                | ScalarValue::Binary(_)
                | ScalarValue::LargeBinary(_) => {
                    BloomFilterStatistics::check_scalar(sbbf, inner, parquet_type)
                }
                _ => true,
            },
            _ => true,
        }
    }
}

impl PruningStatistics for BloomFilterStatistics {
    fn min_values(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn max_values(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn num_containers(&self) -> usize {
        1
    }

    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        None
    }

    /// Use bloom filters to determine if we are sure this column can not
    /// possibly contain `values`
    ///
    /// The `contained` API returns false if the bloom filters knows that *ALL*
    /// of the values in a column are not present.
    fn contained(&self, column: &Column, values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        let (sbbf, parquet_type) = self.column_sbbf.get(column.name.as_str())?;

        // Bloom filters are probabilistic data structures that can return false
        // positives (i.e. it might return true even if the value is not
        // present) however, the bloom filter will return `false` if the value is
        // definitely not present.

        let known_not_present = values
            .iter()
            .map(|value| BloomFilterStatistics::check_scalar(sbbf, value, parquet_type))
            // The row group doesn't contain any of the values if
            // all the checks are false
            .all(|v| !v);

        let contains = if known_not_present {
            Some(false)
        } else {
            // Given the bloom filter is probabilistic, we can't be sure that
            // the row group actually contains the values. Return `None` to
            // indicate this uncertainty
            None
        };

        Some(BooleanArray::from(vec![contains]))
    }
}

/// Wraps a slice of [`RowGroupMetaData`] in a way that implements [`PruningStatistics`]
struct RowGroupPruningStatistics<'a> {
    parquet_schema: &'a SchemaDescriptor,
    row_group_metadatas: Vec<&'a RowGroupMetaData>,
    arrow_schema: &'a Schema,
}

impl<'a> RowGroupPruningStatistics<'a> {
    /// Return an iterator over the row group metadata
    fn metadata_iter(&'a self) -> impl Iterator<Item = &'a RowGroupMetaData> + 'a {
        self.row_group_metadatas.iter().copied()
    }

    fn statistics_converter<'b>(&'a self, column: &'b Column) -> Result<StatisticsConverter<'a>> {
        Ok(StatisticsConverter::try_new(
            &column.name,
            self.arrow_schema,
            self.parquet_schema,
        )?)
    }
}

impl PruningStatistics for RowGroupPruningStatistics<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.statistics_converter(column)
            .and_then(|c| Ok(c.row_group_mins(self.metadata_iter())?))
            .ok()
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.statistics_converter(column)
            .and_then(|c| Ok(c.row_group_maxes(self.metadata_iter())?))
            .ok()
    }

    fn num_containers(&self) -> usize {
        self.row_group_metadatas.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.statistics_converter(column)
            .and_then(|c| Ok(c.row_group_null_counts(self.metadata_iter())?))
            .ok()
            .map(|counts| Arc::new(counts) as ArrayRef)
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        // Row counts are container-level — read directly from row group metadata.
        let counts: UInt64Array = self
            .metadata_iter()
            .map(|rg| Some(rg.num_rows() as u64))
            .collect();
        Some(Arc::new(counts) as ArrayRef)
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}
