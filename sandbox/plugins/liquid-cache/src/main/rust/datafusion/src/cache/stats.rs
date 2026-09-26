use super::LiquidCacheParquet;
use crate::{cache::id::ParquetArrayID, sync::Arc};
use arrow::array::{ArrayBuilder, RecordBatch, StringBuilder, UInt64Builder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use liquid_cache::cache::CacheEntry;
use parquet::{
    arrow::ArrowWriter, basic::Compression, errors::ParquetError,
    file::properties::WriterProperties,
};
use std::{fs::File, path::Path};

struct StatsWriter {
    writer: ArrowWriter<File>,
    schema: SchemaRef,
    file_path_builder: StringBuilder,
    row_group_id_builder: UInt64Builder,
    column_id_builder: UInt64Builder,
    row_start_id_builder: UInt64Builder,
    row_count_builder: UInt64Builder,
    memory_size_builder: UInt64Builder,
    cache_type_builder: StringBuilder,
    reference_count_builder: UInt64Builder,
}

impl StatsWriter {
    fn new(file_path: impl AsRef<Path>) -> Result<Self, ParquetError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_group_id", DataType::UInt64, false),
            Field::new("column_id", DataType::UInt64, false),
            Field::new("row_start_id", DataType::UInt64, false),
            Field::new("row_count", DataType::UInt64, true),
            Field::new("memory_size", DataType::UInt64, false),
            Field::new("cache_type", DataType::Utf8, false),
            Field::new("file_path", DataType::Utf8, false),
            Field::new("reference_count", DataType::UInt64, false),
        ]));

        let file = File::create(file_path)?;
        let write_props = WriterProperties::builder()
            .set_compression(Compression::LZ4)
            .set_created_by("liquid-cache-stats".to_string())
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(write_props))?;
        Ok(Self {
            writer,
            schema,
            file_path_builder: StringBuilder::with_capacity(8192, 8192),
            row_group_id_builder: UInt64Builder::new(),
            column_id_builder: UInt64Builder::new(),
            row_start_id_builder: UInt64Builder::new(),
            row_count_builder: UInt64Builder::new(),
            memory_size_builder: UInt64Builder::new(),
            cache_type_builder: StringBuilder::with_capacity(8192, 8192),
            reference_count_builder: UInt64Builder::new(),
        })
    }

    fn build_batch(&mut self) -> Result<RecordBatch, ParquetError> {
        let row_group_id_array = self.row_group_id_builder.finish();
        let column_id_array = self.column_id_builder.finish();
        let row_start_id_array = self.row_start_id_builder.finish();
        let row_count_array = self.row_count_builder.finish();
        let memory_size_array = self.memory_size_builder.finish();
        let cache_type_array = self.cache_type_builder.finish();
        let file_path_array = self.file_path_builder.finish();
        let reference_count_array = self.reference_count_builder.finish();
        Ok(RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(row_group_id_array),
                Arc::new(column_id_array),
                Arc::new(row_start_id_array),
                Arc::new(row_count_array),
                Arc::new(memory_size_array),
                Arc::new(cache_type_array),
                Arc::new(file_path_array),
                Arc::new(reference_count_array),
            ],
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    fn append_entry(
        &mut self,
        file_path: &str,
        row_group_id: u64,
        column_id: u64,
        row_start_id: u64,
        row_count: Option<u64>,
        memory_size: u64,
        cache_type: &str,
        reference_count: u64,
    ) -> Result<(), ParquetError> {
        self.row_group_id_builder.append_value(row_group_id);
        self.column_id_builder.append_value(column_id);
        self.row_start_id_builder.append_value(row_start_id);
        self.row_count_builder.append_option(row_count);
        self.memory_size_builder.append_value(memory_size);
        self.cache_type_builder.append_value(cache_type);
        self.reference_count_builder.append_value(reference_count);
        self.file_path_builder.append_value(file_path);
        if self.row_start_id_builder.len() >= 8192 {
            let batch = self.build_batch()?;
            self.writer.write(&batch)?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(), ParquetError> {
        let batch = self.build_batch()?;
        self.writer.write(&batch)?;
        self.writer.close()?;
        Ok(())
    }
}

impl LiquidCacheParquet {
    /// Get the memory usage of the cache in bytes.
    pub fn compute_memory_usage_bytes(&self) -> u64 {
        self.cache_store.budget().memory_usage_bytes() as u64
    }

    /// Write the stats of the cache to a parquet file.
    pub fn write_stats(&self, parquet_file_path: impl AsRef<Path>) -> Result<(), ParquetError> {
        let mut writer = StatsWriter::new(parquet_file_path)?;
        self.cache_store.for_each_entry(|entry_id, cached_batch| {
            let memory_size = cached_batch.memory_usage_bytes();
            let row_count = match cached_batch {
                CacheEntry::MemoryArrow(array) => Some(array.len() as u64),
                CacheEntry::MemoryLiquid(array) => Some(array.len() as u64),
            };
            let cache_type = match cached_batch {
                CacheEntry::MemoryArrow(_) => "InMemory",
                CacheEntry::MemoryLiquid(_) => "LiquidMemory",
            };
            let reference_count = cached_batch.reference_count();
            let entry_id = ParquetArrayID::from(*entry_id);
            writer
                .append_entry(
                    &entry_id.display_path(),
                    entry_id.row_group_id_inner(),
                    entry_id.column_id_inner(),
                    entry_id.batch_id_inner() * self.batch_size() as u64,
                    row_count,
                    memory_size as u64,
                    cache_type,
                    reference_count as u64,
                )
                .unwrap();
        });

        writer.finish()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use crate::cache::id::BatchID;

    use super::*;
    use arrow::{
        array::{Array, AsArray},
        datatypes::UInt64Type,
    };
    use bytes::Bytes;
    use liquid_cache::{cache::Evict, cache_policies::LiquidPolicy};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
    use tempfile::NamedTempFile;

    #[test]
    fn test_stats_writer() -> Result<(), ParquetError> {
        let cache = LiquidCacheParquet::new(
            1024,
            usize::MAX,
            Box::new(LiquidPolicy::new()),
            Box::new(Evict),
        );
        let fields: Vec<Field> = (0..8)
            .map(|i| Field::new(format!("test_{i}"), DataType::Int32, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let array = Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]));
        let num_rows = 8 * 8 * 8 * 8;

        let mut row_group_id_sum = 0;
        let mut column_id_sum = 0;
        let mut row_start_id_sum = 0;
        let mut row_count_sum = 0;
        let mut memory_size_sum = 0;
        for file_no in 0..8 {
            let file_name = format!("test_{file_no}.parquet");
            let file = cache.register_or_get_file(file_name, schema.clone());
            for rg in 0..8 {
                // Mark all columns as predicate columns so inserts are accepted.
                let row_group = file.create_row_group(rg, (0..8).collect());
                for col in 0..8 {
                    let column = row_group.get_column(col).unwrap();
                    for batch in 0..8 {
                        let batch_id = BatchID::from_raw(batch);
                        assert!(column.insert(batch_id, array.clone()).is_ok());
                        row_group_id_sum += rg;
                        column_id_sum += col;
                        row_start_id_sum += *batch_id as u64 * cache.batch_size() as u64;
                        row_count_sum += array.len() as u64;
                        memory_size_sum += array.get_array_memory_size();

                        if batch.is_multiple_of(2) {
                            _ = column.get_arrow_array_test_only(batch_id).unwrap();
                        }
                    }
                }
            }
        }

        let mut tmp_file = NamedTempFile::new()?;
        cache.write_stats(tmp_file.path())?;

        // Read and verify stats
        let mut bytes = Vec::new();
        tmp_file.read_to_end(&mut bytes)?;
        let bytes = Bytes::from(bytes);
        let reader = ParquetRecordBatchReader::try_new(bytes, 8192)?;

        let batch = reader.into_iter().next().unwrap()?;
        assert_eq!(batch.num_rows(), num_rows);

        macro_rules! uint64_col {
            ($batch:expr, $col_idx:expr) => {
                $batch
                    .column_by_name($col_idx)
                    .unwrap()
                    .as_primitive::<UInt64Type>()
            };
        }

        let row_group_id_array = uint64_col!(batch, "row_group_id");
        let column_id_array = uint64_col!(batch, "column_id");
        let row_start_id_array = uint64_col!(batch, "row_start_id");
        let row_count_array = uint64_col!(batch, "row_count");
        let memory_size_array = uint64_col!(batch, "memory_size");

        assert_eq!(
            row_group_id_array.iter().map(|v| v.unwrap()).sum::<u64>(),
            row_group_id_sum
        );
        assert_eq!(
            column_id_array.iter().map(|v| v.unwrap()).sum::<u64>(),
            column_id_sum
        );
        assert_eq!(
            row_start_id_array.iter().map(|v| v.unwrap()).sum::<u64>(),
            row_start_id_sum
        );
        assert_eq!(
            row_count_array.iter().map(|v| v.unwrap()).sum::<u64>(),
            row_count_sum
        );
        assert_eq!(
            memory_size_array.iter().map(|v| v.unwrap()).sum::<u64>(),
            memory_size_sum as u64
        );

        Ok(())
    }
}
