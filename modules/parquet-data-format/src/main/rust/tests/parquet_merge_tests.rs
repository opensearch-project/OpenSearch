use parquet_dataformat_jni::process_parquet_files;
use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::PathBuf;

/// Helper to get test file from resources
fn test_file(name: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("../../test/resources/parquetTestFiles");
    path.push(name);
    path.to_string_lossy().to_string()
}

/// Helper to read a Parquet file into record batches
fn read_batches(path: &str) -> Vec<RecordBatch> {
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    reader.map(|r| r.unwrap()).collect()
}

#[test]
fn test_process_parquet_files_empty_input() {
    let output_path = std::env::temp_dir().join("test_output_empty.parquet");
    let result = process_parquet_files(&[], output_path.to_str().unwrap(), &[]);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().to_string(), "No input files provided");
}

#[test]
fn test_process_parquet_files_nonexistent_file() {
    let output_path = std::env::temp_dir().join("test_output_nonexistent.parquet");
    let result = process_parquet_files(&["/nonexistent/file.parquet".to_string()], output_path.to_str().unwrap(), &[]);
    assert!(result.is_err());
}

#[test]
fn test_process_single_file() {
    let input_path = test_file("small_file1.parquet");
    let output_path = std::env::temp_dir().join("test_output_single.parquet");

    process_parquet_files(&[input_path.clone()], output_path.to_str().unwrap(), &[]).unwrap();

    let batches = read_batches(output_path.to_str().unwrap());
    assert!(!batches.is_empty());

    // Verify ___row_id increments
    for (batch_index, batch) in batches.iter().enumerate() {
        let row_id_idx = batch.schema().fields().iter().position(|f| f.name() == "___row_id").unwrap();
        let row_id_column = batch.column(row_id_idx).as_any().downcast_ref::<Int64Array>().unwrap();

        for i in 0..batch.num_rows() {
            assert_eq!(row_id_column.value(i), (batch_index * batch.num_rows() + i) as i64);
        }
    }

    std::fs::remove_file(output_path).ok();
}

#[test]
fn test_merge_files_with_complete_data_verification() {
    let input1 = test_file("small_file1.parquet");
    let input2 = test_file("small_file2.parquet");
    let output_path = std::env::temp_dir().join("test_output_complete_merge.parquet");

    process_parquet_files(&[input1, input2], output_path.to_str().unwrap(), &[]).unwrap();

    let batches = read_batches(output_path.to_str().unwrap());
    let mut all_row_ids = vec![];
    let mut all_names = vec![];
    let mut all_ages = vec![];
    let mut all_cities = vec![];

    for batch in batches {
        let schema = batch.schema();
        let row_id_idx = schema.fields().iter().position(|f| f.name() == "___row_id").unwrap();
        let name_idx = schema.fields().iter().position(|f| f.name() == "Name").unwrap();
        let age_idx = schema.fields().iter().position(|f| f.name() == "Age").unwrap();
        let city_idx = schema.fields().iter().position(|f| f.name() == "City").unwrap();

        let row_id_col = batch.column(row_id_idx).as_any().downcast_ref::<Int64Array>().unwrap();
        let name_col = batch.column(name_idx).as_any().downcast_ref::<StringArray>().unwrap();
        let age_col = batch.column(age_idx).as_any().downcast_ref::<Int64Array>().unwrap();
        let city_col = batch.column(city_idx).as_any().downcast_ref::<StringArray>().unwrap();

        for i in 0..batch.num_rows() {
            all_row_ids.push(row_id_col.value(i));
            all_names.push(name_col.value(i).to_string());
            all_ages.push(age_col.value(i));
            all_cities.push(city_col.value(i).to_string());
        }
    }

    assert_eq!(all_row_ids, vec![0, 1, 2, 3]);
    assert_eq!(all_names, vec!["John", "Jane", "Shailesh", "Singh"]);
    assert_eq!(all_ages, vec![30, 25, 23, 6]);
    assert_eq!(all_cities, vec!["New York", "London", "Delhi", "Bangalore"]);

    std::fs::remove_file(output_path).ok();
}
