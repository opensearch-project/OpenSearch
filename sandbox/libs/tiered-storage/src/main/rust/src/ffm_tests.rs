use super::*;

#[test]
fn test_destroy_null_returns_error() {
    assert!(ts_destroy_tiered_object_store(0) < 0);
}

#[test]
fn test_create_and_destroy_no_leak() {
    let store_ptr = ts_create_tiered_object_store(0, 0);
    assert!(store_ptr > 0);
    assert_eq!(ts_destroy_tiered_object_store(store_ptr), 0);
}

#[test]
fn test_register_files_null_store_returns_error() {
    let entries = b"test.parquet\nremote/test.parquet";
    let result = ts_register_files(0, entries.as_ptr(), entries.len() as i64, 1, 1);
    assert!(result < 0);
}

#[test]
fn test_remove_file_null_store_returns_error() {
    let result = ts_remove_file(0, b"test.parquet".as_ptr(), 12);
    assert!(result < 0);
}

#[test]
fn test_register_files_and_remove_round_trip() {
    let store_ptr = ts_create_tiered_object_store(0, 0);
    assert!(store_ptr > 0);

    // Batch register: two files as Remote (triplets: path\nremotePath\nsize\n...)
    let entries = b"data/seg_0.parquet\nremote/seg_0.parquet\n1024\ndata/local.parquet\n\n0";
    let result = ts_register_files(store_ptr, entries.as_ptr(), entries.len() as i64, 2, 1);
    assert_eq!(result, 0);

    // Remove one
    let result = ts_remove_file(store_ptr, b"data/seg_0.parquet".as_ptr(), 18);
    assert_eq!(result, 0);

    assert_eq!(ts_destroy_tiered_object_store(store_ptr), 0);
}

#[test]
fn test_register_files_invalid_location_returns_error() {
    let store_ptr = ts_create_tiered_object_store(0, 0);
    assert!(store_ptr > 0);

    let entries = b"test.parquet\nremote/test.parquet\n2048";
    let result = ts_register_files(store_ptr, entries.as_ptr(), entries.len() as i64, 1, 99);
    assert!(result < 0);

    assert_eq!(ts_destroy_tiered_object_store(store_ptr), 0);
}
