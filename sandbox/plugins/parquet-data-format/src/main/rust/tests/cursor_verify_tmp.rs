// Temporary verification test for the parquet_decode_page_at_row retained cursor.
// Verifies sequential page decode (cursor reuse), row-group transitions, and
// backwards seeks (cursor invalidation) all return values identical to the
// row-by-row slow path.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use opensearch_parquet_format::ffm::{
    parquet_close_column_reader, parquet_decode_page_at_row, parquet_open_column_reader,
    parquet_read_value_at_row,
};

const N_ROWS: i64 = 5000;

fn expected(i: i64) -> Option<i64> {
    if i % 7 == 0 {
        None
    } else {
        Some(i * 3)
    }
}

fn write_test_file(path: &str) {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(100)
        .set_write_batch_size(100)
        .set_max_row_group_size(2000) // 3 row groups: 2000 + 2000 + 1000
        .build();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    // Write in small batches so data_page_row_count_limit takes effect.
    let mut i = 0i64;
    while i < N_ROWS {
        let end = (i + 100).min(N_ROWS);
        let vals: Int64Array = (i..end).map(expected).collect();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vals)]).unwrap();
        writer.write(&batch).unwrap();
        i = end;
    }
    writer.close().unwrap();
}

unsafe fn open_reader(path: &str) -> i64 {
    let col = "v";
    let h = parquet_open_column_reader(
        path.as_ptr(),
        path.len() as i64,
        col.as_ptr(),
        col.len() as i64,
        1, // TYPE_INT64
    );
    assert!(h >= 0, "open failed: {}", h);
    h
}

/// Decodes the page containing `row`; returns (first_row, last_row, values, presence_bits).
unsafe fn decode_page(handle: i64, row: i64) -> (i64, i64, Vec<i64>, Vec<i64>) {
    let mut first = 0i64;
    let mut last = 0i64;
    let mut actual_len = 0i64;
    let cap_rows = 8192usize;
    let mut values = vec![0i64; cap_rows];
    let mut presence = vec![0i64; (cap_rows + 63) / 64];
    let rc = parquet_decode_page_at_row(
        handle,
        row,
        &mut first,
        &mut last,
        values.as_mut_ptr() as *mut u8,
        (values.len() * 8) as i64,
        &mut actual_len,
        std::ptr::null_mut(),
        0,
        presence.as_mut_ptr(),
        presence.len() as i64,
    );
    assert_eq!(rc, 0, "decode failed rc={} at row {}", rc, row);
    let rows = (last - first + 1) as usize;
    values.truncate(rows);
    presence.truncate((rows + 63) / 64);
    (first, last, values, presence)
}

unsafe fn assert_page_correct(first: i64, last: i64, values: &[i64], presence: &[i64]) {
    for (k, g) in (first..=last).enumerate() {
        let present = (presence[k / 64] >> (k % 64)) & 1 == 1;
        match expected(g) {
            Some(v) => {
                assert!(present, "row {} should be present", g);
                assert_eq!(values[k], v, "row {} value mismatch", g);
            }
            None => {
                assert!(!present, "row {} should be null", g);
                assert_eq!(values[k], 0, "null row {} slot must be 0", g);
            }
        }
    }
}

#[test]
fn cursor_sequential_backward_and_slowpath_consistency() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cursor_verify.parquet");
    let path = path.to_str().unwrap().to_string();
    write_test_file(&path);

    unsafe {
        let h = open_reader(&path);

        // 1. Full sequential sweep (the hot path): decode every page in order,
        //    crossing row-group boundaries. Cursor should be reused within each
        //    row group and rebuilt across groups; values must be exact.
        let mut row = 0i64;
        let mut n_pages = 0;
        while row < N_ROWS {
            let (first, last, values, presence) = decode_page(h, row);
            assert_eq!(first, row, "pages must tile the row space");
            assert_page_correct(first, last, &values, &presence);
            row = last + 1;
            n_pages += 1;
        }
        assert!(
            n_pages > 3,
            "expected page-granular layout (page index), got {} pages",
            n_pages
        );

        // 2. Backwards seek: re-decode an early page after the sweep (cursor is
        //    positioned at EOF — must fall back to a fresh reader, not garbage).
        let (first, last, values, presence) = decode_page(h, 150);
        assert!(first <= 150 && last >= 150);
        assert_page_correct(first, last, &values, &presence);

        // 3. Re-decode the SAME page again (cursor position == last+1 > first — miss).
        let (f2, l2, v2, p2) = decode_page(h, 150);
        assert_eq!((f2, l2), (first, last));
        assert_eq!(v2, values);
        assert_eq!(p2, presence);

        // 4. Skip-ahead within the same row group (cursor hit with skip > 0):
        //    jump from the page after row 150 to a page much later in the same group.
        let (f3, l3, v3, p3) = decode_page(h, 1900); // still in rg 0 (rows 0..2000)
        assert!(f3 <= 1900 && l3 >= 1900);
        assert_page_correct(f3, l3, &v3, &p3);

        // 5. Cross-check random rows against the slow path (fresh reader per call).
        for &r in &[0i64, 1, 6, 7, 99, 100, 1999, 2000, 2001, 3999, 4000, 4999] {
            let mut present = 0i64;
            let mut long = 0i64;
            let rc = parquet_read_value_at_row(
                h,
                r,
                &mut present,
                &mut long,
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
            );
            assert_eq!(rc, 0);
            match expected(r) {
                Some(v) => {
                    assert_eq!(present, 1, "slow path row {}", r);
                    assert_eq!(long, v, "slow path row {}", r);
                }
                None => assert_eq!(present, 0, "slow path row {}", r),
            }
        }

        assert_eq!(parquet_close_column_reader(h), 0);
    }
}
