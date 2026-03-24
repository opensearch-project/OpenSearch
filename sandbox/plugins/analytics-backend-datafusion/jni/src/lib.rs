
use std::fs::File;
use std::sync::Arc;

use arrow::array::{Array, Int32Array, RecordBatch, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::to_ffi;
use datafusion::prelude::*;
use parquet::arrow::ArrowWriter;
use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::jlong;
use jni::JNIEnv;
use tokio::runtime::Runtime;

#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_createRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let rt = Runtime::new().expect("Failed to create Tokio runtime");
    Box::into_raw(Box::new(rt)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_destroyRuntime(
    _env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
) {
    if runtime_ptr != 0 {
        unsafe { drop(Box::from_raw(runtime_ptr as *mut Runtime)) };
    }
}

/// Creates a test parquet file with category (string) + amount (int) data.
/// Used for integration testing.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_createTestParquet<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
) {
    let path_str: String = env.get_string(&path).expect("Invalid path").into();
    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "A", "B", "A", "C", "B", "A", "C", "D",
            ])),
            Arc::new(Int32Array::from(vec![100, 200, 300, 150, 250, 400, 350, 175])),
        ],
    )
    .expect("Failed to create batch");
    let file = File::create(&path_str).expect("Failed to create file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("Failed to create writer");
    writer.write(&batch).expect("Failed to write batch");
    writer.close().expect("Failed to close writer");
}

/// Executes a SQL query against a parquet file and streams results via JNI callback.
///
/// The callback object must implement:
///   void onBatch(long schemaAddr, long arrayAddr)  — called per Arrow batch
///   void onComplete()                               — called when stream ends
///
/// Schema/array addresses point to heap-allocated FFI structs. Java takes ownership
/// via Arrow's release callback; the Box wrapper is intentionally leaked (~64 bytes/batch).
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_executeAndStream<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    runtime_ptr: jlong,
    path: JString<'local>,
    sql: JString<'local>,
    callback: JObject<'local>,
) {
    let rt = unsafe { &*(runtime_ptr as *const Runtime) };
    let path_str: String = env.get_string(&path).expect("Invalid path").into();
    let sql_str: String = env.get_string(&sql).expect("Invalid sql").into();

    let batches = rt.block_on(async {
        let ctx = SessionContext::new();
        ctx.register_parquet("t", &path_str, ParquetReadOptions::default())
            .await
            .expect("Failed to register parquet");
        let df = ctx.sql(&sql_str).await.expect("Failed to execute SQL");
        df.collect().await.expect("Failed to collect results")
    });

    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let struct_array: StructArray = batch.clone().into();
        let (ffi_array, ffi_schema) =
            to_ffi(&struct_array.into_data()).expect("FFI export failed");

        // Heap-allocate so Java can access via Arrow's C Data Interface
        let array_ptr = Box::into_raw(Box::new(ffi_array)) as jlong;
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as jlong;

        env.call_method(
            &callback,
            "onBatch",
            "(JJ)V",
            &[JValue::Long(schema_ptr), JValue::Long(array_ptr)],
        )
        .expect("onBatch callback failed");
    }

    env.call_method(&callback, "onComplete", "()V", &[])
        .expect("onComplete callback failed");
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ffi::from_ffi;
    use datafusion::prelude::*;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    use std::sync::Arc;

    use arrow::array::Array;
    use super::*;

    fn create_test_parquet(path: &str) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    "A", "B", "A", "C", "B", "A", "C", "D",
                ])),
                Arc::new(Int32Array::from(vec![100, 200, 300, 150, 250, 400, 350, 175])),
            ],
        )
        .unwrap();
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn test_query_parquet() {
        let path = "/tmp/test_analytics_poc.parquet";
        create_test_parquet(path);

        let ctx = SessionContext::new();
        ctx.register_parquet("t", path, ParquetReadOptions::default())
            .await
            .unwrap();
        let df = ctx.sql(
            "SELECT category, COUNT(*) as cnt, SUM(amount) as total \
             FROM t GROUP BY category ORDER BY category",
        )
        .await
        .unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 4); // A, B, C, D
    }

    #[tokio::test]
    async fn test_ffi_roundtrip() {
        let path = "/tmp/test_analytics_ffi.parquet";
        create_test_parquet(path);

        let ctx = SessionContext::new();
        ctx.register_parquet("t", path, ParquetReadOptions::default())
            .await
            .unwrap();
        let df = ctx
            .sql("SELECT category, amount FROM t ORDER BY category")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        let batch = &results[0];
        let struct_array: StructArray = batch.clone().into();
        let (ffi_array, ffi_schema) = to_ffi(&struct_array.into_data()).unwrap();
        let imported = unsafe { from_ffi(ffi_array, &ffi_schema) }.unwrap();
        let imported_struct = StructArray::from(imported);
        assert_eq!(imported_struct.len(), batch.num_rows());
    }
}
