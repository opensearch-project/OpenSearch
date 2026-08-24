/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};

use arrow_array::ffi::{from_ffi, FFI_ArrowArray};
use arrow_array::{RecordBatch, StructArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use datafusion::common::DataFusionError;

const CANCELLED: i64 = -1;
const ERROR: i64 = -2;
const EMPTY_BATCH: i64 = -3;
const ERROR_CAPACITY: usize = 2048;

type CreateSourceFn = unsafe extern "C" fn(i64, *const i32, i64, *mut u8, i64) -> i32;
type NextBatchFn =
    unsafe extern "C" fn(i64, i32, *mut FFI_ArrowArray, *mut FFI_ArrowSchema, *mut u8, i64) -> i64;
type CancelSourceFn = unsafe extern "C" fn(i64, i32);
type ReleaseSourceFn = unsafe extern "C" fn(i64, i32);

static CREATE_SOURCE: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());
static NEXT_BATCH: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());
static CANCEL_SOURCE: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());
static RELEASE_SOURCE: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

#[no_mangle]
pub unsafe extern "C" fn df_register_arrow_batch_source_callbacks(
    create_source: CreateSourceFn,
    next_batch: NextBatchFn,
    cancel_source: CancelSourceFn,
    release_source: ReleaseSourceFn,
) {
    CREATE_SOURCE.store(create_source as *mut (), Ordering::Release);
    NEXT_BATCH.store(next_batch as *mut (), Ordering::Release);
    CANCEL_SOURCE.store(cancel_source as *mut (), Ordering::Release);
    RELEASE_SOURCE.store(release_source as *mut (), Ordering::Release);
}

fn create_callback() -> Result<CreateSourceFn, DataFusionError> {
    let pointer = CREATE_SOURCE.load(Ordering::Acquire);
    if pointer.is_null() {
        return Err(DataFusionError::Execution(
            "Arrow batch source callbacks are not registered".into(),
        ));
    }
    Ok(unsafe { std::mem::transmute::<*mut (), CreateSourceFn>(pointer) })
}

fn next_callback() -> Result<NextBatchFn, DataFusionError> {
    let pointer = NEXT_BATCH.load(Ordering::Acquire);
    if pointer.is_null() {
        return Err(DataFusionError::Execution(
            "Arrow batch source callbacks are not registered".into(),
        ));
    }
    Ok(unsafe { std::mem::transmute::<*mut (), NextBatchFn>(pointer) })
}

fn cancel_callback() -> Option<CancelSourceFn> {
    let pointer = CANCEL_SOURCE.load(Ordering::Acquire);
    (!pointer.is_null()).then(|| unsafe { std::mem::transmute::<*mut (), CancelSourceFn>(pointer) })
}

fn release_callback() -> Option<ReleaseSourceFn> {
    let pointer = RELEASE_SOURCE.load(Ordering::Acquire);
    (!pointer.is_null())
        .then(|| unsafe { std::mem::transmute::<*mut (), ReleaseSourceFn>(pointer) })
}

#[derive(Debug)]
pub struct ArrowBatchSourceHandle {
    binding_id: i64,
    source_key: i32,
    cancelled: AtomicBool,
}

impl ArrowBatchSourceHandle {
    pub fn create(binding_id: i64, projection: &[usize]) -> Result<Self, DataFusionError> {
        let projected: Result<Vec<i32>, _> =
            projection.iter().copied().map(i32::try_from).collect();
        let projected = projected.map_err(|_| {
            DataFusionError::Execution("Arrow batch source projection exceeds i32".into())
        })?;
        let callback = create_callback()?;
        let mut error = [0u8; ERROR_CAPACITY];
        let key = unsafe {
            callback(
                binding_id,
                projected.as_ptr(),
                projected.len() as i64,
                error.as_mut_ptr(),
                error.len() as i64,
            )
        };
        if key < 0 {
            return Err(callback_error(
                &error,
                format!("Java failed to create Arrow batch source binding {binding_id}"),
            ));
        }
        Ok(Self {
            binding_id,
            source_key: key,
            cancelled: AtomicBool::new(false),
        })
    }

    pub fn next_batch(&self) -> Result<Option<RecordBatch>, DataFusionError> {
        let callback = next_callback()?;
        let mut array = FFI_ArrowArray::empty();
        let mut schema = FFI_ArrowSchema::empty();
        let mut error = [0u8; ERROR_CAPACITY];
        let status = unsafe {
            callback(
                self.binding_id,
                self.source_key,
                &mut array,
                &mut schema,
                error.as_mut_ptr(),
                error.len() as i64,
            )
        };
        match status {
            0 => Ok(None),
            CANCELLED => Err(DataFusionError::Execution("query cancelled".into())),
            ERROR => Err(callback_error(
                &error,
                "Java Arrow batch source failed".into(),
            )),
            EMPTY_BATCH => import_batch(array, schema, 0),
            rows if rows > 0 => import_batch(array, schema, rows as usize),
            other => Err(DataFusionError::Execution(format!(
                "unexpected Arrow batch callback status {other}"
            ))),
        }
    }

    /// Requests cooperative cancellation without releasing the active source.
    pub fn cancel(&self) {
        if self.cancelled.swap(true, Ordering::AcqRel) == false {
            if let Some(callback) = cancel_callback() {
                unsafe { callback(self.binding_id, self.source_key) };
            }
        }
    }
}

impl Drop for ArrowBatchSourceHandle {
    fn drop(&mut self) {
        self.cancel();
        if let Some(callback) = release_callback() {
            unsafe { callback(self.binding_id, self.source_key) };
        }
    }
}

fn import_batch(
    array: FFI_ArrowArray,
    schema: FFI_ArrowSchema,
    expected_rows: usize,
) -> Result<Option<RecordBatch>, DataFusionError> {
    let mut data = unsafe { from_ffi(array, &schema) }.map_err(|error| {
        DataFusionError::Execution(format!("failed to import Java Arrow batch: {error}"))
    })?;
    data.align_buffers();
    let batch = RecordBatch::from(StructArray::from(data));
    if batch.num_rows() != expected_rows {
        return Err(DataFusionError::Execution(format!(
            "Arrow batch callback returned row count {expected_rows}, imported {}",
            batch.num_rows()
        )));
    }
    Ok(Some(batch))
}

fn callback_error(buffer: &[u8], fallback: String) -> DataFusionError {
    let length = buffer
        .iter()
        .position(|value| *value == 0)
        .unwrap_or(buffer.len());
    if length == 0 {
        DataFusionError::Execution(fallback)
    } else {
        DataFusionError::Execution(String::from_utf8_lossy(&buffer[..length]).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize};
    use std::sync::{Arc, Mutex};

    use arrow_array::ffi::FFI_ArrowArray;
    use arrow_array::{Array, Int64Array, RecordBatch, StructArray};
    use arrow_schema::ffi::FFI_ArrowSchema;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::catalog::TableProvider;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;

    use super::*;
    use crate::arrow_batch_source::table_provider::ArrowBatchRecordBatchStream;
    use crate::arrow_batch_source::ArrowBatchTableProvider;

    static TEST_LOCK: Mutex<()> = Mutex::new(());
    static PROJECTION: Mutex<Vec<i32>> = Mutex::new(Vec::new());
    static NEXT_COUNT: AtomicUsize = AtomicUsize::new(0);
    static BLOCKING_PULL_ENTERED: AtomicBool = AtomicBool::new(false);
    static CANCEL_COUNT: AtomicUsize = AtomicUsize::new(0);
    static RELEASE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static SOURCE_KEY: AtomicI32 = AtomicI32::new(1);

    struct CallbackReset {
        create: *mut (),
        next: *mut (),
        cancel: *mut (),
        release: *mut (),
    }

    impl CallbackReset {
        fn capture() -> Self {
            Self {
                create: CREATE_SOURCE.load(Ordering::Acquire),
                next: NEXT_BATCH.load(Ordering::Acquire),
                cancel: CANCEL_SOURCE.load(Ordering::Acquire),
                release: RELEASE_SOURCE.load(Ordering::Acquire),
            }
        }
    }

    impl Drop for CallbackReset {
        fn drop(&mut self) {
            CREATE_SOURCE.store(self.create, Ordering::Release);
            NEXT_BATCH.store(self.next, Ordering::Release);
            CANCEL_SOURCE.store(self.cancel, Ordering::Release);
            RELEASE_SOURCE.store(self.release, Ordering::Release);
        }
    }

    unsafe extern "C" fn create_source(
        binding_id: i64,
        projection: *const i32,
        projection_length: i64,
        error: *mut u8,
        error_capacity: i64,
    ) -> i32 {
        if binding_id == 40 {
            write_error(error, error_capacity, "create failed");
            return -1;
        }
        let values = std::slice::from_raw_parts(projection, projection_length as usize);
        *PROJECTION.lock().unwrap() = values.to_vec();
        SOURCE_KEY.fetch_add(1, Ordering::SeqCst)
    }

    unsafe extern "C" fn next_batch(
        binding_id: i64,
        _source_key: i32,
        array: *mut FFI_ArrowArray,
        schema: *mut FFI_ArrowSchema,
        error: *mut u8,
        error_capacity: i64,
    ) -> i64 {
        if binding_id == 20 {
            return CANCELLED;
        }
        if binding_id == 30 {
            write_error(error, error_capacity, "next failed");
            return ERROR;
        }
        if binding_id == 70 {
            BLOCKING_PULL_ENTERED.store(true, Ordering::Release);
            while CANCEL_COUNT.load(Ordering::Acquire) == 0 {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            return CANCELLED;
        }
        if NEXT_COUNT.fetch_add(1, Ordering::SeqCst) > 0 {
            return 0;
        }
        let values = if binding_id == 50 {
            Vec::new()
        } else {
            vec![7_i64, 9_i64]
        };
        let row_count = values.len();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        let struct_array: StructArray = batch.into();
        let data = struct_array.into_data();
        std::ptr::write(array, FFI_ArrowArray::new(&data));
        std::ptr::write(schema, FFI_ArrowSchema::try_from(data.data_type()).unwrap());
        if row_count == 0 {
            EMPTY_BATCH
        } else {
            row_count as i64
        }
    }

    unsafe extern "C" fn cancel_source(_binding_id: i64, _source_key: i32) {
        CANCEL_COUNT.fetch_add(1, Ordering::SeqCst);
    }

    unsafe extern "C" fn release_source(_binding_id: i64, _source_key: i32) {
        RELEASE_COUNT.fetch_add(1, Ordering::SeqCst);
    }

    unsafe fn write_error(pointer: *mut u8, capacity: i64, message: &str) {
        let capacity = capacity as usize;
        let length = message.len().min(capacity.saturating_sub(1));
        std::ptr::copy_nonoverlapping(message.as_ptr(), pointer, length);
        *pointer.add(length) = 0;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn callbacks_and_provider_preserve_projection_status_and_release() {
        let _guard = TEST_LOCK.lock().unwrap();
        let _reset = CallbackReset::capture();
        unsafe {
            df_register_arrow_batch_source_callbacks(
                create_source,
                next_batch,
                cancel_source,
                release_source,
            );
        }
        NEXT_COUNT.store(0, Ordering::SeqCst);
        CANCEL_COUNT.store(0, Ordering::SeqCst);
        RELEASE_COUNT.store(0, Ordering::SeqCst);

        let create_error = ArrowBatchSourceHandle::create(40, &[]).unwrap_err();
        assert!(create_error.to_string().contains("create failed"));

        let cancelled = ArrowBatchSourceHandle::create(20, &[]).unwrap();
        assert!(cancelled
            .next_batch()
            .unwrap_err()
            .to_string()
            .contains("cancelled"));
        drop(cancelled);

        let failed = ArrowBatchSourceHandle::create(30, &[]).unwrap();
        assert!(failed
            .next_batch()
            .unwrap_err()
            .to_string()
            .contains("next failed"));
        drop(failed);

        NEXT_COUNT.store(0, Ordering::SeqCst);
        let empty = ArrowBatchSourceHandle::create(50, &[]).unwrap();
        let empty_batch = empty.next_batch().unwrap().unwrap();
        assert_eq!(empty_batch.num_rows(), 0);
        assert!(empty.next_batch().unwrap().is_none());
        drop(empty);

        let explicitly_cancelled = ArrowBatchSourceHandle::create(60, &[]).unwrap();
        explicitly_cancelled.cancel();
        explicitly_cancelled.cancel();
        drop(explicitly_cancelled);

        NEXT_COUNT.store(0, Ordering::SeqCst);
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let provider = ArrowBatchTableProvider::new(schema, 10, 0);
        let context = SessionContext::new();
        let plan = provider
            .scan(&context.state(), Some(&vec![1]), &[], None)
            .await
            .unwrap();
        let mut stream = plan.execute(0, context.task_ctx()).unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(0).name(), "b");
        assert!(stream.next().await.is_none());
        drop(stream);
        assert_eq!(*PROJECTION.lock().unwrap(), vec![1]);
        assert_eq!(CANCEL_COUNT.load(Ordering::SeqCst), 5);
        assert_eq!(RELEASE_COUNT.load(Ordering::SeqCst), 5);

        CANCEL_COUNT.store(0, Ordering::SeqCst);
        BLOCKING_PULL_ENTERED.store(false, Ordering::SeqCst);
        let token = tokio_util::sync::CancellationToken::new();
        let source = Arc::new(ArrowBatchSourceHandle::create(70, &[]).unwrap());
        let mut blocking_stream = ArrowBatchRecordBatchStream::new(
            Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, true)])),
            source,
            Some(token.clone()),
        );
        let cancel = tokio::spawn(async move {
            while BLOCKING_PULL_ENTERED.load(Ordering::Acquire) == false {
                tokio::task::yield_now().await;
            }
            token.cancel();
        });
        let error =
            tokio::time::timeout(std::time::Duration::from_secs(10), blocking_stream.next())
                .await
                .expect("blocking pull cancellation timed out")
                .expect("cancelled stream must return an error")
                .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        cancel.await.unwrap();
        drop(blocking_stream);
        assert_eq!(CANCEL_COUNT.load(Ordering::SeqCst), 1);
        assert_eq!(RELEASE_COUNT.load(Ordering::SeqCst), 6);
    }
}
