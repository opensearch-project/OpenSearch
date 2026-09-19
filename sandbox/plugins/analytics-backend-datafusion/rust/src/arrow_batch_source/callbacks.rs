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
