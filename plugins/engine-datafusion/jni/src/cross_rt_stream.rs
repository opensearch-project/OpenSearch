use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use crate::executor::{DedicatedExecutor, JobError};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt, ready};
use tokio::sync::mpsc::{Sender, channel};
use tokio_stream::wrappers::ReceiverStream;

// Copy of - https://github.com/influxdata/influxdb3_core/blob/main/iox_query/src/exec/cross_rt_stream.rs
/// A stream adapter that bridges data from one Tokio runtime to another.
///
/// This is useful when you need to execute a DataFusion stream on a dedicated
/// executor (e.g., for CPU-intensive work) but consume the results on a different
/// runtime (e.g., the main I/O runtime).
///
///
/// The stream uses a channel-based approach:
/// - A "driver" future runs on the source runtime, pulling data from the source stream
/// - Data is sent through an MPSC channel to the consumer runtime
/// - The `CrossRtStream` polls both the driver and the receiver to ensure proper cleanup
///
///
/// We need to poll both the driver and the inner receiver because:
/// 1. The inner stream tells us when data is available or the channel is closed
/// 2. The driver tells us when the background task has fully completed
/// 3. We only return `Poll::Ready(None)` when BOTH are done to ensure proper cleanup
pub struct CrossRtStream {
    /// The background task that drives the source stream and sends data through the channel.
    /// This future runs on the dedicated executor and handles:
    /// - Polling the source stream
    /// - Sending results through the channel
    /// - Error handling and conversion
    driver: BoxFuture<'static, ()>,

    /// Tracks whether the driver future has completed.
    /// We need to poll the driver to completion even after the channel closes
    /// to ensure proper cleanup and avoid leaked resources.
    driver_ready: bool,

    /// The receiving end of the channel, wrapped in a stream adapter.
    /// This receives `RecordBatch` results from the driver running on another runtime.
    inner: ReceiverStream<Result<RecordBatch, DataFusionError>>,

    /// Tracks whether the inner stream has ended (channel closed or exhausted).
    /// Once true, we only need to wait for the driver to complete before returning `Poll::Ready(None)`.
    inner_done: bool,

    /// The Arrow schema for the record batches in this stream.
    /// Cached here so it can be returned synchronously without runtime interaction.
    schema: SchemaRef,
}

impl CrossRtStream {
    /// Creates a new `CrossRtStream` with a custom driver function.
    ///
    /// # Arguments
    ///
    /// * `f` - A function that receives a channel sender and returns a future.
    ///         This future will be the driver that sends data through the channel.
    /// * `schema` - The Arrow schema for the record batches
    ///
    /// # Type Parameters
    ///
    /// * `F` - The function type that creates the driver future
    /// * `Fut` - The future type returned by `F`, must be `Send + 'static`
    fn new_with_tx<F, Fut>(f: F, schema: SchemaRef) -> Self
    where
        F: FnOnce(Sender<Result<RecordBatch, DataFusionError>>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        // Create a channel with buffer size 1
        let (tx, rx) = channel(1);

        // Create the driver future by calling the provided function
        let driver = f(tx).boxed();

        Self {
            driver,
            driver_ready: false,
            inner: ReceiverStream::new(rx),
            inner_done: false,
            schema,
        }
    }

    /// Creates a new `CrossRtStream` from a DataFusion stream and dedicated executor.
    ///
    /// This is the primary constructor that sets up cross-runtime streaming.
    ///
    /// # How it works
    ///
    /// 1. Captures the source stream's schema
    /// 2. Spawns a task on the dedicated executor that:
    ///    - Polls the source stream
    ///    - Sends each result through the channel
    ///    - Stops if the channel is closed (consumer dropped)
    /// 3. Wraps the spawned task to handle executor errors (panics, shutdown)
    ///
    /// # Arguments
    ///
    /// * `stream` - The source DataFusion stream to read from
    /// * `exec` - The dedicated executor where the stream should be polled
    pub fn new_with_df_error_stream(
        stream: SendableRecordBatchStream,
        exec: DedicatedExecutor,
    ) -> Self {
        let schema = stream.schema();

        Self::new_with_tx(
            |tx| {
                // Clone the sender for the inner task
                let tx_captured = tx.clone();

                // Create the inner task that pulls from the stream
                let fut = async move {
                    // Pin the stream to poll it
                    tokio::pin!(stream);

                    // Pull items from the stream and send them through the channel
                    while let Some(res) = stream.next().await {
                        // If send fails, the receiver was dropped, so stop
                        if tx_captured.send(res).await.is_err() {
                            return;
                        }
                    }
                };

                // Wrap the inner task in executor error handling
                async move {
                    // Spawn the task on the dedicated executor
                    if let Err(e) = exec.spawn(fut).await {
                        // Convert executor errors to DataFusion errors
                        let err = match e {
                            JobError::Panic { msg } => {
                                DataFusionError::Execution(format!("Panic: {}", msg))
                            }
                            JobError::WorkerGone => {
                                DataFusionError::Execution("Worker gone".to_string())
                            }
                        };
                        // Try to send the error; if it fails, the receiver is already gone
                        tx.send(Err(err)).await.ok();
                    }
                }
            },
            schema,
        )
    }

    /// Returns the Arrow schema for this stream.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for CrossRtStream {
    type Item = Result<RecordBatch, DataFusionError>;

    /// Polls the stream for the next item.
    ///
    /// # Polling Strategy
    ///
    /// This implementation carefully manages two futures:
    /// 1. The driver (background task)
    /// 2. The inner receiver stream
    ///
    /// The stream only completes (`Poll::Ready(None)`) when:
    /// - The inner stream has ended (channel closed), AND
    /// - The driver has completed
    ///
    /// This ensures proper cleanup and prevents resource leaks.
    ///
    /// # State Machine
    ///
    /// ```text
    /// ┌─────────────────────────────────────────────────┐
    /// │ Initial State                                    │
    /// │ driver_ready: false, inner_done: false          │
    /// └─────────────────────────────────────────────────┘
    ///                        │
    ///                        ▼
    ///         ┌──────────────────────────────┐
    ///         │ Poll driver (non-blocking)   │
    ///         │ Update driver_ready if ready │
    ///         └──────────────────────────────┘
    ///                        │
    ///                        ▼
    ///              ┌─────────────────┐
    ///              │ inner_done?     │
    ///              └─────────────────┘
    ///                 │           │
    ///                No          Yes
    ///                 │           │
    ///                 ▼           ▼
    ///         ┌─────────────┐  ┌──────────────┐
    ///         │ Poll inner  │  │ driver_ready?│
    ///         │ stream      │  └──────────────┘
    ///         └─────────────┘      │      │
    ///              │              Yes    No
    ///         ┌────┴────┐          │      │
    ///        Some      None        │      │
    ///         │          │          │      │
    ///         ▼          ▼          ▼      ▼
    ///    Return item  Set inner_done  Ready(None)  Pending
    ///                      │
    ///                      ▼
    ///              ┌──────────────┐
    ///              │ driver_ready?│
    ///              └──────────────┘
    ///                  │      │
    ///                 Yes    No
    ///                  │      │
    ///                  ▼      ▼
    ///             Ready(None) Pending
    /// ```
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        // Always poll the driver to ensure it makes progress
        // We do this non-blocking: if it's not ready, we continue anyway
        if !this.driver_ready {
            let res = this.driver.poll_unpin(cx);
            if res.is_ready() {
                this.driver_ready = true;
            }
        }

        // Check if the inner stream already ended
        if this.inner_done {
            // Inner stream is done; only complete if driver is also done
            if this.driver_ready {
                Poll::Ready(None)
            } else {
                // Driver still running; keep polling
                Poll::Pending
            }
        } else {
            // Poll the inner stream for the next item
            match ready!(this.inner.poll_next_unpin(cx)) {
                None => {
                    // Inner stream ended (channel closed)
                    this.inner_done = true;

                    // Only complete if driver is also done
                    if this.driver_ready {
                        Poll::Ready(None)
                    } else {
                        // Driver still running; wait for it to complete
                        Poll::Pending
                    }
                }
                Some(x) => {
                    // Got an item from the stream; return it
                    Poll::Ready(Some(x))
                }
            }
        }
    }
}