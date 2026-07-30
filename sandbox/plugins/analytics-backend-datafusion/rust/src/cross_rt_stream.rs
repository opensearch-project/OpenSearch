/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::executor::{DedicatedExecutor, JobError};
use crate::phantom_corrector::PhantomCorrector;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use tokio::task::AbortHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

/// Fires its `oneshot` when dropped. Held inside the spawned task body so the signal is sent on
/// every exit path (drain, error, abort-unwind, panic).
struct DoneGuard(Option<oneshot::Sender<()>>);

impl Drop for DoneGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

// This is used to execute a DataFusion stream on a dedicated CPU executor but consume the results on
// the main I/O runtime.
// Based on InfluxDB's cross_rt_stream pattern.
// https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/query_planning/thread_pools.rs
pub struct CrossRtStream {
    driver: BoxFuture<'static, ()>,
    driver_ready: bool,
    inner: ReceiverStream<Result<RecordBatch, DataFusionError>>,
    inner_done: bool,
    schema: SchemaRef,
    phantom_corrector: Option<Arc<PhantomCorrector>>,
}

impl CrossRtStream {
    fn new_with_tx<F, Fut>(f: F, schema: SchemaRef) -> Self
    where
        F: FnOnce(Sender<Result<RecordBatch, DataFusionError>>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (tx, rx) = channel(2);
        let driver = f(tx).boxed();
        Self {
            driver,
            driver_ready: false,
            inner: ReceiverStream::new(rx),
            inner_done: false,
            schema,
            phantom_corrector: None,
        }
    }

    /// Creates a CrossRtStream that runs the DataFusion stream on the CPU executor.
    pub fn new_with_df_error_stream(
        stream: SendableRecordBatchStream,
        exec: DedicatedExecutor,
    ) -> Self {
        let (cross_rt, _abort_handle, _done_rx) =
            Self::new_with_df_error_stream_cancellable(stream, exec, None);
        cross_rt
    }

    /// Like [`new_with_df_error_stream`](Self::new_with_df_error_stream), but also returns an
    /// [`AbortHandle`] and a `oneshot::Receiver` that fires once the spawned task has fully dropped
    /// (completion or abort) — the barrier `stream_close` waits on before the allocator closes.
    ///
    /// When `cancel_token` is supplied, cancel is cooperative: the producer breaks on the token and
    /// runs the drop(stream)+drain cleanup, instead of an abort() mid-send that skips it. Callers
    /// that pass `None` are unchanged.
    pub fn new_with_df_error_stream_cancellable(
        stream: SendableRecordBatchStream,
        exec: DedicatedExecutor,
        cancel_token: Option<CancellationToken>,
    ) -> (Self, Option<AbortHandle>, oneshot::Receiver<()>) {
        let schema = stream.schema();
        let (tx, rx) = channel(1);
        let tx_captured = tx.clone();
        let (done_tx, done_rx) = oneshot::channel();

        let fut = async move {
            let _done = DoneGuard(Some(done_tx));
            let mut stream = Box::pin(stream);
            // Cooperative cancel: select each await against the token so a cancel breaks the loop
            // and falls through to the drop(stream)+drain below, rather than an abort() mid-send.
            loop {
                let next = tokio::select! {
                    biased;
                    _ = async { match &cancel_token { Some(t) => t.cancelled().await, None => std::future::pending::<()>().await } } => break,
                    n = stream.next() => n,
                };
                let res = match next {
                    Some(r) => r,
                    None => break,
                };
                let sent = tokio::select! {
                    biased;
                    _ = async { match &cancel_token { Some(t) => t.cancelled().await, None => std::future::pending::<()>().await } } => break,
                    r = tx_captured.send(res) => r,
                };
                if sent.is_err() {
                    break;
                }
            }
            // Drop the inner stream while this future is still polled — frees the aggregate's own
            // GroupValues and schedules its child producer tasks for abort. The remaining deferred
            // child drops are reaped by stream_close (it waits on task_done, then flush_cpu_runtime).
            drop(stream);
        };

        let (abort_handle, join_fut) = exec.spawn_with_abort_handle(fut);

        let driver = async move {
            if let Err(e) = join_fut.await {
                let err = match e {
                    JobError::Panic { msg } => {
                        DataFusionError::Execution(format!("Panic: {}", msg))
                    }
                    JobError::WorkerGone => DataFusionError::Execution("Worker gone".to_string()),
                };
                tx.send(Err(err)).await.ok();
            }
        }
        .boxed();

        let cross_rt = Self {
            driver,
            driver_ready: false,
            inner: ReceiverStream::new(rx),
            inner_done: false,
            schema,
            phantom_corrector: None,
        };

        (cross_rt, abort_handle, done_rx)
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Attach a phantom corrector for self-correcting budget.
    pub fn with_phantom_corrector(mut self, corrector: Arc<PhantomCorrector>) -> Self {
        self.phantom_corrector = Some(corrector);
        self
    }
}

impl Stream for CrossRtStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        if !this.driver_ready {
            if this.driver.poll_unpin(cx).is_ready() {
                this.driver_ready = true;
            }
        }

        if this.inner_done {
            return if this.driver_ready {
                Poll::Ready(None)
            } else {
                Poll::Pending
            };
        }

        match this.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(item)) => {
                if let (Some(corrector), Ok(batch)) = (&this.phantom_corrector, &item) {
                    corrector.observe_batch(batch.get_array_memory_size());
                }
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                this.inner_done = true;
                if this.driver_ready {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::Int64Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;

    use crate::executor::DedicatedExecutor;

    fn test_exec() -> DedicatedExecutor {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(2).enable_all();
        DedicatedExecutor::new("test-cpu", builder, num_cpus::get())
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]))
    }

    fn test_batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_cross_rt_stream_basic() {
        let exec = test_exec();
        let schema = test_schema();
        let batches = vec![Ok(test_batch(&[1, 2, 3])), Ok(test_batch(&[4, 5]))];
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(batches),
        ));

        let cross = CrossRtStream::new_with_df_error_stream(inner, exec.clone());
        let wrapped = RecordBatchStreamAdapter::new(cross.schema(), cross);
        tokio::pin!(wrapped);

        let mut total_rows = 0;
        while let Some(batch) = wrapped.next().await {
            total_rows += batch.unwrap().num_rows();
        }
        assert_eq!(total_rows, 5);
        exec.join_blocking();
    }

    #[tokio::test]
    async fn test_cross_rt_stream_empty() {
        let exec = test_exec();
        let schema = test_schema();
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::empty(),
        ));

        let cross = CrossRtStream::new_with_df_error_stream(inner, exec.clone());
        let wrapped = RecordBatchStreamAdapter::new(cross.schema(), cross);
        tokio::pin!(wrapped);

        assert!(wrapped.next().await.is_none());
        exec.join_blocking();
    }

    #[tokio::test]
    async fn test_cross_rt_stream_error_propagation() {
        let exec = test_exec();
        let schema = test_schema();
        let batches: Vec<Result<RecordBatch, DataFusionError>> = vec![
            Ok(test_batch(&[1])),
            Err(DataFusionError::Execution("test error".to_string())),
        ];
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(batches),
        ));

        let cross = CrossRtStream::new_with_df_error_stream(inner, exec.clone());
        let wrapped = RecordBatchStreamAdapter::new(cross.schema(), cross);
        tokio::pin!(wrapped);

        // First batch succeeds
        assert!(wrapped.next().await.unwrap().is_ok());
        // Second batch is an error
        let err = wrapped.next().await.unwrap().unwrap_err();
        assert!(err.to_string().contains("test error"));
        exec.join_blocking();
    }

    #[tokio::test]
    async fn test_cross_rt_stream_schema_preserved() {
        let exec = test_exec();
        let schema = test_schema();
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::empty::<Result<RecordBatch, DataFusionError>>(),
        ));

        let cross = CrossRtStream::new_with_df_error_stream(inner, exec.clone());
        assert_eq!(cross.schema(), schema);
        exec.join_blocking();
    }

    // done_rx must fire only after the spawned task fully unwinds (its borrowed batches dropped),
    // on both exit paths: full drain and abort.

    #[tokio::test]
    async fn done_rx_fires_after_full_drain() {
        let exec = test_exec();
        let schema = test_schema();
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(test_batch(&[1, 2, 3]))]),
        ));

        let (cross, _abort, done_rx) =
            CrossRtStream::new_with_df_error_stream_cancellable(inner, exec.clone(), None);
        let wrapped = RecordBatchStreamAdapter::new(cross.schema(), cross);
        tokio::pin!(wrapped);
        while wrapped.next().await.is_some() {}

        assert!(
            done_rx.await.is_ok(),
            "done_rx must fire once the spawned task fully drops"
        );
        exec.join_blocking();
    }

    #[tokio::test]
    async fn done_rx_fires_after_abort() {
        let exec = test_exec();
        let schema = test_schema();
        // Never-ending stream so the task is still live when we abort it.
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::pending::<Result<RecordBatch, DataFusionError>>(),
        ));

        let (cross, abort, done_rx) =
            CrossRtStream::new_with_df_error_stream_cancellable(inner, exec.clone(), None);
        // Hold the stream so the abort, not a drop, is what ends the task.
        let _wrapped = RecordBatchStreamAdapter::new(cross.schema(), cross);

        abort
            .expect("cancellable constructor yields an abort handle")
            .abort();

        let fired = tokio::time::timeout(std::time::Duration::from_secs(5), done_rx).await;
        assert!(fired.is_ok(), "done_rx must fire after the task is aborted");
        exec.join_blocking();
    }

    // Firing the token breaks the loop and fires done_rx without an abort(), so the producer runs
    // its drop+drain cleanup that frees the aggregate's GroupValues.
    #[tokio::test]
    async fn cancellation_token_breaks_loop_and_fires_done_rx() {
        let exec = test_exec();
        let schema = test_schema();
        // Never-ending stream: the only way the task ends is the cooperative cancel.
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::pending::<Result<RecordBatch, DataFusionError>>(),
        ));

        let token = CancellationToken::new();
        let (cross, _abort, done_rx) = CrossRtStream::new_with_df_error_stream_cancellable(
            inner,
            exec.clone(),
            Some(token.clone()),
        );
        // Hold the stream so a consumer-side drop is NOT what ends the task — the token is.
        let _wrapped = RecordBatchStreamAdapter::new(cross.schema(), cross);

        token.cancel();

        let fired = tokio::time::timeout(std::time::Duration::from_secs(5), done_rx).await;
        assert!(
            fired.is_ok(),
            "cancelling the token must break the loop and fire done_rx"
        );
        assert!(
            fired.unwrap().is_ok(),
            "done_rx must complete, not be dropped"
        );
        exec.join_blocking();
    }

    // A token that is never fired must not perturb the normal drain path: the stream completes and
    // all its batches are delivered.
    #[tokio::test]
    async fn uncancelled_token_drains_normally() {
        let exec = test_exec();
        let schema = test_schema();
        let batches = vec![Ok(test_batch(&[1, 2, 3])), Ok(test_batch(&[4, 5]))];
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(batches),
        ));

        let token = CancellationToken::new(); // never cancelled
        let (cross, _abort, done_rx) =
            CrossRtStream::new_with_df_error_stream_cancellable(inner, exec.clone(), Some(token));
        let wrapped = RecordBatchStreamAdapter::new(cross.schema(), cross);
        tokio::pin!(wrapped);

        let mut total_rows = 0;
        while let Some(batch) = wrapped.next().await {
            total_rows += batch.unwrap().num_rows();
        }
        assert_eq!(
            total_rows, 5,
            "all rows delivered when the token is never fired"
        );
        assert!(
            done_rx.await.is_ok(),
            "done_rx fires on normal drain even with a token present"
        );
        exec.join_blocking();
    }

    // Cancelling BEFORE the first poll still terminates cleanly (the biased select checks the token
    // first), exercising the immediate-cancel race.
    #[tokio::test]
    async fn cancel_before_first_poll_terminates() {
        let exec = test_exec();
        let schema = test_schema();
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::pending::<Result<RecordBatch, DataFusionError>>(),
        ));

        let token = CancellationToken::new();
        token.cancel(); // already cancelled before the task runs
        let (cross, _abort, done_rx) =
            CrossRtStream::new_with_df_error_stream_cancellable(inner, exec.clone(), Some(token));
        let _wrapped = RecordBatchStreamAdapter::new(cross.schema(), cross);

        let fired = tokio::time::timeout(std::time::Duration::from_secs(5), done_rx).await;
        assert!(
            fired.is_ok(),
            "a pre-cancelled token must still terminate the task"
        );
        exec.join_blocking();
    }
}
