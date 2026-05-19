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
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::AbortHandle;
use tokio_stream::wrappers::ReceiverStream;

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
}

impl CrossRtStream {
    fn new_with_tx<F, Fut>(f: F, schema: SchemaRef) -> Self
    where
        F: FnOnce(Sender<Result<RecordBatch, DataFusionError>>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (tx, rx) = channel(1);
        let driver = f(tx).boxed();
        Self {
            driver,
            driver_ready: false,
            inner: ReceiverStream::new(rx),
            inner_done: false,
            schema,
        }
    }

    /// Creates a CrossRtStream that runs the DataFusion stream on the CPU executor.
    pub fn new_with_df_error_stream(
        stream: SendableRecordBatchStream,
        exec: DedicatedExecutor,
    ) -> Self {
        let (cross_rt, _abort_handle) = Self::new_with_df_error_stream_cancellable(stream, exec);
        cross_rt
    }

    /// Like [`new_with_df_error_stream`](Self::new_with_df_error_stream), but also returns
    /// an [`AbortHandle`] that can be used to cancel the CPU task externally.
    pub fn new_with_df_error_stream_cancellable(
        stream: SendableRecordBatchStream,
        exec: DedicatedExecutor,
    ) -> (Self, Option<AbortHandle>) {
        let schema = stream.schema();
        let (tx, rx) = channel(1);
        let tx_captured = tx.clone();

        let fut = async move {
            tokio::pin!(stream);
            while let Some(res) = stream.next().await {
                if tx_captured.send(res).await.is_err() {
                    return;
                }
            }
        };

        let (abort_handle, join_fut) = exec.spawn_with_abort_handle(fut);

        let driver = async move {
            if let Err(e) = join_fut.await {
                let err = match e {
                    JobError::Panic { msg } => {
                        DataFusionError::Execution(format!("Panic: {}", msg))
                    }
                    JobError::WorkerGone => {
                        DataFusionError::Execution("Worker gone".to_string())
                    }
                };
                tx.send(Err(err)).await.ok();
            }
        }.boxed();

        let cross_rt = Self {
            driver,
            driver_ready: false,
            inner: ReceiverStream::new(rx),
            inner_done: false,
            schema,
        };

        (cross_rt, abort_handle)
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
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
        DedicatedExecutor::new("test-cpu", builder)
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
}
