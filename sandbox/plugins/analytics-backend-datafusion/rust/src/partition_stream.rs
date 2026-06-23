/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Streaming input for coordinator-reduce execution.
//!
//! A [`channel`] produces a paired [`PartitionStreamSender`] /
//! [`PartitionStreamReceiver`]. The sender is exposed through the FFM bridge so
//! Java can push Arrow `RecordBatch`es via [`PartitionStreamSender::try_send`]
//! (fast path) and, when the channel is full, by cloning the inner
//! [`PartitionStreamSender::clone_tx`] into a spawned task that completes via
//! the async-completion upcall. The receiver implements DataFusion's
//! [`RecordBatchStream`] and is wrapped in a [`SingleReceiverPartition`] so it
//! can be registered on a `SessionContext` as a `StreamingTable`.
//!
//! # Backpressure
//!
//! The underlying mpsc is bounded (capacity 4) — chosen small so the sender
//! back-pressures when the DataFusion execute side falls behind. Under load a
//! full channel makes [`PartitionStreamSender::try_send`] return `Full`; the
//! FFM bridge then parks the send on the IO runtime (Java parks on a virtual
//! thread), which naturally stalls the shard response pipeline.
//!
//! # Single-consumer contract
//!
//! [`SingleReceiverPartition::execute`] hands out the receiver exactly once. Any
//! subsequent `execute()` call on the same partition returns an already-closed
//! empty stream rather than panicking — matches the "take once, then empty"
//! contract expected by DataFusion's `StreamingTable` when a partition is
//! re-executed.

use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{stream, Stream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

/// Bounded channel capacity. Small by design — producers back-pressure when the
/// DataFusion execute side falls behind.
const CHANNEL_CAPACITY: usize = 4;

/// Producer side of a partition stream.
///
/// Owned by the FFM bridge via `Box::into_raw`; dropping the sender (e.g. via
/// `df_sender_close`) closes the channel, which signals EOF to the DataFusion
/// receiver side.
pub struct PartitionStreamSender {
    tx: mpsc::Sender<Result<RecordBatch, DataFusionError>>,
    schema: SchemaRef,
}

impl PartitionStreamSender {
    /// Returns the schema this sender was created with.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Non-blocking push. Returns `Ok(())` on the fast path, or a
    /// [`TrySendError`] when the channel is full (`Full`, carrying the batch
    /// back so the caller can retry it on a spawned task) or the receiver has
    /// been dropped (`Closed`).
    pub fn try_send(
        &self,
        batch: Result<RecordBatch, DataFusionError>,
    ) -> Result<(), TrySendError<Result<RecordBatch, DataFusionError>>> {
        self.tx.try_send(batch)
    }

    /// Clones the inner `mpsc::Sender` (two `Arc` bumps). The clone is moved
    /// into a spawned task to complete a full-channel send across an `await`
    /// without borrowing the boxed [`PartitionStreamSender`] — which is what
    /// makes [`crate::api::sender_close`] safe against in-flight sends.
    ///
    /// # EOF semantics
    /// The channel signals EOF only when **all** `Sender` clones drop. A pending
    /// in-flight send holds a clone, so closing the sender (dropping the boxed
    /// `PartitionStreamSender`) defers EOF until the queued batch is delivered —
    /// EOF is therefore ordered strictly after every accepted batch.
    pub fn clone_tx(&self) -> mpsc::Sender<Result<RecordBatch, DataFusionError>> {
        self.tx.clone()
    }
}

impl fmt::Debug for PartitionStreamSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionStreamSender")
            .field("schema", &self.schema)
            .finish()
    }
}

/// Consumer side of a partition stream.
///
/// Implements [`Stream`] + [`RecordBatchStream`] so DataFusion can poll it
/// directly. Typically handed to [`SingleReceiverPartition`] and registered on
/// a `SessionContext` as a `StreamingTable`.
pub struct PartitionStreamReceiver {
    rx: mpsc::Receiver<Result<RecordBatch, DataFusionError>>,
    schema: SchemaRef,
}

impl fmt::Debug for PartitionStreamReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionStreamReceiver")
            .field("schema", &self.schema)
            .finish()
    }
}

impl Stream for PartitionStreamReceiver {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl RecordBatchStream for PartitionStreamReceiver {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Creates a paired sender/receiver over a bounded mpsc (capacity
/// [`CHANNEL_CAPACITY`]).
///
/// Both halves share the provided [`SchemaRef`]. Dropping the sender closes the
/// channel — the receiver's `poll_next` then yields `Ready(None)` once any
/// buffered batches are drained, which DataFusion interprets as end-of-input.
pub fn channel(schema: SchemaRef) -> (PartitionStreamSender, PartitionStreamReceiver) {
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let sender = PartitionStreamSender {
        tx,
        schema: Arc::clone(&schema),
    };
    let receiver = PartitionStreamReceiver { rx, schema };
    (sender, receiver)
}

/// Wraps a [`PartitionStreamReceiver`] so it can be registered as a DataFusion
/// `StreamingTable` partition.
///
/// DataFusion's [`PartitionStream::execute`] contract may invoke `execute` more
/// than once across the life of a plan. The receiver can only be consumed once,
/// so the first `execute` takes it and subsequent calls return an empty stream
/// (zero batches, end-of-stream immediately) rather than panicking.
pub(crate) struct SingleReceiverPartition {
    schema: SchemaRef,
    receiver: Mutex<Option<PartitionStreamReceiver>>,
}

impl SingleReceiverPartition {
    pub(crate) fn new(receiver: PartitionStreamReceiver) -> Self {
        Self {
            schema: Arc::clone(&receiver.schema),
            receiver: Mutex::new(Some(receiver)),
        }
    }
}

impl fmt::Debug for SingleReceiverPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SingleReceiverPartition")
            .field("schema", &self.schema)
            .finish()
    }
}

impl PartitionStream for SingleReceiverPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let taken = self
            .receiver
            .lock()
            .expect("partition mutex poisoned")
            .take();
        match taken {
            Some(receiver) => Box::pin(receiver),
            None => {
                // Second+ execute: hand back an already-closed empty stream so
                // DataFusion sees the partition as drained.
                Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&self.schema),
                    stream::empty(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]))
    }

    fn test_batch(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn channel_preserves_schema() {
        let schema = test_schema();
        let (sender, receiver) = channel(Arc::clone(&schema));
        assert_eq!(sender.schema(), &schema);
        assert_eq!(RecordBatchStream::schema(&receiver), schema);
    }

    #[tokio::test]
    async fn receiver_yields_sent_batches_then_eof() {
        let schema = test_schema();
        let (sender, mut receiver) = channel(Arc::clone(&schema));

        let producer_schema = Arc::clone(&schema);
        let producer = tokio::spawn(async move {
            sender
                .tx
                .send(Ok(test_batch(&producer_schema, &[1, 2])))
                .await
                .unwrap();
            sender
                .tx
                .send(Ok(test_batch(&producer_schema, &[3])))
                .await
                .unwrap();
            drop(sender);
        });

        let first = receiver.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 2);
        let second = receiver.next().await.unwrap().unwrap();
        assert_eq!(second.num_rows(), 1);
        assert!(receiver.next().await.is_none());
        producer.await.unwrap();
    }

    #[tokio::test]
    async fn try_send_fast_path_then_eof() {
        let schema = test_schema();
        let (sender, mut receiver) = channel(Arc::clone(&schema));

        // Capacity is 4; a single batch fits the fast path without blocking.
        sender.try_send(Ok(test_batch(&schema, &[7, 8, 9]))).expect("fast-path send");
        drop(sender);

        let batch = receiver.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert!(receiver.next().await.is_none());
    }

    #[tokio::test]
    async fn try_send_full_then_clone_tx_drains() {
        let schema = test_schema();
        let (sender, mut receiver) = channel(Arc::clone(&schema));

        // Fill the channel to capacity (4) on the fast path.
        for _ in 0..CHANNEL_CAPACITY {
            sender.try_send(Ok(test_batch(&schema, &[1]))).expect("within capacity");
        }
        // The next try_send must report Full and hand the batch back.
        let overflow = test_batch(&schema, &[2]);
        let tx = match sender.try_send(Ok(overflow)) {
            Err(TrySendError::Full(batch)) => {
                // Mirror the FFM bridge: clone the inner Sender and complete the
                // send on a task once capacity frees up.
                let tx = sender.clone_tx();
                tokio::spawn(async move {
                    tx.send(batch).await.expect("receiver still live");
                });
                sender.clone_tx()
            }
            other => panic!("expected Full, got {:?}", other.is_ok()),
        };
        drop(tx);
        drop(sender);

        // Drain all five batches (4 fast-path + 1 deferred), then EOF.
        let mut count = 0;
        while receiver.next().await.is_some() {
            count += 1;
        }
        assert_eq!(count, CHANNEL_CAPACITY + 1);
    }

    #[tokio::test]
    async fn try_send_reports_receiver_dropped() {
        let schema = test_schema();
        let (sender, receiver) = channel(Arc::clone(&schema));
        drop(receiver);

        match sender.try_send(Ok(test_batch(&schema, &[1]))) {
            Err(TrySendError::Closed(_)) => {}
            other => panic!("expected Closed, got is_ok={}", other.is_ok()),
        }
    }

    #[tokio::test]
    async fn single_receiver_partition_executes_once_then_empty() {
        let schema = test_schema();
        let (sender, receiver) = channel(Arc::clone(&schema));
        let partition = SingleReceiverPartition::new(receiver);
        assert_eq!(partition.schema(), &schema);

        let producer_schema = Arc::clone(&schema);
        let producer = tokio::spawn(async move {
            sender
                .tx
                .send(Ok(test_batch(&producer_schema, &[42])))
                .await
                .unwrap();
            drop(sender);
        });

        let ctx = Arc::new(TaskContext::default());
        let mut first = partition.execute(Arc::clone(&ctx));
        let batch = first.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(first.next().await.is_none());
        producer.await.unwrap();

        // Second execute() must not panic and must yield an empty stream.
        let mut second = partition.execute(ctx);
        assert!(second.next().await.is_none());
    }
}
