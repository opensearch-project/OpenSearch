/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Future, Stream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::callbacks::ArrowBatchSourceHandle;

/// DataFusion table backed by externally allocated Arrow batches.
///
/// The Java owner must use a breaker-accounted allocator. Imported buffers remain charged
/// to that allocator for their lifetime and do not use coordinator reduce-budget admission.
#[derive(Debug)]
pub struct ArrowBatchTableProvider {
    schema: SchemaRef,
    binding_id: i64,
    task_id: i64,
}

impl ArrowBatchTableProvider {
    pub fn new(schema: SchemaRef, binding_id: i64, task_id: i64) -> Self {
        Self {
            schema,
            binding_id,
            task_id,
        }
    }
}

#[async_trait]
impl TableProvider for ArrowBatchTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let projected_schema = Arc::new(self.schema.project(&projection)?);
        Ok(Arc::new(ArrowBatchSourceExec::new(
            projected_schema,
            projection,
            self.binding_id,
            crate::query_tracker::get_cancellation_token(self.task_id),
        )))
    }
}

pub struct ArrowBatchSourceExec {
    schema: SchemaRef,
    projection: Vec<usize>,
    binding_id: i64,
    cancellation_token: Option<CancellationToken>,
    properties: Arc<PlanProperties>,
}

impl ArrowBatchSourceExec {
    fn new(
        schema: SchemaRef,
        projection: Vec<usize>,
        binding_id: i64,
        cancellation_token: Option<CancellationToken>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            schema,
            projection,
            binding_id,
            cancellation_token,
            properties,
        }
    }
}

impl fmt::Debug for ArrowBatchSourceExec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArrowBatchSourceExec")
            .field("projection", &self.projection)
            .field("binding_id", &self.binding_id)
            .finish()
    }
}

impl DisplayAs for ArrowBatchSourceExec {
    fn fmt_as(
        &self,
        _display_type: DisplayFormatType,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            formatter,
            "ArrowBatchSourceExec: projection={:?}",
            self.projection
        )
    }
}

impl ExecutionPlan for ArrowBatchSourceExec {
    fn name(&self) -> &str {
        "ArrowBatchSourceExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "ArrowBatchSourceExec partition {partition} out of range"
            )));
        }
        let source = Arc::new(ArrowBatchSourceHandle::create(
            self.binding_id,
            &self.projection,
        )?);
        Ok(Box::pin(ArrowBatchRecordBatchStream::new(
            Arc::clone(&self.schema),
            source,
            self.cancellation_token.clone(),
        )))
    }
}

pub(super) struct ArrowBatchRecordBatchStream {
    schema: SchemaRef,
    source: Arc<ArrowBatchSourceHandle>,
    cancellation: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    cancellation_requested: bool,
    pending: Option<JoinHandle<Result<Option<RecordBatch>>>>,
    finished: bool,
}

impl ArrowBatchRecordBatchStream {
    pub(super) fn new(
        schema: SchemaRef,
        source: Arc<ArrowBatchSourceHandle>,
        cancellation_token: Option<CancellationToken>,
    ) -> Self {
        let cancellation = cancellation_token.map(|token| {
            Box::pin(async move { token.cancelled().await })
                as Pin<Box<dyn Future<Output = ()> + Send>>
        });
        Self {
            schema,
            source,
            cancellation,
            cancellation_requested: false,
            pending: None,
            finished: false,
        }
    }

    fn poll_cancellation(&mut self, context: &mut Context<'_>) {
        if self.cancellation_requested {
            return;
        }
        if let Some(cancellation) = self.cancellation.as_mut() {
            if cancellation.as_mut().poll(context).is_ready() {
                self.cancellation_requested = true;
                self.source.cancel();
            }
        }
    }

    fn cancelled(&mut self) -> Poll<Option<Result<RecordBatch>>> {
        self.finished = true;
        Poll::Ready(Some(Err(DataFusionError::Execution(
            "query cancelled".into(),
        ))))
    }
}

impl fmt::Debug for ArrowBatchRecordBatchStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArrowBatchRecordBatchStream")
            .field("finished", &self.finished)
            .finish()
    }
}

impl Stream for ArrowBatchRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        self.poll_cancellation(context);
        if self.cancellation_requested && self.pending.is_none() {
            return self.cancelled();
        }
        if self.pending.is_none() {
            let source = Arc::clone(&self.source);
            self.pending = Some(tokio::task::spawn_blocking(move || source.next_batch()));
        }
        let handle = self.pending.as_mut().expect("pending source request");
        match Pin::new(handle).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.pending = None;
                if self.cancellation_requested {
                    return self.cancelled();
                }
                match result {
                    Err(error) => {
                        self.finished = true;
                        Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                            "Arrow batch source task failed: {error}"
                        )))))
                    }
                    Ok(Err(error)) => {
                        self.finished = true;
                        Poll::Ready(Some(Err(error)))
                    }
                    Ok(Ok(None)) => {
                        self.finished = true;
                        Poll::Ready(None)
                    }
                    Ok(Ok(Some(batch))) => {
                        if batch.schema().fields() != self.schema.fields() {
                            self.finished = true;
                            return Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                                "Arrow batch schema mismatch: expected {:?}, got {:?}",
                                self.schema,
                                batch.schema()
                            )))));
                        }
                        Poll::Ready(Some(Ok(batch)))
                    }
                }
            }
        }
    }
}

impl Drop for ArrowBatchRecordBatchStream {
    fn drop(&mut self) {
        // Cancellation is cooperative. Sources that keep the default no-op cancel method
        // remain retained by an active blocking task until that pull returns safely.
        self.source.cancel();
        if let Some(handle) = self.pending.take() {
            handle.abort();
        }
    }
}

impl RecordBatchStream for ArrowBatchRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
