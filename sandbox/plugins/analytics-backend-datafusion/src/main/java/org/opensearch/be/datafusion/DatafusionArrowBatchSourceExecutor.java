/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed under
 * the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.exec.FragmentResources;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.analytics.spi.ArrowBatchSourceExecutor;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory;
import org.opensearch.analytics.spi.ArrowBatchSourcePlan;
import org.opensearch.analytics.spi.DelegationThreadTracker;
import org.opensearch.be.datafusion.arrow.ArrowBatchSourceCallbacks;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** DataFusion implementation of the generic Arrow pull-source execution SPI. */
final class DatafusionArrowBatchSourceExecutor implements ArrowBatchSourceExecutor {

    private final DataFusionService service;

    DatafusionArrowBatchSourceExecutor(DataFusionService service) {
        this.service = Objects.requireNonNull(service, "service");
    }

    @Override
    public EngineResultStream execute(
        BufferAllocator resultAllocator,
        ArrowBatchSourcePlan plan,
        ArrowBatchSourceFactory sourceFactory,
        Task task,
        DelegationThreadTracker threadTracker
    ) {
        Objects.requireNonNull(resultAllocator, "resultAllocator");
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(sourceFactory, "sourceFactory");
        if (task instanceof CancellableTask cancellableTask && cancellableTask.isCancelled()) {
            sourceFactory.close();
            throw new TaskCancelledException("Arrow batch source execution cancelled before setup");
        }

        ArrowBatchSourceCallbacks.Registration registration;
        try {
            registration = ArrowBatchSourceCallbacks.register(sourceFactory, threadTracker);
        } catch (RuntimeException | Error throwable) {
            closeAfterFailure(throwable, sourceFactory::close);
            throw throwable;
        }

        DatafusionLocalSession session = null;
        StreamHandle output = null;
        AnalyticsShardTask shardTask = task instanceof AnalyticsShardTask analyticsShardTask ? analyticsShardTask : null;
        long taskId = task == null ? 0L : task.getId();
        try {
            session = new DatafusionLocalSession(service.getNativeRuntime().get());
            NativeBridge.registerArrowBatchSourceProvider(
                session.getPointer(),
                plan.inputId(),
                plan.planBytes(),
                registration.bindingId(),
                taskId
            );
            if (shardTask != null) {
                shardTask.setCancellationListener(() -> NativeBridge.cancelQuery(taskId));
            }
            long streamPointer = NativeBridge.executeLocalPlan(session.getPointer(), plan.planBytes(), taskId);
            output = new StreamHandle(streamPointer, service.getNativeRuntime());
            DatafusionResultStream delegate = new DatafusionResultStream(output, resultAllocator);
            return new OwnedResultStream(delegate, session, registration, shardTask, taskId);
        } catch (RuntimeException | Error throwable) {
            if (shardTask != null) {
                shardTask.clearCancellationListener();
            }
            if (taskId != 0L) {
                closeAfterFailure(throwable, () -> NativeBridge.cancelQuery(taskId));
            }
            if (output != null) {
                closeAfterFailure(throwable, output::close);
            }
            if (session != null) {
                closeAfterFailure(throwable, session::close);
            }
            closeAfterFailure(throwable, registration::close);
            throw throwable;
        }
    }

    private static void closeAfterFailure(Throwable failure, Runnable closeAction) {
        try {
            closeAction.run();
        } catch (RuntimeException | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    /** Owns the native output, local session, callback binding, and source factory. */
    private static final class OwnedResultStream implements EngineResultStream, FragmentResources.MetricsCapable {
        private final DatafusionResultStream delegate;
        private final DatafusionLocalSession session;
        private final ArrowBatchSourceCallbacks.Registration registration;
        private final AnalyticsShardTask shardTask;
        private final long taskId;
        private final AtomicBoolean closed = new AtomicBoolean();

        private OwnedResultStream(
            DatafusionResultStream delegate,
            DatafusionLocalSession session,
            ArrowBatchSourceCallbacks.Registration registration,
            AnalyticsShardTask shardTask,
            long taskId
        ) {
            this.delegate = delegate;
            this.session = session;
            this.registration = registration;
            this.shardTask = shardTask;
            this.taskId = taskId;
        }

        @Override
        public Iterator<EngineResultBatch> iterator() {
            return delegate.iterator();
        }

        @Override
        public byte[] getMetricsJson() {
            return delegate.getMetricsJson();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true) == false) {
                return;
            }
            if (shardTask != null) {
                shardTask.clearCancellationListener();
            }
            Throwable failure = null;
            if (taskId != 0L) {
                try {
                    NativeBridge.cancelQuery(taskId);
                } catch (RuntimeException | Error throwable) {
                    failure = throwable;
                }
            }
            failure = close(delegate::close, failure);
            failure = close(session::close, failure);
            failure = close(registration::close, failure);
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
        }

        private static Throwable close(Runnable closeAction, Throwable failure) {
            try {
                closeAction.run();
            } catch (RuntimeException | Error closeFailure) {
                if (failure == null) {
                    return closeFailure;
                }
                failure.addSuppressed(closeFailure);
            }
            return failure;
        }
    }
}
