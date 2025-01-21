/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.custom;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamReader;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.arrow.spi.StreamTicketFactory;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskAwareRequest;
import org.opensearch.tasks.TaskManager;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Wraps a StreamManager to make it work with the TaskManager.
 */
public class StreamManagerWrapper implements StreamManager {

    private final Supplier<StreamManager> streamManager;
    private final TaskManager taskManager;

    public StreamManagerWrapper(Supplier<StreamManager> streamManager, TaskManager taskManager) {
        super();
        this.streamManager = streamManager;
        this.taskManager = taskManager;
    }

    @Override
    public StreamTicket registerStream(StreamProducer producer, TaskId parentTaskId) {
        StreamProducerTaskWrapper wrappedProducer = new StreamProducerTaskWrapper(producer, taskManager, parentTaskId);
        StreamTicket ticket = streamManager.get().registerStream(wrappedProducer, parentTaskId);
        wrappedProducer.setDescription(ticket.toString());
        return ticket;
    }

    @Override
    public StreamReader getStreamReader(StreamTicket ticket) {
        return streamManager.get().getStreamReader(ticket);
    }

    @Override
    public StreamTicketFactory getStreamTicketFactory() {
        return streamManager.get().getStreamTicketFactory();
    }

    @Override
    public void close() throws Exception {
        streamManager.get().close();
    }

    static class StreamProducerTaskWrapper implements StreamProducer {

        private final StreamProducer streamProducer;
        private final TaskManager taskManager;
        private final TaskId parentTaskId;
        private String description = "";

        public StreamProducerTaskWrapper(StreamProducer streamProducer, TaskManager taskManager, TaskId parentTaskId) {
            this.streamProducer = streamProducer;
            this.taskManager = taskManager;
            this.parentTaskId = parentTaskId;
        }

        void setDescription(String description) {
            this.description = description;
        }

        @Override
        public String getAction() {
            return streamProducer.getAction();
        }

        @Override
        public VectorSchemaRoot createRoot(BufferAllocator allocator) {
            return streamProducer.createRoot(allocator);
        }

        @Override
        public BatchedJob createJob(BufferAllocator allocator) {
            BatchedJob job = streamProducer.createJob(allocator);
            return new BatchedJobTaskWrapper(job, parentTaskId, taskManager, description, getAction());
        }

        @Override
        public int estimatedRowCount() {
            return streamProducer.estimatedRowCount();
        }

        @Override
        public void close() throws IOException {
            streamProducer.close();
        }

        static class BatchedJobTaskWrapper implements BatchedJob, TaskAwareRequest {
            private final BatchedJob batchedJob;
            private final TaskManager taskManager;
            private TaskId parentTaskId;
            private final String description;
            private final String action;

            public BatchedJobTaskWrapper(
                BatchedJob batchedJob,
                TaskId parentTaskId,
                TaskManager taskManager,
                String description,
                String action
            ) {
                this.batchedJob = batchedJob;
                this.taskManager = taskManager;
                this.parentTaskId = parentTaskId;
                this.description = description;
                this.action = action;
            }

            @Override
            public void run(VectorSchemaRoot root, FlushSignal flushSignal) {
                final Task task;
                final Releasable unregisterChildNode = registerChildNode();
                try {
                    task = taskManager.register("stream_task", action, this);
                } catch (TaskCancelledException e) {
                    unregisterChildNode.close();
                    throw e;
                }

                try (ThreadContext.StoredContext ignored = taskManager.taskExecutionStarted(task)) {
                    batchedJob.run(root, flushSignal);
                } finally {
                    Releasables.close(unregisterChildNode, () -> taskManager.unregister(task));
                }
            }

            @Override
            public void onCancel() {
                batchedJob.onCancel();
            }

            @Override
            public boolean isCancelled() {
                return batchedJob.isCancelled();
            }

            @Override
            public void setParentTask(TaskId taskId) {
                this.parentTaskId = taskId;
            }

            @Override
            public TaskId getParentTask() {
                return parentTaskId;
            }

            @Override
            public String getDescription() {
                return description;
            }

            private Releasable registerChildNode() {
                if (parentTaskId.isSet()) {
                    return taskManager.registerChildNode(parentTaskId.getId(), taskManager.localNode());
                } else {
                    return () -> {};
                }
            }
        }
    }
}
