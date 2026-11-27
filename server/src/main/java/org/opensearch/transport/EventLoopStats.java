/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Queue depth of the Netty event-loop workers that read, write, and dispatch network messages.
 * <p>
 * {@code pending_tasks} is the number of tasks waiting to run. It does not include the task currently executing on a
 * worker. A non-zero value means work is waiting behind a busy or blocked network thread.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.10.0")
public class EventLoopStats implements Writeable, ToXContentFragment {

    private final boolean sharedGroup;
    private final List<Worker> workers;

    public EventLoopStats(boolean sharedGroup, List<Worker> workers) {
        this.sharedGroup = sharedGroup;
        this.workers = List.copyOf(Objects.requireNonNull(workers, "workers"));
    }

    public EventLoopStats(StreamInput in) throws IOException {
        sharedGroup = in.readBoolean();
        workers = List.copyOf(in.readList(Worker::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(sharedGroup);
        out.writeList(workers);
    }

    /**
     * @return true when HTTP and the node-to-node transport are configured to use this same event-loop group.
     *         This is the default ({@code http.netty.worker_count} of {@code 0}). When true, the same workers are
     *         reported under both {@code transport.event_loop} and {@code http.event_loop} and must not be added
     *         together.
     */
    public boolean isSharedGroup() {
        return sharedGroup;
    }

    public List<Worker> getWorkers() {
        return workers;
    }

    public int getWorkerCount() {
        return workers.size();
    }

    /**
     * @return the sum of tasks waiting on every worker in this group
     */
    public long getPendingTasks() {
        long pendingTasks = 0;
        for (Worker worker : workers) {
            pendingTasks += worker.getPendingTasks();
        }
        return pendingTasks;
    }

    /**
     * @return the largest per-worker queue. An average can hide one saturated worker.
     */
    public int getMaxPendingTasks() {
        int maxPendingTasks = 0;
        for (Worker worker : workers) {
            maxPendingTasks = Math.max(maxPendingTasks, worker.getPendingTasks());
        }
        return maxPendingTasks;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.EVENT_LOOP);
        builder.field(Fields.SHARED_GROUP, sharedGroup);
        builder.field(Fields.WORKER_COUNT, getWorkerCount());
        builder.field(Fields.PENDING_TASKS, getPendingTasks());
        builder.field(Fields.MAX_PENDING_TASKS, getMaxPendingTasks());
        builder.startArray(Fields.WORKERS);
        for (Worker worker : workers) {
            worker.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EventLoopStats that = (EventLoopStats) o;
        return sharedGroup == that.sharedGroup && workers.equals(that.workers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sharedGroup, workers);
    }

    @Override
    public String toString() {
        return "EventLoopStats{sharedGroup=" + sharedGroup + ", workers=" + workers + "}";
    }

    /**
     * Pending-task count for one Netty event-loop worker.
     *
     * @opensearch.api
     */
    @PublicApi(since = "3.10.0")
    public static class Worker implements Writeable, ToXContentFragment {

        private final String name;
        private final int pendingTasks;

        public Worker(String name, int pendingTasks) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("worker name is required");
            }
            if (pendingTasks < 0) {
                throw new IllegalArgumentException("pending tasks must be non-negative but was [" + pendingTasks + "]");
            }
            this.name = name;
            this.pendingTasks = pendingTasks;
        }

        public Worker(StreamInput in) throws IOException {
            this(in.readString(), in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVInt(pendingTasks);
        }

        public String getName() {
            return name;
        }

        public int getPendingTasks() {
            return pendingTasks;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.NAME, name);
            builder.field(Fields.PENDING_TASKS, pendingTasks);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Worker worker = (Worker) o;
            return pendingTasks == worker.pendingTasks && name.equals(worker.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, pendingTasks);
        }

        @Override
        public String toString() {
            return "Worker{name='" + name + "', pendingTasks=" + pendingTasks + "}";
        }
    }

    static final class Fields {
        static final String EVENT_LOOP = "event_loop";
        static final String SHARED_GROUP = "shared_group";
        static final String WORKER_COUNT = "worker_count";
        static final String PENDING_TASKS = "pending_tasks";
        static final String MAX_PENDING_TASKS = "max_pending_tasks";
        static final String WORKERS = "workers";
        static final String NAME = "name";
    }
}
