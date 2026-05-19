/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * 8 fields from {@code tokio_metrics::RuntimeMonitor} describing
 * per-worker thread pool behavior for a single Tokio runtime.
 */
public class RuntimeMetrics implements Writeable {
    /** Number of worker threads in the runtime. */
    public final long workersCount;
    /** Total number of task polls across all workers. */
    public final long totalPollsCount;
    /** Total time workers spent executing tasks, in milliseconds. */
    public final long totalBusyDurationMs;
    /** Total number of times tasks were pushed to the overflow queue. */
    public final long totalOverflowCount;
    /** Current depth of the global injection queue. */
    public final long globalQueueDepth;
    /** Current depth of the blocking thread pool queue. */
    public final long blockingQueueDepth;
    /** Number of tasks currently alive (spawned but not yet completed) on this runtime. */
    public final long numAliveTasks;
    /** Total number of tasks spawned on this runtime since creation. */
    public final long spawnedTasksCount;
    /** Sum of all per-worker local queue depths (tasks queued on worker-local run queues). */
    public final long totalLocalQueueDepth;

    /**
     * Construct from explicit field values.
     *
     * @param workersCount        number of worker threads
     * @param totalPollsCount     total task polls across all workers
     * @param totalBusyDurationMs total busy time in milliseconds
     * @param totalOverflowCount  total overflow queue pushes
     * @param globalQueueDepth    current global injection queue depth
     * @param blockingQueueDepth  current blocking thread pool queue depth
     * @param numAliveTasks       tasks currently alive
     * @param spawnedTasksCount   total tasks spawned since creation
     * @param totalLocalQueueDepth     sum of per-worker local queue depths
     */
    public RuntimeMetrics(
        long workersCount,
        long totalPollsCount,
        long totalBusyDurationMs,
        long totalOverflowCount,
        long globalQueueDepth,
        long blockingQueueDepth,
        long numAliveTasks,
        long spawnedTasksCount,
        long totalLocalQueueDepth
    ) {
        this.workersCount = workersCount;
        this.totalPollsCount = totalPollsCount;
        this.totalBusyDurationMs = totalBusyDurationMs;
        this.totalOverflowCount = totalOverflowCount;
        this.globalQueueDepth = globalQueueDepth;
        this.blockingQueueDepth = blockingQueueDepth;
        this.numAliveTasks = numAliveTasks;
        this.spawnedTasksCount = spawnedTasksCount;
        this.totalLocalQueueDepth = totalLocalQueueDepth;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public RuntimeMetrics(StreamInput in) throws IOException {
        this.workersCount = in.readVLong();
        this.totalPollsCount = in.readVLong();
        this.totalBusyDurationMs = in.readVLong();
        this.totalOverflowCount = in.readVLong();
        this.globalQueueDepth = in.readVLong();
        this.blockingQueueDepth = in.readVLong();
        this.numAliveTasks = in.readVLong();
        this.spawnedTasksCount = in.readVLong();
        this.totalLocalQueueDepth = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(workersCount);
        out.writeVLong(totalPollsCount);
        out.writeVLong(totalBusyDurationMs);
        out.writeVLong(totalOverflowCount);
        out.writeVLong(globalQueueDepth);
        out.writeVLong(blockingQueueDepth);
        out.writeVLong(numAliveTasks);
        out.writeVLong(spawnedTasksCount);
        out.writeVLong(totalLocalQueueDepth);
    }

    /**
     * Render all 8 fields as snake_case JSON fields.
     *
     * @param builder the XContent builder to write to
     * @throws IOException if writing fails
     */
    public void toXContent(XContentBuilder builder) throws IOException {
        builder.field("workers_count", workersCount);
        builder.field("total_polls_count", totalPollsCount);
        builder.field("total_busy_duration_ms", totalBusyDurationMs);
        builder.field("total_overflow_count", totalOverflowCount);
        builder.field("global_queue_depth", globalQueueDepth);
        builder.field("blocking_queue_depth", blockingQueueDepth);
        builder.field("num_alive_tasks", numAliveTasks);
        builder.field("spawned_tasks_count", spawnedTasksCount);
        builder.field("total_local_queue_depth", totalLocalQueueDepth);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RuntimeMetrics that = (RuntimeMetrics) o;
        return workersCount == that.workersCount
            && totalPollsCount == that.totalPollsCount
            && totalBusyDurationMs == that.totalBusyDurationMs
            && totalOverflowCount == that.totalOverflowCount
            && globalQueueDepth == that.globalQueueDepth
            && blockingQueueDepth == that.blockingQueueDepth
            && numAliveTasks == that.numAliveTasks
            && spawnedTasksCount == that.spawnedTasksCount
            && totalLocalQueueDepth == that.totalLocalQueueDepth;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            workersCount,
            totalPollsCount,
            totalBusyDurationMs,
            totalOverflowCount,
            globalQueueDepth,
            blockingQueueDepth,
            numAliveTasks,
            spawnedTasksCount,
            totalLocalQueueDepth
        );
    }
}
