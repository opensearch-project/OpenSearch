/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.tasks.Task;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TaskResourceUsageTracker is used to track completions and cancellations of search related tasks.
 *
 * @opensearch.internal
 */
public abstract class TaskResourceUsageTracker {
    /**
     * Counts the number of cancellations made due to this tracker.
     */
    private final AtomicLong searchTaskCancellationCount = new AtomicLong();
    private final AtomicLong searchShardTaskCancellationCount = new AtomicLong();

    public long incrementSearchTaskCancellations() {
        return searchTaskCancellationCount.incrementAndGet();
    }

    public long incrementSearchShardTaskCancellations() {
        return searchShardTaskCancellationCount.incrementAndGet();
    }

    public long getSearchTaskCancellationCount() {
        return searchTaskCancellationCount.get();
    }

    public long getSearchShardTaskCancellationCount() {
        return searchShardTaskCancellationCount.get();
    }

    /**
     * Returns a unique name for this tracker.
     */
    public abstract String name();

    /**
     * Notifies the tracker to update its state when a task execution completes.
     */
    public void update(Task task) {}

    /**
     * Returns the cancellation reason for the given task, if it's eligible for cancellation.
     */
    public abstract Optional<TaskCancellation.Reason> checkAndMaybeGetCancellationReason(Task task);

    /**
     * Returns the tracker's state for SearchTasks as seen in the stats API.
     */
    public abstract Stats searchTaskStats(List<? extends Task> activeTasks);

    /**
     * Returns the tracker's state for SearchShardTasks as seen in the stats API.
     */
    public abstract Stats searchShardTaskStats(List<? extends Task> activeTasks);

    /**
     * Represents the tracker's state as seen in the stats API.
     */
    public interface Stats extends ToXContentObject, Writeable {}
}
