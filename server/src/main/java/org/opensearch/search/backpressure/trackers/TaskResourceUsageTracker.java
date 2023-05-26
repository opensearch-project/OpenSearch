/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
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
    private final AtomicLong cancellations = new AtomicLong();

    public long incrementCancellations() {
        return cancellations.incrementAndGet();
    }

    public long getCancellations() {
        return cancellations.get();
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
     * Returns the tracker's state for tasks as seen in the stats API.
     */
    public abstract Stats stats(List<? extends Task> activeTasks);

    /**
     * Represents the tracker's state as seen in the stats API.
     */
    public interface Stats extends ToXContentObject, Writeable {}
}
