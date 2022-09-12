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
import org.opensearch.search.backpressure.TaskCancellation;
import org.opensearch.tasks.Task;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ResourceUsageTracker is used to track completions and cancellations of search related tasks.
 */
public abstract class ResourceUsageTracker {
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
    public abstract void update(Task task);

    /**
     * Returns the cancellation reason for the given task, if it's eligible for cancellation.
     */
    public abstract Optional<TaskCancellation.Reason> cancellationReason(Task task);

    /**
     * Returns the current state of the tracker as seen in the "_node/stats/search_backpressure" API.
     */
    public abstract Stats currentStats(List<Task> activeTasks);

    /**
     * Interface for the tracker's state as seen in the "_node/stats/search_backpressure" API.
     */
    public interface Stats extends ToXContentObject, Writeable {}
}
