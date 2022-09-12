/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.search.backpressure.stats.CancelledTaskStats;
import org.opensearch.search.backpressure.trackers.ResourceUsageTracker;
import org.opensearch.tasks.CancellableTask;

import java.util.List;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * TaskCancellation is a wrapper for a task and its cancellation reasons.
 */
public class TaskCancellation implements Comparable<TaskCancellation> {
    private final CancellableTask task;
    private final List<Reason> reasons;
    private final LongSupplier timeNanosSupplier;

    public TaskCancellation(CancellableTask task, List<Reason> reasons, LongSupplier timeNanosSupplier) {
        this.task = task;
        this.reasons = reasons;
        this.timeNanosSupplier = timeNanosSupplier;
    }

    public CancellableTask getTask() {
        return task;
    }

    public List<Reason> getReasons() {
        return reasons;
    }

    public String getReasonString() {
        return reasons.stream().map(Reason::getMessage).collect(Collectors.joining(", "));
    }

    /**
     * Cancels the task and increments the cancellation counters for all breaching resource usage trackers.
     */
    public CancelledTaskStats cancel() {
        task.cancel("resource consumption exceeded [" + getReasonString() + "]");
        reasons.forEach(reason -> reason.getTracker().incrementCancellations());

        return new CancelledTaskStats(
            task.getTotalResourceStats().getCpuTimeInNanos(),
            task.getTotalResourceStats().getMemoryInBytes(),
            timeNanosSupplier.getAsLong() - task.getStartTimeNanos()
        );
    }

    /**
     * Returns the sum of all cancellation scores.
     *
     * A zero score indicates no thresholds were breached, i.e., the task should not be cancelled.
     * A task with higher score suggests greater possibility of recovering the node when that task is cancelled.
     */
    public int totalCancellationScore() {
        return reasons.stream().mapToInt(Reason::getCancellationScore).sum();
    }

    /**
     * A task is eligible for cancellation if it has one or more cancellation reasons, and is not already cancelled.
     */
    public boolean isEligibleForCancellation() {
        return (task.isCancelled() == false) && (reasons.size() > 0);
    }

    @Override
    public int compareTo(TaskCancellation other) {
        return Integer.compare(totalCancellationScore(), other.totalCancellationScore());
    }

    /**
     * Represents the cancellation reason for a task.
     */
    public static class Reason {
        private final ResourceUsageTracker tracker;
        private final String message;
        private final int cancellationScore;

        public Reason(ResourceUsageTracker tracker, String message, int cancellationScore) {
            this.tracker = tracker;
            this.message = message;
            this.cancellationScore = cancellationScore;
        }

        public ResourceUsageTracker getTracker() {
            return tracker;
        }

        public String getMessage() {
            return message;
        }

        public int getCancellationScore() {
            return cancellationScore;
        }
    }
}
