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
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * TaskResourceUsageTrackers is used to hold all the {@link TaskResourceUsageTracker} objects.
 *
 * @opensearch.internal
 */
public class TaskResourceUsageTrackers {
    private TaskResourceUsageTracker cpuUsageTracker;
    private TaskResourceUsageTracker heapUsageTracker;
    private TaskResourceUsageTracker elapsedTimeTracker;

    public TaskResourceUsageTrackers() { }


    /**
     * adds the cpuUsageTracker
     * @param cpuUsageTracker
     */
    public void addCpuUsageTracker(final TaskResourceUsageTracker cpuUsageTracker) {
        this.cpuUsageTracker = cpuUsageTracker;
    }


    /**
     * adds the heapUsageTracker
     * @param heapUsageTracker
     */
    public void addHeapUsageTracker(final TaskResourceUsageTracker heapUsageTracker) {
        this.heapUsageTracker = heapUsageTracker;
    }


    /**
     * adds the elapsedTimeTracker
     * @param elapsedTimeTracker
     */
    public void addElapsedTimeTracker(final TaskResourceUsageTracker elapsedTimeTracker) {
        this.elapsedTimeTracker = elapsedTimeTracker;
    }


    /**
     * getter for cpuUsageTracker
     * @return
     */
    public TaskResourceUsageTracker getCpuUsageTracker() {
        return cpuUsageTracker;
    }


    /**
     * getter for heapUsageTacker
     * @return
     */
    public TaskResourceUsageTracker getHeapUsageTracker() {
        return heapUsageTracker;
    }


    /**
     * getter for elapsedTimeTracker
     * @return
     */
    public TaskResourceUsageTracker getElapsedTimeTracker() {
        return elapsedTimeTracker;
    }


    /**
     * Method to access all available {@link TaskResourceUsageTracker}
     * @return
     */
    public List<TaskResourceUsageTracker> all() {
        return List.of(heapUsageTracker, cpuUsageTracker, elapsedTimeTracker);
    }

    /**
     * TaskResourceUsageTracker is used to track completions and cancellations of search related tasks.
     * @opensearch.internal
     */
    public static abstract class TaskResourceUsageTracker {
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
         * Method to get taskCancellations due to this tracker for the given {@link CancellableTask} tasks
         * @param tasks
         * @param cancellationCallback
         * @return
         */
        public List<TaskCancellation> getTaskCancellations(List<CancellableTask> tasks, Runnable cancellationCallback) {
            return tasks.stream().map(
                task -> this.getTaskCancellation(task, cancellationCallback)
            ).collect(Collectors.toList());
        }

        private TaskCancellation getTaskCancellation(final CancellableTask task, final Runnable cancellationCallback) {
            Optional<TaskCancellation.Reason> reason = checkAndMaybeGetCancellationReason(task);
            List<TaskCancellation.Reason> reasons = new ArrayList<>();
            reason.ifPresent(reasons::add);

            return new TaskCancellation(task, reasons, List.of(cancellationCallback));
        }

        /**
         * Represents the tracker's state as seen in the stats API.
         */
        public interface Stats extends ToXContentObject, Writeable {}
    }
}
