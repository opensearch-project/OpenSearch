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
import java.util.EnumMap;
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
    private final EnumMap<TaskResourceUsageTrackerType, TaskResourceUsageTracker> all;

    public TaskResourceUsageTrackers() {
        all = new EnumMap<>(TaskResourceUsageTrackerType.class);
    }

    /**
     * adds the cpuUsageTracker
     * @param cpuUsageTracker
     */
    public void addCpuUsageTracker(final TaskResourceUsageTracker cpuUsageTracker) {
        this.cpuUsageTracker = cpuUsageTracker;
        all.put(TaskResourceUsageTrackerType.CPU_USAGE_TRACKER, cpuUsageTracker);
    }

    /**
     * adds the heapUsageTracker
     * @param heapUsageTracker
     */
    public void addHeapUsageTracker(final TaskResourceUsageTracker heapUsageTracker) {
        this.heapUsageTracker = heapUsageTracker;
        all.put(TaskResourceUsageTrackerType.HEAP_USAGE_TRACKER, heapUsageTracker);
    }

    /**
     * adds the elapsedTimeTracker
     * @param elapsedTimeTracker
     */
    public void addElapsedTimeTracker(final TaskResourceUsageTracker elapsedTimeTracker) {
        this.elapsedTimeTracker = elapsedTimeTracker;
        all.put(TaskResourceUsageTrackerType.ELAPSED_TIME_TRACKER, elapsedTimeTracker);
    }

    /**
     * getter for cpuUsageTracker
     * @return
     */
    public Optional<TaskResourceUsageTracker> getCpuUsageTracker() {
        return Optional.ofNullable(cpuUsageTracker);
    }

    /**
     * getter for heapUsageTacker
     * @return
     */
    public Optional<TaskResourceUsageTracker> getHeapUsageTracker() {
        return Optional.ofNullable(heapUsageTracker);
    }

    /**
     * getter for elapsedTimeTracker
     * @return
     */
    public Optional<TaskResourceUsageTracker> getElapsedTimeTracker() {
        return Optional.ofNullable(elapsedTimeTracker);
    }

    /**
     * Method to access all available {@link TaskResourceUsageTracker}
     * @return
     */
    public List<TaskResourceUsageTracker> all() {
        return new ArrayList<>(all.values());
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
        protected ResourceUsageBreachEvaluator resourceUsageBreachEvaluator;

        /**
         * for test purposes only
         * @param resourceUsageBreachEvaluator
         */
        public void setResourceUsageBreachEvaluator(final ResourceUsageBreachEvaluator resourceUsageBreachEvaluator) {
            this.resourceUsageBreachEvaluator = resourceUsageBreachEvaluator;
        }

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
        public Optional<TaskCancellation.Reason> checkAndMaybeGetCancellationReason(Task task) {
            return resourceUsageBreachEvaluator.evaluate(task);
        }

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
            return tasks.stream()
                .map(task -> this.getTaskCancellation(task, cancellationCallback))
                .filter(TaskCancellation::isEligibleForCancellation)
                .map(taskCancellation -> {
                    List<Runnable> onCancelCallbacks = new ArrayList<>(taskCancellation.getOnCancelCallbacks());
                    onCancelCallbacks.add(this::incrementCancellations);
                    return new TaskCancellation(taskCancellation.getTask(), taskCancellation.getReasons(), onCancelCallbacks);
                })
                .collect(Collectors.toList());
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

        /**
         * This interface carries the logic to decide whether a task should be cancelled or not
         */
        public interface ResourceUsageBreachEvaluator {
            /**
             * evaluates whether the task is eligible for cancellation based on {@link TaskResourceUsageTracker} implementation
             * @param task
             * @return a {@link TaskCancellation.Reason} why this task should be cancelled otherwise empty
             */
            public Optional<TaskCancellation.Reason> evaluate(final Task task);
        }
    }
}
