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
    private final EnumMap<TaskResourceUsageTrackerType, TaskResourceUsageTracker> all;

    public TaskResourceUsageTrackers() {
        all = new EnumMap<>(TaskResourceUsageTrackerType.class);
    }

    /**
     * adds the tracker for the TrackerType
     * @param tracker is {@link TaskResourceUsageTracker} implementation which will be added
     * @param trackerType is {@link TaskResourceUsageTrackerType} which depicts the implementation type
     */
    public void addTracker(final TaskResourceUsageTracker tracker, final TaskResourceUsageTrackerType trackerType) {
        all.put(trackerType, tracker);
    }

    /**
     * getter for tracker for a {@link TaskResourceUsageTrackerType}
     * @param type for which the implementation is returned
     * @return the {@link TaskResourceUsageTrackerType}
     */
    public Optional<TaskResourceUsageTracker> getTracker(TaskResourceUsageTrackerType type) {
        return Optional.ofNullable(all.get(type));
    }

    /**
     * Method to access all available {@link TaskResourceUsageTracker}
     * @return all enabled and available {@link TaskResourceUsageTracker}s
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
         * @param resourceUsageBreachEvaluator which suggests whether a task should be cancelled or not
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
         * @param tasks cancellation eligible tasks due to node duress and search traffic threshold breach
         * @return the list of tasks which are breaching task level thresholds for this {@link TaskResourceUsageTracker}
         */
        public List<TaskCancellation> getTaskCancellations(List<CancellableTask> tasks) {
            return tasks.stream()
                .map(task -> this.getTaskCancellation(task, List.of(this::incrementCancellations)))
                .filter(TaskCancellation::isEligibleForCancellation)
                .collect(Collectors.toList());
        }

        private TaskCancellation getTaskCancellation(final CancellableTask task, final List<Runnable> cancellationCallback) {
            Optional<TaskCancellation.Reason> reason = checkAndMaybeGetCancellationReason(task);
            List<TaskCancellation.Reason> reasons = new ArrayList<>();
            reason.ifPresent(reasons::add);

            return new TaskCancellation(task, reasons, cancellationCallback);
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
             * @param task is input to this method on which the cancellation evaluation is performed
             * @return a {@link TaskCancellation.Reason} why this task should be cancelled otherwise empty
             */
            public Optional<TaskCancellation.Reason> evaluate(final Task task);
        }
    }
}
