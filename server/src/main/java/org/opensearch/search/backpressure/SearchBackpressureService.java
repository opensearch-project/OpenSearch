/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.util.TokenBucket;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.process.ProcessProbe;
import org.opensearch.search.backpressure.trackers.NodeResourceUsageTracker;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTracker;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.tasks.TaskResourceTrackingService.TaskCompletionListener;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * SearchBackpressureService is responsible for monitoring and cancelling in-flight search tasks if they are
 * breaching resource usage limits when the node is in duress.
 *
 * @opensearch.internal
 */
public class SearchBackpressureService implements Runnable, TaskCompletionListener, SearchBackpressureSettings.Listener {
    private static final Logger logger = LogManager.getLogger(SearchBackpressureService.class);

    private final SearchBackpressureSettings settings;
    private final TaskResourceTrackingService taskResourceTrackingService;
    private final LongSupplier timeNanosSupplier;

    private final List<NodeResourceUsageTracker> nodeResourceUsageTrackers;
    private final List<TaskResourceUsageTracker> taskResourceUsageTrackers;

    private final AtomicReference<TokenBucket> taskCancellationRateLimiter = new AtomicReference<>();
    private final AtomicReference<TokenBucket> taskCancellationRatioLimiter = new AtomicReference<>();

    // Currently, only the state of SearchShardTask is being tracked.
    // This can be generalized to Map<TaskType, SearchBackpressureState> once we start supporting cancellation of SearchTasks as well.
    private final SearchBackpressureState state = new SearchBackpressureState();

    public SearchBackpressureService(
        SearchBackpressureSettings settings,
        TaskResourceTrackingService taskResourceTrackingService,
        ThreadPool threadPool
    ) {
        this(
            settings,
            taskResourceTrackingService,
            threadPool,
            System::nanoTime,
            List.of(
                new NodeResourceUsageTracker(
                    () -> ProcessProbe.getInstance().getProcessCpuPercent() / 100.0 >= settings.getNodeDuressCpuThreshold()
                ),
                new NodeResourceUsageTracker(
                    () -> JvmStats.jvmStats().getMem().getHeapUsedPercent() / 100.0 >= settings.getNodeDuressHeapThreshold()
                )
            ),
            Collections.emptyList()
        );
    }

    public SearchBackpressureService(
        SearchBackpressureSettings settings,
        TaskResourceTrackingService taskResourceTrackingService,
        ThreadPool threadPool,
        LongSupplier timeNanosSupplier,
        List<NodeResourceUsageTracker> nodeResourceUsageTrackers,
        List<TaskResourceUsageTracker> taskResourceUsageTrackers
    ) {
        this.settings = settings;
        this.settings.setListener(this);
        this.taskResourceTrackingService = taskResourceTrackingService;
        this.taskResourceTrackingService.addTaskCompletionListener(this);
        this.timeNanosSupplier = timeNanosSupplier;
        this.nodeResourceUsageTrackers = nodeResourceUsageTrackers;
        this.taskResourceUsageTrackers = taskResourceUsageTrackers;

        this.taskCancellationRateLimiter.set(
            new TokenBucket(timeNanosSupplier, getSettings().getCancellationRateNanos(), getSettings().getCancellationBurst())
        );

        this.taskCancellationRatioLimiter.set(
            new TokenBucket(state::getCompletionCount, getSettings().getCancellationRatio(), getSettings().getCancellationBurst())
        );

        threadPool.scheduleWithFixedDelay(this, getSettings().getInterval(), ThreadPool.Names.GENERIC);
    }

    @Override
    public void run() {
        try {
            doRun();
        } catch (Exception e) {
            logger.debug("failure in search search backpressure", e);
        }
    }

    public void doRun() {
        if (getSettings().isEnabled() == false) {
            return;
        }

        if (isNodeInDuress() == false) {
            return;
        }

        // We are only targeting in-flight cancellation of SearchShardTask for now.
        List<CancellableTask> searchShardTasks = getSearchShardTasks();

        // Force-refresh usage stats of these tasks before making a cancellation decision.
        taskResourceTrackingService.refreshResourceStats(searchShardTasks.toArray(new Task[0]));

        // Skip cancellation if the increase in heap usage is not due to search requests.
        if (isHeapUsageDominatedBySearch(searchShardTasks) == false) {
            return;
        }

        for (TaskCancellation taskCancellation : getTaskCancellations(searchShardTasks)) {
            logger.debug(
                "cancelling task [{}] due to high resource consumption [{}]",
                taskCancellation.getTask().getId(),
                taskCancellation.getReasonString()
            );

            if (getSettings().isEnforced() == false) {
                continue;
            }

            // Independently remove tokens from both token buckets.
            boolean rateLimitReached = taskCancellationRateLimiter.get().request() == false;
            boolean ratioLimitReached = taskCancellationRatioLimiter.get().request() == false;

            // Stop cancelling tasks if there are no tokens in either of the two token buckets.
            if (rateLimitReached && ratioLimitReached) {
                logger.debug("task cancellation limit reached");
                state.incrementLimitReachedCount();
                break;
            }

            taskCancellation.cancel();
            state.incrementCancellationCount();
        }
    }

    /**
     * Returns true if the node is in duress consecutively for the past 'n' observations.
     */
    boolean isNodeInDuress() {
        boolean isNodeInDuress = false;
        int numConsecutiveBreaches = getSettings().getNodeDuressNumConsecutiveBreaches();

        for (NodeResourceUsageTracker tracker : nodeResourceUsageTrackers) {
            if (tracker.check() >= numConsecutiveBreaches) {
                isNodeInDuress = true;  // not breaking the loop so that each tracker's streak gets updated.
            }
        }

        return isNodeInDuress;
    }

    /**
     * Returns true if the increase in heap usage is due to search requests.
     */
    boolean isHeapUsageDominatedBySearch(List<CancellableTask> searchShardTasks) {
        long runningTasksHeapUsage = searchShardTasks.stream().mapToLong(task -> task.getTotalResourceStats().getMemoryInBytes()).sum();
        long searchTasksHeapThreshold = getSettings().getSearchHeapThresholdBytes();
        if (runningTasksHeapUsage < searchTasksHeapThreshold) {
            logger.debug("heap usage not dominated by search requests [{}/{}]", runningTasksHeapUsage, searchTasksHeapThreshold);
            return false;
        }

        return true;
    }

    /**
     * Filters and returns the list of currently running SearchShardTasks.
     */
    List<CancellableTask> getSearchShardTasks() {
        return taskResourceTrackingService.getResourceAwareTasks()
            .values()
            .stream()
            .filter(task -> task instanceof SearchShardTask)
            .map(task -> (CancellableTask) task)
            .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Returns a TaskCancellation wrapper containing the list of reasons (possibly zero), along with an overall
     * cancellation score for the given task. Cancelling a task with a higher score has better chance of recovering the
     * node from duress.
     */
    TaskCancellation getTaskCancellation(CancellableTask task) {
        List<TaskCancellation.Reason> reasons = new ArrayList<>();
        List<Runnable> callbacks = new ArrayList<>();

        for (TaskResourceUsageTracker tracker : taskResourceUsageTrackers) {
            Optional<TaskCancellation.Reason> reason = tracker.cancellationReason(task);
            if (reason.isPresent()) {
                reasons.add(reason.get());
                callbacks.add(() -> tracker.update(task));
            }
        }

        return new TaskCancellation(task, reasons, callbacks);
    }

    /**
     * Returns a list of TaskCancellations sorted by descending order of their cancellation scores.
     */
    List<TaskCancellation> getTaskCancellations(List<CancellableTask> tasks) {
        return tasks.stream()
            .map(this::getTaskCancellation)
            .filter(TaskCancellation::isEligibleForCancellation)
            .sorted(Comparator.reverseOrder())
            .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public void onTaskCompleted(Task task) {
        if (getSettings().isEnabled() == false) {
            return;
        }

        if (task instanceof SearchShardTask == false) {
            return;
        }

        SearchShardTask searchShardTask = (SearchShardTask) task;
        if (searchShardTask.isCancelled() == false) {
            state.incrementCompletionCount();
        }

        List<Exception> exceptions = new ArrayList<>();
        for (TaskResourceUsageTracker tracker : taskResourceUsageTrackers) {
            try {
                tracker.update(searchShardTask);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
    }

    @Override
    public void onCancellationRatioChanged() {
        taskCancellationRatioLimiter.set(
            new TokenBucket(state::getCompletionCount, getSettings().getCancellationRatio(), getSettings().getCancellationBurst())
        );
    }

    @Override
    public void onCancellationRateChanged() {
        taskCancellationRateLimiter.set(
            new TokenBucket(timeNanosSupplier, getSettings().getCancellationRateNanos(), getSettings().getCancellationBurst())
        );
    }

    @Override
    public void onCancellationBurstChanged() {
        onCancellationRatioChanged();
        onCancellationRateChanged();
    }

    public SearchBackpressureSettings getSettings() {
        return settings;
    }

    public SearchBackpressureState getState() {
        return state;
    }
}
