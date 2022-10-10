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
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.util.TokenBucket;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.process.ProcessProbe;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.search.backpressure.stats.CancelledTaskStats;
import org.opensearch.search.backpressure.stats.SearchBackpressureStats;
import org.opensearch.search.backpressure.stats.SearchShardTaskStats;
import org.opensearch.search.backpressure.trackers.CpuUsageTracker;
import org.opensearch.search.backpressure.trackers.ElapsedTimeTracker;
import org.opensearch.search.backpressure.trackers.HeapUsageTracker;
import org.opensearch.search.backpressure.trackers.NodeDuressTracker;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTracker;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.tasks.TaskResourceTrackingService.TaskCompletionListener;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
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
public class SearchBackpressureService extends AbstractLifecycleComponent
    implements
        TaskCompletionListener,
        SearchBackpressureSettings.Listener {
    private static final Logger logger = LogManager.getLogger(SearchBackpressureService.class);

    private volatile Scheduler.Cancellable scheduledFuture;

    private final SearchBackpressureSettings settings;
    private final TaskResourceTrackingService taskResourceTrackingService;
    private final ThreadPool threadPool;
    private final LongSupplier timeNanosSupplier;

    private final List<NodeDuressTracker> nodeDuressTrackers;
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
                new NodeDuressTracker(
                    () -> ProcessProbe.getInstance().getProcessCpuPercent() / 100.0 >= settings.getNodeDuressSettings().getCpuThreshold()
                ),
                new NodeDuressTracker(
                    () -> JvmStats.jvmStats().getMem().getHeapUsedPercent() / 100.0 >= settings.getNodeDuressSettings().getHeapThreshold()
                )
            ),
            List.of(new CpuUsageTracker(settings), new HeapUsageTracker(settings), new ElapsedTimeTracker(settings, System::nanoTime))
        );
    }

    public SearchBackpressureService(
        SearchBackpressureSettings settings,
        TaskResourceTrackingService taskResourceTrackingService,
        ThreadPool threadPool,
        LongSupplier timeNanosSupplier,
        List<NodeDuressTracker> nodeDuressTrackers,
        List<TaskResourceUsageTracker> taskResourceUsageTrackers
    ) {
        this.settings = settings;
        this.settings.addListener(this);
        this.taskResourceTrackingService = taskResourceTrackingService;
        this.taskResourceTrackingService.addTaskCompletionListener(this);
        this.threadPool = threadPool;
        this.timeNanosSupplier = timeNanosSupplier;
        this.nodeDuressTrackers = nodeDuressTrackers;
        this.taskResourceUsageTrackers = taskResourceUsageTrackers;

        this.taskCancellationRateLimiter.set(
            new TokenBucket(timeNanosSupplier, getSettings().getCancellationRateNanos(), getSettings().getCancellationBurst())
        );

        this.taskCancellationRatioLimiter.set(
            new TokenBucket(state::getCompletionCount, getSettings().getCancellationRatio(), getSettings().getCancellationBurst())
        );
    }

    void doRun() {
        if (getSettings().isEnabled() == false) {
            return;
        }

        if (isNodeInDuress() == false) {
            return;
        }

        // We are only targeting in-flight cancellation of SearchShardTask for now.
        List<SearchShardTask> searchShardTasks = getSearchShardTasks();

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
        }
    }

    /**
     * Returns true if the node is in duress consecutively for the past 'n' observations.
     */
    boolean isNodeInDuress() {
        boolean isNodeInDuress = false;
        int numSuccessiveBreaches = getSettings().getNodeDuressSettings().getNumSuccessiveBreaches();

        for (NodeDuressTracker tracker : nodeDuressTrackers) {
            if (tracker.check() >= numSuccessiveBreaches) {
                isNodeInDuress = true;  // not breaking the loop so that each tracker's streak gets updated.
            }
        }

        return isNodeInDuress;
    }

    /**
     * Returns true if the increase in heap usage is due to search requests.
     */
    boolean isHeapUsageDominatedBySearch(List<SearchShardTask> searchShardTasks) {
        long usage = searchShardTasks.stream().mapToLong(task -> task.getTotalResourceStats().getMemoryInBytes()).sum();
        long threshold = getSettings().getSearchShardTaskSettings().getTotalHeapBytesThreshold();
        if (usage < threshold) {
            logger.debug("heap usage not dominated by search requests [{}/{}]", usage, threshold);
            return false;
        }

        return true;
    }

    /**
     * Filters and returns the list of currently running SearchShardTasks.
     */
    List<SearchShardTask> getSearchShardTasks() {
        return taskResourceTrackingService.getResourceAwareTasks()
            .values()
            .stream()
            .filter(task -> task instanceof SearchShardTask)
            .map(task -> (SearchShardTask) task)
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
            Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
            if (reason.isPresent()) {
                reasons.add(reason.get());
                callbacks.add(tracker::incrementCancellations);
            }
        }

        if (task instanceof SearchShardTask) {
            callbacks.add(state::incrementCancellationCount);
            callbacks.add(() -> state.setLastCancelledTaskStats(CancelledTaskStats.from(task, timeNanosSupplier)));
        }

        return new TaskCancellation(task, reasons, callbacks);
    }

    /**
     * Returns a list of TaskCancellations sorted by descending order of their cancellation scores.
     */
    List<TaskCancellation> getTaskCancellations(List<? extends CancellableTask> tasks) {
        return tasks.stream()
            .map(this::getTaskCancellation)
            .filter(TaskCancellation::isEligibleForCancellation)
            .sorted(Comparator.reverseOrder())
            .collect(Collectors.toUnmodifiableList());
    }

    SearchBackpressureSettings getSettings() {
        return settings;
    }

    SearchBackpressureState getState() {
        return state;
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

    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
            try {
                doRun();
            } catch (Exception e) {
                logger.debug("failure in search search backpressure", e);
            }
        }, getSettings().getInterval(), ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {}

    public SearchBackpressureStats nodeStats() {
        List<SearchShardTask> searchShardTasks = getSearchShardTasks();

        SearchShardTaskStats searchShardTaskStats = new SearchShardTaskStats(
            state.getCancellationCount(),
            state.getLimitReachedCount(),
            state.getLastCancelledTaskStats(),
            taskResourceUsageTrackers.stream()
                .collect(Collectors.toUnmodifiableMap(t -> TaskResourceUsageTrackerType.fromName(t.name()), t -> t.stats(searchShardTasks)))
        );

        return new SearchBackpressureStats(searchShardTaskStats, getSettings().isEnabled(), getSettings().isEnforced());
    }
}
