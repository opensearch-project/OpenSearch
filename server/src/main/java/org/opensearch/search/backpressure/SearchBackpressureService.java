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
import org.opensearch.action.search.SearchTask;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.util.TokenBucket;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.process.ProcessProbe;
import org.opensearch.search.backpressure.settings.SearchBackpressureMode;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.search.backpressure.stats.SearchBackpressureStats;
import org.opensearch.search.backpressure.stats.SearchShardTaskStats;
import org.opensearch.search.backpressure.stats.SearchTaskStats;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private final Map<Class<? extends Task>, SearchBackpressureState> searchBackpressureStates = new HashMap<>() {
        {
            put(SearchTask.class, new SearchBackpressureState());
            put(SearchShardTask.class, new SearchBackpressureState());
        }
    };

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
            new TokenBucket(this::getTaskCompletionCount, getSettings().getCancellationRatio(), getSettings().getCancellationBurst())
        );
    }

    private long getTaskCompletionCount() {
        return searchBackpressureStates.get(SearchTask.class).getCompletionCount() + searchBackpressureStates.get(SearchShardTask.class)
            .getCompletionCount();
    }

    void doRun() {
        SearchBackpressureMode mode = getSettings().getMode();
        if (mode == SearchBackpressureMode.DISABLED) {
            return;
        }

        if (isNodeInDuress() == false) {
            return;
        }

        List<CancellableTask> searchTasks = getSearchTasks();
        List<CancellableTask> searchShardTasks = getSearchShardTasks();
        List<CancellableTask> cancellableTasks = new ArrayList<>();

        // Force-refresh usage stats of these tasks before making a cancellation decision.
        taskResourceTrackingService.refreshResourceStats(searchTasks.toArray(new Task[0]));
        taskResourceTrackingService.refreshResourceStats(searchShardTasks.toArray(new Task[0]));

        // Check if increase in heap usage is due to SearchTasks
        if (isHeapUsageDominatedBySearch(searchTasks, getSettings().getSearchTaskSettings().getTotalHeapBytesThreshold())) {
            cancellableTasks.addAll(searchTasks);
        }

        // Check if increase in heap usage is due to SearchShardTasks
        if (isHeapUsageDominatedBySearch(searchShardTasks, getSettings().getSearchShardTaskSettings().getTotalHeapBytesThreshold())) {
            cancellableTasks.addAll(searchShardTasks);
        }

        if (cancellableTasks.isEmpty()) {
            return;
        }

        for (TaskCancellation taskCancellation : getTaskCancellations(cancellableTasks)) {
            logger.debug(
                "[{} mode] cancelling task [{}] due to high resource consumption [{}]",
                mode.getName(),
                taskCancellation.getTask().getId(),
                taskCancellation.getReasonString()
            );

            if (mode != SearchBackpressureMode.ENFORCED) {
                continue;
            }

            // Independently remove tokens from both token buckets.
            boolean rateLimitReached = taskCancellationRateLimiter.get().request() == false;
            boolean ratioLimitReached = taskCancellationRatioLimiter.get().request() == false;

            // Stop cancelling tasks if there are no tokens in either of the two token buckets.
            if (rateLimitReached && ratioLimitReached) {
                logger.debug("task cancellation limit reached");
                SearchBackpressureState searchBackpressureState = searchBackpressureStates.get(
                    (taskCancellation.getTask() instanceof SearchTask) ? SearchTask.class : SearchShardTask.class
                );
                searchBackpressureState.incrementLimitReachedCount();
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
    boolean isHeapUsageDominatedBySearch(List<CancellableTask> cancellableTasks, long threshold) {
        long usage = cancellableTasks.stream().mapToLong(task -> task.getTotalResourceStats().getMemoryInBytes()).sum();
        if (usage < threshold) {
            logger.debug("heap usage not dominated by search requests [{}/{}]", usage, threshold);
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
            .map(task -> (SearchShardTask) task)
            .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Filters and returns the list of currently running SearchTasks.
     */
    List<CancellableTask> getSearchTasks() {
        return taskResourceTrackingService.getResourceAwareTasks()
            .values()
            .stream()
            .filter(task -> task instanceof SearchTask)
            .map(task -> (SearchTask) task)
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
                if (task instanceof SearchTask) {
                    callbacks.add(tracker::incrementSearchTaskCancellations);
                } else {
                    callbacks.add(tracker::incrementSearchShardTaskCancellations);
                }
                reasons.add(reason.get());
            }
        }

        if (task instanceof SearchTask) {
            callbacks.add(searchBackpressureStates.get(SearchTask.class)::incrementCancellationCount);
        } else {
            callbacks.add(searchBackpressureStates.get(SearchShardTask.class)::incrementCancellationCount);
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

    SearchBackpressureState getSearchTasksState() {
        return searchBackpressureStates.get(SearchTask.class);
    }

    SearchBackpressureState getSearchShardTasksState() {
        return searchBackpressureStates.get(SearchShardTask.class);
    }

    @Override
    public void onTaskCompleted(Task task) {
        if (getSettings().getMode() == SearchBackpressureMode.DISABLED) {
            return;
        }

        if (task instanceof SearchTask == false && task instanceof SearchShardTask == false) {
            return;
        }

        CancellableTask cancellableTask = (CancellableTask) task;
        SearchBackpressureState searchBackpressureState = searchBackpressureStates.get(
            (task instanceof SearchTask) ? SearchTask.class : SearchShardTask.class
        );
        if (cancellableTask.isCancelled() == false) {
            searchBackpressureState.incrementCompletionCount();
        }

        List<Exception> exceptions = new ArrayList<>();
        for (TaskResourceUsageTracker tracker : taskResourceUsageTrackers) {
            try {
                tracker.update(task);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
    }

    @Override
    public void onCancellationRatioChanged() {
        taskCancellationRatioLimiter.set(
            new TokenBucket(this::getTaskCompletionCount, getSettings().getCancellationRatio(), getSettings().getCancellationBurst())
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
        List<CancellableTask> searchTasks = getSearchTasks();
        List<CancellableTask> searchShardTasks = getSearchShardTasks();

        SearchTaskStats searchTaskStats = new SearchTaskStats(
            searchBackpressureStates.get(SearchTask.class).getCancellationCount(),
            searchBackpressureStates.get(SearchTask.class).getLimitReachedCount(),
            taskResourceUsageTrackers.stream()
                .collect(
                    Collectors.toUnmodifiableMap(t -> TaskResourceUsageTrackerType.fromName(t.name()), t -> t.searchTaskStats(searchTasks))
                )
        );

        SearchShardTaskStats searchShardTaskStats = new SearchShardTaskStats(
            searchBackpressureStates.get(SearchShardTask.class).getCancellationCount(),
            searchBackpressureStates.get(SearchShardTask.class).getLimitReachedCount(),
            taskResourceUsageTrackers.stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        t -> TaskResourceUsageTrackerType.fromName(t.name()),
                        t -> t.searchShardTaskStats(searchShardTasks)
                    )
                )
        );

        return new SearchBackpressureStats(searchTaskStats, searchShardTaskStats, getSettings().getMode());
    }
}
