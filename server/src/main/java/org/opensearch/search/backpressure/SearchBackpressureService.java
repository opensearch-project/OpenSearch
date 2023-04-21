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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.process.ProcessProbe;
import org.opensearch.search.backpressure.settings.SearchBackpressureMode;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.search.backpressure.settings.SearchShardTaskSettings;
import org.opensearch.search.backpressure.settings.SearchTaskSettings;
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
import org.opensearch.tasks.SearchBackpressureTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.tasks.TaskResourceTrackingService.TaskCompletionListener;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.opensearch.search.backpressure.trackers.HeapUsageTracker.isHeapTrackingSupported;

/**
 * SearchBackpressureService is responsible for monitoring and cancelling in-flight search tasks if they are
 * breaching resource usage limits when the node is in duress.
 *
 * @opensearch.internal
 */
public class SearchBackpressureService extends AbstractLifecycleComponent implements TaskCompletionListener {
    private static final Logger logger = LogManager.getLogger(SearchBackpressureService.class);

    private volatile Scheduler.Cancellable scheduledFuture;

    private final SearchBackpressureSettings settings;
    private final TaskResourceTrackingService taskResourceTrackingService;
    private final ThreadPool threadPool;
    private final LongSupplier timeNanosSupplier;

    private final List<NodeDuressTracker> nodeDuressTrackers;
    private final Map<Class<? extends SearchBackpressureTask>, List<TaskResourceUsageTracker>> taskTrackers;

    private final Map<Class<? extends SearchBackpressureTask>, SearchBackpressureState> searchBackpressureStates;
    private final TaskManager taskManager;

    public SearchBackpressureService(
        SearchBackpressureSettings settings,
        TaskResourceTrackingService taskResourceTrackingService,
        ThreadPool threadPool,
        TaskManager taskManager
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
            getTrackers(
                settings.getSearchTaskSettings()::getCpuTimeNanosThreshold,
                settings.getSearchTaskSettings()::getHeapVarianceThreshold,
                settings.getSearchTaskSettings()::getHeapPercentThreshold,
                settings.getSearchTaskSettings().getHeapMovingAverageWindowSize(),
                settings.getSearchTaskSettings()::getElapsedTimeNanosThreshold,
                settings.getClusterSettings(),
                SearchTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE
            ),
            getTrackers(
                settings.getSearchShardTaskSettings()::getCpuTimeNanosThreshold,
                settings.getSearchShardTaskSettings()::getHeapVarianceThreshold,
                settings.getSearchShardTaskSettings()::getHeapPercentThreshold,
                settings.getSearchShardTaskSettings().getHeapMovingAverageWindowSize(),
                settings.getSearchShardTaskSettings()::getElapsedTimeNanosThreshold,
                settings.getClusterSettings(),
                SearchShardTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE
            ),
            taskManager
        );
    }

    public SearchBackpressureService(
        SearchBackpressureSettings settings,
        TaskResourceTrackingService taskResourceTrackingService,
        ThreadPool threadPool,
        LongSupplier timeNanosSupplier,
        List<NodeDuressTracker> nodeDuressTrackers,
        List<TaskResourceUsageTracker> searchTaskTrackers,
        List<TaskResourceUsageTracker> searchShardTaskTrackers,
        TaskManager taskManager
    ) {
        this.settings = settings;
        this.taskResourceTrackingService = taskResourceTrackingService;
        this.taskResourceTrackingService.addTaskCompletionListener(this);
        this.threadPool = threadPool;
        this.timeNanosSupplier = timeNanosSupplier;
        this.nodeDuressTrackers = nodeDuressTrackers;
        this.taskManager = taskManager;

        this.searchBackpressureStates = Map.of(
            SearchTask.class,
            new SearchBackpressureState(
                timeNanosSupplier,
                getSettings().getSearchTaskSettings().getCancellationRateNanos(),
                getSettings().getSearchTaskSettings().getCancellationBurst(),
                getSettings().getSearchTaskSettings().getCancellationRatio()
            ),
            SearchShardTask.class,
            new SearchBackpressureState(
                timeNanosSupplier,
                getSettings().getSearchShardTaskSettings().getCancellationRateNanos(),
                getSettings().getSearchShardTaskSettings().getCancellationBurst(),
                getSettings().getSearchShardTaskSettings().getCancellationRatio()
            )
        );
        this.settings.getSearchTaskSettings().addListener(searchBackpressureStates.get(SearchTask.class));
        this.settings.getSearchShardTaskSettings().addListener(searchBackpressureStates.get(SearchShardTask.class));

        this.taskTrackers = Map.of(SearchTask.class, searchTaskTrackers, SearchShardTask.class, searchShardTaskTrackers);
    }

    void doRun() {
        SearchBackpressureMode mode = getSettings().getMode();
        if (mode == SearchBackpressureMode.DISABLED) {
            return;
        }

        if (isNodeInDuress() == false) {
            return;
        }

        List<CancellableTask> searchTasks = getTaskByType(SearchTask.class);
        List<CancellableTask> searchShardTasks = getTaskByType(SearchShardTask.class);
        List<CancellableTask> cancellableTasks = new ArrayList<>();

        // Force-refresh usage stats of these tasks before making a cancellation decision.
        taskResourceTrackingService.refreshResourceStats(searchTasks.toArray(new Task[0]));
        taskResourceTrackingService.refreshResourceStats(searchShardTasks.toArray(new Task[0]));

        // Check if increase in heap usage is due to SearchTasks
        if (HeapUsageTracker.isHeapUsageDominatedBySearch(
            searchTasks,
            getSettings().getSearchTaskSettings().getTotalHeapPercentThreshold()
        )) {
            cancellableTasks.addAll(searchTasks);
        }

        // Check if increase in heap usage is due to SearchShardTasks
        if (HeapUsageTracker.isHeapUsageDominatedBySearch(
            searchShardTasks,
            getSettings().getSearchShardTaskSettings().getTotalHeapPercentThreshold()
        )) {
            cancellableTasks.addAll(searchShardTasks);
        }

        // none of the task type is breaching the heap usage thresholds and hence we do not cancel any tasks
        if (cancellableTasks.isEmpty()) {
            return;
        }

        for (TaskCancellation taskCancellation : getTaskCancellations(cancellableTasks)) {
            logger.warn(
                "[{} mode] cancelling task [{}] due to high resource consumption [{}]",
                mode.getName(),
                taskCancellation.getTask().getId(),
                taskCancellation.getReasonString()
            );

            if (mode != SearchBackpressureMode.ENFORCED) {
                continue;
            }

            Class<? extends SearchBackpressureTask> taskType = getTaskType(taskCancellation.getTask());

            // Independently remove tokens from both token buckets.
            SearchBackpressureState searchBackpressureState = searchBackpressureStates.get(taskType);
            boolean rateLimitReached = searchBackpressureState.getRateLimiter().request() == false;
            boolean ratioLimitReached = searchBackpressureState.getRatioLimiter().request() == false;

            // Stop cancelling tasks if there are no tokens in either of the two token buckets.
            if (rateLimitReached && ratioLimitReached) {
                logger.debug("task cancellation limit reached");
                searchBackpressureState.incrementLimitReachedCount();
                break;
            }

            taskCancellation.cancelTaskAndDescendants(taskManager);
        }
    }

    /**
     * Given a task, returns the type of the task
     */
    Class<? extends SearchBackpressureTask> getTaskType(Task task) {
        if (task instanceof SearchTask) {
            return SearchTask.class;
        } else if (task instanceof SearchShardTask) {
            return SearchShardTask.class;
        } else {
            throw new IllegalArgumentException("task must be instance of either SearchTask or SearchShardTask");
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

    /**
     * Filters and returns the list of currently running tasks of specified type.
     */
    <T extends CancellableTask & SearchBackpressureTask> List<CancellableTask> getTaskByType(Class<T> type) {
        return taskResourceTrackingService.getResourceAwareTasks()
            .values()
            .stream()
            .filter(type::isInstance)
            .map(type::cast)
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
        Class<? extends SearchBackpressureTask> taskType = getTaskType(task);
        List<TaskResourceUsageTracker> trackers = taskTrackers.get(taskType);
        for (TaskResourceUsageTracker tracker : trackers) {
            Optional<TaskCancellation.Reason> reason = tracker.checkAndMaybeGetCancellationReason(task);
            if (reason.isPresent()) {
                callbacks.add(tracker::incrementCancellations);
                reasons.add(reason.get());
            }
        }
        callbacks.add(searchBackpressureStates.get(taskType)::incrementCancellationCount);

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

    SearchBackpressureState getSearchBackpressureState(Class<? extends SearchBackpressureTask> taskType) {
        return searchBackpressureStates.get(taskType);
    }

    /**
     * Given the threshold suppliers, returns the list of applicable trackers
     */
    public static List<TaskResourceUsageTracker> getTrackers(
        LongSupplier cpuThresholdSupplier,
        DoubleSupplier heapVarianceSupplier,
        DoubleSupplier heapPercentThresholdSupplier,
        int heapMovingAverageWindowSize,
        LongSupplier ElapsedTimeNanosSupplier,
        ClusterSettings clusterSettings,
        Setting<Integer> windowSizeSetting
    ) {
        List<TaskResourceUsageTracker> trackers = new ArrayList<>();
        trackers.add(new CpuUsageTracker(cpuThresholdSupplier));
        if (isHeapTrackingSupported()) {
            trackers.add(
                new HeapUsageTracker(
                    heapVarianceSupplier,
                    heapPercentThresholdSupplier,
                    heapMovingAverageWindowSize,
                    clusterSettings,
                    windowSizeSetting
                )
            );
        } else {
            logger.warn("heap size couldn't be determined");
        }
        trackers.add(new ElapsedTimeTracker(ElapsedTimeNanosSupplier, System::nanoTime));
        return Collections.unmodifiableList(trackers);
    }

    @Override
    public void onTaskCompleted(Task task) {
        if (getSettings().getMode() == SearchBackpressureMode.DISABLED) {
            return;
        }

        if (task instanceof SearchBackpressureTask == false) {
            return;
        }

        CancellableTask cancellableTask = (CancellableTask) task;
        Class<? extends SearchBackpressureTask> taskType = getTaskType(task);
        if (cancellableTask.isCancelled() == false) {
            searchBackpressureStates.get(taskType).incrementCompletionCount();
        }

        List<Exception> exceptions = new ArrayList<>();
        List<TaskResourceUsageTracker> trackers = taskTrackers.get(taskType);
        for (TaskResourceUsageTracker tracker : trackers) {
            try {
                tracker.update(task);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
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
        List<CancellableTask> searchTasks = getTaskByType(SearchTask.class);
        List<CancellableTask> searchShardTasks = getTaskByType(SearchShardTask.class);
        SearchTaskStats searchTaskStats = new SearchTaskStats(
            searchBackpressureStates.get(SearchTask.class).getCancellationCount(),
            searchBackpressureStates.get(SearchTask.class).getLimitReachedCount(),
            taskTrackers.get(SearchTask.class)
                .stream()
                .collect(Collectors.toUnmodifiableMap(t -> TaskResourceUsageTrackerType.fromName(t.name()), t -> t.stats(searchTasks)))
        );

        SearchShardTaskStats searchShardTaskStats = new SearchShardTaskStats(
            searchBackpressureStates.get(SearchShardTask.class).getCancellationCount(),
            searchBackpressureStates.get(SearchShardTask.class).getLimitReachedCount(),
            taskTrackers.get(SearchShardTask.class)
                .stream()
                .collect(Collectors.toUnmodifiableMap(t -> TaskResourceUsageTrackerType.fromName(t.name()), t -> t.stats(searchShardTasks)))
        );

        return new SearchBackpressureStats(searchTaskStats, searchShardTaskStats, getSettings().getMode());
    }
}
