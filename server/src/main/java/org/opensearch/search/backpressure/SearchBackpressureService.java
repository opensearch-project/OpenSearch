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
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.TimeBasedExpiryTracker;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsProbe;
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
import org.opensearch.search.backpressure.trackers.NativeMemoryUsageTracker;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers.NodeDuressTracker;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackers;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackers.TaskResourceUsageTracker;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.SearchBackpressureTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.tasks.TaskResourceTrackingService.TaskCompletionListener;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.opensearch.search.backpressure.trackers.HeapUsageTracker.isHeapTrackingSupported;
import static org.opensearch.search.backpressure.trackers.NativeMemoryUsageTracker.isNativeTrackingSupported;

/**
 * SearchBackpressureService is responsible for monitoring and cancelling in-flight search tasks if they are
 * breaching resource usage limits when the node is in duress.
 *
 * @opensearch.internal
 */
public class SearchBackpressureService extends AbstractLifecycleComponent implements TaskCompletionListener {
    private static final Logger logger = LogManager.getLogger(SearchBackpressureService.class);
    // Tracker-apply rules:
    //   - When native-memory duress is active, we run ONLY the native-memory and elapsed-time
    //     trackers. CPU and heap trackers are intentionally skipped so that a runaway native
    //     allocator doesn't trigger unrelated cancellation paths (heap is still low, CPU may
    //     not be pegged). See the dedicated short-circuit in doRun().
    //   - Otherwise, CPU and heap trackers run when their corresponding resource is in duress,
    //     elapsed-time always runs, and the native-memory tracker runs only under its own
    //     native-memory duress condition.
    private static final Map<TaskResourceUsageTrackerType, Function<NodeDuressTrackers, Boolean>> trackerApplyConditions = Map.of(
        TaskResourceUsageTrackerType.CPU_USAGE_TRACKER,
        (nodeDuressTrackers) -> nodeDuressTrackers.isResourceInDuress(ResourceType.CPU),
        TaskResourceUsageTrackerType.HEAP_USAGE_TRACKER,
        (nodeDuressTrackers) -> isHeapTrackingSupported() && nodeDuressTrackers.isResourceInDuress(ResourceType.MEMORY),
        TaskResourceUsageTrackerType.ELAPSED_TIME_TRACKER,
        (nodeDuressTrackers) -> true,
        TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER,
        (nodeDuressTrackers) -> isNativeTrackingSupported() && nodeDuressTrackers.isNativeMemoryInDuress()
    );

    private volatile Scheduler.Cancellable scheduledFuture;

    private final SearchBackpressureSettings settings;
    private final TaskResourceTrackingService taskResourceTrackingService;
    private final ThreadPool threadPool;

    private final NodeDuressTrackers nodeDuressTrackers;
    private final Map<Class<? extends SearchBackpressureTask>, TaskResourceUsageTrackers> taskTrackers;

    private final Map<Class<? extends SearchBackpressureTask>, SearchBackpressureState> searchBackpressureStates;
    private final TaskManager taskManager;
    private final WorkloadGroupService workloadGroupService;

    public SearchBackpressureService(
        SearchBackpressureSettings settings,
        TaskResourceTrackingService taskResourceTrackingService,
        ThreadPool threadPool,
        TaskManager taskManager,
        WorkloadGroupService workloadGroupService
    ) {
        this(settings, taskResourceTrackingService, threadPool, System::nanoTime, new NodeDuressTrackers(new EnumMap<>(ResourceType.class) {
            {
                put(
                    ResourceType.CPU,
                    new NodeDuressTracker(
                        () -> ProcessProbe.getInstance().getProcessCpuPercent() / 100.0 >= settings.getNodeDuressSettings()
                            .getCpuThreshold(),
                        () -> settings.getNodeDuressSettings().getNumSuccessiveBreaches()
                    )
                );
                put(
                    ResourceType.MEMORY,
                    new NodeDuressTracker(
                        () -> JvmStats.jvmStats().getMem().getHeapUsedPercent() / 100.0 >= settings.getNodeDuressSettings()
                            .getHeapThreshold(),
                        () -> settings.getNodeDuressSettings().getNumSuccessiveBreaches()
                    )
                );
                put(ResourceType.NATIVE_MEMORY, new NodeDuressTracker(() -> {
                    // Native-memory duress probe. OsProbe.getProcessNativeMemoryFraction()
                    // returns nativeBytes / NATIVE_MEMORY_DENOMINATOR_BYTES on Linux, or -1.0
                    // when the underlying RssAnon signal is unavailable (non-Linux or
                    // kernels < 4.5). The denominator is a POC constant in OsProbe — replace
                    // with total physical memory or a cgroup limit before shipping.
                    double used = OsProbe.getInstance().getProcessNativeMemoryBytes();
                    double totalNative = settings.getSearchTaskSettings().getTotalNativeMemoryBytesThreshold();
                    double usedFraction = used / totalNative;
                    if (usedFraction < 0.0d) {
                        logger.info("[nativemem-bp] duress-probe: native memory signal unavailable");
                        return false;
                    }
                    double threshold = settings.getNodeDuressSettings().getNativeMemoryThreshold();
                    boolean breached = usedFraction >= threshold;
                    logger.info(
                        "[nativemem-bp] duress-probe: usedFraction={}, threshold={}, breached={}",
                        String.format(java.util.Locale.ROOT, "%.4f", usedFraction),
                        String.format(java.util.Locale.ROOT, "%.4f", threshold),
                        breached
                    );
                    return breached;
                }, () -> settings.getNodeDuressSettings().getNumSuccessiveBreaches()));
            }
        }, new TimeBasedExpiryTracker(System::nanoTime)),
            getTrackers(
                settings.getSearchTaskSettings()::getCpuTimeNanosThreshold,
                settings.getSearchTaskSettings()::getHeapVarianceThreshold,
                settings.getSearchTaskSettings()::getHeapPercentThreshold,
                settings.getSearchTaskSettings().getHeapMovingAverageWindowSize(),
                settings.getSearchTaskSettings()::getElapsedTimeNanosThreshold,
                settings.getSearchTaskSettings()::getNativeMemoryPercentThreshold,
                settings.getClusterSettings(),
                SearchTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE
            ),
            getTrackers(
                settings.getSearchShardTaskSettings()::getCpuTimeNanosThreshold,
                settings.getSearchShardTaskSettings()::getHeapVarianceThreshold,
                settings.getSearchShardTaskSettings()::getHeapPercentThreshold,
                settings.getSearchShardTaskSettings().getHeapMovingAverageWindowSize(),
                settings.getSearchShardTaskSettings()::getElapsedTimeNanosThreshold,
                settings.getSearchShardTaskSettings()::getNativeMemoryPercentThreshold,
                settings.getClusterSettings(),
                SearchShardTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE
            ),
            taskManager,
            workloadGroupService
        );
    }

    SearchBackpressureService(
        SearchBackpressureSettings settings,
        TaskResourceTrackingService taskResourceTrackingService,
        ThreadPool threadPool,
        LongSupplier timeNanosSupplier,
        NodeDuressTrackers nodeDuressTrackers,
        TaskResourceUsageTrackers searchTaskTrackers,
        TaskResourceUsageTrackers searchShardTaskTrackers,
        TaskManager taskManager,
        WorkloadGroupService workloadGroupService
    ) {
        this.settings = settings;
        this.taskResourceTrackingService = taskResourceTrackingService;
        this.taskResourceTrackingService.addTaskCompletionListener(this);
        this.threadPool = threadPool;
        this.nodeDuressTrackers = nodeDuressTrackers;
        this.taskManager = taskManager;
        this.workloadGroupService = workloadGroupService;

        this.searchBackpressureStates = Map.of(
            SearchTask.class,
            new SearchBackpressureState(
                timeNanosSupplier,
                getSettings().getSearchTaskSettings().getCancellationRateNanos(),
                getSettings().getSearchTaskSettings().getCancellationBurst(),
                getSettings().getSearchTaskSettings().getCancellationRatio(),
                getSettings().getSearchTaskSettings().getCancellationRate()
            ),
            SearchShardTask.class,
            new SearchBackpressureState(
                timeNanosSupplier,
                getSettings().getSearchShardTaskSettings().getCancellationRateNanos(),
                getSettings().getSearchShardTaskSettings().getCancellationBurst(),
                getSettings().getSearchShardTaskSettings().getCancellationRatio(),
                getSettings().getSearchShardTaskSettings().getCancellationRate()
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

        if (nodeDuressTrackers.isNodeInDuress() == false) {
            return;
        }

        List<CancellableTask> searchTasks = getTaskByType(SearchTask.class);
        List<CancellableTask> searchShardTasks = getTaskByType(SearchShardTask.class);

        // Native-memory duress takes precedence: it's a symptom of off-heap pressure the heap
        // share check isn't designed to diagnose, so we skip the heap-dominance gate and only
        // run the native-memory + elapsed-time trackers. Heap/CPU trackers would target the
        // wrong workload here.
        final boolean inNativeMemoryDuress = nodeDuressTrackers.isNativeMemoryInDuress();
        if (inNativeMemoryDuress) {
            logger.info(
                "[nativemem-bp] doRun: native-memory duress active, short-circuiting to "
                    + "[NATIVE_MEMORY_USAGE_TRACKER, ELAPSED_TIME_TRACKER] — searchTasks={}, searchShardTasks={}",
                searchTasks.size(),
                searchShardTasks.size()
            );
        } else {
            logger.info(
                "[nativemem-bp] doRun: node in duress (non-native-memory) — searchTasks={}, searchShardTasks={}",
                searchTasks.size(),
                searchShardTasks.size()
            );
        }

         Map<Class<? extends SearchBackpressureTask>, List<CancellableTask>> cancellableTasks;

            boolean isHeapUsageDominatedBySearchTasks = isHeapUsageDominatedBySearch(
                searchTasks,
                getSettings().getSearchTaskSettings().getTotalHeapPercentThreshold()
            );
            boolean isHeapUsageDominatedBySearchShardTasks = isHeapUsageDominatedBySearch(
                searchShardTasks,
                getSettings().getSearchShardTaskSettings().getTotalHeapPercentThreshold()
            );
            cancellableTasks = Map.of(
                SearchTask.class,
                isHeapUsageDominatedBySearchTasks ? searchTasks : Collections.emptyList(),
                SearchShardTask.class,
                isHeapUsageDominatedBySearchShardTasks ? searchShardTasks : Collections.emptyList()
            );

        if (inNativeMemoryDuress) {
             cancellableTasks = Map.of(SearchTask.class, searchTasks, SearchShardTask.class, searchShardTasks);
            if (shouldApply(TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER)) {
                logger.info("[nativemem-bp] doRun: refreshing tracker [{}]", TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER.getName());
                for (TaskResourceUsageTrackers trackers : taskTrackers.values()) {
                    trackers.getTracker(TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER).ifPresent(TaskResourceUsageTracker::refresh);
                }
            }
        }

        // Force-refresh usage stats of these tasks before making a cancellation decision.
        taskResourceTrackingService.refreshResourceStats(searchTasks.toArray(new Task[0]));
        taskResourceTrackingService.refreshResourceStats(searchShardTasks.toArray(new Task[0]));

        List<TaskCancellation> taskCancellations = new ArrayList<>();
        for (TaskResourceUsageTrackerType trackerType : TaskResourceUsageTrackerType.values()) {
            if (shouldApply(trackerType)) {
                int before = taskCancellations.size();
                addResourceTrackerBasedCancellations(trackerType, taskCancellations, cancellableTasks);
                int added = taskCancellations.size() - before;
                logger.info(
                    "[nativemem-bp] doRun: tracker [{}] applied — produced {} cancellation reason(s)",
                    trackerType.getName(),
                    added
                );
            } else {
                logger.info("[nativemem-bp] doRun: tracker [{}] not applied (shouldApply=false)", trackerType.getName());
            }
        }

        // Since these cancellations might be duplicate due to multiple trackers causing cancellation for same task
        // We need to merge them
        int beforeMerge = taskCancellations.size();
        taskCancellations = mergeTaskCancellations(taskCancellations).stream()
            .map(this::addSBPStateUpdateCallback)
            .filter(TaskCancellation::isEligibleForCancellation)
            .collect(Collectors.toList());
        logger.info(
            "[nativemem-bp] doRun: merged {} reasons -> {} eligible cancellations",
            beforeMerge,
            taskCancellations.size()
        );

        for (TaskCancellation taskCancellation : taskCancellations) {
            logger.warn(
                "[{} mode] cancelling task [{}] due to high resource consumption [{}]",
                mode.getName(),
                taskCancellation.getTask().getId(),
                taskCancellation.getReasonString()
            );

            if (mode != SearchBackpressureMode.ENFORCED) {
                logger.info(
                    "[nativemem-bp] doRun: mode={} (not ENFORCED) — would-cancel task={} only logged, not actioned",
                    mode.getName(),
                    taskCancellation.getTask().getId()
                );
                continue;
            }

            Class<? extends SearchBackpressureTask> taskType = getTaskType(taskCancellation.getTask());

            // Independently remove tokens from both token buckets.
            SearchBackpressureState searchBackpressureState = searchBackpressureStates.get(taskType);
            boolean rateLimitReached = searchBackpressureState.getRateLimiter().request() == false;
            boolean ratioLimitReached = searchBackpressureState.getRatioLimiter().request() == false;

            // Stop cancelling tasks if there are no tokens in either of the two token buckets.
            if (rateLimitReached && ratioLimitReached) {
                logger.warn(
                    "[nativemem-bp] doRun: cancellation limit reached for {} — task={} not cancelled (rateLimit={} ratioLimit={})",
                    taskType.getSimpleName(),
                    taskCancellation.getTask().getId(),
                    rateLimitReached,
                    ratioLimitReached
                );
                searchBackpressureState.incrementLimitReachedCount();
                break;
            }

            logger.info(
                "[nativemem-bp] doRun: actioning cancellation — task={} type={} rateLimitOk={} ratioLimitOk={}",
                taskCancellation.getTask().getId(),
                taskType.getSimpleName(),
                rateLimitReached == false,
                ratioLimitReached == false
            );
            taskCancellation.cancelTaskAndDescendants(taskManager);
        }
    }

    /**
     * Had to define this method to help mock this static method to test the scenario where SearchTraffic should not be
     * penalised when not breaching the threshold
     * @param searchTasks inFlight co-ordinator requests
     * @param threshold   miniumum  jvm allocated bytes ratio w.r.t. available heap
     * @return a boolean value based on whether the threshold is breached
     */
    boolean isHeapUsageDominatedBySearch(List<CancellableTask> searchTasks, double threshold) {
        return HeapUsageTracker.isHeapUsageDominatedBySearch(searchTasks, threshold);
    }

    private TaskCancellation addSBPStateUpdateCallback(TaskCancellation taskCancellation) {
        CancellableTask task = taskCancellation.getTask();
        Runnable toAddCancellationCallbackForSBPState = searchBackpressureStates.get(SearchShardTask.class)::incrementCancellationCount;
        if (task instanceof SearchTask) {
            toAddCancellationCallbackForSBPState = searchBackpressureStates.get(SearchTask.class)::incrementCancellationCount;
        }
        List<Runnable> newOnCancelCallbacks = new ArrayList<>(taskCancellation.getOnCancelCallbacks());
        newOnCancelCallbacks.add(toAddCancellationCallbackForSBPState);
        return new TaskCancellation(task, taskCancellation.getReasons(), newOnCancelCallbacks);
    }

    private boolean shouldApply(TaskResourceUsageTrackerType trackerType) {
        return trackerApplyConditions.get(trackerType).apply(nodeDuressTrackers);
    }

    private List<TaskCancellation> addResourceTrackerBasedCancellations(
        TaskResourceUsageTrackerType type,
        List<TaskCancellation> taskCancellations,
        Map<Class<? extends SearchBackpressureTask>, List<CancellableTask>> cancellableTasks
    ) {
        for (Map.Entry<Class<? extends SearchBackpressureTask>, TaskResourceUsageTrackers> taskResourceUsageTrackers : taskTrackers
            .entrySet()) {
            final Optional<TaskResourceUsageTracker> taskResourceUsageTracker = taskResourceUsageTrackers.getValue().getTracker(type);
            final Class<? extends SearchBackpressureTask> taskType = taskResourceUsageTrackers.getKey();

            taskResourceUsageTracker.ifPresent(
                tracker -> taskCancellations.addAll(tracker.getTaskCancellations(cancellableTasks.get(taskType)))
            );
        }

        return taskCancellations;
    }

    /**
     * Method to reduce the taskCancellations into unique bunch
     * @param taskCancellations all task cancellations
     * @return unique task cancellations
     */
    private List<TaskCancellation> mergeTaskCancellations(final List<TaskCancellation> taskCancellations) {
        final Map<Long, TaskCancellation> uniqueTaskCancellations = new HashMap<>();

        for (TaskCancellation taskCancellation : taskCancellations) {
            final long taskId = taskCancellation.getTask().getId();
            uniqueTaskCancellations.put(taskId, uniqueTaskCancellations.getOrDefault(taskId, taskCancellation).merge(taskCancellation));
        }

        return new ArrayList<>(uniqueTaskCancellations.values());
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
        return nodeDuressTrackers.isNodeInDuress();
    }

    /*
      Returns true if the increase in heap usage is due to search requests.
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
            .filter(workloadGroupService::shouldSBPHandle)
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
    public static TaskResourceUsageTrackers getTrackers(
        LongSupplier cpuThresholdSupplier,
        DoubleSupplier heapVarianceSupplier,
        DoubleSupplier heapPercentThresholdSupplier,
        int heapMovingAverageWindowSize,
        LongSupplier ElapsedTimeNanosSupplier,
        DoubleSupplier nativeMemoryPercentThresholdSupplier,
        ClusterSettings clusterSettings,
        Setting<Integer> heapWindowSizeSetting
    ) {
        TaskResourceUsageTrackers trackers = new TaskResourceUsageTrackers();
        trackers.addTracker(new CpuUsageTracker(cpuThresholdSupplier), TaskResourceUsageTrackerType.CPU_USAGE_TRACKER);
        if (isHeapTrackingSupported()) {
            trackers.addTracker(
                new HeapUsageTracker(
                    heapVarianceSupplier,
                    heapPercentThresholdSupplier,
                    heapMovingAverageWindowSize,
                    clusterSettings,
                    heapWindowSizeSetting
                ),
                TaskResourceUsageTrackerType.HEAP_USAGE_TRACKER
            );
        } else {
            logger.warn("heap size couldn't be determined");
        }
        trackers.addTracker(
            new ElapsedTimeTracker(ElapsedTimeNanosSupplier, System::nanoTime),
            TaskResourceUsageTrackerType.ELAPSED_TIME_TRACKER
        );
        // Native-memory tracker pulls per-task bytes from a snapshot installed via
        // NativeMemoryUsageTracker#setSnapshotSupplier (typically by a backend plugin).
        // Per-task evaluation reads from the snapshot map — no FFI call per task. The
        // service calls tracker.refresh() once per tick to rebuild the snapshot.
        //
        // Gate on isNativeTrackingSupported() so we only register the tracker on platforms
        // where the duress probe and total-physical-memory readings are meaningful.
        if (isNativeTrackingSupported()) {
            trackers.addTracker(
                new NativeMemoryUsageTracker(nativeMemoryPercentThresholdSupplier),
                TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER
            );
        } else {
            logger.warn("native memory tracking not supported on this platform");
        }
        return trackers;
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
        TaskResourceUsageTrackers trackers = taskTrackers.get(taskType);
        for (TaskResourceUsageTracker tracker : trackers.all()) {
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
        // One refresh per tracker before stats() iterates activeTasks twice (max + avg).
        // Mirrors the refresh pass in doRun() so both paths stay consistent — a tracker
        // that caches an expensive cross-boundary read never has to re-read per task.
        logger.info(
            "[nativemem-bp] nodeStats: refreshing all trackers — searchTasks={}, searchShardTasks={}",
            searchTasks.size(),
            searchShardTasks.size()
        );
        for (TaskResourceUsageTrackers trackers : taskTrackers.values()) {
            for (TaskResourceUsageTracker tracker : trackers.all()) {
                tracker.refresh();
            }
        }
        SearchTaskStats searchTaskStats = new SearchTaskStats(
            searchBackpressureStates.get(SearchTask.class).getCancellationCount(),
            searchBackpressureStates.get(SearchTask.class).getLimitReachedCount(),
            searchBackpressureStates.get(SearchTask.class).getCompletionCount(),
            taskTrackers.get(SearchTask.class)
                .all()
                .stream()
                .collect(Collectors.toUnmodifiableMap(t -> TaskResourceUsageTrackerType.fromName(t.name()), t -> t.stats(searchTasks)))
        );

        SearchShardTaskStats searchShardTaskStats = new SearchShardTaskStats(
            searchBackpressureStates.get(SearchShardTask.class).getCancellationCount(),
            searchBackpressureStates.get(SearchShardTask.class).getLimitReachedCount(),
            searchBackpressureStates.get(SearchShardTask.class).getCompletionCount(),
            taskTrackers.get(SearchShardTask.class)
                .all()
                .stream()
                .collect(Collectors.toUnmodifiableMap(t -> TaskResourceUsageTrackerType.fromName(t.name()), t -> t.stats(searchShardTasks)))
        );

        return new SearchBackpressureStats(searchTaskStats, searchShardTaskStats, getSettings().getMode());
    }
}
