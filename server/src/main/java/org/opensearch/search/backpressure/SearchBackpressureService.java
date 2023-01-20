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
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.tasks.TaskResourceTrackingService.TaskCompletionListener;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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
        SearchTaskSettings.Listener,
        SearchShardTaskSettings.Listener {
    private static final Logger logger = LogManager.getLogger(SearchBackpressureService.class);

    private volatile Scheduler.Cancellable scheduledFuture;

    private final SearchBackpressureSettings settings;
    private final TaskResourceTrackingService taskResourceTrackingService;
    private final ThreadPool threadPool;
    private final LongSupplier timeNanosSupplier;

    private final List<NodeDuressTracker> nodeDuressTrackers;
    private final Map<Class<? extends SearchBackpressureTask>, List<TaskResourceUsageTracker>> taskTrackers;

    private final Map<Class<? extends SearchBackpressureTask>, AtomicReference<TokenBucket>> rateLimiters;
    private final Map<Class<? extends SearchBackpressureTask>, AtomicReference<TokenBucket>> ratioLimiters;

    private final Map<Class<? extends SearchBackpressureTask>, SearchBackpressureState> searchBackpressureStates;

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
            List.of(
                new CpuUsageTracker(settings.getSearchTaskSettings()::getCpuTimeNanosThreshold),
                new HeapUsageTracker(
                    settings.getSearchTaskSettings()::getHeapVarianceThreshold,
                    settings.getSearchTaskSettings()::getHeapBytesThreshold,
                    settings.getSearchTaskSettings().getHeapMovingAverageWindowSize(),
                    settings.getClusterSettings(),
                    SearchTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE
                ),
                new ElapsedTimeTracker(settings.getSearchTaskSettings()::getElapsedTimeNanosThreshold, System::nanoTime)
            ),
            List.of(
                new CpuUsageTracker(settings.getSearchShardTaskSettings()::getCpuTimeNanosThreshold),
                new HeapUsageTracker(
                    settings.getSearchShardTaskSettings()::getHeapVarianceThreshold,
                    settings.getSearchShardTaskSettings()::getHeapBytesThreshold,
                    settings.getSearchShardTaskSettings().getHeapMovingAverageWindowSize(),
                    settings.getClusterSettings(),
                    SearchShardTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE
                ),
                new ElapsedTimeTracker(settings.getSearchShardTaskSettings()::getElapsedTimeNanosThreshold, System::nanoTime)
            )
        );
    }

    public SearchBackpressureService(
        SearchBackpressureSettings settings,
        TaskResourceTrackingService taskResourceTrackingService,
        ThreadPool threadPool,
        LongSupplier timeNanosSupplier,
        List<NodeDuressTracker> nodeDuressTrackers,
        List<TaskResourceUsageTracker> searchTaskTrackers,
        List<TaskResourceUsageTracker> searchShardTaskTrackers
    ) {
        this.settings = settings;
        this.settings.getSearchTaskSettings().addListener(this);
        this.settings.getSearchShardTaskSettings().addListener(this);
        this.taskResourceTrackingService = taskResourceTrackingService;
        this.taskResourceTrackingService.addTaskCompletionListener(this);
        this.threadPool = threadPool;
        this.timeNanosSupplier = timeNanosSupplier;
        this.nodeDuressTrackers = nodeDuressTrackers;

        this.searchBackpressureStates = Map.of(
            SearchTask.class,
            new SearchBackpressureState(),
            SearchShardTask.class,
            new SearchBackpressureState()
        );

        this.taskTrackers = Map.of(SearchTask.class, searchTaskTrackers, SearchShardTask.class, searchShardTaskTrackers);

        this.rateLimiters = Map.of(
            SearchTask.class,
            new AtomicReference<>(
                new TokenBucket(
                    timeNanosSupplier,
                    getSettings().getSearchTaskSettings().getCancellationRateNanos(),
                    getSettings().getSearchTaskSettings().getCancellationBurst()
                )
            ),
            SearchShardTask.class,
            new AtomicReference<>(
                new TokenBucket(
                    timeNanosSupplier,
                    getSettings().getSearchShardTaskSettings().getCancellationRateNanos(),
                    getSettings().getSearchShardTaskSettings().getCancellationBurst()
                )
            )
        );

        this.ratioLimiters = Map.of(
            SearchTask.class,
            new AtomicReference<>(
                new TokenBucket(
                    this::getSearchTaskCompletionCount,
                    getSettings().getSearchTaskSettings().getCancellationRatio(),
                    getSettings().getSearchTaskSettings().getCancellationBurst()
                )
            ),
            SearchShardTask.class,
            new AtomicReference<>(
                new TokenBucket(
                    this::getSearchShardTaskCompletionCount,
                    getSettings().getSearchShardTaskSettings().getCancellationRatio(),
                    getSettings().getSearchShardTaskSettings().getCancellationBurst()
                )
            )
        );
    }

    private long getSearchTaskCompletionCount() {
        return searchBackpressureStates.get(SearchTask.class).getCompletionCount();
    }

    private long getSearchShardTaskCompletionCount() {
        return searchBackpressureStates.get(SearchShardTask.class).getCompletionCount();
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
        if (isHeapUsageDominatedBySearch(searchTasks, getSettings().getSearchTaskSettings().getTotalHeapBytesThreshold())) {
            cancellableTasks.addAll(searchTasks);
        }

        // Check if increase in heap usage is due to SearchShardTasks
        if (isHeapUsageDominatedBySearch(searchShardTasks, getSettings().getSearchShardTaskSettings().getTotalHeapBytesThreshold())) {
            cancellableTasks.addAll(searchShardTasks);
        }

        // none of the task type is breaching the heap usage thresholds and hence we do not cancel any tasks
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

            Class<? extends SearchBackpressureTask> taskType = getTaskType(taskCancellation.getTask());

            // Independently remove tokens from both token buckets.
            boolean rateLimitReached = rateLimiters.get(taskType).get().request() == false;
            boolean ratioLimitReached = ratioLimiters.get(taskType).get().request() == false;

            // Stop cancelling tasks if there are no tokens in either of the two token buckets.
            if (rateLimitReached && ratioLimitReached) {
                logger.debug("task cancellation limit reached");
                SearchBackpressureState searchBackpressureState = searchBackpressureStates.get(taskType);
                if (searchBackpressureState != null) {
                    searchBackpressureState.incrementLimitReachedCount();
                }
                break;
            }

            taskCancellation.cancel();
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
            throw new IllegalArgumentException("");
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

        SearchBackpressureState searchBackpressureState = searchBackpressureStates.get(taskType);
        if (searchBackpressureState != null) {
            callbacks.add(searchBackpressureState::incrementCancellationCount);
        }

        return new TaskCancellation(task, reasons, callbacks);
    }

    /**
     * Returns a list of TaskCancellations sorted by descending order of their cancellation scores.
     */
    List<TaskCancellation> getTaskCancellations(List<? extends CancellableTask> tasks) {
        List<TaskCancellation> t = tasks.stream()
            .map(this::getTaskCancellation)
            .filter(TaskCancellation::isEligibleForCancellation)
            .sorted(Comparator.reverseOrder())
            .collect(Collectors.toUnmodifiableList());
        return t;
    }

    SearchBackpressureSettings getSettings() {
        return settings;
    }

    SearchBackpressureState getSearchBackpressureTaskStats(Class<? extends SearchBackpressureTask> taskType) {
        return searchBackpressureStates.get(taskType);
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
            SearchBackpressureState searchBackpressureState = searchBackpressureStates.get(taskType);
            if (searchBackpressureState != null) {
                searchBackpressureState.incrementCompletionCount();
            }
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
    public void onCancellationRatioSearchTaskChanged() {
        ratioLimiters.get(SearchTask.class)
            .set(
                new TokenBucket(
                    this::getSearchTaskCompletionCount,
                    getSettings().getSearchTaskSettings().getCancellationRatio(),
                    getSettings().getSearchTaskSettings().getCancellationBurst()
                )
            );
    }

    @Override
    public void onCancellationRateSearchTaskChanged() {
        rateLimiters.get(SearchTask.class)
            .set(
                new TokenBucket(
                    timeNanosSupplier,
                    getSettings().getSearchTaskSettings().getCancellationRateNanos(),
                    getSettings().getSearchTaskSettings().getCancellationBurst()
                )
            );
    }

    @Override
    public void onCancellationBurstSearchTaskChanged() {
        onCancellationRatioSearchTaskChanged();
        onCancellationRateSearchTaskChanged();
    }

    @Override
    public void onCancellationRatioSearchShardTaskChanged() {
        ratioLimiters.get(SearchShardTask.class)
            .set(
                new TokenBucket(
                    this::getSearchShardTaskCompletionCount,
                    getSettings().getSearchShardTaskSettings().getCancellationRatio(),
                    getSettings().getSearchShardTaskSettings().getCancellationBurst()
                )
            );
    }

    @Override
    public void onCancellationRateSearchShardTaskChanged() {
        rateLimiters.get(SearchShardTask.class)
            .set(
                new TokenBucket(
                    timeNanosSupplier,
                    getSettings().getSearchShardTaskSettings().getCancellationRateNanos(),
                    getSettings().getSearchShardTaskSettings().getCancellationBurst()
                )
            );
    }

    @Override
    public void onCancellationBurstSearchShardTaskChanged() {
        onCancellationRatioSearchShardTaskChanged();
        onCancellationRateSearchShardTaskChanged();
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
