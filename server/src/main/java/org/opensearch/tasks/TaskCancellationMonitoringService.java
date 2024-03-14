/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This monitoring service is responsible to track long-running(defined by a threshold) cancelled tasks as part of
 * node stats.
 */
public class TaskCancellationMonitoringService extends AbstractLifecycleComponent implements TaskManager.TaskEventListeners {

    private static final Logger logger = LogManager.getLogger(TaskCancellationMonitoringService.class);
    private final static List<Class<? extends CancellableTask>> TASKS_TO_TRACK = Arrays.asList(SearchShardTask.class);

    private volatile Scheduler.Cancellable scheduledFuture;
    private final ThreadPool threadPool;
    private final TaskManager taskManager;
    /**
     * This is to keep track of currently running cancelled tasks. This is needed to accurately calculate cumulative
     * sum(from genesis) of cancelled tasks which have been running beyond a threshold and avoid double count
     * problem.
     * For example:
     * A task M was cancelled at some point of time and continues to run for long. This Monitoring service sees this
     * M for the first time and adds it as part of stats. In next iteration of monitoring service, it might see
     * this M(if still running) again, but using below map we will not double count this task as part of our cumulative
     * metric.
     */
    private final Map<Long, Boolean> cancelledTaskTracker;
    /**
     *  This map holds statistics for each cancellable task type.
     */
    private final Map<Class<? extends CancellableTask>, TaskCancellationStatsHolder> cancellationStatsHolder;
    private final TaskCancellationMonitoringSettings taskCancellationMonitoringSettings;

    public TaskCancellationMonitoringService(
        ThreadPool threadPool,
        TaskManager taskManager,
        TaskCancellationMonitoringSettings taskCancellationMonitoringSettings
    ) {
        this.threadPool = threadPool;
        this.taskManager = taskManager;
        this.taskCancellationMonitoringSettings = taskCancellationMonitoringSettings;
        this.cancelledTaskTracker = new ConcurrentHashMap<>();
        cancellationStatsHolder = TASKS_TO_TRACK.stream()
            .collect(Collectors.toConcurrentMap(task -> task, task -> new TaskCancellationStatsHolder()));
        taskManager.addTaskEventListeners(this);
    }

    void doRun() {
        if (!taskCancellationMonitoringSettings.isEnabled() || this.cancelledTaskTracker.isEmpty()) {
            return;
        }
        Map<Class<? extends CancellableTask>, List<CancellableTask>> taskCancellationListByType = getCurrentRunningTasksPostCancellation();
        taskCancellationListByType.forEach((key, value) -> {
            long uniqueTasksRunningCount = value.stream().filter(task -> {
                if (this.cancelledTaskTracker.containsKey(task.getId()) && !this.cancelledTaskTracker.get(task.getId())) {
                    // Mark it as seen by the stats logic.
                    this.cancelledTaskTracker.put(task.getId(), true);
                    return true;
                } else {
                    return false;
                }
            }).count();
            cancellationStatsHolder.get(key).totalLongRunningCancelledTaskCount.inc(uniqueTasksRunningCount);
        });
    }

    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
            try {
                doRun();
            } catch (Exception e) {
                logger.debug("Exception occurred in Task monitoring service", e);
            }
        }, taskCancellationMonitoringSettings.getInterval(), ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

    // For testing
    protected Map<Long, Boolean> getCancelledTaskTracker() {
        return this.cancelledTaskTracker;
    }

    /**
     * Invoked when a task is completed. This helps us to disable monitoring service when there are no cancelled tasks
     * running to avoid wasteful work.
     * @param task task which got completed.
     */
    @Override
    public void onTaskCompleted(Task task) {
        if (!TASKS_TO_TRACK.contains(task.getClass())) {
            return;
        }
        this.cancelledTaskTracker.entrySet().removeIf(entry -> entry.getKey() == task.getId());
    }

    /**
     * Invoked when a task is cancelled. This is to keep track of tasks being cancelled. More importantly also helps
     * us to enable this monitoring service only when needed.
     * @param task task which got cancelled.
     */
    @Override
    public void onTaskCancelled(CancellableTask task) {
        if (!TASKS_TO_TRACK.contains(task.getClass())) {
            return;
        }
        // Add task to tracker and mark it as not seen(false) yet by the stats logic.
        this.cancelledTaskTracker.putIfAbsent(task.getId(), false);
    }

    public TaskCancellationStats stats() {
        Map<Class<? extends CancellableTask>, List<CancellableTask>> currentRunningCancelledTasks =
            getCurrentRunningTasksPostCancellation();
        return new TaskCancellationStats(
            new SearchShardTaskCancellationStats(
                Optional.of(currentRunningCancelledTasks).map(mapper -> mapper.get(SearchShardTask.class)).map(List::size).orElse(0),
                cancellationStatsHolder.get(SearchShardTask.class).totalLongRunningCancelledTaskCount.count()
            )
        );
    }

    private Map<Class<? extends CancellableTask>, List<CancellableTask>> getCurrentRunningTasksPostCancellation() {
        long currentTimeInNanos = System.nanoTime();

        return taskManager.getCancellableTasks()
            .values()
            .stream()
            .filter(task -> TASKS_TO_TRACK.contains(task.getClass()))
            .filter(CancellableTask::isCancelled)
            .filter(task -> {
                long runningTimeSinceCancellationSeconds = TimeUnit.NANOSECONDS.toSeconds(
                    currentTimeInNanos - task.getCancellationStartTimeNanos()
                );
                return runningTimeSinceCancellationSeconds >= taskCancellationMonitoringSettings.getDuration().getSeconds();
            })
            .collect(Collectors.groupingBy(CancellableTask::getClass, Collectors.toList()));
    }

    /**
     * Holds stats related to monitoring service
     */
    public static class TaskCancellationStatsHolder {
        CounterMetric totalLongRunningCancelledTaskCount = new CounterMetric();
    }
}
