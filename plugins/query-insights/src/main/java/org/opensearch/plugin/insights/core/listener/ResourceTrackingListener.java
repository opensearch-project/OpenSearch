/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.listener;

import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.tasks.TaskResourceTrackingService.TaskCompletionListener;
import org.opensearch.tasks.TaskResourceTrackingService.TaskStartListener;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Listener for task level resource usage
 */
public class ResourceTrackingListener implements TaskCompletionListener, TaskStartListener {
    private final QueryInsightsService queryInsightsService;

    /**
     * Constructor of ResourceTrackingListener
     * @param queryInsightsService queryInsightsService
     * @param taskResourceTrackingService taskResourceTrackingService
     */
    public ResourceTrackingListener(QueryInsightsService queryInsightsService, TaskResourceTrackingService taskResourceTrackingService) {
        this.queryInsightsService = queryInsightsService;
        taskResourceTrackingService.addTaskCompletionListener(this);
        taskResourceTrackingService.addTaskStartListener(this);
    }

    @Override
    public void onTaskCompleted(Task task) {
        TaskResourceInfo info = new TaskResourceInfo(
            task.getAction(),
            task.getId(),
            task.getParentTaskId().getId(),
            task.getTotalResourceStats()
        );
        long parentTaskId = task.getParentTaskId().getId();
        if (parentTaskId == -1) {
            parentTaskId = task.getId();
        }
        this.queryInsightsService.taskStatusMap.get(parentTaskId).decrementAndGet();
        queryInsightsService.taskRecordsQueue.add(info);
    }

    @Override
    public void onTaskStarts(Task task) {
        long parentTaskId = task.getParentTaskId().getId();
        if (parentTaskId == -1) {
            parentTaskId = task.getId();
        }
        this.queryInsightsService.taskStatusMap.putIfAbsent(parentTaskId, new AtomicInteger(0));
        this.queryInsightsService.taskStatusMap.get(parentTaskId).incrementAndGet();
    }
}
