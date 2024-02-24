/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.listener;

import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.model.SearchTaskMetadata;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.tasks.TaskResourceTrackingService.TaskCompletionListener;
import org.opensearch.tasks.TaskResourceTrackingService.TaskStartListener;

import java.util.concurrent.atomic.AtomicInteger;

public class ResourceTrackingListener implements TaskCompletionListener, TaskStartListener {

    private final TaskResourceTrackingService taskResourceTrackingService;
    private final QueryInsightsService queryInsightsService;

    public ResourceTrackingListener (
        QueryInsightsService queryInsightsService,
        TaskResourceTrackingService taskResourceTrackingService
    ) {
        this.queryInsightsService = queryInsightsService;
        this.taskResourceTrackingService = taskResourceTrackingService;
        this.taskResourceTrackingService.addTaskCompletionListener(this);
        this.taskResourceTrackingService.addTaskStartListener(this);
    }
    @Override
    public void onTaskCompleted(Task task) {
        long taskGroupId = task.getParentTaskId().getId();
        if (taskGroupId == -1) {
            taskGroupId = task.getId();
        }
        SearchTaskMetadata info = new SearchTaskMetadata(
            task.getAction(), task.getId(), taskGroupId, task.getTotalResourceStats()
        );

        int pendingTaskCount = this.queryInsightsService.taskStatusMap.get(taskGroupId).decrementAndGet();
        if (pendingTaskCount == 0) {
            this.queryInsightsService.taskStatusMap.remove(taskGroupId);
        }
        queryInsightsService.taskRecordsQueue.add(info);
        System.out.println(String.format("id = %s, parent = %s, resource = %s, action = %s, total CPU and MEM: %s, %s", task.getId(), task.getParentTaskId(), task.getResourceStats(), task.getAction(),task.getTotalResourceUtilization(ResourceStats.CPU),task.getTotalResourceUtilization(ResourceStats.MEMORY) ));
    }

    @Override
    public void onTaskStarts(Task task) {
        long taskGroupId = task.getParentTaskId().getId();
        if (taskGroupId == -1) {
            taskGroupId = task.getId();
        }
        this.queryInsightsService.taskStatusMap.putIfAbsent(taskGroupId, new AtomicInteger(0));
        this.queryInsightsService.taskStatusMap.get(taskGroupId).incrementAndGet();
    }
}
