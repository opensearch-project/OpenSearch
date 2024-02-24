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
        TaskResourceInfo info = new TaskResourceInfo();
        info.taskResourceUsage = task.getTotalResourceStats();
        info.taskId = task.getId();
        info.action = task.getAction();
        info.parentTaskId = task.getParentTaskId().getId();
        long parentTaskId = task.getParentTaskId().getId();
        if (parentTaskId == -1) {
            parentTaskId = task.getId();
        }

        this.queryInsightsService.taskStatusMap.get(parentTaskId).decrementAndGet();
        queryInsightsService.taskRecordsQueue.add(info);
//        System.out.println(String.format("id = %s, parent = %s, resource = %s, action = %s, total CPU and MEM: %s, %s", task.getId(), task.getParentTaskId(), task.getResourceStats(), task.getAction(),task.getTotalResourceUtilization(ResourceStats.CPU),task.getTotalResourceUtilization(ResourceStats.MEMORY) ));
    }

    @Override
    public void onTaskStarts(Task task) {
        long parentId = task.getParentTaskId().getId();
        if (parentId == -1) {
            parentId = task.getId();
        }
        this.queryInsightsService.taskStatusMap.putIfAbsent(parentId, new AtomicInteger(0));
        this.queryInsightsService.taskStatusMap.get(parentId).incrementAndGet();
    }
}
