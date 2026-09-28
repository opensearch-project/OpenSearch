/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.task;

import org.opensearch.common.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskAwareRequest;
import org.opensearch.tasks.TaskManager;

import java.util.Map;

/**
 * Lightweight {@link TaskAwareRequest} for registering an {@link AnalyticsQueryTask}
 * with {@link TaskManager}. Mirrors how {@code SearchRequest.createTask()} returns
 * a {@code SearchTask}.
 *
 * @opensearch.internal
 */
public final class AnalyticsQueryTaskRequest implements TaskAwareRequest {

    private final String queryId;
    private final TimeValue cancelAfterTimeInterval;
    private TaskId parentTaskId = TaskId.EMPTY_TASK_ID;

    public AnalyticsQueryTaskRequest(String queryId, @Nullable TimeValue cancelAfterTimeInterval) {
        this.queryId = queryId;
        this.cancelAfterTimeInterval = cancelAfterTimeInterval;
    }

    @Override
    public void setParentTask(TaskId taskId) {
        this.parentTaskId = taskId;
    }

    @Override
    public TaskId getParentTask() {
        return parentTaskId;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new AnalyticsQueryTask(id, type, action, queryId, parentTaskId, headers, cancelAfterTimeInterval);
    }
}
