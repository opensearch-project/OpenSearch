/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks.consumer;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.logging.OpenSearchLogMessage;
import org.opensearch.tasks.Task;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class TaskDetailsLogMessage extends OpenSearchLogMessage {
    TaskDetailsLogMessage(Task task) {
        super(prepareMap(task), message(task));
    }

    private static Map<String, Object> prepareMap(Task task) {
        Map<String, Object> messageFields = new HashMap<>();
        messageFields.put("taskId", task.getId());
        messageFields.put("type", task.getType());
        messageFields.put("action", task.getAction());
        messageFields.put("description", task.getDescription());
        messageFields.put("start_time_millis", TimeUnit.NANOSECONDS.toMillis(task.getStartTime()));
        messageFields.put("parentTaskId", task.getParentTaskId());
        messageFields.put("resource_stats", task.getResourceStats());
        messageFields.put("metadata", task instanceof SearchShardTask ? ((SearchShardTask) task).getTaskMetadata() : null);
        return messageFields;
    }

    // Message will be used in plaintext logs
    private static String message(Task task) {
        StringBuilder sb = new StringBuilder();
        sb.append("taskId:[")
            .append(task.getId())
            .append("], ")
            .append("type:[")
            .append(task.getType())
            .append("], ")
            .append("action:[")
            .append(task.getAction())
            .append("], ")
            .append("description:[")
            .append(task.getDescription())
            .append("], ")
            .append("start_time_millis:[")
            .append(TimeUnit.NANOSECONDS.toMillis(task.getStartTime()))
            .append("], ")
            .append("resource_stats:[")
            .append(task.getResourceStats())
            .append("], ")
            .append("metadata:[")
            .append(task instanceof SearchShardTask ? ((SearchShardTask) task).getTaskMetadata() : null)
            .append("]");
        return sb.toString();
    }
}
