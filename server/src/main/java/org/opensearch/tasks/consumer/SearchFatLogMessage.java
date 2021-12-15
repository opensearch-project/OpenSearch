/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks.consumer;

import org.opensearch.common.logging.OpenSearchLogMessage;
import org.opensearch.tasks.TaskStatsContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class SearchFatLogMessage extends OpenSearchLogMessage {
    SearchFatLogMessage(TaskStatsContext context) {
        super(prepareMap(context), message(context));
    }

    private static Map<String, Object> prepareMap(TaskStatsContext context) {
        Map<String, Object> messageFields = new HashMap<>();
        messageFields.put("taskId", context.getTaskId());
        messageFields.put("type", context.getType());
        messageFields.put("action", context.getType());
        messageFields.put("description", context.getDescription());
        messageFields.put("start_time_millis", TimeUnit.NANOSECONDS.toMillis(context.getStartTime()));
        messageFields.put("took_millis", TimeUnit.NANOSECONDS.toMillis(context.getRunningTimeNanos()));
        messageFields.put("parentTaskId", context.getParentTaskId());
        messageFields.put("resource_stats", context.getAllStats());
        return messageFields;
    }

    // Message will be used in plaintext logs
    private static String message(TaskStatsContext context) {
        StringBuilder sb = new StringBuilder();
        sb.append("taskId[")
            .append(context.getTaskId())
            .append("], ")
            .append("type[")
            .append(context.getType())
            .append("], ")
            .append("action[")
            .append(context.getAction())
            .append("], ")
            .append("description[")
            .append(context.getDescription())
            .append("], ")
            .append("start_time_millis[")
            .append(TimeUnit.NANOSECONDS.toMillis(context.getStartTime()))
            .append("], ")
            .append("took_millis[")
            .append(TimeUnit.NANOSECONDS.toMillis(context.getRunningTimeNanos()))
            .append("], ")
            .append("resource_stats[")
            .append(context.getAllStats())
            .append("], ");
        return sb.toString();
    }
}
