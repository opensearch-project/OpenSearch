/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*
*/

package org.opensearch.tasks;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.ProtobufWriteable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Information about a currently running task.
* <p>
* Tasks are used for communication with transport actions. As a result, they can contain callback
* references as well as mutable state. That makes it impractical to send tasks over transport channels
* and use in APIs. Instead, immutable and writeable ProtobufTaskInfo objects are used to represent
* snapshot information about currently running tasks.
*
* @opensearch.internal
*/
public final class ProtobufTaskInfo implements ProtobufWriteable, ToXContentFragment {
    private final ProtobufTaskId taskId;

    private final String type;

    private final String action;

    private final String description;

    private final long startTime;

    private final long runningTimeNanos;

    private final ProtobufTask.Status status;

    private final boolean cancellable;

    private final boolean cancelled;

    private final ProtobufTaskId parentTaskId;

    private final Map<String, String> headers;

    private final ProtobufTaskResourceStats resourceStats;

    public ProtobufTaskInfo(
        ProtobufTaskId taskId,
        String type,
        String action,
        String description,
        ProtobufTask.Status status,
        long startTime,
        long runningTimeNanos,
        boolean cancellable,
        boolean cancelled,
        ProtobufTaskId parentTaskId,
        Map<String, String> headers,
        ProtobufTaskResourceStats resourceStats
    ) {
        if (cancellable == false && cancelled == true) {
            throw new IllegalArgumentException("task cannot be cancelled");
        }
        this.taskId = taskId;
        this.type = type;
        this.action = action;
        this.description = description;
        this.status = status;
        this.startTime = startTime;
        this.runningTimeNanos = runningTimeNanos;
        this.cancellable = cancellable;
        this.cancelled = cancelled;
        this.parentTaskId = parentTaskId;
        this.headers = headers;
        this.resourceStats = resourceStats;
    }

    public ProtobufTaskId getTaskId() {
        return taskId;
    }

    public long getId() {
        return taskId.getId();
    }

    public String getType() {
        return type;
    }

    public String getAction() {
        return action;
    }

    public String getDescription() {
        return description;
    }

    /**
     * The status of the running task. Only available if TaskInfos were build
    * with the detailed flag.
    */
    public ProtobufTask.Status getStatus() {
        return status;
    }

    /**
     * Returns the task start time
    */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns the task running time
    */
    public long getRunningTimeNanos() {
        return runningTimeNanos;
    }

    /**
     * Returns true if the task supports cancellation
    */
    public boolean isCancellable() {
        return cancellable;
    }

    /**
     * Returns true if the task has been cancelled
    */
    public boolean isCancelled() {
        return cancelled;
    }

    /**
     * Returns the parent task id
    */
    public ProtobufTaskId getParentTaskId() {
        return parentTaskId;
    }

    /**
     * Returns the task headers
    */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Returns the task resource information
    */
    public ProtobufTaskResourceStats getResourceStats() {
        return resourceStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("node", taskId.getNodeId());
        builder.field("id", taskId.getId());
        builder.field("type", type);
        builder.field("action", action);
        if (status != null) {
            builder.field("status", status, params);
        }
        if (description != null) {
            builder.field("description", description);
        }
        builder.timeField("start_time_in_millis", "start_time", startTime);
        if (builder.humanReadable()) {
            builder.field("running_time", new TimeValue(runningTimeNanos, TimeUnit.NANOSECONDS).toString());
        }
        builder.field("running_time_in_nanos", runningTimeNanos);
        builder.field("cancellable", cancellable);
        builder.field("cancelled", cancelled);
        if (parentTaskId.isSet()) {
            builder.field("parent_task_id", parentTaskId.toString());
        }
        builder.startObject("headers");
        for (Map.Entry<String, String> attribute : headers.entrySet()) {
            builder.field(attribute.getKey(), attribute.getValue());
        }
        builder.endObject();
        if (resourceStats != null) {
            builder.startObject("resource_stats");
            resourceStats.toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'writeTo'");
    }
}
