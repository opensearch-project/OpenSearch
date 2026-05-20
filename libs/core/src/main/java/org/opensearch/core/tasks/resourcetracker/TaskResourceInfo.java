/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.tasks.resourcetracker;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Task resource usage information with minimal information about the task
 * <p>
 * Writeable TaskResourceInfo objects are used to represent resource usage
 * information of running tasks, which can be propagated to coordinator node
 * to infer query-level resource usage
 *
 *  @opensearch.api
 */
@PublicApi(since = "2.15.0")
public class TaskResourceInfo implements Writeable, ToXContentObject {
    private final String action;
    private final long taskId;
    private final long parentTaskId;
    private final String nodeId;
    private final TaskResourceUsage taskResourceUsage;

    private static final ParseField ACTION = new ParseField("action");
    private static final ParseField TASK_ID = new ParseField("taskId");
    private static final ParseField PARENT_TASK_ID = new ParseField("parentTaskId");
    private static final ParseField NODE_ID = new ParseField("nodeId");
    private static final ParseField TASK_RESOURCE_USAGE = new ParseField("taskResourceUsage");

    public TaskResourceInfo(
        final String action,
        final long taskId,
        final long parentTaskId,
        final String nodeId,
        final TaskResourceUsage taskResourceUsage
    ) {
        this.action = action;
        this.taskId = taskId;
        this.parentTaskId = parentTaskId;
        this.nodeId = nodeId;
        this.taskResourceUsage = taskResourceUsage;
    }

    public static final ConstructingObjectParser<TaskResourceInfo, Void> PARSER = new ConstructingObjectParser<>(
        "task_resource_info",
        a -> new Builder().setAction((String) a[0])
            .setTaskId((Long) a[1])
            .setParentTaskId((Long) a[2])
            .setNodeId((String) a[3])
            .setTaskResourceUsage((TaskResourceUsage) a[4])
            .build()
    );

    static {
        PARSER.declareString(constructorArg(), ACTION);
        PARSER.declareLong(constructorArg(), TASK_ID);
        PARSER.declareLong(constructorArg(), PARENT_TASK_ID);
        PARSER.declareString(constructorArg(), NODE_ID);
        PARSER.declareObject(constructorArg(), TaskResourceUsage.PARSER, TASK_RESOURCE_USAGE);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACTION.getPreferredName(), this.action);
        builder.field(TASK_ID.getPreferredName(), this.taskId);
        builder.field(PARENT_TASK_ID.getPreferredName(), this.parentTaskId);
        builder.field(NODE_ID.getPreferredName(), this.nodeId);
        builder.startObject(TASK_RESOURCE_USAGE.getPreferredName());
        this.taskResourceUsage.toXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * Builder for {@link TaskResourceInfo}
     */
    public static class Builder {
        private TaskResourceUsage taskResourceUsage;
        private String action;
        private long taskId;
        private long parentTaskId;
        private String nodeId;

        public Builder setTaskResourceUsage(final TaskResourceUsage taskResourceUsage) {
            this.taskResourceUsage = taskResourceUsage;
            return this;
        }

        public Builder setAction(final String action) {
            this.action = action;
            return this;
        }

        public Builder setTaskId(final long taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder setParentTaskId(final long parentTaskId) {
            this.parentTaskId = parentTaskId;
            return this;
        }

        public Builder setNodeId(final String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public TaskResourceInfo build() {
            return new TaskResourceInfo(action, taskId, parentTaskId, nodeId, taskResourceUsage);
        }
    }

    /**
     * Read task info from a stream.
     *
     * @param in StreamInput to read
     * @return {@link TaskResourceInfo}
     * @throws IOException IOException
     */
    public static TaskResourceInfo readFromStream(StreamInput in) throws IOException {
        return new TaskResourceInfo.Builder().setAction(in.readString())
            .setTaskId(in.readLong())
            .setParentTaskId(in.readLong())
            .setNodeId(in.readString())
            .setTaskResourceUsage(TaskResourceUsage.readFromStream(in))
            .build();
    }

    /**
     * Get TaskResourceUsage
     *
     * @return taskResourceUsage
     */
    public TaskResourceUsage getTaskResourceUsage() {
        return taskResourceUsage;
    }

    /**
     * Get parent task id
     *
     * @return parent task id
     */
    public long getParentTaskId() {
        return parentTaskId;
    }

    /**
     * Get task id
     * @return task id
     */
    public long getTaskId() {
        return taskId;
    }

    /**
     * Get node id
     * @return node id
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Get task action
     * @return task action
     */
    public String getAction() {
        return action;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(action);
        out.writeLong(taskId);
        out.writeLong(parentTaskId);
        out.writeString(nodeId);
        taskResourceUsage.writeTo(out);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != TaskResourceInfo.class) {
            return false;
        }
        TaskResourceInfo other = (TaskResourceInfo) obj;
        return action.equals(other.action)
            && taskId == other.taskId
            && parentTaskId == other.parentTaskId
            && Objects.equals(nodeId, other.nodeId)
            && taskResourceUsage.equals(other.taskResourceUsage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(action, taskId, parentTaskId, nodeId, taskResourceUsage);
    }
}
