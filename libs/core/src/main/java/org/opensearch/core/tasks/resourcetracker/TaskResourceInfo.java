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
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

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
@PublicApi(since = "2.1.0")
public class TaskResourceInfo implements Writeable, ToXContentFragment {
    public TaskResourceUsage taskResourceUsage;
    public String action;
    public long taskId;
    public long parentTaskId;

    public TaskResourceInfo() {
        this.action = "";
        this.taskId = -1L;
        this.taskResourceUsage = new TaskResourceUsage(0, 0);
    }

    public TaskResourceInfo(String action, long taskId, long cpuTimeInNanos, long memoryInBytes) {
        this.action = action;
        this.taskId = taskId;
        this.taskResourceUsage = new TaskResourceUsage(cpuTimeInNanos, memoryInBytes);
    }

    /**
     * Read from a stream.
     */
    public static TaskResourceInfo readFromStream(StreamInput in) throws IOException {
        TaskResourceInfo info = new TaskResourceInfo();
        info.action = in.readString();
        info.taskId = in.readLong();
        info.taskResourceUsage = TaskResourceUsage.readFromStream(in);
        info.parentTaskId = in.readLong();
        return info;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(action);
        out.writeLong(taskId);
        taskResourceUsage.writeTo(out);
        out.writeLong(parentTaskId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // TODO: change to a constant
        builder.field("Action", action);
        taskResourceUsage.toXContent(builder, params);
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, false, true);
    }

    // Implements equals and hashcode for testing
    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != TaskResourceInfo.class) {
            return false;
        }
        TaskResourceInfo other = (TaskResourceInfo) obj;
        return action.equals(other.action) && taskId == other.taskId && taskResourceUsage.equals(other.taskResourceUsage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(action, taskId, taskResourceUsage);
    }
}
