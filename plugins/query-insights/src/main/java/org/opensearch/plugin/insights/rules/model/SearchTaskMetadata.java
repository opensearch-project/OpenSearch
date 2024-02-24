/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

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
public class SearchTaskMetadata implements Writeable, ToXContentObject {
    public TaskResourceUsage taskResourceUsage;
    public String action;
    public long taskId;
    public long parentTaskId;


    /**
     * Create the TopQueries Object from StreamInput
     * @param in A {@link StreamInput} object.
     * @throws IOException IOException
     */
    public SearchTaskMetadata(final StreamInput in) throws IOException {
        action = in.readString();
        taskId = in.readLong();
        taskResourceUsage = TaskResourceUsage.readFromStream(in);
        parentTaskId = in.readLong();
    }

    /**
     * Create the TopQueries Object
     */
    public SearchTaskMetadata(String action, long taskId, long parentTaskId, TaskResourceUsage taskResourceUsage) {
        this.action = action;
        this.taskId = taskId;
        this.taskResourceUsage = taskResourceUsage;
        this.parentTaskId = parentTaskId;
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
        builder.startObject();
        // TODO: change to a constant
        builder.field("Action", action);
        builder.field("TaskId", taskId);
        builder.field("ParentTaskId", parentTaskId);
        taskResourceUsage.toXContent(builder, params);
        return builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, false, true);
    }

    // Implements equals and hashcode for testing
    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != SearchTaskMetadata.class) {
            return false;
        }
        SearchTaskMetadata other = (SearchTaskMetadata) obj;
        return action.equals(other.action) && taskId == other.taskId && taskResourceUsage.equals(other.taskResourceUsage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(action, taskId, taskResourceUsage);
    }
}
