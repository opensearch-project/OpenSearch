/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.tasks.resourcetracker;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

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
@PublicApi(since = "2.15.0")
public class TaskResourceInfo implements Writeable {
    private TaskResourceUsage taskResourceUsage;
    private String action;
    private long taskId;
    private long parentTaskId;

    public TaskResourceInfo() {
        this.action = "";
        this.taskId = -1L;
        this.taskResourceUsage = new TaskResourceUsage(0, 0);
    }

    public TaskResourceInfo(String action, long taskId, long parentTaskId, TaskResourceUsage taskResourceUsage) {
        this.action = action;
        this.taskId = taskId;
        this.parentTaskId = parentTaskId;
        this.taskResourceUsage = taskResourceUsage;
    }

    /**
     * Read task info from a stream.
     *
     * @param in StreamInput to read
     * @return {@link TaskResourceInfo}
     * @throws IOException IOException
     */
    public static TaskResourceInfo readFromStream(StreamInput in) throws IOException {
        TaskResourceInfo info = new TaskResourceInfo();
        info.action = in.readString();
        info.taskId = in.readLong();
        info.taskResourceUsage = TaskResourceUsage.readFromStream(in);
        info.parentTaskId = in.readLong();
        return info;
    }

    /**
     * Get taskResourceUsage
     *
     * @return taskResourceUsage
     */
    public TaskResourceUsage getTaskResourceUsage() {
        return taskResourceUsage;
    }

    /**
     * Set taskResourceUsage
     * @param taskResourceUsage the updated taskResourceUsage
     */
    public void setTaskResourceUsage(TaskResourceUsage taskResourceUsage) {
        this.taskResourceUsage = taskResourceUsage;
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
     * Set parent task id
     * @param parentTaskId parent task id
     */
    public void setParentTaskId(long parentTaskId) {
        this.parentTaskId = parentTaskId;
    }

    /**
     * Get task id
     * @return task id
     */
    public long getTaskId() {
        return taskId;
    }

    /**
     * Set task id
     * @param taskId task id
     */
    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    /**
     * Get task action
     * @return task action
     */
    public String getAction() {
        return action;
    }

    /**
     * Set task action
     * @param action task action
     */
    public void setAction(String action) {
        this.action = action;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(action);
        out.writeLong(taskId);
        taskResourceUsage.writeTo(out);
        out.writeLong(parentTaskId);
    }

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
