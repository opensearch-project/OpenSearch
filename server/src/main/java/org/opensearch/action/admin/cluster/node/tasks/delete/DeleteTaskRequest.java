/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks.delete;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A request to delete a stored completed task result.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.8.0")
public class DeleteTaskRequest extends ActionRequest {
    private TaskId taskId = TaskId.EMPTY_TASK_ID;

    public DeleteTaskRequest() {}

    public DeleteTaskRequest(StreamInput in) throws IOException {
        super(in);
        taskId = TaskId.readFromStream(in);
    }

    public TaskId getTaskId() {
        return taskId;
    }

    /**
     * Set the TaskId to delete. Required.
     */
    public DeleteTaskRequest setTaskId(TaskId taskId) {
        this.taskId = taskId;
        return this;
    }

    DeleteTaskRequest nodeRequest(String thisNodeId, long thisTaskId) {
        DeleteTaskRequest copy = new DeleteTaskRequest();
        copy.setParentTask(thisNodeId, thisTaskId);
        copy.setTaskId(taskId);
        return copy;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (false == getTaskId().isSet()) {
            validationException = addValidationError("task id is required", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        taskId.writeTo(out);
    }
}
