/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.node.tasks.get;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.tasks.TaskId;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A request to get node tasks
 *
 * @opensearch.internal
 */
public class GetTaskRequest extends ActionRequest {
    private TaskId taskId = TaskId.EMPTY_TASK_ID;
    private boolean waitForCompletion = false;
    private TimeValue timeout = null;

    /**
     * Get the TaskId to look up.
     */
    public GetTaskRequest() {}

    public GetTaskRequest(StreamInput in) throws IOException {
        super(in);
        taskId = TaskId.readFromStream(in);
        timeout = in.readOptionalTimeValue();
        waitForCompletion = in.readBoolean();
    }

    public TaskId getTaskId() {
        return taskId;
    }

    /**
     * Set the TaskId to look up. Required.
     */
    public GetTaskRequest setTaskId(TaskId taskId) {
        this.taskId = taskId;
        return this;
    }

    /**
     * Should this request wait for all found tasks to complete?
     */
    public boolean getWaitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Should this request wait for all found tasks to complete?
     */
    public GetTaskRequest setWaitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Timeout to wait for any async actions this request must take. It must take anywhere from 0 to 2.
     */
    public TimeValue getTimeout() {
        return timeout;
    }

    /**
     * Timeout to wait for any async actions this request must take. It must take anywhere from 0 to 2.
     */
    public GetTaskRequest setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    GetTaskRequest nodeRequest(String thisNodeId, long thisTaskId) {
        GetTaskRequest copy = new GetTaskRequest();
        copy.setParentTask(thisNodeId, thisTaskId);
        copy.setTaskId(taskId);
        copy.setTimeout(timeout);
        copy.setWaitForCompletion(waitForCompletion);
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
        out.writeOptionalTimeValue(timeout);
        out.writeBoolean(waitForCompletion);
    }
}
