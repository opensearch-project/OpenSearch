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

package org.opensearch.action.support.tasks;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CollectionUtils;
import org.opensearch.core.common.Strings;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A base class for task requests
 *
 * @opensearch.internal
 */
public class BaseTasksRequest<Request extends BaseTasksRequest<Request>> extends ActionRequest {

    public static final String[] ALL_ACTIONS = Strings.EMPTY_ARRAY;

    public static final String[] ALL_NODES = Strings.EMPTY_ARRAY;

    private String[] nodes = ALL_NODES;

    private TimeValue timeout;

    private String[] actions = ALL_ACTIONS;

    private TaskId parentTaskId = TaskId.EMPTY_TASK_ID;

    private TaskId taskId = TaskId.EMPTY_TASK_ID;

    // NOTE: This constructor is only needed, because the setters in this class,
    // otherwise it can be removed and above fields can be made final.
    public BaseTasksRequest() {}

    protected BaseTasksRequest(StreamInput in) throws IOException {
        super(in);
        taskId = TaskId.readFromStream(in);
        parentTaskId = TaskId.readFromStream(in);
        nodes = in.readStringArray();
        actions = in.readStringArray();
        timeout = in.readOptionalTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        taskId.writeTo(out);
        parentTaskId.writeTo(out);
        out.writeStringArrayNullable(nodes);
        out.writeStringArrayNullable(actions);
        out.writeOptionalTimeValue(timeout);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (taskId.isSet() && nodes.length > 0) {
            validationException = addValidationError("task id cannot be used together with node ids", validationException);
        }
        return validationException;
    }

    /**
     * Sets the list of action masks for the actions that should be returned
     */
    @SuppressWarnings("unchecked")
    public final Request setActions(String... actions) {
        this.actions = actions;
        return (Request) this;
    }

    /**
     * Return the list of action masks for the actions that should be returned
     */
    public String[] getActions() {
        return actions;
    }

    public final String[] getNodes() {
        return nodes;
    }

    @SuppressWarnings("unchecked")
    public final Request setNodes(String... nodes) {
        this.nodes = nodes;
        return (Request) this;
    }

    /**
     * Returns the id of the task that should be processed.
     *
     * By default tasks with any ids are returned.
     */
    public TaskId getTaskId() {
        return taskId;
    }

    @SuppressWarnings("unchecked")
    public final Request setTaskId(TaskId taskId) {
        this.taskId = taskId;
        return (Request) this;
    }

    /**
     * Returns the parent task id that tasks should be filtered by
     */
    public TaskId getParentTaskId() {
        return parentTaskId;
    }

    @SuppressWarnings("unchecked")
    public Request setParentTaskId(TaskId parentTaskId) {
        this.parentTaskId = parentTaskId;
        return (Request) this;
    }

    public TimeValue getTimeout() {
        return this.timeout;
    }

    @SuppressWarnings("unchecked")
    public final Request setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
    }

    @SuppressWarnings("unchecked")
    public final Request setTimeout(String timeout) {
        this.timeout = TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout");
        return (Request) this;
    }

    public boolean match(Task task) {
        if (CollectionUtils.isEmpty(getActions()) == false && Regex.simpleMatch(getActions(), task.getAction()) == false) {
            return false;
        }
        if (getTaskId().isSet()) {
            if (getTaskId().getId() != task.getId()) {
                return false;
            }
        }
        if (parentTaskId.isSet()) {
            if (parentTaskId.equals(task.getParentTaskId()) == false) {
                return false;
            }
        }
        return true;
    }
}
