/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks.create;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.rest.action.admin.cluster.ClusterTask;

import java.io.IOException;
import java.util.Map;

/**
 * Creates a new task.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class CreateTaskRequest extends ActionRequest {

    private String actionName;

    public CreateTaskRequest() {}

    public CreateTaskRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public ClusterTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new ClusterTask(id, type, action, parentTaskId, headers, this.getCancelAfterTimeInterval());
    }

    public void setActionName(String actionName) {
        this.actionName = actionName;
    }

    public String getActionName() {
        return this.actionName;
    }
}
