/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.node.tasks.delete.DeleteTaskRequest;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.DELETE;

/**
 * Rest action to delete a stored completed task result.
 *
 * @opensearch.api
 */
public class RestDeleteTaskAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(DELETE, "/_tasks/{task_id}"));
    }

    @Override
    public String getName() {
        return "delete_task_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        DeleteTaskRequest deleteTaskRequest = new DeleteTaskRequest();
        deleteTaskRequest.setTaskId(new TaskId(request.param("task_id")));
        return channel -> client.admin().cluster().deleteTask(deleteTaskRequest, new RestToXContentListener<>(channel));
    }
}
