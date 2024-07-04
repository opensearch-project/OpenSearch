/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin;

import org.opensearch.action.admin.cluster.node.tasks.create.CreateTaskRequest;
import org.opensearch.action.admin.cluster.node.tasks.create.CreateTaskResponse;
import org.opensearch.action.admin.cluster.node.tasks.unregister.UnregisterTaskRequest;
import org.opensearch.action.admin.cluster.node.tasks.unregister.UnregisterTaskResponse;
import org.opensearch.action.support.TimeoutTaskCancellationUtility;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rest.RestResponse;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;

public class AdminAPIsUtility {

    public CancellableTask createAndCancelCancellableTask(NodeClient client, TimeValue timeout) {
        CreateTaskRequest createTaskRequest = new CreateTaskRequest();
        createTaskRequest.setCancelAfterTimeInterval(timeout);
        final CancellableTask[] task = {null};
        client.admin().cluster().createTask(createTaskRequest, new ActionListener<CreateTaskResponse>() {
            @Override
            public void onResponse(CreateTaskResponse createTaskResponse) {
                task[0] = (CancellableTask) createTaskResponse.getTask();
                ActionListener<RestResponse> listener = TimeoutTaskCancellationUtility.wrapWithCancellationListener(
                    client,
                    task[0],
                    task[0].getCancellationTimeout(),
                    new ActionListener<RestResponse>() {
                        @Override
                        public void onResponse(RestResponse restResponse) {}
                        @Override
                        public void onFailure(Exception e) {}
                    }
                );
            }
            @Override
            public void onFailure(Exception e) {

            }
        });
        return task[0];
    }

    public void unregisterTask(NodeClient client, Task task) {
        UnregisterTaskRequest unregisterTaskRequest = new UnregisterTaskRequest(task);
        client.admin().cluster().UnregisterTask(unregisterTaskRequest, new ActionListener<UnregisterTaskResponse>() {
            @Override
            public void onResponse(UnregisterTaskResponse unregisterTaskResponse) {}
            @Override
            public void onFailure(Exception e) {}
        });

    }
}
