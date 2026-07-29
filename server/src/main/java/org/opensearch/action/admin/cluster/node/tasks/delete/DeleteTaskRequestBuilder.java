/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks.delete;

import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * Builder for the request to delete a stored completed task result.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.8.0")
public class DeleteTaskRequestBuilder extends ActionRequestBuilder<DeleteTaskRequest, AcknowledgedResponse> {
    public DeleteTaskRequestBuilder(OpenSearchClient client, DeleteTaskAction action) {
        super(client, action, new DeleteTaskRequest());
    }

    /**
     * Set the TaskId to delete. Required.
     */
    public final DeleteTaskRequestBuilder setTaskId(TaskId taskId) {
        request.setTaskId(taskId);
        return this;
    }
}
