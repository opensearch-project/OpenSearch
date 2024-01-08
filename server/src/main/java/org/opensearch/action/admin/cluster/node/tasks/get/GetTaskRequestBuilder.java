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

import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;

/**
 * Builder for the request to retrieve the list of tasks running on the specified nodes
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class GetTaskRequestBuilder extends ActionRequestBuilder<GetTaskRequest, GetTaskResponse> {
    public GetTaskRequestBuilder(OpenSearchClient client, GetTaskAction action) {
        super(client, action, new GetTaskRequest());
    }

    /**
     * Set the TaskId to look up. Required.
     */
    public final GetTaskRequestBuilder setTaskId(TaskId taskId) {
        request.setTaskId(taskId);
        return this;
    }

    /**
     * Should this request wait for all found tasks to complete?
     */
    public final GetTaskRequestBuilder setWaitForCompletion(boolean waitForCompletion) {
        request.setWaitForCompletion(waitForCompletion);
        return this;
    }

    /**
     * Timeout to wait for any async actions this request must take. It must take anywhere from 0 to 2.
     */
    public final GetTaskRequestBuilder setTimeout(TimeValue timeout) {
        request.setTimeout(timeout);
        return this;
    }
}
