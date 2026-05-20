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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.reindex;

import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.support.tasks.TasksRequestBuilder;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * Java API support for changing the throttle on reindex tasks while they are running.
 */
public class RethrottleRequestBuilder extends TasksRequestBuilder<RethrottleRequest, ListTasksResponse, RethrottleRequestBuilder> {
    public RethrottleRequestBuilder(OpenSearchClient client, ActionType<ListTasksResponse> action) {
        super(client, action, new RethrottleRequest());
    }

    /**
     * Set the throttle to apply to all matching requests in sub-requests per second. {@link Float#POSITIVE_INFINITY} means set no throttle.
     * Throttling is done between batches, as we start the next scroll requests. That way we can increase the scroll's timeout to make sure
     * that it contains any time that we might wait.
     */
    public RethrottleRequestBuilder setRequestsPerSecond(float requestsPerSecond) {
        request.setRequestsPerSecond(requestsPerSecond);
        return this;
    }
}
