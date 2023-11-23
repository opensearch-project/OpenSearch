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

package org.opensearch.action.admin.cluster.node.tasks.cancel;

import org.opensearch.action.support.tasks.TasksRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.annotation.PublicApi;

/**
 * Builder for the request to cancel tasks running on the specified nodes
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class CancelTasksRequestBuilder extends TasksRequestBuilder<CancelTasksRequest, CancelTasksResponse, CancelTasksRequestBuilder> {

    public CancelTasksRequestBuilder(OpenSearchClient client, CancelTasksAction action) {
        super(client, action, new CancelTasksRequest());
    }

    public CancelTasksRequestBuilder waitForCompletion(boolean waitForCompletion) {
        request.setWaitForCompletion(waitForCompletion);
        return this;
    }
}
