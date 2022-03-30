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

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

public class RestPendingClusterTasksAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPendingClusterTasksAction.class);
    private static final String MASTER_TIMEOUT_DEPRECATED_MESSAGE =
        "Deprecated parameter [master_timeout] used. To promote inclusive language, please use [cluster_manager_timeout] instead. It will be unsupported in a future major version.";

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cluster/pending_tasks"));
    }

    @Override
    public String getName() {
        return "pending_cluster_tasks_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        PendingClusterTasksRequest pendingClusterTasksRequest = new PendingClusterTasksRequest();
        pendingClusterTasksRequest.masterNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", pendingClusterTasksRequest.masterNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(pendingClusterTasksRequest, request);
        pendingClusterTasksRequest.local(request.paramAsBoolean("local", pendingClusterTasksRequest.local()));
        return channel -> client.admin().cluster().pendingClusterTasks(pendingClusterTasksRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Parse the deprecated request parameter 'master_timeout', and add deprecated log if the parameter is used.
     * It also validates whether the value of 'master_timeout' is the same with 'cluster_manager_timeout'.
     * Remove the method along with MASTER_ROLE.
     * @deprecated As of 2.0, because promoting inclusive language.
     */
    @Deprecated
    private static void parseDeprecatedMasterTimeoutParameter(PendingClusterTasksRequest pendingClusterTasksRequest, RestRequest request) {
        final String deprecatedTimeoutParam = "master_timeout";
        if (request.hasParam(deprecatedTimeoutParam)) {
            deprecationLogger.deprecate("cluster_get_setting_master_timeout_parameter", MASTER_TIMEOUT_DEPRECATED_MESSAGE);
            request.validateParamValuesAreEqual(deprecatedTimeoutParam, "cluster_manager_timeout");
            pendingClusterTasksRequest.masterNodeTimeout(
                request.paramAsTime(deprecatedTimeoutParam, pendingClusterTasksRequest.masterNodeTimeout())
            );
        }
    }
}
