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

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.indices.open.OpenIndexAction;
import org.opensearch.action.admin.indices.open.OpenIndexRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.tasks.LoggingTaskListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.action.support.master.AcknowledgedRequest.DEFAULT_TASK_EXECUTION_TIMEOUT;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Transport action to open an index
 *
 * @opensearch.api
 */
public class RestOpenIndexAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestOpenIndexAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/_open"), new Route(POST, "/{index}/_open")));
    }

    @Override
    public String getName() {
        return "open_index_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(Strings.splitStringByCommaToArray(request.param("index")));
        openIndexRequest.timeout(request.paramAsTime("timeout", openIndexRequest.timeout()));
        openIndexRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", openIndexRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(openIndexRequest, request, deprecationLogger, getName());
        openIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, openIndexRequest.indicesOptions()));
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            openIndexRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        if (request.paramAsBoolean("wait_for_completion", true)) {
            return channel -> client.admin().indices().open(openIndexRequest, new RestToXContentListener<>(channel));
        } else {
            // Running opening index asynchronously, return a task immediately and store the task's result when it completes
            openIndexRequest.setShouldStoreResult(true);
            /*
             * Replace the ack timeout by task_execution_timeout so that the task can take a longer time to execute but not finish in 30s
             * by default, task_execution_timeout defaults to 1h.
             */
            openIndexRequest.timeout(request.paramAsTime("task_execution_timeout", DEFAULT_TASK_EXECUTION_TIMEOUT));
            /*
             * Add some validation before so the user can get some error immediately. The
             * task can't totally validate until it starts but this is better than nothing.
             */
            ActionRequestValidationException validationException = openIndexRequest.validate();
            if (validationException != null) {
                throw validationException;
            }
            return sendTask(
                client.getLocalNodeId(),
                client.executeLocally(OpenIndexAction.INSTANCE, openIndexRequest, LoggingTaskListener.instance())
            );
        }

    }
}
