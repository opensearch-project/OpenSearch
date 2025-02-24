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

import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Transport action to rollover index
 *
 * @opensearch.api
 */
public class RestRolloverIndexAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestRolloverIndexAction.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in rollover "
        + "index requests is deprecated. The parameter will be removed in the next major version.";

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/{index}/_rollover"), new Route(POST, "/{index}/_rollover/{new_index}")));
    }

    @Override
    public String getName() {
        return "rollover_index_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (request.hasParam("include_type_name")) {
            deprecationLogger.deprecate("index_rollover_with_types", TYPES_DEPRECATION_MESSAGE);
        }
        RolloverRequest rolloverIndexRequest = new RolloverRequest(request.param("index"), request.param("new_index"));
        request.applyContentParser(parser -> rolloverIndexRequest.fromXContent(parser));
        rolloverIndexRequest.dryRun(request.paramAsBoolean("dry_run", false));
        rolloverIndexRequest.timeout(request.paramAsTime("timeout", rolloverIndexRequest.timeout()));
        rolloverIndexRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", rolloverIndexRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(rolloverIndexRequest, request, deprecationLogger, getName());
        rolloverIndexRequest.getCreateIndexRequest()
            .waitForActiveShards(ActiveShardCount.parseString(request.param("wait_for_active_shards")));
        return channel -> client.admin().indices().rolloverIndex(rolloverIndexRequest, new RestToXContentListener<>(channel));
    }
}
