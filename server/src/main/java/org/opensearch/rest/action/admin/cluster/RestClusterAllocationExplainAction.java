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

import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestBuilderListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Class handling cluster allocation explanation at the REST level
 *
 * @opensearch.api
 */
public class RestClusterAllocationExplainAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_cluster/allocation/explain"), new Route(POST, "/_cluster/allocation/explain")));
    }

    @Override
    public String getName() {
        return "cluster_allocation_explain_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ClusterAllocationExplainRequest req;
        if (request.hasContentOrSourceParam() == false) {
            // Empty request signals "explain the first unassigned shard you find"
            req = new ClusterAllocationExplainRequest();
        } else {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                req = ClusterAllocationExplainRequest.parse(parser);
            }
        }

        req.includeYesDecisions(request.paramAsBoolean("include_yes_decisions", false));
        req.includeDiskInfo(request.paramAsBoolean("include_disk_info", false));
        return channel -> client.admin()
            .cluster()
            .allocationExplain(req, new RestBuilderListener<ClusterAllocationExplainResponse>(channel) {
                @Override
                public RestResponse buildResponse(ClusterAllocationExplainResponse response, XContentBuilder builder) throws IOException {
                    response.getExplanation().toXContent(builder, ToXContent.EMPTY_PARAMS);
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
    }
}
