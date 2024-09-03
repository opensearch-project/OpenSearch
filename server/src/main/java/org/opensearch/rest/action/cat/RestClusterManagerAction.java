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

package org.opensearch.rest.action.cat;

import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.Table;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * _cat API action to list cluster_manager information
 *
 * @opensearch.api
 */
public class RestClusterManagerAction extends AbstractCatAction {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestClusterManagerAction.class);

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // The deprecated path will be removed in a future major version.
        return singletonList(new ReplacedRoute(GET, "/_cat/cluster_manager", "/_cat/master"));
    }

    @Override
    public String getName() {
        return "cat_cluster_manager_action";
    }

    @Override
    public void documentation(StringBuilder sb) {
        sb.append("/_cat/cluster_manager\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().nodes(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", clusterStateRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(clusterStateRequest, request, deprecationLogger, getName());

        return channel -> client.admin().cluster().state(clusterStateRequest, new RestResponseListener<ClusterStateResponse>(channel) {
            @Override
            public RestResponse buildResponse(final ClusterStateResponse clusterStateResponse) throws Exception {
                return RestTable.buildResponse(buildTable(request, clusterStateResponse), channel);
            }
        });
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders()
            .addCell("id", "desc:node id")
            .addCell("host", "alias:h;desc:host name")
            .addCell("ip", "desc:ip address ")
            .addCell("node", "alias:n;desc:node name")
            .endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, ClusterStateResponse state) {
        Table table = getTableWithHeader(request);
        DiscoveryNodes nodes = state.getState().nodes();

        table.startRow();
        DiscoveryNode clusterManager = nodes.get(nodes.getClusterManagerNodeId());
        if (clusterManager == null) {
            table.addCell("-");
            table.addCell("-");
            table.addCell("-");
            table.addCell("-");
        } else {
            table.addCell(clusterManager.getId());
            table.addCell(clusterManager.getHostName());
            table.addCell(clusterManager.getHostAddress());
            table.addCell(clusterManager.getName());
        }
        table.endRow();

        return table;
    }
}
