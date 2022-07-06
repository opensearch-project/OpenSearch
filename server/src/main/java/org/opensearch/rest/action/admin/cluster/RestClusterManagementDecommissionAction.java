/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.management.decommission.ClusterManagementDecommissionAction;
import org.opensearch.action.admin.cluster.management.decommission.ClusterManagementDecommissionRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Strings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Transport action to decommission nodes
 *
 * @opensearch.api
 */
public class RestClusterManagementDecommissionAction extends BaseRestHandler {

    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(30L);
    private static final Logger logger = LogManager.getLogger(RestClusterManagementDecommissionAction.class);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(PUT, "_cluster/management/decommission"));
    }

    @Override
    public String getName() {
        return "cluster_management_decommission_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ClusterManagementDecommissionRequest clusterManagementDecommissionRequest = resolveClusterManagementDecommissionRequest(request);
        return channel -> client.admin()
            .cluster()
            .decommission(clusterManagementDecommissionRequest, new RestToXContentListener<>(channel));
    }

    ClusterManagementDecommissionRequest resolveClusterManagementDecommissionRequest(final RestRequest request) {
        String nodeIds = null;
        String nodeNames = null;

        if (request.hasParam("node_ids")) {
            nodeIds = request.param("node_ids");
        }

        if (request.hasParam("node_names")) {
            nodeNames = request.param("node_names");
        }

        return new ClusterManagementDecommissionRequest(
            Strings.splitStringByCommaToArray(nodeIds),
            Strings.splitStringByCommaToArray(nodeNames),
            TimeValue.parseTimeValue(request.param("timeout"), DEFAULT_TIMEOUT, getClass().getSimpleName() + ".timeout")
        );
    }

}
