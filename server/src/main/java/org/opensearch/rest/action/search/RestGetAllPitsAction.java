/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.search.GetAllPitNodesRequest;
import org.opensearch.action.search.GetAllPitNodesResponse;
import org.opensearch.action.search.GetAllPitsAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestActionListener;
import org.opensearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Rest action for retrieving all active PIT IDs across all nodes
 */
public class RestGetAllPitsAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "get_all_pit_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(false);
        clusterStateRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", clusterStateRequest.clusterManagerNodeTimeout())
        );
        clusterStateRequest.clear().nodes(true).routingTable(true).indices("*");
        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) {
                final List<DiscoveryNode> nodes = new LinkedList<>();
                for (ObjectCursor<DiscoveryNode> cursor : clusterStateResponse.getState().nodes().getDataNodes().values()) {
                    DiscoveryNode node = cursor.value;
                    nodes.add(node);
                }
                DiscoveryNode[] disNodesArr = new DiscoveryNode[nodes.size()];
                nodes.toArray(disNodesArr);
                GetAllPitNodesRequest getAllPITNodesRequest = new GetAllPitNodesRequest(
                        request.paramAsBoolean("include_all", false), disNodesArr);
                client.execute(GetAllPitsAction.INSTANCE, getAllPITNodesRequest, new RestResponseListener<GetAllPitNodesResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(final GetAllPitNodesResponse getAllPITNodesResponse) throws Exception {
                        try (XContentBuilder builder = channel.newBuilder()) {
                            builder.startObject();
                            if(getAllPITNodesResponse.hasFailures()) {
                                builder.startArray("failures");
                                for (int idx = 0; idx < getAllPITNodesResponse.failures().size(); idx++) {
                                    builder.field(getAllPITNodesResponse.failures().get(idx).nodeId(),
                                            getAllPITNodesResponse.failures().get(idx).getDetailedMessage());
                                }
                                builder.endArray();
                            }
                            builder.field("pits", getAllPITNodesResponse.getPITIDs());
                            if(getAllPITNodesRequest.getIncludeAll()) {
                                builder.startArray("nodeResults");
                                for (int idx = 0; idx < getAllPITNodesResponse.getNodes().size(); idx++) {
                                    builder.value(getAllPITNodesResponse.getNodes().get(idx));
                                }
                                builder.endArray();
                            }
                            builder.endObject();
                            return new BytesRestResponse(RestStatus.OK, builder);
                        }
                    }
                });
            }
        });
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(Collections.singletonList(new Route(GET, "/_search/point_in_time/all")));
    }

}
