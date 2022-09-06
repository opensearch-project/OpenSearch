/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import org.opensearch.action.ActionListener;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.GetAllPitNodesRequest;
import org.opensearch.action.search.GetAllPitNodesResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActionListener;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.DELETE;

/**
 * Rest action for deleting PIT contexts
 */
public class RestDeletePitAction extends BaseRestHandler {

    private final Supplier<DiscoveryNodes> nodesInCluster;

    public RestDeletePitAction(Supplier<DiscoveryNodes> nodesInCluster) {
        super();
        this.nodesInCluster = nodesInCluster;
    }

    @Override
    public String getName() {
        return "delete_pit_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String allPitIdsQualifier = "_all";
        final DeletePitRequest deletePITRequest;
        if (request.path().contains(allPitIdsQualifier)) {
            deletePITRequest = new DeletePitRequest(asList(allPitIdsQualifier));
        } else {
            deletePITRequest = new DeletePitRequest();
            request.withContentOrSourceParamParserOrNull((xContentParser -> {
                if (xContentParser != null) {
                    try {
                        deletePITRequest.fromXContent(xContentParser);
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Failed to parse request body", e);
                    }
                }
            }));
        }
        /**
         * Delete all active PIT reader contexts leveraging list all PITs
         *
         * For Cross cluster PITs :
         * - mixed cluster PITs ( PIT comprising local and remote ) will be fully deleted. Since there will atleast be
         * one reader context with PIT ID present in local cluster, 'Get all PITs' will retrieve the PIT ID with which
         * we can completely delete the PIT contexts in both local and remote cluster.
         * - fully remote PITs will not be deleted as 'Get all PITs' operates on local cluster only and no PIT info can
         * be retrieved when it's fully remote.
         */
        if (request.path().contains(allPitIdsQualifier)) {
            final List<DiscoveryNode> nodes = new ArrayList<>();
            for (DiscoveryNode node : nodesInCluster.get()) {
                nodes.add(node);
            }
            DiscoveryNode[] disNodesArr = nodes.toArray(new DiscoveryNode[nodes.size()]);
            GetAllPitNodesRequest getAllPitNodesRequest = new GetAllPitNodesRequest(disNodesArr);
            return deleteAllPits(client, getAllPitNodesRequest, deletePITRequest);
        } else {
            return channel -> client.deletePits(deletePITRequest, new RestStatusToXContentListener<DeletePitResponse>(channel));
        }
    }

    private RestChannelConsumer deleteAllPits(
        NodeClient client,
        GetAllPitNodesRequest getAllPitNodesRequest,
        DeletePitRequest deletePitRequest
    ) {
        return channel -> client.admin().cluster().listAllPits(getAllPitNodesRequest, new RestActionListener<>(channel) {
            @Override
            public void processResponse(final GetAllPitNodesResponse getAllPitNodesResponse) throws IOException {
                // return empty response when no PITs are retrieved
                if (getAllPitNodesResponse.getPitInfos().size() == 0) {
                    DeletePitResponse response = new DeletePitResponse(new ArrayList<>());
                    ActionListener listener = new RestStatusToXContentListener<DeletePitResponse>(channel);
                    listener.onResponse(response);
                    return;
                }
                deletePitRequest.clearAndSetPitIds(
                    getAllPitNodesResponse.getPitInfos().stream().map(r -> r.getPitId()).collect(Collectors.toList())
                );
                client.deletePits(deletePitRequest, new RestStatusToXContentListener<DeletePitResponse>(channel));
            }
        });
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(DELETE, "/_search/point_in_time"), new Route(DELETE, "/_search/point_in_time/_all")));
    }
}
