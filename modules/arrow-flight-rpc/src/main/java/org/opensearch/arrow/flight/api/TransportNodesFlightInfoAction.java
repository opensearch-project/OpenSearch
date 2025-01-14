/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.api;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.arrow.flight.bootstrap.FlightService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportNodesFlightInfoAction extends TransportNodesAction<
    NodesFlightInfoRequest,
    NodesFlightInfoResponse,
    NodesFlightInfoRequest.NodeFlightInfoRequest,
    NodeFlightInfo> {

    private final FlightService flightService;

    @Inject
    public TransportNodesFlightInfoAction(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        FlightService flightService
    ) {
        super(
            NodesFlightInfoAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            NodesFlightInfoRequest::new,
            NodesFlightInfoRequest.NodeFlightInfoRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodeFlightInfo.class
        );
        this.flightService = flightService;
    }

    @Override
    protected NodesFlightInfoResponse newResponse(
        NodesFlightInfoRequest request,
        List<NodeFlightInfo> nodeFlightInfos,
        List<FailedNodeException> failures
    ) {
        return new NodesFlightInfoResponse(clusterService.getClusterName(), nodeFlightInfos, failures);
    }

    @Override
    protected NodesFlightInfoRequest.NodeFlightInfoRequest newNodeRequest(NodesFlightInfoRequest request) {
        return new NodesFlightInfoRequest.NodeFlightInfoRequest(request);
    }

    @Override
    protected NodeFlightInfo newNodeResponse(StreamInput in) throws IOException {
        return new NodeFlightInfo(in);
    }

    @Override
    protected NodeFlightInfo nodeOperation(NodesFlightInfoRequest.NodeFlightInfoRequest request) {
        return new NodeFlightInfo(clusterService.localNode(), flightService.getBoundAddress());
    }
}
