/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.action;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.datafusion.DataFusionService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for retrieving DataFusion information from nodes
 */
public class TransportNodesDataFusionInfoAction extends TransportNodesAction<
    NodesDataFusionInfoRequest,
    NodesDataFusionInfoResponse,
    NodesDataFusionInfoRequest.NodeDataFusionInfoRequest,
    NodeDataFusionInfo> {

    private final DataFusionService dataFusionService;

    /**
     * Constructor for TransportNodesDataFusionInfoAction.
     * @param threadPool The thread pool.
     * @param clusterService The cluster service.
     * @param transportService The transport service.
     * @param actionFilters The action filters.
     * @param dataFusionService The DataFusion service.
     */
    @Inject
    public TransportNodesDataFusionInfoAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        DataFusionService dataFusionService
    ) {
        super(
            NodesDataFusionInfoAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            NodesDataFusionInfoRequest::new,
            NodesDataFusionInfoRequest.NodeDataFusionInfoRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodeDataFusionInfo.class
        );
        this.dataFusionService = dataFusionService;
    }

    /**
     * Creates a new nodes response.
     * @param request The nodes request.
     * @param responses The list of node responses.
     * @param failures The list of failed node exceptions.
     * @return The nodes response.
     */
    @Override
    protected NodesDataFusionInfoResponse newResponse(
        NodesDataFusionInfoRequest request,
        List<NodeDataFusionInfo> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesDataFusionInfoResponse(clusterService.getClusterName(), responses, failures);
    }

    /**
     * Creates a new node request.
     * @param request The nodes request.
     * @return The node request.
     */
    @Override
    protected NodesDataFusionInfoRequest.NodeDataFusionInfoRequest newNodeRequest(NodesDataFusionInfoRequest request) {
        return new NodesDataFusionInfoRequest.NodeDataFusionInfoRequest();
    }

    @Override
    protected NodeDataFusionInfo newNodeResponse(StreamInput in) throws IOException {
        return new NodeDataFusionInfo(in);
    }

    /**
     * Handles the node request and returns the node response.
     * @param request The node request.
     * @return The node response.
     */
    @Override
    protected NodeDataFusionInfo nodeOperation(NodesDataFusionInfoRequest.NodeDataFusionInfoRequest request) {
        try {
            return new NodeDataFusionInfo(
                clusterService.localNode(),
                dataFusionService.getVersion()
            );
        } catch (Exception e) {
            return new NodeDataFusionInfo(
                clusterService.localNode(),
                "unknown"
            );
        }
    }
}
