/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.decommission.DecommissionService;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.http.HttpStats;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransportPutDecommissionAction extends TransportClusterManagerNodeAction<
    PutDecommissionRequest,
    PutDecommissionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutDecommissionAction.class);

    private final DecommissionService decommissionService;
    private final TransportAddVotingConfigExclusionsAction exclusionsAction;

    private final TimeValue decommissionedNodeRequestCheckInterval;

    @Inject
    public TransportPutDecommissionAction(
        TransportService transportService,
        ClusterService clusterService,
        DecommissionService decommissionService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TransportAddVotingConfigExclusionsAction exclusionsAction
    ) {
        super(
            PutDecommissionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutDecommissionRequest::new,
            indexNameExpressionResolver
        );
        this.decommissionService = decommissionService;
        this.exclusionsAction = exclusionsAction;
        // TODO Decide on the frequency
        this.decommissionedNodeRequestCheckInterval = TimeValue.timeValueMillis(3000);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutDecommissionResponse read(StreamInput in) throws IOException {
        return new PutDecommissionResponse(in);
    }

    @Override
    protected void masterOperation(
        PutDecommissionRequest request,
        ClusterState state,
        ActionListener<PutDecommissionResponse> listener) throws Exception {

        List<DiscoveryNode> decommissionedNodes = getDecommissionedNodes(request, state);
        checkHttpStatsForDecommissionedNodes(request, decommissionedNodes, listener, state);
    }

    private void checkHttpStatsForDecommissionedNodes(PutDecommissionRequest request,
                                                      List<DiscoveryNode> decommissionedNodes,
                                                      ActionListener<PutDecommissionResponse> listener,
                                                      ClusterState state) {
        ActionListener<NodesStatsResponse> nodesStatsResponseActionListener = new ActionListener<NodesStatsResponse>() {
            @Override
            public void onResponse(NodesStatsResponse nodesStatsResponse) {
                boolean hasActiveConnections = false;
                List<NodeStats> responseNodes = nodesStatsResponse.getNodes();
                for (int i=0; i < responseNodes.size(); i++) {
                    HttpStats httpStats = responseNodes.get(i).getHttp();
                    if (httpStats != null && httpStats.getServerOpen() != 0) {
                        hasActiveConnections = true;
                        break;
                    }
                }
                if (hasActiveConnections) {
                    // Slow down the next call to get the Http stats from the decommissioned nodes.
                    scheduleDecommissionNodesRequestCheck(decommissionedNodes, this);
                } else {
                    decommissionService.initiateAttributeDecommissioning(
                            request.getDecommissionAttribute(),
                            new ActionListener<ClusterStateUpdateResponse>() {
                                @Override
                                public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                                    logger.info("Decommission acknowledged. ");
                                    listener.onResponse(new PutDecommissionResponse(true));
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    logger.info("decommission request for attribute [{}] failed", e.getCause());
                                    listener.onFailure(e);
                                }
                            },
                            state);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        };
        waitForGracefulDecommission(decommissionedNodes, nodesStatsResponseActionListener);
    }

    private void scheduleDecommissionNodesRequestCheck(List<DiscoveryNode> decommissionedNodes, ActionListener<NodesStatsResponse> listener) {
        transportService.getThreadPool().schedule(new Runnable() {
            @Override
            public void run() {
                // Check again for active connections. Repeat the process till we have no more active requests open.
                waitForGracefulDecommission(decommissionedNodes, listener);
            }

            @Override
            public String toString() {
                return TransportPutDecommissionAction.class.getName() + "::checkHttpStatsForDecommissionedNodes";
            }
        }, decommissionedNodeRequestCheckInterval, org.opensearch.threadpool.ThreadPool.Names.SAME);
    }

    private void waitForGracefulDecommission(List<DiscoveryNode> decommissionedNodes, ActionListener<NodesStatsResponse> listener) {

        if(decommissionedNodes == null || decommissionedNodes.isEmpty()) {
            return;
        }
        String[] nodes = decommissionedNodes.stream()
            .map(DiscoveryNode::getId)
            .toArray(String[]::new);

        if (nodes.length == 0) {
            return;
        }

        final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(nodes);
        nodesStatsRequest.clear();
        nodesStatsRequest.addMetric(NodesStatsRequest.Metric.HTTP.metricName());

        transportService.sendRequest(
            transportService.getLocalNode(),
            NodesStatsAction.NAME,
            nodesStatsRequest,
            new TransportResponseHandler<NodesStatsResponse>() {
                @Override
                public void handleResponse(NodesStatsResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public NodesStatsResponse read(StreamInput in) throws IOException {
                    return new NodesStatsResponse(in);
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(PutDecommissionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    public List<DiscoveryNode> getDecommissionedNodes(PutDecommissionRequest request, ClusterState clusterState) {
        List<DiscoveryNode> decommissionedNodes = new ArrayList<>();
        DiscoveryNode[] discoveryNodes = clusterState.nodes().getNodes().values().toArray(DiscoveryNode.class);

        if (request.getDecommissionAttribute().attributeValue() != null) {
            final String zoneName = request.getDecommissionAttribute().attributeValue();
            assert zoneName != null : "No nodes to decommission";
            for (DiscoveryNode discoveryNode : discoveryNodes) {
                if (zoneName.equals(discoveryNode.getAttributes().get(request.getDecommissionAttribute().attributeName()))) {
                    decommissionedNodes.add(discoveryNode);
                }
            }
        }
        return decommissionedNodes;
    }
}
