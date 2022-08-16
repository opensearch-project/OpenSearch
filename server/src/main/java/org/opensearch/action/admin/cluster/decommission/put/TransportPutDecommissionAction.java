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
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsResponse;
import org.opensearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.decommission.DecommissionService;
import org.opensearch.cluster.metadata.DecommissionedAttributeMetadata;
import org.opensearch.cluster.metadata.DecommissionedAttributesMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Setting;
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

        boolean currentMasterDecommission = false;
        DiscoveryNode masterNode = clusterService.state().getNodes().getMasterNode();
        for (String decommissionedZone : request.getDecommissionAttribute().attributeValues()) {
            if (masterNode.getAttributes().get(request.getDecommissionAttribute().attributeName()).equals(decommissionedZone)) {
                currentMasterDecommission = true;
            }
        }

        if (currentMasterDecommission) {
            logger.info("Current master in getting decommissioned. Will first abdicate master and then execute the decommission");
            ActionListener<AddVotingConfigExclusionsResponse> addVotingConfigExclusionsListener = new ActionListener<>() {
                @Override
                public void onResponse(AddVotingConfigExclusionsResponse addVotingConfigExclusionsResponse) {
                    logger.info("Master abdicated - Response received");
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            };
            exclusionsAction.execute(new AddVotingConfigExclusionsRequest(masterNode.getName()), addVotingConfigExclusionsListener);
            throw new NotClusterManagerException("abdicated");
        }

        List<DiscoveryNode> decommissionedNodes = getDecommissionedNodes(state);
        checkHttpStatsForDecommissionedNodes(decommissionedNodes, listener);

        decommissionService.registerDecommissionAttribute(
            request,
            ActionListener.delegateFailure(
                listener,
                (delegatedListener, response) -> delegatedListener.onResponse(new PutDecommissionResponse(response.isAcknowledged()))
            )
        );
    }

    private void checkHttpStatsForDecommissionedNodes(List<DiscoveryNode> decommissionedNodes, ActionListener<PutDecommissionResponse> listener) {
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

    public List<DiscoveryNode> getDecommissionedNodes(ClusterState clusterState) {
        List<DiscoveryNode> decommissionedNodes = new ArrayList<>();
        DiscoveryNode[] discoveryNodes = clusterState.nodes().getNodes().values().toArray(DiscoveryNode.class);
        DecommissionedAttributesMetadata decommissionedAttributes = clusterState.metadata().custom(DecommissionedAttributesMetadata.TYPE);
        DecommissionedAttributeMetadata metadata = decommissionedAttributes.decommissionedAttribute("awareness");
        assert metadata != null : "No nodes to decommission";
        DecommissionAttribute decommissionAttribute = metadata.decommissionedAttribute();
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            for (String zone : decommissionAttribute.attributeValues()) {
                if (zone.equals(discoveryNode.getAttributes().get(decommissionAttribute.attributeName()))) {
                    decommissionedNodes.add(discoveryNode);
                }
            }
        }
        return decommissionedNodes;
    }

//    private static List<DiscoveryNode> resolveDecommissionedNodes(
//        PutDecommissionRequest request,
//        ClusterState state
//    ) {
//        return getDecommissionedNodes(
//            state
//        );
//    }
//    List<DiscoveryNode> getDecommissionedNodes(ClusterState currentState) {
//        final DiscoveryNodes currentNodes = currentState.nodes();
//        List<DiscoveryNode> decommissionedNodes = new ArrayList<>();
//        if (nodeIds.length >= 1) {
//            for (String nodeId : nodeIds) {
//                if (currentNodes.nodeExists(nodeId)) {
//                    DiscoveryNode discoveryNode = currentNodes.get(nodeId);
//                    decommissionedNodes.add(discoveryNode);
//                }
//            }
//        } else {
//            assert nodeNames.length >= 1;
//            Map<String, DiscoveryNode> existingNodes = StreamSupport.stream(currentNodes.spliterator(), false)
//                .collect(Collectors.toMap(DiscoveryNode::getName, Function.identity()));
//
//            for (String nodeName : nodeNames) {
//                if (existingNodes.containsKey(nodeName)) {
//                    DiscoveryNode discoveryNode = existingNodes.get(nodeName);
//                    decommissionedNodes.add(discoveryNode);
//                }
//            }
//        }
//        return decommissionedNodes;
//    }
}
