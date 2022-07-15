/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.management.decommission;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor;
import org.opensearch.cluster.decommission.DecommissionService;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportPutDecommissionAction extends TransportClusterManagerNodeAction<
    PutDecommissionRequest,
    AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutDecommissionAction.class);
    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    private final DecommissionService decommissionService;

    private volatile List<String> decommissionedNodesID;

    @Inject
    public TransportPutDecommissionAction(
        TransportService transportService,
        ClusterService clusterService,
        DecommissionService decommissionService,
        ThreadPool threadPool,
        AllocationService allocationService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
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
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
        this.decommissionService = decommissionService;
    }

    private void setDecommissionedNodesID(List<String> decommissionedNodesID) {
        this.decommissionedNodesID = decommissionedNodesID;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(
        PutDecommissionRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener) throws Exception {
        decommissionService.registerDecommissionAttribute(
            request,
            ActionListener.delegateFailure(
                listener,
                (delegatedListener, response) -> delegatedListener.onResponse(new AcknowledgedResponse(response.isAcknowledged()))
            )
        );
//        List<DiscoveryNode> decommissionedNodes = resolveDecommissionedNodes(request, state);
//        for(DiscoveryNode decommissionedNode: decommissionedNodes) {
//            removeDecommissionedNodes(decommissionedNode);
//        }
    }

    @Override
    protected ClusterBlockException checkBlock(PutDecommissionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void removeDecommissionedNodes(DiscoveryNode discoveryNode) {
        clusterService.submitStateUpdateTask(
            "node-decommissioned",
            new NodeRemovalClusterStateTaskExecutor.Task(discoveryNode, "node is decommissioned"),
            ClusterStateTaskConfig.build(Priority.IMMEDIATE),
            nodeRemovalExecutor,
            nodeRemovalExecutor
        );
        logger.info("Hello. I am OpenSearch!");
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
