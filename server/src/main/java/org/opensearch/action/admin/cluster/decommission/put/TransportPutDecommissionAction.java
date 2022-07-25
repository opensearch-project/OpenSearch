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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.decommission.DecommissionService;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportPutDecommissionAction extends TransportClusterManagerNodeAction<
    PutDecommissionRequest,
    PutDecommissionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutDecommissionAction.class);

    private final DecommissionService decommissionService;

    private volatile List<String> decommissionedNodesID;

    @Inject
    public TransportPutDecommissionAction(
        TransportService transportService,
        ClusterService clusterService,
        DecommissionService decommissionService,
        ThreadPool threadPool,
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
    protected PutDecommissionResponse read(StreamInput in) throws IOException {
        return new PutDecommissionResponse(in);
    }

    @Override
    protected void masterOperation(
        PutDecommissionRequest request,
        ClusterState state,
        ActionListener<PutDecommissionResponse> listener) throws Exception {
        decommissionService.registerDecommissionAttribute(
            request,
            ActionListener.delegateFailure(
                listener,
                (delegatedListener, response) -> delegatedListener.onResponse(new PutDecommissionResponse(response.isAcknowledged()))
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(PutDecommissionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
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
