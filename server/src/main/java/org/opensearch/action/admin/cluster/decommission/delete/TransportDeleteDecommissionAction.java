/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.delete;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsResponse;
import org.opensearch.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.decommission.DecommissionService;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportDeleteDecommissionAction extends TransportClusterManagerNodeAction<DeleteDecommissionRequest, DeleteDecommissionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteDecommissionAction.class);

    private final DecommissionService decommissionService;

    private final TransportClearVotingConfigExclusionsAction clearExclusionsAction;

    @Inject
    public TransportDeleteDecommissionAction(
            TransportService transportService,
            ClusterService clusterService,
            DecommissionService decommissionService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            TransportClearVotingConfigExclusionsAction clearExclusionsAction
    ) {
        super(
                DeleteDecommissionAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                DeleteDecommissionRequest::new,
                indexNameExpressionResolver
        );
        this.decommissionService = decommissionService;
        this.clearExclusionsAction = clearExclusionsAction;
        // TODO Decide on the frequency
//        this.decommissionedNodeRequestCheckInterval = TimeValue.timeValueMillis(3000);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DeleteDecommissionResponse read(StreamInput in) throws IOException {
        return new DeleteDecommissionResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDecommissionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
            DeleteDecommissionRequest request,
            ClusterState state,
            ActionListener<DeleteDecommissionResponse> listener) throws Exception {

        boolean currentMasterRecommission = false;
        DiscoveryNode masterNode = clusterService.state().getNodes().getMasterNode();
        for (String decommissionedZone : request.getDecommissionAttribute().attributeValues()) {
            if (masterNode.getAttributes().get(request.getDecommissionAttribute().attributeName()).equals(decommissionedZone)) {
                currentMasterRecommission = true;
            }
        }

        if (currentMasterRecommission) {
            logger.info("Removing Master voting exclusion configuration.");
            ActionListener<ClearVotingConfigExclusionsResponse> addVotingConfigExclusionsListener = new ActionListener<>() {
                @Override
                public void onResponse(ClearVotingConfigExclusionsResponse clearVotingConfigExclusionsResponse) {
                    logger.info("Master included - Response received");
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            };
            clearExclusionsAction.execute(new ClearVotingConfigExclusionsRequest(), addVotingConfigExclusionsListener);
            throw new NotClusterManagerException("abdicated");
        }

        decommissionService.registerRecommissionAttribute(
                request,
                ActionListener.delegateFailure(
                        listener,
                        (delegatedListener, response) -> delegatedListener.onResponse(new DeleteDecommissionResponse(response.isAcknowledged()))
                )
        );
    }
}
