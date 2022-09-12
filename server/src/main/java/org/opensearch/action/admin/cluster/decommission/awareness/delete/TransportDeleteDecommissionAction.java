/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.delete;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
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

public class TransportDeleteDecommissionAction extends TransportClusterManagerNodeAction<
    DeleteDecommissionRequest,
    DeleteDecommissionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteDecommissionAction.class);

    DecommissionService decommissionService;

    @Inject
    public TransportDeleteDecommissionAction(
        TransportService transportService,
        ClusterService clusterService,
        DecommissionService decommissionService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
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
        ActionListener<DeleteDecommissionResponse> listener
    ) {
        // TODO: Enable when service class change is merged
        logger.info("Received delete decommission Request");
        decommissionService.clearDecommissionStatus(new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                listener.onResponse(new DeleteDecommissionResponse(true));
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Recommission failed with exception " + e.getMessage());
                listener.onFailure(e);
            }
        });

    }
}
