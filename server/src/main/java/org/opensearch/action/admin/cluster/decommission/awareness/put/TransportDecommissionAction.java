/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for registering decommission
 *
 * @opensearch.internal
 */
public class TransportDecommissionAction extends TransportClusterManagerNodeAction<DecommissionRequest, DecommissionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDecommissionAction.class);

    @Inject
    public TransportDecommissionAction(
        TransportService transportService,
        ClusterService clusterService,
        // DecommissionService decommissionService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            DecommissionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DecommissionRequest::new,
            indexNameExpressionResolver
        );
        // TODO - uncomment when integrating with the service
        // this.decommissionService = decommissionService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DecommissionResponse read(StreamInput in) throws IOException {
        return new DecommissionResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(DecommissionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(DecommissionRequest request, ClusterState state, ActionListener<DecommissionResponse> listener)
        throws Exception {
        logger.info("initiating awareness attribute [{}] decommissioning", request.getDecommissionAttribute().toString());
        listener.onResponse(new DecommissionResponse(true)); // TODO - remove after integration
        // TODO - uncomment when integrating with the service
        // decommissionService.initiateAttributeDecommissioning(
        // request.getDecommissionAttribute(),
        // listener,
        // state
        // );
    }
}
