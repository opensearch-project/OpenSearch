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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

public class TransportClusterManagementDecommissionAction
    extends TransportClusterManagerNodeAction<ClusterManagementDecommissionRequest, ClusterManagementDecommissionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterManagementDecommissionAction.class);
    private final AllocationService allocationService;

    @Inject
    public TransportClusterManagementDecommissionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        AllocationService allocationService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterManagementDecommissionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterManagementDecommissionRequest::new,
            indexNameExpressionResolver
        );
        this.allocationService = allocationService;
    }

    @Override
    protected String executor() {
        return null;
    }

    @Override
    protected ClusterManagementDecommissionResponse read(StreamInput in) throws IOException {
        return null;
    }

    @Override
    protected void masterOperation(ClusterManagementDecommissionRequest request, ClusterState state, ActionListener<ClusterManagementDecommissionResponse> listener) throws Exception {

    }

    @Override
    protected ClusterBlockException checkBlock(ClusterManagementDecommissionRequest request, ClusterState state) {
        return null;
    }
}
