/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;

/**
 * Transport action to get WorkloadGroup
 *
 * @opensearch.experimental
 */
public class TransportGetWorkloadGroupAction extends TransportClusterManagerNodeReadAction<
    GetWorkloadGroupRequest,
    GetWorkloadGroupResponse> {
    private static final Logger logger = LogManager.getLogger(SearchPipelineService.class);

    /**
     * Constructor for TransportGetWorkloadGroupAction
     *
     * @param clusterService - a {@link ClusterService} object
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param threadPool - a {@link ThreadPool} object
     * @param indexNameExpressionResolver - a {@link IndexNameExpressionResolver} object
     */
    @Inject
    public TransportGetWorkloadGroupAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetWorkloadGroupAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetWorkloadGroupRequest::new,
            indexNameExpressionResolver,
            true
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetWorkloadGroupResponse read(StreamInput in) throws IOException {
        return new GetWorkloadGroupResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(GetWorkloadGroupRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void clusterManagerOperation(
        GetWorkloadGroupRequest request,
        ClusterState state,
        ActionListener<GetWorkloadGroupResponse> listener
    ) throws Exception {
        final String name = request.getName();
        final Collection<WorkloadGroup> resultGroups = WorkloadGroupPersistenceService.getFromClusterStateMetadata(name, state);

        if (resultGroups.isEmpty() && name != null && !name.isEmpty()) {
            logger.warn("No WorkloadGroup exists with the provided name: {}", name);
            throw new ResourceNotFoundException("No WorkloadGroup exists with the provided name: " + name);
        }
        listener.onResponse(new GetWorkloadGroupResponse(resultGroups, RestStatus.OK));
    }
}
