/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.opensearch.threadpool.ThreadPool.Names.SAME;

/**
 * Transport action to update WorkloadGroup
 *
 * @opensearch.experimental
 */
public class TransportUpdateWorkloadGroupAction extends TransportClusterManagerNodeAction<
    UpdateWorkloadGroupRequest,
    UpdateWorkloadGroupResponse> {

    private final WorkloadGroupPersistenceService workloadGroupPersistenceService;

    /**
     * Constructor for TransportUpdateWorkloadGroupAction
     *
     * @param threadPool - {@link ThreadPool} object
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param indexNameExpressionResolver - {@link IndexNameExpressionResolver} object
     * @param workloadGroupPersistenceService - a {@link WorkloadGroupPersistenceService} object
     */
    @Inject
    public TransportUpdateWorkloadGroupAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        WorkloadGroupPersistenceService workloadGroupPersistenceService
    ) {
        super(
            UpdateWorkloadGroupAction.NAME,
            transportService,
            workloadGroupPersistenceService.getClusterService(),
            threadPool,
            actionFilters,
            UpdateWorkloadGroupRequest::new,
            indexNameExpressionResolver
        );
        this.workloadGroupPersistenceService = workloadGroupPersistenceService;
    }

    @Override
    protected void clusterManagerOperation(
        UpdateWorkloadGroupRequest request,
        ClusterState clusterState,
        ActionListener<UpdateWorkloadGroupResponse> listener
    ) {
        workloadGroupPersistenceService.updateInClusterStateMetadata(request, listener);
    }

    @Override
    protected String executor() {
        return SAME;
    }

    @Override
    protected UpdateWorkloadGroupResponse read(StreamInput in) throws IOException {
        return new UpdateWorkloadGroupResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateWorkloadGroupRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
