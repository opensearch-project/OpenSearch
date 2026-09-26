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
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.opensearch.threadpool.ThreadPool.Names.SAME;

/**
 * Transport action to create WorkloadGroup
 *
 * @opensearch.experimental
 */
public class TransportCreateWorkloadGroupAction extends TransportClusterManagerNodeAction<
    CreateWorkloadGroupRequest,
    CreateWorkloadGroupResponse> {

    private final WorkloadGroupPersistenceService workloadGroupPersistenceService;

    /**
     * Constructor for TransportCreateWorkloadGroupAction
     *
     * @param threadPool - {@link ThreadPool} object
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param indexNameExpressionResolver - {@link IndexNameExpressionResolver} object
     * @param workloadGroupPersistenceService - a {@link WorkloadGroupPersistenceService} object
     */
    @Inject
    public TransportCreateWorkloadGroupAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        WorkloadGroupPersistenceService workloadGroupPersistenceService
    ) {
        super(
            CreateWorkloadGroupAction.NAME,
            transportService,
            workloadGroupPersistenceService.getClusterService(),
            threadPool,
            actionFilters,
            CreateWorkloadGroupRequest::new,
            indexNameExpressionResolver
        );
        this.workloadGroupPersistenceService = workloadGroupPersistenceService;
    }

    /**
     * Validates the throttling config on the node that accepted the request, before forwarding. {@code clusterManagerOperation}
     * alone is not enough: forwarding to a pre-{@code V_3_9_0} manager strips the wire-gated {@code throttling} field before
     * the manager-side check sees it (and an older manager has no such check), yielding a 200 for a group persisted without
     * throttling. Any node that can parse a {@code throttling} body is 3.9+, so checking here catches the pre-3.9 peer; the
     * manager-side call stays authoritative.
     *
     * @param task task associated with the request
     * @param request create workload group request
     * @param listener listener notified with the response or failure
     */
    @Override
    protected void doExecute(Task task, CreateWorkloadGroupRequest request, ActionListener<CreateWorkloadGroupResponse> listener) {
        try {
            WorkloadGroupPersistenceService.validateThrottlingIsEnforceable(
                request.getWorkloadGroup().getMutableWorkloadGroupFragment().getThrottling(),
                clusterService.state()
            );
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        super.doExecute(task, request, listener);
    }

    @Override
    protected void clusterManagerOperation(
        CreateWorkloadGroupRequest request,
        ClusterState clusterState,
        ActionListener<CreateWorkloadGroupResponse> listener
    ) {
        try {
            WorkloadGroupPersistenceService.validateThrottlingIsEnforceable(
                request.getWorkloadGroup().getMutableWorkloadGroupFragment().getThrottling(),
                clusterState
            );
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        workloadGroupPersistenceService.persistInClusterStateMetadata(request.getWorkloadGroup(), listener);
    }

    @Override
    protected String executor() {
        return SAME;
    }

    @Override
    protected CreateWorkloadGroupResponse read(StreamInput in) throws IOException {
        return new CreateWorkloadGroupResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateWorkloadGroupRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
