/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.awarenesshealth;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for obtaining Cluster Awareness Health
 *
 * @opensearch.internal
 */
public class TransportClusterAwarenessHealthAction extends TransportClusterManagerNodeReadAction<
    ClusterAwarenessHealthRequest,
    ClusterAwarenessHealthResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterAwarenessHealthAction.class);

    private final AllocationService allocationService;

    @Inject
    public TransportClusterAwarenessHealthAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AllocationService allocationService
    ) {
        super(
            ClusterAwarenessHealthAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterAwarenessHealthRequest::new,
            indexNameExpressionResolver
        );
        this.allocationService = allocationService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterAwarenessHealthResponse read(StreamInput in) throws IOException {
        return new ClusterAwarenessHealthResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterAwarenessHealthRequest request, ClusterState state) {
        // we want users to be able to call this even when there are global blocks
        return null;
    }

    @Override
    protected final void clusterManagerOperation(
        ClusterAwarenessHealthRequest request,
        ClusterState state,
        ActionListener<ClusterAwarenessHealthResponse> listener
    ) throws Exception {
        logger.warn("attempted to execute a cluster health operation without a task");
        throw new UnsupportedOperationException("task parameter is required for this operation");
    }

    @Override
    protected void clusterManagerOperation(
        final Task task,
        ClusterAwarenessHealthRequest request,
        ClusterState state,
        ActionListener<ClusterAwarenessHealthResponse> listener
    ) {
        ClusterSettings currentSettings = clusterService.getClusterSettings();
        String awarenessAttributeName = request.getAttributeName();
        int numberOfPendingTasks = clusterService.getClusterManagerService().numberOfPendingTasks();
        TimeValue maxWaitTime = clusterService.getClusterManagerService().getMaxTaskWaitTime();
        int numberOfInflightFetches = allocationService.getNumberOfInFlightFetches();
        int delayedUnassignedShards = UnassignedInfo.getNumberOfDelayedUnassigned(state);
        listener.onResponse(
            getResponse(
                state,
                currentSettings,
                awarenessAttributeName,
                numberOfPendingTasks,
                maxWaitTime,
                numberOfInflightFetches,
                delayedUnassignedShards
            )
        );
    }

    private ClusterAwarenessHealthResponse getResponse(
        ClusterState clusterState,
        ClusterSettings clusterSettings,
        String awarenessAttributeName,
        int numberOfPendingTasks,
        TimeValue maxWeightTime,
        int numberOfInflightFetches,
        int delayedUnassignedShards
    ) {
        return new ClusterAwarenessHealthResponse(
            clusterState,
            clusterSettings,
            awarenessAttributeName,
            numberOfPendingTasks,
            maxWeightTime,
            numberOfInflightFetches,
            delayedUnassignedShards
        );
    }
}
