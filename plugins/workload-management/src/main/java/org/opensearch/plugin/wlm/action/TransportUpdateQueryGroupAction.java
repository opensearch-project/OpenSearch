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
import org.opensearch.plugin.wlm.service.QueryGroupPersistenceService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.opensearch.threadpool.ThreadPool.Names.SAME;

/**
 * Transport action to update QueryGroup
 *
 * @opensearch.experimental
 */
public class TransportUpdateQueryGroupAction extends TransportClusterManagerNodeAction<UpdateQueryGroupRequest, UpdateQueryGroupResponse> {

    private final QueryGroupPersistenceService queryGroupPersistenceService;

    /**
     * Constructor for TransportUpdateQueryGroupAction
     *
     * @param threadPool - {@link ThreadPool} object
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param indexNameExpressionResolver - {@link IndexNameExpressionResolver} object
     * @param queryGroupPersistenceService - a {@link QueryGroupPersistenceService} object
     */
    @Inject
    public TransportUpdateQueryGroupAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        QueryGroupPersistenceService queryGroupPersistenceService
    ) {
        super(
            UpdateQueryGroupAction.NAME,
            transportService,
            queryGroupPersistenceService.getClusterService(),
            threadPool,
            actionFilters,
            UpdateQueryGroupRequest::new,
            indexNameExpressionResolver
        );
        this.queryGroupPersistenceService = queryGroupPersistenceService;
    }

    @Override
    protected void clusterManagerOperation(
        UpdateQueryGroupRequest request,
        ClusterState clusterState,
        ActionListener<UpdateQueryGroupResponse> listener
    ) {
        queryGroupPersistenceService.updateInClusterStateMetadata(request, listener);
    }

    @Override
    protected String executor() {
        return SAME;
    }

    @Override
    protected UpdateQueryGroupResponse read(StreamInput in) throws IOException {
        return new UpdateQueryGroupResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateQueryGroupRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
