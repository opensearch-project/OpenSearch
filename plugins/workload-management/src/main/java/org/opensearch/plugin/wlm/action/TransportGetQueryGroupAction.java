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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.wlm.service.QueryGroupPersistenceService;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport action to get QueryGroup
 *
 * @opensearch.experimental
 */
public class TransportGetQueryGroupAction extends TransportClusterManagerNodeReadAction<GetQueryGroupRequest, GetQueryGroupResponse> {
    private static final Logger logger = LogManager.getLogger(SearchPipelineService.class);

    /**
     * Constructor for TransportGetQueryGroupAction
     *
     * @param clusterService - a {@link ClusterService} object
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param threadPool - a {@link ThreadPool} object
     * @param indexNameExpressionResolver - a {@link IndexNameExpressionResolver} object
     */
    @Inject
    public TransportGetQueryGroupAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetQueryGroupAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetQueryGroupRequest::new,
            indexNameExpressionResolver,
            true
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetQueryGroupResponse read(StreamInput in) throws IOException {
        return new GetQueryGroupResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(GetQueryGroupRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void clusterManagerOperation(GetQueryGroupRequest request, ClusterState state, ActionListener<GetQueryGroupResponse> listener)
        throws Exception {
        String name = request.getName();
        List<QueryGroup> resultGroups = QueryGroupPersistenceService.getFromClusterStateMetadata(name, state);

        if (resultGroups.isEmpty() && name != null && !name.isEmpty()) {
            logger.warn("No QueryGroup exists with the provided name: {}", name);
            throw new IllegalArgumentException("No QueryGroup exists with the provided name: " + name);
        }
        listener.onResponse(new GetQueryGroupResponse(resultGroups, RestStatus.OK));
    }
}
