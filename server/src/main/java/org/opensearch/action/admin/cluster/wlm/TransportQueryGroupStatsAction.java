/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.wlm;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.wlm.QueryGroupService;
import org.opensearch.wlm.stats.QueryGroupStats;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for obtaining QueryGroupStats
 *
 * @opensearch.experimental
 */
public class TransportQueryGroupStatsAction extends TransportNodesAction<
    QueryGroupStatsRequest,
    QueryGroupStatsResponse,
    QueryGroupStatsRequest,
    QueryGroupStats> {

    QueryGroupService queryGroupService;

    @Inject
    public TransportQueryGroupStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        QueryGroupService queryGroupService,
        ActionFilters actionFilters
    ) {
        super(
            QueryGroupStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            QueryGroupStatsRequest::new,
            QueryGroupStatsRequest::new,
            ThreadPool.Names.MANAGEMENT,
            QueryGroupStats.class
        );
        this.queryGroupService = queryGroupService;
    }

    @Override
    protected QueryGroupStatsResponse newResponse(
        QueryGroupStatsRequest request,
        List<QueryGroupStats> queryGroupStats,
        List<FailedNodeException> failures
    ) {
        return new QueryGroupStatsResponse(clusterService.getClusterName(), queryGroupStats, failures);
    }

    @Override
    protected QueryGroupStatsRequest newNodeRequest(QueryGroupStatsRequest request) {
        return request;
    }

    @Override
    protected QueryGroupStats newNodeResponse(StreamInput in) throws IOException {
        return new QueryGroupStats(in);
    }

    @Override
    protected QueryGroupStats nodeOperation(QueryGroupStatsRequest queryGroupStatsRequest) {
        return queryGroupService.nodeStats(queryGroupStatsRequest.getQueryGroupIds(), queryGroupStatsRequest.isBreach());
    }
}
