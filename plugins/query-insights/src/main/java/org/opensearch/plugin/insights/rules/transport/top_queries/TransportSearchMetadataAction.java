/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.top_queries;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.top_queries.SearchMetadata;
import org.opensearch.plugin.insights.rules.action.top_queries.SearchMetadataAction;
import org.opensearch.plugin.insights.rules.action.top_queries.SearchMetadataRequest;
import org.opensearch.plugin.insights.rules.action.top_queries.SearchMetadataResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for cluster/node level top queries information.
 *
 * @opensearch.internal
 */
public class TransportSearchMetadataAction extends TransportNodesAction<
    SearchMetadataRequest,
    SearchMetadataResponse,
    TransportSearchMetadataAction.NodeRequest,
    SearchMetadata> {

    private final QueryInsightsService queryInsightsService;

    /**
     * Create the TransportTopQueriesAction Object

     * @param threadPool The OpenSearch thread pool to run async tasks
     * @param clusterService The clusterService of this node
     * @param transportService The TransportService of this node
     * @param queryInsightsService The topQueriesByLatencyService associated with this Transport Action
     * @param actionFilters the action filters
     */
    @Inject
    public TransportSearchMetadataAction(
        final ThreadPool threadPool,
        final ClusterService clusterService,
        final TransportService transportService,
        final QueryInsightsService queryInsightsService,
        final ActionFilters actionFilters
    ) {
        super(
            SearchMetadataAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            SearchMetadataRequest::new,
            NodeRequest::new,
            ThreadPool.Names.GENERIC,
            SearchMetadata.class
        );
        this.queryInsightsService = queryInsightsService;
    }

    @Override
    protected SearchMetadataResponse newResponse(
        final SearchMetadataRequest searchMetadataRequest,
        final List<SearchMetadata> responses,
        final List<FailedNodeException> failures
    ) {
        return new SearchMetadataResponse(
            clusterService.getClusterName(),
            responses,
            failures
        );
    }

    @Override
    protected NodeRequest newNodeRequest(final SearchMetadataRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected SearchMetadata newNodeResponse(final StreamInput in) throws IOException {
        return new SearchMetadata(in);
    }

    @Override
    protected SearchMetadata nodeOperation(final NodeRequest nodeRequest) {
        final SearchMetadataRequest request = nodeRequest.request;
        return new SearchMetadata(
            clusterService.localNode(),
            queryInsightsService.getQueryRecordsList(),
            queryInsightsService.getTaskRecordsList(),
            queryInsightsService.getTaskStatusMap()
        );
    }

    /**
     * Inner Node Top Queries Request
     *
     * @opensearch.internal
     */
    public static class NodeRequest extends TransportRequest {

        final SearchMetadataRequest request;

        /**
         * Create the NodeResponse object from StreamInput
         *
         * @param in the StreamInput to read the object
         * @throws IOException IOException
         */
        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new SearchMetadataRequest(in);
        }

        /**
         * Create the NodeResponse object from a TopQueriesRequest
         * @param request the TopQueriesRequest object
         */
        public NodeRequest(final SearchMetadataRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
