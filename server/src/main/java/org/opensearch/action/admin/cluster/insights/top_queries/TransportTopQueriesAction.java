/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


package org.opensearch.action.admin.cluster.insights.top_queries;

import org.opensearch.OpenSearchException;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.search.QueryLatencyInsightService;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
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
public class TransportTopQueriesAction extends TransportNodesAction<
    TopQueriesRequest,
    TopQueriesResponse,
    TransportTopQueriesAction.NodeRequest,
    TopQueries> {

    private final QueryLatencyInsightService queryLatencyInsightService;

    @Inject
    public TransportTopQueriesAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        QueryLatencyInsightService queryLatencyInsightService,
        ActionFilters actionFilters
    ) {
        super(
            TopQueriesAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            TopQueriesRequest::new,
            NodeRequest::new,
            ThreadPool.Names.GENERIC,
            TopQueries.class
        );
        this.queryLatencyInsightService = queryLatencyInsightService;
    }

    @Override
    protected TopQueriesResponse newResponse(
        TopQueriesRequest topQueriesRequest,
        List<TopQueries> responses,
        List<FailedNodeException> failures
    ) {
        return new TopQueriesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(TopQueriesRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected TopQueries newNodeResponse(StreamInput in) throws IOException {
        return new TopQueries(in);
    }

    @Override
    protected TopQueries nodeOperation(NodeRequest nodeRequest) {
        TopQueriesRequest request = nodeRequest.request;
        if (request.getMetricType() == TopQueriesRequest.Metric.LATENCY) {
            return new TopQueries(
                clusterService.localNode(),
                queryLatencyInsightService.getQueryData()
            );
        } else {
            throw new OpenSearchException(String.format("invalid metric type %s", request.getMetricType()));
        }

    }

    /**
     * Inner Node Top Queries Request
     *
     * @opensearch.internal
     */
    public static class NodeRequest extends TransportRequest {

        TopQueriesRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new TopQueriesRequest(in);
        }

        public NodeRequest(TopQueriesRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
