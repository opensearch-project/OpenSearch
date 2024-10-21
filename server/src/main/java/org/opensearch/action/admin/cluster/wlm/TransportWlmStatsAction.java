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
import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.wlm.QueryGroupService;
import org.opensearch.wlm.stats.WlmStats;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for obtaining WlmStats
 *
 * @opensearch.experimental
 */
public class TransportWlmStatsAction extends TransportNodesAction<
    WlmStatsRequest,
    WlmStatsResponse,
    TransportWlmStatsAction.NodeWlmStatsRequest,
    WlmStats> {

    final QueryGroupService queryGroupService;

    @Inject
    public TransportWlmStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        QueryGroupService queryGroupService,
        ActionFilters actionFilters
    ) {
        super(
            WlmStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            WlmStatsRequest::new,
            NodeWlmStatsRequest::new,
            ThreadPool.Names.MANAGEMENT,
            WlmStats.class
        );
        this.queryGroupService = queryGroupService;
    }

    @Override
    protected WlmStatsResponse newResponse(WlmStatsRequest request, List<WlmStats> wlmStats, List<FailedNodeException> failures) {
        return new WlmStatsResponse(clusterService.getClusterName(), wlmStats, failures);
    }

    @Override
    protected NodeWlmStatsRequest newNodeRequest(WlmStatsRequest request) {
        return new NodeWlmStatsRequest(request);
    }

    @Override
    protected WlmStats newNodeResponse(StreamInput in) throws IOException {
        return new WlmStats(in);
    }

    @Override
    protected WlmStats nodeOperation(NodeWlmStatsRequest nodeWlmStatsRequest) {
        assert transportService.getLocalNode() != null;
        WlmStatsRequest request = nodeWlmStatsRequest.request;
        return new WlmStats(transportService.getLocalNode(), queryGroupService.nodeStats(request.getQueryGroupIds(), request.isBreach()));
    }

    /**
     * Inner WlmStatsRequest
     *
     * @opensearch.experimental
     */
    public static class NodeWlmStatsRequest extends BaseNodeRequest {

        protected WlmStatsRequest request;

        public NodeWlmStatsRequest(StreamInput in) throws IOException {
            super(in);
            request = new WlmStatsRequest(in);
        }

        NodeWlmStatsRequest(WlmStatsRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }

        public DiscoveryNode[] getDiscoveryNodes() {
            return this.request.concreteNodes();
        }
    }
}
