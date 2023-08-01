/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ProtobufActionFilters;
import org.opensearch.action.support.nodes.ProtobufTransportNodesAction;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.node.ProtobufNodeService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

/**
 * Transport action for obtaining OpenSearch Node Stats
*
* @opensearch.internal
*/
public class ProtobufTransportNodesStatsAction extends ProtobufTransportNodesAction<
    ProtobufNodesStatsRequest,
    ProtobufNodesStatsResponse,
    ProtobufTransportNodesStatsAction.NodeStatsRequest,
    ProtobufNodeStats> {

    private final ProtobufNodeService nodeService;

    @Inject
    public ProtobufTransportNodesStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ProtobufNodeService nodeService,
        ProtobufActionFilters actionFilters
    ) {
        super(
            NodesStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ProtobufNodesStatsRequest::new,
            NodeStatsRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ProtobufNodeStats.class
        );
        this.nodeService = nodeService;
    }

    @Override
    protected ProtobufNodesStatsResponse newResponse(
        ProtobufNodesStatsRequest request,
        List<ProtobufNodeStats> responses,
        List<FailedNodeException> failures
    ) {
        return new ProtobufNodesStatsResponse(new ClusterName(clusterService.getClusterName().value()), responses, failures);
    }

    @Override
    protected NodeStatsRequest newNodeRequest(ProtobufNodesStatsRequest request) {
        return new NodeStatsRequest(request);
    }

    @Override
    protected ProtobufNodeStats nodeOperation(NodeStatsRequest nodeStatsRequest) {
        ProtobufNodesStatsRequest request = nodeStatsRequest.request;
        Set<String> metrics = request.requestedMetrics();
        ProtobufNodeStats protobufNodeStats = nodeService.stats(
            request.indices(),
            ProtobufNodesStatsRequest.Metric.OS.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.PROCESS.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.JVM.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.THREAD_POOL.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.FS.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.TRANSPORT.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.HTTP.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.BREAKER.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.SCRIPT.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.DISCOVERY.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.INGEST.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.ADAPTIVE_SELECTION.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.SCRIPT_CACHE.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.INDEXING_PRESSURE.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.SHARD_INDEXING_PRESSURE.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.SEARCH_BACKPRESSURE.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.CLUSTER_MANAGER_THROTTLING.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.WEIGHTED_ROUTING_STATS.containedIn(metrics),
            ProtobufNodesStatsRequest.Metric.FILE_CACHE_STATS.containedIn(metrics)
        );
        return protobufNodeStats;
    }

    /**
     * Inner Node Stats Request
    *
    * @opensearch.internal
    */
    public static class NodeStatsRequest extends TransportRequest {

        ProtobufNodesStatsRequest request;

        public NodeStatsRequest(byte[] data) throws IOException {
            request = new ProtobufNodesStatsRequest(data);
        }

        public NodeStatsRequest(ProtobufNodesStatsRequest request) {
            this.request = request;
        }

        public ProtobufNodesStatsRequest request() {
            return request;
        }

        @Override
        public void writeTo(OutputStream out) throws IOException {
            request.writeTo(out);
        }
    }

    @Override
    protected ProtobufNodeStats newNodeResponse(byte[] in) throws IOException {
        return new ProtobufNodeStats(in);
    }
}
