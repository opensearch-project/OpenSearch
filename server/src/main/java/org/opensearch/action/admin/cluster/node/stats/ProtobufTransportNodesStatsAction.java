/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.stats;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.action.ProtobufFailedNodeException;
import org.opensearch.action.support.ProtobufActionFilters;
import org.opensearch.action.support.nodes.ProtobufTransportNodesAction;
import org.opensearch.cluster.ProtobufClusterName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.node.ProtobufNodeService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ProtobufTransportRequest;
import org.opensearch.transport.ProtobufTransportService;

import java.io.IOException;
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
        ProtobufTransportService transportService,
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
    protected ProtobufNodesStatsResponse newResponse(ProtobufNodesStatsRequest request, List<ProtobufNodeStats> responses, List<ProtobufFailedNodeException> failures) {
        return new ProtobufNodesStatsResponse(new ProtobufClusterName(clusterService.getClusterName().value()), responses, failures);
    }

    @Override
    protected NodeStatsRequest newNodeRequest(ProtobufNodesStatsRequest request) {
        return new NodeStatsRequest(request);
    }

    @Override
    protected ProtobufNodeStats newNodeResponse(CodedInputStream in) throws IOException {
        return new ProtobufNodeStats(in);
    }

    @Override
    protected ProtobufNodeStats nodeOperation(NodeStatsRequest nodeStatsRequest) {
        ProtobufNodesStatsRequest request = nodeStatsRequest.request;
        Set<String> metrics = request.requestedMetrics();
        return nodeService.stats(
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
    }

    /**
     * Inner Node Stats Request
    *
    * @opensearch.internal
    */
    public static class NodeStatsRequest extends ProtobufTransportRequest {

        ProtobufNodesStatsRequest request;

        public NodeStatsRequest(CodedInputStream in) throws IOException {
            super(in);
            request = new ProtobufNodesStatsRequest(in);
        }

        NodeStatsRequest(ProtobufNodesStatsRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
