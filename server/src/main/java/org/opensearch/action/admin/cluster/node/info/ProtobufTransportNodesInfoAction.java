/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.info;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.action.ProtobufFailedNodeException;
import org.opensearch.action.support.ProtobufActionFilters;
import org.opensearch.action.support.nodes.ProtobufTransportNodesAction;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.node.ProtobufNodeService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ProtobufTransportRequest;
import org.opensearch.transport.ProtobufTransportService;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Transport action for OpenSearch Node Information
*
* @opensearch.internal
*/
public class ProtobufTransportNodesInfoAction extends ProtobufTransportNodesAction<
    ProtobufNodesInfoRequest,
    ProtobufNodesInfoResponse,
    ProtobufTransportNodesInfoAction.NodeInfoRequest,
    ProtobufNodeInfo> {

    private final ProtobufNodeService nodeService;

    @Inject
    public ProtobufTransportNodesInfoAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        ProtobufTransportService transportService,
        ProtobufNodeService nodeService,
        ProtobufActionFilters actionFilters
    ) {
        super(
            NodesInfoAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ProtobufNodesInfoRequest::new,
            NodeInfoRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ProtobufNodeInfo.class
        );
        this.nodeService = nodeService;
    }

    @Override
    protected ProtobufNodesInfoResponse newResponse(
        ProtobufNodesInfoRequest nodesInfoRequest,
        List<ProtobufNodeInfo> responses,
        List<ProtobufFailedNodeException> failures
    ) {
        return new ProtobufNodesInfoResponse(new ClusterName(clusterService.getClusterName().value()), responses, failures);
    }

    @Override
    protected NodeInfoRequest newNodeRequest(ProtobufNodesInfoRequest request) {
        return new NodeInfoRequest(request);
    }

    @Override
    protected ProtobufNodeInfo newNodeResponse(CodedInputStream in) throws IOException {
        return new ProtobufNodeInfo(in);
    }

    @Override
    protected ProtobufNodeInfo nodeOperation(NodeInfoRequest nodeRequest) {
        ProtobufNodesInfoRequest request = nodeRequest.request;
        Set<String> metrics = request.requestedMetrics();
        ProtobufNodeInfo protobufNodeInfo = nodeService.info(
            metrics.contains(ProtobufNodesInfoRequest.Metric.SETTINGS.metricName()),
            metrics.contains(ProtobufNodesInfoRequest.Metric.OS.metricName()),
            metrics.contains(ProtobufNodesInfoRequest.Metric.PROCESS.metricName()),
            metrics.contains(ProtobufNodesInfoRequest.Metric.JVM.metricName()),
            metrics.contains(ProtobufNodesInfoRequest.Metric.THREAD_POOL.metricName()),
            metrics.contains(ProtobufNodesInfoRequest.Metric.TRANSPORT.metricName()),
            metrics.contains(ProtobufNodesInfoRequest.Metric.HTTP.metricName()),
            metrics.contains(ProtobufNodesInfoRequest.Metric.PLUGINS.metricName()),
            metrics.contains(ProtobufNodesInfoRequest.Metric.INGEST.metricName()),
            metrics.contains(ProtobufNodesInfoRequest.Metric.AGGREGATIONS.metricName()),
            metrics.contains(ProtobufNodesInfoRequest.Metric.INDICES.metricName()),
            metrics.contains(ProtobufNodesInfoRequest.Metric.SEARCH_PIPELINES.metricName())
        );
        return protobufNodeInfo;
    }

    /**
     * Inner Node Info Request
    *
    * @opensearch.internal
    */
    public static class NodeInfoRequest extends ProtobufTransportRequest {

        ProtobufNodesInfoRequest request;

        public NodeInfoRequest(CodedInputStream in) throws IOException {
            super(in);
            request = new ProtobufNodesInfoRequest(in);
        }

        public NodeInfoRequest(ProtobufNodesInfoRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
