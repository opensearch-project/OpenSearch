/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.node.info;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.node.NodeService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Transport action for OpenSearch Node Information
 *
 * @opensearch.internal
 */
public class TransportNodesInfoAction extends TransportNodesAction<
    NodesInfoRequest,
    NodesInfoResponse,
    TransportNodesInfoAction.NodeInfoRequest,
    NodeInfo> {

    private final NodeService nodeService;

    @Inject
    public TransportNodesInfoAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        NodeService nodeService,
        ActionFilters actionFilters
    ) {
        super(
            NodesInfoAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            NodesInfoRequest::new,
            NodeInfoRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodeInfo.class
        );
        this.nodeService = nodeService;
    }

    @Override
    protected NodesInfoResponse newResponse(
        NodesInfoRequest nodesInfoRequest,
        List<NodeInfo> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesInfoResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeInfoRequest newNodeRequest(NodesInfoRequest request) {
        return new NodeInfoRequest(request);
    }

    @Override
    protected NodeInfo newNodeResponse(StreamInput in) throws IOException {
        return new NodeInfo(in);
    }

    @Override
    protected NodeInfo nodeOperation(NodeInfoRequest nodeRequest) {
        NodesInfoRequest request = nodeRequest.request;
        Set<String> metrics = request.requestedMetrics();
        return nodeService.info(
            metrics.contains(NodesInfoRequest.Metric.SETTINGS.metricName()),
            metrics.contains(NodesInfoRequest.Metric.OS.metricName()),
            metrics.contains(NodesInfoRequest.Metric.PROCESS.metricName()),
            metrics.contains(NodesInfoRequest.Metric.JVM.metricName()),
            metrics.contains(NodesInfoRequest.Metric.THREAD_POOL.metricName()),
            metrics.contains(NodesInfoRequest.Metric.TRANSPORT.metricName()),
            metrics.contains(NodesInfoRequest.Metric.HTTP.metricName()),
            metrics.contains(NodesInfoRequest.Metric.PLUGINS.metricName()),
            metrics.contains(NodesInfoRequest.Metric.INGEST.metricName()),
            metrics.contains(NodesInfoRequest.Metric.AGGREGATIONS.metricName()),
            metrics.contains(NodesInfoRequest.Metric.INDICES.metricName()),
            metrics.contains(NodesInfoRequest.Metric.SEARCH_PIPELINES.metricName())
        );
    }

    /**
     * Inner Node Info Request
     *
     * @opensearch.internal
     */
    public static class NodeInfoRequest extends TransportRequest {

        NodesInfoRequest request;

        public NodeInfoRequest(StreamInput in) throws IOException {
            super(in);
            request = new NodesInfoRequest(in);
        }

        public NodeInfoRequest(NodesInfoRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
