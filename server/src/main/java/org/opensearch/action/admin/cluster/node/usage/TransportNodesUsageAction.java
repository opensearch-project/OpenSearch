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

package org.opensearch.action.admin.cluster.node.usage;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.aggregations.support.AggregationUsageService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;
import org.opensearch.usage.UsageService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Transport action for collecting OpenSearch telemetry
 *
 * @opensearch.internal
 */
public class TransportNodesUsageAction extends TransportNodesAction<
    NodesUsageRequest,
    NodesUsageResponse,
    TransportNodesUsageAction.NodeUsageRequest,
    NodeUsage> {

    private final UsageService restUsageService;
    private final AggregationUsageService aggregationUsageService;
    private final long sinceTime;

    @Inject
    public TransportNodesUsageAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        UsageService restUsageService,
        AggregationUsageService aggregationUsageService
    ) {
        super(
            NodesUsageAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            NodesUsageRequest::new,
            NodeUsageRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodeUsage.class
        );
        this.restUsageService = restUsageService;
        this.aggregationUsageService = aggregationUsageService;
        this.sinceTime = System.currentTimeMillis();
    }

    @Override
    protected NodesUsageResponse newResponse(NodesUsageRequest request, List<NodeUsage> responses, List<FailedNodeException> failures) {
        return new NodesUsageResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeUsageRequest newNodeRequest(NodesUsageRequest request) {
        return new NodeUsageRequest(request);
    }

    @Override
    protected NodeUsage newNodeResponse(StreamInput in) throws IOException {
        return new NodeUsage(in);
    }

    @Override
    protected NodeUsage nodeOperation(NodeUsageRequest nodeUsageRequest) {
        NodesUsageRequest request = nodeUsageRequest.request;
        Map<String, Long> restUsage = request.restActions() ? restUsageService.getRestUsageStats() : null;
        Map<String, Object> aggsUsage = request.aggregations() ? aggregationUsageService.getUsageStats() : null;
        return new NodeUsage(clusterService.localNode(), System.currentTimeMillis(), sinceTime, restUsage, aggsUsage);
    }

    /**
     * Inner Node Usage Request
     *
     * @opensearch.internal
     */
    public static class NodeUsageRequest extends TransportRequest {

        NodesUsageRequest request;

        public NodeUsageRequest(StreamInput in) throws IOException {
            super(in);
            request = new NodesUsageRequest(in);
        }

        NodeUsageRequest(NodesUsageRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
