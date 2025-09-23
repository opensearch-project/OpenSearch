/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.cache;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Transport action for pruning remote file cache across multiple nodes.
 *
 * @opensearch.internal
 */
public class TransportPruneCacheAction extends TransportNodesAction<
    PruneCacheRequest,
    PruneCacheResponse,
    TransportPruneCacheAction.NodeRequest,
    NodePruneCacheResponse> {

    private final FileCache fileCache;

    @Inject
    public TransportPruneCacheAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        @Nullable FileCache fileCache
    ) {
        super(
            PruneCacheAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            PruneCacheRequest::new,
            NodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodePruneCacheResponse.class
        );
        this.fileCache = fileCache;
    }

    @Override
    protected void resolveRequest(PruneCacheRequest request, ClusterState clusterState) {
        assert request.concreteNodes() == null : "request concreteNodes shouldn't be set";

        String[] nodeIds = clusterState.nodes().resolveNodes(request.nodesIds());
        List<DiscoveryNode> warmNodes = Arrays.stream(nodeIds)
            .map(clusterState.nodes()::get)
            .filter(Objects::nonNull)
            .filter(DiscoveryNode::isWarmNode)
            .collect(Collectors.toList());

        if (warmNodes.isEmpty() && nodeIds.length > 0) {
            throw new IllegalArgumentException(
                "No warm nodes found matching the specified criteria. " + "FileCache operations can only target warm nodes."
            );
        }

        if (warmNodes.isEmpty() && nodeIds.length == 0) {
            warmNodes = clusterState.nodes().getNodes().values().stream().filter(DiscoveryNode::isWarmNode).collect(Collectors.toList());
        }

        request.setConcreteNodes(warmNodes.toArray(new DiscoveryNode[warmNodes.size()]));
    }

    @Override
    protected PruneCacheResponse newResponse(
        PruneCacheRequest request,
        List<NodePruneCacheResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new PruneCacheResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(PruneCacheRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodePruneCacheResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodePruneCacheResponse(in);
    }

    @Override
    protected NodePruneCacheResponse nodeOperation(NodeRequest nodeRequest) {
        PruneCacheRequest request = nodeRequest.getRequest();

        if (fileCache == null) {
            return new NodePruneCacheResponse(transportService.getLocalNode(), 0, 0);
        }

        try {
            long capacity = fileCache.capacity();
            long prunedBytes = fileCache.prune();

            return new NodePruneCacheResponse(transportService.getLocalNode(), prunedBytes, capacity);

        } catch (Exception e) {
            throw new RuntimeException("FileCache prune operation failed on node " + transportService.getLocalNode().getId(), e);
        }
    }

    /**
     * Node-level request for cache pruning operation.
     */
    public static class NodeRequest extends TransportRequest {
        private PruneCacheRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new PruneCacheRequest(in);
        }

        public NodeRequest(PruneCacheRequest request) {
            this.request = Objects.requireNonNull(request, "PruneCacheRequest cannot be null");
        }

        public PruneCacheRequest getRequest() {
            return request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
