/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.filecache;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Transport action for pruning remote file cache across multiple nodes.
 *
 * @opensearch.internal
 */
public class TransportPruneFileCacheAction extends TransportNodesAction<
    PruneFileCacheRequest,
    PruneFileCacheResponse,
    TransportPruneFileCacheAction.NodeRequest,
    NodePruneFileCacheResponse> {

    private final FileCache fileCache;

    @Inject
    public TransportPruneFileCacheAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        @Nullable FileCache fileCache
    ) {
        super(
            PruneFileCacheAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            PruneFileCacheRequest::new,
            NodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodePruneFileCacheResponse.class
        );
        this.fileCache = fileCache;
    }

    @Override
    protected void resolveRequest(PruneFileCacheRequest request, ClusterState clusterState) {
        assert request.concreteNodes() == null : "request concreteNodes shouldn't be set";

        List<DiscoveryNode> allWarmNodes = new ArrayList<>(clusterState.nodes().getWarmNodes().values());

        List<DiscoveryNode> warmNodes;
        if (request.nodesIds() != null && request.nodesIds().length > 0) {
            String[] resolvedNodeIds = clusterState.nodes().resolveNodes(request.nodesIds());
            Set<String> requestedIds = Set.of(resolvedNodeIds);

            warmNodes = allWarmNodes.stream().filter(node -> requestedIds.contains(node.getId())).collect(Collectors.toList());

            if (warmNodes.isEmpty()) {
                throw new IllegalArgumentException(
                    "No warm nodes found matching the specified criteria. " + "FileCache operations can only target warm nodes."
                );
            }
        } else {
            warmNodes = allWarmNodes;
        }

        request.setConcreteNodes(warmNodes.toArray(new DiscoveryNode[warmNodes.size()]));
    }

    @Override
    protected PruneFileCacheResponse newResponse(
        PruneFileCacheRequest request,
        List<NodePruneFileCacheResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new PruneFileCacheResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(PruneFileCacheRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodePruneFileCacheResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodePruneFileCacheResponse(in);
    }

    @Override
    protected NodePruneFileCacheResponse nodeOperation(NodeRequest nodeRequest) {
        if (fileCache == null) {
            return new NodePruneFileCacheResponse(transportService.getLocalNode(), 0, 0);
        }

        try {
            long capacity = fileCache.capacity();
            long prunedBytes = fileCache.prune();

            return new NodePruneFileCacheResponse(transportService.getLocalNode(), prunedBytes, capacity);

        } catch (Exception e) {
            throw new RuntimeException("FileCache prune operation failed on node " + transportService.getLocalNode().getId(), e);
        }
    }

    /**
     * Node-level request for cache pruning operation.
     */
    public static class NodeRequest extends TransportRequest {
        private PruneFileCacheRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new PruneFileCacheRequest(in);
        }

        public NodeRequest(PruneFileCacheRequest request) {
            this.request = Objects.requireNonNull(request, "PruneFileCacheRequest cannot be null");
        }

        public PruneFileCacheRequest getRequest() {
            return request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
