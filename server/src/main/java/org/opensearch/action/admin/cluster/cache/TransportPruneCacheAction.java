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
import java.util.List;
import java.util.Objects;
import java.util.Set;
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

        // First collect all warm nodes from the cluster (more efficient approach)
        List<DiscoveryNode> allWarmNodes = clusterState.nodes()
            .getNodes()
            .values()
            .stream()
            .filter(DiscoveryNode::isWarmNode)
            .collect(Collectors.toList());

        List<DiscoveryNode> warmNodes;

        // If specific nodes are requested, take intersection with warm nodes
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
            // No specific nodes requested - use all warm nodes
            warmNodes = allWarmNodes;
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
