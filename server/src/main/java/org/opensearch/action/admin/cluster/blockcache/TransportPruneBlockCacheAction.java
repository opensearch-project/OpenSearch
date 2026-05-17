/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.blockcache;

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
import org.opensearch.plugins.BlockCache;
import org.opensearch.plugins.BlockCacheRegistry;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Transport action for pruning a named block cache across warm nodes.
 *
 * @opensearch.internal
 */
public class TransportPruneBlockCacheAction extends TransportNodesAction<
    PruneBlockCacheRequest,
    PruneBlockCacheResponse,
    TransportPruneBlockCacheAction.NodeRequest,
    NodePruneBlockCacheResponse> {

    private final BlockCacheRegistry blockCacheRegistry;

    @Inject
    public TransportPruneBlockCacheAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        @Nullable BlockCacheRegistry blockCacheRegistry
    ) {
        super(
            PruneBlockCacheAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            PruneBlockCacheRequest::new,
            NodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodePruneBlockCacheResponse.class
        );
        this.blockCacheRegistry = blockCacheRegistry;
    }

    @Override
    protected void resolveRequest(PruneBlockCacheRequest request, ClusterState clusterState) {
        assert request.concreteNodes() == null : "request concreteNodes shouldn't be set";

        List<DiscoveryNode> allWarmNodes = new ArrayList<>(clusterState.nodes().getWarmNodes().values());

        List<DiscoveryNode> warmNodes;
        if (request.nodesIds() != null && request.nodesIds().length > 0) {
            String[] resolvedNodeIds = clusterState.nodes().resolveNodes(request.nodesIds());
            Set<String> requestedIds = Set.of(resolvedNodeIds);
            warmNodes = allWarmNodes.stream().filter(node -> requestedIds.contains(node.getId())).collect(Collectors.toList());
            if (warmNodes.isEmpty()) {
                throw new IllegalArgumentException(
                    "No warm nodes found matching the specified criteria. BlockCache operations can only target warm nodes."
                );
            }
        } else {
            warmNodes = allWarmNodes;
        }

        request.setConcreteNodes(warmNodes.toArray(new DiscoveryNode[0]));
    }

    @Override
    protected PruneBlockCacheResponse newResponse(
        PruneBlockCacheRequest request,
        List<NodePruneBlockCacheResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new PruneBlockCacheResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(PruneBlockCacheRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodePruneBlockCacheResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodePruneBlockCacheResponse(in);
    }

    @Override
    protected NodePruneBlockCacheResponse nodeOperation(NodeRequest nodeRequest) {
        String cacheName = nodeRequest.getRequest().getCacheName();

        if (blockCacheRegistry == null) {
            return new NodePruneBlockCacheResponse(transportService.getLocalNode(), false);
        }

        Optional<BlockCache> cache = blockCacheRegistry.get(cacheName);
        if (cache.isEmpty()) {
            throw new IllegalArgumentException(
                "No block cache registered with name [" + cacheName + "]. Valid values are: " + getRegisteredCacheNames()
            );
        }

        cache.get().clear();
        return new NodePruneBlockCacheResponse(transportService.getLocalNode(), true);
    }

    private String getRegisteredCacheNames() {
        // BlockCacheRegistry doesn't expose a list method; return the known constant for now.
        return "[disk]";
    }

    /**
     * Node-level request wrapper.
     */
    public static class NodeRequest extends TransportRequest {
        private final PruneBlockCacheRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new PruneBlockCacheRequest(in);
        }

        public NodeRequest(PruneBlockCacheRequest request) {
            this.request = Objects.requireNonNull(request, "PruneBlockCacheRequest cannot be null");
        }

        public PruneBlockCacheRequest getRequest() {
            return request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
