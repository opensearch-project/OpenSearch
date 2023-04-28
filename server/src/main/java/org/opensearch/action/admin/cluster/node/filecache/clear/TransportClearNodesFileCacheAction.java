/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.filecache.clear;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.node.Node;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for clearing filecache on {@link org.opensearch.cluster.node.DiscoveryNodeRole#SEARCH_ROLE} nodes
 *
 * @opensearch.internal
 */
public class TransportClearNodesFileCacheAction extends TransportNodesAction<
    ClearNodesFileCacheRequest,
    ClearNodesFileCacheResponse,
    TransportClearNodesFileCacheAction.ClearNodeFileCacheRequest,
    ClearNodeFileCacheResponse> {

    private final Node node;

    @Inject
    public TransportClearNodesFileCacheAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        Node node,
        ActionFilters actionFilters
    ) {
        super(
            ClearNodesFileCacheAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ClearNodesFileCacheRequest::new,
            ClearNodeFileCacheRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ClearNodeFileCacheResponse.class
        );
        this.node = node;

    }

    @Override
    protected ClearNodesFileCacheResponse newResponse(
        ClearNodesFileCacheRequest request,
        List<ClearNodeFileCacheResponse> clearNodeFileCacheResponses,
        List<FailedNodeException> failures
    ) {
        return new ClearNodesFileCacheResponse(clusterService.getClusterName(), clearNodeFileCacheResponses, failures);
    }

    @Override
    protected ClearNodeFileCacheRequest newNodeRequest(ClearNodesFileCacheRequest request) {
        return new ClearNodeFileCacheRequest(request);
    }

    @Override
    protected ClearNodeFileCacheResponse newNodeResponse(StreamInput in) throws IOException {
        return new ClearNodeFileCacheResponse(in);
    }

    @Override
    protected ClearNodeFileCacheResponse nodeOperation(ClearNodeFileCacheRequest request) {
        DiscoveryNode discoveryNode = transportService.getLocalNode();
        if (discoveryNode.isSearchNode()) {
            long count = node.fileCache().prune();
            return new ClearNodeFileCacheResponse(transportService.getLocalNode(), true, count);
        }
        return new ClearNodeFileCacheResponse(transportService.getLocalNode(), false, 0);
    }

    /**
     * Transport request object for clearing filecache on nodes
     *
     * @opensearch.internal
     */
    public static class ClearNodeFileCacheRequest extends TransportRequest {
        ClearNodesFileCacheRequest request;

        public ClearNodeFileCacheRequest(StreamInput in) throws IOException {
            super(in);
            request = new ClearNodesFileCacheRequest(in);
        }

        ClearNodeFileCacheRequest(ClearNodesFileCacheRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
