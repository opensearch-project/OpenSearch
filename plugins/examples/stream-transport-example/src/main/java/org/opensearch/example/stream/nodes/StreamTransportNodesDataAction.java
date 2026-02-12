/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream.nodes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.StreamTransportNodesAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;
import java.util.List;

/**
 * Demonstrates streaming across multiple nodes with aggregation
 */
public class StreamTransportNodesDataAction extends StreamTransportNodesAction<
    StreamNodesDataRequest,
    StreamNodesDataResponse,
    NodeStreamDataRequest,
    NodeStreamDataResponse> {

    private static final Logger logger = LogManager.getLogger(StreamTransportNodesDataAction.class);
    private final TransportService transportService;

    @Inject
    public StreamTransportNodesDataAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        @Nullable StreamTransportService streamTransportService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(
            StreamNodesDataAction.NAME,
            threadPool,
            clusterService,
            streamTransportService,
            transportService,
            actionFilters,
            StreamNodesDataRequest::new,
            NodeStreamDataRequest::new,
            ThreadPool.Names.GENERIC
        );
        if (streamTransportService == null) {
            throw new IllegalStateException("StreamTransportService is required but not available");
        }
        this.transportService = transportService;
    }

    @Override
    protected void nodeStreamOperation(NodeStreamDataRequest request, TransportChannel channel, Task task) throws IOException {
        try {
            DiscoveryNode localNode = transportService.getLocalNode();

            for (int i = 1; i <= request.getCount(); i++) {
                NodeStreamDataResponse response = new NodeStreamDataResponse(localNode, "Node " + localNode.getName() + " - item " + i, i);

                channel.sendResponseBatch(response);

                if (i < request.getCount() && request.getDelayMs() > 0) {
                    Thread.sleep(request.getDelayMs());
                }
            }

            channel.completeStream();

        } catch (StreamException e) {
            if (e.getErrorCode() == StreamErrorCode.CANCELLED) {
                logger.info("Client cancelled stream: {}", e.getMessage());
            } else {
                channel.sendResponse(e);
            }
        } catch (Exception e) {
            channel.sendResponse(e);
        }
    }

    @Override
    protected NodeStreamDataRequest newNodeRequest(StreamNodesDataRequest request) {
        return new NodeStreamDataRequest(request);
    }

    @Override
    protected NodeStreamDataResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodeStreamDataResponse(in);
    }

    @Override
    protected StreamNodesDataResponse newResponse(
        StreamNodesDataRequest request,
        List<NodeStreamDataResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new StreamNodesDataResponse(clusterService.getClusterName(), responses, failures);
    }
}
