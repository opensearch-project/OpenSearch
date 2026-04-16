/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.ScanResponse;
import org.opensearch.analytics.exec.action.AnalyticsScanAction;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Singleton;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * Stateless transport dispatch component for scan requests. Owns
 * {@link TransportService} (or {@link StreamTransportService}) and
 * connection lookup. Does NOT track per-query or per-node concurrency
 * state — callers provide their own {@link PendingExecutions} instance
 * to gate dispatch concurrency.
 *
 * <p>Also registers the server-side scan request handler at construction
 * time (delegating fragment execution to {@link AnalyticsSearchService}).
 *
 * <p>Marked {@link Singleton} because the constructor has a side effect —
 * registering the transport request handler — and double-registration throws.
 *
 * @opensearch.internal
 */
@Singleton
public class AnalyticsSearchTransportService {
    private final TransportService transportService;
    private final ClusterService clusterService;

    /**
     * Guice-injected constructor. Selects {@link StreamTransportService} when
     * available (Arrow Flight configured), otherwise falls back to regular
     * {@link TransportService}. Registers the server-side scan request handler.
     */
    @Inject
    public AnalyticsSearchTransportService(
        TransportService transportService,
        @Nullable StreamTransportService streamTransportService,
        ClusterService clusterService,
        AnalyticsSearchService searchService,
        IndicesService indicesService
    ) {
        this.transportService = streamTransportService != null ? streamTransportService : transportService;
        this.clusterService = clusterService;
        registerScanHandler(this.transportService, searchService, indicesService);
    }

    /**
     * Test-only constructor. Skips handler registration since tests either
     * install their own mock handlers or don't exercise the inbound path.
     */
    public AnalyticsSearchTransportService(TransportService transportService, ClusterService clusterService) {
        this.transportService = Objects.requireNonNull(transportService, "TransportService must not be null");
        this.clusterService = clusterService;
    }

    /**
     * Registers the server-side handler for {@link AnalyticsScanAction#NAME}.
     * Routes {@link FragmentExecutionRequest} to {@link AnalyticsSearchService}
     * and responds with a {@link ScanResponse}.
     */
    private static void registerScanHandler(
        TransportService transportService,
        AnalyticsSearchService searchService,
        IndicesService indicesService
    ) {
        transportService.registerRequestHandler(
            AnalyticsScanAction.NAME,
            ThreadPool.Names.SAME,
            false,
            true,
            AdmissionControlActionType.SEARCH,
            FragmentExecutionRequest::new,
            (request, channel, task) -> {
                IndexShard shard = indicesService.indexServiceSafe(request.getShardId().getIndex()).getShard(request.getShardId().id());
                ScanResponse response = searchService.executeFragment(request, shard);
                channel.sendResponse(response);
            }
        );
    }

    /**
     * Resolves the connection to the given target node via this class's
     * {@link ClusterService} and {@link TransportService}.
     */
    Transport.Connection getConnection(String clusterAlias, String nodeId) {
        DiscoveryNode node = clusterService.state().nodes().get(nodeId);
        return transportService.getConnection(node);
    }

    /**
     * Dispatches a scan request to the target data node, gated by the
     * caller-provided {@link PendingExecutions}. Uses the typed
     * {@link AnalyticsScanAction} and delivers streaming {@link ScanResponse}
     * batches to the listener.
     *
     * @param request    the fragment execution request
     * @param targetNode the node hosting the target shard
     * @param listener   the streaming response listener for scan batches
     * @param parentTask the parent task for child-request propagation
     * @param pending    the per-node concurrency gate owned by the caller
     */
    public void dispatchScan(
        FragmentExecutionRequest request,
        DiscoveryNode targetNode,
        StreamingResponseListener<ScanResponse> listener,
        Task parentTask,
        PendingExecutions pending
    ) {
        TransportResponseHandler<ScanResponse> handler = new TransportResponseHandler<>() {
            @Override
            public ScanResponse read(StreamInput in) throws IOException {
                return new ScanResponse(in);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public void handleStreamResponse(StreamTransportResponse<ScanResponse> stream) {
                try {
                    ScanResponse current;
                    ScanResponse last = null;
                    while ((current = stream.nextResponse()) != null) {
                        if (last != null) {
                            listener.onStreamResponse(last, false);
                        }
                        last = current;
                    }
                    if (last != null) {
                        listener.onStreamResponse(last, true);
                    }
                } catch (Exception e) {
                    listener.onFailure(e);
                } finally {
                    try {
                        stream.close();
                    } catch (Exception ignore) {}
                    pending.finishAndRunNext();
                }
            }

            @Override
            public void handleResponse(ScanResponse response) {
                try {
                    listener.onStreamResponse(response, true);
                } finally {
                    pending.finishAndRunNext();
                }
            }

            @Override
            public void handleException(TransportException e) {
                try {
                    listener.onFailure(e);
                } finally {
                    pending.finishAndRunNext();
                }
            }
        };

        pending.tryRun(() -> {
            try {
                Transport.Connection connection = getConnection(null, targetNode.getId());
                transportService.sendChildRequest(
                    connection,
                    AnalyticsScanAction.NAME,
                    request,
                    parentTask,
                    TransportRequestOptions.EMPTY,
                    handler
                );
            } catch (Exception e) {
                try {
                    listener.onFailure(e);
                } finally {
                    pending.finishAndRunNext();
                }
            }
        });
    }
}
