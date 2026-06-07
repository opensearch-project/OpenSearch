/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.exec.action.FetchByRowIdsAction;
import org.opensearch.analytics.exec.action.FetchByRowIdsRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Singleton;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;

/**
 * Stateless transport dispatch component for fragment requests. Owns the
 * {@link StreamTransportService} (analytics-engine is streaming-only) and
 * connection lookup.
 *
 * <p>Does NOT track per-query or per-node concurrency state — callers provide
 * their own {@link PendingExecutions} instance to gate dispatch concurrency.
 *
 * @opensearch.internal
 */
@Singleton
public class AnalyticsSearchTransportService {
    private static final Logger logger = LogManager.getLogger(AnalyticsSearchTransportService.class);

    private final StreamTransportService transportService;
    private final ClusterService clusterService;

    @Inject
    public AnalyticsSearchTransportService(
        StreamTransportService streamTransportService,
        ClusterService clusterService,
        AnalyticsSearchService searchService,
        IndicesService indicesService,
        TaskResourceTrackingService taskResourceTrackingService
    ) {
        if (streamTransportService == null) {
            throw new IllegalStateException(
                "analytics-engine requires the STREAM_TRANSPORT feature flag to be enabled "
                    + "("
                    + FeatureFlags.STREAM_TRANSPORT
                    + "=true)"
            );
        }
        searchService.setTaskResourceTrackingService(taskResourceTrackingService);
        this.transportService = streamTransportService;
        this.clusterService = clusterService;
        registerStreamingFragmentHandler(this.transportService, searchService, indicesService);
        registerFetchByRowIdsHandler(this.transportService, searchService, indicesService);
    }

    private static void registerStreamingFragmentHandler(
        StreamTransportService transportService,
        AnalyticsSearchService searchService,
        IndicesService indicesService
    ) {
        transportService.registerRequestHandler(
            FragmentExecutionAction.NAME,
            ThreadPool.Names.SAME,
            false,
            true,
            AdmissionControlActionType.SEARCH,
            FragmentExecutionRequest::new,
            (request, channel, task) -> {
                IndexShard shard = indicesService.indexServiceSafe(request.getShardId().getIndex()).getShard(request.getShardId().id());
                searchService.executeFragmentStreamingAsync(
                    request,
                    shard,
                    (AnalyticsShardTask) task,
                    channelResponseHandler(channel),
                    ContextAwareExecutor.wrap(
                        transportService.getThreadPool().executor(ThreadPool.Names.SEARCH),
                        transportService.getThreadPool()
                    )
                );
            }
        );
    }

    /**
     * Mirrors {@link #registerStreamingFragmentHandler} for the QTF fetch-by-rowids path.
     * Forks the iterator drain onto the SEARCH executor via
     * {@link AnalyticsSearchService#executeFetchByRowIdsAsync} so a slow coordinator parks
     * a search thread, not the transport thread, and the engine sees natural backpressure
     * through the blocking {@code channel.sendResponseBatch} call.
     */
    private static void registerFetchByRowIdsHandler(
        StreamTransportService transportService,
        AnalyticsSearchService searchService,
        IndicesService indicesService
    ) {
        transportService.registerRequestHandler(
            FetchByRowIdsAction.NAME,
            ThreadPool.Names.SAME,
            false,
            true,
            AdmissionControlActionType.SEARCH,
            FetchByRowIdsRequest::new,
            (request, channel, task) -> {
                IndexShard shard = indicesService.indexServiceSafe(request.getShardId().getIndex()).getShard(request.getShardId().id());
                searchService.executeFetchByRowIdsAsync(
                    request,
                    shard,
                    (AnalyticsShardTask) task,
                    channelResponseHandler(channel),
                    ContextAwareExecutor.wrap(
                        transportService.getThreadPool().executor(ThreadPool.Names.SEARCH),
                        transportService.getThreadPool()
                    )
                );
            }
        );
    }

    /**
     * Adapter from {@link AnalyticsSearchService.StreamingFragmentResponseHandler} to the
     * channel streaming API. Each batch is sent on the channel; onComplete completes the
     * stream; onFailure ignores cancellation and forwards everything else as an exception
     * response. Shared by the streaming-fragment and fetch-by-rowids handlers — the dispatch
     * shape is identical, only the request type differs.
     */
    private static AnalyticsSearchService.StreamingFragmentResponseHandler channelResponseHandler(TransportChannel channel) {
        return new AnalyticsSearchService.StreamingFragmentResponseHandler() {
            @Override
            public void onBatch(EngineResultBatch batch) throws Exception {
                channel.sendResponseBatch(new FragmentExecutionArrowResponse(batch.getArrowRoot()));
            }

            @Override
            public void onComplete() {
                channel.completeStream();
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (Exception sendException) {
                    throw new RuntimeException(sendException);
                }
            }
        };
    }

    Transport.Connection getConnection(String clusterAlias, String nodeId) {
        DiscoveryNode node = clusterService.state().nodes().get(nodeId);
        return transportService.getConnection(node);
    }

    public void dispatchFragmentStreaming(
        FragmentExecutionRequest request,
        DiscoveryNode targetNode,
        StreamingResponseListener<FragmentExecutionArrowResponse> listener,
        Task parentTask,
        PendingExecutions pending
    ) {
        dispatchStreaming(FragmentExecutionAction.NAME, request, targetNode, listener, parentTask, pending);
    }

    /**
     * Dispatches a QTF fetch-by-rowids RPC to {@code targetNode}. Mirrors
     * {@link #dispatchFragmentStreaming} — same streaming-response handler shape, same
     * {@link PendingExecutions} gating, same cancellation propagation. Different action
     * name routes the request to {@link FetchByRowIdsAction} on the data node.
     */
    public void dispatchFetchByRowIds(
        FetchByRowIdsRequest request,
        DiscoveryNode targetNode,
        StreamingResponseListener<FragmentExecutionArrowResponse> listener,
        Task parentTask,
        PendingExecutions pending
    ) {
        dispatchStreaming(FetchByRowIdsAction.NAME, request, targetNode, listener, parentTask, pending);
    }

    /**
     * Shared streaming dispatch path for {@link FragmentExecutionAction} and
     * {@link FetchByRowIdsAction}. Drains the response stream inline — backpressure flows
     * because {@code listener.onStreamResponse} blocks until the downstream sink accepts
     * the batch, which gates the next {@code stream.nextResponse} call and propagates
     * gRPC flow control back to the data node.
     */
    private void dispatchStreaming(
        String actionName,
        TransportRequest request,
        DiscoveryNode targetNode,
        StreamingResponseListener<FragmentExecutionArrowResponse> listener,
        Task parentTask,
        PendingExecutions pending
    ) {
        TransportResponseHandler<FragmentExecutionArrowResponse> handler = new TransportResponseHandler<>() {
            @Override
            public FragmentExecutionArrowResponse read(StreamInput in) throws IOException {
                return new FragmentExecutionArrowResponse(in);
            }

            @Override
            public boolean skipsDeserialization() {
                return true;
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public void handleStreamResponse(StreamTransportResponse<FragmentExecutionArrowResponse> stream) {
                try {
                    FragmentExecutionArrowResponse last = stream.nextResponse();
                    while (last != null) {
                        FragmentExecutionArrowResponse next = stream.nextResponse();
                        boolean isLast = next == null;
                        boolean keepReading = listener.onStreamResponse(last, isLast);
                        if (!keepReading) {
                            if (next != null) {
                                if (next.getRoot() != null) {
                                    next.getRoot().close();
                                }
                                logger.debug("[early-term] cancelling shard stream: reduce input satisfied (downstream consumer finished)");
                                stream.cancel("reduce input satisfied (downstream consumer finished)", null);
                            }
                            return;
                        }
                        last = next;
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
            public void handleResponse(FragmentExecutionArrowResponse response) {
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

        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();
        pending.tryRun(() -> {
            try {
                Transport.Connection connection = getConnection(null, targetNode.getId());
                transportService.sendChildRequest(connection, actionName, request, parentTask, options, handler);
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
