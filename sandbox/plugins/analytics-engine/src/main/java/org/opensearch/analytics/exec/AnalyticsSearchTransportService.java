/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
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
import org.opensearch.transport.ConnectTransportException;
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
            private Schema batchSchema = null;
            private BufferAllocator batchAllocator = null;

            @Override
            public void onBatch(EngineResultBatch batch) throws Exception {
                VectorSchemaRoot root = batch.getArrowRoot();
                if (batchSchema == null && root.getFieldVectors().isEmpty() == false) {
                    batchSchema = root.getSchema();
                    batchAllocator = root.getFieldVectors().getFirst().getAllocator();
                }
                // On success Flight takes ownership of root. If sendResponseBatch throws (e.g. the
                // stream already closed after a mid-stream failure), Flight never took ownership, so
                // close root here to avoid leaking the imported batch.
                try {
                    channel.sendResponseBatch(new FragmentExecutionArrowResponse(root));
                } catch (Exception e) {
                    try {
                        root.close();
                    } catch (Exception ce) {
                        e.addSuppressed(ce);
                    }
                    throw e;
                }
            }

            @Override
            public void onComplete() {
                channel.completeStream();
            }

            @Override
            public void onCompleteWithMetrics(byte[] metrics) {
                if (batchSchema != null && batchAllocator != null) {
                    VectorSchemaRoot sentinel = VectorSchemaRoot.create(batchSchema, batchAllocator);
                    sentinel.setRowCount(0);
                    try {
                        channel.sendResponseBatch(new FragmentExecutionArrowResponse(sentinel, metrics));
                    } catch (Exception e) {
                        // Stream may already be cancelled — close sentinel to prevent leak
                        sentinel.close();
                    }
                }
                channel.completeStream();
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    channel.sendResponse(AnalyticsTransportErrors.toWireError(e));
                } catch (Exception sendException) {
                    throw new RuntimeException(sendException);
                }
            }
        };
    }

    /** Closes a response's claimed Arrow root if present, swallowing close failures. */
    private static void closeResponseQuietly(FragmentExecutionArrowResponse response) {
        if (response == null || response.getRoot() == null) {
            return;
        }
        try {
            response.getRoot().close();
        } catch (Exception ignore) {}
    }

    Transport.Connection getConnection(DiscoveryNode node) {
        if (node == null) {
            // The target left the cluster between planning and dispatch. Surface a clean
            // ConnectTransportException instead of letting a null node reach the connection
            // manager, where it NPEs ("Cannot invoke Object.hashCode() because key is null").
            throw new ConnectTransportException(null, "target node left the cluster before dispatch");
        }
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
                // We own each claimed response root until it's handed to the consumer. `last`/`next`
                // hold what the loop still owns; null each the instant ownership transfers so the
                // finally can release any undelivered prefetch on failure (stream.close() frees only
                // the cursor, not claimed roots).
                FragmentExecutionArrowResponse last = null;
                FragmentExecutionArrowResponse next = null;
                // Set true only on an expected end (full drain or sentinel). Any other exit leaves it
                // false so the finally cancel()s the stream rather than just closing the cursor.
                boolean terminatedCleanly = false;
                try {
                    last = stream.nextResponse();
                    while (last != null) {
                        // Profiling sentinel: 0 rows with metadata attached. Deliver metrics and exit.
                        if (last.getRoot() != null && last.getRoot().getRowCount() == 0 && last.getMetadata() != null) {
                            listener.onStreamComplete(last.getMetadata());
                            last.getRoot().close();
                            last = null;
                            terminatedCleanly = true;
                            return;
                        }

                        next = stream.nextResponse();
                        // Treat sentinel as end-of-stream: the current batch is the last real data batch.
                        boolean nextIsSentinel = next != null
                            && next.getRoot() != null
                            && next.getRoot().getRowCount() == 0
                            && next.getMetadata() != null;
                        boolean isLast = next == null || nextIsSentinel;

                        // Deliver metrics BEFORE signalling isLast so they're stored on the
                        // task before the profile snapshot fires.
                        if (nextIsSentinel) {
                            listener.onStreamComplete(next.getMetadata());
                            next.getRoot().close();
                            next = null;
                        }

                        // Consumer takes ownership of `last` (it closes the root on all its paths).
                        // Null our ref before the call so a throw can't make finally double-close it.
                        FragmentExecutionArrowResponse delivering = last;
                        last = null;
                        boolean keepReading = listener.onStreamResponse(delivering, isLast);
                        if (nextIsSentinel) {
                            // Sentinel = clean end-of-stream; producer is already completing.
                            terminatedCleanly = true;
                            return;
                        }
                        if (!keepReading) {
                            // Consumer stopped early (satisfied or stage failed); the finally cancels
                            // the stream. Release any undelivered prefetch first.
                            if (!isLast && next != null) {
                                if (next.getRoot() != null) next.getRoot().close();
                                next = null;
                            }
                            return;
                        }
                        last = next;
                        next = null;
                    }
                    // Loop exited because nextResponse() returned null — the stream drained fully.
                    terminatedCleanly = true;
                } catch (Exception e) {
                    listener.onFailure(AnalyticsTransportErrors.fromWireError(e));
                } finally {
                    // Release any batches the loop still owns and never delivered.
                    closeResponseQuietly(last);
                    closeResponseQuietly(next);
                    // On an abnormal exit, cancel() (not close()) so the data-node producer tears down
                    // its FlightServerChannel; close() alone frees only this cursor and strands the
                    // producer's streamRoot. Mirrors core StreamSearchTransportService.
                    try {
                        if (terminatedCleanly) {
                            stream.close();
                        } else {
                            stream.cancel("analytics stream aborted before clean end-of-stream", null);
                        }
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
                    listener.onFailure(AnalyticsTransportErrors.fromWireError(e));
                } finally {
                    pending.finishAndRunNext();
                }
            }
        };

        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();
        pending.tryRun(() -> {
            try {
                Transport.Connection connection = getConnection(targetNode);
                transportService.sendChildRequest(connection, actionName, request, parentTask, options, handler);
            } catch (Exception e) {
                try {
                    listener.onFailure(AnalyticsTransportErrors.fromWireError(e));
                } finally {
                    pending.finishAndRunNext();
                }
            }
        });
    }
}
