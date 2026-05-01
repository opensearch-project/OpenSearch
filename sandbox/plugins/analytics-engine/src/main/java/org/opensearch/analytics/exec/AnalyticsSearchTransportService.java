/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Singleton;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
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
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.Iterator;

/**
 * Stateless transport dispatch component for fragment requests. Owns
 * {@link TransportService} (or {@link StreamTransportService}) and
 * connection lookup.
 *
 * <p>Does NOT track per-query or per-node concurrency
 * state — callers provide their own {@link PendingExecutions} instance
 * to gate dispatch concurrency.
 *
 * @opensearch.internal
 */
@Singleton
public class AnalyticsSearchTransportService {
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final boolean streamingEnabled;

    @Inject
    public AnalyticsSearchTransportService(
        TransportService transportService,
        @Nullable StreamTransportService streamTransportService,
        ClusterService clusterService,
        AnalyticsSearchService searchService,
        IndicesService indicesService
    ) {
        this.streamingEnabled = streamTransportService != null;
        this.transportService = this.streamingEnabled ? streamTransportService : transportService;
        this.clusterService = clusterService;
        if (this.streamingEnabled) {
            registerStreamingFragmentHandler(this.transportService, searchService, indicesService);
        } else {
            registerFragmentHandler(this.transportService, searchService, indicesService);
        }
    }

    public boolean isStreamingEnabled() {
        return streamingEnabled;
    }

    private static void registerFragmentHandler(
        TransportService transportService,
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
                FragmentExecutionResponse response = searchService.executeFragment(request, shard, (AnalyticsShardTask) task);
                channel.sendResponse(response);
            }
        );
    }

    private static void registerStreamingFragmentHandler(
        TransportService transportService,
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
                try (FragmentResources ctx = searchService.executeFragmentStreaming(request, shard, (AnalyticsShardTask) task)) {
                    Iterator<EngineResultBatch> it = ctx.stream().iterator();
                    while (it.hasNext()) {
                        EngineResultBatch batch = it.next();
                        channel.sendResponseBatch(new FragmentExecutionArrowResponse(batch.getArrowRoot()));
                    }
                    channel.completeStream();
                } catch (StreamException e) {
                    if (e.getErrorCode() != StreamErrorCode.CANCELLED) {
                        channel.sendResponse(e);
                    }
                    // CANCELLED: channel already torn down — exit silently
                } catch (Exception e) {
                    channel.sendResponse(e);
                }
            }
        );
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
        dispatchFragment(
            request,
            targetNode,
            listener,
            parentTask,
            pending,
            in -> new FragmentExecutionArrowResponse(in),
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            true
        );
    }

    public void dispatchFragment(
        FragmentExecutionRequest request,
        DiscoveryNode targetNode,
        StreamingResponseListener<FragmentExecutionResponse> listener,
        Task parentTask,
        PendingExecutions pending
    ) {
        dispatchFragment(
            request,
            targetNode,
            listener,
            parentTask,
            pending,
            in -> new FragmentExecutionResponse(in),
            TransportRequestOptions.EMPTY,
            false
        );
    }

    private <T extends ActionResponse> void dispatchFragment(
        FragmentExecutionRequest request,
        DiscoveryNode targetNode,
        StreamingResponseListener<T> listener,
        Task parentTask,
        PendingExecutions pending,
        Writeable.Reader<T> reader,
        TransportRequestOptions options,
        boolean skipsDeserialization
    ) {
        TransportResponseHandler<T> handler = new TransportResponseHandler<>() {
            @Override
            public T read(StreamInput in) throws IOException {
                return reader.read(in);
            }

            @Override
            public boolean skipsDeserialization() {
                return skipsDeserialization;
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public void handleStreamResponse(StreamTransportResponse<T> stream) {
                try {
                    T current;
                    T last = null;
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
            public void handleResponse(T response) {
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
                transportService.sendChildRequest(connection, FragmentExecutionAction.NAME, request, parentTask, options, handler);
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
