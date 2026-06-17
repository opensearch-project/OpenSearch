/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.util.List;

import org.mockito.ArgumentCaptor;

import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies fragment handler registration and the stream-drain ownership contract in
 * {@link AnalyticsSearchTransportService#dispatchFragmentStreaming} — specifically that a batch
 * prefetched (read one ahead, to compute {@code isLast}) but never delivered to the consumer is
 * released when the stream fails mid-drain. Otherwise its claimed Arrow root (POOL_FLIGHT buffers)
 * would leak on every failed query.
 */
public class AnalyticsSearchTransportServiceTests extends OpenSearchTestCase {

    public void testFragmentHandlerRegisteredWithSameExecutor() {
        StreamTransportService transportService = mock(StreamTransportService.class);
        AnalyticsSearchService searchService = mock(AnalyticsSearchService.class);
        IndicesService indicesService = mock(IndicesService.class);
        ClusterService clusterService = mock(ClusterService.class);
        TaskResourceTrackingService taskResourceTrackingService = mock(TaskResourceTrackingService.class);

        new AnalyticsSearchTransportService(transportService, clusterService, searchService, indicesService, taskResourceTrackingService);

        verify(transportService).registerRequestHandler(
            eq(FragmentExecutionAction.NAME),
            eq(ThreadPool.Names.SAME),
            anyBoolean(),
            anyBoolean(),
            any(),
            any(),
            any()
        );
    }

    /**
     * When the consumer's {@code onStreamResponse} throws mid-drain, the drain loop owns a
     * prefetched batch it never delivered. That batch's root must be closed by the loop's finally —
     * {@code stream.close()} only frees the cursor, not the already-claimed response root.
     */
    public void testPrefetchedRootReleasedWhenConsumerThrows() throws Exception {
        try (RootAllocator root = new RootAllocator(Long.MAX_VALUE)) {
            // last: delivered to the consumer (which throws). next: prefetched, never delivered.
            VectorSchemaRoot lastRoot = newIntRoot(root, "a", 1);
            VectorSchemaRoot nextRoot = newIntRoot(root, "b", 2);
            FragmentExecutionArrowResponse last = new FragmentExecutionArrowResponse(lastRoot);
            FragmentExecutionArrowResponse next = new FragmentExecutionArrowResponse(nextRoot);

            @SuppressWarnings("unchecked")
            StreamTransportResponse<FragmentExecutionArrowResponse> stream = mock(StreamTransportResponse.class);
            when(stream.nextResponse()).thenReturn(last, next, null);

            TransportResponseHandler<FragmentExecutionArrowResponse> handler = captureHandler(new StreamingResponseListener<>() {
                @Override
                public boolean onStreamResponse(FragmentExecutionArrowResponse response, boolean isLast) {
                    throw new RuntimeException("consumer failed mid-stream");
                }

                @Override
                public void onFailure(Exception e) {}
            });

            handler.handleStreamResponse(stream);

            // next was prefetched and never delivered, so the drain loop's finally must release it.
            assertTrue("prefetched-but-undelivered root buffers must be freed", isClosed(nextRoot));
            // The undelivered prefetch is closed by the loop; the delivered batch's ownership left the
            // loop, so only the consumer is responsible for it — close it here to avoid the test leaking.
            if (!isClosed(lastRoot)) {
                lastRoot.close();
            }
            verify(stream).close();
        }
    }

    /** Builds and registers the service, captures the streaming handler passed to sendChildRequest. */
    private TransportResponseHandler<FragmentExecutionArrowResponse> captureHandler(
        StreamingResponseListener<FragmentExecutionArrowResponse> listener
    ) {
        StreamTransportService transportService = mock(StreamTransportService.class);
        AnalyticsSearchService searchService = mock(AnalyticsSearchService.class);
        IndicesService indicesService = mock(IndicesService.class);
        ClusterService clusterService = mock(ClusterService.class);
        TaskResourceTrackingService taskResourceTrackingService = mock(TaskResourceTrackingService.class);

        // getConnection(null, nodeId) -> clusterService.state().nodes().get(nodeId) -> getConnection(node)
        DiscoveryNode node = mock(DiscoveryNode.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.nodes()).thenReturn(nodes);
        when(nodes.get(any())).thenReturn(node);
        when(transportService.getConnection(node)).thenReturn(mock(Transport.Connection.class));

        AnalyticsSearchTransportService service = new AnalyticsSearchTransportService(
            transportService,
            clusterService,
            searchService,
            indicesService,
            taskResourceTrackingService
        );

        @SuppressWarnings("unchecked")
        ArgumentCaptor<TransportResponseHandler<FragmentExecutionArrowResponse>> handlerCaptor = ArgumentCaptor.forClass(
            TransportResponseHandler.class
        );
        // Capture the handler instead of actually sending.
        doAnswer(inv -> null).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                eq(FragmentExecutionAction.NAME),
                any(),
                any(),
                any(),
                handlerCaptor.capture()
            );

        DiscoveryNode target = mock(DiscoveryNode.class);
        when(target.getId()).thenReturn("node-1");
        service.dispatchFragmentStreaming(
            mock(FragmentExecutionRequest.class),
            target,
            listener,
            mock(Task.class),
            new PendingExecutions(1)
        );

        List<TransportResponseHandler<FragmentExecutionArrowResponse>> handlers = handlerCaptor.getAllValues();
        assertFalse("handler must have been dispatched to sendChildRequest", handlers.isEmpty());
        return handlers.get(handlers.size() - 1);
    }

    private static VectorSchemaRoot newIntRoot(BufferAllocator allocator, String name, int value) {
        Field field = new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(singletonList(field));
        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, allocator);
        IntVector vec = (IntVector) vsr.getVector(name);
        vec.allocateNew(1);
        vec.set(0, value);
        vsr.setRowCount(1);
        return vsr;
    }

    /** A closed VectorSchemaRoot has released its buffers — its vectors report zero buffer size. */
    private static boolean isClosed(VectorSchemaRoot vsr) {
        return vsr.getFieldVectors().stream().allMatch(v -> v.getBufferSize() == 0);
    }
}
