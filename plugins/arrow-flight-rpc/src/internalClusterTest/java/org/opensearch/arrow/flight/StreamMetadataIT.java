/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.arrow.transport.ArrowBatchResponse;
import org.opensearch.arrow.transport.ArrowBatchResponseHandler;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

/**
 * End-to-end test for {@link ArrowBatchResponse}'s metadata channel: producer attaches
 * opaque bytes to a batch via {@code new ArrowBatchResponse(root, metadata)}; consumer
 * reads them via {@code response.getMetadata()}. Wire path is Arrow Flight's
 * {@code putNext(ArrowBuf)}.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 2, maxNumDataNodes = 2)
public class StreamMetadataIT extends OpenSearchIntegTestCase {

    private static final Schema DATA_SCHEMA = new Schema(
        List.of(
            new Field("batch_id", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)
        )
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().ensureAtLeastNumDataNodes(2);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPlugin.class, ArrowBasePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            new PluginInfo(
                FlightStreamPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                FlightStreamPlugin.class.getName(),
                null,
                List.of(ArrowBasePlugin.class.getName()),
                false
            )
        );
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testMetadataAttachedToLastBatch() throws Exception {
        DiscoveryNode node = getClusterState().nodes().iterator().next();
        StreamTransportService sts = internalCluster().getInstance(StreamTransportService.class);

        int dataBatches = 4;
        int rowsPerBatch = 3;
        byte[] expectedMetrics = "{\"output_rows\":12,\"elapsed_compute_ns\":12345}".getBytes(StandardCharsets.UTF_8);

        Sink sink = new Sink();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();

        sts.sendRequest(
            node,
            FragmentAction.NAME,
            new FragmentRequest(dataBatches, rowsPerBatch, true),
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            new CollectingHandler(sink, latch, failure)
        );

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertNull(String.valueOf(failure.get()), failure.get());

        assertEquals(dataBatches, sink.batchesSeen);
        assertEquals(dataBatches * rowsPerBatch, sink.dataRowTotal);
        assertEquals(1, sink.metadataObservations);
        assertEquals(dataBatches - 1, sink.metadataBatchIndex);
        assertArrayEquals(expectedMetrics, sink.metrics);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testNoMetadataWhenProfilingDisabled() throws Exception {
        DiscoveryNode node = getClusterState().nodes().iterator().next();
        StreamTransportService sts = internalCluster().getInstance(StreamTransportService.class);

        int dataBatches = 3;
        int rowsPerBatch = 5;

        Sink sink = new Sink();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();

        sts.sendRequest(
            node,
            FragmentAction.NAME,
            new FragmentRequest(dataBatches, rowsPerBatch, false),
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            new CollectingHandler(sink, latch, failure)
        );

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertNull(String.valueOf(failure.get()), failure.get());

        assertEquals(dataBatches, sink.batchesSeen);
        assertEquals(dataBatches * rowsPerBatch, sink.dataRowTotal);
        assertEquals(0, sink.metadataObservations);
        assertNull(sink.metrics);
    }

    static class Sink {
        int batchesSeen = 0;
        int dataRowTotal = 0;
        int metadataObservations = 0;
        int metadataBatchIndex = -1;
        byte[] metrics = null;
    }

    static class CollectingHandler extends ArrowBatchResponseHandler<FragmentResponse> {
        private final Sink sink;
        private final CountDownLatch latch;
        private final AtomicReference<Exception> failure;
        private final List<VectorSchemaRoot> retainedRoots = new ArrayList<>();

        CollectingHandler(Sink sink, CountDownLatch latch, AtomicReference<Exception> failure) {
            this.sink = sink;
            this.latch = latch;
            this.failure = failure;
        }

        @Override
        public void handleStreamResponse(StreamTransportResponse<FragmentResponse> stream) {
            try {
                FragmentResponse response;
                while ((response = stream.nextResponse()) != null) {
                    int idx = sink.batchesSeen++;
                    VectorSchemaRoot root = response.getRoot();
                    sink.dataRowTotal += root.getRowCount();
                    if (response.getMetadata() != null) {
                        sink.metadataObservations++;
                        sink.metadataBatchIndex = idx;
                        sink.metrics = response.getMetadata();
                    }
                    retainedRoots.add(root);
                }
                stream.close();
            } catch (Exception e) {
                failure.set(e);
                stream.cancel("test error", e);
            } finally {
                for (VectorSchemaRoot r : retainedRoots)
                    r.close();
                latch.countDown();
            }
        }

        @Override
        public void handleException(TransportException exp) {
            failure.set(exp);
            latch.countDown();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public FragmentResponse read(StreamInput in) throws IOException {
            return new FragmentResponse(in);
        }
    }

    public static class FragmentResponse extends ArrowBatchResponse {
        public FragmentResponse(VectorSchemaRoot root) {
            super(root);
        }

        public FragmentResponse(VectorSchemaRoot root, byte[] metadata) {
            super(root, metadata);
        }

        public FragmentResponse(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class FragmentRequest extends ActionRequest {
        private final int batchCount;
        private final int rowsPerBatch;
        private final boolean profile;

        FragmentRequest(int batchCount, int rowsPerBatch, boolean profile) {
            this.batchCount = batchCount;
            this.rowsPerBatch = rowsPerBatch;
            this.profile = profile;
        }

        public FragmentRequest(StreamInput in) throws IOException {
            super(in);
            this.batchCount = in.readInt();
            this.rowsPerBatch = in.readInt();
            this.profile = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(batchCount);
            out.writeInt(rowsPerBatch);
            out.writeBoolean(profile);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class FragmentAction extends ActionType<FragmentResponse> {
        public static final FragmentAction INSTANCE = new FragmentAction();
        public static final String NAME = "cluster:internal/test/fragment_with_metadata";

        private FragmentAction() {
            super(NAME, FragmentResponse::new);
        }
    }

    public static class TransportFragmentAction extends TransportAction<FragmentRequest, FragmentResponse> {
        private final BufferAllocator allocator;

        @Inject
        public TransportFragmentAction(
            StreamTransportService streamTransportService,
            ActionFilters actionFilters,
            ArrowNativeAllocator nativeAllocator
        ) {
            super(FragmentAction.NAME, actionFilters, streamTransportService.getTaskManager());
            this.allocator = nativeAllocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_FLIGHT)
                .newChildAllocator("metadata-test", 0, Long.MAX_VALUE);
            streamTransportService.registerRequestHandler(
                FragmentAction.NAME,
                ThreadPool.Names.GENERIC,
                FragmentRequest::new,
                this::handle
            );
        }

        @Override
        protected void doExecute(Task task, FragmentRequest request, ActionListener<FragmentResponse> listener) {
            listener.onFailure(new UnsupportedOperationException("Use StreamTransportService"));
        }

        private void handle(FragmentRequest request, TransportChannel channel, Task task) throws IOException {
            try {
                long totalRows = 0;
                for (int b = 0; b < request.batchCount; b++) {
                    VectorSchemaRoot root = createDataBatch(b, request.rowsPerBatch);
                    boolean isLast = (b == request.batchCount - 1);
                    if (isLast && request.profile) {
                        byte[] metrics = ("{\"output_rows\":" + (totalRows + request.rowsPerBatch) + ",\"elapsed_compute_ns\":12345}")
                            .getBytes(StandardCharsets.UTF_8);
                        channel.sendResponseBatch(new FragmentResponse(root, metrics));
                    } else {
                        channel.sendResponseBatch(new FragmentResponse(root));
                    }
                    totalRows += request.rowsPerBatch;
                }
                channel.completeStream();
            } catch (StreamException e) {
                if (e.getErrorCode() != StreamErrorCode.CANCELLED) channel.sendResponse(e);
            } catch (Exception e) {
                channel.sendResponse(e);
            }
        }

        private VectorSchemaRoot createDataBatch(int batchIndex, int rowCount) {
            VectorSchemaRoot root = VectorSchemaRoot.create(DATA_SCHEMA, allocator);
            IntVector batchId = (IntVector) root.getVector("batch_id");
            IntVector value = (IntVector) root.getVector("value");
            batchId.allocateNew();
            value.allocateNew();
            for (int i = 0; i < rowCount; i++) {
                batchId.setSafe(i, batchIndex);
                value.setSafe(i, batchIndex * 1000 + i);
            }
            batchId.setValueCount(rowCount);
            value.setValueCount(rowCount);
            root.setRowCount(rowCount);
            return root;
        }
    }

    public static class TestPlugin extends Plugin implements ActionPlugin {
        public TestPlugin() {}

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(FragmentAction.INSTANCE, TransportFragmentAction.class));
        }
    }
}
