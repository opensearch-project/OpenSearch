/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.arrow.flight.transport.ArrowBatchResponse;
import org.opensearch.arrow.flight.transport.ArrowStreamHandler;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

/**
 * Integration tests for the native Arrow transport path.
 * Tests that ArrowBatchResponse + ArrowStreamHandler bypass byte serialization
 * and deliver typed VectorSchemaRoot data end-to-end over Arrow Flight.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 2, maxNumDataNodes = 2)
public class NativeArrowTransportIT extends OpenSearchIntegTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().ensureAtLeastNumDataNodes(2);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(NativeArrowTestPlugin.class, FlightStreamPlugin.class);
    }

    // ──── Test: Single batch of native Arrow data ────

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testSingleBatchNativeArrow() throws Exception {
        for (DiscoveryNode node : getClusterState().nodes()) {
            StreamTransportService streamTransportService = internalCluster().getInstance(StreamTransportService.class);

            List<ArrowDataResponse> responses = new ArrayList<>();
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Exception> failure = new AtomicReference<>();

            StreamTransportResponseHandler<ArrowDataResponse> handler = createArrowHandler(responses, latch, failure);

            ArrowDataRequest request = new ArrowDataRequest(1, 3); // 1 batch, 3 rows
            streamTransportService.sendRequest(
                node,
                ArrowDataAction.NAME,
                request,
                TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
                handler
            );

            assertTrue("Stream should complete within 10s", latch.await(10, TimeUnit.SECONDS));
            assertNull("No exception expected", failure.get());
            assertEquals("Should receive 1 batch", 1, responses.size());

            ArrowDataResponse response = responses.get(0);
            VectorSchemaRoot root = response.getArrowRoot();
            assertNotNull("Root should not be null", root);
            assertEquals("Should have 3 rows", 3, root.getRowCount());

            // Verify typed columns — NOT VarBinary blobs
            assertEquals(2, root.getSchema().getFields().size());
            assertEquals("name", root.getSchema().getFields().get(0).getName());
            assertEquals("age", root.getSchema().getFields().get(1).getName());

            // Verify actual data
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            IntVector ageVector = (IntVector) root.getVector("age");
            assertEquals("Alice", new String(nameVector.get(0), StandardCharsets.UTF_8));
            assertEquals("Bob", new String(nameVector.get(1), StandardCharsets.UTF_8));
            assertEquals("Carol", new String(nameVector.get(2), StandardCharsets.UTF_8));
            assertEquals(30, ageVector.get(0));
            assertEquals(31, ageVector.get(1));
            assertEquals(32, ageVector.get(2));

            response.close();
        }
    }

    // ──── Test: Multiple batches of native Arrow data ────

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testMultipleBatchesNativeArrow() throws Exception {
        for (DiscoveryNode node : getClusterState().nodes()) {
            StreamTransportService streamTransportService = internalCluster().getInstance(StreamTransportService.class);

            List<ArrowDataResponse> responses = new ArrayList<>();
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Exception> failure = new AtomicReference<>();

            StreamTransportResponseHandler<ArrowDataResponse> handler = createArrowHandler(responses, latch, failure);

            ArrowDataRequest request = new ArrowDataRequest(3, 2); // 3 batches, 2 rows each
            streamTransportService.sendRequest(
                node,
                ArrowDataAction.NAME,
                request,
                TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
                handler
            );

            assertTrue("Stream should complete within 10s", latch.await(10, TimeUnit.SECONDS));
            assertNull("No exception expected", failure.get());
            assertEquals("Should receive 3 batches", 3, responses.size());

            for (int i = 0; i < 3; i++) {
                ArrowDataResponse response = responses.get(i);
                VectorSchemaRoot root = response.getArrowRoot();
                assertEquals("Each batch should have 2 rows", 2, root.getRowCount());
                assertEquals("name", root.getSchema().getFields().get(0).getName());
                assertEquals("age", root.getSchema().getFields().get(1).getName());

                // Verify data in each batch
                VarCharVector nameVector = (VarCharVector) root.getVector("name");
                IntVector ageVector = (IntVector) root.getVector("age");
                assertNotNull("Name vector should not be null", nameVector.get(0));
                assertEquals(30, ageVector.get(0));
                assertEquals(31, ageVector.get(1));

                response.close();
            }
        }
    }

    // ──── Helper: create handler that implements ArrowStreamHandler ────

    private StreamTransportResponseHandler<ArrowDataResponse> createArrowHandler(
        List<ArrowDataResponse> responses,
        CountDownLatch latch,
        AtomicReference<Exception> failure
    ) {
        return new ArrowAwareResponseHandler(responses, latch, failure);
    }

    // ──── Inner classes: Action, Request, Response, Handler, Plugin ────

    /**
     * Response that carries native Arrow VectorSchemaRoot.
     * Implements ArrowBatchResponse so FlightOutboundHandler sends it directly.
     */
    public static class ArrowDataResponse extends ActionResponse implements ArrowBatchResponse {
        private VectorSchemaRoot root;
        private BufferAllocator allocator;

        public ArrowDataResponse(VectorSchemaRoot root) {
            this.root = root;
        }

        public ArrowDataResponse(VectorSchemaRoot root, BufferAllocator allocator) {
            this.root = root;
            this.allocator = allocator;
        }

        public ArrowDataResponse(StreamInput in) throws IOException {
            super(in);
            // Fallback deserialization for Netty4 — not expected in native Arrow path
            throw new UnsupportedOperationException("Netty4 fallback not implemented in this test");
        }

        @Override
        public VectorSchemaRoot getArrowRoot() {
            return root;
        }

        @Override
        public Schema getArrowSchema() {
            return root.getSchema();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // Fallback serialization for Netty4
            throw new UnsupportedOperationException("Netty4 fallback not implemented in this test");
        }

        public void close() {
            if (root != null) {
                root.close();
            }
            if (allocator != null) {
                allocator.close();
            }
        }
    }

    /**
     * Request specifying how many batches and rows per batch.
     */
    public static class ArrowDataRequest extends ActionRequest {
        private int batchCount;
        private int rowsPerBatch;

        public ArrowDataRequest(int batchCount, int rowsPerBatch) {
            this.batchCount = batchCount;
            this.rowsPerBatch = rowsPerBatch;
        }

        public ArrowDataRequest(StreamInput in) throws IOException {
            super(in);
            this.batchCount = in.readInt();
            this.rowsPerBatch = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(batchCount);
            out.writeInt(rowsPerBatch);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public int getBatchCount() {
            return batchCount;
        }

        public int getRowsPerBatch() {
            return rowsPerBatch;
        }
    }

    /**
     * Action type registration.
     */
    public static class ArrowDataAction extends ActionType<ArrowDataResponse> {
        public static final ArrowDataAction INSTANCE = new ArrowDataAction();
        public static final String NAME = "cluster:internal/test/arrow_data";

        private ArrowDataAction() {
            super(NAME, ArrowDataResponse::new);
        }
    }

    /**
     * Server-side transport action: creates VectorSchemaRoot and sends via ArrowBatchResponse.
     */
    public static class TransportArrowDataAction extends TransportAction<ArrowDataRequest, ArrowDataResponse> {

        private static final String[] NAMES = { "Alice", "Bob", "Carol", "Dave", "Eve" };

        @Inject
        public TransportArrowDataAction(StreamTransportService streamTransportService, ActionFilters actionFilters) {
            super(ArrowDataAction.NAME, actionFilters, streamTransportService.getTaskManager());
            streamTransportService.registerRequestHandler(
                ArrowDataAction.NAME,
                ThreadPool.Names.GENERIC,
                ArrowDataRequest::new,
                this::handleStreamRequest
            );
        }

        @Override
        protected void doExecute(Task task, ArrowDataRequest request, ActionListener<ArrowDataResponse> listener) {
            listener.onFailure(new UnsupportedOperationException("Use StreamTransportService"));
        }

        private void handleStreamRequest(ArrowDataRequest request, TransportChannel channel, Task task) throws IOException {
            try {
                for (int batch = 0; batch < request.getBatchCount(); batch++) {
                    BufferAllocator allocator = new RootAllocator();
                    VectorSchemaRoot root = createTestBatch(allocator, request.getRowsPerBatch(), batch);
                    channel.sendResponseBatch(new ArrowDataResponse(root));
                }
                channel.completeStream();
            } catch (Exception e) {
                channel.sendResponse(e);
            }
        }

        private VectorSchemaRoot createTestBatch(BufferAllocator allocator, int rowCount, int batchIndex) {
            Schema schema = new Schema(
                List.of(
                    new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                    new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null)
                )
            );
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            IntVector ageVector = (IntVector) root.getVector("age");
            nameVector.allocateNew();
            ageVector.allocateNew();

            for (int i = 0; i < rowCount; i++) {
                String name = NAMES[(batchIndex * rowCount + i) % NAMES.length];
                nameVector.setSafe(i, name.getBytes(StandardCharsets.UTF_8));
                ageVector.setSafe(i, 30 + i);
            }
            root.setRowCount(rowCount);
            return root;
        }
    }

    /**
     * Response handler that implements ArrowStreamHandler to receive native Arrow data.
     * Deep-copies the VectorSchemaRoot since the Flight stream reuses its internal root.
     */
    static class ArrowAwareResponseHandler
        implements
            StreamTransportResponseHandler<ArrowDataResponse>,
            ArrowStreamHandler<ArrowDataResponse> {

        private final List<ArrowDataResponse> responses;
        private final CountDownLatch latch;
        private final AtomicReference<Exception> failure;

        ArrowAwareResponseHandler(List<ArrowDataResponse> responses, CountDownLatch latch, AtomicReference<Exception> failure) {
            this.responses = responses;
            this.latch = latch;
            this.failure = failure;
        }

        @Override
        public ArrowDataResponse readArrow(VectorSchemaRoot root) {
            // Deep-copy: the Flight stream reuses its root between next() calls,
            // so we must copy the data to an independent allocator/root.
            // Use a child allocator from the stream's allocator tree to avoid
            // "buffers must share the same root" errors with Arrow buffer operations.
            BufferAllocator streamAllocator = root.getFieldVectors().get(0).getAllocator();
            BufferAllocator childAllocator = streamAllocator.newChildAllocator("native-arrow-copy", 0, Long.MAX_VALUE);
            VectorSchemaRoot copy = VectorSchemaRoot.create(root.getSchema(), childAllocator);
            for (int i = 0; i < root.getFieldVectors().size(); i++) {
                FieldVector source = root.getFieldVectors().get(i);
                FieldVector target = copy.getFieldVectors().get(i);
                source.makeTransferPair(target).splitAndTransfer(0, root.getRowCount());
            }
            copy.setRowCount(root.getRowCount());
            return new ArrowDataResponse(copy, childAllocator);
        }

        @Override
        public void handleStreamResponse(StreamTransportResponse<ArrowDataResponse> streamResponse) {
            try {
                ArrowDataResponse response;
                while ((response = streamResponse.nextResponse()) != null) {
                    responses.add(response);
                }
                streamResponse.close();
                latch.countDown();
            } catch (Exception e) {
                failure.set(e);
                streamResponse.cancel("Test error", e);
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
        public ArrowDataResponse read(StreamInput in) throws IOException {
            // Fallback for Netty4 — not expected in this test
            return new ArrowDataResponse(in);
        }
    }

    /**
     * Plugin that registers the Arrow-native transport action.
     */
    public static class NativeArrowTestPlugin extends Plugin implements ActionPlugin {
        public NativeArrowTestPlugin() {}

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Collections.singletonList(new ActionHandler<>(ArrowDataAction.INSTANCE, TransportArrowDataAction.class));
        }
    }
}
