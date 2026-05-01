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
import org.opensearch.arrow.flight.transport.ArrowFlightChannel;
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
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

/**
 * Integration test for the native Arrow transport path.
 * Tests serial and parallel batch production with zero-copy transfer,
 * verifying typed data integrity end-to-end.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 2, maxNumDataNodes = 2)
public class NativeArrowTransportIT extends OpenSearchIntegTestCase {

    private static final Schema TEST_SCHEMA = new Schema(
        List.of(
            new Field("batch_id", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
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
        return List.of(NativeArrowTestPlugin.class, FlightStreamPlugin.class);
    }

    // ── Tests ──

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testSingleBatchNativeArrow() throws Exception {
        {
            DiscoveryNode node = getClusterState().nodes().iterator().next();
            List<ReceivedBatch> batches = sendAndReceive(node, 1, 3, 1);

            assertEquals("Should receive 1 batch", 1, batches.size());
            ReceivedBatch batch = batches.get(0);
            assertEquals(3, batch.rowCount);
            assertEquals(0, batch.batchId);
            assertBatchIntegrity(batch);
        }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testMultipleBatchesSerialNativeArrow() throws Exception {
        {
            DiscoveryNode node = getClusterState().nodes().iterator().next();
            List<ReceivedBatch> batches = sendAndReceive(node, 5, 4, 1);

            assertEquals("Should receive 5 batches", 5, batches.size());
            Set<Integer> batchIds = new HashSet<>();
            for (ReceivedBatch batch : batches) {
                assertEquals(4, batch.rowCount);
                assertBatchIntegrity(batch);
                batchIds.add(batch.batchId);
            }
            assertEquals("All batch IDs should be unique", 5, batchIds.size());
        }
    }

    /**
     * Collects every batch without reading vector data, fully drains and closes the stream, then
     * verifies each retained batch still holds its data. Mirrors an async consumer that defers
     * reading until after the stream has advanced or been closed.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testBatchesSurviveStreamAdvanceAndClose() throws Exception {
        DiscoveryNode node = getClusterState().nodes().iterator().next();
        StreamTransportService sts = internalCluster().getInstance(StreamTransportService.class);
        List<TestArrowResponse> retained = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();

        int batchCount = 3;
        int rowsPerBatch = 4;
        sts.sendRequest(
            node,
            TestArrowAction.NAME,
            new TestArrowRequest(batchCount, rowsPerBatch, 1),
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            new StreamTransportResponseHandler<TestArrowResponse>() {
                @Override
                public void handleStreamResponse(StreamTransportResponse<TestArrowResponse> streamResponse) {
                    try {
                        TestArrowResponse response;
                        // Collect references WITHOUT reading vector data — defer that until after close.
                        while ((response = streamResponse.nextResponse()) != null) {
                            retained.add(response);
                        }
                        streamResponse.close();
                    } catch (Exception e) {
                        failure.set(e);
                        streamResponse.cancel("Test error", e);
                    } finally {
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
                public TestArrowResponse read(StreamInput in) throws IOException {
                    return new TestArrowResponse(in);
                }
            }
        );

        assertTrue("Stream should complete within 30s", latch.await(30, TimeUnit.SECONDS));
        assertNull("No exception expected: " + failure.get(), failure.get());
        assertEquals(batchCount, retained.size());

        try {
            // Every retained batch must still have its data intact even though the stream has
            // advanced and closed.
            for (int batchIdx = 0; batchIdx < retained.size(); batchIdx++) {
                VectorSchemaRoot root = retained.get(batchIdx).getRoot();
                assertEquals("row count must survive stream close", rowsPerBatch, root.getRowCount());
                IntVector batchIdVec = (IntVector) root.getVector("batch_id");
                VarCharVector nameVec = (VarCharVector) root.getVector("name");
                IntVector valueVec = (IntVector) root.getVector("value");
                assertEquals("valueCount must survive stream close", rowsPerBatch, batchIdVec.getValueCount());
                for (int row = 0; row < rowsPerBatch; row++) {
                    assertEquals("batch_id survives", batchIdx, batchIdVec.get(row));
                    assertEquals("name survives", "row-" + batchIdx + "-" + row, new String(nameVec.get(row), StandardCharsets.UTF_8));
                    assertEquals("value survives", batchIdx * 1000 + row, valueVec.get(row));
                }
            }
        } finally {
            for (TestArrowResponse r : retained) {
                r.getRoot().close();
            }
        }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testParallelBatchProduction() throws Exception {
        // 100 batches, 10 rows each, produced by 5 parallel threads.
        // Each batch has a unique batch_id. Verifies:
        // - All 100 batches arrive
        // - No batch is lost or duplicated
        // - Each batch's data is internally consistent (batch_id matches across all rows)
        {
            DiscoveryNode node = getClusterState().nodes().iterator().next();
            List<ReceivedBatch> batches = sendAndReceive(node, 100, 10, 5);

            assertEquals("Should receive 100 batches", 100, batches.size());

            Set<Integer> batchIds = new HashSet<>();
            for (ReceivedBatch batch : batches) {
                assertEquals("Each batch should have 10 rows", 10, batch.rowCount);
                assertBatchIntegrity(batch);
                assertTrue("Batch ID should be in range [0, 100)", batch.batchId >= 0 && batch.batchId < 100);
                batchIds.add(batch.batchId);
            }
            assertEquals("All 100 batch IDs must be present (no lost/duplicated batches)", 100, batchIds.size());
        }
    }

    // ── Helpers ──

    private List<ReceivedBatch> sendAndReceive(DiscoveryNode node, int batchCount, int rowsPerBatch, int parallelism) throws Exception {
        StreamTransportService sts = internalCluster().getInstance(StreamTransportService.class);
        List<ReceivedBatch> batches = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();

        sts.sendRequest(
            node,
            TestArrowAction.NAME,
            new TestArrowRequest(batchCount, rowsPerBatch, parallelism),
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            new TestArrowResponseHandler(batches, latch, failure)
        );

        assertTrue("Stream should complete within 30s", latch.await(30, TimeUnit.SECONDS));
        assertNull("No exception expected: " + failure.get(), failure.get());
        return batches;
    }

    /** Verifies that all rows in a batch have the same batch_id and consistent name/value. */
    private void assertBatchIntegrity(ReceivedBatch batch) {
        for (int i = 0; i < batch.rowCount; i++) {
            assertEquals("batch_id must be consistent across all rows", batch.batchId, batch.batchIds.get(i).intValue());
            // name = "row-{batchId}-{rowIndex}"
            String expectedName = "row-" + batch.batchId + "-" + i;
            assertEquals("Name must match expected pattern", expectedName, batch.names.get(i));
            // value = batchId * 1000 + rowIndex
            assertEquals("Value must match expected pattern", batch.batchId * 1000 + i, batch.values.get(i).intValue());
        }
    }

    /** Deep-copies data from a VectorSchemaRoot. */
    static class ReceivedBatch {
        final int rowCount;
        final int batchId;
        final List<Integer> batchIds;
        final List<String> names;
        final List<Integer> values;

        ReceivedBatch(VectorSchemaRoot root) {
            this.rowCount = root.getRowCount();
            IntVector batchIdVector = (IntVector) root.getVector("batch_id");
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            IntVector valueVector = (IntVector) root.getVector("value");
            this.batchIds = new ArrayList<>();
            this.names = new ArrayList<>();
            this.values = new ArrayList<>();
            for (int i = 0; i < rowCount; i++) {
                batchIds.add(batchIdVector.get(i));
                names.add(new String(nameVector.get(i), StandardCharsets.UTF_8));
                values.add(valueVector.get(i));
            }
            this.batchId = rowCount > 0 ? batchIds.get(0) : -1;
        }
    }

    // ── Inner classes: Action, Request, Response, Handler, Plugin ──

    public static class TestArrowResponse extends ArrowBatchResponse {
        public TestArrowResponse(VectorSchemaRoot root) {
            super(root);
        }

        public TestArrowResponse(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class TestArrowRequest extends ActionRequest {
        private final int batchCount;
        private final int rowsPerBatch;
        private final int parallelism;

        public TestArrowRequest(int batchCount, int rowsPerBatch, int parallelism) {
            this.batchCount = batchCount;
            this.rowsPerBatch = rowsPerBatch;
            this.parallelism = parallelism;
        }

        public TestArrowRequest(StreamInput in) throws IOException {
            super(in);
            this.batchCount = in.readInt();
            this.rowsPerBatch = in.readInt();
            this.parallelism = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(batchCount);
            out.writeInt(rowsPerBatch);
            out.writeInt(parallelism);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class TestArrowAction extends ActionType<TestArrowResponse> {
        public static final TestArrowAction INSTANCE = new TestArrowAction();
        public static final String NAME = "cluster:internal/test/native_arrow";

        private TestArrowAction() {
            super(NAME, TestArrowResponse::new);
        }
    }

    /**
     * Server-side handler. Produces batches using a thread pool.
     * Each producer thread creates a batch with its own child allocator,
     * puts it on a queue. The main thread drains the queue and sends
     * batches via sendResponseBatch(). The framework does zero-copy transfer
     * on the executor thread.
     */
    public static class TransportTestArrowAction extends TransportAction<TestArrowRequest, TestArrowResponse> {

        @Inject
        public TransportTestArrowAction(StreamTransportService streamTransportService, ActionFilters actionFilters) {
            super(TestArrowAction.NAME, actionFilters, streamTransportService.getTaskManager());
            streamTransportService.registerRequestHandler(
                TestArrowAction.NAME,
                ThreadPool.Names.GENERIC,
                TestArrowRequest::new,
                this::handleStreamRequest
            );
        }

        @Override
        protected void doExecute(Task task, TestArrowRequest request, ActionListener<TestArrowResponse> listener) {
            listener.onFailure(new UnsupportedOperationException("Use StreamTransportService"));
        }

        private void handleStreamRequest(TestArrowRequest request, TransportChannel channel, Task task) throws IOException {
            BufferAllocator allocator = ArrowFlightChannel.from(channel).getAllocator();

            try {
                if (request.parallelism <= 1) {
                    // Serial production
                    for (int batch = 0; batch < request.batchCount; batch++) {
                        channel.sendResponseBatch(new TestArrowResponse(createBatch(allocator, batch, request.rowsPerBatch)));
                    }
                } else {
                    // Parallel production: N threads produce batches into a queue,
                    // main thread drains and sends serially.
                    BlockingQueue<TestArrowResponse> queue = new LinkedBlockingQueue<>();
                    CountDownLatch producersDone = new CountDownLatch(request.batchCount);
                    ExecutorService producers = Executors.newFixedThreadPool(request.parallelism);

                    for (int batch = 0; batch < request.batchCount; batch++) {
                        final int batchIndex = batch;
                        producers.submit(() -> {
                            try {
                                VectorSchemaRoot root = createBatch(allocator, batchIndex, request.rowsPerBatch);
                                queue.put(new TestArrowResponse(root));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                producersDone.countDown();
                            }
                        });
                    }

                    // Drain: send batches as they become available
                    int sent = 0;
                    while (sent < request.batchCount) {
                        TestArrowResponse response = queue.poll(10, TimeUnit.SECONDS);
                        if (response == null) throw new IOException("Timed out waiting for producer");
                        channel.sendResponseBatch(response);
                        sent++;
                    }

                    producersDone.await(30, TimeUnit.SECONDS);
                    producers.shutdown();
                }
                channel.completeStream();
            } catch (StreamException e) {
                if (e.getErrorCode() != StreamErrorCode.CANCELLED) channel.sendResponse(e);
            } catch (Exception e) {
                channel.sendResponse(e);
            }
        }

        private VectorSchemaRoot createBatch(BufferAllocator allocator, int batchIndex, int rowCount) {
            VectorSchemaRoot root = VectorSchemaRoot.create(TEST_SCHEMA, allocator);

            IntVector batchIdVector = (IntVector) root.getVector("batch_id");
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            IntVector valueVector = (IntVector) root.getVector("value");
            batchIdVector.allocateNew();
            nameVector.allocateNew();
            valueVector.allocateNew();

            for (int i = 0; i < rowCount; i++) {
                batchIdVector.setSafe(i, batchIndex);
                nameVector.setSafe(i, ("row-" + batchIndex + "-" + i).getBytes(StandardCharsets.UTF_8));
                valueVector.setSafe(i, batchIndex * 1000 + i);
            }
            root.setRowCount(rowCount);
            return root;
        }
    }

    static class TestArrowResponseHandler implements StreamTransportResponseHandler<TestArrowResponse> {
        private final List<ReceivedBatch> batches;
        private final CountDownLatch latch;
        private final AtomicReference<Exception> failure;

        TestArrowResponseHandler(List<ReceivedBatch> batches, CountDownLatch latch, AtomicReference<Exception> failure) {
            this.batches = batches;
            this.latch = latch;
            this.failure = failure;
        }

        @Override
        public void handleStreamResponse(StreamTransportResponse<TestArrowResponse> streamResponse) {
            try {
                TestArrowResponse response;
                while ((response = streamResponse.nextResponse()) != null) {
                    batches.add(new ReceivedBatch(response.getRoot()));
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
        public TestArrowResponse read(StreamInput in) throws IOException {
            return new TestArrowResponse(in);
        }
    }

    public static class NativeArrowTestPlugin extends Plugin implements ActionPlugin {
        public NativeArrowTestPlugin() {}

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(TestArrowAction.INSTANCE, TransportTestArrowAction.class));
        }
    }
}
