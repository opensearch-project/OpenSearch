/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.TransportMessageListener;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ArrowBatchResponseTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private FlightOutboundHandler outboundHandler;
    private ExecutorService executor;
    private FlightServerChannel mockFlightChannel;
    private TransportMessageListener mockListener;
    private BufferAllocator allocator;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
        threadPool = new TestThreadPool(getTestName());
        executor = Executors.newSingleThreadExecutor();
        outboundHandler = new FlightOutboundHandler("test-node", Version.CURRENT, new String[0], new StatsTracker(), threadPool);

        mockFlightChannel = mock(FlightServerChannel.class);
        when(mockFlightChannel.getExecutor()).thenReturn(executor);
        when(mockFlightChannel.getAllocator()).thenReturn(allocator);
        when(mockFlightChannel.getRoot()).thenReturn(null);

        mockListener = mock(TransportMessageListener.class);
        outboundHandler.setMessageListener(mockListener);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        allocator.close();
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        threadPool.shutdown();
        super.tearDown();
    }

    /**
     * A test TransportResponse that implements ArrowBatchResponse,
     * carrying a native VectorSchemaRoot.
     */
    static class TestArrowResponse extends TransportResponse implements ArrowBatchResponse {
        private final VectorSchemaRoot root;

        TestArrowResponse(VectorSchemaRoot root) {
            this.root = root;
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
            // Fallback for non-Flight transports — not expected to be called in native path
            throw new AssertionError("writeTo should not be called for native Arrow path");
        }
    }

    /**
     * A regular (non-Arrow) test response for comparison.
     */
    static class TestByteResponse extends TransportResponse {
        private final String data;

        TestByteResponse(String data) {
            this.data = data;
        }

        TestByteResponse(StreamInput in) throws IOException {
            this.data = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(data);
        }

        String getData() {
            return data;
        }
    }

    private VectorSchemaRoot createTestRoot() {
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

        nameVector.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
        nameVector.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
        ageVector.setSafe(0, 30);
        ageVector.setSafe(1, 25);

        root.setRowCount(2);
        return root;
    }

    public void testArrowBatchResponseSkipsWriteTo() throws Exception {
        VectorSchemaRoot testRoot = createTestRoot();
        TestArrowResponse arrowResponse = new TestArrowResponse(testRoot);

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(mockListener).onResponseSent(anyLong(), anyString(), any(TransportResponse.class));

        outboundHandler.sendResponseBatch(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            mock(FlightTransportChannel.class),
            1L,
            "test-action",
            arrowResponse,
            false,
            false
        );

        assertTrue("Batch should complete", latch.await(5, TimeUnit.SECONDS));

        // Verify sendArrowBatch was called (native path) instead of sendBatch (byte path)
        verify(mockFlightChannel).sendArrowBatch(any(ByteBuffer.class), any(ArrowBatchResponse.class));
        verify(mockFlightChannel, never()).sendBatch(any(ByteBuffer.class), any(VectorStreamOutput.class));

        testRoot.close();
    }

    public void testNonArrowResponseUsesExistingBytePath() throws Exception {
        TestByteResponse byteResponse = new TestByteResponse("hello");

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(mockListener).onResponseSent(anyLong(), anyString(), any(TransportResponse.class));

        outboundHandler.sendResponseBatch(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            mock(FlightTransportChannel.class),
            1L,
            "test-action",
            byteResponse,
            false,
            false
        );

        assertTrue("Batch should complete", latch.await(5, TimeUnit.SECONDS));

        // Verify sendBatch was called (byte path) instead of sendArrowBatch (native path)
        verify(mockFlightChannel, never()).sendArrowBatch(any(ByteBuffer.class), any(ArrowBatchResponse.class));
    }

    public void testArrowBatchResponseInterface() {
        VectorSchemaRoot testRoot = createTestRoot();
        TestArrowResponse response = new TestArrowResponse(testRoot);

        assertSame(testRoot, response.getArrowRoot());
        assertEquals(testRoot.getSchema(), response.getArrowSchema());
        assertEquals(2, response.getArrowRoot().getRowCount());

        // Verify schema has typed columns (not VarBinary blobs)
        Schema schema = response.getArrowSchema();
        assertEquals(2, schema.getFields().size());
        assertEquals("name", schema.getFields().get(0).getName());
        assertEquals("age", schema.getFields().get(1).getName());

        testRoot.close();
    }

    public void testArrowStreamHandlerInterface() {
        VectorSchemaRoot testRoot = createTestRoot();

        ArrowStreamHandler<TestArrowResponse> handler = root -> new TestArrowResponse(root);
        TestArrowResponse result = handler.readArrow(testRoot);

        assertSame(testRoot, result.getArrowRoot());
        assertEquals(2, result.getArrowRoot().getRowCount());

        testRoot.close();
    }
}
