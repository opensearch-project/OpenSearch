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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for {@link DefaultPlanExecutor}'s row-materialization boundary.
 *
 * <p>The end-to-end {@code execute(RelNode, Object)} path involves Guice-wired
 * dependencies (TransportService, Scheduler, TaskManager, CapabilityRegistry,
 * EngineContext, NodeClient) and is exercised by internal cluster tests.
 * These unit tests cover the one deterministic piece of behavior that lives
 * in this class: batches-to-rows conversion at the external API edge.
 */
public class DefaultPlanExecutorTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testBatchesToRowsEmpty() {
        Iterable<Object[]> rows = DefaultPlanExecutor.batchesToRows(List.of());
        assertFalse("no batches → no rows", rows.iterator().hasNext());
    }

    public void testBatchesToRowsSingleBatchIntegers() {
        VectorSchemaRoot batch = makeIntBatch("x", 10, 20, 30);
        List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch)));
        assertEquals(3, rows.size());
        assertArrayEquals(new Object[] { 10 }, rows.get(0));
        assertArrayEquals(new Object[] { 20 }, rows.get(1));
        assertArrayEquals(new Object[] { 30 }, rows.get(2));
    }

    public void testBatchesToRowsMultipleBatchesPreservesOrder() {
        VectorSchemaRoot batch1 = makeIntBatch("x", 1, 2);
        VectorSchemaRoot batch2 = makeIntBatch("x", 3);
        VectorSchemaRoot batch3 = makeIntBatch("x", 4, 5);
        List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch1, batch2, batch3)));
        assertEquals(5, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertEquals(2, rows.get(1)[0]);
        assertEquals(3, rows.get(2)[0]);
        assertEquals(4, rows.get(3)[0]);
        assertEquals(5, rows.get(4)[0]);
    }

    public void testBatchesToRowsMultipleColumns() {
        Schema schema = new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
            )
        );
        VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator);
        batch.allocateNew();
        BigIntVector ids = (BigIntVector) batch.getVector(0);
        VarCharVector names = (VarCharVector) batch.getVector(1);
        ids.setSafe(0, 100L);
        ids.setSafe(1, 200L);
        names.setSafe(0, "alice".getBytes(StandardCharsets.UTF_8));
        names.setSafe(1, "bob".getBytes(StandardCharsets.UTF_8));
        batch.setRowCount(2);

        List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch)));
        assertEquals(2, rows.size());
        assertEquals(100L, rows.get(0)[0]);
        assertEquals("alice", rows.get(0)[1]);
        assertEquals(200L, rows.get(1)[0]);
        assertEquals("bob", rows.get(1)[1]);
    }

    public void testBatchesToRowsHandlesNulls() {
        VectorSchemaRoot batch = VectorSchemaRoot.create(
            new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null))),
            allocator
        );
        batch.allocateNew();
        IntVector vec = (IntVector) batch.getVector(0);
        vec.setSafe(0, 1);
        vec.setNull(1);
        vec.setSafe(2, 3);
        batch.setRowCount(3);

        List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch)));
        assertEquals(3, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertNull(rows.get(1)[0]);
        assertEquals(3, rows.get(2)[0]);
    }

    public void testBatchesToRowsVarCharDecodedAsString() {
        VectorSchemaRoot batch = VectorSchemaRoot.create(
            new Schema(List.of(new Field("s", FieldType.nullable(ArrowType.Utf8.INSTANCE), null))),
            allocator
        );
        batch.allocateNew();
        VarCharVector vec = (VarCharVector) batch.getVector(0);
        vec.setSafe(0, "hello".getBytes(StandardCharsets.UTF_8));
        vec.setSafe(1, "world".getBytes(StandardCharsets.UTF_8));
        batch.setRowCount(2);

        List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch)));
        assertEquals("hello", rows.get(0)[0]);
        assertEquals("world", rows.get(1)[0]);
        assertTrue(rows.get(0)[0] instanceof String);
    }

    public void testBuildBatchesListenerSuccessRunsCleanupOnce() {
        AtomicInteger cleanupCount = new AtomicInteger(0);
        AtomicReference<Iterable<Object[]>> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ActionListener<Iterable<Object[]>> downstream = ActionListener.wrap(result::set, failure::set);

        ActionListener<Iterable<VectorSchemaRoot>> batchesListener = DefaultPlanExecutor.buildBatchesListener(
            downstream,
            cleanupCount::incrementAndGet
        );

        VectorSchemaRoot batch = makeIntBatch("x", 1, 2);
        batchesListener.onResponse(List.of(batch));

        assertEquals(1, cleanupCount.get());
        assertNotNull(result.get());
        assertEquals(2, toList(result.get()).size());
        assertNull(failure.get());
    }

    public void testBuildBatchesListenerFailureRunsCleanupOnce() {
        AtomicInteger cleanupCount = new AtomicInteger(0);
        AtomicReference<Iterable<Object[]>> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ActionListener<Iterable<Object[]>> downstream = ActionListener.wrap(result::set, failure::set);

        ActionListener<Iterable<VectorSchemaRoot>> batchesListener = DefaultPlanExecutor.buildBatchesListener(
            downstream,
            cleanupCount::incrementAndGet
        );

        Exception cause = new RuntimeException("upstream failure");
        batchesListener.onFailure(cause);

        assertEquals(1, cleanupCount.get());
        assertNull(result.get());
        assertSame(cause, failure.get());
    }

    public void testBuildBatchesListenerConversionFailureRoutesToFailureWithSingleCleanup() {
        AtomicInteger cleanupCount = new AtomicInteger(0);
        AtomicReference<Iterable<Object[]>> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ActionListener<Iterable<Object[]>> downstream = ActionListener.wrap(result::set, failure::set);

        ActionListener<Iterable<VectorSchemaRoot>> batchesListener = DefaultPlanExecutor.buildBatchesListener(
            downstream,
            cleanupCount::incrementAndGet
        );

        Iterable<VectorSchemaRoot> badBatches = () -> { throw new RuntimeException("conversion failed"); };
        batchesListener.onResponse(badBatches);

        assertEquals("cleanup must run exactly once when conversion throws", 1, cleanupCount.get());
        assertNull(result.get());
        assertNotNull(failure.get());
        assertEquals("conversion failed", failure.get().getMessage());
    }

    public void testBuildBatchesListenerCleanupFailureOnSuccessRoutesToFailure() {
        AtomicInteger cleanupCount = new AtomicInteger(0);
        AtomicReference<Iterable<Object[]>> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ActionListener<Iterable<Object[]>> downstream = ActionListener.wrap(result::set, failure::set);

        Runnable cleanup = () -> {
            cleanupCount.incrementAndGet();
            throw new RuntimeException("cleanup failed");
        };
        ActionListener<Iterable<VectorSchemaRoot>> batchesListener = DefaultPlanExecutor.buildBatchesListener(downstream, cleanup);

        VectorSchemaRoot batch = makeIntBatch("x", 1, 2);
        batchesListener.onResponse(List.of(batch));

        assertEquals("cleanup runs exactly once even when it throws", 1, cleanupCount.get());
        assertNull("downstream onResponse must not fire when cleanup throws on success path", result.get());
        assertNotNull(failure.get());
        assertEquals("cleanup failed", failure.get().getMessage());
    }

    public void testBatchesToRowsClosesBatches() {
        BufferAllocator child = allocator.newChildAllocator("test", 0, Long.MAX_VALUE);
        VectorSchemaRoot batch = makeIntBatch(child, "x", 1, 2);
        long before = child.getAllocatedMemory();
        assertTrue("batch should hold allocated memory", before > 0);
        DefaultPlanExecutor.batchesToRows(List.of(batch));
        assertEquals("batch buffers should be released after batchesToRows", 0, child.getAllocatedMemory());
        child.close();
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private VectorSchemaRoot makeIntBatch(String fieldName, int... values) {
        return makeIntBatch(allocator, fieldName, values);
    }

    private VectorSchemaRoot makeIntBatch(BufferAllocator alloc, String fieldName, int... values) {
        Field field = new Field(fieldName, FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(List.of(field));
        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, alloc);
        vsr.allocateNew();
        IntVector vec = (IntVector) vsr.getVector(0);
        for (int i = 0; i < values.length; i++) {
            vec.setSafe(i, values[i]);
        }
        vsr.setRowCount(values.length);
        return vsr;
    }

    private static <T> List<T> toList(Iterable<T> it) {
        List<T> out = new ArrayList<>();
        Iterator<T> iter = it.iterator();
        while (iter.hasNext())
            out.add(iter.next());
        return out;
    }
}
