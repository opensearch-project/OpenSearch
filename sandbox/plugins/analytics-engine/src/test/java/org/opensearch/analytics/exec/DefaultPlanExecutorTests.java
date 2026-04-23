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
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
        try {
            List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch)));
            assertEquals(3, rows.size());
            assertArrayEquals(new Object[] { 10 }, rows.get(0));
            assertArrayEquals(new Object[] { 20 }, rows.get(1));
            assertArrayEquals(new Object[] { 30 }, rows.get(2));
        } finally {
            batch.close();
        }
    }

    public void testBatchesToRowsMultipleBatchesPreservesOrder() {
        VectorSchemaRoot batch1 = makeIntBatch("x", 1, 2);
        VectorSchemaRoot batch2 = makeIntBatch("x", 3);
        VectorSchemaRoot batch3 = makeIntBatch("x", 4, 5);
        try {
            List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch1, batch2, batch3)));
            assertEquals(5, rows.size());
            assertEquals(1, rows.get(0)[0]);
            assertEquals(2, rows.get(1)[0]);
            assertEquals(3, rows.get(2)[0]);
            assertEquals(4, rows.get(3)[0]);
            assertEquals(5, rows.get(4)[0]);
        } finally {
            batch1.close();
            batch2.close();
            batch3.close();
        }
    }

    public void testBatchesToRowsMultipleColumns() {
        Schema schema = new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
            )
        );
        VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator);
        try {
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
        } finally {
            batch.close();
        }
    }

    public void testBatchesToRowsHandlesNulls() {
        VectorSchemaRoot batch = VectorSchemaRoot.create(
            new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null))),
            allocator
        );
        try {
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
        } finally {
            batch.close();
        }
    }

    public void testBatchesToRowsVarCharDecodedAsString() {
        VectorSchemaRoot batch = VectorSchemaRoot.create(
            new Schema(List.of(new Field("s", FieldType.nullable(ArrowType.Utf8.INSTANCE), null))),
            allocator
        );
        try {
            batch.allocateNew();
            VarCharVector vec = (VarCharVector) batch.getVector(0);
            vec.setSafe(0, "hello".getBytes(StandardCharsets.UTF_8));
            vec.setSafe(1, "world".getBytes(StandardCharsets.UTF_8));
            batch.setRowCount(2);

            List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch)));
            assertEquals("hello", rows.get(0)[0]);
            assertEquals("world", rows.get(1)[0]);
            // explicit type check — we return String, not the raw Text the underlying getObject returns
            assertTrue(rows.get(0)[0] instanceof String);
        } finally {
            batch.close();
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private VectorSchemaRoot makeIntBatch(String fieldName, int... values) {
        Field field = new Field(fieldName, FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(List.of(field));
        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, allocator);
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
