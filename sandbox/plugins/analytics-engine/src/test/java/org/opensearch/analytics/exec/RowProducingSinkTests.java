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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * Tests for {@link RowProducingSink}.
 */
public class RowProducingSinkTests extends OpenSearchTestCase {

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

    public void testFeedSingleBatch() {
        RowProducingSink sink = new RowProducingSink();

        VectorSchemaRoot batch = makeVsr(List.of("name", "age"), new Object[][] { { "alice", "30" }, { "bob", "25" } });
        sink.feed(batch);

        assertEquals(2, sink.getRowCount());
        Iterator<VectorSchemaRoot> it = sink.readResult().iterator();
        assertTrue(it.hasNext());
        assertSame(batch, it.next());
        assertFalse(it.hasNext());

        sink.close();
    }

    public void testFeedMultipleBatchesPreservesOrder() {
        RowProducingSink sink = new RowProducingSink();

        VectorSchemaRoot r1 = makeVsr(List.of("id"), new Object[][] { { "1" } });
        VectorSchemaRoot r2 = makeVsr(List.of("id"), new Object[][] { { "2" } });
        VectorSchemaRoot r3 = makeVsr(List.of("id"), new Object[][] { { "3" } });

        sink.feed(r1);
        sink.feed(r2);
        sink.feed(r3);

        assertEquals(3, sink.getRowCount());
        Iterator<VectorSchemaRoot> it = sink.readResult().iterator();
        assertSame(r1, it.next());
        assertSame(r2, it.next());
        assertSame(r3, it.next());
        assertFalse(it.hasNext());

        sink.close();
    }

    public void testFieldNamesCapturedFromFirstNonEmptyBatch() {
        RowProducingSink sink = new RowProducingSink();

        // First batch has no fields (empty schema)
        VectorSchemaRoot emptyBatch = VectorSchemaRoot.create(new Schema(List.of()), allocator);
        emptyBatch.setRowCount(0);
        sink.feed(emptyBatch);

        // Second batch has field names
        VectorSchemaRoot withFields = makeVsr(List.of("col_a"), new Object[][] { { "y" } });
        sink.feed(withFields);

        // Field names should come from the second batch
        assertEquals("y", sink.getValueAt("col_a", 0).toString());

        sink.close();
    }

    public void testGetValueAtValidColumnAndRow() {
        RowProducingSink sink = new RowProducingSink();

        VectorSchemaRoot batch = makeVsr(List.of("col1", "col2"), new Object[][] { { "hello", "42" } });
        sink.feed(batch);

        assertEquals("hello", sink.getValueAt("col1", 0).toString());
        assertEquals("42", sink.getValueAt("col2", 0).toString());

        sink.close();
    }

    public void testGetValueAtUnknownColumnReturnsNull() {
        RowProducingSink sink = new RowProducingSink();

        VectorSchemaRoot batch = makeVsr(List.of("col1"), new Object[][] { { "data" } });
        sink.feed(batch);

        assertNull(sink.getValueAt("nonexistent", 0));

        sink.close();
    }

    public void testGetValueAtOutOfRangeRowReturnsNull() {
        RowProducingSink sink = new RowProducingSink();

        VectorSchemaRoot batch = makeVsr(List.of("col1"), new Object[][] { { "only_row" } });
        sink.feed(batch);

        assertNull(sink.getValueAt("col1", 5));

        sink.close();
    }

    public void testEmptySinkReturnsEmptyResult() {
        RowProducingSink sink = new RowProducingSink();

        assertEquals(0, sink.getRowCount());
        Iterator<VectorSchemaRoot> it = sink.readResult().iterator();
        assertFalse(it.hasNext());

        sink.close();
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private VectorSchemaRoot makeVsr(List<String> fieldNames, Object[][] rows) {
        List<Field> fields = fieldNames.stream().map(name -> new Field(name, FieldType.nullable(ArrowType.Utf8.INSTANCE), null)).toList();
        Schema schema = new Schema(fields);
        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, allocator);
        vsr.allocateNew();
        int rowCount = rows.length;
        for (int col = 0; col < fieldNames.size(); col++) {
            VarCharVector vec = (VarCharVector) vsr.getVector(col);
            for (int r = 0; r < rowCount; r++) {
                Object value = rows[r][col];
                if (value == null) {
                    vec.setNull(r);
                } else {
                    vec.setSafe(r, value.toString().getBytes(StandardCharsets.UTF_8));
                }
            }
            vec.setValueCount(rowCount);
        }
        vsr.setRowCount(rowCount);
        return vsr;
    }
}
