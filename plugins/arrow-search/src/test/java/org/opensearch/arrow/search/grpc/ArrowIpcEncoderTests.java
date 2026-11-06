/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.grpc;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.arrow.search.Column;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.spi.StreamPayload;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class ArrowIpcEncoderTests extends OpenSearchTestCase {

    private static final Schema SCHEMA = new Schema(
        List.of(column("l", "long"), column("d", "double"), column("b", "boolean"), column("k", "keyword"), column("ip", "ip"))
    );

    private BufferAllocator allocator;

    private static Field column(String name, String mappingType) {
        return new Column(name, Column.columnType(mappingType), Set.of(mappingType)).toArrowField();
    }

    @Before
    public void createAllocator() {
        allocator = new RootAllocator();
    }

    @After
    public void closeAllocator() {
        assertEquals("encoded batches must release their memory", 0, allocator.getAllocatedMemory());
        allocator.close();
    }

    private VectorSchemaRoot batch(int rows, int seed) {
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
        root.allocateNew();
        for (int i = 0; i < rows; i++) {
            int v = seed + i;
            if (v % 3 == 0) {
                root.getVector("l").setNull(i);
            } else {
                ((BigIntVector) root.getVector("l")).setSafe(i, v);
            }
            ((Float8Vector) root.getVector("d")).setSafe(i, v / 2.0);
            ((BitVector) root.getVector("b")).setSafe(i, v % 2);
            if (v % 4 == 0) {
                root.getVector("k").setNull(i);
            } else {
                ((VarCharVector) root.getVector("k")).setSafe(i, ("value-" + "x".repeat(v % 7) + v).getBytes(StandardCharsets.UTF_8));
            }
            byte[] ip = new byte[16];
            ip[15] = (byte) v;
            ((FixedSizeBinaryVector) root.getVector("ip")).setSafe(i, ip);
        }
        root.setRowCount(rows);
        return root;
    }

    private static void append(ByteArrayOutputStream out, StreamPayload payload) {
        for (ByteBuffer segment : payload.segments()) {
            ByteBuffer copy = segment.duplicate();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            out.write(bytes, 0, bytes.length);
        }
        payload.release();
    }

    private static List<String> rows(VectorSchemaRoot root) {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < root.getRowCount(); i++) {
            StringBuilder row = new StringBuilder();
            for (var vector : root.getFieldVectors()) {
                Object value = vector.getObject(i);
                row.append(value instanceof byte[] bytes ? Arrays.toString(bytes) : String.valueOf(value)).append('|');
            }
            rows.add(row.toString());
        }
        return rows;
    }

    public void testPayloadsFormAStreamReadableByArrow() throws Exception {
        List<String> expected = new ArrayList<>();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        append(stream, ArrowIpcEncoder.schema(SCHEMA));
        int batches = randomIntBetween(1, 4);
        for (int b = 0; b < batches; b++) {
            VectorSchemaRoot root = batch(randomIntBetween(0, 50), b * 100);
            expected.addAll(rows(root));
            StreamPayload payload = ArrowIpcEncoder.recordBatch(root);
            // Only the flatbuffer header is on heap; body buffers and padding are direct, so the transport need not copy them.
            assertFalse(payload.segments().get(0).isDirect());
            assertTrue(payload.segments().subList(1, payload.segments().size()).stream().allMatch(ByteBuffer::isDirect));
            append(stream, payload);
        }

        List<String> actual = new ArrayList<>();
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(stream.toByteArray()), allocator)) {
            assertEquals(SCHEMA, reader.getVectorSchemaRoot().getSchema());
            int read = 0;
            while (reader.loadNextBatch()) {
                actual.addAll(rows(reader.getVectorSchemaRoot()));
                read++;
            }
            assertEquals(batches, read);
        }
        assertEquals(expected, actual);
    }

    public void testTruncatedBatchEncodesOnlyRemainingRows() throws Exception {
        VectorSchemaRoot root = batch(20, 1);
        List<String> expected = rows(root).subList(0, 7);
        root.setRowCount(7);

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        append(stream, ArrowIpcEncoder.schema(SCHEMA));
        append(stream, ArrowIpcEncoder.recordBatch(root));

        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(stream.toByteArray()), allocator)) {
            assertTrue(reader.loadNextBatch());
            assertEquals(expected, rows(reader.getVectorSchemaRoot()));
        }
    }

    public void testReleasingThePayloadFreesTheBatch() {
        StreamPayload payload = ArrowIpcEncoder.recordBatch(batch(10, 0));
        assertTrue(allocator.getAllocatedMemory() > 0);
        payload.release();
        assertEquals(0, allocator.getAllocatedMemory());
    }
}
