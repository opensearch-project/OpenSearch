/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowIpcSourceTests extends OpenSearchTestCase {

    private static final Schema SCHEMA = new Schema(
            List.of(
                    new Field("id", FieldType.nullable(new ArrowType.Utf8()), null),
                    new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));

    private static VectorSchemaRoot batch(BufferAllocator allocator, String[] ids, int[] values) {
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
        VarCharVector idVector = (VarCharVector) root.getVector("id");
        IntVector valueVector = (IntVector) root.getVector("value");
        idVector.allocateNew(ids.length);
        valueVector.allocateNew(ids.length);
        for (int i = 0; i < ids.length; i++) {
            idVector.setSafe(i, ids[i].getBytes(StandardCharsets.UTF_8));
            valueVector.setSafe(i, values[i]);
        }
        root.setRowCount(ids.length);
        return root;
    }

    public void testSingleBatchRowCountAndFullRead() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot data = batch(allocator, new String[] { "a", "b", "c" }, new int[] { 1, 2, 3 })) {
            byte[] ipcBytes = ArrowTestUtils.writeIpc(data);
            try (SeekableByteChannel channel = new ByteArrayReadableSeekableByteChannel(ipcBytes);
                    ArrowIpcSource source = ArrowIpcSource.fromChannel(channel, 123L, "v1")) {
                assertEquals(3, source.getRowCount());
                assertEquals(Long.valueOf(123L), source.getVersionTimestamp());
                assertEquals("v1", source.getVersion());

                try (ArrowReader reader = source.getReader()) {
                    assertTrue(reader.loadNextBatch());
                    VectorSchemaRoot vsr = reader.getVectorSchemaRoot();
                    assertEquals(3, vsr.getRowCount());
                    assertEquals("a", new String(((VarCharVector) vsr.getVector("id")).get(0), StandardCharsets.UTF_8));
                    assertEquals("c", new String(((VarCharVector) vsr.getVector("id")).get(2), StandardCharsets.UTF_8));
                    assertFalse(reader.loadNextBatch());
                }
            }
        }
    }

    public void testMultiBatchConcatenation() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot batch1 = batch(allocator, new String[] { "a", "b" }, new int[] { 1, 2 });
                VectorSchemaRoot batch2 = batch(allocator, new String[] { "c", "d", "e" }, new int[] { 3, 4, 5 })) {
            byte[] ipcBytes = ArrowTestUtils.writeIpc(batch1, batch2);
            try (SeekableByteChannel channel = new ByteArrayReadableSeekableByteChannel(ipcBytes);
                    ArrowIpcSource source = ArrowIpcSource.fromChannel(channel, null, "v2")) {
                assertEquals(5, source.getRowCount());
                try (ArrowReader reader = source.getReader()) {
                    assertTrue(reader.loadNextBatch());
                    VectorSchemaRoot vsr = reader.getVectorSchemaRoot();
                    assertEquals(5, vsr.getRowCount());
                    VarCharVector idVector = (VarCharVector) vsr.getVector("id");
                    assertEquals("a", new String(idVector.get(0), StandardCharsets.UTF_8));
                    assertEquals("e", new String(idVector.get(4), StandardCharsets.UTF_8));
                }
            }
        }
    }

    public void testRowRangeSlicing() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot data = batch(
                        allocator,
                        new String[] { "a", "b", "c", "d", "e" },
                        new int[] { 1, 2, 3, 4, 5 })) {
            byte[] ipcBytes = ArrowTestUtils.writeIpc(data);
            try (SeekableByteChannel channel = new ByteArrayReadableSeekableByteChannel(ipcBytes);
                    ArrowIpcSource source = ArrowIpcSource.fromChannel(channel, null, "v3")) {
                try (ArrowReader reader = source.getReader(1, 4)) {
                    assertTrue(reader.loadNextBatch());
                    VectorSchemaRoot vsr = reader.getVectorSchemaRoot();
                    assertEquals(3, vsr.getRowCount());
                    VarCharVector idVector = (VarCharVector) vsr.getVector("id");
                    IntVector valueVector = (IntVector) vsr.getVector("value");
                    assertEquals("b", new String(idVector.get(0), StandardCharsets.UTF_8));
                    assertEquals("c", new String(idVector.get(1), StandardCharsets.UTF_8));
                    assertEquals("d", new String(idVector.get(2), StandardCharsets.UTF_8));
                    assertEquals(2, valueVector.get(0));
                    assertEquals(3, valueVector.get(1));
                    assertEquals(4, valueVector.get(2));
                    assertFalse(reader.loadNextBatch());
                }

                try (ArrowReader reader = source.getReader(3)) {
                    assertTrue(reader.loadNextBatch());
                    assertEquals(2, reader.getVectorSchemaRoot().getRowCount());
                }
            }
        }
    }

    public void testInvalidRangeThrows() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot data = batch(allocator, new String[] { "a", "b" }, new int[] { 1, 2 })) {
            byte[] ipcBytes = ArrowTestUtils.writeIpc(data);
            try (SeekableByteChannel channel = new ByteArrayReadableSeekableByteChannel(ipcBytes);
                    ArrowIpcSource source = ArrowIpcSource.fromChannel(channel, null, "v4")) {
                expectThrows(IllegalArgumentException.class, () -> source.getReader(-1, 1));
                expectThrows(IllegalArgumentException.class, () -> source.getReader(1, 0));
                expectThrows(IllegalArgumentException.class, () -> source.getReader(0, 3));
            }
        }
    }
}
