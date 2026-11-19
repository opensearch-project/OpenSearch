/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.opensearch.arrow.search.Column;
import org.opensearch.arrow.search.Column.ColumnType;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class BatchBytesTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Before
    public void createAllocator() {
        allocator = new RootAllocator();
    }

    @After
    public void closeAllocator() {
        assertEquals(0, allocator.getAllocatedMemory());
        allocator.close();
    }

    private VectorSchemaRoot randomRoot(int rows) {
        List<Column> columns = new ArrayList<>();
        int count = randomIntBetween(1, 6);
        for (int c = 0; c < count; c++) {
            ColumnType type = randomFrom(ColumnType.values());
            columns.add(new Column("c" + c, type, Set.of("x"), randomBoolean()));
        }
        VectorSchemaRoot root = VectorSchemaRoot.create(Column.toSchema(columns), allocator);
        root.allocateNew();
        for (FieldVector vector : root.getFieldVectors()) {
            for (int i = 0; i < rows; i++) {
                if (randomInt(4) == 0) {
                    if (vector instanceof ListVector == false) {
                        while (vector.getValueCapacity() <= i) {
                            vector.reAlloc();
                        }
                    }
                    vector.setNull(i);
                } else if (vector instanceof ListVector list) {
                    int start = list.startNewValue(i);
                    int items = randomIntBetween(0, 4);
                    for (int v = 0; v < items; v++) {
                        setValue(list.getDataVector(), start + v);
                    }
                    list.endValue(i, items);
                } else {
                    setValue(vector, i);
                }
            }
        }
        root.setRowCount(rows);
        return root;
    }

    private static void setValue(FieldVector vector, int i) {
        if (vector instanceof VarCharVector varChar) {
            varChar.setSafe(i, randomAlphaOfLength(randomIntBetween(0, 40)).getBytes(StandardCharsets.UTF_8));
        } else if (vector instanceof FixedSizeBinaryVector binary) {
            binary.setSafe(i, new byte[16]);
        } else if (vector instanceof BitVector bits) {
            bits.setSafe(i, randomInt(1));
        } else {
            BaseFixedWidthVector fixed = (BaseFixedWidthVector) vector;
            while (fixed.getValueCapacity() <= i) {
                fixed.reAlloc();
            }
            fixed.getDataBuffer().setByte((long) i * fixed.getTypeWidth(), 1);
            fixed.setIndexDefined(i);
        }
    }

    private static long serializedBytes(VectorSchemaRoot root) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch()) {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), batch, IpcOption.DEFAULT);
        }
        return out.size();
    }

    public void testBoundsTheIpcMessageOfEveryPrefix() throws Exception {
        int rows = randomIntBetween(0, 300);
        try (VectorSchemaRoot root = randomRoot(rows)) {
            for (int prefix : new int[] { 0, rows / 3, rows }) {
                long estimate = BatchBytes.messageBytes(root, prefix);
                ColumnWriters.setRowCount(root, prefix);
                long actual = serializedBytes(root);
                assertTrue("estimate " + estimate + " below actual " + actual, estimate >= actual);
                // Only the metadata is estimated; the body is exact.
                long slack = BatchBytes.metadataBytes(root);
                assertTrue("estimate " + estimate + " too far above actual " + actual, estimate - actual <= slack);
                ColumnWriters.setRowCount(root, rows);
            }
        }
    }

    public void testGrowsWithRows() {
        try (VectorSchemaRoot root = randomRoot(100)) {
            long previous = -1;
            for (int prefix = 0; prefix <= 100; prefix++) {
                long bytes = BatchBytes.messageBytes(root, prefix);
                assertTrue(bytes >= previous);
                previous = bytes;
            }
        }
    }
}
