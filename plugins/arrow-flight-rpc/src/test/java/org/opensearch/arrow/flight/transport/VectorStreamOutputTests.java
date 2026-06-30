/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class VectorStreamOutputTests extends OpenSearchTestCase {
    private RootAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        allocator.close();
    }

    // ── ByteSerialized tests ──

    public void testByteSerializedConstructorWithoutExistingRoot() throws IOException {
        try (VectorStreamOutput output = VectorStreamOutput.create(allocator, null)) {
            output.writeByte((byte) 42);
            VectorSchemaRoot root = output.getRoot();
            assertNotNull(root);
            assertEquals(1, root.getRowCount());
        }
    }

    public void testByteSerializedConstructorWithExistingRoot() throws IOException {
        Field field = new Field("0", new FieldType(true, new ArrowType.Binary(), null, null), null);
        VarBinaryVector vector = (VarBinaryVector) field.createVector(allocator);
        vector.setInitialCapacity(16);
        vector.allocateNew();

        VectorSchemaRoot existingRoot = new VectorSchemaRoot(List.of(vector));
        try (VectorStreamOutput output = VectorStreamOutput.create(allocator, existingRoot)) {
            output.writeByte((byte) 7);
            VectorSchemaRoot root = output.getRoot();
            assertSame(existingRoot, root);
            assertEquals(1, root.getRowCount());
            byte[] value = ((VarBinaryVector) root.getVector(0)).get(0);
            assertEquals(1, value.length);
            assertEquals((byte) 7, value[0]);
        }
    }

    public void testWriteByteTriggersFlushTempBuffer() throws IOException {
        try (VectorStreamOutput output = VectorStreamOutput.create(allocator, null)) {
            // Write exactly 8192 bytes to fill the temp buffer, then one more to trigger flush
            for (int i = 0; i < 8193; i++) {
                output.writeByte((byte) (i & 0xFF));
            }
            VectorSchemaRoot root = output.getRoot();
            // First 8192 bytes flushed as one row, remaining 1 byte flushed as second row
            assertEquals(2, root.getRowCount());
            VarBinaryVector vector = (VarBinaryVector) root.getVector(0);
            assertEquals(8192, vector.get(0).length);
            assertEquals(1, vector.get(1).length);
        }
    }

    public void testWriteBytesWithInvalidOffsetThrows() throws IOException {
        try (VectorStreamOutput output = VectorStreamOutput.create(allocator, null)) {
            byte[] data = new byte[5];
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> output.writeBytes(data, 3, 5));
            assertTrue(e.getMessage().contains("Illegal offset"));
        }
    }

    public void testWriteBytesWithInvalidLengthThrows() throws IOException {
        try (VectorStreamOutput output = VectorStreamOutput.create(allocator, null)) {
            byte[] data = new byte[5];
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> output.writeBytes(data, 0, 10));
            assertTrue(e.getMessage().contains("Illegal offset"));
        }
    }

    public void testWriteBytesWithZeroLengthIsNoOp() throws IOException {
        try (VectorStreamOutput output = VectorStreamOutput.create(allocator, null)) {
            output.writeBytes(new byte[5], 0, 0);
            VectorSchemaRoot root = output.getRoot();
            assertEquals(0, root.getRowCount());
        }
    }

    public void testWriteBytesFlushesPendingTempBuffer() throws IOException {
        try (VectorStreamOutput output = VectorStreamOutput.create(allocator, null)) {
            // Write some bytes via writeByte to fill tempBuffer partially
            output.writeByte((byte) 1);
            output.writeByte((byte) 2);
            // Now writeBytes should flush the pending temp buffer first
            byte[] data = new byte[] { 3, 4, 5 };
            output.writeBytes(data, 0, 3);

            VectorSchemaRoot root = output.getRoot();
            assertEquals(2, root.getRowCount());
            VarBinaryVector vector = (VarBinaryVector) root.getVector(0);
            // Row 0: flushed temp buffer (2 bytes from writeByte)
            byte[] row0 = vector.get(0);
            assertEquals(2, row0.length);
            assertEquals((byte) 1, row0[0]);
            assertEquals((byte) 2, row0[1]);
            // Row 1: direct writeBytes data
            byte[] row1 = vector.get(1);
            assertEquals(3, row1.length);
            assertEquals((byte) 3, row1[0]);
        }
    }

    public void testResetClearsState() throws IOException {
        try (VectorStreamOutput output = VectorStreamOutput.create(allocator, null)) {
            output.writeByte((byte) 1);
            output.writeBytes(new byte[] { 2, 3 }, 0, 2);

            output.reset();

            // After reset, writing and getting root should start fresh
            output.writeByte((byte) 99);
            VectorSchemaRoot root = output.getRoot();
            assertEquals(1, root.getRowCount());
            byte[] value = ((VarBinaryVector) root.getVector(0)).get(0);
            assertEquals(1, value.length);
            assertEquals((byte) 99, value[0]);
        }
    }

    public void testGetRootCreatesRootOnFirstCallReusesOnSecond() throws IOException {
        try (VectorStreamOutput output = VectorStreamOutput.create(allocator, null)) {
            output.writeByte((byte) 1);
            VectorSchemaRoot root1 = output.getRoot();
            assertNotNull(root1);

            output.writeByte((byte) 2);
            VectorSchemaRoot root2 = output.getRoot();
            assertSame(root1, root2);
        }
    }

    public void testCloseReleasesVector() throws IOException {
        VectorStreamOutput output = VectorStreamOutput.create(allocator, null);
        output.writeByte((byte) 1);
        output.close();
        // After close, allocator should have no outstanding allocations
        assertEquals(0, allocator.getAllocatedMemory());
    }

    // ── NativeArrow tests ──

    public void testNativeArrowGetRootReturnsSameRoot() throws IOException {
        Field field = new Field("0", new FieldType(true, new ArrowType.Binary(), null, null), null);
        VarBinaryVector vector = (VarBinaryVector) field.createVector(allocator);
        vector.allocateNew();
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(vector));

        try (VectorStreamOutput output = VectorStreamOutput.forNativeArrow(root)) {
            assertSame(root, output.getRoot());
            assertSame(root, output.getRoot());
        } finally {
            root.close();
        }
    }

    public void testNativeArrowWriteByteIsNoOp() throws IOException {
        Field field = new Field("0", new FieldType(true, new ArrowType.Binary(), null, null), null);
        VarBinaryVector vector = (VarBinaryVector) field.createVector(allocator);
        vector.allocateNew();
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(vector));

        try (VectorStreamOutput output = VectorStreamOutput.forNativeArrow(root)) {
            output.writeByte((byte) 42);
            // Vector should remain unchanged
            assertEquals(0, vector.getValueCount());
        } finally {
            root.close();
        }
    }

    public void testNativeArrowWriteBytesIsNoOp() throws IOException {
        Field field = new Field("0", new FieldType(true, new ArrowType.Binary(), null, null), null);
        VarBinaryVector vector = (VarBinaryVector) field.createVector(allocator);
        vector.allocateNew();
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(vector));

        try (VectorStreamOutput output = VectorStreamOutput.forNativeArrow(root)) {
            output.writeBytes(new byte[] { 1, 2, 3 }, 0, 3);
            assertEquals(0, vector.getValueCount());
        } finally {
            root.close();
        }
    }

    public void testNativeArrowFlushAndResetAreNoOps() throws IOException {
        Field field = new Field("0", new FieldType(true, new ArrowType.Binary(), null, null), null);
        VarBinaryVector vector = (VarBinaryVector) field.createVector(allocator);
        vector.allocateNew();
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(vector));

        try (VectorStreamOutput output = VectorStreamOutput.forNativeArrow(root)) {
            output.flush();
            output.reset();
            // Root should still be the same and accessible
            assertSame(root, output.getRoot());
        } finally {
            root.close();
        }
    }

    public void testNativeArrowCloseDoesNotCloseRoot() throws IOException {
        Field field = new Field("0", new FieldType(true, new ArrowType.Binary(), null, null), null);
        VarBinaryVector vector = (VarBinaryVector) field.createVector(allocator);
        vector.allocateNew();
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(vector));

        try {
            VectorStreamOutput output = VectorStreamOutput.forNativeArrow(root);
            output.close();
            // Root should still be usable after NativeArrow.close()
            assertNotNull(root.getSchema());
            assertEquals(1, root.getFieldVectors().size());
        } finally {
            root.close();
        }
    }
}
