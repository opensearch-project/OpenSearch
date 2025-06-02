/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.stream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.text.Text;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArrowStreamOutputTests extends OpenSearchTestCase {
    private RootAllocator allocator;
    private ArrowStreamOutput output;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
        output = new ArrowStreamOutput(allocator);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (output != null) {
            output.close();
        }
        allocator.close();
    }

    public void testWriteByte() throws IOException {
        byte testValue = 42;
        output.writeByte(testValue);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        TinyIntVector vector = (TinyIntVector) root.getVector(0);
        assertEquals(1, vector.getValueCount());
        assertEquals(testValue, vector.get(0));
        assertEquals("root.0", vector.getField().getName());
    }

    public void testWriteInt() throws IOException {
        int testValue = 12345;
        output.writeInt(testValue);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        IntVector vector = (IntVector) root.getVector(0);
        assertEquals(1, vector.getValueCount());
        assertEquals(testValue, vector.get(0));
        assertEquals("root.0", vector.getField().getName());
    }

    public void testWriteLong() throws IOException {
        long testValue = 123456789L;
        output.writeLong(testValue);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        BigIntVector vector = (BigIntVector) root.getVector(0);
        assertEquals(1, vector.getValueCount());
        assertEquals(testValue, vector.get(0));
        assertEquals("root.0", vector.getField().getName());
    }

    public void testWriteBoolean() throws IOException {
        output.writeBoolean(true);
        output.writeBoolean(false);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(2, root.getFieldVectors().size());

        BitVector vector1 = (BitVector) root.getVector(0);
        assertEquals(1, vector1.getValueCount());
        assertEquals(1, vector1.get(0));
        assertEquals("root.0", vector1.getField().getName());

        BitVector vector2 = (BitVector) root.getVector(1);
        assertEquals(1, vector2.getValueCount());
        assertEquals(0, vector2.get(0));
        assertEquals("root.1", vector2.getField().getName());
    }

    public void testWriteFloat() throws IOException {
        float testValue = 3.14f;
        output.writeFloat(testValue);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        Float4Vector vector = (Float4Vector) root.getVector(0);
        assertEquals(1, vector.getValueCount());
        assertEquals(testValue, vector.get(0), 0.001f);
        assertEquals("root.0", vector.getField().getName());
    }

    public void testWriteDouble() throws IOException {
        double testValue = 3.141592653589793;
        output.writeDouble(testValue);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        Float8Vector vector = (Float8Vector) root.getVector(0);
        assertEquals(1, vector.getValueCount());
        assertEquals(testValue, vector.get(0), 0.000001);
        assertEquals("root.0", vector.getField().getName());
    }

    public void testWriteString() throws IOException {
        String testValue = "Hello, Arrow!";
        output.writeString(testValue);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        VarCharVector vector = (VarCharVector) root.getVector(0);
        assertEquals(1, vector.getValueCount());
        assertEquals(testValue, new String(vector.get(0), StandardCharsets.UTF_8));
        assertEquals("root.0", vector.getField().getName());
    }

    public void testWriteText() throws IOException {
        Text testValue = new Text("Hello, Text!");
        output.writeText(testValue);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        VarCharVector vector = (VarCharVector) root.getVector(0);
        assertEquals(1, vector.getValueCount());
        assertEquals(testValue.toString(), new String(vector.get(0), StandardCharsets.UTF_8));
        assertEquals("root.0", vector.getField().getName());
    }

    public void testWriteBytes() throws IOException {
        byte[] testValue = "test bytes".getBytes(StandardCharsets.UTF_8);
        output.writeBytes(testValue, 0, testValue.length);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        VarBinaryVector vector = (VarBinaryVector) root.getVector(0);
        assertEquals(1, vector.getValueCount());
        assertArrayEquals(testValue, vector.get(0));
        assertEquals("root.0", vector.getField().getName());
    }

    public void testWriteBytesWithOffset() throws IOException {
        byte[] sourceBytes = "Hello World Test".getBytes(StandardCharsets.UTF_8);
        int offset = 6;
        int length = 5;
        output.writeBytes(sourceBytes, offset, length);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        VarBinaryVector vector = (VarBinaryVector) root.getVector(0);
        assertEquals(1, vector.getValueCount());
        byte[] expected = "World".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expected, vector.get(0));
    }

    public void testWriteEmptyBytes() throws IOException {
        output.writeBytes(new byte[0], 0, 0);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        VarBinaryVector vector = (VarBinaryVector) root.getVector(0);
        assertEquals(1, vector.getValueCount());
        assertTrue(vector.isNull(0));
    }

    public void testMultiplePrimitiveValues() throws IOException {
        output.writeInt(100);
        output.writeString("test");
        output.writeBoolean(true);
        output.writeLong(999L);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(4, root.getFieldVectors().size());

        // Verify column names follow ordinal pattern
        assertEquals("root.0", root.getVector(0).getField().getName());
        assertEquals("root.1", root.getVector(1).getField().getName());
        assertEquals("root.2", root.getVector(2).getField().getName());
        assertEquals("root.3", root.getVector(3).getField().getName());

        // Verify values
        assertEquals(100, ((IntVector) root.getVector(0)).get(0));
        assertEquals("test", new String(((VarCharVector) root.getVector(1)).get(0), StandardCharsets.UTF_8));
        assertEquals(1, ((BitVector) root.getVector(2)).get(0));
        assertEquals(999L, ((BigIntVector) root.getVector(3)).get(0));
    }

    public void testWriteNamedWriteable() throws IOException {
        TestNamedWriteable testWriteable = new TestNamedWriteable("test-name", 42, "test-value");
        output.writeNamedWriteable(testWriteable);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        StructVector structVector = (StructVector) root.getVector(0);
        assertEquals("root.0", structVector.getField().getName());
        assertEquals("test-writeable", structVector.getField().getMetadata().get("name"));

        // The struct should have nested fields for the writeable's data
        assertTrue(structVector.getValueCount() > 0);
    }

    public void testWriteList() throws IOException {
        List<TestWriteable> testList = Arrays.asList(
            new TestWriteable(1, "first"),
            new TestWriteable(2, "second"),
            new TestWriteable(3, "third")
        );

        output.writeList(testList);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        StructVector structVector = (StructVector) root.getVector(0);
        assertEquals("root.0", structVector.getField().getName());

        // List should have multiple rows for each element
        assertTrue(structVector.getValueCount() > 0);
    }

    public void testWriteEmptyList() throws IOException {
        List<TestWriteable> emptyList = Collections.emptyList();
        output.writeList(emptyList);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        StructVector structVector = (StructVector) root.getVector(0);
        assertEquals("root.0", structVector.getField().getName());
    }

    public void testWriteNullMap() throws IOException {
        output.writeMap(null);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        StructVector structVector = (StructVector) root.getVector(0);
        assertEquals("root.0", structVector.getField().getName());
        assertEquals(1, structVector.getValueCount());
        assertTrue(structVector.isNull(0));
    }

    public void testWriteEmptyMap() throws IOException {
        Map<String, Object> emptyMap = new HashMap<>();
        output.writeMap(emptyMap);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        StructVector structVector = (StructVector) root.getVector(0);
        assertEquals("root.0", structVector.getField().getName());
        assertEquals(1, structVector.getValueCount());
        assertTrue(structVector.isNull(0));
    }

    public void testWriteNonEmptyMapThrowsException() throws IOException {
        Map<String, Object> nonEmptyMap = new HashMap<>();
        nonEmptyMap.put("key", "value");

        expectThrows(UnsupportedOperationException.class, () -> output.writeMap(nonEmptyMap));
    }

    public void testPathManagerNavigation() throws IOException {
        ArrowStreamOutput.PathManager pathManager = output.getPathManager();

        assertEquals("root", pathManager.getCurrentPath());
        assertEquals(0, pathManager.getCurrentRow());

        // Add a child and move to it
        int childOrdinal = pathManager.addChild();
        assertEquals(0, childOrdinal);

        pathManager.moveToChild(true);
        assertEquals("root.0", pathManager.getCurrentPath());
        assertEquals(0, pathManager.getCurrentRow());

        // Move back to parent
        pathManager.moveToParent();
        assertEquals("root", pathManager.getCurrentPath());
    }

    public void testPathManagerNextRow() throws IOException {
        ArrowStreamOutput.PathManager pathManager = output.getPathManager();

        assertEquals(0, pathManager.getCurrentRow());
        pathManager.nextRow();
        assertEquals(1, pathManager.getCurrentRow());
        pathManager.nextRow();
        assertEquals(2, pathManager.getCurrentRow());
    }

    public void testPathManagerMultipleChildren() throws IOException {
        ArrowStreamOutput.PathManager pathManager = output.getPathManager();

        int child1 = pathManager.addChild();
        int child2 = pathManager.addChild();
        int child3 = pathManager.addChild();

        assertEquals(0, child1);
        assertEquals(1, child2);
        assertEquals(2, child3);
    }

    public void testReset() throws IOException {
        output.writeInt(42);
        output.writeString("test");

        VectorSchemaRoot rootBefore = output.getUnifiedRoot();
        assertEquals(2, rootBefore.getFieldVectors().size());

        output.reset();

        VectorSchemaRoot rootAfter = output.getUnifiedRoot();
        assertEquals(0, rootAfter.getFieldVectors().size());

        ArrowStreamOutput.PathManager pathManager = output.getPathManager();
        assertEquals("root", pathManager.getCurrentPath());
        assertEquals(0, pathManager.getCurrentRow());
    }

    public void testColumnOrderingValidation() throws IOException {
        // This test verifies that columns must be added in order
        output.writeInt(1);
        output.writeString("test");

        // The implementation should handle proper ordering internally
        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(2, root.getFieldVectors().size());
        assertEquals("root.0", root.getVector(0).getField().getName());
        assertEquals("root.1", root.getVector(1).getField().getName());
    }

    public void testVIntAndVLong() throws IOException {
        output.writeVInt(12345);
        output.writeVLong(123456789L);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(2, root.getFieldVectors().size());

        // VInt and VLong should be stored as regular Int and Long
        IntVector intVector = (IntVector) root.getVector(0);
        assertEquals(12345, intVector.get(0));

        BigIntVector longVector = (BigIntVector) root.getVector(1);
        assertEquals(123456789L, longVector.get(0));
    }

    public void testZLong() throws IOException {
        long testValue = 987654321L;
        output.writeZLong(testValue);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(1, root.getFieldVectors().size());

        BigIntVector vector = (BigIntVector) root.getVector(0);
        assertEquals(testValue, vector.get(0));
    }

    public void testGetRoots() throws IOException {
        output.writeInt(42);

        Map<String, VectorSchemaRoot> roots = output.getRoots();
        assertNotNull(roots);
        assertTrue(roots.containsKey("root"));

        VectorSchemaRoot rootVector = roots.get("root");
        assertEquals(1, rootVector.getFieldVectors().size());
    }

    public void testFlushThrowsUnsupportedException() {
        expectThrows(UnsupportedOperationException.class, () -> output.flush());
    }

    // Helper classes for testing
    private static class TestWriteable implements Writeable {
        private final int intValue;
        private final String stringValue;

        TestWriteable(int intValue, String stringValue) {
            this.intValue = intValue;
            this.stringValue = stringValue;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(intValue);
            out.writeString(stringValue);
        }
    }

    private static class TestNamedWriteable implements NamedWriteable {
        private final String name;
        private final int intValue;
        private final String stringValue;

        TestNamedWriteable(String name, int intValue, String stringValue) {
            this.name = name;
            this.intValue = intValue;
            this.stringValue = stringValue;
        }

        @Override
        public String getWriteableName() {
            return "test-writeable";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeInt(intValue);
            out.writeString(stringValue);
        }
    }
}
