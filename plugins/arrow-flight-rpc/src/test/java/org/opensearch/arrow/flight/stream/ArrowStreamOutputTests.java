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

        assertEquals("root.0", root.getVector(0).getField().getName());
        assertEquals("root.1", root.getVector(1).getField().getName());
        assertEquals("root.2", root.getVector(2).getField().getName());
        assertEquals("root.3", root.getVector(3).getField().getName());

        assertEquals(100, ((IntVector) root.getVector(0)).get(0));
        assertEquals("test", new String(((VarCharVector) root.getVector(1)).get(0), StandardCharsets.UTF_8));
        assertEquals(1, ((BitVector) root.getVector(2)).get(0));
        assertEquals(999L, ((BigIntVector) root.getVector(3)).get(0));
    }

    public void testWriteNamedWriteable() throws IOException {
        TestNamedWriteable testWriteable = new TestNamedWriteable("test-name", 42, "test-value");
        output.writeNamedWriteable(testWriteable);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(4, root.getFieldVectors().size());

        StructVector structVector = (StructVector) root.getVector(0);
        assertEquals("root.0", structVector.getField().getName());
        assertEquals("test-writeable", structVector.getField().getMetadata().get("name"));
    }

    public void testWriteList() throws IOException {
        List<TestWriteable> testList = Arrays.asList(
            new TestWriteable(1, "first"),
            new TestWriteable(2, "second"),
            new TestWriteable(3, "third")
        );

        output.writeList(testList);

        VectorSchemaRoot root = output.getUnifiedRoot();
        // 1 int vector, 1 string vector, 1 boolean vector to determine if values need to be written
        assertEquals(3, root.getFieldVectors().size());
    }

    public void testWriteEmptyList() throws IOException {
        List<TestWriteable> emptyList = Collections.emptyList();
        output.writeList(emptyList);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(0, root.getFieldVectors().size());
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

        int childOrdinal = pathManager.addChild();
        assertEquals(0, childOrdinal);

        pathManager.moveToChild(true);
        assertEquals("root.0", pathManager.getCurrentPath());
        assertEquals(0, pathManager.getCurrentRow());

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
        output.writeInt(1);
        output.writeString("test");

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

    public void testWriteMinMaxValues() throws IOException {
        output.writeByte(Byte.MIN_VALUE);
        output.writeByte(Byte.MAX_VALUE);
        output.writeInt(Integer.MIN_VALUE);
        output.writeInt(Integer.MAX_VALUE);
        output.writeLong(Long.MIN_VALUE);
        output.writeLong(Long.MAX_VALUE);
        output.writeFloat(Float.MIN_VALUE);
        output.writeFloat(Float.MAX_VALUE);
        output.writeFloat(Float.NEGATIVE_INFINITY);
        output.writeFloat(Float.POSITIVE_INFINITY);
        output.writeFloat(Float.NaN);
        output.writeDouble(Double.MIN_VALUE);
        output.writeDouble(Double.MAX_VALUE);
        output.writeDouble(Double.NEGATIVE_INFINITY);
        output.writeDouble(Double.POSITIVE_INFINITY);
        output.writeDouble(Double.NaN);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(16, root.getFieldVectors().size());

        TinyIntVector byteVector1 = (TinyIntVector) root.getVector(0);
        TinyIntVector byteVector2 = (TinyIntVector) root.getVector(1);
        assertEquals(Byte.MIN_VALUE, byteVector1.get(0));
        assertEquals(Byte.MAX_VALUE, byteVector2.get(0));

        IntVector intVector1 = (IntVector) root.getVector(2);
        IntVector intVector2 = (IntVector) root.getVector(3);
        assertEquals(Integer.MIN_VALUE, intVector1.get(0));
        assertEquals(Integer.MAX_VALUE, intVector2.get(0));

        BigIntVector longVector1 = (BigIntVector) root.getVector(4);
        BigIntVector longVector2 = (BigIntVector) root.getVector(5);
        assertEquals(Long.MIN_VALUE, longVector1.get(0));
        assertEquals(Long.MAX_VALUE, longVector2.get(0));

        Float4Vector floatVector1 = (Float4Vector) root.getVector(6);
        Float4Vector floatVector2 = (Float4Vector) root.getVector(7);
        Float4Vector floatVector3 = (Float4Vector) root.getVector(8);
        Float4Vector floatVector4 = (Float4Vector) root.getVector(9);
        Float4Vector floatVector5 = (Float4Vector) root.getVector(10);
        assertEquals(Float.MIN_VALUE, floatVector1.get(0), 0.0f);
        assertEquals(Float.MAX_VALUE, floatVector2.get(0), 0.0f);
        assertEquals(Float.NEGATIVE_INFINITY, floatVector3.get(0), 0.0f);
        assertEquals(Float.POSITIVE_INFINITY, floatVector4.get(0), 0.0f);
        assertTrue(Float.isNaN(floatVector5.get(0)));

        Float8Vector doubleVector1 = (Float8Vector) root.getVector(11);
        Float8Vector doubleVector2 = (Float8Vector) root.getVector(12);
        Float8Vector doubleVector3 = (Float8Vector) root.getVector(13);
        Float8Vector doubleVector4 = (Float8Vector) root.getVector(14);
        Float8Vector doubleVector5 = (Float8Vector) root.getVector(15);
        assertEquals(Double.MIN_VALUE, doubleVector1.get(0), 0.0);
        assertEquals(Double.MAX_VALUE, doubleVector2.get(0), 0.0);
        assertEquals(Double.NEGATIVE_INFINITY, doubleVector3.get(0), 0.0);
        assertEquals(Double.POSITIVE_INFINITY, doubleVector4.get(0), 0.0);
        assertTrue(Double.isNaN(doubleVector5.get(0)));
    }

    public void testWriteEmptyAndNullStrings() throws IOException {
        output.writeString("");
        // TODO: how does regular stream output handle null strings?
        // output.writeString(null);
        output.writeText(new Text(""));
        // output.writeText(null);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(2, root.getFieldVectors().size());

        VarCharVector stringVector1 = (VarCharVector) root.getVector(0);
        // VarCharVector stringVector2 = (VarCharVector) root.getVector(1);
        VarCharVector textVector1 = (VarCharVector) root.getVector(1);
        // VarCharVector textVector2 = (VarCharVector) root.getVector(3);

        assertEquals("", new String(stringVector1.get(0), StandardCharsets.UTF_8));
        // assertTrue(stringVector2.isNull(0));
        assertEquals("", new String(textVector1.get(0), StandardCharsets.UTF_8));
        // assertTrue(textVector2.isNull(0));
    }

    public void testWriteLargeStrings() throws IOException {
        StringBuilder largeString = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largeString.append("This is a test string with some content ");
        }
        String testString = largeString.toString();

        output.writeString(testString);
        output.writeText(new Text(testString));

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(2, root.getFieldVectors().size());

        VarCharVector stringVector = (VarCharVector) root.getVector(0);
        VarCharVector textVector = (VarCharVector) root.getVector(1);

        assertEquals(testString, new String(stringVector.get(0), StandardCharsets.UTF_8));
        assertEquals(testString, new String(textVector.get(0), StandardCharsets.UTF_8));
    }

    public void testWriteUnicodeStrings() throws IOException {
        String unicodeString = "Hello 世界 🌍";
        output.writeString(unicodeString);
        output.writeText(new Text(unicodeString));

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(2, root.getFieldVectors().size());

        VarCharVector stringVector = (VarCharVector) root.getVector(0);
        VarCharVector textVector = (VarCharVector) root.getVector(1);

        assertEquals(unicodeString, new String(stringVector.get(0), StandardCharsets.UTF_8));
        assertEquals(unicodeString, new String(textVector.get(0), StandardCharsets.UTF_8));
    }

    public void testWriteLargeByteArrays() throws IOException {
        byte[] largeArray = new byte[10000];
        for (int i = 0; i < largeArray.length; i++) {
            largeArray[i] = (byte) (i % 256);
        }

        output.writeBytes(largeArray, 0, largeArray.length);

        output.writeBytes(largeArray, 5000, 2500);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(2, root.getFieldVectors().size());

        VarBinaryVector binaryVector1 = (VarBinaryVector) root.getVector(0);
        VarBinaryVector binaryVector2 = (VarBinaryVector) root.getVector(1);

        assertArrayEquals(largeArray, binaryVector1.get(0));

        byte[] expectedPartial = new byte[2500];
        System.arraycopy(largeArray, 5000, expectedPartial, 0, 2500);
        assertArrayEquals(expectedPartial, binaryVector2.get(0));
    }

    public void testWriteBytesEdgeCases() throws IOException {
        byte[] testArray = "Hello World".getBytes(StandardCharsets.UTF_8);

        output.writeBytes(testArray, 0, testArray.length);

        output.writeBytes(testArray, testArray.length, 0);

        output.writeBytes(testArray, 0, 1);

        output.writeBytes(testArray, testArray.length - 1, 1);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(4, root.getFieldVectors().size());

        VarBinaryVector vector1 = (VarBinaryVector) root.getVector(0);
        VarBinaryVector vector2 = (VarBinaryVector) root.getVector(1);
        VarBinaryVector vector3 = (VarBinaryVector) root.getVector(2);
        VarBinaryVector vector4 = (VarBinaryVector) root.getVector(3);

        assertArrayEquals(testArray, vector1.get(0));
        assertTrue(vector2.isNull(0)); // Zero length should be null
        assertArrayEquals(new byte[] { testArray[0] }, vector3.get(0));
        assertArrayEquals(new byte[] { testArray[testArray.length - 1] }, vector4.get(0));
    }

    public void testWriteBytesWithInvalidParameters() {
        byte[] testArray = "test".getBytes(StandardCharsets.UTF_8);
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> { output.writeBytes(testArray, -1, 1); });
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> { output.writeBytes(testArray, testArray.length + 1, 1); });
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> { output.writeBytes(testArray, 1, testArray.length); });
    }

    public void testMultipleResetsAndReuse() throws IOException {
        for (int cycle = 0; cycle < 3; cycle++) {
            output.writeInt(cycle);
            output.writeString("cycle-" + cycle);

            VectorSchemaRoot root = output.getUnifiedRoot();
            assertEquals(2, root.getFieldVectors().size());

            IntVector intVector = (IntVector) root.getVector(0);
            VarCharVector stringVector = (VarCharVector) root.getVector(1);

            assertEquals(cycle, intVector.get(0));
            assertEquals("cycle-" + cycle, new String(stringVector.get(0), StandardCharsets.UTF_8));

            output.reset();

            // After reset, should have no vectors
            VectorSchemaRoot resetRoot = output.getUnifiedRoot();
            assertEquals(0, resetRoot.getFieldVectors().size());
        }
    }

    public void testLargeNumberOfColumns() throws IOException {
        int numColumns = 100;
        for (int i = 0; i < numColumns; i++) {
            output.writeInt(i);
        }

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(numColumns, root.getFieldVectors().size());

        for (int i = 0; i < numColumns; i++) {
            IntVector vector = (IntVector) root.getVector(i);
            assertEquals("root." + i, vector.getField().getName());
            assertEquals(i, vector.get(0));
        }
    }

    public void testLargeList() throws IOException {
        List<TestWriteable> largeList = new java.util.ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            largeList.add(new TestWriteable(i, "item-" + i));
        }

        output.writeList(largeList);

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(3, root.getFieldVectors().size());
    }

    public void testMixedDataTypes() throws IOException {
        output.writeInt(42);// 1
        output.writeString("test");// 2
        output.writeBoolean(true);// 3
        output.writeFloat(3.14f);// 4
        output.writeDouble(2.718281828);// 5
        output.writeLong(123456789L);// 6
        output.writeByte((byte) 255);// 7
        output.writeBytes("binary data".getBytes(StandardCharsets.UTF_8), 0, "binary data".length());// 8
        output.writeText(new Text("text data"));// 9

        List<TestWriteable> testList = Arrays.asList(new TestWriteable(1, "list-item-1"), new TestWriteable(2, "list-item-2"));
        output.writeList(testList);// 10, 11, 12(boolean)

        output.writeNamedWriteable(new TestNamedWriteable("named", 999, "named-value"));// 13(struct), 14, 15, 16
        output.writeMap(null);// 17
        output.writeMap(Collections.emptyMap());// 18

        VectorSchemaRoot root = output.getUnifiedRoot();
        assertEquals(18, root.getFieldVectors().size());
    }

    public void testPathManagerComplexNavigation() throws IOException {
        ArrowStreamOutput.PathManager pathManager = output.getPathManager();

        assertEquals("root", pathManager.getCurrentPath());
        assertEquals(0, pathManager.getCurrentRow());

        int child1 = pathManager.addChild();
        int child2 = pathManager.addChild();
        int child3 = pathManager.addChild();

        assertEquals(0, child1);
        assertEquals(1, child2);
        assertEquals(2, child3);

        pathManager.moveToChild(true);
        assertEquals("root.2", pathManager.getCurrentPath());
        assertEquals(0, pathManager.getCurrentRow());

        int grandchild1 = pathManager.addChild();
        int grandchild2 = pathManager.addChild();

        assertEquals(0, grandchild1);
        assertEquals(1, grandchild2);

        pathManager.moveToChild(false);
        assertEquals("root.2.1", pathManager.getCurrentPath());
        assertEquals(0, pathManager.getCurrentRow());

        pathManager.nextRow();
        pathManager.nextRow();
        assertEquals(2, pathManager.getCurrentRow());

        pathManager.moveToParent();
        assertEquals("root.2", pathManager.getCurrentPath());
        assertEquals(0, pathManager.getCurrentRow()); // Should still be 0 since we didn't propagate

        pathManager.moveToParent();
        assertEquals("root", pathManager.getCurrentPath());
        assertEquals(0, pathManager.getCurrentRow());
    }

    public void testPathManagerRowPropagation() throws IOException {
        ArrowStreamOutput.PathManager pathManager = output.getPathManager();

        // Increment row at root level
        pathManager.nextRow();
        pathManager.nextRow();
        assertEquals(2, pathManager.getCurrentRow());

        // Add child and move with row propagation
        int child = pathManager.addChild();
        pathManager.moveToChild(true);
        assertEquals("root.0", pathManager.getCurrentPath());
        assertEquals(2, pathManager.getCurrentRow()); // Should inherit parent's row

        // Move back and try without propagation
        pathManager.moveToParent();
        int child2 = pathManager.addChild();
        pathManager.moveToChild(false);
        assertEquals("root.1", pathManager.getCurrentPath());
        assertEquals(0, pathManager.getCurrentRow()); // Should start at 0
    }

    public void testPathManagerReset() throws IOException {
        ArrowStreamOutput.PathManager pathManager = output.getPathManager();

        pathManager.addChild();
        pathManager.addChild();
        pathManager.moveToChild(true);
        pathManager.nextRow();
        pathManager.addChild();
        pathManager.nextRow();

        // Verify complex state
        assertEquals("root.1", pathManager.getCurrentPath());
        assertEquals(2, pathManager.getCurrentRow());

        // Reset
        pathManager.reset();

        // Verify reset state
        assertEquals("root", pathManager.getCurrentPath());
        assertEquals(0, pathManager.getCurrentRow());
        assertTrue(pathManager.getRow().isEmpty());
        assertTrue(pathManager.getColumn().isEmpty());
    }

    public void testNestedWriteableStructures() throws IOException {
        TestNestedWriteable nested = new TestNestedWriteable(
            "outer",
            new TestWriteable(42, "inner"),
            Arrays.asList(new TestWriteable(1, "first"), new TestWriteable(2, "second"))
        );

        output.writeNamedWriteable(nested);

        VectorSchemaRoot root = output.getUnifiedRoot();
        // 1 struct, 2 for testwriteable, 3 for list
        assertEquals(7, root.getFieldVectors().size());

        // respect serialization order
        StructVector structVector = (StructVector) root.getVector(3);
        assertEquals("root.0", structVector.getField().getName());
        assertEquals("test-nested-writeable", structVector.getField().getMetadata().get("name"));
    }

    // TODO: need to evaluate if this is the right with respect to how column mismatch works, should we fix this?
    // public void testComplexListStructures() throws IOException {
    // List<TestComplexWriteable> complexList = Arrays.asList(
    // new TestComplexWriteable("item1", Arrays.asList("a", "b", "c"), Map.of("key1", 100, "key2", 200)),
    // new TestComplexWriteable("item2", Arrays.asList("x", "y", "z"), Map.of("key3", 300)),
    // new TestComplexWriteable("item2", Arrays.asList("x", "y"), Map.of("key3", 300))
    // );
    //
    // output.writeList(complexList);
    //
    // VectorSchemaRoot root = output.getUnifiedRoot();
    // assertEquals(1, root.getFieldVectors().size());
    //
    // StructVector structVector = (StructVector) root.getVector(0);
    // assertEquals("root.0", structVector.getField().getName());
    // assertTrue(structVector.getValueCount() > 0);
    // }

    public void testListOfLists() throws IOException {
        List<List<TestWriteable>> listOfLists = Arrays.asList(
            Arrays.asList(new TestWriteable(1, "first-inner-1"), new TestWriteable(2, "first-inner-2")),
            Arrays.asList(
                new TestWriteable(3, "second-inner-1"),
                new TestWriteable(4, "second-inner-2"),
                new TestWriteable(5, "second-inner-3")
            ),
            Collections.emptyList()
        );

        output.writeList(listOfLists.stream().map(ListWriteable::new).collect(java.util.stream.Collectors.toList()));

        VectorSchemaRoot root = output.getUnifiedRoot();
        // 1 outer boolean vector, 2 inner vectors and 1 inner boolean vector
        assertEquals(4, root.getFieldVectors().size());
    }

    // Helper classes for testing
    private record TestWriteable(int intValue, String stringValue) implements Writeable {

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(intValue);
            out.writeString(stringValue);
        }
    }

    private record TestNamedWriteable(String name, int intValue, String stringValue) implements NamedWriteable {

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

    private record TestNestedWriteable(String name, TestWriteable inner, List<TestWriteable> list) implements NamedWriteable {

        @Override
        public String getWriteableName() {
            return "test-nested-writeable";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            inner.writeTo(out);
            out.writeList(list);
        }
    }

    private record TestComplexWriteable(String name, List<String> tags, Map<String, Integer> metadata) implements Writeable {

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeStringCollection(tags);
            // For metadata, we'll write it as a simple count and key-value pairs
            out.writeVInt(metadata.size());
            for (Map.Entry<String, Integer> entry : metadata.entrySet()) {
                out.writeString(entry.getKey());
                out.writeInt(entry.getValue());
            }
        }
    }

    private static class ListWriteable implements Writeable {
        private final List<TestWriteable> innerList;

        ListWriteable(List<TestWriteable> innerList) {
            this.innerList = innerList;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(innerList);
        }
    }
}
