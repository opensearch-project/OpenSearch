/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.stream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.text.Text;
import org.opensearch.test.OpenSearchTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArrowStreamInputTests extends OpenSearchTestCase {
    private RootAllocator allocator;
    private NamedWriteableRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
        registry = new NamedWriteableRegistry(
            Arrays.asList(new NamedWriteableRegistry.Entry(TestNamedWriteable.class, "test-writeable", TestNamedWriteable::new))
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        allocator.close();
    }

    public void testReadByte() throws IOException {
        byte testValue = 42;

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeByte(testValue);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                byte result = input.readByte();
                assertEquals(testValue, result);
            }
        }
    }

    public void testReadBytesWithInvalidParameters() throws IOException {
        byte[] testArray = "test".getBytes(StandardCharsets.UTF_8);

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeBytes(testArray, 0, testArray.length);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                byte[] result = new byte[testArray.length];

                expectThrows(IndexOutOfBoundsException.class, () -> { input.readBytes(result, -1, 1); });

                expectThrows(IndexOutOfBoundsException.class, () -> { input.readBytes(result, result.length + 1, 1); });

                expectThrows(IndexOutOfBoundsException.class, () -> { input.readBytes(result, 0, -1); });

                expectThrows(IndexOutOfBoundsException.class, () -> { input.readBytes(result, 1, result.length); });
            }
        }
    }

    public void testReadFromNullByteArray() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeBytes(new byte[0], 0, 0);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                byte[] result = new byte[0];

                input.readBytes(result, 0, 0);
                assertEquals(0, result.length);
            }
        }
    }

    public void testMultipleResetsAndReuse() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeInt(42);
            output.writeString("test");
            output.writeBoolean(true);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {

                for (int cycle = 0; cycle < 3; cycle++) {
                    assertEquals(42, input.readInt());
                    assertEquals("test", input.readString());
                    assertTrue(input.readBoolean());

                    input.reset();
                }
            }
        }
    }

    public void testReadAfterEOF() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeInt(42);
            output.writeString("test");

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                assertEquals(42, input.readInt());
                assertEquals("test", input.readString());

                expectThrows(EOFException.class, input::readInt);
                expectThrows(EOFException.class, input::readString);
                expectThrows(EOFException.class, input::readBoolean);
                expectThrows(EOFException.class, input::readLong);
                expectThrows(EOFException.class, input::readFloat);
                expectThrows(EOFException.class, input::readDouble);
                expectThrows(EOFException.class, input::readByte);
            }
        }
    }

    public void testInvalidNamedWriteableRegistry() throws IOException {
        TestNamedWriteable original = new TestNamedWriteable("test-name", 42, "test-value");

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeNamedWriteable(original);

            VectorSchemaRoot root = output.getUnifiedRoot();

            NamedWriteableRegistry emptyRegistry = new NamedWriteableRegistry(Collections.emptyList());

            try (ArrowStreamInput input = new ArrowStreamInput(root, emptyRegistry)) {
                expectThrows(IOException.class, () -> { input.readNamedWriteable(TestNamedWriteable.class); });
            }
        }
    }

    public void testReadLargeNumberOfColumns() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {

            int numColumns = 100;
            for (int i = 0; i < numColumns; i++) {
                output.writeInt(i);
            }

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                for (int i = 0; i < numColumns; i++) {
                    assertEquals(i, input.readInt());
                }
            }
        }
    }

    public void testReadLargeList() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {

            List<TestWriteable> largeList = new java.util.ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                largeList.add(new TestWriteable(i, "item-" + i));
            }

            output.writeList(largeList);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                List<TestWriteable> result = input.readList(TestWriteable::new);
                assertEquals(largeList.size(), result.size());

                for (int i = 0; i < largeList.size(); i++) {
                    assertEquals(largeList.get(i).intValue, result.get(i).intValue);
                    assertEquals(largeList.get(i).stringValue, result.get(i).stringValue);
                }
            }
        }
    }

    public void testReadMixedDataTypes() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {

            output.writeInt(42);
            output.writeString("test");
            output.writeBoolean(true);
            output.writeFloat(3.14f);
            output.writeDouble(2.718281828);
            output.writeLong(123456789L);
            output.writeByte((byte) 255);
            output.writeBytes("binary data".getBytes(StandardCharsets.UTF_8), 0, "binary data".length());
            output.writeText(new Text("text data"));

            List<TestWriteable> testList = Arrays.asList(new TestWriteable(1, "list-item-1"), new TestWriteable(2, "list-item-2"));
            output.writeList(testList);

            output.writeNamedWriteable(new TestNamedWriteable("named", 999, "named-value"));
            output.writeMap(null);
            output.writeMap(Collections.emptyMap());

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                assertEquals(42, input.readInt());
                assertEquals("test", input.readString());
                assertTrue(input.readBoolean());
                assertEquals(3.14f, input.readFloat(), 0.001f);
                assertEquals(2.718281828, input.readDouble(), 0.000001);
                assertEquals(123456789L, input.readLong());
                assertEquals((byte) 255, input.readByte());

                byte[] binaryResult = new byte["binary data".length()];
                input.readBytes(binaryResult, 0, binaryResult.length);
                assertArrayEquals("binary data".getBytes(StandardCharsets.UTF_8), binaryResult);

                assertEquals("text data", input.readText().toString());

                List<TestWriteable> listResult = input.readList(TestWriteable::new);
                assertEquals(2, listResult.size());
                assertEquals(1, listResult.get(0).intValue);
                assertEquals("list-item-1", listResult.get(0).stringValue);
                assertEquals(2, listResult.get(1).intValue);
                assertEquals("list-item-2", listResult.get(1).stringValue);

                TestNamedWriteable namedResult = input.readNamedWriteable(TestNamedWriteable.class);
                assertEquals("named", namedResult.name);
                assertEquals(999, namedResult.intValue);
                assertEquals("named-value", namedResult.stringValue);

                Map<String, Object> mapResult1 = input.readMap();
                assertTrue(mapResult1.isEmpty());

                Map<String, Object> mapResult2 = input.readMap();
                assertTrue(mapResult2.isEmpty());
            }
        }
    }

    public void testReadInt() throws IOException {
        int testValue = 12345;

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeInt(testValue);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                int result = input.readInt();
                assertEquals(testValue, result);
            }
        }
    }

    public void testReadLong() throws IOException {
        long testValue = 123456789L;

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeLong(testValue);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                long result = input.readLong();
                assertEquals(testValue, result);
            }
        }
    }

    public void testReadBoolean() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeBoolean(true);
            output.writeBoolean(false);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                assertTrue(input.readBoolean());
                assertFalse(input.readBoolean());
            }
        }
    }

    public void testReadFloat() throws IOException {
        float testValue = 3.14f;

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeFloat(testValue);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                float result = input.readFloat();
                assertEquals(testValue, result, 0.001f);
            }
        }
    }

    public void testReadDouble() throws IOException {
        double testValue = 3.141592653589793;

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeDouble(testValue);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                double result = input.readDouble();
                assertEquals(testValue, result, 0.000001);
            }
        }
    }

    public void testReadString() throws IOException {
        String testValue = "Hello, Arrow!";

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeString(testValue);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                String result = input.readString();
                assertEquals(testValue, result);
            }
        }
    }

    public void testReadText() throws IOException {
        Text testValue = new Text("Hello, Text!");

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeText(testValue);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                Text result = input.readText();
                assertEquals(testValue.toString(), result.toString());
            }
        }
    }

    public void testReadBytes() throws IOException {
        byte[] testValue = "test bytes".getBytes(StandardCharsets.UTF_8);

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeBytes(testValue, 0, testValue.length);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                byte[] result = new byte[testValue.length];
                input.readBytes(result, 0, testValue.length);
                assertArrayEquals(testValue, result);
            }
        }
    }

    public void testReadBytesWithWrongLength() throws IOException {
        byte[] testValue = "test".getBytes(StandardCharsets.UTF_8);

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeBytes(testValue, 0, testValue.length);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                byte[] result = new byte[10];
                expectThrows(IOException.class, () -> input.readBytes(result, 0, 10));
            }
        }
    }

    public void testReadMultiplePrimitiveValues() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeInt(100);
            output.writeString("test");
            output.writeBoolean(true);
            output.writeLong(999L);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                assertEquals(100, input.readInt());
                assertEquals("test", input.readString());
                assertTrue(input.readBoolean());
                assertEquals(999L, input.readLong());
            }
        }
    }

    public void testReadNamedWriteable() throws IOException {
        TestNamedWriteable original = new TestNamedWriteable("test-name", 42, "test-value");

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeNamedWriteable(original);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                TestNamedWriteable result = input.readNamedWriteable(TestNamedWriteable.class);
                assertEquals(original.name, result.name);
                assertEquals(original.intValue, result.intValue);
                assertEquals(original.stringValue, result.stringValue);
            }
        }
    }

    public void testReadList() throws IOException {
        List<TestWriteable> originalList = Arrays.asList(
            new TestWriteable(1, "first"),
            new TestWriteable(2, "second"),
            new TestWriteable(3, "third")
        );

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeList(originalList);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                List<TestWriteable> result = input.readList(TestWriteable::new);
                assertEquals(originalList.size(), result.size());

                for (int i = 0; i < originalList.size(); i++) {
                    assertEquals(originalList.get(i).intValue, result.get(i).intValue);
                    assertEquals(originalList.get(i).stringValue, result.get(i).stringValue);
                }
            }
        }
    }

    public void testReadEmptyList() throws IOException {
        List<TestWriteable> emptyList = Collections.emptyList();

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeList(emptyList);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                List<TestWriteable> result = input.readList(TestWriteable::new);
                assertTrue(result.isEmpty());
            }
        }
    }

    public void testReadNullMap() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeMap(null);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                Map<String, Object> result = input.readMap();
                assertTrue(result.isEmpty());
            }
        }
    }

    public void testReadEmptyMap() throws IOException {
        Map<String, Object> emptyMap = new HashMap<>();

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeMap(emptyMap);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                Map<String, Object> result = input.readMap();
                assertTrue(result.isEmpty());
            }
        }
    }

    public void testReadVIntAndVLong() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeVInt(12345);
            output.writeVLong(123456789L);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                assertEquals(12345, input.readVInt());
                assertEquals(123456789L, input.readVLong());
            }
        }
    }

    public void testReadZLong() throws IOException {
        long testValue = 987654321L;

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeZLong(testValue);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                assertEquals(testValue, input.readZLong());
            }
        }
    }

    public void testEOFException() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeInt(42);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                assertEquals(42, input.readInt());

                expectThrows(EOFException.class, () -> input.readInt());
            }
        }
    }

    public void testWrongVectorTypeException() throws IOException {

        TinyIntVector byteVector = new TinyIntVector("root.0", allocator);
        byteVector.allocateNew(1);
        byteVector.setSafe(0, (byte) 42);
        byteVector.setValueCount(1);

        VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(byteVector));

        try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {

            expectThrows(IOException.class, () -> input.readInt());
        } finally {
            root.close();
        }
    }

    public void testMissingVectorException() throws IOException {

        VectorSchemaRoot root = new VectorSchemaRoot(Collections.emptyList());

        try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
            expectThrows(RuntimeException.class, () -> input.readInt());
        } finally {
            root.close();
        }
    }

    public void testNamedWriteableWithoutMetadata() throws IOException {

        StructVector structVector = new StructVector("root.0", allocator, FieldType.nullable(new ArrowType.Struct()), null);
        structVector.allocateNew();
        structVector.setValueCount(1);

        VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(structVector));

        try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
            expectThrows(IOException.class, () -> input.readNamedWriteable(TestNamedWriteable.class));
        } finally {
            root.close();
        }
    }

    public void testReset() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeInt(42);
            output.writeString("test");

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                assertEquals(42, input.readInt());

                input.reset();
                assertEquals(42, input.readInt());
                assertEquals("test", input.readString());
            }
        }
    }

    public void testUnsupportedOperations() throws IOException {
        VectorSchemaRoot root = new VectorSchemaRoot(Collections.emptyList());

        try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
            expectThrows(UnsupportedOperationException.class, () -> input.read());
            expectThrows(UnsupportedOperationException.class, () -> input.available());
        } finally {
            root.close();
        }
    }

    public void testNamedWriteableRegistry() throws IOException {
        VectorSchemaRoot root = new VectorSchemaRoot(Collections.emptyList());

        try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
            assertSame(registry, input.namedWriteableRegistry());
        } finally {
            root.close();
        }
    }

    public void testComplexNestedStructure() throws IOException {

        TestNamedWriteableWithList original = new TestNamedWriteableWithList(
            "complex-test",
            Arrays.asList(new TestWriteable(1, "item1"), new TestWriteable(2, "item2"))
        );

        NamedWriteableRegistry complexRegistry = new NamedWriteableRegistry(
            Arrays.asList(
                new NamedWriteableRegistry.Entry(TestNamedWriteableWithList.class, "complex-writeable", TestNamedWriteableWithList::new)
            )
        );

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeNamedWriteable(original);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, complexRegistry)) {
                TestNamedWriteableWithList result = input.readNamedWriteable(TestNamedWriteableWithList.class);
                assertEquals(original.name, result.name);
                assertEquals(original.items.size(), result.items.size());

                for (int i = 0; i < original.items.size(); i++) {
                    assertEquals(original.items.get(i).intValue, result.items.get(i).intValue);
                    assertEquals(original.items.get(i).stringValue, result.items.get(i).stringValue);
                }
            }
        }
    }

    private static class TestWriteable implements Writeable {
        final int intValue;
        final String stringValue;

        TestWriteable(int intValue, String stringValue) {
            this.intValue = intValue;
            this.stringValue = stringValue;
        }

        TestWriteable(StreamInput in) throws IOException {
            this.intValue = in.readInt();
            this.stringValue = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(intValue);
            out.writeString(stringValue);
        }
    }

    private static class TestNamedWriteable implements NamedWriteable {
        final String name;
        final int intValue;
        final String stringValue;

        TestNamedWriteable(String name, int intValue, String stringValue) {
            this.name = name;
            this.intValue = intValue;
            this.stringValue = stringValue;
        }

        TestNamedWriteable(StreamInput in) throws IOException {
            this.name = in.readString();
            this.intValue = in.readInt();
            this.stringValue = in.readString();
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

    private static class TestNamedWriteableWithList implements NamedWriteable {
        final String name;
        final List<TestWriteable> items;

        TestNamedWriteableWithList(String name, List<TestWriteable> items) {
            this.name = name;
            this.items = items;
        }

        TestNamedWriteableWithList(StreamInput in) throws IOException {
            this.name = in.readString();
            this.items = in.readList(TestWriteable::new);
        }

        @Override
        public String getWriteableName() {
            return "complex-writeable";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeList(items);
        }
    }

    public void testReadMinMaxValues() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
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
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {

                assertEquals(Byte.MIN_VALUE, input.readByte());
                assertEquals(Byte.MAX_VALUE, input.readByte());

                assertEquals(Integer.MIN_VALUE, input.readInt());
                assertEquals(Integer.MAX_VALUE, input.readInt());

                assertEquals(Long.MIN_VALUE, input.readLong());
                assertEquals(Long.MAX_VALUE, input.readLong());

                assertEquals(Float.MIN_VALUE, input.readFloat(), 0.0f);
                assertEquals(Float.MAX_VALUE, input.readFloat(), 0.0f);
                assertEquals(Float.NEGATIVE_INFINITY, input.readFloat(), 0.0f);
                assertEquals(Float.POSITIVE_INFINITY, input.readFloat(), 0.0f);
                assertTrue(Float.isNaN(input.readFloat()));

                assertEquals(Double.MIN_VALUE, input.readDouble(), 0.0);
                assertEquals(Double.MAX_VALUE, input.readDouble(), 0.0);
                assertEquals(Double.NEGATIVE_INFINITY, input.readDouble(), 0.0);
                assertEquals(Double.POSITIVE_INFINITY, input.readDouble(), 0.0);
                assertTrue(Double.isNaN(input.readDouble()));
            }
        }
    }

    public void testReadEmptyAndNullStrings() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeString("");
            output.writeString(null);
            output.writeText(new Text(""));
            output.writeText(null);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                assertEquals("", input.readString());
                assertNull(input.readString());
                assertEquals("", input.readText().toString());
                assertNull(input.readText());
            }
        }
    }

    public void testReadLargeStrings() throws IOException {

        StringBuilder largeString = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largeString.append("This is a test string with some content ");
        }
        String testString = largeString.toString();

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeString(testString);
            output.writeText(new Text(testString));

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                assertEquals(testString, input.readString());
                assertEquals(testString, input.readText().toString());
            }
        }
    }

    public void testReadUnicodeStrings() throws IOException {
        String unicodeString = "Hello 世界 🌍 Здравствуй мир 🚀 مرحبا بالعالم";

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeString(unicodeString);
            output.writeText(new Text(unicodeString));

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                assertEquals(unicodeString, input.readString());
                assertEquals(unicodeString, input.readText().toString());
            }
        }
    }

    public void testReadLargeByteArrays() throws IOException {

        byte[] largeArray = new byte[10000];
        for (int i = 0; i < largeArray.length; i++) {
            largeArray[i] = (byte) (i % 256);
        }

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeBytes(largeArray, 0, largeArray.length);

            byte[] partialArray = new byte[2500];
            System.arraycopy(largeArray, 5000, partialArray, 0, 2500);
            output.writeBytes(partialArray, 0, partialArray.length);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                byte[] result1 = new byte[largeArray.length];
                input.readBytes(result1, 0, largeArray.length);
                assertArrayEquals(largeArray, result1);

                byte[] result2 = new byte[partialArray.length];
                input.readBytes(result2, 0, partialArray.length);
                assertArrayEquals(partialArray, result2);
            }
        }
    }

    public void testReadBytesEdgeCases() throws IOException {
        byte[] testArray = "Hello World".getBytes(StandardCharsets.UTF_8);

        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {

            output.writeBytes(testArray, 0, testArray.length);

            output.writeBytes(testArray, testArray.length, 0);

            output.writeBytes(testArray, 0, 1);

            output.writeBytes(testArray, testArray.length - 1, 1);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                byte[] result1 = new byte[testArray.length];
                input.readBytes(result1, 0, testArray.length);
                assertArrayEquals(testArray, result1);

                byte[] result2 = new byte[0];
                input.readBytes(result2, 0, 0);
                assertEquals(0, result2.length);

                byte[] result3 = new byte[1];
                input.readBytes(result3, 0, 1);
                assertArrayEquals(new byte[] { testArray[0] }, result3);

                byte[] result4 = new byte[1];
                input.readBytes(result4, 0, 1);
                assertArrayEquals(new byte[] { testArray[testArray.length - 1] }, result4);
            }
        }
    }
}
