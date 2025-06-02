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

        // Create Arrow data using ArrowStreamOutput
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeByte(testValue);

            VectorSchemaRoot root = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
                byte result = input.readByte();
                assertEquals(testValue, result);
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
                byte[] result = new byte[10]; // Wrong length
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
                // Try to read beyond available data
                expectThrows(EOFException.class, () -> input.readInt());
            }
        }
    }

    public void testWrongVectorTypeException() throws IOException {
        // Create a vector with wrong type manually
        TinyIntVector byteVector = new TinyIntVector("root.0", allocator);
        byteVector.allocateNew(1);
        byteVector.setSafe(0, (byte) 42);
        byteVector.setValueCount(1);

        VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(byteVector));

        try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
            // Try to read as int when it's actually a byte
            expectThrows(IOException.class, () -> input.readInt());
        } finally {
            root.close();
        }
    }

    public void testMissingVectorException() throws IOException {
        // Create empty root
        VectorSchemaRoot root = new VectorSchemaRoot(Collections.emptyList());

        try (ArrowStreamInput input = new ArrowStreamInput(root, registry)) {
            expectThrows(RuntimeException.class, () -> input.readInt());
        } finally {
            root.close();
        }
    }

    public void testNamedWriteableWithoutMetadata() throws IOException {
        // Create a struct vector without name metadata
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

                // Reset and read again
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
        // Test reading a complex nested structure with NamedWriteable containing a list
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

    // Helper classes for testing
    private static class TestWriteable implements Writeable {
        final int intValue;
        final String stringValue;

        TestWriteable(int intValue, String stringValue) {
            this.intValue = intValue;
            this.stringValue = stringValue;
        }

        TestWriteable(org.opensearch.core.common.io.stream.StreamInput in) throws IOException {
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

        TestNamedWriteable(org.opensearch.core.common.io.stream.StreamInput in) throws IOException {
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

        TestNamedWriteableWithList(org.opensearch.core.common.io.stream.StreamInput in) throws IOException {
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
}
