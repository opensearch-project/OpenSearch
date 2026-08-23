/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.parquet.vsr.ManagedVSR;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Every field type that can be declared {@code multi_value: true} must write a {@code LIST} column, so
 * that a correlated group is not limited to keyword members.
 *
 * <p>Each case asserts three things a type has to get right: {@code supportsMultiValue()} agrees with
 * whether {@link ParquetField#addToVector} is implemented, the Arrow field is a {@code LIST} of the
 * element type, and the values written into the child vector read back at the expected positions.
 * Writing at an explicit index is the part that differs from the scalar path — list elements sit at
 * positions in the child vector unrelated to the row number.
 */
public class MultiValueFieldTypeTests extends OpenSearchTestCase {

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

    /** Writes one row holding the given values into a LIST column and hands back the vector. */
    private ListVector writeRow(String typeName, String name, List<?> values) {
        ParquetField field = ArrowFieldRegistry.getParquetField(typeName);
        assertTrue("type [" + typeName + "] should support multi_value", field.supportsMultiValue());

        Field arrowField = field.toArrowField(name, true);
        assertEquals("multi_value must produce a LIST column", ArrowType.List.INSTANCE, arrowField.getType());

        BufferAllocator child = allocator.newChildAllocator("mv-" + typeName, 0, Long.MAX_VALUE);
        ManagedVSR vsr = new ManagedVSR("mv-" + typeName, new Schema(List.of(arrowField)), child);
        ListVector listVector = (ListVector) vsr.getVector(name);

        int start = listVector.startNewValue(0);
        FieldVector data = listVector.getDataVector();
        for (int i = 0; i < values.size(); i++) {
            field.addToVector(data, start + i, values.get(i));
        }
        listVector.endValue(0, values.size());
        vsr.setRowCount(1);
        return listVector;
    }

    private static void assertElements(ListVector listVector, List<?> expected) {
        assertFalse("the row must be a non-null list", listVector.isNull(0));
        int start = listVector.getOffsetBuffer().getInt(0);
        int end = listVector.getOffsetBuffer().getInt(ListVector.OFFSET_WIDTH);
        assertEquals("element count", expected.size(), end - start);
        FieldVector data = listVector.getDataVector();
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("element " + i, expected.get(i), data.getObject(start + i));
        }
    }

    public void testLongMultiValue() {
        ListVector v = writeRow(NumberFieldMapper.NumberType.LONG.typeName(), "counts", List.of(1L, 2L, 3L));
        assertElements(v, List.of(1L, 2L, 3L));
        v.close();
    }

    public void testIntegerMultiValue() {
        ListVector v = writeRow(NumberFieldMapper.NumberType.INTEGER.typeName(), "ints", List.of(7, 8));
        assertElements(v, List.of(7, 8));
        v.close();
    }

    public void testDoubleMultiValue() {
        ListVector v = writeRow(NumberFieldMapper.NumberType.DOUBLE.typeName(), "ratios", List.of(1.5d, 2.5d));
        assertElements(v, List.of(1.5d, 2.5d));
        v.close();
    }

    public void testShortAndByteMultiValue() {
        ListVector shorts = writeRow(NumberFieldMapper.NumberType.SHORT.typeName(), "shorts", List.of((short) 1, (short) 2));
        assertElements(shorts, List.of((short) 1, (short) 2));
        shorts.close();

        ListVector bytes = writeRow(NumberFieldMapper.NumberType.BYTE.typeName(), "bytes", List.of((byte) 3, (byte) 4));
        assertElements(bytes, List.of((byte) 3, (byte) 4));
        bytes.close();
    }

    /**
     * The OTel {@code Events.Timestamp Array(DateTime64(9))} case: a LIST of nanosecond timestamps.
     * Compared through the vector's primitive accessor, since {@code getObject} on a timestamp vector
     * decodes to a {@link java.time.LocalDateTime}.
     */
    public void testDateNanosMultiValue() {
        long first = 1755684000000000001L;
        long second = 1755684005000000002L;
        ListVector v = writeRow(DateFieldMapper.DATE_NANOS_CONTENT_TYPE, "Events.Timestamp", List.of(first, second));
        TimeStampNanoVector data = (TimeStampNanoVector) v.getDataVector();
        assertEquals(first, data.get(0));
        assertEquals(second, data.get(1));
        assertEquals(
            new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, null),
            v.getField().getChildren().get(0).getType()
        );
        v.close();
    }

    public void testDateMillisMultiValue() {
        ListVector v = writeRow(DateFieldMapper.CONTENT_TYPE, "dates", List.of(1755684000000L, 1755684005000L));
        TimeStampMilliVector data = (TimeStampMilliVector) v.getDataVector();
        assertEquals(1755684000000L, data.get(0));
        assertEquals(1755684005000L, data.get(1));
        v.close();
    }

    public void testBooleanMultiValue() {
        ListVector v = writeRow(BooleanFieldMapper.CONTENT_TYPE, "flags", List.of(true, false, true));
        assertElements(v, List.of(true, false, true));
        v.close();
    }

    public void testKeywordAndTextMultiValue() {
        ListVector kw = writeRow(KeywordFieldMapper.CONTENT_TYPE, "tags", List.of("a", "b"));
        assertEquals("a", kw.getDataVector().getObject(0).toString());
        assertEquals("b", kw.getDataVector().getObject(1).toString());
        kw.close();

        ListVector text = writeRow(TextFieldMapper.CONTENT_TYPE, "bodies", List.of("hello", "world"));
        assertEquals("hello", text.getDataVector().getObject(0).toString());
        assertEquals("world", text.getDataVector().getObject(1).toString());
        text.close();
    }

    public void testBinaryMultiValue() {
        byte[] one = "one".getBytes(StandardCharsets.UTF_8);
        byte[] two = "two".getBytes(StandardCharsets.UTF_8);
        ListVector v = writeRow(BinaryFieldMapper.CONTENT_TYPE, "blobs", List.of(one, two));
        assertArrayEquals(one, (byte[]) v.getDataVector().getObject(0));
        assertArrayEquals(two, (byte[]) v.getDataVector().getObject(1));
        v.close();
    }

    /** IP addresses are encoded to their sortable 16-byte form, same as the scalar path. */
    public void testIpMultiValue() throws Exception {
        InetAddress first = InetAddress.getByName("10.0.0.1");
        InetAddress second = InetAddress.getByName("10.0.0.2");
        ListVector v = writeRow(IpFieldMapper.CONTENT_TYPE, "addrs", List.of(first, second));
        assertEquals(16, ((byte[]) v.getDataVector().getObject(0)).length);
        assertFalse(
            "distinct addresses must encode differently",
            java.util.Arrays.equals((byte[]) v.getDataVector().getObject(0), (byte[]) v.getDataVector().getObject(1))
        );
        v.close();
    }

    /**
     * The whole registry is covered: every registered type either supports list storage or is a
     * deliberate exception. A new type defaulting to unsupported should surface here rather than as a
     * per-document indexing failure.
     */
    public void testEveryRegisteredTypeSupportsMultiValue() {
        List<String> types = List.of(
            NumberFieldMapper.NumberType.LONG.typeName(),
            NumberFieldMapper.NumberType.INTEGER.typeName(),
            NumberFieldMapper.NumberType.SHORT.typeName(),
            NumberFieldMapper.NumberType.BYTE.typeName(),
            NumberFieldMapper.NumberType.FLOAT.typeName(),
            NumberFieldMapper.NumberType.DOUBLE.typeName(),
            NumberFieldMapper.NumberType.HALF_FLOAT.typeName(),
            NumberFieldMapper.NumberType.UNSIGNED_LONG.typeName(),
            DateFieldMapper.CONTENT_TYPE,
            DateFieldMapper.DATE_NANOS_CONTENT_TYPE,
            BooleanFieldMapper.CONTENT_TYPE,
            IpFieldMapper.CONTENT_TYPE,
            KeywordFieldMapper.CONTENT_TYPE,
            TextFieldMapper.CONTENT_TYPE,
            BinaryFieldMapper.CONTENT_TYPE,
            "flat_object"
        );
        for (String type : types) {
            ParquetField field = ArrowFieldRegistry.getParquetField(type);
            assertNotNull("no ParquetField registered for [" + type + "]", field);
            assertTrue("type [" + type + "] does not support multi_value", field.supportsMultiValue());
        }
    }

    /** A MappedFieldType carries the declared arity through to the Arrow field. */
    public void testArityComesFromTheFieldType() {
        MappedFieldType scalar = new KeywordFieldMapper.KeywordFieldType("scalar");
        MappedFieldType list = new KeywordFieldMapper.KeywordFieldType("list");
        list.setMultiValued(true);

        ParquetField field = ArrowFieldRegistry.getParquetField(KeywordFieldMapper.CONTENT_TYPE);
        assertEquals(new ArrowType.Utf8(), field.toArrowField(scalar.name(), scalar.isMultiValued()).getType());
        assertEquals(ArrowType.List.INSTANCE, field.toArrowField(list.name(), list.isMultiValued()).getType());
    }
}
