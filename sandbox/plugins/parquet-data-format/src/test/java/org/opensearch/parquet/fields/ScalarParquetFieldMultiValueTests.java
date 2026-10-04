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
import org.apache.arrow.memory.util.Float16;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.document.InetAddressPoint;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.parquet.fields.core.data.BinaryParquetField;
import org.opensearch.parquet.fields.core.data.BooleanParquetField;
import org.opensearch.parquet.fields.core.data.date.DateNanosParquetField;
import org.opensearch.parquet.fields.core.data.date.DateParquetField;
import org.opensearch.parquet.fields.core.data.number.ByteParquetField;
import org.opensearch.parquet.fields.core.data.number.DoubleParquetField;
import org.opensearch.parquet.fields.core.data.number.FloatParquetField;
import org.opensearch.parquet.fields.core.data.number.HalfFloatParquetField;
import org.opensearch.parquet.fields.core.data.number.IntegerParquetField;
import org.opensearch.parquet.fields.core.data.number.LongParquetField;
import org.opensearch.parquet.fields.core.data.number.ShortParquetField;
import org.opensearch.parquet.fields.core.data.number.UnsignedLongParquetField;
import org.opensearch.parquet.fields.core.data.text.IpParquetField;
import org.opensearch.parquet.fields.core.data.text.TextParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class ScalarParquetFieldMultiValueTests extends OpenSearchTestCase {

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

    public void testIntegralListElements() {
        assertList(
            new ByteParquetField(),
            numberType(NumberFieldMapper.NumberType.BYTE),
            List.of((byte) -2, (byte) 7),
            vector -> assertArrayEquals(new byte[] { -2, 7 }, values((TinyIntVector) vector))
        );
        assertList(
            new ShortParquetField(),
            numberType(NumberFieldMapper.NumberType.SHORT),
            List.of((short) -300, (short) 900),
            vector -> assertArrayEquals(new short[] { -300, 900 }, values((SmallIntVector) vector))
        );
        assertList(
            new IntegerParquetField(),
            numberType(NumberFieldMapper.NumberType.INTEGER),
            List.of(-10, 20),
            vector -> assertArrayEquals(new int[] { -10, 20 }, values((IntVector) vector))
        );
        assertList(
            new LongParquetField(),
            numberType(NumberFieldMapper.NumberType.LONG),
            List.of(-100L, 200L),
            vector -> assertArrayEquals(new long[] { -100L, 200L }, values((BigIntVector) vector))
        );
        assertList(
            new UnsignedLongParquetField(),
            numberType(NumberFieldMapper.NumberType.UNSIGNED_LONG),
            List.of(BigInteger.ZERO, new BigInteger("18446744073709551615")),
            vector -> assertArrayEquals(new long[] { 0L, -1L }, values((UInt8Vector) vector))
        );
    }

    public void testFloatingPointListElements() {
        assertList(new HalfFloatParquetField(), numberType(NumberFieldMapper.NumberType.HALF_FLOAT), List.of(1.5f, -2.5f), vector -> {
            Float2Vector values = (Float2Vector) vector;
            assertEquals(1.5f, Float16.toFloat(values.get(0)), 0.0f);
            assertEquals(-2.5f, Float16.toFloat(values.get(1)), 0.0f);
        });
        assertList(new FloatParquetField(), numberType(NumberFieldMapper.NumberType.FLOAT), List.of(1.25f, -3.5f), vector -> {
            assertEquals(1.25f, ((Float4Vector) vector).get(0), 0.0f);
            assertEquals(-3.5f, ((Float4Vector) vector).get(1), 0.0f);
        });
        assertList(new DoubleParquetField(), numberType(NumberFieldMapper.NumberType.DOUBLE), List.of(1.25d, -3.5d), vector -> {
            assertEquals(1.25d, ((Float8Vector) vector).get(0), 0.0d);
            assertEquals(-3.5d, ((Float8Vector) vector).get(1), 0.0d);
        });
    }

    public void testBooleanAndTemporalListElements() {
        assertList(
            new BooleanParquetField(),
            new BooleanFieldMapper.BooleanFieldType("val"),
            List.of(true, false),
            vector -> assertArrayEquals(new int[] { 1, 0 }, values((BitVector) vector))
        );
        assertList(
            new DateParquetField(),
            new DateFieldMapper.DateFieldType("val"),
            List.of(1_700_000_000_000L, 1_700_000_001_000L),
            vector -> assertArrayEquals(new long[] { 1_700_000_000_000L, 1_700_000_001_000L }, values((TimeStampMilliVector) vector))
        );
        assertList(
            new DateNanosParquetField(),
            new DateFieldMapper.DateFieldType("val", DateFieldMapper.Resolution.NANOSECONDS),
            List.of(1_700_000_000_000_000_000L, 1_700_000_000_000_000_001L),
            vector -> assertArrayEquals(
                new long[] { 1_700_000_000_000_000_000L, 1_700_000_000_000_000_001L },
                values((TimeStampNanoVector) vector)
            )
        );
    }

    public void testTextBinaryAndIpListElements() throws Exception {
        assertList(new TextParquetField(), new TextFieldMapper.TextFieldType("val"), List.of("first", "second"), vector -> {
            assertEquals("first", new String(((VarCharVector) vector).get(0), StandardCharsets.UTF_8));
            assertEquals("second", new String(((VarCharVector) vector).get(1), StandardCharsets.UTF_8));
        });
        byte[] first = new byte[] { 1, 2 };
        byte[] second = new byte[] { 3, 4 };
        assertList(new BinaryParquetField(), new BinaryFieldMapper.BinaryFieldType("val"), List.of(first, second), vector -> {
            assertArrayEquals(first, ((VarBinaryVector) vector).get(0));
            assertArrayEquals(second, ((VarBinaryVector) vector).get(1));
        });
        InetAddress loopback = InetAddress.getByName("127.0.0.1");
        InetAddress v6 = InetAddress.getByName("2001:db8::1");
        assertList(new IpParquetField(), new IpFieldMapper.IpFieldType("val"), List.of(loopback, v6), vector -> {
            assertArrayEquals(InetAddressPoint.encode(loopback), ((VarBinaryVector) vector).get(0));
            assertArrayEquals(InetAddressPoint.encode(v6), ((VarBinaryVector) vector).get(1));
        });
    }

    public void testNullElementAndEmptyListRemainDistinctFromAbsent() {
        IntegerParquetField field = new IntegerParquetField();
        MappedFieldType fieldType = numberType(NumberFieldMapper.NumberType.INTEGER);
        assertList(field, fieldType, Arrays.asList(10, null, 20), vector -> {
            IntVector values = (IntVector) vector;
            assertEquals(10, values.get(0));
            assertTrue(values.isNull(1));
            assertEquals(20, values.get(2));
        });
        assertList(field, fieldType, List.of(), vector -> assertEquals(0, vector.getValueCount()));

        Schema schema = new Schema(List.of(field.toArrowField("val", true)));
        BufferAllocator child = allocator.newChildAllocator("absent-list", 0, Long.MAX_VALUE);
        ManagedVSR vsr = new ManagedVSR("absent-list", schema, child);
        try {
            field.createField(fieldType, vsr, null);
            vsr.setRowCount(1);
            assertTrue(((ListVector) vsr.getVector("val")).isNull(0));
        } finally {
            vsr.moveToFrozen();
            vsr.close();
        }
    }

    private NumberFieldMapper.NumberFieldType numberType(NumberFieldMapper.NumberType type) {
        return new NumberFieldMapper.NumberFieldType("val", type);
    }

    private void assertList(
        ParquetField field,
        MappedFieldType fieldType,
        List<?> input,
        Consumer<org.apache.arrow.vector.FieldVector> verify
    ) {
        assertTrue(field.getClass().getSimpleName(), field.supportsMultiValue());
        Schema schema = new Schema(List.of(field.toArrowField("val", true)));
        BufferAllocator child = allocator.newChildAllocator(field.getClass().getSimpleName(), 0, Long.MAX_VALUE);
        ManagedVSR vsr = new ManagedVSR(field.getClass().getSimpleName(), schema, child);
        try {
            field.createField(fieldType, vsr, input);
            vsr.setRowCount(1);
            ListVector list = (ListVector) vsr.getVector("val");
            assertFalse(list.isNull(0));
            assertEquals(input.size(), list.getInnerValueCount());
            verify.accept(list.getDataVector());
        } finally {
            vsr.moveToFrozen();
            vsr.close();
        }
    }

    private static byte[] values(TinyIntVector vector) {
        return new byte[] { vector.get(0), vector.get(1) };
    }

    private static short[] values(SmallIntVector vector) {
        return new short[] { vector.get(0), vector.get(1) };
    }

    private static int[] values(IntVector vector) {
        return new int[] { vector.get(0), vector.get(1) };
    }

    private static int[] values(BitVector vector) {
        return new int[] { vector.get(0), vector.get(1) };
    }

    private static long[] values(BigIntVector vector) {
        return new long[] { vector.get(0), vector.get(1) };
    }

    private static long[] values(UInt8Vector vector) {
        return new long[] { vector.get(0), vector.get(1) };
    }

    private static long[] values(TimeStampMilliVector vector) {
        return new long[] { vector.get(0), vector.get(1) };
    }

    private static long[] values(TimeStampNanoVector vector) {
        return new long[] { vector.get(0), vector.get(1) };
    }
}
