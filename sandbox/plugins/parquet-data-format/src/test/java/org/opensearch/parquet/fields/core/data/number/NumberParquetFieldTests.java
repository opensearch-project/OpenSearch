/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.number;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class NumberParquetFieldTests extends OpenSearchTestCase {

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

    public void testIntegerFieldArrowType() {
        IntegerParquetField field = new IntegerParquetField();
        ArrowType.Int type = (ArrowType.Int) field.getArrowType();
        assertEquals(32, type.getBitWidth());
        assertTrue(type.getIsSigned());
        assertTrue(field.getFieldType().isNullable());
    }

    public void testIntegerFieldAddToGroup() {
        IntegerParquetField field = new IntegerParquetField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        ManagedVSR vsr = createVSR("int-test", field, "val");
        field.createField(ft, vsr, 42);
        vsr.setRowCount(1);
        IntVector vec = (IntVector) vsr.getVector("val");
        assertEquals(42, vec.get(0));
        cleanupVSR(vsr);
    }

    public void testLongFieldArrowType() {
        LongParquetField field = new LongParquetField();
        ArrowType.Int type = (ArrowType.Int) field.getArrowType();
        assertEquals(64, type.getBitWidth());
        assertTrue(type.getIsSigned());
    }

    public void testLongFieldAddToGroup() {
        LongParquetField field = new LongParquetField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.LONG);
        ManagedVSR vsr = createVSR("long-test", field, "val");
        field.createField(ft, vsr, 123456789L);
        vsr.setRowCount(1);
        assertEquals(123456789L, ((BigIntVector) vsr.getVector("val")).get(0));
        cleanupVSR(vsr);
    }

    public void testFloatFieldArrowType() {
        FloatParquetField field = new FloatParquetField();
        ArrowType.FloatingPoint type = (ArrowType.FloatingPoint) field.getArrowType();
        assertEquals(FloatingPointPrecision.SINGLE, type.getPrecision());
    }

    public void testFloatFieldAddToGroup() {
        FloatParquetField field = new FloatParquetField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.FLOAT);
        ManagedVSR vsr = createVSR("float-test", field, "val");
        field.createField(ft, vsr, 3.14f);
        vsr.setRowCount(1);
        assertEquals(3.14f, ((Float4Vector) vsr.getVector("val")).get(0), 0.001f);
        cleanupVSR(vsr);
    }

    public void testDoubleFieldArrowType() {
        DoubleParquetField field = new DoubleParquetField();
        ArrowType.FloatingPoint type = (ArrowType.FloatingPoint) field.getArrowType();
        assertEquals(FloatingPointPrecision.DOUBLE, type.getPrecision());
    }

    public void testDoubleFieldAddToGroup() {
        DoubleParquetField field = new DoubleParquetField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.DOUBLE);
        ManagedVSR vsr = createVSR("double-test", field, "val");
        field.createField(ft, vsr, 2.718);
        vsr.setRowCount(1);
        assertEquals(2.718, ((Float8Vector) vsr.getVector("val")).get(0), 0.001);
        cleanupVSR(vsr);
    }

    public void testHalfFloatFieldArrowType() {
        HalfFloatParquetField field = new HalfFloatParquetField();
        ArrowType.FloatingPoint type = (ArrowType.FloatingPoint) field.getArrowType();
        assertEquals(FloatingPointPrecision.HALF, type.getPrecision());
    }

    public void testShortFieldArrowType() {
        ShortParquetField field = new ShortParquetField();
        ArrowType.Int type = (ArrowType.Int) field.getArrowType();
        assertEquals(16, type.getBitWidth());
        assertTrue(type.getIsSigned());
    }

    public void testShortFieldAddToGroup() {
        ShortParquetField field = new ShortParquetField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.SHORT);
        ManagedVSR vsr = createVSR("short-test", field, "val");
        field.createField(ft, vsr, (short) 7);
        vsr.setRowCount(1);
        assertEquals(7, ((SmallIntVector) vsr.getVector("val")).get(0));
        cleanupVSR(vsr);
    }

    public void testByteFieldArrowType() {
        ByteParquetField field = new ByteParquetField();
        ArrowType.Int type = (ArrowType.Int) field.getArrowType();
        assertEquals(8, type.getBitWidth());
        assertTrue(type.getIsSigned());
    }

    public void testByteFieldAddToGroup() {
        ByteParquetField field = new ByteParquetField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.BYTE);
        ManagedVSR vsr = createVSR("byte-test", field, "val");
        field.createField(ft, vsr, (byte) 3);
        vsr.setRowCount(1);
        assertEquals(3, ((TinyIntVector) vsr.getVector("val")).get(0));
        cleanupVSR(vsr);
    }

    public void testUnsignedLongFieldArrowType() {
        UnsignedLongParquetField field = new UnsignedLongParquetField();
        ArrowType.Int type = (ArrowType.Int) field.getArrowType();
        assertEquals(64, type.getBitWidth());
        assertFalse(type.getIsSigned());
    }

    public void testTokenCountFieldArrowType() {
        TokenCountParquetField field = new TokenCountParquetField();
        ArrowType.Int type = (ArrowType.Int) field.getArrowType();
        assertEquals(32, type.getBitWidth());
        assertTrue(type.getIsSigned());
    }

    private ManagedVSR createVSR(String id, ParquetField pf, String fieldName) {
        Schema schema = new Schema(List.of(new Field(fieldName, pf.getFieldType(), null)));
        BufferAllocator child = allocator.newChildAllocator(id, 0, Long.MAX_VALUE);
        return new ManagedVSR(id, schema, child);
    }

    private void cleanupVSR(ManagedVSR vsr) {
        vsr.moveToFrozen();
        vsr.close();
    }
}
