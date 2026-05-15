/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link CalciteToArrowSchema}. Pinned to the types analytics-engine emits today;
 * additions should ship with corresponding cases here.
 */
public class CalciteToArrowSchemaTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    public void testPrimitiveNumericTypes() {
        RelDataType rowType = typeFactory.builder()
            .add("tiny", typeFactory.createSqlType(SqlTypeName.TINYINT))
            .add("small", typeFactory.createSqlType(SqlTypeName.SMALLINT))
            .add("i", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .add("l", typeFactory.createSqlType(SqlTypeName.BIGINT))
            .add("f", typeFactory.createSqlType(SqlTypeName.FLOAT))
            .add("d", typeFactory.createSqlType(SqlTypeName.DOUBLE))
            .build();

        Schema schema = CalciteToArrowSchema.convert(rowType);

        assertEquals(6, schema.getFields().size());
        assertEquals(new ArrowType.Int(8, true), schema.findField("tiny").getType());
        assertEquals(new ArrowType.Int(16, true), schema.findField("small").getType());
        assertEquals(new ArrowType.Int(32, true), schema.findField("i").getType());
        assertEquals(new ArrowType.Int(64, true), schema.findField("l").getType());
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), schema.findField("f").getType());
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), schema.findField("d").getType());
    }

    public void testStringAndBoolean() {
        RelDataType rowType = typeFactory.builder()
            .add("s", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("b", typeFactory.createSqlType(SqlTypeName.BOOLEAN))
            .build();
        Schema schema = CalciteToArrowSchema.convert(rowType);
        assertEquals(new ArrowType.Utf8(), schema.findField("s").getType());
        assertEquals(ArrowType.Bool.INSTANCE, schema.findField("b").getType());
    }

    public void testDecimalPreservesPrecisionAndScale() {
        // JavaTypeFactoryImpl caps decimal precision at 19 internally; the converter passes it
        // through unchanged (within Arrow's 38-bit limit).
        RelDataType rowType = typeFactory.builder().add("amt", typeFactory.createSqlType(SqlTypeName.DECIMAL, 19, 5)).build();
        Schema schema = CalciteToArrowSchema.convert(rowType);
        ArrowType.Decimal d = (ArrowType.Decimal) schema.findField("amt").getType();
        assertEquals(19, d.getPrecision());
        assertEquals(5, d.getScale());
        assertEquals(128, d.getBitWidth());
    }

    /** Unknown SqlTypeName falls back to Utf8 — last-resort coercion documented on the converter. */
    public void testUnknownTypeFallsBackToUtf8() {
        // ARRAY isn't in the whitelist; verifies the default branch.
        RelDataType rowType = typeFactory.builder()
            .add("arr", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1))
            .build();
        Schema schema = CalciteToArrowSchema.convert(rowType);
        assertEquals(new ArrowType.Utf8(), schema.findField("arr").getType());
    }

    public void testFieldsAreNullable() {
        RelDataType rowType = typeFactory.builder().add("i", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();
        Schema schema = CalciteToArrowSchema.convert(rowType);
        assertTrue("fallback schema fields are nullable by design", schema.findField("i").isNullable());
    }
}
