/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link ArrowCalciteTypes} Calcite→Arrow mapping used by the QTF
 * (late-materialization) stitch path.
 */
public class ArrowCalciteTypesTests extends OpenSearchTestCase {

    private static final SqlTypeFactoryImpl TYPE_FACTORY = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    /** Lifts TIMESTAMP max-precision to 9; default Calcite caps at 3 and would clamp date_nanos away. */
    private static final SqlTypeFactoryImpl NANOS_TYPE_FACTORY = new SqlTypeFactoryImpl(new RelDataTypeSystemImpl() {
        @Override
        public int getMaxPrecision(SqlTypeName typeName) {
            if (typeName == SqlTypeName.TIMESTAMP || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                return 9;
            }
            return super.getMaxPrecision(typeName);
        }
    });

    private static RelDataType type(SqlTypeName name) {
        return TYPE_FACTORY.createSqlType(name);
    }

    private static RelDataType timestamp(int precision) {
        return NANOS_TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP, precision);
    }

    /**
     * SMALLINT (OpenSearch {@code short}) must map to the wire Arrow type the data node
     * emits — {@code Int(16, true)} per {@code ShortParquetField} — so the Stitcher's
     * copyFromSafe sees matching types. Previously threw "Unsupported Calcite type: SMALLINT".
     */
    public void testSmallintMapsToInt16() {
        assertEquals(new ArrowType.Int(16, true), ArrowCalciteTypes.toArrow(type(SqlTypeName.SMALLINT)));
    }

    /** TINYINT (OpenSearch {@code byte}) -> Int(8, true) per ByteParquetField. */
    public void testTinyintMapsToInt8() {
        assertEquals(new ArrowType.Int(8, true), ArrowCalciteTypes.toArrow(type(SqlTypeName.TINYINT)));
    }

    public void testIntegerAndBigintUnchanged() {
        assertEquals(new ArrowType.Int(32, true), ArrowCalciteTypes.toArrow(type(SqlTypeName.INTEGER)));
        assertEquals(new ArrowType.Int(64, true), ArrowCalciteTypes.toArrow(type(SqlTypeName.BIGINT)));
    }

    /** date ⇒ TIMESTAMP(3) ⇒ MILLISECOND. */
    public void testTimestampPrecision3MapsToMillisecond() {
        assertEquals(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), ArrowCalciteTypes.toArrow(timestamp(3)));
    }

    /** date_nanos ⇒ TIMESTAMP(9) ⇒ NANOSECOND — regression: previously hardcoded MILLISECOND, tripped Stitcher copyFromSafe. */
    public void testTimestampPrecision9MapsToNanosecond() {
        assertEquals(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null), ArrowCalciteTypes.toArrow(timestamp(9)));
    }

    /** Default-precision TIMESTAMP (precision 0) keeps the legacy MILLISECOND mapping. */
    public void testTimestampDefaultPrecisionMapsToMillisecond() {
        assertEquals(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), ArrowCalciteTypes.toArrow(type(SqlTypeName.TIMESTAMP)));
    }

    /**
     * A multi-valued column (index.parquet.multi_value.field) is typed ARRAY in the Calcite schema
     * and is a real LIST column on disk. Previously this threw "Unsupported Calcite type: ARRAY".
     */
    public void testArrayMapsToList() {
        RelDataType arrayOfVarchar = TYPE_FACTORY.createArrayType(type(SqlTypeName.VARCHAR), -1);
        assertEquals(ArrowType.List.INSTANCE, ArrowCalciteTypes.toArrow(arrayOfVarchar));
    }

    /**
     * An Arrow list carries its element type on the Field, not the ArrowType, so toArrowField must
     * be used wherever a Field is built — toArrow alone cannot express the element type.
     */
    public void testToArrowFieldCarriesElementType() {
        RelDataType arrayOfVarchar = TYPE_FACTORY.createArrayType(type(SqlTypeName.VARCHAR), -1);
        Field field = ArrowCalciteTypes.toArrowField("tags", arrayOfVarchar);

        assertEquals("tags", field.getName());
        assertEquals(ArrowType.List.INSTANCE, field.getType());
        assertEquals(1, field.getChildren().size());
        // Must match the write side's ParquetField.LIST_ELEMENT_NAME so the coordinator's expected
        // schema lines up with what the data nodes emit.
        assertEquals("element", field.getChildren().get(0).getName());
        assertEquals(ArrowType.Utf8View.INSTANCE, field.getChildren().get(0).getType());
    }

    /** Scalar columns keep the pre-existing flat Field shape. */
    public void testToArrowFieldScalarHasNoChildren() {
        Field field = ArrowCalciteTypes.toArrowField("age", type(SqlTypeName.INTEGER));
        assertEquals(new ArrowType.Int(32, true), field.getType());
        assertTrue(field.getChildren().isEmpty());
    }

    /** Nested arrays recurse rather than losing the inner element type. */
    public void testToArrowFieldNestedArray() {
        RelDataType inner = TYPE_FACTORY.createArrayType(type(SqlTypeName.BIGINT), -1);
        Field field = ArrowCalciteTypes.toArrowField("matrix", TYPE_FACTORY.createArrayType(inner, -1));
        Field child = field.getChildren().get(0);
        assertEquals(ArrowType.List.INSTANCE, child.getType());
        assertEquals(new ArrowType.Int(64, true), child.getChildren().get(0).getType());
    }
}
