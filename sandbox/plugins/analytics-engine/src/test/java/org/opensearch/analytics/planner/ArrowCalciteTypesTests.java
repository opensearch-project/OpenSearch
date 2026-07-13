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
}
