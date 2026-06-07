/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link ArrowCalciteTypes} Calcite→Arrow mapping used by the QTF
 * (late-materialization) stitch path.
 */
public class ArrowCalciteTypesTests extends OpenSearchTestCase {

    private static final SqlTypeFactoryImpl TYPE_FACTORY = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    private static RelDataType type(SqlTypeName name) {
        return TYPE_FACTORY.createSqlType(name);
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
}
