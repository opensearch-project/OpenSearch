/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class ComparisonSerializerTests extends OpenSearchTestCase {

    private final ComparisonSerializer serializer = new ComparisonSerializer();
    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelDataType integer;

    private static final List<FieldStorageInfo> FIELD_STORAGE = List.of(
        new FieldStorageInfo("price", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
    }

    public void testGreaterThan() {
        RexNode call = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(integer, 0),
            rexBuilder.makeLiteral(100, integer, false)
        );
        RangeQueryBuilder range = (RangeQueryBuilder) serializer.buildQueryBuilder((org.apache.calcite.rex.RexCall) call, FIELD_STORAGE);
        assertEquals("price", range.fieldName());
        assertEquals(100, range.from());
        assertFalse(range.includeLower());
        assertNull(range.to());
    }

    public void testGreaterThanOrEqual() {
        RexNode call = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(integer, 0),
            rexBuilder.makeLiteral(50, integer, false)
        );
        RangeQueryBuilder range = (RangeQueryBuilder) serializer.buildQueryBuilder((org.apache.calcite.rex.RexCall) call, FIELD_STORAGE);
        assertEquals("price", range.fieldName());
        assertEquals(50, range.from());
        assertTrue(range.includeLower());
    }

    public void testLessThan() {
        RexNode call = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeInputRef(integer, 0),
            rexBuilder.makeLiteral(200, integer, false)
        );
        RangeQueryBuilder range = (RangeQueryBuilder) serializer.buildQueryBuilder((org.apache.calcite.rex.RexCall) call, FIELD_STORAGE);
        assertEquals("price", range.fieldName());
        assertEquals(200, range.to());
        assertFalse(range.includeUpper());
        assertNull(range.from());
    }

    public void testLessThanOrEqual() {
        RexNode call = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(integer, 0),
            rexBuilder.makeLiteral(999, integer, false)
        );
        RangeQueryBuilder range = (RangeQueryBuilder) serializer.buildQueryBuilder((org.apache.calcite.rex.RexCall) call, FIELD_STORAGE);
        assertEquals("price", range.fieldName());
        assertEquals(999, range.to());
        assertTrue(range.includeUpper());
    }

    public void testReversedOperandFlipsDirection() {
        // 100 < col ⇒ col > 100
        RexNode call = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeLiteral(100, integer, false),
            rexBuilder.makeInputRef(integer, 0)
        );
        RangeQueryBuilder range = (RangeQueryBuilder) serializer.buildQueryBuilder((org.apache.calcite.rex.RexCall) call, FIELD_STORAGE);
        assertEquals("price", range.fieldName());
        assertEquals(100, range.from());
        assertFalse(range.includeLower());
    }
}
