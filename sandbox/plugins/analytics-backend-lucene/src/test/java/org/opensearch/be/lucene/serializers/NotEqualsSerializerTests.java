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
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class NotEqualsSerializerTests extends OpenSearchTestCase {

    private final NotEqualsSerializer serializer = new NotEqualsSerializer();
    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelDataType varchar;

    private static final List<FieldStorageInfo> FIELD_STORAGE = List.of(
        new FieldStorageInfo("status", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }

    public void testNotEqualsProducesExistsAndMustNotTerm() {
        RexNode call = rexBuilder.makeCall(
            SqlStdOperatorTable.NOT_EQUALS,
            rexBuilder.makeInputRef(varchar, 0),
            rexBuilder.makeLiteral("error")
        );
        QueryBuilder qb = serializer.buildQueryBuilder((org.apache.calcite.rex.RexCall) call, FIELD_STORAGE);
        assertTrue(qb instanceof BoolQueryBuilder);
        BoolQueryBuilder bool = (BoolQueryBuilder) qb;
        assertEquals(1, bool.mustNot().size());
        assertTrue(bool.mustNot().get(0) instanceof TermQueryBuilder);
        TermQueryBuilder term = (TermQueryBuilder) bool.mustNot().get(0);
        assertEquals("status", term.fieldName());
        assertEquals("error", term.value());
        // SQL: NULL != 'error' is UNKNOWN, so documents without the field must not match.
        assertEquals(1, bool.filter().size());
        assertTrue(bool.filter().get(0) instanceof ExistsQueryBuilder);
        assertEquals("status", ((ExistsQueryBuilder) bool.filter().get(0)).fieldName());
    }

    public void testNotEqualsRoutesToExactMatchSubfield() {
        List<FieldStorageInfo> withSubfield = List.of(
            new FieldStorageInfo("msg", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false, "keyword")
        );
        RexNode call = rexBuilder.makeCall(
            SqlStdOperatorTable.NOT_EQUALS,
            rexBuilder.makeInputRef(varchar, 0),
            rexBuilder.makeLiteral("hello")
        );
        QueryBuilder qb = serializer.buildQueryBuilder((org.apache.calcite.rex.RexCall) call, withSubfield);
        BoolQueryBuilder bool = (BoolQueryBuilder) qb;
        TermQueryBuilder term = (TermQueryBuilder) bool.mustNot().get(0);
        assertEquals("msg.keyword", term.fieldName());
        // exists on the field itself, as vanilla does; the term targets the keyword multifield.
        assertEquals("msg", ((ExistsQueryBuilder) bool.filter().get(0)).fieldName());
    }

    public void testReversedOperandOrder() {
        RexNode call = rexBuilder.makeCall(
            SqlStdOperatorTable.NOT_EQUALS,
            rexBuilder.makeLiteral("active"),
            rexBuilder.makeInputRef(varchar, 0)
        );
        QueryBuilder qb = serializer.buildQueryBuilder((org.apache.calcite.rex.RexCall) call, FIELD_STORAGE);
        BoolQueryBuilder bool = (BoolQueryBuilder) qb;
        TermQueryBuilder term = (TermQueryBuilder) bool.mustNot().get(0);
        assertEquals("status", term.fieldName());
        assertEquals("active", term.value());
    }
}
