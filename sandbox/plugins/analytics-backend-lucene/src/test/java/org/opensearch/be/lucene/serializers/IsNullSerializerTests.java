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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class IsNullSerializerTests extends OpenSearchTestCase {

    private final IsNullSerializer isNullSerializer = new IsNullSerializer(false);
    private final IsNullSerializer isNotNullSerializer = new IsNullSerializer(true);
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

    public void testIsNullProducesMustNotExists() {
        RexNode call = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, rexBuilder.makeInputRef(varchar, 0));
        QueryBuilder qb = isNullSerializer.buildQueryBuilder((RexCall) call, FIELD_STORAGE);
        assertTrue(qb instanceof BoolQueryBuilder);
        BoolQueryBuilder bool = (BoolQueryBuilder) qb;
        assertEquals(1, bool.mustNot().size());
        assertTrue(bool.mustNot().get(0) instanceof ExistsQueryBuilder);
        assertEquals("status", ((ExistsQueryBuilder) bool.mustNot().get(0)).fieldName());
    }

    public void testIsNotNullProducesExists() {
        RexNode call = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, rexBuilder.makeInputRef(varchar, 0));
        QueryBuilder qb = isNotNullSerializer.buildQueryBuilder((RexCall) call, FIELD_STORAGE);
        assertTrue(qb instanceof ExistsQueryBuilder);
        assertEquals("status", ((ExistsQueryBuilder) qb).fieldName());
    }

    public void testRoutesToExactMatchSubfield() {
        List<FieldStorageInfo> withSubfield = List.of(
            new FieldStorageInfo("msg", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false, "keyword")
        );
        RexNode call = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, rexBuilder.makeInputRef(varchar, 0));
        QueryBuilder qb = isNotNullSerializer.buildQueryBuilder((RexCall) call, withSubfield);
        assertTrue(qb instanceof ExistsQueryBuilder);
        assertEquals("msg.keyword", ((ExistsQueryBuilder) qb).fieldName());
    }
}
