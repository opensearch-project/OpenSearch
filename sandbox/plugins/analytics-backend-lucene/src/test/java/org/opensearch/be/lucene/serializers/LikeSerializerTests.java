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
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class LikeSerializerTests extends OpenSearchTestCase {

    private final LikeSerializer serializer = new LikeSerializer();
    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelDataType varchar;

    private static final List<FieldStorageInfo> FIELD_STORAGE = List.of(
        new FieldStorageInfo("name", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }

    public void testLikeProducesWildcardQuery() {
        RexNode call = rexBuilder.makeCall(
            SqlLibraryOperators.ILIKE,
            rexBuilder.makeInputRef(varchar, 0),
            rexBuilder.makeLiteral("%apple%")
        );
        QueryBuilder qb = serializer.buildQueryBuilder((RexCall) call, FIELD_STORAGE);
        assertTrue(qb instanceof WildcardQueryBuilder);
        WildcardQueryBuilder wqb = (WildcardQueryBuilder) qb;
        assertEquals("name", wqb.fieldName());
        assertEquals("*apple*", wqb.value());
    }

    public void testLikePrefixPattern() {
        RexNode call = rexBuilder.makeCall(
            SqlLibraryOperators.ILIKE,
            rexBuilder.makeInputRef(varchar, 0),
            rexBuilder.makeLiteral("prefix%")
        );
        QueryBuilder qb = serializer.buildQueryBuilder((RexCall) call, FIELD_STORAGE);
        WildcardQueryBuilder wqb = (WildcardQueryBuilder) qb;
        assertEquals("prefix*", wqb.value());
    }

    public void testLikeSingleCharWildcard() {
        RexNode call = rexBuilder.makeCall(SqlLibraryOperators.ILIKE, rexBuilder.makeInputRef(varchar, 0), rexBuilder.makeLiteral("te_t"));
        QueryBuilder qb = serializer.buildQueryBuilder((RexCall) call, FIELD_STORAGE);
        WildcardQueryBuilder wqb = (WildcardQueryBuilder) qb;
        assertEquals("te?t", wqb.value());
    }

    public void testPatternConversion() {
        assertEquals("*", LikeSerializer.convertSqlLikeToLuceneWildcard("%"));
        assertEquals("?", LikeSerializer.convertSqlLikeToLuceneWildcard("_"));
        assertEquals("*foo?bar*", LikeSerializer.convertSqlLikeToLuceneWildcard("%foo_bar%"));
        assertEquals("literal", LikeSerializer.convertSqlLikeToLuceneWildcard("literal"));
        assertEquals("%", LikeSerializer.convertSqlLikeToLuceneWildcard("\\%"));
        assertEquals("_", LikeSerializer.convertSqlLikeToLuceneWildcard("\\_"));
    }
}
