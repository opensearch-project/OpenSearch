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
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link LikeSerializer}: prefix-vs-wildcard dispatch, ILIKE case-insensitivity,
 * SQL→Lucene wildcard conversion, and refusal to delegate negated LIKE.
 */
public class LikeSerializerTests extends OpenSearchTestCase {

    private final LikeSerializer serializer = new LikeSerializer();
    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelDataType varchar;

    private static final List<FieldStorageInfo> FIELD_STORAGE = List.of(
        new FieldStorageInfo("str0", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }

    private RexNode like(String pattern) {
        return rexBuilder.makeCall(SqlStdOperatorTable.LIKE, rexBuilder.makeInputRef(varchar, 0), rexBuilder.makeLiteral(pattern));
    }

    private RexNode ilike(String pattern) {
        return rexBuilder.makeCall(SqlLibraryOperators.ILIKE, rexBuilder.makeInputRef(varchar, 0), rexBuilder.makeLiteral(pattern));
    }

    private QueryBuilder build(RexNode call) {
        return serializer.buildQueryBuilder((org.apache.calcite.rex.RexCall) call, FIELD_STORAGE);
    }

    public void testPrefixPatternBuildsPrefixQuery() {
        QueryBuilder qb = build(like("OFF%"));
        assertTrue("prefix-shaped LIKE → PrefixQuery", qb instanceof PrefixQueryBuilder);
        PrefixQueryBuilder p = (PrefixQueryBuilder) qb;
        assertEquals("str0", p.fieldName());
        assertEquals("OFF", p.value());
        assertFalse("SQL LIKE is case-sensitive", p.caseInsensitive());
    }

    public void testContainsPatternBuildsWildcardQuery() {
        QueryBuilder qb = build(like("%foo%"));
        assertTrue("contains-shaped LIKE → WildcardQuery", qb instanceof WildcardQueryBuilder);
        WildcardQueryBuilder w = (WildcardQueryBuilder) qb;
        assertEquals("str0", w.fieldName());
        assertEquals("*foo*", w.value());
    }

    public void testUnderscoreBuildsWildcardQuery() {
        WildcardQueryBuilder w = (WildcardQueryBuilder) build(like("on_"));
        assertEquals("on?", w.value());
    }

    public void testIlikeIsCaseInsensitive() {
        PrefixQueryBuilder p = (PrefixQueryBuilder) build(ilike("furn%"));
        assertTrue("ILIKE → caseInsensitive", p.caseInsensitive());
        assertEquals("furn", p.value());

        WildcardQueryBuilder w = (WildcardQueryBuilder) build(ilike("%mid%"));
        assertTrue(w.caseInsensitive());
    }

    public void testMatchAllPatternIsEmptyPrefix() {
        // LIKE '%' matches every (non-null) value → empty-prefix PrefixQuery, a valid superset.
        PrefixQueryBuilder p = (PrefixQueryBuilder) build(like("%"));
        assertEquals("", p.value());
    }

    public void testEscapedWildcardTreatedAsLiteral() {
        // '50\%%' → literal "50%" prefix + trailing wildcard → PrefixQuery("50%")
        PrefixQueryBuilder p = (PrefixQueryBuilder) build(like("50\\%%"));
        assertEquals("50%", p.value());
    }

    public void testLuceneMetacharsEscapedInWildcard() {
        // a real '*' in the LIKE literal must be escaped so Lucene matches it literally
        WildcardQueryBuilder w = (WildcardQueryBuilder) build(like("a*b%c"));
        assertEquals("a\\*b*c", w.value());
    }

    /** Calcite forbids a negated LIKE as a RexCall, so NOT LIKE arrives as NOT(LIKE(...)) — the
     *  serializer only ever sees positive LIKE. Pins that representation contract. */
    public void testNegatedLikeOperatorCannotBeConstructedAsRexCall() {
        expectThrows(
            AssertionError.class,
            () -> rexBuilder.makeCall(SqlStdOperatorTable.NOT_LIKE, rexBuilder.makeInputRef(varchar, 0), rexBuilder.makeLiteral("foo%"))
        );
    }
}
