/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for {@link SargSerializer}: IN-points → TermsQuery, intervals → RangeQuery (bounds and
 * range-unions), and refusal of NOT IN / all / none.
 */
public class SargSerializerTests extends OpenSearchTestCase {

    private final SargSerializer serializer = new SargSerializer();
    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelDataType keywordType;

    private static final List<FieldStorageInfo> FIELD_STORAGE = List.of(
        new FieldStorageInfo("str0", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        keywordType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }

    /** Build an NlsString the way Calcite stores a VARCHAR literal, without touching the avatica
     *  ByteString ctor path (which isn't on the test classpath). */
    private NlsString str(String s) {
        return (NlsString) ((org.apache.calcite.rex.RexLiteral) rexBuilder.makeLiteral(s)).getValue();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private QueryBuilder build(RangeSet rangeSet) {
        Sarg sarg = Sarg.of(RexUnknownAs.UNKNOWN, ImmutableRangeSet.copyOf(rangeSet));
        RexNode ref = rexBuilder.makeInputRef(keywordType, 0);
        RexNode sargLit = rexBuilder.makeSearchArgumentLiteral(sarg, keywordType);
        RexCall call = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, List.of(ref, sargLit));
        return serializer.buildQueryBuilder(call, FIELD_STORAGE);
    }

    public void testInListBuildsTermsQuery() {
        RangeSet<NlsString> points = TreeRangeSet.create();
        points.add(Range.singleton(str("apple")));
        points.add(Range.singleton(str("banana")));
        points.add(Range.singleton(str("cherry")));

        TermsQueryBuilder qb = (TermsQueryBuilder) build(points);
        assertEquals("str0", qb.fieldName());
        assertEquals(List.of("apple", "banana", "cherry"), qb.values());
    }

    public void testBetweenBuildsClosedRangeQuery() {
        RangeSet<NlsString> r = TreeRangeSet.create();
        r.add(Range.closed(str("a"), str("m")));

        RangeQueryBuilder qb = (RangeQueryBuilder) build(r);
        assertEquals("str0", qb.fieldName());
        assertEquals("a", qb.from());
        assertEquals("m", qb.to());
        assertTrue("BETWEEN is inclusive lower", qb.includeLower());
        assertTrue("BETWEEN is inclusive upper", qb.includeUpper());
    }

    public void testHalfOpenRangeBounds() {
        // [a, m) → from inclusive, to exclusive
        RangeSet<NlsString> r = TreeRangeSet.create();
        r.add(Range.closedOpen(str("a"), str("m")));
        RangeQueryBuilder qb = (RangeQueryBuilder) build(r);
        assertTrue(qb.includeLower());
        assertFalse(qb.includeUpper());
    }

    public void testGreaterThanBuildsOpenLowerRange() {
        // (k, +inf) → from exclusive, no upper bound
        RangeSet<NlsString> r = TreeRangeSet.create();
        r.add(Range.greaterThan(str("k")));
        RangeQueryBuilder qb = (RangeQueryBuilder) build(r);
        assertEquals("k", qb.from());
        assertFalse(qb.includeLower());
        assertNull("no upper bound", qb.to());
    }

    public void testRangeUnionBuildsShouldBool() {
        // [a, c] ∪ [x, z] → bool{ should range, should range }
        RangeSet<NlsString> r = TreeRangeSet.create();
        r.add(Range.closed(str("a"), str("c")));
        r.add(Range.closed(str("x"), str("z")));

        BoolQueryBuilder bool = (BoolQueryBuilder) build(r);
        assertEquals(2, bool.should().size());
        assertTrue(bool.should().get(0) instanceof RangeQueryBuilder);
        assertTrue(bool.should().get(1) instanceof RangeQueryBuilder);
    }

    public void testNotInRefused() {
        // (-inf, 1) ∪ (1, +inf) over INTEGER == NOT IN (1) → isComplementedPoints → refuse.
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RangeSet<BigDecimal> r = TreeRangeSet.create();
        r.add(Range.lessThan(BigDecimal.ONE));
        r.add(Range.greaterThan(BigDecimal.ONE));
        Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.UNKNOWN, ImmutableRangeSet.copyOf(r));
        RexNode ref = rexBuilder.makeInputRef(intType, 0);
        RexNode sargLit = rexBuilder.makeSearchArgumentLiteral(sarg, intType);
        RexCall call = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, List.of(ref, sargLit));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> serializer.buildQueryBuilder(call, FIELD_STORAGE));
        assertTrue(e.getMessage().contains("not delegatable"));
    }
}
