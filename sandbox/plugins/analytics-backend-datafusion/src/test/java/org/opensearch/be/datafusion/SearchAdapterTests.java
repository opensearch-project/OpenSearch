/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for {@link SearchAdapter}. Calcite's {@code SEARCH(x, Sarg[...])} is a
 * compact, expanded form for {@code IN}-lists, {@code BETWEEN}, and unions of
 * ranges; DataFusion's substrait consumer doesn't recognize {@code Sarg} as a
 * literal, so the adapter expands it back into native comparison/OR trees
 * before the plan is serialized.
 */
public class SearchAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelDataType intType;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    }

    /** Builds SEARCH(x, Sarg[{1, 2, 3}]) — the IN-list shape. */
    private RexCall buildInListSearch() {
        RangeSet<BigDecimal> points = TreeRangeSet.create();
        points.add(Range.singleton(BigDecimal.valueOf(1)));
        points.add(Range.singleton(BigDecimal.valueOf(2)));
        points.add(Range.singleton(BigDecimal.valueOf(3)));
        Sarg<BigDecimal> sarg = Sarg.of(org.apache.calcite.rex.RexUnknownAs.UNKNOWN, ImmutableRangeSet.copyOf(points));
        RexNode xRef = rexBuilder.makeInputRef(intType, 0);
        RexNode sargLit = rexBuilder.makeSearchArgumentLiteral(sarg, intType);
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, List.of(xRef, sargLit));
    }

    /** Builds SEARCH(x, Sarg[[1..10]]) — the BETWEEN shape. */
    private RexCall buildBetweenSearch() {
        RangeSet<BigDecimal> rangeSet = TreeRangeSet.create();
        rangeSet.add(Range.closed(BigDecimal.valueOf(1), BigDecimal.valueOf(10)));
        Sarg<BigDecimal> sarg = Sarg.of(org.apache.calcite.rex.RexUnknownAs.UNKNOWN, ImmutableRangeSet.copyOf(rangeSet));
        RexNode xRef = rexBuilder.makeInputRef(intType, 0);
        RexNode sargLit = rexBuilder.makeSearchArgumentLiteral(sarg, intType);
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, List.of(xRef, sargLit));
    }

    /**
     * The core contract: SEARCH is expanded. The resulting RexNode must not
     * contain any Sarg literal or SEARCH call — it has to be something the
     * downstream substrait consumer knows.
     */
    public void testAdaptExpandsInListSearchAwayFromSearchOperator() {
        RexCall original = buildInListSearch();
        RexNode adapted = new SearchAdapter().adapt(original, List.of(), cluster);

        assertFalse("expansion must not leave a SEARCH call at the root", isSearch(adapted));
        assertTrue("expansion must not leave any nested SEARCH call", containsNoSearchOrSarg(adapted));
    }

    /**
     * BETWEEN-style Sargs expand to AND(ge, le). Same acceptance criterion as
     * the IN-list case: no SEARCH and no Sarg literals in the output.
     */
    public void testAdaptExpandsBetweenSearchAwayFromSearchOperator() {
        RexCall original = buildBetweenSearch();
        RexNode adapted = new SearchAdapter().adapt(original, List.of(), cluster);

        assertFalse(isSearch(adapted));
        assertTrue(containsNoSearchOrSarg(adapted));
    }

    /**
     * Non-SEARCH calls must pass through untouched — the adapter is a no-op for
     * anything that isn't SEARCH. Guards against collateral damage if the
     * adapter gets registered against a different ScalarFunction by mistake.
     */
    public void testAdaptPassesThroughNonSearchCall() {
        RexNode xRef = rexBuilder.makeInputRef(intType, 0);
        RexNode tenLit = rexBuilder.makeLiteral(10, intType, false);
        RexCall greaterThan = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, List.of(xRef, tenLit));

        RexNode adapted = new SearchAdapter().adapt(greaterThan, List.of(), cluster);

        assertSame("non-SEARCH input must pass through unmodified", greaterThan, adapted);
    }

    // ── helpers ────────────────────────────────────────────────────────────

    private static boolean isSearch(RexNode node) {
        return node instanceof RexCall call && call.getKind() == SqlKind.SEARCH;
    }

    /** Returns false if the tree still carries a SEARCH call or a Sarg literal at any depth. */
    private static boolean containsNoSearchOrSarg(RexNode node) {
        if (isSearch(node)) return false;
        if (node instanceof org.apache.calcite.rex.RexLiteral lit && lit.getValue() instanceof Sarg) {
            return false;
        }
        if (node instanceof RexCall call) {
            for (RexNode operand : call.getOperands()) {
                if (!containsNoSearchOrSarg(operand)) return false;
            }
        }
        return true;
    }
}
