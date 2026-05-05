/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.spi.FilterTreeShape;

import java.util.List;

/**
 * Unit tests for {@link FilterTreeShapeDeriver}.
 */
public class FilterTreeShapeDeriverTests extends BasePlannerRulesTests {

    private static final String DRIVING = "datafusion";
    private static final String ACCEPTING = "lucene";

    public void testNoDelegation() {
        // Single native predicate — no delegation
        RexNode nativePred = annotated(DRIVING);
        OpenSearchFilter filter = buildFilter(nativePred);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING);
        assertEquals("No delegation should return PLAIN", FilterTreeShape.PLAIN, shape);
    }

    public void testSingleDelegatedPredicate() {
        // Single delegated predicate under implicit AND
        RexNode delegated = annotated(ACCEPTING);
        RexNode nativePred = annotated(DRIVING);
        RexNode andNode = rexBuilder.makeCall(SqlStdOperatorTable.AND, nativePred, delegated);
        OpenSearchFilter filter = buildFilter(andNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING);
        assertEquals(FilterTreeShape.SINGLE_AND, shape);
    }

    public void testMultipleDelegatedUnderAnd() {
        // Multiple delegated predicates under AND — still SINGLE_AND
        RexNode delegated1 = annotated(ACCEPTING);
        RexNode delegated2 = annotated(ACCEPTING);
        RexNode nativePred = annotated(DRIVING);
        RexNode andNode = rexBuilder.makeCall(SqlStdOperatorTable.AND, nativePred, delegated1, delegated2);
        OpenSearchFilter filter = buildFilter(andNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING);
        assertEquals(FilterTreeShape.SINGLE_AND, shape);
    }

    public void testOrWithDelegatedAndNative() {
        // OR mixing delegated and native → MIXED_BOOLEAN
        RexNode delegated = annotated(ACCEPTING);
        RexNode nativePred = annotated(DRIVING);
        RexNode orNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, nativePred, delegated);
        OpenSearchFilter filter = buildFilter(orNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING);
        assertEquals(FilterTreeShape.MIXED_BOOLEAN, shape);
    }

    public void testNotWithDelegated() {
        // NOT wrapping delegated + native → MIXED_BOOLEAN
        RexNode delegated = annotated(ACCEPTING);
        RexNode nativePred = annotated(DRIVING);
        RexNode andNode = rexBuilder.makeCall(SqlStdOperatorTable.AND, nativePred, delegated);
        RexNode notNode = rexBuilder.makeCall(SqlStdOperatorTable.NOT, andNode);
        OpenSearchFilter filter = buildFilter(notNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING);
        assertEquals(FilterTreeShape.MIXED_BOOLEAN, shape);
    }

    public void testOrWithOnlyDelegated() {
        // OR with only delegated predicates (no driving backend) — SINGLE_AND (no mixing)
        RexNode delegated1 = annotated(ACCEPTING);
        RexNode delegated2 = annotated(ACCEPTING);
        RexNode orNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, delegated1, delegated2);
        RexNode nativePred = annotated(DRIVING);
        RexNode andNode = rexBuilder.makeCall(SqlStdOperatorTable.AND, nativePred, orNode);
        OpenSearchFilter filter = buildFilter(andNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING);
        assertEquals(FilterTreeShape.SINGLE_AND, shape);
    }

    // ---- Helpers ----

    private AnnotatedPredicate annotated(String backendId) {
        RelDataType boolType = typeFactory.createJavaType(boolean.class);
        RexNode literal = rexBuilder.makeLiteral(true);
        return new AnnotatedPredicate(boolType, literal, List.of(backendId), 0);
    }

    private OpenSearchFilter buildFilter(RexNode condition) {
        return new OpenSearchFilter(
            cluster,
            RelTraitSet.createEmpty(),
            stubScan(mockTable("test_index", "col")),
            condition,
            List.of(DRIVING)
        );
    }
}
