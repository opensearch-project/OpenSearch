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

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING, false);
        assertEquals("No delegation should return PLAIN", FilterTreeShape.NO_DELEGATION, shape);
    }

    public void testSingleDelegatedPredicate() {
        // Single delegated predicate under implicit AND
        RexNode delegated = annotated(ACCEPTING);
        RexNode nativePred = annotated(DRIVING);
        RexNode andNode = rexBuilder.makeCall(SqlStdOperatorTable.AND, nativePred, delegated);
        OpenSearchFilter filter = buildFilter(andNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING, false);
        assertEquals(FilterTreeShape.CONJUNCTIVE, shape);
    }

    public void testMultipleDelegatedUnderAnd() {
        // Multiple delegated predicates under AND — still SINGLE_AND
        RexNode delegated1 = annotated(ACCEPTING);
        RexNode delegated2 = annotated(ACCEPTING);
        RexNode nativePred = annotated(DRIVING);
        RexNode andNode = rexBuilder.makeCall(SqlStdOperatorTable.AND, nativePred, delegated1, delegated2);
        OpenSearchFilter filter = buildFilter(andNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING, false);
        assertEquals(FilterTreeShape.CONJUNCTIVE, shape);
    }

    public void testOrWithDelegatedAndNative() {
        // OR mixing delegated and native → MIXED_BOOLEAN
        RexNode delegated = annotated(ACCEPTING);
        RexNode nativePred = annotated(DRIVING);
        RexNode orNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, nativePred, delegated);
        OpenSearchFilter filter = buildFilter(orNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING, false);
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, shape);
    }

    public void testNotWithDelegated() {
        // NOT wrapping delegated + native → MIXED_BOOLEAN
        RexNode delegated = annotated(ACCEPTING);
        RexNode nativePred = annotated(DRIVING);
        RexNode andNode = rexBuilder.makeCall(SqlStdOperatorTable.AND, nativePred, delegated);
        RexNode notNode = rexBuilder.makeCall(SqlStdOperatorTable.NOT, andNode);
        OpenSearchFilter filter = buildFilter(notNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING, false);
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, shape);
    }

    public void testOrWithOnlyDelegated() {
        // OR(delegated, delegated) — all same-backend, combined into one expression.
        // No tree evaluator needed post-combine.
        RexNode delegated1 = annotated(ACCEPTING);
        RexNode delegated2 = annotated(ACCEPTING);
        RexNode orNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, delegated1, delegated2);
        RexNode nativePred = annotated(DRIVING);
        RexNode andNode = rexBuilder.makeCall(SqlStdOperatorTable.AND, nativePred, orNode);
        OpenSearchFilter filter = buildFilter(andNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING, false);
        assertEquals(FilterTreeShape.CONJUNCTIVE, shape);
    }

    public void testBareNotOfDelegated() {
        // NOT(delegated) — single correctness-delegated predicate, combined into one
        // expression (BoolQuery with must_not). No tree evaluator needed post-combine.
        RexNode delegated = annotated(ACCEPTING);
        RexNode notNode = rexBuilder.makeCall(SqlStdOperatorTable.NOT, delegated);
        OpenSearchFilter filter = buildFilter(notNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING, false);
        assertEquals(FilterTreeShape.CONJUNCTIVE, shape);
    }

    // ---- Helpers ----

    private AnnotatedPredicate annotated(String backendId) {
        RelDataType boolType = typeFactory.createJavaType(boolean.class);
        RexNode literal = rexBuilder.makeLiteral(true);
        return new AnnotatedPredicate(boolType, literal, List.of(backendId), 0);
    }

    /** Creates a dual-viable predicate narrowed to the driving backend with perf-delegation to accepting. */
    private AnnotatedPredicate perfDelegated() {
        RelDataType boolType = typeFactory.createJavaType(boolean.class);
        RexNode literal = rexBuilder.makeLiteral(true);
        AnnotatedPredicate dualViable = new AnnotatedPredicate(boolType, literal, List.of(DRIVING, ACCEPTING), 0);
        return (AnnotatedPredicate) dualViable.narrowTo(DRIVING);
    }

    public void testOrWithCorrectnessAndPerfDelegated() {
        // OR(correctness-delegated, perf-delegated) — perf won't combine under OR → INTERLEAVED
        RexNode correctness = annotated(ACCEPTING);
        RexNode perf = perfDelegated();
        RexNode orNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, correctness, perf);
        OpenSearchFilter filter = buildFilter(orNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING, false);
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, shape);
    }

    public void testAndWithCorrectnessAndPerfDelegated() {
        // AND(correctness-delegated, perf-delegated) — perf combines under AND → CONJUNCTIVE
        RexNode correctness = annotated(ACCEPTING);
        RexNode perf = perfDelegated();
        RexNode andNode = rexBuilder.makeCall(SqlStdOperatorTable.AND, correctness, perf);
        OpenSearchFilter filter = buildFilter(andNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING, false);
        assertEquals(FilterTreeShape.CONJUNCTIVE, shape);
    }

    public void testNotOfPerfDelegated() {
        // NOT(perf-delegated) — perf under NOT won't combine → INTERLEAVED
        RexNode perf = perfDelegated();
        RexNode notNode = rexBuilder.makeCall(SqlStdOperatorTable.NOT, perf);
        OpenSearchFilter filter = buildFilter(notNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING, false);
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, shape);
    }

    // ---- fuse_dual_viable interaction ----

    /**
     * Same OR(correctness, perf) shape as {@link #testOrWithCorrectnessAndPerfDelegated} but
     * with {@code fuseDualViable=true}: combiner fuses both delegated children (correctness ∪
     * perf) into a single {@code delegated_predicate} placeholder wrapping the OR. The peer
     * evaluates the whole boolean (driver can't decompose OR into independently-evaluable
     * arms — {@code delegation_possible} only composes under AND). Post-combiner tree is a
     * bare delegated leaf — conjunctive from the data-node evaluator's perspective.
     */
    public void testOrWithCorrectnessAndPerfDelegated_fused_isConjunctive() {
        RexNode correctness = annotated(ACCEPTING);
        RexNode perf = perfDelegated();
        RexNode orNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, correctness, perf);
        OpenSearchFilter filter = buildFilter(orNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING, true);
        assertEquals(FilterTreeShape.CONJUNCTIVE, shape);
    }

    /**
     * NOT(perf-delegated) with {@code fuseDualViable=true}: combiner keeps the perf leaf in
     * the delegation pool under NOT, so the post-combiner tree is a single
     * {@code delegation_possible} wrapping the NOT — conjunctive.
     */
    public void testNotOfPerfDelegated_fused_isConjunctive() {
        RexNode perf = perfDelegated();
        RexNode notNode = rexBuilder.makeCall(SqlStdOperatorTable.NOT, perf);
        OpenSearchFilter filter = buildFilter(notNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING, true);
        assertEquals(FilterTreeShape.CONJUNCTIVE, shape);
    }

    /**
     * Even with {@code fuseDualViable=true}, OR(delegated, native-non-delegable) stays
     * INTERLEAVED — the native arm can't be folded into the peer's delegation no matter
     * what the carve-out policy is. Fusion only collapses the perf-vs-correctness axis.
     */
    public void testOrWithDelegatedAndNative_fused_stillInterleaved() {
        RexNode delegated = annotated(ACCEPTING);
        RexNode nativePred = annotated(DRIVING);
        RexNode orNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, delegated, nativePred);
        OpenSearchFilter filter = buildFilter(orNode);

        FilterTreeShape shape = FilterTreeShapeDeriver.derive(filter, DRIVING, true);
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, shape);
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
