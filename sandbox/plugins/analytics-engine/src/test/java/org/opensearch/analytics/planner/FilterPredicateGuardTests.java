/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link FilterPredicateGuard}.
 */
public class FilterPredicateGuardTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);

    public void testSinglePredicatePassesCountLimit() {
        RexNode predicate = makeComparison();
        // 1 predicate, limit 200 — should pass
        FilterPredicateGuard.validate(predicate, 200);
    }

    public void testFlatOrExceedsCountLimit() {
        // Build: a=1 OR b=2 OR c=3 OR ... (30 predicates, flat)
        List<RexNode> predicates = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            predicates.add(makeComparison());
        }
        RexNode bigOr = buildFlatOr(predicates);

        // 30 predicates with limit 10 — should fail. The guard short-circuits, so the message
        // reports "more than [limit]" rather than the exact leaf count (which it never computes).
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FilterPredicateGuard.validate(bigOr, 10));
        assertTrue(e.getMessage().contains("more than 10 predicates"));
        assertTrue(e.getMessage().contains("maximum allowed [10]"));
    }

    public void testFlatOrPassesCountLimit() {
        List<RexNode> predicates = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            predicates.add(makeComparison());
        }
        RexNode bigOr = buildFlatOr(predicates);

        // 30 predicates with limit 200 — should pass
        FilterPredicateGuard.validate(bigOr, 200);
    }

    public void testDisabledGuardPassesEverything() {
        List<RexNode> predicates = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            predicates.add(makeComparison());
        }
        RexNode bigOr = buildFlatOr(predicates);

        // Limit 0 = disabled — should pass regardless of size
        FilterPredicateGuard.validate(bigOr, 0);
    }

    public void testCountMeasurement() {
        // flat OR with 5 predicates: count=5
        List<RexNode> predicates = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            predicates.add(makeComparison());
        }
        RexNode flatOr = buildFlatOr(predicates);
        assertEquals("leaf count", 5, FilterPredicateGuard.countLeaves(flatOr));
    }

    public void testNestedPredicatesCountedAcrossConnectives() {
        // AND(AND(AND(a=1, b=2), c=3), d=4) — 4 leaf predicates regardless of nesting shape
        RexNode inner = rexBuilder.makeCall(SqlStdOperatorTable.AND, makeComparison(), makeComparison());
        RexNode mid = rexBuilder.makeCall(SqlStdOperatorTable.AND, inner, makeComparison());
        RexNode outer = rexBuilder.makeCall(SqlStdOperatorTable.AND, mid, makeComparison());

        assertEquals("leaf count", 4, FilterPredicateGuard.countLeaves(outer));
    }

    public void testNotDoesNotCountAsPredicate() {
        // NOT(a=1) — 1 leaf predicate; NOT itself doesn't count
        RexNode notNode = rexBuilder.makeCall(SqlStdOperatorTable.NOT, makeComparison());
        assertEquals("leaf count", 1, FilterPredicateGuard.countLeaves(notNode));
    }

    /**
     * A condition tree deep enough to overflow a recursive walk must be rejected with the
     * intended {@link IllegalArgumentException} (HTTP 400), NOT escape as a {@link StackOverflowError}.
     * This is the core regression: the guard's own traversal must be bounded so pathological input
     * cannot defeat the guard before it runs. A right-leaning AND chain of 200k nodes is far past
     * the default JVM recursion limit for this walk.
     */
    public void testDeeplyNestedConditionRejectedWithoutStackOverflow() {
        RexNode deep = buildDeepAndChain(200_000);
        // limit 500 (the production default) — the deep chain has 200k leaves, so it must be rejected.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FilterPredicateGuard.validate(deep, 500));
        assertTrue(e.getMessage().contains("more than 500 predicates"));
        assertTrue(e.getMessage().contains("maximum allowed [500]"));
    }

    /**
     * A deep tree that fits under the limit must still be walked without recursing into a
     * StackOverflowError. Depth here (with only a handful of leaves) exceeds a naive recursive
     * walk's safe depth, proving traversal depth is decoupled from the JVM call stack.
     */
    public void testDeeplyNestedConditionWithinLimitDoesNotOverflow() {
        // 200k-deep chain of NOT(...) around a single comparison: exactly 1 leaf predicate, but
        // 200k levels of nesting. A recursive walk would overflow; the iterative walk must not.
        RexNode deepButOneLeaf = makeComparison();
        for (int i = 0; i < 200_000; i++) {
            deepButOneLeaf = rexBuilder.makeCall(SqlStdOperatorTable.NOT, deepButOneLeaf);
        }
        // 1 leaf, limit 500 — passes without throwing (and without StackOverflowError).
        FilterPredicateGuard.validate(deepButOneLeaf, 500);
        assertEquals("leaf count", 1, FilterPredicateGuard.countLeaves(deepButOneLeaf));
    }

    /**
     * The disabled guard (limit 0) must return immediately without walking the tree at all, so an
     * arbitrarily deep condition can't overflow when the guard is turned off.
     */
    public void testDisabledGuardSkipsDeepTreeEntirely() {
        RexNode deep = buildDeepAndChain(200_000);
        FilterPredicateGuard.validate(deep, 0); // no throw, no overflow
    }

    /**
     * {@code countLeavesUpTo} must stop counting once it reaches the limit, capping the work the
     * guard does on a hostile tree. A 50-leaf flat OR probed with limit 10 returns exactly 10.
     */
    public void testCountLeavesUpToShortCircuitsAtLimit() {
        List<RexNode> predicates = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            predicates.add(makeComparison());
        }
        RexNode bigOr = buildFlatOr(predicates);
        assertEquals("short-circuited count", 10, FilterPredicateGuard.countLeavesUpTo(bigOr, 10));
        // Full count still reachable via the unbounded entry point.
        assertEquals("full count", 50, FilterPredicateGuard.countLeaves(bigOr));
    }

    /** Right-leaning AND chain: AND(a, AND(a, AND(a, ...))) with {@code depth} leaf comparisons. */
    private RexNode buildDeepAndChain(int depth) {
        RexNode node = makeComparison();
        for (int i = 1; i < depth; i++) {
            node = rexBuilder.makeCall(SqlStdOperatorTable.AND, makeComparison(), node);
        }
        return node;
    }

    private RexNode makeComparison() {
        return rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeExactLiteral(BigDecimal.ONE)
        );
    }

    private RexNode buildFlatOr(List<RexNode> operands) {
        if (operands.size() == 1) return operands.get(0);
        return rexBuilder.makeCall(SqlStdOperatorTable.OR, operands);
    }
}
