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
     * Guard-level stack safety: {@link FilterPredicateGuard} must count a very deep tree without
     * recursing on the JVM call stack. This asserts the guard's OWN traversal is iterative — it does
     * NOT claim the system accepts trees this deep. In production, depth is bounded far below this by
     * the PPL/SQL parser ({@code plugins.query.max_expression_depth}) and the DSL XContent nesting
     * limit, and Calcite would overflow building such a tree before the guard ever ran. This test
     * builds the RexNode directly to exercise the guard in isolation.
     */
    public void testDeeplyNestedConditionRejectedWithoutStackOverflow() {
        RexNode deep = buildDeepAndChain(200_000);
        // limit 500 (the production default) — the deep chain has 200k leaves, so the guard rejects it.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FilterPredicateGuard.validate(deep, 500));
        assertTrue(e.getMessage().contains("more than 500 predicates"));
        assertTrue(e.getMessage().contains("maximum allowed [500]"));
    }

    /**
     * Guard-level stack safety for the low-leaf-count case: a tree that is deep but has few leaves
     * must still be walked iteratively (no StackOverflowError) and pass the count check. Again this
     * exercises the guard in isolation; it is not a statement that the system accepts 200k-deep
     * conditions (it does not — see the class Javadoc on upstream depth bounds).
     */
    public void testDeeplyNestedConditionWithinLimitDoesNotOverflow() {
        // 200k-deep chain of NOT(...) around a single comparison: exactly 1 leaf predicate, but
        // 200k levels of nesting. A recursive count would overflow; the iterative count must not.
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

    /** Boundary: exactly maxCount leaves passes; maxCount + 1 is rejected. */
    public void testBoundaryExactlyAtLimitPassesOverByOneRejected() {
        List<RexNode> atLimit = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            atLimit.add(makeComparison());
        }
        // Exactly 10 leaves, limit 10 — passes (validate rejects only when count > maxCount).
        FilterPredicateGuard.validate(buildFlatOr(atLimit), 10);

        List<RexNode> overByOne = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            overByOne.add(makeComparison());
        }
        // 11 leaves, limit 10 — rejected.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FilterPredicateGuard.validate(buildFlatOr(overByOne), 10)
        );
        assertTrue(e.getMessage().contains("maximum allowed [10]"));
    }

    /** A negative limit disables the guard, exactly like 0. */
    public void testNegativeLimitDisablesGuard() {
        List<RexNode> predicates = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            predicates.add(makeComparison());
        }
        FilterPredicateGuard.validate(buildFlatOr(predicates), -1); // no throw
    }

    /** A bare leaf predicate at the top level (not wrapped in a connective) counts as one. */
    public void testBareLeafCountsAsOne() {
        assertEquals("bare comparison is one leaf", 1, FilterPredicateGuard.countLeaves(makeComparison()));
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
