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

        // 30 predicates with limit 10 — should fail
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FilterPredicateGuard.validate(bigOr, 10));
        assertTrue(e.getMessage().contains("30 predicates"));
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
