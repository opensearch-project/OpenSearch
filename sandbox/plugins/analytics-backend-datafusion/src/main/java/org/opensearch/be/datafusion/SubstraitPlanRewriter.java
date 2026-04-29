/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.substrait.expression.Expression;
import io.substrait.expression.ImmutableExpression;
import io.substrait.plan.Plan;
import io.substrait.relation.ExpressionCopyOnWriteVisitor;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.util.EmptyVisitationContext;

/**
 * Single-pass post-processor for Substrait plans before serialization to protobuf.
 *
 * <p>Applies two kinds of rewrites:
 * <ul>
 *   <li><b>Rel-level</b> — structural changes like table name stripping, handled by
 *       {@link RelCopyOnWriteVisitor} overrides.</li>
 *   <li><b>Expression-level</b> — literal/type fixes handled by
 *       {@link ExpressionCopyOnWriteVisitor} overrides. Adding a new expression rewrite
 *       only requires overriding the corresponding {@code visit} method.</li>
 * </ul>
 *
 * @opensearch.internal
 */
class SubstraitPlanRewriter {

    private SubstraitPlanRewriter() {}

    static Plan rewrite(Plan plan) {
        PlanRelVisitor visitor = new PlanRelVisitor();

        List<Plan.Root> roots = new ArrayList<>();
        for (Plan.Root root : plan.getRoots()) {
            Optional<Rel> modified = root.getInput().accept(visitor, null);
            roots.add(modified.isPresent() ? Plan.Root.builder().from(root).input(modified.get()).build() : root);
        }
        return Plan.builder().from(plan).roots(roots).build();
    }

    /**
     * Rel-level visitor. Handles structural rewrites (table name stripping) and delegates
     * expression rewrites to {@link PlanExpressionVisitor}.
     */
    private static class PlanRelVisitor extends RelCopyOnWriteVisitor<RuntimeException> {

        private final PlanExpressionVisitor expressionVisitor = new PlanExpressionVisitor(this);

        // Rewrite expressions inside filter conditions
        @Override
        public Optional<Rel> visit(io.substrait.relation.Filter filter, EmptyVisitationContext ctx) {
            Optional<Rel> newInput = filter.getInput().accept(this, ctx);
            Optional<Expression> rewritten = filter.getCondition().accept(expressionVisitor, ctx);
            if (newInput.isEmpty() && rewritten.isEmpty()) return Optional.empty();
            return Optional.of(
                io.substrait.relation.Filter.builder()
                    .from(filter)
                    .input(newInput.orElse(filter.getInput()))
                    .condition(rewritten.orElse(filter.getCondition()))
                    .build()
            );
        }
    }

    /**
     * Expression-level visitor. Override a {@code visit} method to add a new rewrite.
     * The base class handles recursion into function arguments, casts, if-then, etc.
     */
    private static class PlanExpressionVisitor extends ExpressionCopyOnWriteVisitor<RuntimeException> {

        PlanExpressionVisitor(PlanRelVisitor relVisitor) {
            super(relVisitor);
        }

        // Isthmus hardcodes timestamp literals to precision 6 (microseconds).
        // Parquet stores Timestamp(MILLISECOND), so convert to precision 3.
        @Override
        public Optional<Expression> visit(Expression.PrecisionTimestampLiteral pts, EmptyVisitationContext ctx) {
            if (pts.precision() != 3) {
                return Optional.of(
                    ImmutableExpression.PrecisionTimestampLiteral.builder()
                        .value(toMillis(pts.value(), pts.precision()))
                        .precision(3)
                        .nullable(pts.nullable())
                        .build()
                );
            }
            return Optional.empty();
        }
    }

    private static long toMillis(long value, int precision) {
        return switch (precision) {
            case 0 -> value * 1000L;
            case 6 -> TimeUnit.MICROSECONDS.toMillis(value);
            case 9 -> TimeUnit.NANOSECONDS.toMillis(value);
            default -> throw new IllegalArgumentException(
                "Unsupported timestamp precision: " + precision + ". Expected 0 (seconds), 6 (micros), or 9 (nanos)."
            );
        };
    }
}
