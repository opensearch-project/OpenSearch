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
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.util.EmptyVisitationContext;

/**
 * POJO-layer Substrait plan rewriter. Runs before the plan is serialized to protobuf
 * and patches expressions that isthmus emits incorrectly or that DataFusion's
 * substrait consumer cannot handle. Today it covers two cases:
 * <ul>
 *   <li>{@code PrecisionTimestampLiteral} at precision 9 (ns) is rescaled to precision 3
 *       (ms) — DataFusion's substrait consumer does not coerce ns→ms when comparing
 *       against parquet's ms-stored {@code date} fields. Precision 6 (µs) is left intact
 *       so {@code cast('… .123456' AS TIMESTAMP)} preserves microsecond fractions.</li>
 *   <li>{@code VarCharLiteral} → {@code StrLiteral} — DataFusion 53.1.0's consumer
 *       has no VarCharLiteral arm; the two literal types are byte-identical.</li>
 * </ul>
 *
 * <p>Sibling {@link SubstraitPlanProtoRewriter} runs after this class on the proto layer
 * for fixes that require setting fields the POJO API doesn't expose.
 *
 * <p>TODO: remove this class once both upstream gaps are closed — substrait-java
 * isthmus inspects the source precision when lowering timestamp literals, and
 * datafusion-substrait adds a VarCharLiteral arm to its consumer.
 *
 * @opensearch.internal
 */
class SubstraitPlanPojoRewriter {

    private SubstraitPlanPojoRewriter() {}

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

        // Rewrite expressions inside project expressions (where literals often appear)
        @Override
        public Optional<Rel> visit(Project project, EmptyVisitationContext ctx) {
            /**
             * TODO: it may be better to have this as a LiteralAdapter that the BackendPlanAdapter applies globally, instead of doing this per rel type.
             */
            Optional<Rel> newInput = project.getInput().accept(this, ctx);
            List<Expression> oldExpressions = project.getExpressions();
            List<Expression> newExpressions = null;
            boolean changed = false;
            for (int i = 0; i < oldExpressions.size(); i++) {
                Optional<Expression> rewritten = oldExpressions.get(i).accept(expressionVisitor, ctx);
                if (rewritten.isPresent()) {
                    if (newExpressions == null) {
                        newExpressions = new ArrayList<>(oldExpressions);
                    }
                    newExpressions.set(i, rewritten.get());
                    changed = true;
                }
            }
            if (newInput.isEmpty() && !changed) return Optional.empty();
            return Optional.of(
                Project.builder()
                    .from(project)
                    .input(newInput.orElse(project.getInput()))
                    .expressions(newExpressions != null ? newExpressions : oldExpressions)
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

        // Only coerce precision 9 (ns) → 3 (ms): DataFusion's substrait consumer can't
        // compare a ns literal against parquet's ms-stored Timestamp field. Precision 6
        // (µs) literals pass through so cast('…123456' AS TIMESTAMP) keeps the µs fraction.
        @Override
        public Optional<Expression> visit(Expression.PrecisionTimestampLiteral pts, EmptyVisitationContext ctx) {
            if (pts.precision() == 9) {
                return Optional.of(
                    ImmutableExpression.PrecisionTimestampLiteral.builder()
                        .value(TimeUnit.NANOSECONDS.toMillis(pts.value()))
                        .precision(3)
                        .nullable(pts.nullable())
                        .build()
                );
            }
            return Optional.empty();
        }

        // Convert VarCharLiteral to StrLiteral — DataFusion 53.1.0's Substrait consumer
        // does not support VarChar literals (errors with "Unsupported literal_type: Some(VarChar(...))").
        // VARCHAR and STRING are semantically identical for literals (both represent UTF-8 strings);
        // only the type system distinguishes them via length constraints. Rewriting VarCharLiteral →
        // StrLiteral preserves value and nullability while making the plan consumable by DataFusion.
        // Affects: AddTotals label parameters, Chart/Trendline string options, CASE string constants.
        @Override
        public Optional<Expression> visit(Expression.VarCharLiteral vcl, EmptyVisitationContext ctx) {
            return Optional.of(ImmutableExpression.StrLiteral.builder().value(vcl.value()).nullable(vcl.nullable()).build());
        }
    }

}
