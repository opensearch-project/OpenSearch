/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Pre-isthmus pass that rewrites untyped {@code NULL} literals
 * ({@code RexLiteral} with {@link SqlTypeName#NULL}) to typed null literals
 * inferred from their enclosing operator.
 *
 * <p>Calcite emits an untyped NULL for the implicit {@code ELSE} arm of
 * {@code CASE WHEN cond THEN val END}, which is exactly the shape PPL
 * {@code count(eval(predicate))} lowers to:
 *
 * <pre>{@code
 *   COUNT(CASE WHEN predicate THEN <projected> END)   // ELSE is implicit NULL, type=NULL
 * }</pre>
 *
 * <p>Isthmus' {@code TypeConverter.toSubstrait} rejects {@link SqlTypeName#NULL}
 * with {@code "Unable to convert the type NULL"}. The CASE call's resolved
 * return type already carries the right widened type ({@code NULLABLE BIGINT}
 * for the count-eval shape, etc), so we substitute that.
 *
 * <p>Scope: only CASE call operands are rewritten today. Other untyped-NULL
 * sites (function arguments, comparison RHS, etc) are rare in PPL-generated
 * plans and would need per-operator type-inference to do safely; defer until
 * a concrete test surfaces one.
 *
 * @opensearch.internal
 */
final class UntypedNullPreprocessor {

    private UntypedNullPreprocessor() {}

    /**
     * Walk the RelNode tree, applying the rewrite to every node's expressions.
     * Returns a new tree if any rewrite occurred, otherwise the input unchanged.
     */
    static RelNode rewrite(RelNode root) {
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                return visited.accept(new CaseUntypedNullShuttle(visited.getCluster().getRexBuilder()));
            }
        });
    }

    /**
     * Per-node rex shuttle: for every {@code CASE} call encountered, rewrite any
     * {@link SqlTypeName#NULL}-typed literal operand into a typed null literal
     * matching the CASE's resolved return type.
     */
    private static final class CaseUntypedNullShuttle extends RexShuttle {

        private final RexBuilder rexBuilder;

        CaseUntypedNullShuttle(RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            // Recurse first so nested CASE calls are rewritten bottom-up — each inner CASE is
            // resolved against its own return type, so by the time we look at the outer one,
            // every operand is already typed.
            RexCall recursed = (RexCall) super.visitCall(call);
            if (recursed.getKind() != SqlKind.CASE) {
                return recursed;
            }
            List<RexNode> operands = recursed.getOperands();
            List<RexNode> rewritten = new ArrayList<>(operands.size());
            boolean changed = false;
            for (int i = 0; i < operands.size(); i++) {
                RexNode op = operands.get(i);
                if (isCaseValueOperand(i, operands.size()) && isUntypedNullLiteral(op)) {
                    rewritten.add(rexBuilder.makeNullLiteral(recursed.getType()));
                    changed = true;
                } else {
                    rewritten.add(op);
                }
            }
            return changed ? recursed.clone(recursed.getType(), rewritten) : recursed;
        }

        /**
         * Calcite's CASE operand layout is {@code [cond1, val1, cond2, val2, …, condN, valN, else]}.
         * Conditions sit at even indices except the last operand (the ELSE), which is always
         * a value. Returns true for value operands (the THEN/ELSE arms).
         */
        private static boolean isCaseValueOperand(int index, int total) {
            return (index % 2 == 1) || (index == total - 1);
        }

        private static boolean isUntypedNullLiteral(RexNode node) {
            if (!(node instanceof RexLiteral lit)) {
                return false;
            }
            return lit.isNull() && lit.getType().getSqlTypeName() == SqlTypeName.NULL;
        }
    }
}
