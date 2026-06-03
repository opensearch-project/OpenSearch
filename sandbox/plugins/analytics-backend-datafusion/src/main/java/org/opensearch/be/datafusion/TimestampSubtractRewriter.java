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
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Rewrites {@code MINUS(timestamp, timestamp)} into the difference of the two operands'
 * epoch-second representations: {@code MINUS(to_unixtime(t1), to_unixtime(t2))}.
 *
 * <p><b>Why.</b> A raw {@code timestamp - timestamp} RexCall uses Calcite's built-in
 * {@code SqlStdOperatorTable.MINUS}, a binary operator that is <em>not</em> a named PPL
 * function, so it never reaches the per-function {@code ScalarFunctionAdapter} dispatch.
 * It flows straight into isthmus's {@code RexExpressionConverter}, which has no Substrait
 * mapping for {@code subtract(timestamp, timestamp)} and throws
 * {@code IllegalArgumentException: Unable to convert call -(precision_timestamp, precision_timestamp)}.
 *
 * <p>This shape is emitted by the SQL plugin's auto-date-histogram / {@code bin} path
 * (e.g. {@code DefaultBinHandler.createNumericDefaultBinning}) as
 * {@code MAX(ts) OVER () - MIN(ts) OVER ()} to size buckets from the data's time range.
 *
 * <p><b>Unit.</b> {@code to_unixtime} returns epoch seconds (BIGINT) — the same numeric
 * convention the binning handlers use elsewhere ({@code StandardTimeSpanHandler} works in
 * epoch seconds, scaling by 1000 only for sub-second units). The rewritten BIGINT
 * subtraction is natively Substrait-convertible, and the enclosing magnitude math
 * ({@code LOG10(range)}, {@code POWER(10, FLOOR(...))}) operates on a sane numeric range.
 *
 * <p>Mirrors the {@code to_unixtime}-difference idiom already used by
 * {@link TimestampDiffAdapter} (which builds {@code MINUS(to_unixtime(end), to_unixtime(start))}).
 *
 * <p><b>Identity preservation.</b> This rewriter must be a true no-op for any plan that does not
 * contain a {@code MINUS(timestamp, timestamp)}. It runs on every fragment via the shared
 * pre-Substrait pipeline (including the wrapper / two-phase-aggregate path through
 * {@code convertStandalone}), so it must never rebuild unrelated nodes: calling
 * {@code RelNode.accept(RexShuttle)} on an arbitrary node re-derives that node's expression /
 * aggCall types, which can flip a cached nullable {@code BIGINT} to {@code BIGINT NOT NULL} and
 * trip Calcite's validity assertions (observed on the grouped {@code APPROX_COUNT_DISTINCT}
 * path). To guarantee identity it first detects the target shape with read-only visitors and
 * only applies the rewriting {@link RexShuttle} to {@link RelNode}s whose own local expressions
 * actually contain it.
 *
 * @opensearch.internal
 */
final class TimestampSubtractRewriter {

    private TimestampSubtractRewriter() {}

    static RelNode rewrite(RelNode root) {
        // Preserve object identity for the common path. Even an "equivalent" Calcite rebuild can
        // perturb inferred nullability and trip Project/Aggregate validity assertions, so bail out
        // entirely when there is nothing to rewrite.
        if (!containsTimestampMinus(root)) {
            return root;
        }

        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);

                // Only run the RexShuttle against nodes whose own Rex expressions contain the target
                // shape. Do not touch unrelated Projects / Filters / Aggregates.
                if (!containsTimestampMinusInLocalExpressions(visited)) {
                    return visited;
                }

                RexShuttle shuttle = new RewriteShuttle(visited.getCluster().getRexBuilder());
                RelNode rewritten = visited.accept(shuttle);
                return rewritten == null ? visited : rewritten;
            }
        });
    }

    /** True if any node in the tree rooted at {@code root} holds a {@code MINUS(timestamp, timestamp)}. */
    private static boolean containsTimestampMinus(RelNode root) {
        class Finder extends RelVisitor {
            boolean found;

            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (found) {
                    return;
                }
                if (containsTimestampMinusInLocalExpressions(node)) {
                    found = true;
                    return;
                }
                super.visit(node, ordinal, parent);
            }
        }

        Finder finder = new Finder();
        finder.go(root);
        return finder.found;
    }

    /**
     * True if any of {@code node}'s own (non-recursive) Rex expressions is a timestamp-minus.
     *
     * <p>Calcite 1.41 removed {@code RelNode#getChildExps()}, so expressions are reached via
     * {@code RelNode#accept(RexShuttle)}. The detector shuttle below is strictly read-only — it
     * records a flag and returns every input node by identity. Because the shuttle changes nothing,
     * Calcite's {@code accept} returns the original {@link RelNode} object (no {@code copy} is
     * triggered), so detection does not rebuild the node or perturb its cached types.
     */
    private static boolean containsTimestampMinusInLocalExpressions(RelNode node) {
        boolean[] found = { false };
        node.accept(new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                if (isTimestampMinus(call)) {
                    found[0] = true;
                }
                // Returning the identical operands/call keeps this a no-op so Calcite does not
                // rebuild the enclosing RelNode.
                return super.visitCall(call);
            }
        });
        return found[0];
    }

    private static boolean isTimestampMinus(RexCall call) {
        List<RexNode> operands = call.getOperands();
        return call.getKind() == SqlKind.MINUS && operands.size() == 2 && isTimestamp(operands.get(0)) && isTimestamp(operands.get(1));
    }

    static boolean isTimestamp(RexNode node) {
        SqlTypeName type = node.getType().getSqlTypeName();
        return type == SqlTypeName.TIMESTAMP || type == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    private static final class RewriteShuttle extends RexShuttle {
        private final RexBuilder rexBuilder;

        RewriteShuttle(RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            boolean[] changed = { false };
            List<RexNode> newOperands = visitList(call.getOperands(), changed);
            List<RexNode> operands = changed[0] ? newOperands : call.getOperands();

            if (call.getKind() == SqlKind.MINUS && operands.size() == 2 && isTimestamp(operands.get(0)) && isTimestamp(operands.get(1))) {
                RexNode leftSeconds = rexBuilder.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, operands.get(0));
                RexNode rightSeconds = rexBuilder.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, operands.get(1));
                // BIGINT - BIGINT; preserve the original call's (nullable) type so the
                // enclosing Project/Filter rowType cache stays consistent.
                return rexBuilder.makeCast(call.getType(), rexBuilder.makeCall(SqlStdOperatorTable.MINUS, leftSeconds, rightSeconds), true);
            }

            if (!changed[0]) {
                return call;
            }
            return rexBuilder.makeCall(call.getType(), call.getOperator(), newOperands);
        }
    }
}
