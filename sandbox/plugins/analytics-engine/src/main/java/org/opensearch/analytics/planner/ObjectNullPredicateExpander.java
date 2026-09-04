/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.spi.MakeStructFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Expands {@code IS NULL} / {@code IS NOT NULL} over a materialized {@code object} into the
 * equivalent test over its leaf columns.
 *
 * <pre>
 * IS NULL(make_struct('name', $1, 'env', $2))      ->  $1 IS NULL AND $2 IS NULL
 * IS NOT NULL(make_struct('name', $1, 'env', $2))  ->  $1 IS NOT NULL OR $2 IS NOT NULL
 * </pre>
 *
 * <p><b>Why.</b> {@code named_struct} builds its {@code StructArray} with no validity buffer, so a
 * materialized object is never null whatever its contents. Left alone, {@code isnull} is always
 * false and {@code isnotnull} always true — a silent no-op that also contradicts what the same row
 * renders as, since {@code ArrowValues.structToMap} returns null once every child is null. Filtering
 * on a leaf gives the right answer, which makes the object form easy to miss.
 *
 * <p>Correct because a struct is null exactly when it has no populated field, which is the same
 * condition {@code structToMap} uses to render null. A nested sub-object contributes its own leaves,
 * so the expansion reaches any depth.
 *
 * <p>Rewriting the predicate rather than the value is deliberate: the value cannot be made null
 * today. That needs a struct-typed NULL literal, which DataFusion rejects
 * ({@code "Unsupported CAST from Struct(..)"}) — the same gap that rules out
 * {@code Expression.NestedStruct}. Storing objects as native Parquet structs would fix this class of
 * problem at the source, since an optional group carries its own definition level; see the TODO on
 * {@link ObjectStructMaterializer}. This pass becomes unnecessary then.
 *
 * <p>Still wrong until then: operators that consult null-ness without going through these two, most
 * notably {@code coalesce(obj, x)} / {@code ifnull}, which never fall through to the default.
 *
 * <p>Runs after {@code pushdownRules}, which is what makes the expansion possible —
 * {@code FILTER_PROJECT_TRANSPOSE} has by then inlined the {@code make_struct} into the predicate, so
 * the leaf references are in scope. It is also a performance win: the expanded leaf predicates push
 * down to the metadata driver, where the struct form could only run on DataFusion.
 *
 * @opensearch.internal
 */
public final class ObjectNullPredicateExpander {

    private ObjectNullPredicateExpander() {}

    /**
     * @return the rewritten plan, or {@link Optional#empty()} when no null test over an object was
     *         found (callers keep the original plan)
     */
    public static Optional<RelNode> rewrite(RelNode root) {
        Expander expander = new Expander();
        RelNode rewritten = root.accept(expander);
        return expander.changed ? Optional.of(rewritten) : Optional.empty();
    }

    private static final class Expander extends RelShuttleImpl {

        private boolean changed = false;

        @Override
        public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            return visited.accept(new NullTestShuttle(visited.getCluster().getRexBuilder(), this));
        }

        @Override
        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            RelNode visited = super.visitChild(parent, i, child);
            return visited.accept(new NullTestShuttle(visited.getCluster().getRexBuilder(), this));
        }
    }

    private static final class NullTestShuttle extends RexShuttle {

        private final RexBuilder rexBuilder;
        private final Expander owner;

        NullTestShuttle(RexBuilder rexBuilder, Expander owner) {
            this.rexBuilder = rexBuilder;
            this.owner = owner;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            RexCall visited = (RexCall) super.visitCall(call);
            SqlKind kind = visited.getKind();
            if (kind != SqlKind.IS_NULL && kind != SqlKind.IS_NOT_NULL) {
                return visited;
            }
            RexNode operand = visited.getOperands().get(0);
            if (isMakeStruct(operand) == false) {
                return visited;
            }
            List<RexNode> leaves = leafValues((RexCall) operand);
            if (leaves.isEmpty()) {
                return visited;
            }
            List<RexNode> tests = new ArrayList<>(leaves.size());
            for (RexNode leaf : leaves) {
                tests.add(
                    rexBuilder.makeCall(kind == SqlKind.IS_NULL ? SqlStdOperatorTable.IS_NULL : SqlStdOperatorTable.IS_NOT_NULL, leaf)
                );
            }
            owner.changed = true;
            // A struct is null when every leaf is; not null when any leaf is.
            return kind == SqlKind.IS_NULL ? RexUtil.composeConjunction(rexBuilder, tests) : RexUtil.composeDisjunction(rexBuilder, tests);
        }
    }

    private static boolean isMakeStruct(RexNode node) {
        return node instanceof RexCall call && call.getOperator() == MakeStructFunction.FUNCTION;
    }

    /** Scalar value operands of a {@code make_struct}, descending through nested calls. */
    private static List<RexNode> leafValues(RexCall structCall) {
        List<RexNode> out = new ArrayList<>();
        collect(structCall, out);
        return out;
    }

    private static void collect(RexCall structCall, List<RexNode> out) {
        List<RexNode> operands = structCall.getOperands();
        for (int i = 1; i < operands.size(); i += 2) {
            RexNode value = operands.get(i);
            if (isMakeStruct(value)) {
                collect((RexCall) value, out);
            } else {
                out.add(value);
            }
        }
    }
}
