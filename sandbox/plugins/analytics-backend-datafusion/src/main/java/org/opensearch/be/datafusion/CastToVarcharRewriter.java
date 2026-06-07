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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Pre-isthmus pass that rewrites {@code CAST(boolean AS VARCHAR)} to
 * {@code CASE WHEN val THEN 'TRUE' WHEN NOT val THEN 'FALSE' END} so the engine emits the
 * upper-case literals PPL clients expect (matching {@code tostring(boolean)}).
 *
 * <p>Temporal casts to VARCHAR are intentionally NOT rewritten here — that would force a fixed
 * format pattern (lossy for nanosecond timestamps). The space-separator + nano-preservation fix
 * for temporals lives in {@code ArrowValues}, which post-processes the rendered string.
 *
 * @opensearch.internal
 */
final class CastToVarcharRewriter {

    private CastToVarcharRewriter() {}

    static RelNode rewrite(RelNode root) {
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                return visited.accept(new CastShuttle(visited.getCluster().getRexBuilder()));
            }
        });
    }

    /** Package-visible for unit tests; production code goes through {@link #rewrite}. */
    static RexShuttle newShuttle(RexBuilder rexBuilder) {
        return new CastShuttle(rexBuilder);
    }

    private static final class CastShuttle extends RexShuttle {

        private final RexBuilder rexBuilder;

        CastShuttle(RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            RexCall recursed = (RexCall) super.visitCall(call);
            if (!isCastBooleanToVarchar(recursed)) {
                return recursed;
            }
            return rewriteBooleanCast(recursed, recursed.getOperands().get(0));
        }

        /** True iff the call is CAST(boolean AS VARCHAR/CHAR). */
        private static boolean isCastBooleanToVarchar(RexCall call) {
            if (call.getKind() != SqlKind.CAST && call.getKind() != SqlKind.SAFE_CAST) {
                return false;
            }
            if (call.getOperands().size() != 1) {
                return false;
            }
            SqlTypeName target = call.getType().getSqlTypeName();
            if (target != SqlTypeName.VARCHAR && target != SqlTypeName.CHAR) {
                return false;
            }
            return call.getOperands().get(0).getType().getSqlTypeName() == SqlTypeName.BOOLEAN;
        }

        /** boolean → CASE WHEN v THEN 'TRUE' WHEN NOT v THEN 'FALSE' END (matches tostring(boolean)). */
        private RexNode rewriteBooleanCast(RexCall original, RexNode value) {
            RelDataType varcharType = original.getType();
            RexNode trueLit = rexBuilder.makeLiteral("TRUE");
            RexNode falseLit = rexBuilder.makeLiteral("FALSE");
            RexNode notValue = rexBuilder.makeCall(SqlStdOperatorTable.NOT, value);
            RexNode nullLit = rexBuilder.makeNullLiteral(varcharType);
            return rexBuilder.makeCall(varcharType, SqlStdOperatorTable.CASE, List.of(value, trueLit, notValue, falseLit, nullLit));
        }
    }
}
