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

/** Boolean → varchar emits upper-case TRUE/FALSE (PPL contract matches {@code tostring(boolean)}). */
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

    /** Package-visible for unit tests. */
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
