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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

/** Pre-isthmus pass that rejects malformed CAST(string-lit AS DATE/TIME/TIMESTAMP) and TIMESTAMPADD/DIFF string args. */
// TODO: fold into BackendPlanAdaption alongside CastToVarcharRewriter (unified rejection/coercion model).
final class CastTemporalLiteralValidator {

    private CastTemporalLiteralValidator() {}

    static RelNode rewrite(RelNode root) {
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                return visited.accept(new ValidatingShuttle());
            }
        });
    }

    /** Test-only entry point; production goes through {@link #rewrite}. */
    static RexShuttle newShuttle() {
        return new ValidatingShuttle();
    }

    private static final class ValidatingShuttle extends RexShuttle {

        @Override
        public RexNode visitCall(RexCall call) {
            RexCall recursed = (RexCall) super.visitCall(call);
            if (isCastFromString(recursed)) {
                DatetimeLiteralValidator.Kind kind = kindForTarget(recursed.getType().getSqlTypeName());
                if (kind != null) {
                    DatetimeLiteralValidator.validate(recursed.getOperands().get(0), kind);
                }
                return recursed;
            }
            validateTimestampAddDiffOperands(recursed);
            return recursed;
        }

        /** Substrait can't bind TIMESTAMPADD/DIFF over string args; pre-validate so the user sees the format-hint, not "Unable to convert call". */
        private static void validateTimestampAddDiffOperands(RexCall call) {
            String name = call.getOperator().getName();
            if (!"TIMESTAMPADD".equalsIgnoreCase(name) && !"TIMESTAMPDIFF".equalsIgnoreCase(name)) {
                return;
            }
            int firstArg = "TIMESTAMPADD".equalsIgnoreCase(name) ? 2 : 1;
            for (int i = firstArg; i < call.getOperands().size(); i++) {
                RexNode operand = call.getOperands().get(i);
                if (operand instanceof RexLiteral lit && SqlTypeName.CHAR_TYPES.contains(lit.getType().getSqlTypeName())) {
                    DatetimeLiteralValidator.validate(operand, DatetimeLiteralValidator.Kind.TIMESTAMP);
                }
            }
        }

        private static boolean isCastFromString(RexCall call) {
            if (call.getKind() != SqlKind.CAST && call.getKind() != SqlKind.SAFE_CAST) {
                return false;
            }
            if (call.getOperands().size() != 1) {
                return false;
            }
            if (!(call.getOperands().get(0) instanceof RexLiteral literal)) {
                return false;
            }
            return SqlTypeName.CHAR_TYPES.contains(literal.getType().getSqlTypeName());
        }

        private static DatetimeLiteralValidator.Kind kindForTarget(SqlTypeName target) {
            return switch (target) {
                case DATE -> DatetimeLiteralValidator.Kind.DATE;
                case TIME -> DatetimeLiteralValidator.Kind.TIME;
                case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> DatetimeLiteralValidator.Kind.TIMESTAMP;
                default -> null;
            };
        }
    }
}
