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

/**
 * Pre-isthmus pass that validates string-literal {@code CAST(<lit> AS DATE/TIME/TIMESTAMP)} so
 * malformed inputs surface as a coordinator-side {@link IllegalArgumentException} with the
 * legacy SQL-plugin format-hint wording. Without this guard, the Rust CAST kernel emits
 * {@code "Arrow error: Parser error: Unable to cast to Date32 ..."} which is dropped on the
 * worker→coordinator hop, leaving users with an opaque {@code StreamException}.
 *
 * @opensearch.internal
 */
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

    /** Package-visible for unit tests; production code goes through {@link #rewrite}. */
    static org.apache.calcite.rex.RexShuttle newShuttle() {
        return new ValidatingShuttle();
    }

    private static final class ValidatingShuttle extends RexShuttle {

        @Override
        public RexNode visitCall(RexCall call) {
            RexCall recursed = (RexCall) super.visitCall(call);
            // string-literal cast to DATE/TIME/TIMESTAMP — strict format-hint validation
            if (isCastFromString(recursed)) {
                DatetimeLiteralValidator.Kind kind = kindForTarget(recursed.getType().getSqlTypeName());
                if (kind != null) {
                    DatetimeLiteralValidator.validate(recursed.getOperands().get(0), kind);
                }
                return recursed;
            }
            // TIMESTAMPADD(unit, n, <string-literal>) / TIMESTAMPDIFF(unit, <string-literal>, <string-literal>)
            // — substrait has no binding for TIMESTAMPADD/DIFF over string args, so isthmus rejects
            // with "Unable to convert call". Pre-validate the timestamp literal so a malformed
            // input surfaces as the format-hint exception instead.
            validateTimestampAddDiffOperands(recursed);
            return recursed;
        }

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
