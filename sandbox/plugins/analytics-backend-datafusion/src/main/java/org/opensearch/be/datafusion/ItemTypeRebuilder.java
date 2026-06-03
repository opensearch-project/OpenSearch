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
import org.apache.calcite.sql.SqlOperator;

import java.math.BigDecimal;
import java.util.List;

/** Rewrites struct-field access on {@code pattern_parser} into per-field scalar UDF calls. */
final class ItemTypeRebuilder {

    private ItemTypeRebuilder() {}

    static RelNode rewrite(RelNode root) {
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                RexShuttle shuttle = new RewriteShuttle(visited.getCluster().getRexBuilder());
                RelNode rewritten = visited.accept(shuttle);
                return rewritten == null ? visited : rewritten;
            }
        });
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
            RexNode rewritten = tryRewritePatternParserAccess(call, operands);
            if (rewritten != null) {
                return rewritten;
            }
            if (!changed[0]) {
                return call;
            }
            return rexBuilder.makeCall(call.getType(), call.getOperator(), newOperands);
        }

        private RexNode tryRewritePatternParserAccess(RexCall call, List<RexNode> operands) {
            RexCall innerCall;
            RexNode keyNode;
            if (call.getKind() == org.apache.calcite.sql.SqlKind.ITEM
                && operands.size() == 2
                && operands.get(0) instanceof RexCall ppDirect
                && "pattern_parser".equalsIgnoreCase(ppDirect.getOperator().getName())) {
                innerCall = ppDirect;
                keyNode = operands.get(1);
            } else if ("array_element".equalsIgnoreCase(call.getOperator().getName())
                && operands.size() == 2
                && operands.get(0) instanceof RexCall mapExtract
                && "map_extract".equalsIgnoreCase(mapExtract.getOperator().getName())
                && mapExtract.getOperands().size() == 2
                && operands.get(1) instanceof RexLiteral indexLit
                && indexLit.getValue() instanceof BigDecimal indexBd
                && indexBd.intValueExact() == 1
                && mapExtract.getOperands().get(0) instanceof RexCall ppViaArray
                && "pattern_parser".equalsIgnoreCase(ppViaArray.getOperator().getName())) {
                    innerCall = ppViaArray;
                    keyNode = mapExtract.getOperands().get(1);
                } else {
                    return null;
                }
            String fieldName = extractFieldName(keyNode);
            if (fieldName == null) {
                return null;
            }
            // 3-arg pattern_parser (BRAIN label): the window result is already the matched pattern.
            List<RexNode> ppArgs = innerCall.getOperands();
            if (ppArgs.size() == 3) {
                RexNode source = ppArgs.get(0);
                RexNode windowResult = ppArgs.get(1);
                return switch (fieldName) {
                    case "pattern" -> windowResult;
                    case "tokens" -> rexBuilder.makeCall(
                        DataFusionFragmentConvertor.LOCAL_PATTERN_PARSER_GET_TOKENS_OP,
                        List.of(windowResult, source)
                    );
                    default -> null;
                };
            }
            SqlOperator targetOp = switch (fieldName) {
                case "pattern" -> DataFusionFragmentConvertor.LOCAL_PATTERN_PARSER_GET_PATTERN_OP;
                case "tokens" -> DataFusionFragmentConvertor.LOCAL_PATTERN_PARSER_GET_TOKENS_OP;
                default -> null;
            };
            if (targetOp == null) {
                return null;
            }
            return rexBuilder.makeCall(targetOp, innerCall.getOperands());
        }

        private String extractFieldName(RexNode keyNode) {
            if (keyNode instanceof RexLiteral lit) {
                return lit.getValueAs(String.class);
            }
            if (keyNode instanceof RexCall cast
                && (cast.getKind() == org.apache.calcite.sql.SqlKind.CAST || cast.getKind() == org.apache.calcite.sql.SqlKind.SAFE_CAST)
                && cast.getOperands().size() >= 1
                && cast.getOperands().get(0) instanceof RexLiteral lit) {
                return lit.getValueAs(String.class);
            }
            return null;
        }
    }
}
