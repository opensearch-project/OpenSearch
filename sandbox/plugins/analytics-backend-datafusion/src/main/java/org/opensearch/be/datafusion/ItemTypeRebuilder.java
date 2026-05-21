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

/**
 * Pre-isthmus pass that rewrites the {@code array_element(map_extract(pattern_parser(args),
 * key), 1)} chain — produced by {@link ArrayElementAdapter} for ITEM-on-MAP over
 * PPL {@code PATTERN_PARSER}'s declared {@code MAP<VARCHAR, ANY>} return type — into
 * a direct call against one of the per-field scalar UDFs (
 * {@code pattern_parser_get_pattern}, {@code pattern_parser_get_tokens}).
 *
 * <p>Why bypass struct field access entirely:
 * <ul>
 *   <li>Substrait extension catalog has no {@code ITEM} binding for STRUCT operands,
 *       so {@code ITEM(STRUCT, key)} fails serialization.</li>
 *   <li>Isthmus' {@code RexExpressionConverter.visitFieldAccess} rejects
 *       {@code RexFieldAccess} where the reference expression is a function call
 *       with {@code "RexFieldAccess for SqlKind OTHER_FUNCTION not supported"}.</li>
 *   <li>Hoisting the struct call into a child Project + a top-level FieldAccess
 *       gets past isthmus, but DataFusion's substrait consumer then rejects the
 *       resulting {@code FieldReference} with {@code "Direct reference StructField
 *       with child is not supported"}.</li>
 * </ul>
 *
 * <p>The per-field UDF approach keeps the entire expression flat — the Rust UDF
 * runs the same parse and returns just the requested scalar (Utf8 for "pattern",
 * Map for "tokens"). Two calls share work via the underlying invocation pattern;
 * the cost of re-running the parse twice is negligible compared to the alternative
 * struct-access plumbing.
 *
 * @opensearch.internal
 */
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
            boolean[] changed = {false};
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

        /**
         * Detects the {@code array_element(map_extract(pattern_parser(args), key), 1)}
         * pattern and rewrites it to a direct call against
         * {@link DataFusionFragmentConvertor#LOCAL_PATTERN_PARSER_GET_PATTERN_OP} or
         * {@link DataFusionFragmentConvertor#LOCAL_PATTERN_PARSER_GET_TOKENS_OP},
         * preserving the original {@code pattern_parser}'s operand list.
         */
        private RexNode tryRewritePatternParserAccess(RexCall call, List<RexNode> operands) {
            if (!"array_element".equalsIgnoreCase(call.getOperator().getName())) {
                return null;
            }
            if (operands.size() != 2 || !(operands.get(0) instanceof RexCall mapExtract)) {
                return null;
            }
            if (!"map_extract".equalsIgnoreCase(mapExtract.getOperator().getName())
                || mapExtract.getOperands().size() != 2) {
                return null;
            }
            if (!(operands.get(1) instanceof RexLiteral indexLit)) {
                return null;
            }
            Object indexValue = indexLit.getValue();
            if (!(indexValue instanceof BigDecimal indexBd) || indexBd.intValueExact() != 1) {
                return null;
            }
            if (!(mapExtract.getOperands().get(0) instanceof RexCall innerCall)) {
                return null;
            }
            if (!"pattern_parser".equalsIgnoreCase(innerCall.getOperator().getName())) {
                return null;
            }
            RexNode keyNode = mapExtract.getOperands().get(1);
            String fieldName = extractFieldName(keyNode);
            if (fieldName == null) {
                return null;
            }
            SqlOperator targetOp = switch (fieldName) {
                case "pattern" -> DataFusionFragmentConvertor.LOCAL_PATTERN_PARSER_GET_PATTERN_OP;
                case "tokens" -> DataFusionFragmentConvertor.LOCAL_PATTERN_PARSER_GET_TOKENS_OP;
                default -> null;
            };
            if (targetOp == null) {
                return null;
            }
            // Preserve the outer SAFE_CAST's expected return type by deferring to the
            // operator's own return-type inference (2-arg makeCall). The outer SAFE_CAST
            // will coerce VARCHAR/MAP to whatever the project's row type declares.
            return rexBuilder.makeCall(targetOp, innerCall.getOperands());
        }

        /**
         * Extract the field name from the {@code map_extract} key operand. Calcite
         * may wrap a literal in a CAST when the key type needs coercion, so peek
         * through one CAST level if necessary.
         */
        private String extractFieldName(RexNode keyNode) {
            if (keyNode instanceof RexLiteral lit) {
                return lit.getValueAs(String.class);
            }
            if (keyNode instanceof RexCall cast
                && (cast.getKind() == org.apache.calcite.sql.SqlKind.CAST
                    || cast.getKind() == org.apache.calcite.sql.SqlKind.SAFE_CAST)
                && cast.getOperands().size() >= 1
                && cast.getOperands().get(0) instanceof RexLiteral lit) {
                return lit.getValueAs(String.class);
            }
            return null;
        }
    }
}
