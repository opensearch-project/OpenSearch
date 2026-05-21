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
            RexCall innerCall;
            RexNode keyNode;
            if (call.getKind() == org.apache.calcite.sql.SqlKind.ITEM
                && operands.size() == 2
                && operands.get(0) instanceof RexCall ppDirect
                && "pattern_parser".equalsIgnoreCase(ppDirect.getOperator().getName())) {
                // Direct {@code ITEM(pattern_parser(...), 'key')} shape — produced
                // for the BRAIN label path because ArrayElementAdapter doesn't fire
                // on a pattern_parser whose container type still reads as
                // MAP<VARCHAR, ANY> (the v2 PPL declared type) at adapter time.
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
                // Standard post-ArrayElementAdapter shape:
                // {@code array_element(map_extract(pattern_parser(...), 'key'), 1)}.
                innerCall = ppViaArray;
                keyNode = mapExtract.getOperands().get(1);
            } else {
                return null;
            }
            String fieldName = extractFieldName(keyNode);
            if (fieldName == null) {
                return null;
            }
            // BRAIN label-mode shape: pattern_parser is called with 3 args
            // (source_field, window_result, show_numbered_token). The window has
            // already been rewritten by PplWindowCallRewriter to emit the matched
            // pattern string per row directly, so we can bypass pattern_parser
            // entirely for the "pattern" key — the window result IS the pattern.
            // For "tokens" we route to the 2-arg get_tokens UDF, supplying the
            // window-matched pattern as the pattern arg and the raw source value
            // as the field arg so extract_variables can pull labelled values out.
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
