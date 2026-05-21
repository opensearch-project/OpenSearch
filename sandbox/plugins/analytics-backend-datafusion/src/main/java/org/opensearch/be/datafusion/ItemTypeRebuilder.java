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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Pre-isthmus pass that rebuilds {@code ITEM} calls so their return type is
 * re-derived from the (possibly adapted) operand-0 type.
 *
 * <p>Background: PPL emits {@code PATTERN_PARSER} (and other scalar UDFs) with
 * a declared return type of {@code MAP<VARCHAR, ANY>} (see
 * {@code UserDefinedFunctionUtils.patternStruct}). The PPL Calcite visitor
 * then composes {@code ITEM(parsedNode, "pattern" | "tokens")} on top — at the
 * time these {@code ITEM} calls are constructed, operand 0's type is
 * {@code MAP<VARCHAR, ANY>}, so the ITEM call's frozen return type is
 * {@code ANY}.
 *
 * <p>{@link PatternParserAdapter} runs as a scalar-function adapter inside
 * {@link org.opensearch.analytics.planner.rules.OpenSearchProjectRule} and
 * rewrites the inner {@code PATTERN_PARSER} call to return a concrete
 * {@code STRUCT<pattern: VARCHAR, tokens: MAP<VARCHAR, ARRAY<VARCHAR>>>}.
 * However, the wrapping {@code ITEM} call still carries the original frozen
 * {@code ANY} return type — Calcite {@link RexCall}s are immutable and parent
 * calls don't refresh when a child is replaced. Isthmus' substrait visitor
 * then rejects with {@code "Unable to convert the type ANY"} when it reaches
 * the {@code ITEM}'s declared type.
 *
 * <p>Fix: walk every {@code Project} / {@code Filter} expression tree
 * bottom-up and rebuild every {@code ITEM} {@link RexCall} via
 * {@link RexBuilder#makeCall(org.apache.calcite.sql.SqlOperator, java.util.List)},
 * which re-derives the return type from the current operand types. The result:
 * {@code ITEM(STRUCT, "pattern")} resolves to {@code VARCHAR} (named struct-field
 * access), {@code ITEM(STRUCT, "tokens")} to {@code MAP<VARCHAR, ARRAY<VARCHAR>>},
 * which Substrait can serialise.
 *
 * <p>Only {@link SqlKind#ITEM} is rebuilt — the rest of the tree is preserved.
 * Other operators that explicitly chose their return type (e.g. {@code CAST} /
 * {@code SAFE_CAST}'s destination type) are not touched, so the rewrite is
 * safe even if some operator's inferred type differs from its declared one.
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
                RexShuttle shuttle = new ItemRebuildShuttle(visited.getCluster().getRexBuilder());
                RelNode rewritten = visited.accept(shuttle);
                return rewritten == null ? visited : rewritten;
            }
        });
    }

    private static final class ItemRebuildShuttle extends RexShuttle {
        private final RexBuilder rexBuilder;

        ItemRebuildShuttle(RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            // Children first — bottom-up so operand 0 reflects any deeper
            // rebuilds before we inspect this call.
            boolean[] changed = {false};
            List<RexNode> newOperands = visitList(call.getOperands(), changed);
            // Substitute frozen MAP<VARCHAR, ANY> return type on pattern_parser
            // calls with the concrete STRUCT<pattern: VARCHAR, tokens: MAP<VARCHAR,
            // ARRAY<VARCHAR>>> matching the Rust UDF's Arrow output. The PPL
            // operator declares MAP<VARCHAR, ANY> (legacy v2 path returns a
            // heterogeneous Java map) but Substrait's TypeConverter can't
            // serialise the embedded ANY — so we eagerly substitute here, before
            // isthmus walks the tree's operand types via FunctionFinder.attemptMatch.
            //
            // The substitution mirrors PatternParserAdapter's runtime adapter, but
            // applies at the RexCall level so ALL wrapping calls (ITEM, etc.) see
            // the concrete type by the time substrait emission visits them.
            if ("pattern_parser".equalsIgnoreCase(call.getOperator().getName())
                && call.getType().getSqlTypeName() == SqlTypeName.MAP) {
                RelDataType structType = patternStructType(
                    rexBuilder.getTypeFactory(), call.getType().isNullable());
                List<RexNode> operands = changed[0] ? newOperands : call.getOperands();
                return rexBuilder.makeCall(structType, call.getOperator(), operands);
            }
            if (call.getKind() != SqlKind.ITEM) {
                if (!changed[0]) {
                    return call;
                }
                return rexBuilder.makeCall(call.getType(), call.getOperator(), newOperands);
            }
            List<RexNode> operands = changed[0] ? newOperands : call.getOperands();
            // Re-derive type from the (possibly new) operand types. The 2-arg
            // makeCall(op, operands) overload uses the operator's
            // SqlReturnTypeInference, which for ITEM on a STRUCT with a string
            // literal yields the named field's type — exactly the
            // ANY-elimination we need.
            return rexBuilder.makeCall(call.getOperator(), operands);
        }

        /** Concrete struct shape matching Rust's {@code pattern_parser} UDF output. */
        private RelDataType patternStructType(RelDataTypeFactory typeFactory, boolean nullable) {
            RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
            RelDataType varcharArray = typeFactory.createArrayType(varchar, -1);
            RelDataType tokensMap = typeFactory.createMapType(varchar, varcharArray);
            RelDataType struct = typeFactory.createStructType(
                List.of(varchar, tokensMap),
                List.of("pattern", "tokens")
            );
            return typeFactory.createTypeWithNullability(struct, nullable);
        }
    }
}
