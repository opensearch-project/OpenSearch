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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
                RexBuilder rexBuilder = visited.getCluster().getRexBuilder();
                RexShuttle shuttle = new ItemRebuildShuttle(rexBuilder);
                RelNode rewritten = visited.accept(shuttle);
                if (rewritten == null) {
                    rewritten = visited;
                }
                if (rewritten instanceof Project project) {
                    return maybeHoistStructAccess(project, rexBuilder);
                }
                return rewritten;
            }
        });
    }

    /**
     * Isthmus' {@code RexExpressionConverter.visitFieldAccess} only handles field
     * access whose reference expression is a {@code RexInputRef}, {@code RexFieldAccess},
     * or {@code RexCorrelVariable} — it rejects function-call references with
     * {@code "RexFieldAccess for SqlKind OTHER_FUNCTION not supported"}.
     *
     * <p>When the upstream shuttle has unwound
     * {@code array_element(map_extract(STRUCT_call, key), 1)} to a
     * {@code FieldAccess(STRUCT_call, field)}, the inner {@code STRUCT_call} is a
     * function call. Hoist each unique struct-returning call into a child
     * {@link LogicalProject} so the outer project's field accesses can reference
     * it via an input ref instead.
     *
     * <p>Mirrors {@code OpenSearchProject#liftNestedRexOver} (which hoists
     * {@link org.apache.calcite.rex.RexOver} for the same isthmus-compatibility
     * reason — DataFusion's substrait consumer auto-lifts top-level window
     * functions but not nested ones).
     *
     * @return the original project (unchanged) when no struct-call field access
     *     is found, or a new outer-Project-on-child-Project pair otherwise.
     */
    private static RelNode maybeHoistStructAccess(Project project, RexBuilder rexBuilder) {
        LinkedHashMap<String, RexCall> uniqueStructCalls = new LinkedHashMap<>();
        RexShuttle collector = new RexShuttle() {
            @Override
            public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                RexNode ref = fieldAccess.getReferenceExpr();
                if (ref instanceof RexCall call && ref.getType().isStruct()) {
                    uniqueStructCalls.putIfAbsent(call.toString(), call);
                }
                return super.visitFieldAccess(fieldAccess);
            }
        };
        for (RexNode expr : project.getProjects()) {
            expr.accept(collector);
        }
        if (uniqueStructCalls.isEmpty()) {
            return project;
        }
        RelNode input = project.getInput();
        int inputFieldCount = input.getRowType().getFieldCount();
        List<RexNode> childExprs = new ArrayList<>(inputFieldCount + uniqueStructCalls.size());
        for (int i = 0; i < inputFieldCount; i++) {
            childExprs.add(rexBuilder.makeInputRef(input, i));
        }
        Map<String, Integer> hoistIndex = new LinkedHashMap<>();
        int nextSlot = inputFieldCount;
        for (Map.Entry<String, RexCall> entry : uniqueStructCalls.entrySet()) {
            hoistIndex.put(entry.getKey(), nextSlot++);
            childExprs.add(entry.getValue());
        }
        Project childProject = LogicalProject.create(input, List.of(), childExprs, (List<String>) null);
        RexShuttle rewriter = new RexShuttle() {
            @Override
            public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                RexNode ref = fieldAccess.getReferenceExpr();
                if (ref instanceof RexCall call && ref.getType().isStruct()) {
                    Integer slot = hoistIndex.get(call.toString());
                    if (slot != null) {
                        RexNode inputRef = rexBuilder.makeInputRef(childProject, slot);
                        return rexBuilder.makeFieldAccess(inputRef, fieldAccess.getField().getIndex());
                    }
                }
                return super.visitFieldAccess(fieldAccess);
            }
        };
        List<RexNode> rewrittenOuter = new ArrayList<>(project.getProjects().size());
        for (RexNode expr : project.getProjects()) {
            rewrittenOuter.add(expr.accept(rewriter));
        }
        return LogicalProject.create(childProject, List.of(), rewrittenOuter, project.getRowType());
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
            // Unwind the {@code array_element(map_extract(<container>, <key>), 1)}
            // anti-pattern that {@link ArrayElementAdapter} created for ITEM-on-MAP
            // when the container has since been substituted to a STRUCT (e.g.
            // PATTERN_PARSER's return type was rewritten from MAP<VARCHAR, ANY> to
            // a concrete struct by the branch above). DataFusion's {@code map_extract}
            // only accepts MAP operands, and substrait emission's
            // FunctionFinder.attemptMatch fails on the wrapper chain's frozen ANY
            // return types regardless. Rewriting back to {@code ITEM(STRUCT, key)}
            // lets isthmus serialise it as a native struct-field reference whose
            // type is the field's declared type.
            RexNode unwound = tryUnwindArrayElementMapExtract(call, changed[0] ? newOperands : call.getOperands());
            if (unwound != null) {
                return unwound;
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

        /**
         * Detects the {@code array_element(map_extract(STRUCT_container, key), 1)}
         * chain that {@link ArrayElementAdapter} produces for ITEM-on-MAP, where
         * the container has since been substituted to a STRUCT type (typically
         * because {@code pattern_parser}'s return type was rewritten upstream).
         * Returns a direct {@code ITEM(STRUCT_container, key)} call (whose type
         * is derived via Calcite's struct-field-by-name inference) when the
         * pattern matches, or {@code null} otherwise.
         */
        private RexNode tryUnwindArrayElementMapExtract(RexCall call, List<RexNode> operands) {
            if (!"array_element".equalsIgnoreCase(call.getOperator().getName())) {
                return null;
            }
            if (operands.size() != 2 || !(operands.get(0) instanceof RexCall inner)) {
                return null;
            }
            if (!"map_extract".equalsIgnoreCase(inner.getOperator().getName())
                || inner.getOperands().size() != 2) {
                return null;
            }
            if (!(operands.get(1) instanceof RexLiteral indexLit)) {
                return null;
            }
            Object indexValue = indexLit.getValue();
            if (!(indexValue instanceof BigDecimal indexBd) || indexBd.intValueExact() != 1) {
                return null;
            }
            RexNode container = inner.getOperands().get(0);
            if (!container.getType().isStruct()) {
                return null;
            }
            RexNode key = inner.getOperands().get(1);
            if (!(key instanceof RexLiteral keyLit)) {
                return null;
            }
            String fieldName = keyLit.getValueAs(String.class);
            if (fieldName == null) {
                return null;
            }
            // Isthmus has no ITEM extension binding for STRUCT operands; substrait
            // expects struct-field access as an Expression.FieldReference (Calcite
            // RexFieldAccess). Building that directly lets isthmus serialise it
            // natively via the StructField segment, with the field's declared type.
            return rexBuilder.makeFieldAccess(container, fieldName, true);
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
