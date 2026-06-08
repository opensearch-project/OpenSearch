/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.schema.BinaryType;
import org.opensearch.analytics.schema.IpType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites PPL state-expanding aggregates (TAKE / FIRST / LAST / LIST / VALUES / PATTERN /
 * PERCENTILE_APPROX) onto local stubs the substrait emitter binds via
 * {@link DataFusionFragmentConvertor}'s ADDITIONAL_AGGREGATE_SIGS. Also normalises any
 * RexLiteral{SqlTypeName.SYMBOL} in upstream Projects to VARCHAR — isthmus's
 * LiteralConverter rejects unregistered Enum classes, and PPL's percentile_approx /
 * median emit a SymbolFlag arg purely for type inference.
 */
final class PplAggregateCallRewriter {

    private static final Set<SqlAggFunction> LOCAL_OPS = Set.of(
        DataFusionFragmentConvertor.LOCAL_TAKE_OP,
        DataFusionFragmentConvertor.LOCAL_FIRST_OP,
        DataFusionFragmentConvertor.LOCAL_LAST_OP,
        DataFusionFragmentConvertor.LOCAL_ARRAY_AGG_OP,
        DataFusionFragmentConvertor.LOCAL_LIST_MERGE_OP,
        DataFusionFragmentConvertor.LOCAL_LIST_MERGE_DISTINCT_OP,
        DataFusionFragmentConvertor.LOCAL_PERCENTILE_APPROX_OP,
        DataFusionFragmentConvertor.LOCAL_INTERNAL_PATTERN_OP
    );

    private static final String LIST = "LIST";
    private static final String VALUES = "VALUES";
    private static final String STR_SUFFIX = "$str";

    private PplAggregateCallRewriter() {}

    static RelNode rewrite(RelNode root) {
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                if (visited instanceof Project p) {
                    return normaliseSymbolFlagLiterals(p);
                }
                if (visited instanceof Aggregate agg) {
                    return rewriteAggregate(agg);
                }
                return visited;
            }
        });
    }

    private static RelNode rewriteAggregate(Aggregate agg) {
        Aggregate lifted = liftListValuesOperandsToVarchar(agg);
        List<AggregateCall> oldCalls = lifted.getAggCallList();
        List<AggregateCall> newCalls = new ArrayList<>(oldCalls.size());
        boolean changed = lifted != agg;
        for (AggregateCall call : oldCalls) {
            AggregateCall rewritten = rewriteCall(lifted, call);
            newCalls.add(rewritten);
            changed |= rewritten != call;
        }
        if (!changed) {
            return agg;
        }
        return lifted.copy(lifted.getTraitSet(), lifted.getInput(), lifted.getGroupSet(), lifted.getGroupSets(), newCalls);
    }

    /**
     * Lifts each scalar operand of a {@code LIST}/{@code VALUES} call into a VARCHAR column
     * via a {@link LogicalProject} inserted above {@code agg.getInput()}, so the aggregator
     * produces an {@code ARRAY<VARCHAR>}. Skips ARRAY operands (the partial→final merge path)
     * and operands that are already VARCHAR. Other aggregate calls and group keys are
     * untouched.
     */
    private static Aggregate liftListValuesOperandsToVarchar(Aggregate agg) {
        Map<Integer, Integer> castMap = collectListValuesScalarOperands(agg);
        if (castMap.isEmpty()) {
            return agg;
        }
        RelNode lifted = buildLiftingProject(agg, castMap);
        List<AggregateCall> rewired = rewireListValuesCalls(agg, lifted, castMap);
        return agg.copy(agg.getTraitSet(), lifted, agg.getGroupSet(), agg.getGroupSets(), rewired);
    }

    /**
     * Returns a map from original-input-column-index to new-projected-column-index for every
     * scalar (non-VARCHAR, non-ARRAY) operand of a LIST/VALUES call. Insertion order preserved
     * so cast slots end up contiguous in the lifted Project.
     */
    private static Map<Integer, Integer> collectListValuesScalarOperands(Aggregate agg) {
        List<RelDataTypeField> origFields = agg.getInput().getRowType().getFieldList();
        int origFieldCount = origFields.size();
        Map<Integer, Integer> castMap = new LinkedHashMap<>();
        for (AggregateCall call : agg.getAggCallList()) {
            if (!isListOrValuesCall(call) || call.getArgList().isEmpty()) {
                continue;
            }
            int argIdx = call.getArgList().get(0);
            RelDataType argType = origFields.get(argIdx).getType();
            if (argType.getComponentType() != null || argType.getSqlTypeName() == SqlTypeName.VARCHAR) {
                continue;
            }
            castMap.putIfAbsent(argIdx, origFieldCount + castMap.size());
        }
        return castMap;
    }

    /**
     * Builds the lifting Project: passes through every original column, then appends one
     * VARCHAR column per entry in {@code castMap}. {@link IpType} routes to
     * {@code ip_to_string} and {@link BinaryType} to {@code binary_to_base64} directly —
     * {@link IpBinaryCastFunctionAdapter} would normally rewrite a CAST, but that pass has
     * already run by the time this rewriter fires. Everything else uses a plain CAST.
     */
    private static RelNode buildLiftingProject(Aggregate agg, Map<Integer, Integer> castMap) {
        RelDataTypeFactory typeFactory = agg.getCluster().getTypeFactory();
        RexBuilder rexBuilder = agg.getCluster().getRexBuilder();
        RelDataType varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        List<RelDataTypeField> origFields = agg.getInput().getRowType().getFieldList();
        List<RexNode> projects = new ArrayList<>(origFields.size() + castMap.size());
        List<String> names = new ArrayList<>(origFields.size() + castMap.size());
        for (RelDataTypeField field : origFields) {
            projects.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
            names.add(field.getName());
        }
        for (int srcIdx : castMap.keySet()) {
            RelDataType srcType = origFields.get(srcIdx).getType();
            RexNode srcRef = rexBuilder.makeInputRef(srcType, srcIdx);
            projects.add(toVarchar(srcType, srcRef, varcharNullable, rexBuilder));
            names.add(origFields.get(srcIdx).getName() + STR_SUFFIX);
        }
        return LogicalProject.create(agg.getInput(), List.of(), projects, names, Set.of());
    }

    private static RexNode toVarchar(RelDataType srcType, RexNode srcRef, RelDataType varcharNullable, RexBuilder rexBuilder) {
        if (srcType instanceof IpType) {
            return rexBuilder.makeCall(varcharNullable, IpBinaryCastFunctionAdapter.IP_TO_STRING_OP, List.of(srcRef));
        }
        if (srcType instanceof BinaryType) {
            return rexBuilder.makeCall(varcharNullable, IpBinaryCastFunctionAdapter.BINARY_TO_BASE64_OP, List.of(srcRef));
        }
        return rexBuilder.makeCast(varcharNullable, srcRef);
    }

    /**
     * Rewires each LIST/VALUES call whose operand was lifted to point at its new VARCHAR
     * column. Other aggregate calls keep their original column indices because the lifting
     * Project preserves the original prefix. The explicitReturnType is left null so the
     * downstream LIST/VALUES dispatch in {@link #rewriteCall} recomputes it for the rewired
     * input.
     */
    private static List<AggregateCall> rewireListValuesCalls(Aggregate agg, RelNode lifted, Map<Integer, Integer> castMap) {
        List<AggregateCall> rewired = new ArrayList<>(agg.getAggCallList().size());
        for (AggregateCall call : agg.getAggCallList()) {
            Integer newIdx = isListOrValuesCall(call) && !call.getArgList().isEmpty() ? castMap.get(call.getArgList().get(0)) : null;
            if (newIdx == null) {
                rewired.add(call);
                continue;
            }
            List<Integer> newArgList = new ArrayList<>(call.getArgList());
            newArgList.set(0, newIdx);
            rewired.add(
                AggregateCall.create(
                    call.getAggregation(),
                    call.isDistinct(),
                    call.isApproximate(),
                    call.ignoreNulls(),
                    call.rexList,
                    newArgList,
                    call.filterArg,
                    call.distinctKeys,
                    call.collation,
                    agg.getGroupCount(),
                    lifted,
                    null,
                    call.getName()
                )
            );
        }
        return rewired;
    }

    private static boolean isListOrValuesCall(AggregateCall call) {
        String name = call.getAggregation().getName();
        return LIST.equalsIgnoreCase(name) || VALUES.equalsIgnoreCase(name);
    }

    /** Replace any RexLiteral{SymbolFlag} in {@code project}'s projection list with a VARCHAR literal of the symbol's name. */
    private static RelNode normaliseSymbolFlagLiterals(Project project) {
        List<RexNode> oldProjects = project.getProjects();
        boolean hasSymbol = oldProjects.stream()
            .anyMatch(p -> p instanceof RexLiteral lit && lit.getType().getSqlTypeName() == SqlTypeName.SYMBOL);
        if (!hasSymbol) {
            return project;
        }
        RelDataTypeFactory typeFactory = project.getCluster().getTypeFactory();
        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        List<RexNode> newProjects = new ArrayList<>(oldProjects.size());
        for (RexNode p : oldProjects) {
            if (p instanceof RexLiteral lit && lit.getType().getSqlTypeName() == SqlTypeName.SYMBOL) {
                String name = lit.getValue() == null ? "" : lit.getValue().toString();
                newProjects.add(rexBuilder.makeLiteral(name, varcharType));
            } else {
                newProjects.add(p);
            }
        }
        return LogicalProject.create(
            project.getInput(),
            project.getHints(),
            newProjects,
            project.getRowType().getFieldNames(),
            project.getVariablesSet()
        );
    }

    private static AggregateCall rewriteCall(Aggregate agg, AggregateCall call) {
        SqlAggFunction aggregation = call.getAggregation();
        if (LOCAL_OPS.contains(aggregation)) {
            return call;
        }
        SqlAggFunction targetOp;
        boolean targetDistinct = call.isDistinct();
        RelDataType explicitReturnType = call.getType();
        switch (aggregation.getName().toUpperCase(java.util.Locale.ROOT)) {
            case "TAKE" -> targetOp = DataFusionFragmentConvertor.LOCAL_TAKE_OP;
            case "FIRST" -> targetOp = DataFusionFragmentConvertor.LOCAL_FIRST_OP;
            case "LAST" -> targetOp = DataFusionFragmentConvertor.LOCAL_LAST_OP;
            case "ARG_MIN", "ARG_MAX" -> {
                // ARG_MIN/ARG_MAX(value, ts) -> first_value/last_value(value) with ts as the agg
                // ORDER BY key (DataFusion 53 has no arg_min/max UDAF; first/last_value take ordering).
                if (call.getArgList().size() != 2) {
                    return call;
                }
                boolean isMin = "ARG_MIN".equalsIgnoreCase(aggregation.getName());
                SqlAggFunction op = isMin ? DataFusionFragmentConvertor.LOCAL_FIRST_OP : DataFusionFragmentConvertor.LOCAL_LAST_OP;
                int valueArg = call.getArgList().get(0);
                int timeArg = call.getArgList().get(1);
                RelCollation collation = RelCollations.of(
                    new RelFieldCollation(timeArg, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST)
                );
                RelDataType arg0Type = agg.getInput().getRowType().getFieldList().get(valueArg).getType();
                RelDataType nullableArg0 = agg.getCluster().getTypeFactory().createTypeWithNullability(arg0Type, true);
                return AggregateCall.create(
                    op,
                    targetDistinct,
                    call.isApproximate(),
                    call.ignoreNulls(),
                    call.rexList,
                    List.of(valueArg),
                    call.filterArg,
                    call.distinctKeys,
                    collation,
                    agg.getGroupCount(),
                    agg.getInput(),
                    nullableArg0,
                    call.getName()
                );
            }
            case "LIST", "VALUES" -> {
                // arg0 type distinguishes PARTIAL (raw element → array_agg) from FINAL (array → list_merge).
                if (call.getArgList().isEmpty()) {
                    return call;
                }
                boolean isValues = "VALUES".equalsIgnoreCase(aggregation.getName());
                RelDataType arg0Type = agg.getInput().getRowType().getFieldList().get(call.getArgList().get(0)).getType();
                boolean arg0IsList = arg0Type.getComponentType() != null;
                if (arg0IsList) {
                    targetOp = isValues
                        ? DataFusionFragmentConvertor.LOCAL_LIST_MERGE_DISTINCT_OP
                        : DataFusionFragmentConvertor.LOCAL_LIST_MERGE_OP;
                    targetDistinct = false;
                    explicitReturnType = arg0Type;
                } else {
                    targetOp = DataFusionFragmentConvertor.LOCAL_ARRAY_AGG_OP;
                    targetDistinct = isValues;
                    // Match LOCAL_ARRAY_AGG_OP's nullable ARRAY inference; the 2-arg
                    // createArrayType overload defaults to NOT NULL and trips Calcite's
                    // typeMatchesInferred check.
                    RelDataType arrayType = agg.getCluster().getTypeFactory().createArrayType(arg0Type, -1);
                    explicitReturnType = agg.getCluster().getTypeFactory().createTypeWithNullability(arrayType, true);
                }
            }
            case "PATTERN" -> {
                // PPL declares ARRAY<MAP<VARCHAR, ANY>>; substrait can't carry ANY.
                targetOp = DataFusionFragmentConvertor.LOCAL_INTERNAL_PATTERN_OP;
                explicitReturnType = internalPatternReturnType(agg.getCluster().getTypeFactory());
            }
            case "PERCENTILE_APPROX" -> {
                // Trim the PPL type-flag arg; the substrait emit-time literal-arg normaliser
                // (DataFusionFragmentConvertor#normaliseLiteralArg) rescales the percentile.
                if (call.getArgList().size() < 3) {
                    return call;
                }
                targetOp = DataFusionFragmentConvertor.LOCAL_PERCENTILE_APPROX_OP;
                List<Integer> trimmedArgList = new ArrayList<>(call.getArgList().subList(0, 2));
                RelDataType arg0Type = agg.getInput().getRowType().getFieldList().get(call.getArgList().get(0)).getType();
                RelDataType nullableArg0 = agg.getCluster().getTypeFactory().createTypeWithNullability(arg0Type, true);
                return AggregateCall.create(
                    targetOp,
                    targetDistinct,
                    call.isApproximate(),
                    call.ignoreNulls(),
                    call.rexList,
                    trimmedArgList,
                    call.filterArg,
                    call.distinctKeys,
                    call.collation,
                    agg.getGroupCount(),
                    agg.getInput(),
                    nullableArg0,
                    call.getName()
                );
            }
            default -> {
                return call;
            }
        }
        return AggregateCall.create(
            targetOp,
            targetDistinct,
            call.isApproximate(),
            call.ignoreNulls(),
            call.rexList,
            call.getArgList(),
            call.filterArg,
            call.distinctKeys,
            call.collation,
            agg.getGroupCount(),
            agg.getInput(),
            explicitReturnType,
            call.getName()
        );
    }

    private static RelDataType internalPatternReturnType(RelDataTypeFactory typeFactory) {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RelDataType varcharArray = typeFactory.createArrayType(varchar, -1);
        RelDataType tokensMap = typeFactory.createMapType(varchar, varcharArray);
        RelDataType structType = typeFactory.createStructType(
            List.of(varchar, bigint, tokensMap, varcharArray),
            List.of("pattern", "pattern_count", "tokens", "sample_logs")
        );
        return typeFactory.createArrayType(structType, -1);
    }
}
