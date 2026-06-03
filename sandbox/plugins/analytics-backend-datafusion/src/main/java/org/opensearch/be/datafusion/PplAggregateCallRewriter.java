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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
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
        List<AggregateCall> oldCalls = agg.getAggCallList();
        List<AggregateCall> newCalls = new ArrayList<>(oldCalls.size());
        boolean changed = false;
        for (AggregateCall call : oldCalls) {
            AggregateCall rewritten = rewriteCall(agg, call);
            newCalls.add(rewritten);
            changed |= rewritten != call;
        }
        if (!changed) {
            return agg;
        }
        return agg.copy(agg.getTraitSet(), agg.getInput(), agg.getGroupSet(), agg.getGroupSets(), newCalls);
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
            // PPL `dc`/`distinct_count_approx` lowers to a UDAF named DISTINCT_COUNT_APPROX whose
            // operator identity has no substrait sig. Remap to Calcite's stock
            // APPROX_COUNT_DISTINCT, which ADDITIONAL_AGGREGATE_SIGS binds to DataFusion's native
            // `approx_distinct` (same single-arg expr → bigint shape).
            case "DISTINCT_COUNT_APPROX" -> {
                targetOp = SqlStdOperatorTable.APPROX_COUNT_DISTINCT;
                // APPROX_COUNT_DISTINCT is a count — its return-type inference yields BIGINT NOT NULL.
                // Reusing the PPL call's original (nullable) BIGINT as the explicit type leaves the
                // AggregateCall's declared type disagreeing with the operator's inferred type, which
                // trips Calcite's validity assertion ("aggCall type BIGINT vs inferred BIGINT NOT NULL")
                // on the grouped two-phase path. Pin the explicit type to the operator-consistent
                // NOT NULL BIGINT so declared and inferred agree.
                explicitReturnType = agg.getCluster().getTypeFactory().createTypeWithNullability(call.getType(), false);
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
                    // PPL list/values is ARRAY<VARCHAR>; the operand is cast to VARCHAR on the
                    // substrait arg (LOCAL_ARRAY_AGG_OP#rewriteDataArg). Nullable array matches the
                    // op's inferred type (a NOT NULL array trips Calcite's typeMatchesInferred).
                    RelDataTypeFactory typeFactory = agg.getCluster().getTypeFactory();
                    RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
                    RelDataType arrayType = typeFactory.createArrayType(varchar, -1);
                    explicitReturnType = typeFactory.createTypeWithNullability(arrayType, true);
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
