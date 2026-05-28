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
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Rewrites PPL state-expanding aggregates (TAKE/FIRST/LAST/LIST/VALUES/PATTERN) onto local stubs. */
final class PplAggregateCallRewriter {

    private static final Set<SqlAggFunction> LOCAL_OPS = Set.of(
        DataFusionFragmentConvertor.LOCAL_TAKE_OP,
        DataFusionFragmentConvertor.LOCAL_FIRST_OP,
        DataFusionFragmentConvertor.LOCAL_LAST_OP,
        DataFusionFragmentConvertor.LOCAL_ARRAY_AGG_OP,
        DataFusionFragmentConvertor.LOCAL_LIST_MERGE_OP,
        DataFusionFragmentConvertor.LOCAL_LIST_MERGE_DISTINCT_OP,
        DataFusionFragmentConvertor.LOCAL_INTERNAL_PATTERN_OP
    );

    private PplAggregateCallRewriter() {}

    static RelNode rewrite(RelNode root) {
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                if (!(visited instanceof Aggregate agg)) {
                    return visited;
                }
                List<AggregateCall> oldCalls = agg.getAggCallList();
                List<AggregateCall> newCalls = new ArrayList<>(oldCalls.size());
                boolean changed = false;
                for (AggregateCall call : oldCalls) {
                    AggregateCall rewritten = rewriteCall(agg, call);
                    if (rewritten == call) {
                        newCalls.add(call);
                    } else {
                        newCalls.add(rewritten);
                        changed = true;
                    }
                }
                if (!changed) {
                    return visited;
                }
                return agg.copy(agg.getTraitSet(), agg.getInput(), agg.getGroupSet(), agg.getGroupSets(), newCalls);
            }
        });
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
            case "LIST", "VALUES", "ARRAY_AGG" -> {
                // arg0 type tells us PARTIAL (raw element) vs FINAL (already array):
                // PARTIAL → array_agg (with INVOCATION_DISTINCT for VALUES); rebuild
                // return type as ARRAY<arg0> to repair PPL's lossy STRING_ARRAY.
                // FINAL → list_merge / list_merge_distinct un-nests per-shard arrays.
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
                    explicitReturnType = agg.getCluster().getTypeFactory().createArrayType(arg0Type, -1);
                }
            }
            case "PATTERN" -> {
                // PPL declares ARRAY<MAP<VARCHAR, ANY>>; substrait can't carry ANY.
                targetOp = DataFusionFragmentConvertor.LOCAL_INTERNAL_PATTERN_OP;
                explicitReturnType = internalPatternReturnType(agg.getCluster().getTypeFactory());
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
