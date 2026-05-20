/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AggregateFunction.IntermediateField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Post-Volcano distributed-aggregate decomposition for FINAL aggregate calls. Classifies
 * each call via {@link AggregateFunction#intermediateFields} into pass-through,
 * function-swap (e.g. COUNT→SUM), or engine-native merge (reducer == self), and rebuilds
 * its argList, function, and return type accordingly. Sole owner of
 * {@link AggregateCall#create} on the FINAL side.
 *
 * @opensearch.internal
 */
final class DistributedAggregateRewriter {

    private DistributedAggregateRewriter() {}

    static RelNode rewrite(OpenSearchAggregate finalAgg) {
        RelNode exchange = finalAgg.getInput();
        if (exchange.getInputs().isEmpty()) return finalAgg;
        if (!(exchange.getInputs().get(0) instanceof OpenSearchStageInputScan stageInput)) return finalAgg;

        RelDataTypeFactory tf = finalAgg.getCluster().getTypeFactory();
        int groupCount = finalAgg.getGroupSet().cardinality();
        boolean hasEmptyGroup = finalAgg.getGroupSet().isEmpty();

        RelDataType originalExchangeType = stageInput.getRowType();
        List<RelDataType> exchangeTypes = new ArrayList<>(originalExchangeType.getFieldCount());
        List<String> exchangeNames = new ArrayList<>(originalExchangeType.getFieldCount());
        for (RelDataTypeField f : originalExchangeType.getFieldList()) {
            exchangeTypes.add(f.getType());
            exchangeNames.add(f.getName());
        }

        List<IntermediateField> perCallField = new ArrayList<>(finalAgg.getAggCallList().size());
        for (int i = 0; i < finalAgg.getAggCallList().size(); i++) {
            AggregateCall call = finalAgg.getAggCallList().get(i);
            List<IntermediateField> fields = AggregateFunction.fromSqlAggFunction(call.getAggregation()).intermediateFields();
            IntermediateField field;
            if (fields == null) {
                field = null;
            } else if (fields.size() == 1) {
                field = fields.get(0);
                RelDataType partialOutputType = originalExchangeType.getFieldList().get(groupCount + i).getType();
                exchangeTypes.set(groupCount + i, field.typeResolver().resolve(List.of(partialOutputType), tf));
            } else {
                throw new IllegalStateException(
                    "Multi-field decomposition for ["
                        + call.getAggregation().getName()
                        + "] should have been reduced by OpenSearchAggregateReduceRule during HEP marking"
                );
            }
            perCallField.add(field);
        }

        RelDataType overriddenExchangeType = tf.createStructType(exchangeTypes, exchangeNames);
        RelNode newFinalInput;
        if (overriddenExchangeType.equals(originalExchangeType)) {
            newFinalInput = exchange;
        } else {
            OpenSearchStageInputScan newStageInput = new OpenSearchStageInputScan(
                stageInput.getCluster(),
                stageInput.getTraitSet(),
                stageInput.getChildStageId(),
                overriddenExchangeType,
                stageInput.getViableBackends()
            );
            newFinalInput = exchange.copy(exchange.getTraitSet(), List.of(newStageInput));
        }

        // Re-create captured literal aggregate-args (e.g. TAKE's N) as constant Project
        // columns. SubstraitPlanRewriter.visit(Aggregate) inlines them into the substrait.
        Map<Integer, List<RexLiteral>> extraLiterals = finalAgg.getFinalExtraLiteralArgs();
        Map<Integer, List<Integer>> extraLiteralColIdxByCallIdx;
        if (extraLiterals.isEmpty()) {
            extraLiteralColIdxByCallIdx = Map.of();
        } else {
            int origColCount = newFinalInput.getRowType().getFieldCount();
            List<RexNode> projectExprs = new ArrayList<>(origColCount);
            List<String> projectNames = new ArrayList<>(origColCount);
            for (int idx = 0; idx < origColCount; idx++) {
                RelDataType fieldType = newFinalInput.getRowType().getFieldList().get(idx).getType();
                projectExprs.add(new RexInputRef(idx, fieldType));
                projectNames.add(newFinalInput.getRowType().getFieldList().get(idx).getName());
            }
            java.util.LinkedHashMap<Integer, List<Integer>> idxMap = new java.util.LinkedHashMap<>();
            for (Map.Entry<Integer, List<RexLiteral>> entry : extraLiterals.entrySet()) {
                List<Integer> colIdxs = new ArrayList<>(entry.getValue().size());
                for (int litI = 0; litI < entry.getValue().size(); litI++) {
                    RexLiteral lit = entry.getValue().get(litI);
                    int colIdx = projectExprs.size();
                    projectExprs.add(lit);
                    projectNames.add("$lit_call" + entry.getKey() + "_" + litI);
                    colIdxs.add(colIdx);
                }
                idxMap.put(entry.getKey(), List.copyOf(colIdxs));
            }
            RelDataTypeFactory.Builder rowTypeBuilder = tf.builder();
            for (int idx = 0; idx < projectExprs.size(); idx++) {
                rowTypeBuilder.add(projectNames.get(idx), projectExprs.get(idx).getType());
            }
            newFinalInput = new OpenSearchProject(
                newFinalInput.getCluster(),
                newFinalInput.getTraitSet(),
                newFinalInput,
                projectExprs,
                rowTypeBuilder.build(),
                finalAgg.getViableBackends()
            );
            extraLiteralColIdxByCallIdx = idxMap;
        }

        List<AggregateCall> rebuiltCalls = new ArrayList<>(finalAgg.getAggCallList().size());
        for (int i = 0; i < finalAgg.getAggCallList().size(); i++) {
            AggregateCall call = finalAgg.getAggCallList().get(i);
            IntermediateField field = perCallField.get(i);
            int stateColIdx = groupCount + i;
            String name = overriddenExchangeType.getFieldList().get(stateColIdx).getName();
            List<Integer> extraColIdxs = extraLiteralColIdxByCallIdx.getOrDefault(i, List.of());
            rebuiltCalls.add(buildFinalCall(call, field, stateColIdx, extraColIdxs, name, newFinalInput, hasEmptyGroup));
        }

        return new OpenSearchAggregate(
            finalAgg.getCluster(),
            finalAgg.getTraitSet(),
            newFinalInput,
            finalAgg.getGroupSet(),
            finalAgg.getGroupSets(),
            rebuiltCalls,
            AggregateMode.FINAL,
            finalAgg.getViableBackends(),
            finalAgg.getCallAnnotations(),
            // Cleared so a later copy doesn't re-inject the literal Project.
            Map.of()
        );
    }

    private static AggregateCall buildFinalCall(
        AggregateCall call,
        IntermediateField field,
        int finalArgIdx,
        List<Integer> extraColIdxs,
        String name,
        RelNode newFinalInput,
        boolean hasEmptyGroup
    ) {
        SqlAggFunction aggFunc;
        RelDataType explicitType;
        if (field == null) {
            aggFunc = call.getAggregation();
            explicitType = null;
        } else if (field.reducer() == AggregateFunction.fromSqlAggFunction(call.getAggregation())) {
            // Engine-native merge: keep function identity, pin return type. STATE_EXPANDING
            // pins to the StageInputScan column (PARTIAL's output shape); APPROXIMATE keeps
            // the user-facing return type (e.g. HLL FINAL → BIGINT).
            aggFunc = call.getAggregation();
            AggregateFunction enumFn = AggregateFunction.fromSqlAggFunction(call.getAggregation());
            if (enumFn.getType() == AggregateFunction.Type.STATE_EXPANDING) {
                explicitType = newFinalInput.getRowType().getFieldList().get(finalArgIdx).getType();
            } else {
                explicitType = call.getType();
            }
        } else {
            aggFunc = field.reducer().toSqlAggFunction();
            explicitType = null;
        }

        // State column followed by any forwarded literal-arg columns.
        List<Integer> argList = new ArrayList<>(1 + extraColIdxs.size());
        argList.add(finalArgIdx);
        argList.addAll(extraColIdxs);

        return AggregateCall.create(
            aggFunc,
            call.isDistinct(),
            call.isApproximate(),
            call.ignoreNulls(),
            call.rexList,
            List.copyOf(argList),
            call.filterArg,
            call.distinctKeys,
            call.collation,
            hasEmptyGroup,
            newFinalInput,
            explicitType,
            name
        );
    }
}
