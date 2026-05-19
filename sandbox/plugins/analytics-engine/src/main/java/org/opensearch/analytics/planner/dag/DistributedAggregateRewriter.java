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
import org.apache.calcite.sql.SqlAggFunction;
import org.opensearch.analytics.planner.ArrowCalciteTypes;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AggregateFunction.IntermediateField;

import java.util.ArrayList;
import java.util.List;

/**
 * Post-Volcano distributed-aggregate decomposition. Sole owner of all
 * {@link AggregateCall#create} construction for the FINAL side of a partial/final
 * aggregate pair.
 *
 * <p>Runs after {@code OpenSearchAggregateSplitRule} has structurally split a SINGLE
 * aggregate into {@code FINAL(ExchangeReducer(StageInputScan))}. Given the FINAL
 * aggregate, classifies each aggregate call via {@link AggregateFunction#intermediateFields}
 * and rewrites accordingly.
 *
 * <p>Three classification outcomes per aggregate:
 * <ul>
 *   <li><b>Pass-through</b> (no intermediate fields): arg list rebased to partial's
 *       output column position, function identity preserved, return type re-inferred.</li>
 *   <li><b>Function-swap</b> (one intermediate field, {@code reducer != self}): arg
 *       rebased, function swapped to the declared reducer, exchange column type from
 *       the intermediate field (Arrow-derived). COUNT→SUM is the canonical case.</li>
 *   <li><b>Engine-native merge</b> (one intermediate field, {@code reducer == self}):
 *       function identity preserved, arg rebased, exchange column type from the
 *       intermediate field, return type kept explicit (Calcite cannot infer
 *       {@code approx_count_distinct(Binary)} through its standard signature registry).</li>
 * </ul>
 *
 * <p>Relies on the split rule's structural guarantee that FINAL's input is an
 * ExchangeReducer with a single StageInputScan child. Direct structural access —
 * no recursive tree walks.
 *
 * @opensearch.internal
 */
final class DistributedAggregateRewriter {

    private DistributedAggregateRewriter() {}

    /**
     * Apply the rewrite to a FINAL-mode {@link OpenSearchAggregate}. Returns the input
     * unchanged if the structure doesn't match the split rule's expected shape
     * ({@code FINAL → ExchangeReducer → StageInputScan}) — no-op safety.
     */
    static RelNode rewrite(OpenSearchAggregate finalAgg) {
        RelNode exchange = finalAgg.getInput();
        if (exchange.getInputs().isEmpty()) return finalAgg;
        if (!(exchange.getInputs().get(0) instanceof OpenSearchStageInputScan stageInput)) return finalAgg;

        RelDataTypeFactory tf = finalAgg.getCluster().getTypeFactory();
        int groupCount = finalAgg.getGroupSet().cardinality();
        boolean hasEmptyGroup = finalAgg.getGroupSet().isEmpty();

        // Phase 1: classify each aggCall and record any exchange-column type override.
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
                exchangeTypes.set(groupCount + i, ArrowCalciteTypes.toCalcite(field.arrowType(), tf));
            } else {
                throw new IllegalStateException(
                    "Multi-field decomposition for ["
                        + call.getAggregation().getName()
                        + "] should have been reduced by OpenSearchAggregateReduceRule during HEP marking"
                );
            }
            perCallField.add(field);
        }

        // Phase 2: rebuild StageInputScan with overrides (if any), keeping the ExchangeReducer intact.
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

        // Phase 3: build the rewritten aggCalls — single AggregateCall.create site.
        List<AggregateCall> rebuiltCalls = new ArrayList<>(finalAgg.getAggCallList().size());
        for (int i = 0; i < finalAgg.getAggCallList().size(); i++) {
            AggregateCall call = finalAgg.getAggCallList().get(i);
            IntermediateField field = perCallField.get(i);
            int finalArgIdx = groupCount + i;
            String name = overriddenExchangeType.getFieldList().get(finalArgIdx).getName();
            rebuiltCalls.add(buildFinalCall(call, field, finalArgIdx, name, newFinalInput, hasEmptyGroup));
        }

        return new OpenSearchAggregate(
            finalAgg.getCluster(),
            finalAgg.getTraitSet(),
            newFinalInput,
            finalAgg.getGroupSet(),
            finalAgg.getGroupSets(),
            rebuiltCalls,
            AggregateMode.FINAL,
            finalAgg.getViableBackends()
        );
    }

    // ── Aggregate-call construction (single site) ──

    /**
     * Computes {@code (aggFunc, explicitType)} based on the aggregate's classification,
     * then calls {@link AggregateCall#create} once. When {@code explicitType == null},
     * Calcite infers the return type against {@code newFinalInput}.
     */
    private static AggregateCall buildFinalCall(
        AggregateCall call,
        IntermediateField field,
        int finalArgIdx,
        String name,
        RelNode newFinalInput,
        boolean hasEmptyGroup
    ) {
        SqlAggFunction aggFunc;
        RelDataType explicitType;
        if (field == null) {
            // Pass-through: keep function, Calcite infers type.
            aggFunc = call.getAggregation();
            explicitType = null;
        } else if (field.reducer() == AggregateFunction.fromSqlAggFunction(call.getAggregation())) {
            // Engine-native merge: keep function, preserve declared type (Calcite cannot infer
            // approx_count_distinct(Binary) through its standard signature registry).
            aggFunc = call.getAggregation();
            explicitType = call.getType();
        } else {
            // Function-swap: reducer replaces the function, Calcite infers type.
            aggFunc = field.reducer().toSqlAggFunction();
            explicitType = null;
        }

        return AggregateCall.create(
            aggFunc,
            call.isDistinct(),
            call.isApproximate(),
            call.ignoreNulls(),
            call.rexList,
            List.of(finalArgIdx),
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
