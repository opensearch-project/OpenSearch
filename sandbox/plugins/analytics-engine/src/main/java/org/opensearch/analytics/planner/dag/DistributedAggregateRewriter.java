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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Adapts a FINAL {@link OpenSearchAggregate}'s input chain after the DAG is fragmented:
 * re-types the StageInputScan, inserts a literal-Project for STATE_EXPANDING aggregates, and
 * rebinds aggCalls via {@link FinalAggCallBuilder} when anything changed. Called from
 * {@code BackendPlanAdapter.adaptNode}.
 *
 * <p>TODO: {@link FinalAggCallBuilder} is also used by the split rule, so {@code planner.rules}
 * reaches into {@code planner.dag} — backwards from the usual layering. Move it to
 * {@code planner.rel} once a third construction-time consumer appears.
 *
 * @opensearch.internal
 */
public final class DistributedAggregateRewriter {

    private DistributedAggregateRewriter() {}

    static RelNode rewrite(OpenSearchAggregate finalAgg) {
        assert finalAgg.getMode() == AggregateMode.FINAL : "rewrite is only called on FINAL aggregates";
        RelNode exchange = finalAgg.getInput();
        if (exchange.getInputs().isEmpty()) return finalAgg;
        if (!(exchange.getInputs().get(0) instanceof OpenSearchStageInputScan)) return finalAgg;

        TransformResult result = TransformResult.initial(exchange);
        result = overrideExchangeType(finalAgg, result);
        result = insertLiteralProject(finalAgg, result);
        if (!result.changed(exchange)) return finalAgg;

        List<AggregateCall> rebuiltCalls = FinalAggCallBuilder.buildFinalCalls(
            finalAgg.getAggCallList(),
            finalAgg.getIntermediateFields(),
            finalAgg.getGroupSet().cardinality(),
            result.finalInput(),
            finalAgg.getGroupSet().isEmpty(),
            result.extraLiteralColIdxByCallIdx()
        );
        return OpenSearchAggregate.finalAfterRewrite(finalAgg, result.finalInput(), rebuiltCalls);
    }

    /** Carries the running input chain plus the per-call indexes of any literal columns added. */
    private record TransformResult(RelNode finalInput, Map<Integer, List<Integer>> extraLiteralColIdxByCallIdx) {

        static TransformResult initial(RelNode finalInput) {
            return new TransformResult(finalInput, Map.of());
        }

        boolean changed(RelNode original) {
            return finalInput != original || !extraLiteralColIdxByCallIdx.isEmpty();
        }
    }

    // ── Transformer 1: StageInputScan re-typing ─────────────────────────────

    /**
     * Re-types the StageInputScan when an aggregate's intermediate state has a different shape
     * than PARTIAL's declared output (e.g. APPROX_COUNT_DISTINCT emits a {@code VARBINARY} HLL
     * sketch). Reads the per-call classification stored on FINAL — never re-classifies.
     */
    private static TransformResult overrideExchangeType(OpenSearchAggregate finalAgg, TransformResult current) {
        RelNode exchange = current.finalInput();
        if (exchange.getInputs().isEmpty()) return current;
        if (!(exchange.getInputs().get(0) instanceof OpenSearchStageInputScan stageInput)) return current;

        List<IntermediateField> intermediateFields = finalAgg.getIntermediateFields();
        if (intermediateFields.isEmpty()) return current;

        RelDataTypeFactory typeFactory = finalAgg.getCluster().getTypeFactory();
        RelDataType stageInputType = stageInput.getRowType();
        int groupCount = finalAgg.getGroupSet().cardinality();

        RelDataTypeFactory.Builder rebuiltType = typeFactory.builder();
        boolean changed = false;
        for (int idx = 0; idx < stageInputType.getFieldCount(); idx++) {
            RelDataTypeField column = stageInputType.getFieldList().get(idx);
            // Group keys (idx < groupCount) keep their type; agg columns resolve through the SPI.
            RelDataType resolvedType = resolveColumnType(column, idx - groupCount, intermediateFields, typeFactory);
            if (!resolvedType.equals(column.getType())) changed = true;
            rebuiltType.add(column.getName(), resolvedType);
        }
        if (!changed) return current;

        OpenSearchStageInputScan rebuiltStageInput = new OpenSearchStageInputScan(
            stageInput.getCluster(),
            stageInput.getTraitSet(),
            stageInput.getChildStageId(),
            rebuiltType.build(),
            stageInput.getViableBackends(),
            stageInput.getOutputFieldStorage()
        );
        RelNode rebuiltExchange = exchange.copy(exchange.getTraitSet(), List.of(rebuiltStageInput));
        return new TransformResult(rebuiltExchange, current.extraLiteralColIdxByCallIdx());
    }

    private static RelDataType resolveColumnType(
        RelDataTypeField column,
        int callIdx,
        List<IntermediateField> intermediateFields,
        RelDataTypeFactory typeFactory
    ) {
        if (callIdx < 0 || callIdx >= intermediateFields.size()) return column.getType();
        IntermediateField field = intermediateFields.get(callIdx);
        if (field == null) return column.getType();
        return field.typeResolver().resolve(List.of(column.getType()), typeFactory);
    }

    // ── Transformer 2: literal-Project insertion ────────────────────────────

    private static final String LITERAL_COL_PREFIX = "$lit_call";

    /**
     * Re-introduces a STATE_EXPANDING aggregate's literal config args (e.g. {@code take(field, 10)}'s
     * {@code 10}) as constant columns above the StageInputScan. PARTIAL emits only state, so FINAL
     * needs the literals projected back in to reference them by index.
     */
    private static TransformResult insertLiteralProject(OpenSearchAggregate finalAgg, TransformResult current) {
        Map<Integer, List<RexLiteral>> literalsByCall = finalAgg.getFinalExtraLiteralArgs();
        if (literalsByCall.isEmpty()) return current;

        RelNode input = current.finalInput();
        RelDataType inputType = input.getRowType();
        RelDataTypeFactory.Builder projectType = finalAgg.getCluster().getTypeFactory().builder();
        List<RexNode> projectExpressions = new ArrayList<>(inputType.getFieldCount() + literalsByCall.size());

        // Forward existing input columns unchanged.
        for (int idx = 0; idx < inputType.getFieldCount(); idx++) {
            RelDataType columnType = inputType.getFieldList().get(idx).getType();
            String columnName = inputType.getFieldList().get(idx).getName();
            projectExpressions.add(new RexInputRef(idx, columnType));
            projectType.add(columnName, columnType);
        }

        // Append one column per captured literal; record where each call's literals landed.
        Map<Integer, List<Integer>> literalColumnsByCall = new LinkedHashMap<>();
        for (Map.Entry<Integer, List<RexLiteral>> entry : literalsByCall.entrySet()) {
            int callIdx = entry.getKey();
            List<RexLiteral> literals = entry.getValue();
            List<Integer> columnIdxs = new ArrayList<>(literals.size());
            for (int litIdx = 0; litIdx < literals.size(); litIdx++) {
                RexLiteral literal = literals.get(litIdx);
                columnIdxs.add(projectExpressions.size()); // capture index BEFORE adding
                projectExpressions.add(literal);
                projectType.add(LITERAL_COL_PREFIX + callIdx + "_" + litIdx, literal.getType());
            }
            literalColumnsByCall.put(callIdx, List.copyOf(columnIdxs));
        }

        OpenSearchProject project = new OpenSearchProject(
            input.getCluster(),
            input.getTraitSet(),
            input,
            projectExpressions,
            projectType.build(),
            finalAgg.getViableBackends()
        );
        return new TransformResult(project, Map.copyOf(literalColumnsByCall));
    }

    // ── FinalAggCallBuilder: construction-time aggCall factory ──────────────

    /**
     * Construction-time factory for FINAL aggCalls. Owns argList rebase to {@code groupCount + i},
     * function swap (e.g. COUNT→SUM), and return-type pinning. Pure: no rel-tree walking.
     *
     * <p>Called by the split rule (so Volcano's {@code typeMatchesInferred} passes) and again by
     * {@link #rewrite} when an input adapter changed the chain — idempotent given the same
     * {@code intermediateFields} list.
     */
    public static final class FinalAggCallBuilder {

        private FinalAggCallBuilder() {}

        /**
         * Returns the {@link IntermediateField} per call (parallel to {@code originalCalls}).
         * Run on the ORIGINAL aggCalls — classifying a post-swap call could pick the wrong
         * reducer. A {@code null} entry means the aggregate has no SPI decomposition; FINAL
         * passes such a call through unchanged.
         */
        public static List<IntermediateField> classify(List<AggregateCall> originalCalls) {
            List<IntermediateField> result = new ArrayList<>(originalCalls.size());
            for (AggregateCall call : originalCalls) {
                List<IntermediateField> fields = AggregateFunction.fromSqlAggFunction(call.getAggregation()).intermediateFields();
                if (fields == null) {
                    result.add(null);
                } else if (fields.size() == 1) {
                    result.add(fields.get(0));
                } else {
                    // Multi-field decompositions (AVG, STDDEV, ...) must be reduced upstream.
                    throw new IllegalStateException(
                        "Aggregate [" + call.getAggregation().getName() + "] has multiple intermediate fields; expected at most one"
                    );
                }
            }
            // List.copyOf would NPE on the null pass-through entries.
            return Collections.unmodifiableList(result);
        }

        /** Overload for callers without literal-arg columns to forward (the common case). */
        public static List<AggregateCall> buildFinalCalls(
            List<AggregateCall> calls,
            List<IntermediateField> intermediateFields,
            int groupCount,
            RelNode finalInput,
            boolean hasEmptyGroup
        ) {
            return buildFinalCalls(calls, intermediateFields, groupCount, finalInput, hasEmptyGroup, Map.of());
        }

        /**
         * Rebuilds FINAL aggCalls bound against {@code finalInput}. Each call's argList becomes
         * {@code [groupCount + i, ...extras]}; the function is swapped per its
         * {@link IntermediateField#reducer}; the return type is re-inferred or pinned per the
         * engine-native-merge rules.
         */
        public static List<AggregateCall> buildFinalCalls(
            List<AggregateCall> calls,
            List<IntermediateField> intermediateFields,
            int groupCount,
            RelNode finalInput,
            boolean hasEmptyGroup,
            Map<Integer, List<Integer>> extraLiteralColIdxByCallIdx
        ) {
            List<AggregateCall> rebuilt = new ArrayList<>(calls.size());
            for (int i = 0; i < calls.size(); i++) {
                AggregateCall call = calls.get(i);
                int stateColIdx = groupCount + i;
                String name = finalInput.getRowType().getFieldList().get(stateColIdx).getName();
                List<Integer> extraColIdxs = extraLiteralColIdxByCallIdx.getOrDefault(i, List.of());
                rebuilt.add(buildOne(call, intermediateFields.get(i), stateColIdx, extraColIdxs, name, finalInput, hasEmptyGroup));
            }
            return rebuilt;
        }

        private static AggregateCall buildOne(
            AggregateCall call,
            IntermediateField field,
            int finalArgIdx,
            List<Integer> extraColIdxs,
            String name,
            RelNode finalInput,
            boolean hasEmptyGroup
        ) {
            SqlAggFunction aggFunc;
            RelDataType explicitType;
            if (field == null) {
                // No decomposition — pass through, let Calcite re-infer.
                aggFunc = call.getAggregation();
                explicitType = null;
            } else {
                AggregateFunction enumFn = AggregateFunction.fromSqlAggFunction(call.getAggregation());
                boolean isEngineNativeMerge = field.reducer() == enumFn;
                if (isEngineNativeMerge) {
                    // Same function on FINAL: STATE_EXPANDING pins to the state column type;
                    // others keep the user-facing return type.
                    aggFunc = call.getAggregation();
                    explicitType = enumFn.getType() == AggregateFunction.Type.STATE_EXPANDING
                        ? finalInput.getRowType().getFieldList().get(finalArgIdx).getType()
                        : call.getType();
                } else {
                    // Function swap (COUNT → SUM, etc.). Let Calcite re-infer.
                    aggFunc = field.reducer().toSqlAggFunction();
                    explicitType = null;
                }
            }

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
                -1,  // filterArg dropped: FILTER consumed by PARTIAL on raw rows; FINAL only merges states.
                call.distinctKeys,
                call.collation,
                hasEmptyGroup,
                finalInput,
                explicitType,
                name
            );
        }
    }
}
