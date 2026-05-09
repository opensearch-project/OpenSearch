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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.ArrowCalciteTypes;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AggregateFunction.IntermediateField;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites PARTIAL/FINAL aggregate pairs so the exchange row type precisely
 * describes what the engine emits. Uses {@link AggregateFunction#intermediateFields()}
 * as the single source of truth — no downstream type overrides needed.
 *
 * <p>Runs after {@link BackendPlanAdapter} and before {@link FragmentConversionDriver}.
 *
 * @opensearch.internal
 */
public final class AggregateDecompositionResolver {

    private static final Logger LOGGER = LogManager.getLogger(AggregateDecompositionResolver.class);

    private AggregateDecompositionResolver() {}

    /**
     * Walk the DAG and rewrite all PARTIAL/FINAL aggregate pairs in each stage's plan alternatives.
     */
    public static void resolveAll(QueryDAG dag, CapabilityRegistry registry) {
        resolveStage(dag.rootStage(), registry);
    }

    // Walk children first (post-order), then pair each child's PARTIAL with this stage's FINAL.
    private static void resolveStage(Stage stage, CapabilityRegistry registry) {
        for (Stage child : stage.getChildStages()) {
            resolveStage(child, registry);
        }

        // For each child stage that has a PARTIAL aggregate, rewrite the parent's FINAL.
        // The parent stage's planAlternatives contain the FINAL; child's contain the PARTIAL.
        for (Stage child : stage.getChildStages()) {
            resolvePartialFinalPair(stage, child);
        }
    }

    // For one parent/child stage pair: rewrite each child PARTIAL, then apply the matching FINAL rewrite in the parent.
    private static void resolvePartialFinalPair(Stage parentStage, Stage childStage) {
        List<StagePlan> resolvedChildPlans = new ArrayList<>(childStage.getPlanAlternatives().size());
        List<StagePlan> resolvedParentPlans = new ArrayList<>(parentStage.getPlanAlternatives().size());
        List<RewriteResult> rewriteResults = new ArrayList<>();

        // Process child plans — rewrite PARTIAL aggregates and collect rewrite results
        for (StagePlan childPlan : childStage.getPlanAlternatives()) {
            OpenSearchAggregate partialAgg = findTopAggregate(childPlan.resolvedFragment(), AggregateMode.PARTIAL);
            if (partialAgg == null) {
                resolvedChildPlans.add(childPlan);
                rewriteResults.add(null);
                continue;
            }
            RewriteResult result = rewriteDecomposed(partialAgg);
            rewriteResults.add(result);
            RelNode newChildFragment = replaceFirst(childPlan.resolvedFragment(), partialAgg, result.newPartial(partialAgg));
            resolvedChildPlans.add(new StagePlan(newChildFragment, childPlan.backendId()));
        }

        // If no child had a PARTIAL, nothing to do
        boolean anyChildRewritten = rewriteResults.stream().anyMatch(r -> r != null);
        if (!anyChildRewritten) return;

        childStage.setPlanAlternatives(resolvedChildPlans);

        // Process parent plans — rewrite FINAL aggregates using the rewrite results from child
        for (int i = 0; i < parentStage.getPlanAlternatives().size(); i++) {
            StagePlan parentPlan = parentStage.getPlanAlternatives().get(i);
            RewriteResult result = rewriteResults.get(Math.min(i, rewriteResults.size() - 1));
            if (result == null) {
                resolvedParentPlans.add(parentPlan);
                continue;
            }

            RelNode rewrittenParent = rewriteParentFragment(
                parentPlan.resolvedFragment(),
                result.exchangeRowType,
                childStage.getStageId(),
                result
            );
            resolvedParentPlans.add(new StagePlan(rewrittenParent, parentPlan.backendId()));
        }
        parentStage.setPlanAlternatives(resolvedParentPlans);
    }

    // Apply a child's RewriteResult to one parent fragment: update the StageInputScan's row type and swap in the new FINAL aggCalls.
    private static RelNode rewriteParentFragment(RelNode fragment, RelDataType childRowType, int childStageId, RewriteResult result) {
        // Walk the parent fragment to find the FINAL aggregate and its StageInputScan
        OpenSearchAggregate finalAgg = findTopAggregate(fragment, AggregateMode.FINAL);
        if (finalAgg == null) return fragment;

        // Find the StageInputScan under the FINAL (through ExchangeReducer)
        RelNode finalInput = finalAgg.getInput();
        OpenSearchStageInputScan stageInput = findStageInputScan(finalInput, childStageId);
        if (stageInput == null) return fragment;

        // Rebuild with updated StageInputScan row type
        OpenSearchStageInputScan newStageInput = new OpenSearchStageInputScan(
            stageInput.getCluster(),
            stageInput.getTraitSet(),
            stageInput.getChildStageId(),
            childRowType,
            stageInput.getViableBackends()
        );

        // Rebuild the chain: StageInputScan → ExchangeReducer → FINAL Agg
        RelNode newFinalInput = replaceFirst(finalInput, stageInput, newStageInput);

        // Build the new FINAL with the rewrite result's final calls and updated input
        OpenSearchAggregate newFinal = new OpenSearchAggregate(
            finalAgg.getCluster(),
            finalAgg.getTraitSet(),
            newFinalInput,
            finalAgg.getGroupSet(),
            finalAgg.getGroupSets(),
            result.newFinalCalls,
            AggregateMode.FINAL,
            finalAgg.getViableBackends()
        );

        RelNode top = newFinal;

        // If the original fragment had something above the FINAL, replace it
        if (fragment == finalAgg) {
            return top;
        }
        return replaceFirst(fragment, finalAgg, top);
    }

    /**
     * Core decomposition logic. Produces rewritten PARTIAL calls, FINAL calls, and the
     * exchange row type (from intermediateFields). Per-call classification is delegated
     * to {@link #rewriteAggCall}, which returns one immutable {@link CallRewrite} per
     * input aggregate call — keeping the four output columns (partial, final, exchange
     * type, exchange name) in lockstep.
     *
     * <p>PARTIAL calls use Calcite-natural types (to pass Aggregate validation). The
     * exchange row type (set on StageInputScan) uses intermediateFields types — this
     * is the single source of truth for what the engine actually emits.
     */
    static RewriteResult rewriteDecomposed(OpenSearchAggregate agg) {
        RelDataTypeFactory tf = agg.getCluster().getTypeFactory();
        int groupCount = agg.getGroupSet().cardinality();

        List<AggregateCall> newPartialCalls = new ArrayList<>();
        List<AggregateCall> newFinalCalls = new ArrayList<>();
        List<RelDataType> exchangeFieldTypes = new ArrayList<>();
        List<String> exchangeFieldNames = new ArrayList<>();

        // Group keys pass through to exchange unchanged.
        RelDataType inputRowType = agg.getInput().getRowType();
        for (int groupIdx : agg.getGroupSet()) {
            exchangeFieldTypes.add(inputRowType.getFieldList().get(groupIdx).getType());
            exchangeFieldNames.add(inputRowType.getFieldList().get(groupIdx).getName());
        }

        int finalColIdx = groupCount;
        for (AggregateCall call : agg.getAggCallList()) {
            CallRewrite rw = rewriteAggCall(call, finalColIdx, tf);
            newPartialCalls.add(rw.partialCall());
            newFinalCalls.add(rw.finalCall());
            exchangeFieldTypes.add(rw.exchangeType());
            exchangeFieldNames.add(rw.exchangeName());
            finalColIdx++;
        }

        RelDataType exchangeRowType = tf.createStructType(exchangeFieldTypes, exchangeFieldNames);
        return new RewriteResult(newPartialCalls, newFinalCalls, exchangeRowType);
    }

    // Classify an AggregateCall and dispatch to the matching rewrite (pass-through or single-field).
    private static CallRewrite rewriteAggCall(AggregateCall call, int finalColIdx, RelDataTypeFactory tf) {
        AggregateFunction fn = AggregateFunction.fromSqlAggFunction(call.getAggregation());

        if (fn == null || !fn.hasDecomposition()) {
            return passThroughRewrite(call, finalColIdx);
        }

        List<IntermediateField> iFields = fn.intermediateFields();

        // Multi-field shapes (AVG / STDDEV / VAR) should have been reduced in HEP by
        // OpenSearchAggregateReduceRule before reaching this resolver. If we see one here,
        // FUNCTIONS_TO_REDUCE in that rule is incomplete.
        if (iFields.size() != 1) {
            throw new IllegalStateException(
                "AggregateFunction."
                    + fn
                    + " declares a multi-field decomposition, but the resolver only"
                    + " supports single-field engine-native / function-swap shapes."
                    + " Calcite's AggregateReduceFunctionsRule should reduce multi-field"
                    + " cases during HEP marking. Check that"
                    + " OpenSearchAggregateReduceRule's FUNCTIONS_TO_REDUCE set covers "
                    + call.getAggregation().getName()
                    + "."
            );
        }

        return singleFieldRewrite(call, fn, iFields.get(0), finalColIdx, tf);
    }

    // Pass-through: aggregate has no intermediate-field decomposition; keep the call at PARTIAL
    // and rebind its single arg index at FINAL. Exchange column takes the call's Calcite type.
    private static CallRewrite passThroughRewrite(AggregateCall call, int finalColIdx) {
        return new CallRewrite(
            call,
            rebindCall(call, List.of(finalColIdx)),
            call.getType(),
            call.name != null ? call.name : "expr$" + finalColIdx
        );
    }

    // Single-field decomposition: exchange type comes from IntermediateField; FINAL is either
    // engine-native merge (reducer == self, e.g. APPROX_COUNT_DISTINCT sketch) or function-swap
    // (e.g. COUNT → SUM).
    private static CallRewrite singleFieldRewrite(
        AggregateCall call,
        AggregateFunction fn,
        IntermediateField field,
        int finalColIdx,
        RelDataTypeFactory tf
    ) {
        RelDataType colType = ArrowCalciteTypes.toCalcite(field.arrowType(), tf);
        AggregateCall finalCall = fn.equals(field.reducer())
            ? rebindCall(call, List.of(finalColIdx))                                // engine-native merge (reducer == self)
            : makeCall(field.reducer(), List.of(finalColIdx), colType, call.name, tf); // function-swap

        return new CallRewrite(call, finalCall, colType, call.name != null ? call.name : field.name());
    }

    // ── Helpers ──

    // Copy an AggregateCall with its argument ordinals remapped to the decomposed column positions.
    private static AggregateCall rebindCall(AggregateCall call, List<Integer> newArgs) {
        return AggregateCall.create(
            call.getAggregation(),
            call.isDistinct(),
            call.isApproximate(),
            call.ignoreNulls(),
            call.rexList,
            newArgs,
            call.filterArg,
            call.distinctKeys,
            call.collation,
            call.getType(),
            call.name
        );
    }

    // Build a fresh AggregateCall for a reducer function at FINAL (no distinct, no filter, empty collation).
    private static AggregateCall makeCall(
        AggregateFunction reducer,
        List<Integer> args,
        RelDataType returnType,
        String name,
        RelDataTypeFactory tf
    ) {
        SqlAggFunction sqlAgg = reducer.toSqlAggFunction();
        return AggregateCall.create(
            sqlAgg,
            false,
            false,
            false,
            List.of(),
            args,
            -1,
            null,
            org.apache.calcite.rel.RelCollations.EMPTY,
            returnType,
            name
        );
    }

    // Find the top-most OpenSearchAggregate matching the given mode, walking into inputs recursively.
    private static OpenSearchAggregate findTopAggregate(RelNode node, AggregateMode mode) {
        if (node instanceof OpenSearchAggregate agg && agg.getMode() == mode) {
            return agg;
        }
        // Check if it's wrapped (e.g., Project on top of FINAL)
        for (RelNode input : node.getInputs()) {
            OpenSearchAggregate found = findTopAggregate(input, mode);
            if (found != null) return found;
        }
        return null;
    }

    // Find the StageInputScan for the given child stage id, walking into inputs recursively.
    private static OpenSearchStageInputScan findStageInputScan(RelNode node, int childStageId) {
        if (node instanceof OpenSearchStageInputScan scan && scan.getChildStageId() == childStageId) {
            return scan;
        }
        for (RelNode input : node.getInputs()) {
            OpenSearchStageInputScan found = findStageInputScan(input, childStageId);
            if (found != null) return found;
        }
        return null;
    }

    /**
     * Identity-based RelNode tree rewrite: returns a copy of {@code node} in which the
     * subtree at {@code target} (matched by reference equality) has been replaced with
     * {@code replacement}. Used to swap a rewritten aggregate back into its fragment
     * and to swap an updated StageInputScan into the FINAL subtree.
     */
    private static RelNode replaceFirst(RelNode node, RelNode target, RelNode replacement) {
        if (node == target) return replacement;
        List<RelNode> newInputs = new ArrayList<>();
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode newInput = replaceFirst(input, target, replacement);
            newInputs.add(newInput);
            if (newInput != input) changed = true;
        }
        return changed ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    // ── Inner types ──

    record RewriteResult(List<AggregateCall> newPartialCalls, List<AggregateCall> newFinalCalls, RelDataType exchangeRowType) {
        OpenSearchAggregate newPartial(OpenSearchAggregate original) {
            return copyAgg(original, newPartialCalls);
        }
    }

    // Per-aggCall rewrite: what to emit at PARTIAL, FINAL, and the exchange column.
    private record CallRewrite(AggregateCall partialCall, AggregateCall finalCall, RelDataType exchangeType, String exchangeName) {
    }

    // Shallow-copy an OpenSearchAggregate with a new aggCall list, preserving traits, group sets, and input.
    private static OpenSearchAggregate copyAgg(OpenSearchAggregate original, List<AggregateCall> newCalls) {
        return (OpenSearchAggregate) original.copy(
            original.getTraitSet(),
            original.getInput(),
            original.getGroupSet(),
            original.getGroupSets(),
            newCalls
        );
    }
}
