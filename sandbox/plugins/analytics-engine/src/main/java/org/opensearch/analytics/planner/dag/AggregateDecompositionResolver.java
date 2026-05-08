/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
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
            RelNode newChildFragment = replaceTopAggregate(childPlan.resolvedFragment(), partialAgg, result.newPartial(partialAgg));
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
                parentPlan.resolvedFragment(), result.exchangeRowType, childStage.getStageId(), result);
            resolvedParentPlans.add(new StagePlan(rewrittenParent, parentPlan.backendId()));
        }
        parentStage.setPlanAlternatives(resolvedParentPlans);
    }

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
        RelNode newFinalInput = replaceStageInputScan(finalInput, stageInput, newStageInput);

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
        if (result.projectOnTop != null) {
            top = result.projectOnTop.apply(newFinal);
        }

        // If the original fragment had something above the FINAL, replace it
        if (fragment == finalAgg) {
            return top;
        }
        return replaceTopAggregate(fragment, finalAgg, top);
    }

    /**
     * Core decomposition logic. Produces rewritten PARTIAL calls, FINAL calls,
     * the exchange row type (from intermediateFields), and an optional Project wrapper.
     *
     * <p>PARTIAL calls use Calcite-natural types (to pass Aggregate validation).
     * The exchange row type (set on StageInputScan) uses intermediateFields types —
     * this is the single source of truth for what the engine actually emits.
     */
    static RewriteResult rewriteDecomposed(OpenSearchAggregate agg) {
        RelOptCluster cluster = agg.getCluster();
        RelDataTypeFactory tf = cluster.getTypeFactory();
        RexBuilder rb = cluster.getRexBuilder();
        int groupCount = agg.getGroupSet().cardinality();

        List<AggregateCall> newPartialCalls = new ArrayList<>();
        List<AggregateCall> newFinalCalls = new ArrayList<>();
        List<RelDataType> exchangeFieldTypes = new ArrayList<>();
        List<String> exchangeFieldNames = new ArrayList<>();
        List<PendingProject> projects = new ArrayList<>();

        // Group keys pass through to exchange unchanged
        RelDataType inputRowType = agg.getInput().getRowType();
        for (int groupIdx : agg.getGroupSet()) {
            exchangeFieldTypes.add(inputRowType.getFieldList().get(groupIdx).getType());
            exchangeFieldNames.add(inputRowType.getFieldList().get(groupIdx).getName());
        }

        int finalColIdx = groupCount;
        for (AggregateCall call : agg.getAggCallList()) {
            AggregateFunction fn = resolveFunction(call);

            if (fn == null || !fn.hasDecomposition()) {
                // PASS-THROUGH: keep original call, exchange type = call's natural type
                newPartialCalls.add(call);
                newFinalCalls.add(rebindCall(call, List.of(finalColIdx)));
                exchangeFieldTypes.add(call.getType());
                exchangeFieldNames.add(call.name != null ? call.name : "expr$" + finalColIdx);
                finalColIdx += 1;
                continue;
            }

            List<IntermediateField> iFields = fn.intermediateFields();

            if (iFields.size() == 1 && !fn.hasScalarFinal()) {
                IntermediateField f = iFields.get(0);
                RelDataType colType = ArrowCalciteTypes.toCalcite(f.arrowType(), tf);

                // PARTIAL: keep original call (Calcite validates types internally)
                newPartialCalls.add(call);
                // Exchange type comes from intermediateFields
                exchangeFieldTypes.add(colType);
                exchangeFieldNames.add(call.name != null ? call.name : f.name());

                if (fn.equals(f.reducer())) {
                    // ENGINE-NATIVE (DC): keep FINAL call, rebind arg
                    newFinalCalls.add(rebindCall(call, List.of(finalColIdx)));
                } else {
                    // FUNCTION-SWAP (COUNT→SUM): replace function with reducer
                    newFinalCalls.add(makeCall(f.reducer(), List.of(finalColIdx), colType, call.name, tf));
                }
                finalColIdx += 1;
                continue;
            }

            // PRIMITIVE DECOMP (AVG, STDDEV): N partial calls + N final reducers + Project
            int startFinalIdx = finalColIdx;
            for (IntermediateField f : iFields) {
                RelDataType colType = ArrowCalciteTypes.toCalcite(f.arrowType(), tf);
                // PARTIAL: emit the reducer function with original args (Calcite infers type)
                newPartialCalls.add(primitivePartial(call, f, agg.getInput(), tf));
                // Exchange type from intermediateFields
                exchangeFieldTypes.add(colType);
                exchangeFieldNames.add((call.name != null ? call.name : "expr") + "_" + f.name());
                newFinalCalls.add(makeCall(f.reducer(), List.of(finalColIdx), colType,
                    (call.name != null ? call.name : "expr") + "_" + f.name(), tf));
                finalColIdx += 1;
            }
            projects.add(new PendingProject(fn, startFinalIdx, iFields.size(), call.name, call.getType()));
        }

        RelDataType exchangeRowType = tf.createStructType(exchangeFieldTypes, exchangeFieldNames);

        java.util.function.Function<RelNode, RelNode> projectBuilder =
            projects.isEmpty() ? null : buildProjectWrapper(projects, groupCount, newFinalCalls.size(), rb, tf);

        return new RewriteResult(newPartialCalls, newFinalCalls, exchangeRowType, projectBuilder);
    }

    // ── Helpers ──

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

    private static AggregateCall makeCall(
        AggregateFunction reducer,
        List<Integer> args,
        RelDataType returnType,
        String name,
        RelDataTypeFactory tf
    ) {
        SqlAggFunction sqlAgg = toSqlAggFunction(reducer);
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

    private static AggregateCall primitivePartial(
        AggregateCall originalCall,
        IntermediateField field,
        RelNode input,
        RelDataTypeFactory tf
    ) {
        SqlAggFunction sqlAgg = toSqlAggFunction(field.reducer());
        // Use the original call's type for the PARTIAL call — Calcite validates
        // that the call type matches inferReturnType. The exchange row type
        // (StageInputScan) will carry the correct intermediateFields type.
        // For SUM on INTEGER input, Calcite infers INTEGER; for COUNT, BIGINT NOT NULL.
        // We use the original call's type which Calcite already validated.
        RelDataType callType = originalCall.getType();
        return AggregateCall.create(
            sqlAgg,
            originalCall.isDistinct(),
            originalCall.isApproximate(),
            originalCall.ignoreNulls(),
            originalCall.rexList,
            originalCall.getArgList(),
            originalCall.filterArg,
            originalCall.distinctKeys,
            originalCall.collation,
            callType,
            originalCall.name + "_" + field.name()
        );
    }

    private static SqlAggFunction toSqlAggFunction(AggregateFunction fn) {
        return switch (fn) {
            case SUM -> SqlStdOperatorTable.SUM;
            case SUM0 -> SqlStdOperatorTable.SUM0;
            case MIN -> SqlStdOperatorTable.MIN;
            case MAX -> SqlStdOperatorTable.MAX;
            case COUNT -> SqlStdOperatorTable.COUNT;
            case AVG -> SqlStdOperatorTable.AVG;
            case APPROX_COUNT_DISTINCT -> SqlStdOperatorTable.APPROX_COUNT_DISTINCT;
            default -> throw new IllegalStateException("No SqlAggFunction mapping for: " + fn);
        };
    }

    private static AggregateFunction resolveFunction(AggregateCall call) {
        // Try name-based resolution first for functions with SqlKind.OTHER or ambiguous kinds
        String name = call.getAggregation().getName();
        try {
            return AggregateFunction.fromNameOrError(name);
        } catch (IllegalStateException e) {
            // Fall through to SqlKind-based resolution
        }
        return AggregateFunction.fromSqlKind(call.getAggregation().getKind());
    }

    private static java.util.function.Function<RelNode, RelNode> buildProjectWrapper(
        List<PendingProject> projects,
        int groupCount,
        int totalFinalCalls,
        RexBuilder rb,
        RelDataTypeFactory tf
    ) {
        return (RelNode finalAgg) -> {
            int totalFinalCols = groupCount + totalFinalCalls;
            // Build project expressions:
            // 1. Group keys pass through
            // 2. Non-decomposed agg results pass through
            // 3. Primitive-decomp results get finalExpression applied

            List<RexNode> projectExprs = new ArrayList<>();
            List<String> fieldNames = new ArrayList<>();

            // Group keys
            for (int i = 0; i < groupCount; i++) {
                projectExprs.add(rb.makeInputRef(finalAgg.getRowType().getFieldList().get(i).getType(), i));
                fieldNames.add(finalAgg.getRowType().getFieldList().get(i).getName());
            }

            // Walk through FINAL calls, emitting either pass-through refs or finalExpression
            int finalCallIdx = 0;
            int projectIdx = 0;
            int finalFieldOffset = groupCount;

            // We need to reconstruct the original call order.
            // Non-project calls get a direct ref; project calls get the finalExpression.
            // Track which final columns belong to which project.
            java.util.Set<Integer> projectStartCols = new java.util.HashSet<>();
            for (PendingProject p : projects) {
                projectStartCols.add(p.startFinalCol - groupCount);
            }

            int col = 0;
            int pIdx = 0;
            while (col < totalFinalCalls) {
                // Check if this col starts a project
                PendingProject matchedProject = null;
                for (PendingProject p : projects) {
                    if (p.startFinalCol - groupCount == col) {
                        matchedProject = p;
                        break;
                    }
                }

                if (matchedProject != null) {
                    // Build finalExpression from the N columns
                    List<RexNode> partialRefs = new ArrayList<>();
                    for (int i = 0; i < matchedProject.fieldCount; i++) {
                        int refIdx = groupCount + col + i;
                        partialRefs.add(rb.makeInputRef(finalAgg.getRowType().getFieldList().get(refIdx).getType(), refIdx));
                    }
                    RexNode expr = matchedProject.fn.finalExpression().apply(rb, partialRefs);
                    // Cast to original return type if needed
                    if (!expr.getType().equals(matchedProject.originalReturnType)) {
                        expr = rb.makeCast(matchedProject.originalReturnType, expr);
                    }
                    projectExprs.add(expr);
                    fieldNames.add(matchedProject.originalName);
                    col += matchedProject.fieldCount;
                } else {
                    // Pass-through
                    int refIdx = groupCount + col;
                    projectExprs.add(rb.makeInputRef(finalAgg.getRowType().getFieldList().get(refIdx).getType(), refIdx));
                    fieldNames.add(finalAgg.getRowType().getFieldList().get(refIdx).getName());
                    col += 1;
                }
            }

            RelDataType projectRowType = tf.createStructType(
                projectExprs.stream().map(RexNode::getType).toList(),
                fieldNames
            );

            return LogicalProject.create(finalAgg, List.of(), projectExprs, projectRowType);
        };
    }

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

    private static RelNode replaceTopAggregate(RelNode node, OpenSearchAggregate target, RelNode replacement) {
        if (node == target) return replacement;
        List<RelNode> newInputs = new ArrayList<>();
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode newInput = replaceTopAggregate(input, target, replacement);
            newInputs.add(newInput);
            if (newInput != input) changed = true;
        }
        return changed ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    private static RelNode replaceStageInputScan(RelNode node, OpenSearchStageInputScan target, OpenSearchStageInputScan replacement) {
        if (node == target) return replacement;
        List<RelNode> newInputs = new ArrayList<>();
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode newInput = replaceStageInputScan(input, target, replacement);
            newInputs.add(newInput);
            if (newInput != input) changed = true;
        }
        return changed ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    // ── Inner types ──

    record RewriteResult(
        List<AggregateCall> newPartialCalls,
        List<AggregateCall> newFinalCalls,
        RelDataType exchangeRowType,
        java.util.function.Function<RelNode, RelNode> projectOnTop
    ) {
        OpenSearchAggregate newPartial(OpenSearchAggregate original) {
            return copyAgg(original, newPartialCalls);
        }
    }

    private record PendingProject(
        AggregateFunction fn,
        int startFinalCol,
        int fieldCount,
        String originalName,
        RelDataType originalReturnType
    ) {}

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
