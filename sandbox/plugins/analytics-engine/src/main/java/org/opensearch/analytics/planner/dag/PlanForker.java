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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.AggregateDecomposition;
import org.opensearch.analytics.spi.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Generates plan alternatives for each {@link Stage} in a {@link QueryDAG}.
 *
 * <p>Walks each stage's marked fragment bottom-up. For each operator, generates
 * one {@link StagePlan} per viable backend. Annotations are grouped by target
 * backend to avoid combinatorial explosion — with a single backend (pure DF),
 * this naturally produces one alternative per stage.
 *
 * <p>TODO: gate plan forking based on index stats (size, shard count, doc count).
 * For small indices, generating multiple alternatives adds overhead with minimal benefit.
 *
 * <p>TODO: add pruning via BackendPriority and cost functions when multiple backends
 * are viable for the same stage.
 *
 * @opensearch.internal
 */
public class PlanForker {

    private PlanForker() {}

    public static void forkAll(QueryDAG dag, CapabilityRegistry registry) {
        forkStage(dag.rootStage(), registry);
        applyDecompositions(dag.rootStage(), registry);
    }

    private static void forkStage(Stage stage, CapabilityRegistry registry) {
        for (Stage child : stage.getChildStages()) {
            forkStage(child, registry);
        }
        if (stage.getFragment() == null) {
            return;
        }
        List<Resolved> alternatives = resolve(stage.getFragment(), registry);
        stage.setPlanAlternatives(alternatives.stream().map(resolved -> new StagePlan(resolved.node, resolved.chosenBackend)).toList());
    }

    /** Resolved node paired with the backend chosen at this operator level. */
    private record Resolved(String chosenBackend, RelNode node) {
    }

    private static List<Resolved> resolve(RelNode node, CapabilityRegistry registry) {
        List<List<Resolved>> childAlternativeSets = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            childAlternativeSets.add(resolve(input, registry));
        }

        if (childAlternativeSets.isEmpty()) {
            return resolveOperator(node, List.of(), null);
        }

        if (childAlternativeSets.size() == 1) {
            List<Resolved> results = new ArrayList<>();
            for (Resolved childAlt : childAlternativeSets.getFirst()) {
                results.addAll(resolveOperator(node, List.of(childAlt.node), childAlt.chosenBackend));
            }
            return results;
        }

        // TODO: multi-input operators (joins) — each side is typically a separate stage
        // connected via StageInputScan, so this path may not be needed in practice.
        throw new UnsupportedOperationException("Multi-input plan forking not yet supported for: " + node.getClass().getSimpleName());
    }

    private static List<Resolved> resolveOperator(RelNode node, List<RelNode> children, String childBackend) {
        if (!(node instanceof OpenSearchRelNode openSearchNode)) {
            // Non-OpenSearch node (e.g. StageInputScan infrastructure) — pass through.
            RelNode result = children.isEmpty() ? node : node.copy(node.getTraitSet(), children);
            return List.of(new Resolved(childBackend != null ? childBackend : "", result));
        }

        List<OperatorAnnotation> annotations = openSearchNode.getAnnotations();

        // Filter viable backends: only consider backends that match the child's chosen backend.
        // TODO: delegation will change this — cross-backend pipelines require revisiting
        // how the child backend propagates upward through the operator chain.
        List<String> backendsToConsider = new ArrayList<>();
        for (String backend : openSearchNode.getViableBackends()) {
            if (childBackend == null || backend.equals(childBackend)) {
                backendsToConsider.add(backend);
            }
        }

        List<Resolved> results = new ArrayList<>();
        for (String backend : backendsToConsider) {
            if (annotations.isEmpty()) {
                results.add(new Resolved(backend, openSearchNode.copyResolved(backend, children, List.of())));
                continue;
            }
            // Group annotations by target backend — one plan per distinct annotation backend group.
            // With a single backend, this produces exactly one alternative naturally.
            results.addAll(resolveWithBranching(openSearchNode, backend, children, annotations));
        }
        return results;
    }

    private static List<Resolved> resolveWithBranching(
        OpenSearchRelNode node,
        String backend,
        List<RelNode> children,
        List<OperatorAnnotation> annotations
    ) {
        // TODO: delegation will change this — when annotations have viable backends that differ
        // from the operator's backend, generate one plan per distinct annotation target backend
        // (e.g. DF operator with Lucene annotation for filter delegation).
        // For PR2 (no delegation), always resolve annotations to the operator's own backend.
        List<OperatorAnnotation> resolved = resolveAnnotationsToTarget(annotations, backend, backend);
        return List.of(new Resolved(backend, node.copyResolved(backend, children, resolved)));
    }

    private static List<OperatorAnnotation> resolveAnnotationsToTarget(
        List<OperatorAnnotation> annotations,
        String targetBackend,
        String operatorBackend
    ) {
        List<OperatorAnnotation> resolved = new ArrayList<>();
        for (OperatorAnnotation annotation : annotations) {
            if (annotation.getViableBackends().contains(targetBackend)) {
                resolved.add(annotation.narrowTo(targetBackend));
            } else if (annotation.getViableBackends().contains(operatorBackend)) {
                resolved.add(annotation.narrowTo(operatorBackend));
            } else {
                // Fallback: narrow to first viable backend.
                resolved.add(annotation.narrowTo(annotation.getViableBackends().getFirst()));
            }
        }
        return resolved;
    }

    // ── Aggregate decomposition ─────────────────────────────────────────────────

    /**
     * Walks the DAG bottom-up and applies {@link AggregateDecomposition} to any
     * PARTIAL+FINAL aggregate pair that has decomposable functions (e.g. AVG → SUM+COUNT).
     *
     * <p>For each child stage with a PARTIAL aggregate containing decomposable calls:
     * <ol>
     *   <li>Rewrite the PARTIAL's aggCalls using {@code decomposition.partialCalls()}</li>
     *   <li>Find the corresponding FINAL aggregate in the parent stage</li>
     *   <li>Rewrite the FINAL's aggCalls to merge the partial columns</li>
     *   <li>Insert a Project above the FINAL to compute final expressions (e.g. SUM/COUNT)</li>
     * </ol>
     */
    private static void applyDecompositions(Stage stage, CapabilityRegistry registry) {
        for (Stage child : stage.getChildStages()) {
            applyDecompositions(child, registry);
        }
        // Process each plan alternative in child stages
        for (Stage child : stage.getChildStages()) {
            List<StagePlan> childAlts = new ArrayList<>(child.getPlanAlternatives());
            List<StagePlan> parentAlts = new ArrayList<>(stage.getPlanAlternatives());
            boolean childChanged = false;
            boolean parentChanged = false;

            for (int i = 0; i < childAlts.size(); i++) {
                StagePlan childPlan = childAlts.get(i);
                RelNode childFragment = childPlan.resolvedFragment();
                OpenSearchAggregate partialAgg = findAggregate(childFragment, AggregateMode.PARTIAL);
                if (partialAgg == null) continue;

                String backendId = childPlan.backendId();
                DecompositionResult result = decomposePartial(partialAgg, backendId, registry);
                if (result == null) continue;

                // Rewrite child stage's PARTIAL aggregate
                RelNode newChildFragment = replaceAggregate(childFragment, partialAgg, result.newPartialAgg);
                childAlts.set(i, new StagePlan(newChildFragment, backendId));
                childChanged = true;

                // Rewrite parent stage's FINAL aggregate + insert Project
                for (int j = 0; j < parentAlts.size(); j++) {
                    StagePlan parentPlan = parentAlts.get(j);
                    if (!parentPlan.backendId().equals(backendId)) continue;
                    RelNode newParentFragment = rewriteFinalStage(
                        parentPlan.resolvedFragment(), result, partialAgg, registry
                    );
                    if (newParentFragment != null) {
                        parentAlts.set(j, new StagePlan(newParentFragment, backendId));
                        parentChanged = true;
                    }
                }
            }
            if (childChanged) child.setPlanAlternatives(childAlts);
            if (parentChanged) stage.setPlanAlternatives(parentAlts);
        }
    }

    private record DecompositionResult(
        OpenSearchAggregate newPartialAgg,
        /** Maps original aggCall index → list of partial column indices in the new PARTIAL output. */
        List<List<Integer>> partialColumnMapping,
        /** Decompositions keyed by original aggCall index (null if no decomposition for that call). */
        List<AggregateDecomposition> decompositions
    ) {}

    private static DecompositionResult decomposePartial(
        OpenSearchAggregate partialAgg, String backendId, CapabilityRegistry registry
    ) {
        int groupCount = partialAgg.getGroupSet().cardinality();
        List<AggregateCall> newPartialCalls = new ArrayList<>();
        List<List<Integer>> columnMapping = new ArrayList<>();
        List<AggregateDecomposition> decompositions = new ArrayList<>();
        boolean anyDecomposed = false;

        int outputIdx = groupCount;
        for (AggregateCall aggCall : partialAgg.getAggCallList()) {
            AggregateFunction func = AggregateFunction.fromSqlKind(aggCall.getAggregation().getKind());
            if (func == null) {
                try { func = AggregateFunction.fromNameOrError(aggCall.getAggregation().getName()); }
                catch (IllegalStateException ignored) {}
            }
            AggregateDecomposition decomp = func != null ? registry.getDecomposition(backendId, func) : null;
            decompositions.add(decomp);

            if (decomp != null) {
                List<AggregateCall> partialCalls = decomp.partialCalls(aggCall);
                List<Integer> indices = new ArrayList<>();
                for (AggregateCall pc : partialCalls) {
                    AggregateCall typed = fixCallType(pc, partialAgg);
                    newPartialCalls.add(typed);
                    indices.add(outputIdx++);
                }
                columnMapping.add(indices);
                anyDecomposed = true;
            } else {
                newPartialCalls.add(aggCall);
                columnMapping.add(List.of(outputIdx++));
            }
        }

        if (!anyDecomposed) return null;

        OpenSearchAggregate newPartial = new OpenSearchAggregate(
            partialAgg.getCluster(), partialAgg.getTraitSet(), partialAgg.getInput(),
            partialAgg.getGroupSet(), partialAgg.getGroupSets(), newPartialCalls,
            AggregateMode.PARTIAL, partialAgg.getViableBackends()
        );
        return new DecompositionResult(newPartial, columnMapping, decompositions);
    }

    private static RelNode rewriteFinalStage(
        RelNode rootFragment, DecompositionResult result,
        OpenSearchAggregate originalPartialAgg, CapabilityRegistry registry
    ) {
        OpenSearchAggregate finalAgg = findAggregate(rootFragment, AggregateMode.FINAL);
        if (finalAgg == null) return null;

        int groupCount = finalAgg.getGroupSet().cardinality();
        RexBuilder rexBuilder = finalAgg.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = finalAgg.getCluster().getTypeFactory();

        // The new PARTIAL output row type (after decomposition)
        RelDataType newPartialRowType = result.newPartialAgg.getRowType();

        // Update the StageInputScan inside the root fragment to match the new PARTIAL output
        RelNode updatedFragment = updateStageInputRowType(rootFragment, newPartialRowType);
        // Re-find the final agg in the updated tree
        finalAgg = findAggregate(updatedFragment, AggregateMode.FINAL);

        // Build new FINAL aggCalls that merge the decomposed partial columns
        List<AggregateCall> mergeCallsFlat = new ArrayList<>();
        int origCallIdx = 0;
        for (int i = 0; i < result.decompositions.size(); i++) {
            AggregateDecomposition decomp = result.decompositions.get(i);
            List<Integer> partialCols = result.partialColumnMapping.get(i);

            if (decomp != null) {
                List<AggregateCall> partialCalls = decomp.partialCalls(originalPartialAgg.getAggCallList().get(origCallIdx));
                for (int j = 0; j < partialCols.size(); j++) {
                    int partialCol = partialCols.get(j);
                    RelDataType colType = typeFactory.createTypeWithNullability(
                        newPartialRowType.getFieldList().get(partialCol).getType(), true
                    );
                    // Use the same function as the partial call for the merge
                    // (e.g. approx_count_distinct for HLL, SUM for AVG decomposition)
                    AggregateCall mergeCall = AggregateCall.create(
                        partialCalls.get(j).getAggregation(),
                        false, false, false, List.of(), List.of(partialCol),
                        -1, null, org.apache.calcite.rel.RelCollations.EMPTY, colType, null
                    );
                    mergeCallsFlat.add(mergeCall);
                }
            } else {
                AggregateCall origFinalCall = finalAgg.getAggCallList().get(origCallIdx);
                AggregateCall remapped = origFinalCall.adaptTo(
                    finalAgg.getInput(), List.of(partialCols.get(0)),
                    origFinalCall.filterArg, groupCount, originalPartialAgg.getGroupCount()
                );
                mergeCallsFlat.add(remapped);
            }
            origCallIdx++;
        }

        // Create the new FINAL aggregate
        OpenSearchAggregate newFinal = new OpenSearchAggregate(
            finalAgg.getCluster(), finalAgg.getTraitSet(), finalAgg.getInput(),
            finalAgg.getGroupSet(), finalAgg.getGroupSets(), mergeCallsFlat,
            AggregateMode.FINAL, finalAgg.getViableBackends()
        );

        // Build the Project for final expressions (e.g. SUM/COUNT for AVG)
        List<RexNode> projExprs = new ArrayList<>();
        List<String> projNames = new ArrayList<>();
        for (int g = 0; g < groupCount; g++) {
            projExprs.add(rexBuilder.makeInputRef(newFinal, g));
            projNames.add(newFinal.getRowType().getFieldList().get(g).getName());
        }
        int finalMergeIdx = groupCount;
        boolean needsProject = false;
        for (int i = 0; i < result.decompositions.size(); i++) {
            AggregateDecomposition decomp = result.decompositions.get(i);
            List<Integer> partialCols = result.partialColumnMapping.get(i);
            if (decomp != null) {
                List<RexNode> mergeRefs = new ArrayList<>();
                for (int j = 0; j < partialCols.size(); j++) {
                    mergeRefs.add(rexBuilder.makeInputRef(newFinal, finalMergeIdx++));
                }
                projExprs.add(decomp.finalExpression(rexBuilder, mergeRefs));
                needsProject = true;
            } else {
                projExprs.add(rexBuilder.makeInputRef(newFinal, finalMergeIdx++));
            }
            // Use original aggregate's output field name for the final column
            projNames.add(originalPartialAgg.getRowType().getFieldList().get(groupCount + i).getName());
        }

        RelNode finalResult = newFinal;
        if (needsProject) {
            RelDataType projRowType = typeFactory.createStructType(
                projExprs.stream().map(RexNode::getType).toList(), projNames
            );
            finalResult = new OpenSearchProject(
                finalAgg.getCluster(), finalAgg.getTraitSet(), newFinal,
                projExprs, projRowType, finalAgg.getViableBackends()
            );
        }

        return replaceAggregate(updatedFragment, finalAgg, finalResult);
    }

    /** Updates the row type of any StageInputScan in the tree to match the new partial output. */
    private static RelNode updateStageInputRowType(RelNode node, RelDataType newRowType) {
        if (node instanceof OpenSearchStageInputScan scan) {
            return new OpenSearchStageInputScan(
                scan.getCluster(), scan.getTraitSet(), scan.getChildStageId(), newRowType, scan.getViableBackends()
            );
        }
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode updated = updateStageInputRowType(input, newRowType);
            newInputs.add(updated);
            if (updated != input) changed = true;
        }
        return changed ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    private static OpenSearchAggregate findAggregate(RelNode node, AggregateMode mode) {
        if (node instanceof OpenSearchAggregate agg && agg.getMode() == mode) return agg;
        for (RelNode input : node.getInputs()) {
            OpenSearchAggregate found = findAggregate(input, mode);
            if (found != null) return found;
        }
        return null;
    }

    private static RelNode replaceAggregate(RelNode node, OpenSearchAggregate target, RelNode replacement) {
        if (node == target) return replacement;
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode newInput = replaceAggregate(input, target, replacement);
            newInputs.add(newInput);
            if (newInput != input) changed = true;
        }
        return changed ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    /** Infers the correct return type for an aggCall against the given aggregate's input. */
    private static AggregateCall fixCallType(AggregateCall call, OpenSearchAggregate agg) {
        // Build a temp aggregate with empty calls to get a binding context
        org.apache.calcite.rel.logical.LogicalAggregate tempAgg =
            org.apache.calcite.rel.logical.LogicalAggregate.create(
                agg.getInput(), List.of(), agg.getGroupSet(), agg.getGroupSets(), List.of()
            );
        RelDataType inferred = call.getAggregation().inferReturnType(call.createBinding(tempAgg));
        if (inferred.equals(call.type)) return call;
        return AggregateCall.create(
            call.getAggregation(), call.isDistinct(), call.isApproximate(), call.ignoreNulls(),
            call.rexList, call.getArgList(), call.filterArg, call.distinctKeys,
            call.collation, inferred, call.name
        );
    }
}
