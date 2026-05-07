/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;

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

        // Multi-input operator (Union, coord-side Join, future set-ops). Enumerate the
        // cartesian product of child plan alternatives, keeping only combinations where
        // every child agrees on a single backend — a multi-input operator cannot straddle
        // backends within a single stage. With a single backend (pure DataFusion today)
        // every alternative shares the same backend and the product collapses to one combo;
        // with N backends viable at every branch, the product produces at most N combos
        // per operator (one per backend tuple that survives the agreement filter).
        List<Resolved> results = new ArrayList<>();
        for (List<Resolved> childAlts : childAlternativeSets) {
            if (childAlts.isEmpty()) {
                throw new IllegalStateException(
                    "Multi-input child of [" + node.getClass().getSimpleName() + "] produced no plan alternatives"
                );
            }
        }
        crossProduct(childAlternativeSets, 0, new ArrayList<>(), null, (chosenBackend, combo) -> {
            results.addAll(resolveOperator(node, combo, chosenBackend));
        });
        return results;
    }

    private interface ComboHandler {
        void accept(String chosenBackend, List<RelNode> combo);
    }

    private static void crossProduct(
        List<List<Resolved>> childAlternativeSets,
        int depth,
        List<RelNode> partial,
        String agreedBackend,
        ComboHandler handler
    ) {
        if (depth == childAlternativeSets.size()) {
            handler.accept(agreedBackend, List.copyOf(partial));
            return;
        }
        for (Resolved alt : childAlternativeSets.get(depth)) {
            // All children must converge on the same backend so the parent operator's
            // backend resolution stays unambiguous. The null / empty chosenBackend case
            // (non-OpenSearch pass-through nodes, e.g. StageInputScan infrastructure)
            // does not constrain the agreed backend.
            boolean childContributesBackend = alt.chosenBackend != null && !alt.chosenBackend.isEmpty();
            if (agreedBackend != null && childContributesBackend && !agreedBackend.equals(alt.chosenBackend)) {
                continue;
            }
            String next = agreedBackend != null ? agreedBackend : (childContributesBackend ? alt.chosenBackend : null);
            partial.add(alt.node);
            crossProduct(childAlternativeSets, depth + 1, partial, next, handler);
            partial.removeLast();
        }
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
}
