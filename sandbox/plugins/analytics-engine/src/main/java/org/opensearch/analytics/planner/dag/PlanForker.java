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
import org.opensearch.analytics.spi.OperatorCapability;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Generates plan alternatives for each {@link Stage} in a {@link QueryDAG}.
 *
 * <p>Walks each stage's marked fragment bottom-up, propagating the child's chosen
 * backend upward. An operator can use a different backend than its child only if
 * that backend declares the operator as arrow-compatible (can operate on Arrow
 * batches from another backend's output). Annotations are grouped by backend to
 * avoid combinatorial explosion. Operators with delegation advantage (e.g., filter)
 * branch across annotation groups; others inherit the pipeline backend.
 *
 * @opensearch.internal
 */
// TODO: gate plan forking based on index stats (size, shard count, doc count).
// For small indices, generating multiple alternatives adds overhead with minimal benefit.
// Consider a threshold-based fast path: small index → single plan (primary backend only).
// Also consider benchmark-driven pruning via external config for operator-level backend choices.
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
        stage.setPlanAlternatives(alternatives.stream()
            .map(resolved -> new StagePlan(resolved.node))
            .toList());
    }

    /** Resolved node paired with the backend chosen at this operator level. */
    private record Resolved(String chosenBackend, RelNode node) {}

    private static List<Resolved> resolve(RelNode node, CapabilityRegistry registry) {
        List<List<Resolved>> childAlternativeSets = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            childAlternativeSets.add(resolve(input, registry));
        }

        if (childAlternativeSets.isEmpty()) {
            return resolveOperator(node, List.of(), null, registry);
        }

        if (childAlternativeSets.size() == 1) {
            List<Resolved> results = new ArrayList<>();
            for (Resolved childAlt : childAlternativeSets.getFirst()) {
                results.addAll(resolveOperator(node, List.of(childAlt.node),
                    childAlt.chosenBackend, registry));
            }
            return results;
        }

        // TODO: multi-input operators (joins). Each side will typically be a separate
        // stage connected via StageInputScan, so this path may not be needed.
        throw new UnsupportedOperationException(
            "Multi-input plan forking not yet supported for: " + node.getClass().getSimpleName());
    }

    private static List<Resolved> resolveOperator(RelNode node, List<RelNode> children,
                                                   String childBackend,
                                                   CapabilityRegistry registry) {
        if (!(node instanceof OpenSearchRelNode openSearchNode)) {
            RelNode result = children.isEmpty() ? node : node.copy(node.getTraitSet(), children);
            String backend = childBackend != null ? childBackend : "";
            return List.of(new Resolved(backend, result));
        }

        List<OperatorAnnotation> annotations = openSearchNode.getAnnotations();

        // Filter viable backends: if child chose a backend, only consider backends
        // that are either the same as child's or arrow-compatible for this operator.
        OperatorCapability operatorCap = openSearchNode.getOperatorCapability();
        List<String> backendsToConsider = new ArrayList<>();
        for (String backend : openSearchNode.getViableBackends()) {
            if (childBackend == null || backend.equals(childBackend)
                    || (operatorCap != null && registry.isArrowCompatible(backend, operatorCap))) {
                backendsToConsider.add(backend);
            }
        }

        List<Resolved> results = new ArrayList<>();
        for (String backend : backendsToConsider) {
            if (annotations.isEmpty()) {
                results.add(new Resolved(backend,
                    openSearchNode.copyResolved(backend, children, List.of())));
                continue;
            }

            // Group-based annotation resolution: one plan per distinct backend group.
            // Naturally produces 1 group when all annotations share the same viableBackends.
            results.addAll(resolveWithBranching(openSearchNode, backend, children, annotations));
        }
        return results;
    }

    /** Generates one plan per distinct backend group across annotations. */
    private static List<Resolved> resolveWithBranching(OpenSearchRelNode node, String backend,
                                                       List<RelNode> children,
                                                       List<OperatorAnnotation> annotations) {
        Set<String> allAnnotationBackends = new LinkedHashSet<>();
        for (OperatorAnnotation annotation : annotations) {
            allAnnotationBackends.addAll(annotation.getViableBackends());
        }

        List<Resolved> results = new ArrayList<>();
        for (String targetBackend : allAnnotationBackends) {
            List<OperatorAnnotation> resolved = resolveAnnotationsToTarget(
                annotations, targetBackend, backend);
            results.add(new Resolved(backend,
                node.copyResolved(backend, children, resolved)));
        }
        return results;
    }

    private static List<OperatorAnnotation> resolveAnnotationsToTarget(
            List<OperatorAnnotation> annotations, String targetBackend, String operatorBackend) {
        List<OperatorAnnotation> resolved = new ArrayList<>();
        for (OperatorAnnotation annotation : annotations) {
            if (annotation.getViableBackends().contains(targetBackend)) {
                resolved.add(annotation.narrowTo(targetBackend));
            } else if (annotation.getViableBackends().contains(operatorBackend)) {
                resolved.add(annotation.narrowTo(operatorBackend));
            } else {
                resolved.add(annotation.narrowTo(annotation.getViableBackends().getFirst()));
            }
        }
        return resolved;
    }

}
