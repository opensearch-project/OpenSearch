/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;

import java.util.ArrayList;
import java.util.List;

/**
 * Generates plan alternatives for each {@link Stage} in a {@link QueryDAG}.
 *
 * <p>Walks each stage's marked fragment bottom-up. At each operator, iterates
 * its viableBackends. For each choice, resolves annotations to single backends
 * via {@link OperatorAnnotation#narrowTo}. When an annotation requires delegation
 * and multiple targets exist, branches into multiple plans via cartesian product.
 *
 * <p>No operator-specific logic here — each {@link OpenSearchRelNode} implements
 * {@code getAnnotations()} and {@code copyResolved()} to handle its own internals.
 *
 * @opensearch.internal
 */
public class PlanForker {

    private PlanForker() {}

    /** Generates plan alternatives for all stages in the DAG. */
    public static void forkAll(QueryDAG dag) {
        forkStage(dag.rootStage());
    }

    private static void forkStage(Stage stage) {
        for (Stage child : stage.getChildStages()) {
            forkStage(child);
        }
        if (stage.getFragment() == null) {
            return;
        }
        List<RelNode> alternatives = resolve(stage.getFragment());
        stage.setPlanAlternatives(alternatives.stream().map(StagePlan::new).toList());
    }

    /**
     * Resolves a RelNode into all possible single-backend alternatives.
     * Walks bottom-up: resolves children first, then this operator.
     */
    private static List<RelNode> resolve(RelNode node) {
        // Resolve children first (bottom-up)
        List<List<RelNode>> childAlternativeSets = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            childAlternativeSets.add(resolve(input));
        }

        // Leaf node with no children
        if (childAlternativeSets.isEmpty()) {
            return resolveOperator(node, List.of(List.of()));
        }

        // Cartesian product of child alternatives
        List<List<RelNode>> childCombinations = cartesian(childAlternativeSets);

        List<RelNode> results = new ArrayList<>();
        for (List<RelNode> childCombo : childCombinations) {
            results.addAll(resolveOperator(node, List.of(childCombo)));
        }
        return results;
    }

    /**
     * Resolves a single operator for all its viable backends, given resolved children.
     */
    private static List<RelNode> resolveOperator(RelNode node, List<List<RelNode>> childCombinations) {
        if (!(node instanceof OpenSearchRelNode openSearchNode)) {
            // Non-OpenSearch node (e.g., StageInputScan) — pass through
            List<RelNode> results = new ArrayList<>();
            for (List<RelNode> children : childCombinations) {
                results.add(children.isEmpty() ? node : node.copy(node.getTraitSet(), children));
            }
            return results;
        }

        List<RelNode> results = new ArrayList<>();
        for (String backend : openSearchNode.getViableBackends()) {
            List<OperatorAnnotation> annotations = openSearchNode.getAnnotations();

            if (annotations.isEmpty()) {
                // No annotations — just narrow viableBackends
                for (List<RelNode> children : childCombinations) {
                    results.add(openSearchNode.copyResolved(backend, children, List.of()));
                }
                continue;
            }

            // Resolve each annotation: native or delegation targets
            List<List<OperatorAnnotation>> annotationOptionSets = new ArrayList<>();
            for (OperatorAnnotation annotation : annotations) {
                annotationOptionSets.add(resolveAnnotation(annotation, backend));
            }

            // Cartesian product across annotation options
            List<List<OperatorAnnotation>> annotationCombinations = cartesian(annotationOptionSets);

            for (List<OperatorAnnotation> resolvedAnnotations : annotationCombinations) {
                for (List<RelNode> children : childCombinations) {
                    results.add(openSearchNode.copyResolved(backend, children, resolvedAnnotations));
                }
            }
        }
        return results;
    }

    /**
     * Resolves a single annotation for a chosen backend.
     * Returns one option per viable backend in the annotation — the native option
     * (if the chosen backend can handle it) plus delegation options for every
     * other viable backend. Cost evaluation later decides which is best.
     */
    private static List<OperatorAnnotation> resolveAnnotation(OperatorAnnotation annotation,
                                                              String backend) {
        return annotation.getViableBackends().stream()
            .map(annotation::narrowTo)
            .toList();
    }

    private static <T> List<List<T>> cartesian(List<List<T>> sets) {
        List<List<T>> result = new ArrayList<>();
        result.add(new ArrayList<>());
        for (List<T> set : sets) {
            List<List<T>> expanded = new ArrayList<>();
            for (List<T> existing : result) {
                for (T element : set) {
                    List<T> combined = new ArrayList<>(existing);
                    combined.add(element);
                    expanded.add(combined);
                }
            }
            result = expanded;
        }
        return result;
    }
}
