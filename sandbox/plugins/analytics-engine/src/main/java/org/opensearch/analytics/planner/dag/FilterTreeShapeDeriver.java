/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.spi.FilterTreeShape;

/**
 * Derives {@link FilterTreeShape} from a filter condition while annotations are intact.
 * Must be called before stripping removes the annotations.
 *
 * <p>Single-pass walk: determines both whether delegation exists and whether the tree
 * is mixed (delegated + driving-backend predicates interleaved under OR/NOT).
 *
 * <p><b>Tree shape this walks.</b> {@code OpenSearchFilterRule#annotateCondition}
 * wraps only the leaf predicate {@code RexCall}s in an {@code AnnotatedPredicate};
 * AND/OR/NOT combinators stay as plain {@code RexCall}s. So for
 * {@code WHERE NOT match(message, 'hello')}:
 *
 * <pre>
 *   Before marking:                        After marking:
 *   RexCall(NOT)                           RexCall(NOT)                       ← unchanged
 *     └─ RexCall(MATCH, [...])               └─ AnnotatedPredicate(...)       ← leaf wrapped
 *                                                   └─ original = RexCall(MATCH, [...])
 * </pre>
 *
 * <p>Parents and grandparents of an annotated leaf stay un-annotated — only the
 * deepest predicate {@code RexCall} is wrapped. Walkers must check
 * {@code instanceof AnnotatedPredicate} <i>before</i> {@code instanceof RexCall}
 * because {@code AnnotatedPredicate} extends {@code RexCall}; otherwise the leaf
 * is misclassified as a combinator.
 *
 * @opensearch.internal
 */
final class FilterTreeShapeDeriver {

    private FilterTreeShapeDeriver() {}

    /**
     * Derives the filter tree shape from the filter's condition.
     *
     * <p>TODO: assumes a single OpenSearchFilter per fragment (FILTER_MERGE collapses
     * stacked Filters during pushdownRules). If a future plan can produce multiple
     * adjacent OpenSearchFilters that don't merge, this needs to walk all of them
     * and combine their shapes (any mixed → INTERLEAVED, any delegated → CONJUNCTIVE,
     * else NO_DELEGATION) so derivation stays consistent with delegationBytes which
     * walks the full tree.
     *
     * @param filter              the OpenSearchFilter with annotations intact
     * @param drivingBackendId    the filter operator's resolved backend
     * @return the tree shape, or {@code null} if no delegated annotations exist
     */
    static FilterTreeShape derive(OpenSearchFilter filter, String drivingBackendId) {
        Result result = walk(filter.getCondition(), drivingBackendId);
        if (!result.hasCorrectness && !result.hasPerf) {
            // Nothing delegated post-combine (all native, or a perf leaf under NOT that stays native).
            return FilterTreeShape.NO_DELEGATION;
        }
        return result.interleaved ? FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION : FilterTreeShape.CONJUNCTIVE;
    }

    /**
     * Predicts what the combiner produces for a subtree, mirroring
     * {@link DelegatedPredicateCombiner#combine}. Tracks four facts about the post-combine shape: a
     * correctness-delegated shipment, a surviving performance marker, a driving-backend (native)
     * predicate, and whether it needs the data-node tree evaluator (interleaved).
     *
     * <p>Performance delegation composes only under AND, so a perf leaf doesn't survive under OR/NOT:
     * under OR it's reclassified to correctness (ships to the peer); under NOT it stays native
     * (NOT(=) folds to != with no serializer). The shape label must match what the combiner emits,
     * or the data node mis-routes (the historical "all-docs" disjunction bug).
     */
    private static Result walk(RexNode node, String drivingBackendId) {
        if (node instanceof AnnotatedPredicate predicate) {
            boolean isCorrectness = !predicate.getViableBackends().getFirst().equals(drivingBackendId);
            boolean isPerformance = !predicate.getPerformanceDelegationBackends().isEmpty();
            boolean isDrivingBackend = !isCorrectness && !isPerformance;
            return new Result(isCorrectness, isPerformance, isDrivingBackend, false);
        }
        if (node instanceof RexCall call) {
            boolean isOr = call.getKind() == SqlKind.OR;
            boolean isNot = call.getKind() == SqlKind.NOT;

            boolean hasCorrectness = false;
            boolean hasPerf = false;
            boolean hasDrivingBackend = false;
            boolean interleaved = false;
            for (RexNode operand : call.getOperands()) {
                Result child = walk(operand, drivingBackendId);
                hasCorrectness |= child.hasCorrectness;
                hasPerf |= child.hasPerf;
                hasDrivingBackend |= child.hasDrivingBackend;
                interleaved |= child.interleaved;
            }

            // The data node does performance delegation only on the AND path, so a perf leaf can't
            // stay perf under OR or NOT. Mirror combine():
            if (hasPerf && isOr) {
                // Under OR it's reclassified to correctness and shipped to the peer.
                hasCorrectness = true;
                hasPerf = false;
            } else if (hasPerf && isNot) {
                // Under NOT a dual-equality leaf folds to != (no serializer) and stays native.
                hasDrivingBackend = true;
                hasPerf = false;
            }

            // Interleaving (tree evaluator needed) arises under OR/NOT when a delegated shipment
            // sits alongside a driving-backend operand — the two can't collapse into one peer
            // shipment. Under AND, driving-backend + delegated coexist as separate conjuncts
            // (single-collector path), so AND never introduces interleaving on its own.
            interleaved |= (isOr || isNot) && hasCorrectness && hasDrivingBackend;

            return new Result(hasCorrectness, hasPerf, hasDrivingBackend, interleaved);
        }
        return new Result(false, false, false, false);
    }

    private record Result(boolean hasCorrectness, boolean hasPerf, boolean hasDrivingBackend, boolean interleaved) {
    }
}
