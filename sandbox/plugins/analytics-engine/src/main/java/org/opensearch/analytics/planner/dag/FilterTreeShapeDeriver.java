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
     * @param fuseDualViable      mirrors the {@code analytics.delegation.fuse_dual_viable} cluster
     *                            setting. When {@code true}, the combiner fuses OR-of-same-backend
     *                            delegated leaves into a single {@code delegated_predicate} call
     *                            wrapping the OR; the post-combiner tree the data node sees is
     *                            conjunctive (one delegated scalar at the top level). This deriver
     *                            walks the pre-combiner condition, so it must mirror that fusion
     *                            to keep the data-node evaluator path classification consistent.
     * @return the tree shape, or {@code null} if no delegated annotations exist
     */
    static FilterTreeShape derive(OpenSearchFilter filter, String drivingBackendId, boolean fuseDualViable) {
        Result result = walk(filter.getCondition(), drivingBackendId, fuseDualViable);
        if (!result.hasDelegated) {
            return FilterTreeShape.NO_DELEGATION;
        }
        return result.hasMixed ? FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION : FilterTreeShape.CONJUNCTIVE;
    }

    private static Result walk(RexNode node, String drivingBackendId, boolean fuseDualViable) {
        if (node instanceof AnnotatedPredicate predicate) {
            // Two flavors of delegation count toward "hasDelegated":
            // 1. Correctness — viableBackends differs from operator backend (the only backend
            // that can evaluate is the peer).
            // 2. Performance — operator backend can evaluate natively, but a peer was also
            // viable and is available for opportunistic per-RG consultation.
            boolean isCorrectness = !predicate.getViableBackends().getFirst().equals(drivingBackendId);
            boolean isPerformance = !predicate.getPerformanceDelegationBackends().isEmpty();
            boolean isDelegated = isCorrectness || isPerformance;
            return new Result(isDelegated, false, !isDelegated, isPerformance);
        }
        if (node instanceof RexCall call) {
            boolean isOrNot = call.getKind() == SqlKind.OR || call.getKind() == SqlKind.NOT;

            boolean hasDelegated = false;
            boolean hasDrivingBackend = false;
            boolean hasPerformanceDelegation = false;
            boolean hasMixed = false;

            for (RexNode operand : call.getOperands()) {
                Result childResult = walk(operand, drivingBackendId, fuseDualViable);
                hasDelegated |= childResult.hasDelegated;
                hasDrivingBackend |= childResult.hasDrivingBackend;
                hasMixed |= childResult.hasMixed;
                hasPerformanceDelegation |= childResult.hasPerformanceDelegation;
            }

            // Under OR/NOT, interleaving occurs when delegated children sit alongside something
            // the combiner can't fuse with them:
            // - native (driving-backend) operands — never fuse, always interleaved.
            // - performance-delegated operands — interleaved only when fuseDualViable is off
            // (carve-out throws perf back to native individually); with fusion on the
            // combiner emits a single delegation_possible wrapping the whole OR/NOT.
            boolean unfuseablePeer = hasDrivingBackend || (hasPerformanceDelegation && fuseDualViable == false);
            hasMixed |= isOrNot && hasDelegated && unfuseablePeer;

            return new Result(hasDelegated, hasMixed, hasDrivingBackend, hasPerformanceDelegation);
        }
        return new Result(false, false, false, false);
    }

    private record Result(boolean hasDelegated, boolean hasMixed, boolean hasDrivingBackend, boolean hasPerformanceDelegation) {
    }
}
