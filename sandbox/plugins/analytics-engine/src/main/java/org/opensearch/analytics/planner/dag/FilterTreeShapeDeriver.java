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
        if (!result.hasDelegated) {
            return FilterTreeShape.NO_DELEGATION;
        }
        return result.hasMixed ? FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION : FilterTreeShape.CONJUNCTIVE;
    }

    private static Result walk(RexNode node, String drivingBackendId) {
        if (node instanceof AnnotatedPredicate predicate) {
            // Two flavors of delegation count toward "hasDelegated":
            // 1. Correctness — viableBackends differs from operator backend (the only backend
            // that can evaluate is the peer).
            // 2. Performance — operator backend can evaluate natively, but a peer was also
            // viable and is available for opportunistic per-RG consultation.
            boolean isCorrectness = !predicate.getViableBackends().getFirst().equals(drivingBackendId);
            boolean isPerformance = !predicate.getPerformanceDelegationBackends().isEmpty();
            boolean isDelegated = isCorrectness || isPerformance;
            return new Result(isDelegated, false, !isDelegated);
        }
        if (node instanceof RexCall call) {
            boolean isOrNot = call.getKind() == SqlKind.OR || call.getKind() == SqlKind.NOT;

            boolean hasDelegated = false;
            boolean hasDrivingBackend = false;
            boolean hasMixed = false;

            for (RexNode operand : call.getOperands()) {
                Result childResult = walk(operand, drivingBackendId);
                hasDelegated |= childResult.hasDelegated;
                hasDrivingBackend |= childResult.hasDrivingBackend;
                hasMixed |= childResult.hasMixed;
            }

            // A delegated predicate under OR or NOT requires the tree evaluator —
            // NOT(delegated) needs bitmap inversion which SingleCollector can't do,
            // and OR(delegated, ...) needs multi-collector bitmap combination.
            // Previously this also required hasDrivingBackend, which missed the
            // bare NOT(delegated) case where no driving-backend predicate exists.
            if (isOrNot && hasDelegated) {
                hasMixed = true;
            }

            return new Result(hasDelegated, hasMixed, hasDrivingBackend);
        }
        return new Result(false, false, false);
    }

    private record Result(boolean hasDelegated, boolean hasMixed, boolean hasDrivingBackend) {
    }
}
