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
     * @param filter              the OpenSearchFilter with annotations intact
     * @param drivingBackendId    the filter operator's resolved backend
     * @return the tree shape, or {@code null} if no delegated annotations exist
     */
    static FilterTreeShape derive(OpenSearchFilter filter, String drivingBackendId) {
        Result result = walk(filter.getCondition(), drivingBackendId, false);
        if (!result.hasDelegated) {
            return null;
        }
        return result.hasMixed ? FilterTreeShape.MIXED_BOOLEAN : FilterTreeShape.SINGLE_AND;
    }

    private static Result walk(RexNode node, String drivingBackendId, boolean underOrNot) {
        if (node instanceof AnnotatedPredicate predicate) {
            boolean isDelegated = !predicate.getViableBackends().getFirst().equals(drivingBackendId);
            return new Result(isDelegated, false, !isDelegated);
        }
        if (node instanceof RexCall call) {
            boolean isOrNot = call.getKind() == SqlKind.OR || call.getKind() == SqlKind.NOT;
            boolean childUnderOrNot = underOrNot || isOrNot;

            boolean hasDelegated = false;
            boolean hasDrivingBackend = false;
            boolean hasMixed = false;

            for (RexNode operand : call.getOperands()) {
                Result childResult = walk(operand, drivingBackendId, childUnderOrNot);
                hasDelegated |= childResult.hasDelegated;
                hasDrivingBackend |= childResult.hasDrivingBackend;
                hasMixed |= childResult.hasMixed;
            }

            // Mixed = this OR/NOT node has both delegated and driving-backend descendants
            if (isOrNot && hasDelegated && hasDrivingBackend) {
                hasMixed = true;
            }

            return new Result(hasDelegated, hasMixed, hasDrivingBackend);
        }
        // Leaf (RexInputRef, RexLiteral) — no annotations
        return new Result(false, false, false);
    }

    private record Result(boolean hasDelegated, boolean hasMixed, boolean hasDrivingBackend) {
    }
}
