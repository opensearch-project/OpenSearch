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
        return walk(filter.getCondition(), drivingBackendId);
    }

    private static FilterTreeShape walk(RexNode node, String drivingBackendId) {
        if (node instanceof AnnotatedPredicate predicate) {
            boolean isDelegated = !predicate.getViableBackends().getFirst().equals(drivingBackendId);
            return isDelegated ? FilterTreeShape.SINGLE_AND : FilterTreeShape.PLAIN;
        }
        if (node instanceof RexCall call) {
            boolean isOrNot = call.getKind() == SqlKind.OR || call.getKind() == SqlKind.NOT;

            boolean hasDelegated = false;
            boolean hasDrivingBackend = false;

            for (RexNode operand : call.getOperands()) {
                FilterTreeShape childShape = walk(operand, drivingBackendId);
                if (childShape == FilterTreeShape.MIXED_BOOLEAN) {
                    return FilterTreeShape.MIXED_BOOLEAN;
                }
                if (childShape == FilterTreeShape.PLAIN) {
                    hasDrivingBackend = true;
                } else {
                    hasDelegated = true;
                }
            }

            // OR/NOT with both delegated and driving-backend descendants → MIXED
            if (isOrNot && hasDelegated && hasDrivingBackend) {
                return FilterTreeShape.MIXED_BOOLEAN;
            }

            if (hasDelegated) {
                return FilterTreeShape.SINGLE_AND;
            }
            return FilterTreeShape.PLAIN;
        }
        // Leaf (RexInputRef, RexLiteral) — no annotations
        return FilterTreeShape.PLAIN;
    }
}
