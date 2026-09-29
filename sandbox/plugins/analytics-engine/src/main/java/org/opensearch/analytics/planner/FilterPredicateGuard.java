/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

/**
 * Guards against excessively complex filter predicates by counting leaf predicates
 * in a {@link RexNode} condition tree.
 *
 * <p>A "leaf predicate" is any {@link RexCall} that is not a boolean connective
 * ({@code AND}, {@code OR}, {@code NOT}). A flat {@code a=1 OR b=2 OR c=3} counts
 * as 3 leaf predicates regardless of the tree's nesting shape.
 *
 * <p>Boolean nesting depth is intentionally not guarded here: the SQL plugin's
 * {@code plugins.query.max_expression_depth} (default 1000) already bounds parse-tree
 * recursion depth before a query reaches the analytics engine, and Calcite flattens
 * associative {@code AND}/{@code OR} chains during optimization, so pathological depth
 * rarely survives to this point. Predicate count is the gap that guard doesn't cover —
 * a flat fan-out of many OR-ed conditions passes the SQL plugin's depth check easily.
 *
 * @opensearch.internal
 */
public final class FilterPredicateGuard {

    private FilterPredicateGuard() {}

    /**
     * Validates the filter condition against the configured leaf-predicate count limit.
     * Throws {@link IllegalArgumentException} (HTTP 400) if the limit is exceeded.
     *
     * @param condition the filter's RexNode condition tree
     * @param maxCount  maximum leaf predicates allowed (0 = unlimited)
     */
    public static void validate(RexNode condition, int maxCount) {
        if (maxCount <= 0) {
            return; // guard disabled
        }
        int leafCount = countLeaves(condition);
        if (leafCount > maxCount) {
            throw new IllegalArgumentException(
                "Filter condition contains "
                    + leafCount
                    + " predicates, exceeding the maximum allowed ["
                    + maxCount
                    + "]. Simplify the query by reducing the number of filter conditions."
            );
        }
    }

    /**
     * Returns the number of leaf predicates in the given RexNode tree. Boolean connectives
     * ({@code AND}/{@code OR}/{@code NOT}) are not counted themselves — only their operands
     * contribute, recursively.
     */
    static int countLeaves(RexNode node) {
        if (!(node instanceof RexCall call)) {
            // Literal or input ref — not a predicate by itself
            return 0;
        }

        if (call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR || call.getKind() == SqlKind.NOT) {
            int total = 0;
            for (RexNode operand : call.getOperands()) {
                total += countLeaves(operand);
            }
            return total;
        }

        // Leaf predicate (comparison, function call, etc.)
        return 1;
    }
}
