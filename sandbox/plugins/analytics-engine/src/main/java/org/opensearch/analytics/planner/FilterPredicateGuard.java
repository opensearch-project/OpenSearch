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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

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
 * <p><b>The traversal itself is bounded.</b> A guard that walks the tree recursively is
 * only as safe as the tree is shallow: a pathologically deep boolean condition (deeper than
 * the JVM can recurse) would overflow the stack <em>inside the guard</em> and surface as a
 * {@link StackOverflowError} — an unhandled {@code Error}, not the intended HTTP 400 — before
 * the guard could reject it. Depth flattening upstream makes this unlikely, but a guard whose
 * own safety depends on the input already being well-formed is not a guard. So the walk here is
 * iterative (an explicit stack, never the call stack) and short-circuits the moment the leaf
 * count exceeds the limit: it never visits more than {@code maxCount + 1} leaves, and never
 * pushes deeper than the tree it is rejecting. Regardless of how the input is shaped, the guard
 * either passes it or throws {@link IllegalArgumentException} — it can no longer be made to fail
 * by the very complexity it exists to reject.
 *
 * @opensearch.internal
 */
public final class FilterPredicateGuard {

    private FilterPredicateGuard() {}

    /**
     * Validates the filter condition against the configured leaf-predicate count limit.
     * Throws {@link IllegalArgumentException} (HTTP 400) if the limit is exceeded.
     *
     * <p>The traversal stops as soon as the limit is known to be exceeded, so it is bounded
     * by {@code maxCount} in both work and stack depth — a deeply nested condition cannot make
     * the guard itself overflow the stack.
     *
     * @param condition the filter's RexNode condition tree
     * @param maxCount  maximum leaf predicates allowed (0 = unlimited)
     */
    public static void validate(RexNode condition, int maxCount) {
        if (maxCount <= 0) {
            return; // guard disabled
        }
        // Count up to maxCount + 1: that is all we need to decide, and it caps the work the
        // guard does on a hostile condition tree at one leaf beyond the limit.
        int leafCount = countLeavesUpTo(condition, maxCount + 1);
        if (leafCount > maxCount) {
            throw new IllegalArgumentException(
                "Filter condition contains more than "
                    + maxCount
                    + " predicates, exceeding the maximum allowed ["
                    + maxCount
                    + "]. Simplify the query by reducing the number of filter conditions."
            );
        }
    }

    /**
     * Returns the number of leaf predicates in the given RexNode tree. Boolean connectives
     * ({@code AND}/{@code OR}/{@code NOT}) are not counted themselves — only their operands
     * contribute.
     *
     * <p>Traversal is iterative (explicit stack) rather than recursive, so it is safe against
     * arbitrarily deep condition trees. This method counts the whole tree; {@link #validate}
     * uses the bounded {@link #countLeavesUpTo} variant to avoid doing unbounded work on input
     * it is going to reject anyway.
     */
    static int countLeaves(RexNode node) {
        return countLeavesUpTo(node, Integer.MAX_VALUE);
    }

    /**
     * Counts leaf predicates in {@code node}, stopping early once the running total reaches
     * {@code limit}. Returns a value in {@code [0, limit]}: a return value equal to {@code limit}
     * means "at least {@code limit}" (the walk short-circuited and the true total may be higher).
     *
     * <p>Uses an explicit work stack instead of recursion, so traversal depth is independent of
     * the JVM call-stack depth and cannot overflow it. Combined with the early exit, the guard's
     * cost is bounded by {@code limit} regardless of the tree's size or nesting.
     */
    static int countLeavesUpTo(RexNode node, int limit) {
        if (limit <= 0) {
            return 0;
        }
        int total = 0;
        Deque<RexNode> stack = new ArrayDeque<>();
        stack.push(node);
        while (!stack.isEmpty()) {
            RexNode current = stack.pop();
            if (!(current instanceof RexCall call)) {
                // Literal or input ref — not a predicate by itself.
                continue;
            }
            SqlKind kind = call.getKind();
            if (kind == SqlKind.AND || kind == SqlKind.OR || kind == SqlKind.NOT) {
                // Connective: descend into operands. They only contribute leaves, not depth on
                // the call stack, so a degenerate chain of a million nested ANDs is just a
                // million iterations of this loop, not a million stack frames.
                List<RexNode> operands = call.getOperands();
                for (RexNode operand : operands) {
                    stack.push(operand);
                }
                continue;
            }
            // Leaf predicate (comparison, function call, etc.).
            total++;
            if (total >= limit) {
                // Already at or beyond what the caller cares about — no need to keep walking.
                return limit;
            }
        }
        return total;
    }
}
