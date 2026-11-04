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
 * <p>Boolean nesting <b>depth</b> is intentionally out of scope here — it is bounded upstream,
 * before a condition ever reaches this guard:
 * <ul>
 *   <li>PPL/SQL text queries: the SQL plugin's {@code plugins.query.max_expression_depth}
 *       (default 1000) bounds AST-visitor recursion at parse time and rejects deeper expressions
 *       with HTTP 400.</li>
 *   <li>DSL {@code _search} queries: the XContent (JSON/CBOR/YAML/SMILE) parser enforces a maximum
 *       nesting depth (default 1000), so a {@code bool} tree deeper than that is rejected at parse
 *       time before any query builder — or RexNode — is constructed.</li>
 * </ul>
 * A guard here could not defend depth even if it wanted to: Calcite's own {@code RexUtil.isFlat}
 * recurses the whole AND/OR/NOT subtree while building/normalizing the condition, so a
 * pathologically deep tree overflows inside Calcite <em>before</em> this guard runs. Depth is the
 * upstream parsers' job; count is the gap they leave, and count is what this guard covers.
 *
 * <p>Predicate <b>count</b> is that gap: a flat fan-out of many OR-ed conditions
 * ({@code a=1 OR b=2 OR ... OR z=26}) is shallow — it sails through the depth bounds above — yet
 * multiplies per-predicate planning and execution cost. That is the shape this guard rejects.
 *
 * <p><b>The count traversal itself is bounded.</b> A guard that walks the tree recursively is only
 * as safe as the tree is shallow; even though depth is bounded upstream today, this guard does not
 * rely on that for its own safety. The walk is iterative (an explicit stack, never the call stack)
 * and short-circuits the moment the leaf count exceeds the limit: it never visits more than
 * {@code maxCount + 1} leaves and never recurses on the JVM call stack, so it cannot itself be made
 * to overflow by the complexity it is measuring.
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
