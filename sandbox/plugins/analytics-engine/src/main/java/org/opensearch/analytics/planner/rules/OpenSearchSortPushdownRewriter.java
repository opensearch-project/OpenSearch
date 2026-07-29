/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchUnion;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Post-CBO rewriter for non-aggregate TopK. Copies the bottom-most collated
 * {@link OpenSearchSort} to just below the exchange so each shard ships only its local top-N
 * sorted rows; the coordinator Sort still merges and applies the final offset/limit. Exact
 * (no oversampling): a row in the global window is within its own shard's top-{@code (offset+fetch)}.
 *
 * <p>Targets a collated Sort sitting directly above either an {@link OpenSearchExchangeReducer}
 * (scan case) or an {@code UNION ALL} {@link OpenSearchUnion} whose arms are each gathered by an
 * ER — in which case the Sort is pushed below <em>every</em> arm's ER (arm row types match the
 * union output, so the collation maps 1:1). Offset is not pushed: the shard fetch widens to
 * {@code offset+fetch} and the offset stays on the coordinator.
 *
 * <p>Handles {@code Sort(collation,[offset,]fetch)} (SQL) and {@code Sort(fetch) → Sort(collation)}
 * (PPL {@code sort | head}). Walks only the single-input coordinator spine and skips the aggregate
 * path (ER feeding a PARTIAL aggregate, owned by {@link OpenSearchTopKRewriter}).
 *
 * <p>TODO: this is a temporary workaround. The correct approach is proper trait propagation in
 * Calcite's Volcano planner (collation/limit pushdown through the exchange); replace this rewrite
 * once that is in place.
 *
 * @opensearch.internal
 */
public final class OpenSearchSortPushdownRewriter {

    private OpenSearchSortPushdownRewriter() {}

    public static Optional<RelNode> rewrite(RelNode root) {
        RewriteContext m = find(root, null);
        if (m == null) return Optional.empty();
        RelNode replacement = rewriteTarget(m.below, m.collated, shardFetch(m.bound));
        return replacement == null ? Optional.empty() : Optional.of(replaceInTree(root, m.below, replacement));
    }

    /**
     * Walks the single-input coordinator spine for the bottom-most collated Sort directly above a
     * push target (an ER or UNION ALL), bounded by a fetch. {@code fetchSortAbove} is an
     * immediately-enclosing pure-fetch Sort (carries the limit for the two-node PPL shape). Stops at
     * multi-input ops (other than a matched UNION ALL) and leaves.
     */
    private static RewriteContext find(RelNode node, OpenSearchSort fetchSortAbove) {
        if (node instanceof OpenSearchSort sort) {
            boolean collated = sort.getCollation().getFieldCollations().isEmpty() == false;
            if (collated) {
                // Bound = this Sort if it carries a fetch; else the enclosing pure-fetch Sort, but
                // only when this Sort has no offset of its own (which the enclosing one can't honor).
                OpenSearchSort bound = sort.fetch != null ? sort : (sort.offset == null ? fetchSortAbove : null);
                RelNode below = sort.getInput();
                if (bound != null && canComputeShardFetch(bound) && isPushTarget(below)) {
                    return new RewriteContext(sort, bound, below);
                }
            } else if (sort.fetch != null && canComputeShardFetch(sort) && isPushTarget(sort.getInput())) {
                // Bare LIMIT, no ORDER BY (`head N`): push a fetch-only Sort below the ER so each
                // shard caps at N instead of streaming its whole scan. Safe with no order — the
                // coordinator just needs some N rows, and each shard's local N supplies them.
                return new RewriteContext(sort, sort, sort.getInput());
            }
            OpenSearchSort carry = (collated == false && sort.fetch != null) ? sort : null;
            return find(sort.getInput(), carry);
        }
        // Follow only the single-input coordinator spine. Multi-input ops (Join) are intentionally
        // not traversed — pushing a sort into one join branch is unsafe; UNION ALL is instead matched
        // directly as a push target above (see isPushTarget).
        return node.getInputs().size() == 1 ? find(node.getInputs().get(0), null) : null;
    }

    /** A node we can push a Sort below: an eligible ER, or a UNION ALL with at least one eligible ER arm. */
    private static boolean isPushTarget(RelNode below) {
        if (below instanceof OpenSearchExchangeReducer er) return eligibleChildBelowER(er);
        if (below instanceof OpenSearchUnion union && union.all) {
            for (RelNode arm : union.getInputs()) {
                if (arm instanceof OpenSearchExchangeReducer er && eligibleChildBelowER(er)) return true;
            }
        }
        return false;
    }

    /** Rebuilds the target with the shard Sort pushed below the ER (scan case) or below each arm's ER (union). */
    private static RelNode rewriteTarget(RelNode below, OpenSearchSort collated, RexNode fetch) {
        if (below instanceof OpenSearchExchangeReducer er) {
            return pushBelow(er, collated, fetch);
        }
        OpenSearchUnion union = (OpenSearchUnion) below;
        List<RelNode> arms = new ArrayList<>(union.getInputs().size());
        boolean pushed = false;
        for (RelNode arm : union.getInputs()) {
            if (arm instanceof OpenSearchExchangeReducer er && eligibleChildBelowER(er)) {
                arms.add(pushBelow(er, collated, fetch));
                pushed = true;
            } else {
                arms.add(arm);
            }
        }
        return pushed ? union.copy(union.getTraitSet(), arms, union.all) : null;
    }

    /** ER with the shard Sort inserted between it and its input. */
    private static RelNode pushBelow(OpenSearchExchangeReducer er, OpenSearchSort collated, RexNode fetch) {
        RelNode erInput = er.getInput();
        OpenSearchSort shardSort = new OpenSearchSort(
            collated.getCluster(),
            erInput.getTraitSet(),
            erInput,
            collated.getCollation(),
            null,
            fetch,
            collated.getViableBackends()
        );
        return er.copy(er.getTraitSet(), List.of(shardSort));
    }

    /**
     * Eligible to push a Sort below: not the aggregate path, not already pushed, and not gathering a
     * Join — joins are explicitly out of scope (a LIMIT cannot be safely pushed below a join).
     */
    private static boolean eligibleChildBelowER(OpenSearchExchangeReducer er) {
        RelNode input = er.getInput();
        return isAggregatePath(er) == false && (input instanceof OpenSearchSort) == false && (input instanceof OpenSearchJoin) == false;
    }

    /**
     * True when the shard fetch is computable: no offset, or offset and fetch are both literals to sum.
     * TODO: support non-literal (complex expression) offset/fetch by summing as a RexNode — out of scope for now.
     */
    private static boolean canComputeShardFetch(OpenSearchSort bound) {
        return bound.offset == null || (bound.offset instanceof RexLiteral && bound.fetch instanceof RexLiteral);
    }

    /** Shard fetch = fetch when there's no offset, else offset + fetch (offset stays on the coordinator). */
    private static RexNode shardFetch(OpenSearchSort bound) {
        if (bound.offset == null) return bound.fetch;
        int sum = RexLiteral.intValue(bound.offset) + RexLiteral.intValue(bound.fetch);
        return bound.getCluster()
            .getRexBuilder()
            .makeLiteral(sum, bound.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);
    }

    private static boolean isAggregatePath(OpenSearchExchangeReducer er) {
        return er.getInput() instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.PARTIAL;
    }

    /** Replaces oldNode with newNode in the tree (single occurrence), rebuilding ancestors. */
    private static RelNode replaceInTree(RelNode root, RelNode oldNode, RelNode newNode) {
        if (root == oldNode) return newNode;
        List<RelNode> children = root.getInputs();
        RelNode[] newChildren = new RelNode[children.size()];
        boolean changed = false;
        for (int i = 0; i < children.size(); i++) {
            newChildren[i] = replaceInTree(children.get(i), oldNode, newNode);
            if (newChildren[i] != children.get(i)) changed = true;
        }
        return changed ? root.copy(root.getTraitSet(), List.of(newChildren)) : root;
    }

    /** Carries the matched collated Sort, the fetch-bounding Sort, and the target node to rewrite. */
    private record RewriteContext(OpenSearchSort collated, OpenSearchSort bound, RelNode below) {
    }
}
