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
import org.opensearch.analytics.planner.rel.OpenSearchSort;

import java.util.List;
import java.util.Optional;

/**
 * Post-CBO rewriter for non-aggregate TopK. Copies the bottom-most collated
 * {@link OpenSearchSort} to just below the {@link OpenSearchExchangeReducer}, so each
 * shard ships only its local top-N already-sorted rows. The coordinator Sort is left
 * intact to merge the per-shard streams and apply the final offset/limit.
 *
 * <p>Two plan shapes collapse to "collated Sort directly above an ER, bounded by a fetch":
 * <pre>
 *   Sort(collation, [offset], fetch)        SQL: ORDER BY .. LIMIT N [OFFSET M]   (bound = the Sort)
 *     ER
 *
 *   Sort([offset], fetch) → Sort(collation) PPL: sort x | head N                  (bound = the outer Sort)
 *     ER
 * </pre>
 *
 * <p>Offset handling: a shard must NOT skip its own local rows, so the shard Sort drops
 * the offset and widens its fetch to {@code offset + fetch}; the offset stays only on the
 * coordinator. Exact (no oversampling): a row in the global window {@code [offset+1 ..
 * offset+fetch]} is within its own shard's top-{@code (offset+fetch)}.
 *
 * <p>Skips the aggregate path (ER feeding a PARTIAL aggregate) — that case belongs to
 * {@link OpenSearchTopKRewriter}.
 *
 * @opensearch.internal
 */
public final class OpenSearchSortPushdownRewriter {

    private OpenSearchSortPushdownRewriter() {}

    public static Optional<RelNode> rewrite(RelNode root) {
        Match m = find(root, null);
        if (m == null) return Optional.empty();

        RelNode erInput = m.er.getInput();
        OpenSearchSort shardSort = new OpenSearchSort(
            m.collated.getCluster(),
            erInput.getTraitSet(),
            erInput,
            m.collated.getCollation(),
            null,
            shardFetch(m.bound),
            m.collated.getViableBackends()
        );
        RelNode newER = m.er.copy(m.er.getTraitSet(), List.of(shardSort));
        return Optional.of(replaceInTree(root, m.er, newER));
    }

    /**
     * Finds the bottom-most collated Sort sitting directly above an ER, bounded by a fetch.
     * {@code fetchSortAbove} is an immediately-enclosing pure-fetch Sort (carries the limit
     * for the two-node PPL shape).
     */
    private static Match find(RelNode node, OpenSearchSort fetchSortAbove) {
        if (node instanceof OpenSearchSort sort) {
            boolean collated = sort.getCollation().getFieldCollations().isEmpty() == false;
            if (collated) {
                // Bound = this Sort if it carries a fetch; else the enclosing pure-fetch Sort,
                // but only when this Sort has no offset of its own (which we couldn't honor below).
                OpenSearchSort bound = sort.fetch != null ? sort : (sort.offset == null ? fetchSortAbove : null);
                RelNode below = sort.getInput();
                if (bound != null
                    && foldable(bound)
                    && below instanceof OpenSearchExchangeReducer er
                    && isAggregatePath(er) == false
                    && (er.getInput() instanceof OpenSearchSort) == false) {
                    return new Match(sort, bound, er);
                }
            }
            // A pure-fetch Sort (no collation, has fetch) carries its offset+fetch to its direct child.
            OpenSearchSort carry = (collated == false && sort.fetch != null) ? sort : null;
            return find(sort.getInput(), carry);
        }
        for (RelNode child : node.getInputs()) {
            Match m = find(child, null);
            if (m != null) return m;
        }
        return null;
    }

    /** Foldable when there's no offset, or both offset and fetch are literals we can sum. */
    private static boolean foldable(OpenSearchSort bound) {
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

    private record Match(OpenSearchSort collated, OpenSearchSort bound, OpenSearchExchangeReducer er) {
    }
}
