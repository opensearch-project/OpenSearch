/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.settings.AnalyticsApproximationSettings;
import org.opensearch.common.settings.Settings;

import java.util.List;
import java.util.Optional;

/**
 * Post-CBO rewriter that inserts a per-partition Sort+Limit between PARTIAL and ER
 * for TopK aggregation queries. Runs before DAGBuilder splits stages.
 *
 * <p>Pattern: Sort(collation) → [Project] → Aggregate(FINAL) → ER → Aggregate(PARTIAL) → Scan
 */
public final class OpenSearchTopKRewriter {

    private OpenSearchTopKRewriter() {}

    public static Optional<RelNode> rewrite(RelNode root, PlannerContext context) {
        SortAboveFinal match = findSortAboveFinal(root);
        if (match == null) return Optional.empty();

        OpenSearchSort sort = match.sort;
        OpenSearchAggregate finalAgg = match.finalAgg;

        if (finalAgg.getGroupSet().isEmpty()) return Optional.empty();

        RelNode erNode = finalAgg.getInput();
        if (!(erNode instanceof OpenSearchExchangeReducer er)) return Optional.empty();
        // TODO: Consider applying TopK even for single-shard topologies in a fast-followup.
        if (er.getInputs().isEmpty()) return Optional.empty();
        RelNode partialNode = er.getInputs().get(0);
        if (!(partialNode instanceof OpenSearchAggregate partial) || partial.getMode() != AggregateMode.PARTIAL) {
            return Optional.empty();
        }

        double factor = resolveOversamplingFactor(context);
        if (factor <= 0.0) return Optional.empty();

        long coordLimit = (sort.fetch instanceof RexLiteral lit) ? RexLiteral.intValue(lit) : 10_000L;
        long shardSize = (long) Math.ceil(coordLimit * factor) + coordLimit;
        if (shardSize > Integer.MAX_VALUE) return Optional.empty();

        RexBuilder rb = sort.getCluster().getRexBuilder();
        RexNode shardSizeLiteral = rb.makeLiteral(
            (int) shardSize,
            sort.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER),
            true
        );
        OpenSearchSort shardSort = new OpenSearchSort(
            sort.getCluster(),
            partial.getTraitSet(),
            partial,
            sort.getCollation(),
            null,
            shardSizeLiteral,
            partial.getViableBackends(),
            true
        );

        RelNode newER = er.copy(er.getTraitSet(), List.of(shardSort));
        RelNode newFinal = finalAgg.copy(finalAgg.getTraitSet(), List.of(newER));

        RelNode result = replaceInTree(root, finalAgg, newFinal);
        return Optional.of(result);
    }

    /**
     * Walks the tree to find the <em>deepest</em> collated Sort above a FINAL aggregate. Recursing
     * before matching is deliberate: PPL {@code ... | sort c | head N} arrives as a collated outer
     * {@code LogicalSystemLimit(fetch=querySizeLimit)} over the user's collated {@code Sort(fetch=N)},
     * and the oversampling must derive the shard fetch from the inner (tighter) N — not the outer
     * size cap — or every shard over-fetches {@code ceil(querySizeLimit * factor) + querySizeLimit}
     * rows. Preferring the deepest match honors the innermost limit.
     */
    private static SortAboveFinal findSortAboveFinal(RelNode node) {
        for (RelNode child : node.getInputs()) {
            SortAboveFinal found = findSortAboveFinal(child);
            if (found != null) return found;
        }
        if (node instanceof OpenSearchSort sort && !sort.getCollation().getFieldCollations().isEmpty()) {
            OpenSearchAggregate finalAgg = findFinalAgg(sort.getInput());
            if (finalAgg != null) return new SortAboveFinal(sort, finalAgg);
        }
        return null;
    }

    private static OpenSearchAggregate findFinalAgg(RelNode node) {
        if (node instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.FINAL) return agg;
        if (node.getInputs().size() == 1) return findFinalAgg(node.getInputs().get(0));
        return null;
    }

    /** Replaces oldNode with newNode in the tree (single occurrence). */
    private static RelNode replaceInTree(RelNode root, RelNode oldNode, RelNode newNode) {
        if (root == oldNode) return newNode;
        List<RelNode> children = root.getInputs();
        boolean changed = false;
        RelNode[] newChildren = new RelNode[children.size()];
        for (int i = 0; i < children.size(); i++) {
            newChildren[i] = replaceInTree(children.get(i), oldNode, newNode);
            if (newChildren[i] != children.get(i)) changed = true;
        }
        if (!changed) return root;
        return root.copy(root.getTraitSet(), List.of(newChildren));
    }

    private static double resolveOversamplingFactor(PlannerContext context) {
        // TODO: Move to per-index setting once index-pattern/alias resolution is handled.
        Settings clusterSettings = context.getClusterState().metadata().settings();
        if (clusterSettings == null) return 0.0;
        return AnalyticsApproximationSettings.SHARD_BUCKET_OVERSAMPLING_FACTOR.get(clusterSettings);
    }

    private record SortAboveFinal(OpenSearchSort sort, OpenSearchAggregate finalAgg) {
    }
}
