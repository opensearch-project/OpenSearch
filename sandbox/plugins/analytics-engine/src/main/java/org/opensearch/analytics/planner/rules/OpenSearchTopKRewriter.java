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
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.settings.AnalyticsApproximationSettings;
import org.opensearch.cluster.metadata.IndexMetadata;

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
        // Find bottommost Sort with collation
        SortAboveFinal match = findSortAboveFinal(root);
        if (match == null) return Optional.empty();

        OpenSearchSort sort = match.sort;
        OpenSearchAggregate finalAgg = match.finalAgg;

        // Gate: must have group-by
        if (finalAgg.getGroupSet().isEmpty()) return Optional.empty();

        // Gate: must have ER → PARTIAL below
        RelNode erNode = finalAgg.getInput();
        if (!(erNode instanceof OpenSearchExchangeReducer er)) return Optional.empty();
        if (er.getInputs().isEmpty()) return Optional.empty();
        RelNode partialNode = er.getInputs().get(0);
        if (!(partialNode instanceof OpenSearchAggregate partial) || partial.getMode() != AggregateMode.PARTIAL) {
            return Optional.empty();
        }

        // Gate: read factor from index setting
        double factor = resolveOversamplingFactor(partial, context);
        if (factor <= 0.0) return Optional.empty();

        // Compute shardSize
        long coordLimit = (sort.fetch instanceof RexLiteral lit) ? RexLiteral.intValue(lit) : 10_000L;
        long shardSize = (long) Math.ceil(coordLimit * factor) + coordLimit;
        if (shardSize > Integer.MAX_VALUE) return Optional.empty();

        // Build per-partition Sort: same collation as the user's Sort, with fetch=shardSize
        RexBuilder rb = sort.getCluster().getRexBuilder();
        RexNode fetchLiteral = rb.makeLiteral((int) shardSize, sort.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);
        OpenSearchSort shardSort = new OpenSearchSort(
            sort.getCluster(),
            partial.getTraitSet(),
            partial,
            sort.getCollation(),
            null,
            fetchLiteral,
            partial.getViableBackends(),
            true // perPartition
        );

        // Rewire: ER's input becomes the new shardSort
        RelNode newER = er.copy(er.getTraitSet(), List.of(shardSort));
        RelNode newFinal = finalAgg.copy(finalAgg.getTraitSet(), List.of(newER));

        // Rebuild the tree above FINAL (replace finalAgg with newFinal in the original tree)
        RelNode result = replaceInTree(root, finalAgg, newFinal);
        return Optional.of(result);
    }

    /** Walks the tree to find the bottommost Sort with collation above a FINAL aggregate. */
    private static SortAboveFinal findSortAboveFinal(RelNode node) {
        if (node instanceof OpenSearchSort sort && !sort.getCollation().getFieldCollations().isEmpty()) {
            // Look for FINAL below (possibly through a Project)
            OpenSearchAggregate finalAgg = findFinalBelow(sort.getInput());
            if (finalAgg != null) return new SortAboveFinal(sort, finalAgg);
        }
        // Recurse into children
        for (RelNode child : node.getInputs()) {
            SortAboveFinal found = findSortAboveFinal(child);
            if (found != null) return found;
        }
        return null;
    }

    private static OpenSearchAggregate findFinalBelow(RelNode node) {
        if (node instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.FINAL) return agg;
        // Look through Project
        if (node.getInputs().size() == 1) return findFinalBelow(node.getInputs().get(0));
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

    private static double resolveOversamplingFactor(OpenSearchAggregate partial, PlannerContext context) {
        // Walk to TableScan to get index name, then look up setting
        OpenSearchTableScan scan = findScan(partial);
        if (scan == null) return 0.0;
        String indexName = scan.getTable().getQualifiedName().get(scan.getTable().getQualifiedName().size() - 1);
        IndexMetadata meta = context.getClusterState().metadata().index(indexName);
        if (meta == null) return 0.0;
        if (meta.getSettings() == null) return 0.0;
        return AnalyticsApproximationSettings.INDEX_ANALYTICS_SHARD_BUCKET_OVERSAMPLING_FACTOR.get(meta.getSettings());
    }

    private static OpenSearchTableScan findScan(RelNode node) {
        if (node instanceof OpenSearchTableScan scan) return scan;
        for (RelNode child : node.getInputs()) {
            OpenSearchTableScan found = findScan(child);
            if (found != null) return found;
        }
        return null;
    }

    private record SortAboveFinal(OpenSearchSort sort, OpenSearchAggregate finalAgg) {
    }
}
