/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rel.OpenSearchUnion;
import org.opensearch.analytics.planner.rel.OpenSearchValues;

import java.util.List;

/**
 * Copies an OpenSearch RelNode tree to a new cluster so all nodes register
 * with the new cluster's planner (Volcano).
 *
 * <p>Rebuilds distribution traits using the per-query {@link OpenSearchDistributionTraitDef}
 * so Calcite's identity-based trait matching works.
 *
 * <p>TODO: eliminate this by having frontends create RelNodes with the Volcano
 * cluster from the start.
 *
 * @opensearch.internal
 */
public class RelNodeUtils {

    private RelNodeUtils() {}

    /** Unwraps HepRelVertex to get the actual RelNode inside. */
    public static RelNode unwrapHep(RelNode node) {
        if (node instanceof HepRelVertex vertex) {
            return vertex.getCurrentRel();
        }
        return node;
    }

    public static RelNode copyToCluster(RelNode node, RelOptCluster newCluster, OpenSearchDistributionTraitDef distTraitDef) {
        List<RelNode> newInputs = node.getInputs().stream().map(input -> copyToCluster(input, newCluster, distTraitDef)).toList();

        RelTraitSet newTraits = rebuildTraits(node, newCluster, distTraitDef);

        if (node instanceof OpenSearchTableScan scan) {
            return new OpenSearchTableScan(newCluster, newTraits, scan.getTable(), scan.getViableBackends(), scan.getOutputFieldStorage());
        } else if (node instanceof OpenSearchFilter filter) {
            return new OpenSearchFilter(newCluster, newTraits, newInputs.getFirst(), filter.getCondition(), filter.getViableBackends());
        } else if (node instanceof OpenSearchAggregate aggregate) {
            return new OpenSearchAggregate(
                newCluster,
                newTraits,
                newInputs.getFirst(),
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggregate.getAggCallList(),
                aggregate.getMode(),
                aggregate.getViableBackends(),
                aggregate.getCallAnnotations(),
                aggregate.getFinalExtraLiteralArgs()
            );
        } else if (node instanceof OpenSearchSort sort) {
            return new OpenSearchSort(
                newCluster,
                newTraits,
                newInputs.getFirst(),
                sort.getCollation(),
                sort.offset,
                sort.fetch,
                sort.getViableBackends()
            );
        } else if (node instanceof OpenSearchProject project) {
            return new OpenSearchProject(
                newCluster,
                newTraits,
                newInputs.getFirst(),
                project.getProjects(),
                project.getRowType(),
                project.getViableBackends()
            );
        } else if (node instanceof OpenSearchJoin join) {
            return new OpenSearchJoin(
                newCluster,
                newTraits,
                newInputs.get(0),
                newInputs.get(1),
                join.getCondition(),
                join.getJoinType(),
                join.getViableBackends()
            );
        } else if (node instanceof OpenSearchUnion union) {
            return new OpenSearchUnion(newCluster, newTraits, newInputs, union.all, union.getViableBackends());
        } else if (node instanceof OpenSearchValues values) {
            return new OpenSearchValues(newCluster, newTraits, values.getRowType(), values.getTuples(), values.getViableBackends());
        } else if (node instanceof OpenSearchExchangeReducer reducer) {
            return new OpenSearchExchangeReducer(
                newCluster,
                newTraits,
                newInputs.getFirst(),
                reducer.getViableBackends(),
                reducer.getExchangeInfo()
            );
        }

        throw new UnsupportedOperationException("Cannot copy node type: " + node.getClass().getSimpleName());
    }

    private static RelTraitSet rebuildTraits(RelNode node, RelOptCluster newCluster, OpenSearchDistributionTraitDef distTraitDef) {
        RelTraitSet traits = newCluster.traitSet().replace(OpenSearchConvention.INSTANCE);

        for (int index = 0; index < node.getTraitSet().size(); index++) {
            org.apache.calcite.plan.RelTrait trait = node.getTraitSet().getTrait(index);
            if (trait instanceof OpenSearchDistribution oldDist) {
                // Preserve the full distribution (kind, type, keys, tableId).
                traits = traits.replace(distTraitDef.from(oldDist));
            }
        }

        return traits;
    }

    /**
     * Finds the first node of the given type in the fragment's single-input chain.
     * Returns {@code null} if not found.
     *
     * <p>TODO: migrate existing findLeaf/findFilter usages in FragmentConversionDriver to use this.
     */
    @SuppressWarnings("unchecked")
    public static <T extends RelNode> T findNode(RelNode node, Class<T> type) {
        if (type.isInstance(node)) {
            return (T) node;
        }
        if (!node.getInputs().isEmpty()) {
            return findNode(node.getInputs().getFirst(), type);
        }
        return null;
    }

    /**
     * Qualified name of the first {@link OpenSearchTableScan} reachable from {@code node},
     * searching all inputs (so it finds the scan under a multi-input join/union). Returns
     * {@code null} if none is present.
     */
    public static String findTableName(RelNode node) {
        if (node == null) {
            return null;
        }
        if (node instanceof OpenSearchTableScan scan) {
            return scan.getTable().getQualifiedName().getLast();
        }
        for (RelNode input : node.getInputs()) {
            String name = findTableName(input);
            if (name != null) {
                return name;
            }
        }
        return null;
    }

    /** Maximum recursion depth when walking a RelNode tree to extract indices. */
    static final int MAX_EXTRACT_INDICES_DEPTH = 15;

    /**
     * Extracts all index names referenced by {@link org.apache.calcite.rel.core.TableScan}
     * nodes in the plan. Walks the tree up to {@link #MAX_EXTRACT_INDICES_DEPTH} levels to
     * guard against pathologically deep plans constructed from complex user queries.
     */
    public static String[] extractIndices(RelNode plan) {
        java.util.Set<String> indices = new java.util.LinkedHashSet<>();
        if (!collectIndices(plan, indices, 0)) {
            throw new IllegalStateException("Query plan exceeds maximum depth (" + MAX_EXTRACT_INDICES_DEPTH + ") for index extraction");
        }
        return indices.toArray(String[]::new);
    }

    private static boolean collectIndices(RelNode node, java.util.Set<String> indices, int depth) {
        if (depth >= MAX_EXTRACT_INDICES_DEPTH) {
            return false;
        }
        if (node instanceof org.apache.calcite.rel.core.TableScan scan) {
            java.util.List<String> names = scan.getTable().getQualifiedName();
            indices.add(names.get(names.size() - 1));
        }
        for (RelNode input : node.getInputs()) {
            if (!collectIndices(input, indices, depth + 1)) {
                return false;
            }
        }
        return true;
    }

}
