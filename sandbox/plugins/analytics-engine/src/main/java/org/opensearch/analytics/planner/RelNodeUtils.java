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
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

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
                aggregate.getViableBackends()
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
        }

        throw new UnsupportedOperationException("Cannot copy node type: " + node.getClass().getSimpleName());
    }

    private static RelTraitSet rebuildTraits(RelNode node, RelOptCluster newCluster, OpenSearchDistributionTraitDef distTraitDef) {
        RelTraitSet traits = newCluster.traitSet().replace(OpenSearchConvention.INSTANCE);

        for (int index = 0; index < node.getTraitSet().size(); index++) {
            org.apache.calcite.plan.RelTrait trait = node.getTraitSet().getTrait(index);
            if (trait instanceof OpenSearchDistribution oldDist) {
                traits = traits.replace(distTraitDef.fromType(oldDist.getType(), oldDist.getKeys()));
            }
        }

        return traits;
    }

    /**
     * Extracts the single backend from the leaf operator in a resolved fragment.
     * After resolution, every operator has exactly one viable backend. Throws if
     * the leaf has more than one (indicates resolution didn't complete).
     */
    public static String extractLeafBackendFromResolvedFragment(RelNode node) {
        if (node.getInputs().isEmpty()) {
            if (node instanceof OpenSearchRelNode leafNode) {
                List<String> backends = leafNode.getViableBackends();
                if (backends.size() != 1) {
                    throw new IllegalStateException(
                        "Expected exactly 1 viable backend on resolved leaf [" + node.getClass().getSimpleName() + "], got " + backends
                    );
                }
                return backends.getFirst();
            }
            throw new IllegalStateException("Leaf node [" + node.getClass().getSimpleName() + "] is not an OpenSearchRelNode");
        }
        for (RelNode input : node.getInputs()) {
            String backend = extractLeafBackendFromResolvedFragment(input);
            if (backend != null) {
                return backend;
            }
        }
        return null;
    }
}
