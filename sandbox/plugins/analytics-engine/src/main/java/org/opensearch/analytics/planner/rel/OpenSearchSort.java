/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * OpenSearch custom Sort carrying viable backend list.
 *
 * @opensearch.internal
 */
public class OpenSearchSort extends Sort implements OpenSearchRelNode {

    private final List<String> viableBackends;
    private final boolean perPartition;

    public OpenSearchSort(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RelCollation collation,
        RexNode offset,
        RexNode fetch,
        List<String> viableBackends
    ) {
        this(cluster, traitSet, input, collation, offset, fetch, viableBackends, false);
    }

    public OpenSearchSort(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RelCollation collation,
        RexNode offset,
        RexNode fetch,
        List<String> viableBackends,
        boolean perPartition
    ) {
        super(cluster, traitSet, input, collation, offset, fetch);
        this.viableBackends = viableBackends;
        this.perPartition = perPartition;
    }

    /** True when this Sort runs per-shard (shard-bucket oversampling). */
    public boolean isPerPartition() {
        return perPartition;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /** Sort doesn't change schema — pass through child's field storage. */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        if (input instanceof OpenSearchRelNode openSearchInput) {
            return openSearchInput.getOutputFieldStorage();
        }
        return List.of();
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new OpenSearchSort(getCluster(), traitSet, input, collation, offset, fetch, viableBackends, perPartition);
    }

    /**
     * Treat our Sort as a concrete physical operator, not a Calcite collation enforcer.
     *
     * <p>Calcite's default classifies a Sort with collation as an enforcer — Volcano then
     * registers it into a {@code required=true} subset that's never marked delivered. That
     * confuses the gather-rule path, which looks for delivered subsets when converting an
     * inner Sort's RelSet to SINGLETON. We don't use Calcite's collation-trait enforcement,
     * so mark the Sort delivered like any other operator.
     */
    @Override
    public boolean isEnforcer() {
        return false;
    }

    /**
     * A collated Sort needs globally-ordered input. Our {@link OpenSearchExchangeReducer}
     * is a concat gather (not a merge exchange), so per-partition sort + ER produces
     * partition-locally ordered rows concatenated in arrival order — wrong. Returning
     * infinite cost unless the input is EXECUTION(SINGLETON) keeps that shape unplannable;
     * {@link #passThroughTraits} supplies the legal alternative by DEMANDING a gathered input
     * (ER below the Sort), which {@link OpenSearchConvention#enforce} materializes.
     *
     * <p>The gate applies only to a COLLATED sort, and must agree exactly with
     * {@link #ridesChildDistribution} — if the cost gate rejected a shape the trait hook advertises as
     * legal, Volcano would explore it and then find every alternative infinite ("Missing conversion is
     * OpenSearchSort[]"). A pure LIMIT and a perPartition top-N both ride a partitioned input: the
     * coordinator needs only SOME N rows and each partition's local N supplies them.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (perPartition || ridesChildDistribution()) {
            return planner.getCostFactory().makeTinyCost();
        }
        for (RelNode input : getInputs()) {
            for (int i = 0; i < input.getTraitSet().size(); i++) {
                RelTrait trait = input.getTraitSet().getTrait(i);
                if (trait instanceof OpenSearchDistribution distribution) {
                    boolean singletonOrAny = distribution.getType() == RelDistribution.Type.SINGLETON
                        || distribution.getType() == RelDistribution.Type.ANY;
                    if (!singletonOrAny) {
                        return planner.getCostFactory().makeInfiniteCost();
                    }
                }
            }
        }
        return planner.getCostFactory().makeTinyCost();
    }

    // ---- PhysicalNode (top-down trait propagation) ----

    /**
     * A collated-or-limiting Sort DEMANDS a fully-gathered input, for the same reason
     * {@link #computeSelfCost} charges infinite cost otherwise: {@link OpenSearchExchangeReducer} is a
     * concat gather, not a merge exchange, so sorting per-partition and concatenating yields
     * partition-locally ordered rows in arrival order — wrong. Expressing that demand here is what
     * replaces {@code OpenSearchSortSplitRule}: Calcite materializes the reducer below the Sort via
     * {@link OpenSearchConvention#enforce} instead of a rule transforming the tree.
     *
     * <p>Two carve-outs, both load-bearing:
     * <ul>
     *   <li>A {@code perPartition} Sort is a shard-local top-N deliberately placed BELOW the gather by
     *       {@code OpenSearchSortPushdownRewriter} / {@code OpenSearchTopKRewriter}. It must RIDE its
     *       child's distribution — demanding SINGLETON here would hoist it above the gather and leave the
     *       shard fragment streaming every row (measured: 51ms → 5391ms over 10M rows).</li>
     *   <li>A Sort with no collation AND no fetch/offset is a no-op, so it imposes nothing and rides.</li>
     * </ul>
     */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        OpenSearchDistribution requiredDistribution = OpenSearchRelNode.distributionOf(required);
        if (requiredDistribution == null) {
            return null;
        }
        if (perPartition || ridesChildDistribution()) {
            // Rides: pass the demand straight down and deliver whatever the child delivers.
            return Pair.of(getTraitSet().replace(requiredDistribution), List.of(getInput().getTraitSet().replace(requiredDistribution)));
        }
        // Global sort/limit: only a SINGLETON request is satisfiable, and the input must be gathered.
        if (requiredDistribution.getType() != RelDistribution.Type.SINGLETON) {
            return null;
        }
        OpenSearchDistributionTraitDef traitDef = (OpenSearchDistributionTraitDef) requiredDistribution.getTraitDef();
        OpenSearchDistribution singleton = traitDef.coordSingleton();
        return Pair.of(getTraitSet().replace(singleton), List.of(getInput().getTraitSet().replace(singleton)));
    }

    /**
     * Only a riding Sort derives from its child. A global sort's output is SINGLETON regardless of what
     * the child offers, and claiming otherwise would let a parent consume it as partitioned.
     */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        if (childId != 0) {
            return null;
        }
        OpenSearchDistribution childDistribution = OpenSearchRelNode.distributionOf(childTraits);
        if (childDistribution == null) {
            return null;
        }
        if (perPartition || ridesChildDistribution()) {
            return Pair.of(getTraitSet().replace(childDistribution), List.of(childTraits));
        }
        return null;
    }

    /**
     * A global sort/limit must not have traits derived into it: {@link #computeSelfCost} charges infinite
     * cost unless its input is SINGLETON, and Calcite's default {@code LEFT_FIRST} derivation would build
     * a {@code Sort(RANDOM)} variant without the gather that makes it legal (a dead memo entry that fails
     * the whole plan). A riding Sort — perPartition shard-local top-N, or a no-op — derives normally.
     */
    @Override
    public DeriveMode getDeriveMode() {
        return (perPartition || ridesChildDistribution()) ? DeriveMode.LEFT_FIRST : DeriveMode.PROHIBITED;
    }

    /**
     * True when this Sort imposes no ORDERING requirement on its input, so it can ride a partitioned
     * child. Covers both a genuine no-op (no collation, no fetch/offset) and a pure LIMIT: a
     * partition-local fetch is correct because the coordinator needs only SOME N rows and each
     * partition's local N supplies them — the same reasoning
     * {@code OpenSearchSortPushdownRewriter} uses to push a bare {@code head N} below the gather.
     * Only a COLLATED sort needs the global gather (our ER is a concat, not a merge).
     */
    private boolean ridesChildDistribution() {
        return getCollation().getFieldCollations().isEmpty();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchSort(getCluster(), getTraitSet(), children.getFirst(), getCollation(), offset, fetch, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return LogicalSort.create(strippedChildren.getFirst(), getCollation(), offset, fetch);
    }
}
