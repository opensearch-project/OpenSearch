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
     * A Sort imposes no cost of its own beyond the one invariant every operator shares: an input whose
     * placement is still UNRESOLVED has no defined cost, because nothing above it has a defined location or
     * a defined correctness. The marking phase deliberately registers such a seed (see
     * {@code OpenSearchSortRule}), and this is what confines it to the ANY subset so only the concrete
     * alternatives {@link #passThroughTraits} / {@link #deriveTraits} produce are consumable.
     *
     * <p>The placement REQUIREMENT — a global sort/limit needs its input gathered — is asserted, not priced.
     * It used to be an infinite-cost branch, i.e. legality expressed through a channel that exists for
     * ranking; that shape is now unreachable because both propagation directions state the requirement and
     * set self and input traits together, so the illegal pair cannot be constructed. See
     * {@link #assertPlacementIsLegal}.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (hasUnresolvedInput()) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        assert assertPlacementIsLegal();
        return planner.getCostFactory().makeTinyCost();
    }

    /**
     * The placement a Sort shape implies. ASSERTED rather than priced, and asserted rather than deleted,
     * because a violation is a silently WRONG RESULT, not a slow plan: {@link OpenSearchExchangeReducer} is a
     * concat gather, not a merge exchange, so sorting each partition and concatenating yields
     * partition-locally ordered rows in arrival order, and a per-partition {@code fetch=N} over P partitions
     * returns N×P rows instead of N.
     *
     * <p>Unreachable because the requirement is stated in BOTH propagation directions — Volcano builds
     * alternatives from either, so closing only one leaves the other able to produce the illegal pair:
     * {@link #passThroughTraits} answers a non-singleton demand with the gathered shape rather than riding
     * it, and {@link #deriveTraits} derives the gathered variant over a partitioned child. The two carve-outs
     * that legitimately ride a partitioned input — an already-pushed {@code perPartition} top-N and a no-op
     * Sort — are the same ones both hooks recognise, so this predicate cannot disagree with them.
     *
     * @return always {@code true}, so this reads as {@code assert assertPlacementIsLegal()}
     * @throws IllegalStateException when a global sort/limit meets input it cannot correctly consume
     */
    private boolean assertPlacementIsLegal() {
        // An UNRESOLVED seed makes no placement claim, so it cannot contradict its input; only a node that
        // has COMMITTED to a distribution can be illegal.
        OpenSearchDistribution selfDistribution = OpenSearchRelNode.distributionOf(getTraitSet());
        if (selfDistribution == null || selfDistribution.getType() == RelDistribution.Type.ANY) {
            return true;
        }
        if (perPartition || ridesChildDistribution()) {
            return true;
        }
        // No distribution trait at all is not the same as an unresolved one: it carries no placement claim
        // either way, so there is nothing to check.
        OpenSearchDistribution inputDistribution = OpenSearchRelNode.distributionOf(getInput().getTraitSet());
        if (inputDistribution == null) {
            return true;
        }
        if (inputDistribution.getType() != RelDistribution.Type.SINGLETON) {
            throw new IllegalStateException(
                "Global sort/limit over partitioned input ["
                    + inputDistribution
                    + "] would concat partition-locally ordered rows and return fetch×partitions rows"
            );
        }
        return true;
    }

    // ---- PhysicalNode (top-down trait propagation) ----

    /**
     * A collated-or-limiting Sort DEMANDS a fully-gathered input, for the same reason
     * {@link #assertPlacementIsLegal} refuses anything else: {@link OpenSearchExchangeReducer} is a
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
     *       shard fragment streaming every row instead of its top N.</li>
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
        // Global sort/limit: it gathers its input and its output is therefore COORDINATOR+SINGLETON,
        // whatever was asked for. Answering a NON-singleton demand with the singleton shape (rather than
        // declining) is deliberate and load-bearing: this Sort may sit under a join, whose inputs are asked
        // for RANDOM(SHARD) or WORKER+HASH. No exchange can MOVE data to RANDOM(SHARD) — that is a scan's
        // natural locality — so declining leaves the subset empty and the whole query fails with
        // "Missing conversion is OpenSearchSort[]". Returning the gathered shape lets the parent see a
        // SINGLETON child and place its own exchange, which reproduces the pre-top-down plan
        // (Sort(fetch) over an ER, per DAGShapeTests case2/case4). Calcite tolerates a passThrough whose
        // delivered traits differ from the request; it costs the result and the parent re-enforces.
        OpenSearchDistributionTraitDef traitDef = (OpenSearchDistributionTraitDef) requiredDistribution.getTraitDef();
        // A SINGLETON demand passes through VERBATIM, locality included. The root asks for anySingleton
        // (locality null), which a 1-shard SINGLETON(SHARD) subtree already satisfies — narrowing it to
        // COORDINATOR here inserted a gather on every single-shard query, for data that was already on one
        // node. A NON-singleton demand (a join asking its input for RANDOM(SHARD) or WORKER+HASH) still gets
        // the gathered shape, per the paragraph above.
        OpenSearchDistribution singleton = requiredDistribution.getType() == RelDistribution.Type.SINGLETON
            ? requiredDistribution
            : traitDef.coordSingleton();
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
        // A global sort/limit over a PARTITIONED child: the only legal shape gathers the child first, so
        // derive the gathered variant rather than declining. Declining leaves the Sort with no node in the
        // partitioned subset its parent (a join demanding RANDOM(SHARD) / WORKER+HASH inputs) created, and
        // since no exchange can produce RANDOM(SHARD) the subset stays empty → CannotPlanException
        // "Missing conversion is OpenSearchSort[]" (DAGShapeTests case2/case4, whose right join input is a
        // bare LIMIT). Requesting coordSingleton on the input makes Calcite insert the reducer BELOW this
        // Sort, which is the pre-top-down shape: Sort(fetch) over ER over the shard scan.
        if (childDistribution.getType() == RelDistribution.Type.SINGLETON) {
            // Already gathered: this Sort delivers the child's singleton VERBATIM, locality included. Saying
            // so matters even though passThroughTraits would answer a SINGLETON demand the same way, because
            // passThrough only fires on a demand and the demand is not always for this locality. A parent
            // that gathers to COORDINATOR asks for COORDINATOR+SINGLETON; if that is the only alternative
            // this Sort offers, the gather lands BELOW it and a shard-local sort/limit becomes a
            // coordinator-side one — every row crosses the wire instead of the top N. Declining also hides
            // the arm's real placement from the split rules, which read it to decide whether a co-located
            // (no-exchange) plan is available at all.
            return Pair.of(getTraitSet().replace(childDistribution), List.of(childTraits));
        }
        OpenSearchDistributionTraitDef traitDef = (OpenSearchDistributionTraitDef) childDistribution.getTraitDef();
        OpenSearchDistribution singleton = traitDef.coordSingleton();
        return Pair.of(getTraitSet().replace(singleton), List.of(childTraits.replace(singleton)));
    }

    /**
     * Every Sort shape derives from its single input, but they answer differently (see
     * {@link #deriveTraits}): a riding Sort adopts the child's distribution, while a global sort/limit
     * derives the GATHERED variant so the reducer lands below it.
     *
     * <p>{@code LEFT_FIRST} rather than {@code PROHIBITED} even for a global sort: it must contribute a
     * node to whatever subset its parent created, or a join demanding a partitioned input leaves the
     * Sort's subset empty and the query dies with "Missing conversion is OpenSearchSort[]". The reason
     * {@code PROHIBITED} was needed originally — Calcite's DEFAULT derivation manufacturing an illegal
     * {@code Sort(RANDOM)} variant, which cost then had to reject — no longer applies, because
     * {@link #deriveTraits} now returns the gathered shape instead of passing the partitioned child trait
     * through. That is also what lets the rejection be an assertion rather than a price.
     */
    @Override
    public DeriveMode getDeriveMode() {
        return DeriveMode.LEFT_FIRST;
    }

    /**
     * True when this Sort constrains NOTHING, so it can ride a partitioned child: no collation, no fetch
     * and no offset — a genuine no-op.
     *
     * <p><b>A bare LIMIT does NOT ride.</b> It is tempting to let one, on the reasoning that "the
     * coordinator needs only SOME N rows and each partition's local N supplies them" — but that argument
     * justifies pushing a limit DOWN to the shards, not REPLACING the coordinator's limit with it. A
     * riding {@code Sort(fetch=N)} executes once per partition and nothing caps the concatenated result,
     * so a 2-shard {@code LIMIT 10} returns 20 rows. The correct shape keeps a limit on BOTH sides of the
     * gather — {@code Sort(fetch=N) / ER / Sort(fetch=N, perPartition) / scan} — which is what
     * {@code OpenSearchSortPushdownRewriter} builds by pushing a {@code perPartition} copy below the ER
     * while leaving this one above it. So a fetch/offset Sort must DEMAND SINGLETON (gathering its input),
     * and only the already-pushed {@code perPartition} copy rides.
     *
     * <p>Collation is likewise not the whole test: our ER is a concat, not a merge exchange, so a collated
     * sort needs the global gather too. Both conditions therefore fold into "constrains nothing".
     *
     * <p><b>In practice this never returns true, and that is Calcite's doing, not ours.</b> Its {@code Sort}
     * constructor asserts {@code "trivial sort"} against precisely this shape, so with assertions on (the
     * suites, and every {@code -ea} JVM) such a Sort cannot be built at all — {@code perPartition} is the only
     * rider that actually occurs. Kept as a total predicate rather than deleted, so the three callers stay
     * readable as "does this Sort constrain anything?" instead of silently meaning "is it perPartition?", and
     * so the answer stays correct if that constructor assertion is ever relaxed. Pinned by
     * {@code OpenSearchSortPlacementAssertionTests}.
     */
    private boolean ridesChildDistribution() {
        return getCollation().getFieldCollations().isEmpty() && fetch == null && offset == null;
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
