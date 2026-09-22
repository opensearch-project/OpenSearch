/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;

import java.util.ArrayList;
import java.util.List;

/**
 * Produces SINGLETON variants of passthrough-marked single-input operators (Filter,
 * Project, Sort, FINAL Aggregate) so Volcano can bridge the root's SINGLETON demand
 * down to a SINGLETON-producing rel after a split rule fires.
 *
 * <p>HEP marking passes each parent the child's trait, so over a multi-shard scan the
 * whole tree wears RANDOM. When AggregateSplit later adds a SINGLETON subset inside
 * the Aggregate's RelSet, parents have no SINGLETON-demanding variant to reach it.
 * This rule creates that variant; trait then propagates up derive-step by derive-step.
 *
 * <p>Skips TableScan, ExchangeReducer, Join, Union (handled elsewhere), SINGLE
 * Aggregate (needs OpenSearchAggregateSplitRule's structural split), and PARTIAL
 * Aggregate (shard-side by contract).
 *
 * <h2>Why this rule exists at all</h2>
 *
 * <p>Volcano is running in <b>bottom-up</b> mode. In bottom-up mode it does not
 * auto-propagate trait demands from a parent down to a child's RelSet — a parent only
 * sees the variants its child has already produced.
 *
 * <p>Concrete failure without this rule:
 * <pre>
 * Marked tree (multi-shard, all SHARD+RANDOM after HEP):
 *   Project(RANDOM)
 *     Aggregate(SINGLE, RANDOM)
 *       Scan(RANDOM)
 *
 * After AggregateSplitRule fires the Aggregate's RelSet contains:
 *   [SHARD+RANDOM]            : SINGLE
 *   [COORDINATOR+SINGLETON]   : FINAL ← ER ← PARTIAL ← Scan
 *
 * Root demands SINGLETON. Volcano walks down looking for a SINGLETON subset.
 * Project's RelSet only contains the (RANDOM) variant — no SINGLETON Project exists,
 * so Volcano falls back to its converter machinery and plants an ER ABOVE the Project:
 *   ER ← Project(RANDOM) ← Aggregate(SINGLE, RANDOM) ← Scan
 * Raw rows ship to coord, the FINAL-SINGLETON variant AggregateSplit built is unreached.
 * </pre>
 *
 * <p>This rule fixes that by manufacturing a SINGLETON-trait copy of each spine
 * operator. The Project's RelSet now contains a {@code Project(SINGLETON)} variant
 * which demands SINGLETON from its child, hitting the {@code FINAL-SINGLETON} subset
 * directly. The ER stays <i>inside</i> the aggregate split (between PARTIAL and FINAL)
 * where only pre-aggregated rows transit.
 *
 * <p>Split rules don't cover this: each split only adds variants for its own operator
 * and doesn't know about its parents. Trait propagation up the spine has to come from
 * somewhere else.
 *
 * <h2>The fix that would let us delete this rule</h2>
 *
 * <p>Switch Volcano to top-down mode ({@code setTopDownOpt}) and implement
 * {@code PhysicalNode.passThrough()} on each operator. Calcite would then auto-generate
 * the SINGLETON variants of parents during planning, and this rule + the marker rules +
 * AggregateSplit-as-Volcano-rule would collapse into a much smaller set. Tracked as a
 * future refactor — out of scope for the CBO-only ER insertion work.
 *
 * @opensearch.internal
 */
public class OpenSearchDistributionDeriveRule extends RelOptRule {

    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchDistributionDeriveRule(PlannerContext context) {
        super(operand(RelNode.class, any()), "OpenSearchDistributionDeriveRule");
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        // Only fire on single-input operators we know how to recreate at SINGLETON.
        // Excludes OpenSearchTableScan (source), OpenSearchExchangeReducer (already SINGLETON),
        // OpenSearchJoin (has its own split rule), OpenSearchUnion (N-ary; handled via arms).
        if (!(rel instanceof OpenSearchFilter
            || rel instanceof OpenSearchProject
            || rel instanceof OpenSearchSort
            || rel instanceof OpenSearchAggregate)) return false;
        // SINGLE aggregates should NOT be derived to EXECUTION(SINGLETON): that would
        // bypass {@link OpenSearchAggregateSplitRule}'s PARTIAL/FINAL decomposition and
        // ship raw rows to coord instead of pre-aggregating. PARTIAL and FINAL CAN be
        // derived — needed so nested aggregates over coord-side pipelines can plan.
        if (rel instanceof OpenSearchAggregate aggregate && aggregate.getMode() == AggregateMode.SINGLE) return false;
        return !isSingleton(rel.getTraitSet());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        RelTraitSet singletonTraits = rel.getTraitSet().replace(distTraitDef.coordSingleton());
        RelNode singletonInput = convert(rel.getInputs().getFirst(), singletonTraits);
        List<RelNode> newInputs = new ArrayList<>(rel.getInputs().size());
        newInputs.add(singletonInput);
        for (int i = 1; i < rel.getInputs().size(); i++)
            newInputs.add(rel.getInputs().get(i));
        call.transformTo(rel.copy(singletonTraits, newInputs));
    }

    private static boolean isSingleton(RelTraitSet traits) {
        OpenSearchDistribution dist = findDistribution(traits);
        return dist != null && dist.getType() == RelDistribution.Type.SINGLETON;
    }

    private static OpenSearchDistribution findDistribution(RelTraitSet traits) {
        for (int i = 0; i < traits.size(); i++) {
            RelTrait trait = traits.getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) return dist;
        }
        return null;
    }
}
