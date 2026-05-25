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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchUnion;
import org.opensearch.analytics.spi.EngineCapability;

import java.util.ArrayList;
import java.util.List;

/**
 * HEP marker: {@link Union} → {@link OpenSearchUnion}.
 *
 * <p>Wraps every arm in an {@link OpenSearchExchangeReducer} so DAGBuilder cuts a
 * separate child stage per Union branch. Each child stage is then routed to its own
 * shard set (ShardTargetResolver finds the first {@code OpenSearchTableScan} in its
 * fragment, which now scans only that branch's index) and produces a distinct input
 * partition at the coordinator.
 *
 * <p>RANDOM arms need the gather; SINGLETON arms (single-shard tables, FINAL
 * aggregate outputs, etc.) are also wrapped — the ER is logically a no-op for
 * SINGLETON but the structural cut is what guarantees per-branch stage isolation,
 * which is essential when branches reference different indices. The
 * ConverterImpl-based ER dedupes into the input's RelSet subset when the input
 * already delivers SINGLETON, so no redundant ER is emitted.
 *
 * <p>Validates inputs are marked, intersects viable backends, and filters by
 * {@link EngineCapability#UNION}. Empty {@link Values} inputs are dropped (they
 * contribute zero rows — produced by {@code | append [ ]} subsearches with no source).
 * If only one non-empty input remains the Union collapses to that input.
 *
 * @opensearch.internal
 */
public class OpenSearchUnionRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchUnionRule(PlannerContext context) {
        super(operand(Union.class, any()), "OpenSearchUnionRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return !(call.rel(0) instanceof OpenSearchUnion);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Union union = call.rel(0);

        List<RelNode> markedInputs = new ArrayList<>(union.getInputs().size());
        List<String> viableBackends = null;

        for (RelNode input : union.getInputs()) {
            RelNode unwrapped = RelNodeUtils.unwrapHep(input);
            if (unwrapped instanceof Values values && values.getTuples().isEmpty()) {
                // Empty Values arms contribute no rows — drop them (from `append [ ]`).
                continue;
            }
            if (!(unwrapped instanceof OpenSearchRelNode openSearchInput)) {
                throw new IllegalStateException(
                    "Union rule encountered unmarked input ["
                        + unwrapped.getClass().getSimpleName()
                        + "]. "
                        + "All inputs must be converted to OpenSearchRelNode before union."
                );
            }
            markedInputs.add(unwrapped);
            if (viableBackends == null) {
                viableBackends = new ArrayList<>(openSearchInput.getViableBackends());
            } else {
                viableBackends.retainAll(openSearchInput.getViableBackends());
            }
        }

        if (markedInputs.isEmpty()) {
            throw new IllegalStateException("Union rule encountered Union with all-empty inputs");
        }

        if (markedInputs.size() == 1) {
            // Single non-empty input — collapse the Union.
            call.transformTo(markedInputs.getFirst());
            return;
        }

        List<String> unionCapable = context.getCapabilityRegistry().operatorBackends(EngineCapability.UNION);
        viableBackends.retainAll(unionCapable);

        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("No backend supports UNION among viable backends after intersecting inputs");
        }

        // HEP marking only — no ER insertion. OpenSearchUnion's cost gate (all inputs
        // must be SINGLETON) drives Volcano to insert ERs on each arm via TraitDef.convert.
        OpenSearchDistributionTraitDef distTraitDef = context.getDistributionTraitDef();
        RelTraitSet unionTraits = markedInputs.getFirst().getTraitSet().replace(distTraitDef.coordSingleton());
        call.transformTo(new OpenSearchUnion(union.getCluster(), unionTraits, markedInputs, union.all, viableBackends));
    }
}
