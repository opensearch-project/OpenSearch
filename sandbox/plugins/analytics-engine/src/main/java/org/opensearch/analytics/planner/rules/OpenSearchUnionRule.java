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
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.opensearch.analytics.planner.CapabilityResolutionUtils;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchUnion;
import org.opensearch.analytics.spi.EngineCapability;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts {@link Union} → {@link OpenSearchUnion}.
 *
 * <p>Validates that all inputs are marked, intersects their viable backends, and
 * filters by {@link EngineCapability#UNION}. Empty {@link Values} inputs (the
 * shape produced by an {@code | append [ ]} subsearch with no source) are dropped
 * — they contribute zero rows to the result. If only one non-empty input remains
 * the Union node is collapsed to that input.
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
                // Empty values inputs contribute no rows — drop them. Only meaningful
                // for testAppendEmptySearchCommand-style queries where `append [ ]`
                // yields a LogicalValues(tuples=[[]]) with the union's output schema.
                continue;
            }
            if (!(unwrapped instanceof OpenSearchRelNode openSearchInput)) {
                throw new IllegalStateException(
                    "Union rule encountered unmarked input [" + unwrapped.getClass().getSimpleName() + "]. "
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
            // Defensive — Calcite shouldn't construct a Union with all-empty inputs, but
            // surfacing a clear message beats letting downstream rules fail mysteriously.
            throw new IllegalStateException("Union rule encountered Union with all-empty inputs");
        }

        if (markedInputs.size() == 1) {
            // Single non-empty input — collapse the Union. Row type is preserved by
            // construction (Calcite requires every Union input to share the row type).
            call.transformTo(markedInputs.getFirst());
            return;
        }

        List<String> unionCapable = context.getCapabilityRegistry().operatorBackends(EngineCapability.UNION);
        viableBackends.retainAll(unionCapable);

        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("No backend supports UNION among viable backends after intersecting inputs");
        }

        // Wrap every input in an OpenSearchExchangeReducer so DAGBuilder cuts a
        // separate child stage per Union branch. Each child stage is then routed to
        // its own shard set (ShardTargetResolver finds the first OpenSearchTableScan
        // in its fragment, which now scans only that branch's index) and produces a
        // distinct input partition at the coordinator.
        //
        // RANDOM inputs need the gather; SINGLETON inputs (single-shard tables, FINAL
        // aggregate outputs, etc.) are also wrapped — the ER is logically a no-op for
        // SINGLETON but the structural cut is what guarantees per-branch stage isolation,
        // which is essential when branches reference different indices.
        OpenSearchDistributionTraitDef distTraitDef = context.getDistributionTraitDef();
        List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(context.getCapabilityRegistry(), viableBackends);

        List<RelNode> gatheredInputs = new ArrayList<>(markedInputs.size());
        for (RelNode markedInput : markedInputs) {
            RelTraitSet singletonTraits = markedInput.getTraitSet().replace(distTraitDef.singleton());
            gatheredInputs.add(new OpenSearchExchangeReducer(union.getCluster(), singletonTraits, markedInput, reduceViable));
        }

        RelTraitSet unionTraits = gatheredInputs.getFirst().getTraitSet().replace(distTraitDef.singleton());
        call.transformTo(new OpenSearchUnion(union.getCluster(), unionTraits, gatheredInputs, union.all, viableBackends));
    }
}
