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
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;

import java.util.List;

/**
 * Registers a HASH-distributed alternative for an equi-{@link OpenSearchJoin} that was marked by
 * {@link OpenSearchJoinRule} with SINGLETON reducer inputs. Produces a sibling {@code OpenSearchJoin}
 * whose left input is an {@link OpenSearchShuffleExchange} on the left join keys and right input is
 * an {@link OpenSearchShuffleExchange} on the right join keys.
 *
 * <p>The explicit two-exchange insertion is required: Calcite's {@code Convention.enforce()} cannot
 * express "HASH(leftKeys) on left input AND HASH(rightKeys) on right input" with a single required
 * distribution on the join node. Matches OLAP's {@code MppJoinRule} pattern one-for-one.
 *
 * <p>Skips non-equi joins ({@code JoinInfo.leftKeys.isEmpty()}). Cost estimation at the scheduler
 * level ({@code JoinStrategyAdvisor} in {@code exec/join}) decides between this alternative and the
 * coordinator-centric one.
 *
 * @opensearch.internal
 */
public class OpenSearchHashJoinRule extends RelOptRule {

    private static final Logger LOGGER = LogManager.getLogger(OpenSearchHashJoinRule.class);

    private final PlannerContext context;

    public OpenSearchHashJoinRule(PlannerContext context) {
        super(operand(OpenSearchJoin.class, any()), "OpenSearchHashJoinRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchJoin join = call.rel(0);
        JoinInfo joinInfo = join.analyzeCondition();
        // Equi-joins only — shuffle can't partition by a non-key predicate.
        if (joinInfo.leftKeys.isEmpty()) {
            return false;
        }
        // Skip joins already rewritten to a HASH-shuffle shape — gate on the join node's own
        // distribution trait rather than its children, because Volcano wraps child inputs in
        // RelSubsets which makes `instanceof OpenSearchShuffleExchange` always false and would
        // cause infinite rule re-fire.
        OpenSearchDistribution dist = join.getTraitSet().getTrait(context.getDistributionTraitDef());
        return dist == null || dist.getType() != RelDistribution.Type.HASH_DISTRIBUTED;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchJoin join = call.rel(0);
        JoinInfo joinInfo = join.analyzeCondition();

        // Take the join's inputs as-is — they are RelSubsets inside Volcano, representing the
        // equivalence class of every alternative for that child. Don't unwrap or peek at
        // OpenSearchExchangeReducer here: the alternative we register becomes a sibling of the
        // SINGLETON gather in the same set, and Volcano's trait conversion resolves which
        // physical form is cheaper.
        RelNode leftInput = join.getLeft();
        RelNode rightInput = join.getRight();

        OpenSearchDistributionTraitDef distTraitDef = context.getDistributionTraitDef();
        List<Integer> leftKeys = joinInfo.leftKeys.toIntegerList();
        List<Integer> rightKeys = joinInfo.rightKeys.toIntegerList();

        // Partition count is 0 at planning time — the coordinator sets the real count on DAG rewrite
        // (see OpenSearchDistributionTraitDef for rationale).
        RelTraitSet leftShuffleTraits = leftInput.getTraitSet().replace(distTraitDef.hash(leftKeys));
        RelTraitSet rightShuffleTraits = rightInput.getTraitSet().replace(distTraitDef.hash(rightKeys));
        RelNode leftShuffle = new OpenSearchShuffleExchange(
            join.getCluster(),
            leftShuffleTraits,
            leftInput,
            leftKeys,
            0,
            join.getViableBackends()
        );
        RelNode rightShuffle = new OpenSearchShuffleExchange(
            join.getCluster(),
            rightShuffleTraits,
            rightInput,
            rightKeys,
            0,
            join.getViableBackends()
        );

        // The shuffled join itself carries HASH(leftKeys) as its required distribution — the
        // physical execution happens at workers that own one partition each. This HASH trait is
        // also what matches() checks to prevent re-fire on the new alternative.
        RelTraitSet joinTraits = join.getTraitSet().replace(distTraitDef.hash(leftKeys));
        RelNode shuffleJoin = new OpenSearchJoin(
            join.getCluster(),
            joinTraits,
            leftShuffle,
            rightShuffle,
            join.getCondition(),
            join.getJoinType(),
            join.getViableBackends()
        );

        LOGGER.debug(
            "Registered HASH join alternative: leftKeys={}, rightKeys={}, viableBackends={}",
            leftKeys,
            rightKeys,
            join.getViableBackends()
        );
        call.transformTo(shuffleJoin);
    }
}
