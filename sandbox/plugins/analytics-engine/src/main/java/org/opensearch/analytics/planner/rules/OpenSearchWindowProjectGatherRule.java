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
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;

/**
 * Registers the gathered alternative for a window ({@code RexOver}) {@link OpenSearchProject} that the
 * marking phase left over partitioned input: {@code Project(over, COORDINATOR+SINGLETON-input)}.
 *
 * <p><b>Why this rule exists.</b> A window project's frame semantics are global, so
 * {@link OpenSearchProject#computeSelfCost} charges INFINITE cost unless its input is SINGLETON.
 * {@code OpenSearchProjectRule} marks the project with its child's traits, which on a multi-shard scan
 * means the only member of the project's set is priced out of every plan. Satisfying a SINGLETON demand
 * from above by stacking an {@code OpenSearchExchangeReducer} ABOVE the project does not help — that ER's
 * input is the infinite project, so the gathered candidate is infinite too. The gather has to go BELOW.
 *
 * <p>In principle top-down mode covers this: {@link OpenSearchProject#passThroughTraits} turns a SINGLETON
 * requirement into a SINGLETON demand on the project's own input. But whether Calcite offers a node a
 * passThrough depends on the order groups are optimized in, and it does not always happen — for a window
 * project feeding a JOIN ARM underneath a Sort+Aggregate (the shape PPL {@code appendcol} lowers to) no
 * passThrough is attempted, every candidate stays infinite, and planning fails outright with
 * {@code CannotPlanException: There are not enough rules to produce a node with desired properties …
 * the cost is still infinite}. This rule makes the legal alternative unconditional rather than dependent
 * on that scheduling, which is the same {@code convert(child, coordSingleton)} idiom
 * {@link OpenSearchAggregateSplitRule} and {@code OpenSearchJoinSplitRule} already use.
 *
 * @opensearch.internal
 */
public class OpenSearchWindowProjectGatherRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchWindowProjectGatherRule(PlannerContext context) {
        super(operand(OpenSearchProject.class, operand(RelNode.class, any())), "OpenSearchWindowProjectGatherRule");
        this.context = context;
    }

    /**
     * Gated on the project's OWN distribution trait, never on its input's type: the input is a
     * {@code RelSubset} during Volcano, and matching on it re-fires forever. The transformed project is
     * SINGLETON, so it cannot match again — which is what terminates the rule.
     */
    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchProject project = call.rel(0);
        if (!project.containsOver()) {
            return false;
        }
        OpenSearchDistribution distribution = OpenSearchRelNode.distributionOf(project.getTraitSet());
        if (distribution == null) {
            return false;
        }
        // ANY is Volcano's "still exploring" placeholder — no decision to correct yet.
        return distribution.getType() != RelDistribution.Type.SINGLETON && distribution.getType() != RelDistribution.Type.ANY;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchProject project = call.rel(0);
        RelNode child = call.rel(1);

        RelTraitSet singletonTraits = project.getTraitSet().replace(context.getDistributionTraitDef().coordSingleton());
        call.transformTo(
            new OpenSearchProject(
                project.getCluster(),
                singletonTraits,
                convert(child, singletonTraits),
                project.getProjects(),
                project.getRowType(),
                project.getViableBackends()
            )
        );
    }
}
