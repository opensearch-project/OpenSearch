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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.spi.AggregateFunction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Volcano rule that splits an {@link OpenSearchAggregate} into PARTIAL + FINAL when the
 * input is partitioned. Both halves carry the original aggCall list — actual aggregate-call
 * rewriting (arg rebasing, COUNT→SUM, engine-native merge) runs post-Volcano in
 * {@link org.opensearch.analytics.planner.dag.DistributedAggregateRewriter}. The exchange
 * is inserted automatically via the SINGLETON trait request on partial's output.
 *
 * @opensearch.internal
 */
public class OpenSearchAggregateSplitRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchAggregateSplitRule(PlannerContext context) {
        super(operand(OpenSearchAggregate.class, operand(RelNode.class, any())), "OpenSearchAggregateSplitRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        return aggregate.getMode() == AggregateMode.SINGLE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        RelNode child = call.rel(1);

        // SINGLE-on-SINGLETON alternative — wins when the child already gathers below.
        RelTraitSet singletonTraits = aggregate.getTraitSet().replace(context.getDistributionTraitDef().coordSingleton());
        RelNode singletonChild = convert(child, singletonTraits);
        OpenSearchAggregate singleOnSingleton = new OpenSearchAggregate(
            aggregate.getCluster(),
            singletonTraits,
            singletonChild,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList(),
            AggregateMode.SINGLE,
            aggregate.getViableBackends(),
            aggregate.getCallAnnotations()
        );

        // PARTIAL + ER + FINAL alternative — wins when child is shard-partitioned.
        RelTraitSet partialTraits = child.getTraitSet().replace(OpenSearchConvention.INSTANCE);
        OpenSearchAggregate partial = new OpenSearchAggregate(
            aggregate.getCluster(),
            partialTraits,
            child,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList(),
            AggregateMode.PARTIAL,
            aggregate.getViableBackends(),
            aggregate.getCallAnnotations()
        );
        RelTraitSet finalTraits = partial.getTraitSet().replace(context.getDistributionTraitDef().coordSingleton());
        RelNode gathered = convert(partial, finalTraits);
        Map<Integer, List<RexLiteral>> finalExtraLiterals = captureLiteralArgsForFinal(aggregate.getAggCallList(), child);

        OpenSearchAggregate finalAggregate = new OpenSearchAggregate(
            aggregate.getCluster(),
            finalTraits,
            gathered,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList(),
            AggregateMode.FINAL,
            aggregate.getViableBackends(),
            aggregate.getCallAnnotations(),
            finalExtraLiterals
        );

        call.getPlanner().ensureRegistered(singleOnSingleton, aggregate);
        call.transformTo(finalAggregate);
    }

    /**
     * For each STATE_EXPANDING aggCall, capture literal non-state args (e.g. TAKE's N)
     * from the child Project so {@code DistributedAggregateRewriter} can re-create them
     * below FINAL. Calls with non-literal args contribute no entry — the FINAL rewrite
     * then takes its single-state-column path.
     */
    private static Map<Integer, List<RexLiteral>> captureLiteralArgsForFinal(List<AggregateCall> aggCalls, RelNode child) {
        if (!(RelNodeUtils.unwrapHep(child) instanceof Project project)) {
            return Map.of();
        }
        List<RexNode> projects = project.getProjects();
        Map<Integer, List<RexLiteral>> captured = new LinkedHashMap<>();
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall call = aggCalls.get(i);
            AggregateFunction fn = AggregateFunction.fromSqlAggFunction(call.getAggregation());
            if (fn == null || fn.getType() != AggregateFunction.Type.STATE_EXPANDING) continue;
            List<Integer> args = call.getArgList();
            if (args.size() < 2) continue;
            // arg 0 is the value/state column; args 1+ are the configuration literals.
            List<RexLiteral> literals = new ArrayList<>(args.size() - 1);
            boolean allLiteral = true;
            for (int a = 1; a < args.size(); a++) {
                int colIdx = args.get(a);
                if (colIdx < 0 || colIdx >= projects.size() || !(projects.get(colIdx) instanceof RexLiteral lit)) {
                    allLiteral = false;
                    break;
                }
                literals.add(lit);
            }
            if (allLiteral && !literals.isEmpty()) {
                captured.put(i, List.copyOf(literals));
            }
        }
        return captured;
    }
}
