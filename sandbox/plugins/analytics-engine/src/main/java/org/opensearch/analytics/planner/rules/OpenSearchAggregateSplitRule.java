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
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;

import java.util.ArrayList;
import java.util.List;

/**
 * Volcano CBO rule that splits an {@link OpenSearchAggregate} into
 * PARTIAL + FINAL when the input is partitioned.
 *
 * <p>Requests SINGLETON distribution on the partial output, letting Volcano's
 * trait enforcement (via {@code ExpandConversionRule} + {@code OpenSearchDistributionTraitDef})
 * automatically insert an {@code OpenSearchExchangeReducer}.
 *
 * <p>TODO (plan forking): aggregate decomposition is intentionally deferred to plan forking
 * resolution, after a single backend has been chosen per alternative. Decomposition is
 * backend-specific — different backends may emit different partial state schemas for the
 * same function (e.g. standard SUM+COUNT for AVG vs a backend's native running state).
 * Applying decomposition here would force a single schema before backends are resolved,
 * which breaks the multi-alternative model.
 *
 * <p>During plan forking resolution, for each PARTIAL+FINAL pair in a chosen-backend alternative:
 * <ol>
 *   <li>Look up {@link org.opensearch.analytics.spi.AggregateCapability#decomposition()} for
 *       each AggregateCall using the chosen backend.</li>
 *   <li>If null: apply Calcite's {@code AggregateReduceFunctionsRule} to rewrite
 *       AVG → SUM/COUNT, STDDEV → SUM(x²)+SUM(x)+COUNT, etc.</li>
 *   <li>If non-null: use {@link org.opensearch.analytics.spi.AggregateDecomposition#partialCalls(AggregateCall)}
 *       to rewrite PARTIAL's aggCalls and output row type, and
 *       {@code AggregateDecomposition.finalExpression()} to
 *       rewrite FINAL's aggCalls. Both must be updated together — the exchange row type
 *       between them must be consistent within the same plan alternative.</li>
 * </ol>
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

        // Partial aggregate: runs on each partition, keeps input's traits
        RelTraitSet partialTraits = child.getTraitSet().replace(OpenSearchConvention.INSTANCE);
        OpenSearchAggregate partial = new OpenSearchAggregate(
            aggregate.getCluster(),
            partialTraits,
            child,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList(),
            AggregateMode.PARTIAL,
            aggregate.getViableBackends()
        );

        // Request SINGLETON distribution — Volcano inserts Exchange automatically
        RelTraitSet singletonTraits = partial.getTraitSet().replace(context.getDistributionTraitDef().singleton());
        RelNode gathered = convert(partial, singletonTraits);

        // Final aggregate: merges partial states at coordinator.
        // Remap argLists: the PARTIAL output has group-by columns first, then one
        // column per aggCall. Each FINAL aggCall must reference its corresponding
        // column in the PARTIAL output, not the original input column.
        int groupCount = aggregate.getGroupSet().cardinality();
        List<AggregateCall> finalCalls = new ArrayList<>();
        for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
            AggregateCall origCall = aggregate.getAggCallList().get(i);
            finalCalls.add(origCall.adaptTo(gathered, List.of(groupCount + i), origCall.filterArg, groupCount, aggregate.getGroupCount()));
        }
        OpenSearchAggregate finalAggregate = new OpenSearchAggregate(
            aggregate.getCluster(),
            singletonTraits,
            gathered,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            finalCalls,
            AggregateMode.FINAL,
            aggregate.getViableBackends()
        );

        call.transformTo(finalAggregate);
    }
}
