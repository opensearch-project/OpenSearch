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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.spi.OperatorCapability;

import java.util.List;

/**
 * Converts {@link Sort} → {@link OpenSearchSort}.
 *
 * <p>Validates that the chosen backend supports {@link OperatorCapability#SORT}.
 *
 * <p>TODO: for multi-shard Sort+Limit, the split into partial sort
 * per shard + final merge sort at coordinator happens via CBO trait
 * propagation (same as aggregate split).
 *
 * @opensearch.internal
 */
public class OpenSearchSortRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchSortRule(PlannerContext context) {
        super(operand(Sort.class, operand(RelNode.class, any())), "OpenSearchSortRule");
        this.context = context;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        RelNode child = call.rel(1);

        if (sort instanceof OpenSearchSort) {
            return;
        }

        if (!(child instanceof OpenSearchRelNode openSearchChild)) {
            throw new IllegalStateException("Sort rule encountered unmarked child [" + child.getClass().getSimpleName() + "]");
        }

        List<String> childViableBackends = openSearchChild.getViableBackends();
        List<String> sortCapable = context.getCapabilityRegistry().operatorBackends(OperatorCapability.SORT);

        List<String> viableBackends = childViableBackends.stream().filter(sortCapable::contains).toList();

        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("No backend supports SORT capability among " + childViableBackends);
        }

        call.transformTo(
            new OpenSearchSort(
                sort.getCluster(),
                child.getTraitSet(),
                RelNodeUtils.unwrapHep(sort.getInput()),
                sort.getCollation(),
                sort.offset,
                sort.fetch,
                viableBackends
            )
        );
    }
}
