/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Uncollect;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchUncollect;
import org.opensearch.analytics.spi.EngineCapability;

import java.util.List;

/** Marks Uncollect for execution on backends supporting multi-value expansion. */
public class OpenSearchUncollectRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchUncollectRule(PlannerContext context) {
        super(operand(Uncollect.class, operand(RelNode.class, any())), "OpenSearchUncollectRule");
        this.context = context;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Uncollect uncollect = call.rel(0);
        if (uncollect instanceof OpenSearchUncollect) {
            return;
        }
        RelNode input = RelNodeUtils.unwrapHep(call.rel(1));
        if (!(input instanceof OpenSearchRelNode openSearchInput)) {
            throw new IllegalStateException("Uncollect rule encountered unmarked child [" + input.getClass().getSimpleName() + "]");
        }
        List<String> expansionBackends = context.getCapabilityRegistry().operatorBackends(EngineCapability.MULTI_VALUE_EXPAND);
        List<String> viableBackends = openSearchInput.getViableBackends().stream().filter(expansionBackends::contains).toList();
        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("No viable backend supports multi-value expansion");
        }
        call.transformTo(
            new OpenSearchUncollect(
                uncollect.getCluster(),
                input.getTraitSet(),
                input,
                uncollect.withOrdinality,
                uncollect.getItemAliases(),
                viableBackends
            )
        );
    }
}
