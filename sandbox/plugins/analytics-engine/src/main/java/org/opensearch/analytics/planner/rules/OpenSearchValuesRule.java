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
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchValues;
import org.opensearch.analytics.spi.EngineCapability;

import java.util.List;

/**
 * Converts {@link LogicalValues} → {@link OpenSearchValues}. Narrows viable backends
 * to those declaring {@link EngineCapability#VALUES} — literal-row materialisation
 * needs the backend to honour a Substrait {@code VirtualTable} (or equivalent), so
 * scan capability alone isn't enough.
 *
 * @opensearch.internal
 */
public class OpenSearchValuesRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchValuesRule(PlannerContext context) {
        super(operand(Values.class, none()), "OpenSearchValuesRule");
        this.context = context;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Values values = call.rel(0);
        if (values instanceof OpenSearchValues) {
            return;
        }
        List<String> viableBackends = context.getCapabilityRegistry().operatorBackends(EngineCapability.VALUES);
        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("No backend supports VALUES");
        }
        RelTraitSet traits = RelTraitSet.createEmpty()
            .plus(OpenSearchConvention.INSTANCE)
            .plus(context.getDistributionTraitDef().coordSingleton());
        call.transformTo(new OpenSearchValues(values.getCluster(), traits, values.getRowType(), values.getTuples(), viableBackends));
    }
}
