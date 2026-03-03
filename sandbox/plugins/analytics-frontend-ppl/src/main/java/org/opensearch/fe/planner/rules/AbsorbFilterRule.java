/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.opensearch.analytics.backend.EngineCapabilities;
import org.opensearch.fe.planner.rel.OpenSearchBoundaryTableScan;

/**
 * RelOptRule that absorbs a {@link LogicalFilter} into an {@link OpenSearchBoundaryTableScan}.
 *
 * <p>Pattern: {@code LogicalFilter} on top of {@code OpenSearchBoundaryTableScan}.
 *
 * <p>When the rule matches, it checks whether the engine supports the filter operator
 * and all functions in the filter condition via {@link EngineCapabilities}. If supported,
 * the filter is absorbed into the boundary node's logical fragment by wrapping the
 * existing fragment with a new {@code LogicalFilter}.
 *
 * <p>This is NOT a ConverterRule — it transforms an already-converted boundary node
 * by growing its internal logical fragment.
 */
public class AbsorbFilterRule extends RelOptRule {

    private final EngineCapabilities capabilities;

    /**
     * Create a rule instance with the given engine capabilities.
     *
     * @param capabilities the engine capabilities used to gate absorption
     * @return a new AbsorbFilterRule
     */
    public static AbsorbFilterRule create(EngineCapabilities capabilities) {
        return new AbsorbFilterRule(capabilities);
    }

    private AbsorbFilterRule(EngineCapabilities capabilities) {
        super(operand(LogicalFilter.class, operand(OpenSearchBoundaryTableScan.class, none())), "AbsorbFilterRule");
        this.capabilities = capabilities;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        OpenSearchBoundaryTableScan boundary = call.rel(1);

        // Check that the engine supports the filter operator and all functions in the condition
        if (!capabilities.supportsOperator(filter) || !capabilities.supportsAllFunctions(filter.getCondition())) {
            return;
        }

        // Wrap the existing logical fragment with the filter to build the new absorbed subtree
        LogicalFilter absorbedFilter = filter.copy(filter.getTraitSet(), boundary.getLogicalFragment(), filter.getCondition());

        // Create a new boundary node with the expanded logical fragment
        OpenSearchBoundaryTableScan newBoundary = new OpenSearchBoundaryTableScan(
            boundary.getCluster(),
            boundary.getTraitSet(),
            boundary.getTable(),
            absorbedFilter,
            boundary.getEngineExecutor()
        );

        call.transformTo(newBoundary);
    }
}
