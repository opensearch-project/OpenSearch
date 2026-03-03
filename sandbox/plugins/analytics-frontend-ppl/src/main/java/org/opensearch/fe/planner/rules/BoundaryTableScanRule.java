/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.opensearch.fe.planner.PlanExecutor;
import org.opensearch.fe.planner.rel.OpenSearchBoundaryTableScan;

/**
 * ConverterRule: LogicalTableScan (Convention.NONE) → OpenSearchBoundaryTableScan (OPENSEARCH).
 *
 * <p>Converts a {@link LogicalTableScan} into an {@link OpenSearchBoundaryTableScan} with the
 * scan itself as the initial logical fragment. The boundary node carries an {@link PlanExecutor}
 * so it can delegate execution at {@code bind()} time.
 */
public class BoundaryTableScanRule extends ConverterRule {

    private final PlanExecutor planExecutor;

    /**
     * Create a rule instance that converts LogicalTableScan to OpenSearchBoundaryTableScan.
     *
     * @param planExecutor the engine executor passed to the boundary node for bind-time execution
     * @return a new BoundaryTableScanRule
     */
    public static BoundaryTableScanRule create(PlanExecutor planExecutor) {
        return new BoundaryTableScanRule(
            Config.INSTANCE.withConversion(LogicalTableScan.class, Convention.NONE, EnumerableConvention.INSTANCE, "BoundaryTableScanRule"),
            planExecutor
        );
    }

    private BoundaryTableScanRule(Config config, PlanExecutor planExecutor) {
        super(config);
        this.planExecutor = planExecutor;
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalTableScan scan = (LogicalTableScan) rel;
        return new OpenSearchBoundaryTableScan(
            scan.getCluster(),
            scan.getTraitSet().replace(EnumerableConvention.INSTANCE),
            scan.getTable(),
            scan,
            planExecutor
        );
    }
}
