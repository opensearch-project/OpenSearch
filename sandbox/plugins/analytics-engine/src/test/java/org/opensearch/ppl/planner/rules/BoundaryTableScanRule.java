/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.ppl.planner.rel.OpenSearchBoundaryTableScan;

/**
 * ConverterRule: LogicalTableScan (Convention.NONE) → OpenSearchBoundaryTableScan (OPENSEARCH).
 *
 * <p>Converts a {@link LogicalTableScan} into an {@link OpenSearchBoundaryTableScan} with the
 * scan itself as the initial logical fragment. The boundary node carries an {@link QueryPlanExecutor}
 * so it can delegate execution at {@code bind()} time.
 */
public class BoundaryTableScanRule extends ConverterRule {

    private final QueryPlanExecutor queryPlanExecutor;

    /**
     * Create a rule instance that converts LogicalTableScan to OpenSearchBoundaryTableScan.
     *
     * @param QueryPlanExecutor the engine executor passed to the boundary node for bind-time execution
     * @return a new BoundaryTableScanRule
     */
    public static BoundaryTableScanRule create(QueryPlanExecutor QueryPlanExecutor) {
        return new BoundaryTableScanRule(
            Config.INSTANCE.withConversion(LogicalTableScan.class, Convention.NONE, EnumerableConvention.INSTANCE, "BoundaryTableScanRule"),
            QueryPlanExecutor
        );
    }

    private BoundaryTableScanRule(Config config, QueryPlanExecutor queryPlanExecutor) {
        super(config);
        this.queryPlanExecutor = queryPlanExecutor;
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalTableScan scan = (LogicalTableScan) rel;
        return new OpenSearchBoundaryTableScan(
            scan.getCluster(),
            scan.getTraitSet().replace(EnumerableConvention.INSTANCE),
            scan.getTable(),
            scan,
            queryPlanExecutor
        );
    }
}
