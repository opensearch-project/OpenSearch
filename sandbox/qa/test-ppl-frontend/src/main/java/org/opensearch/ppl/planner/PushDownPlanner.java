/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.planner;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.ppl.planner.rel.OpenSearchBoundaryTableScan;

/**
 * Produces a mixed plan where supported operators are absorbed into an
 * {@link OpenSearchBoundaryTableScan} and unsupported operators remain as
 * Calcite logical nodes.
 *
 * <p><b>Phase 1 (BoundaryTableScanShuttle):</b> Replaces every
 * {@code LogicalTableScan} with an {@code OpenSearchBoundaryTableScan}
 * carrying the scan as its initial logical fragment.
 *
 * <p><b>Phase 2 (HepPlanner):</b> Runs absorb rules to push supported
 * operators into the boundary node's logical fragment. Unsupported operators
 * (e.g., projects containing functions not in the back-end's
 * {@link SqlOperatorTable}) remain above the boundary node and execute
 * in-process via Janino bytecode.
 */
public class PushDownPlanner {

    private final SqlOperatorTable operatorTable;
    private final QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor;

    /**
     * @param operatorTable supported functions from the back-end engines
     * @param planExecutor  engine executor passed to boundary nodes for bind-time execution
     */
    public PushDownPlanner(SqlOperatorTable operatorTable, QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor) {
        this.operatorTable = operatorTable;
        this.planExecutor = planExecutor;
    }

    /**
     * Optimizes the input RelNode by pushing supported operators into a boundary node.
     *
     * <ol>
     *   <li>Phase 1: Replace LogicalTableScan → OpenSearchBoundaryTableScan</li>
     *   <li>Phase 2: HepPlanner absorbs supported filter/project/aggregate/sort into boundary node</li>
     * </ol>
     *
     * @param input the logical RelNode produced by PPLToRelNodeService
     * @return a mixed plan with boundary nodes carrying the OPENSEARCH convention
     */
    public RelNode plan(RelNode input) {
        // Phase 1: Replace scans with boundary nodes
        RelNode withBoundary = input.accept(new BoundaryTableScanShuttle(planExecutor));

        // Phase 2: Absorb supported operators into boundary nodes
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        HepPlanner hepPlanner = new HepPlanner(programBuilder.build());
        hepPlanner.setRoot(withBoundary);
        return hepPlanner.findBestExp();
    }

    /**
     * Shuttle that replaces every {@link LogicalTableScan} with an
     * {@link OpenSearchBoundaryTableScan} carrying the scan as its initial
     * logical fragment.
     */
    private static class BoundaryTableScanShuttle extends RelShuttleImpl {
        private final QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor;

        BoundaryTableScanShuttle(QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor) {
            this.planExecutor = planExecutor;
        }

        @Override
        public RelNode visit(TableScan scan) {
            if (scan instanceof LogicalTableScan) {
                RelTraitSet traitSet = scan.getCluster().traitSetOf(EnumerableConvention.INSTANCE);
                return new OpenSearchBoundaryTableScan(scan.getCluster(), traitSet, scan.getTable(), scan, planExecutor);
            }
            return scan;
        }
    }
}
