/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.planner;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.opensearch.analytics.backend.EngineCapabilities;
import org.opensearch.fe.planner.rel.OpenSearchBoundaryTableScan;
import org.opensearch.fe.planner.rules.AbsorbAggregateRule;
import org.opensearch.fe.planner.rules.AbsorbFilterRule;
import org.opensearch.fe.planner.rules.AbsorbProjectRule;
import org.opensearch.fe.planner.rules.AbsorbSortRule;

/**
 * Produces a mixed plan where supported operators are absorbed into an
 * {@link OpenSearchBoundaryTableScan} and unsupported operators remain as
 * Calcite logical nodes.
 *
 * <p><b>Phase 1 (BoundaryTableScanShuttle):</b> Replaces every
 * {@code LogicalTableScan} with an {@code OpenSearchBoundaryTableScan}
 * carrying the scan as its initial logical fragment.
 *
 * <p><b>Phase 2 (HepPlanner):</b> Runs {@link AbsorbFilterRule} and
 * {@link AbsorbProjectRule} to absorb supported operators into the boundary
 * node's logical fragment. Unsupported operators (e.g., projects containing
 * functions not in {@link EngineCapabilities}) remain above the boundary node
 * and execute in-process via Janino bytecode.
 */
public class PushDownPlanner {

    private final EngineCapabilities capabilities;
    private final PlanExecutor planExecutor;

    /**
     * @param capabilities engine capabilities used to gate which operators are pushed down
     * @param planExecutor engine executor passed to boundary nodes for bind-time execution
     */
    public PushDownPlanner(EngineCapabilities capabilities, PlanExecutor planExecutor) {
        this.capabilities = capabilities;
        this.planExecutor = planExecutor;
    }

    /**
     * Optimizes the input RelNode by pushing supported operators into a boundary node.
     *
     * <ol>
     *   <li>Phase 1: Replace LogicalTableScan → OpenSearchBoundaryTableScan</li>
     *   <li>Phase 2: HepPlanner absorbs supported filter/project into boundary node</li>
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
        programBuilder.addRuleInstance(AbsorbFilterRule.create(capabilities));
        programBuilder.addRuleInstance(AbsorbProjectRule.create(capabilities));
        programBuilder.addRuleInstance(AbsorbAggregateRule.create(capabilities));
        programBuilder.addRuleInstance(AbsorbSortRule.create(capabilities));

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
        private final PlanExecutor planExecutor;

        BoundaryTableScanShuttle(PlanExecutor planExecutor) {
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
