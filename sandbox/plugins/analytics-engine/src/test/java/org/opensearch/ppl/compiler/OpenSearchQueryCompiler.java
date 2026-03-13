/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.compiler;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelRunner;
import org.opensearch.ppl.planner.rel.OpenSearchBoundaryTableScan;
import org.opensearch.sql.api.UnifiedQueryContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * Compiles Calcite {@link RelNode} plans into executable {@link PreparedStatement}s.
 *
 * <p>Rebuilds the plan tree in a fresh {@link RelOptCluster} with
 * {@link Convention#NONE} traits before calling {@code prepareStatement()}.
 * This is necessary because the plan from {@code PushDownPlanner} uses a
 * planner that already has nodes registered, and re-registering causes
 * assertions in Calcite's Volcano planner.
 */
public class OpenSearchQueryCompiler {

    private final UnifiedQueryContext context;

    public OpenSearchQueryCompiler(UnifiedQueryContext context) {
        this.context = context;
    }

    /**
     * Compiles a plan into an executable {@link PreparedStatement}.
     */
    public PreparedStatement compile(RelNode plan) {
        if (plan == null) {
            throw new IllegalArgumentException("RelNode plan must not be null");
        }
        try {
            RelNode detached = detachFromPlanner(plan);
            Connection connection = context.getPlanContext().connection;
            RelRunner runner = connection.unwrap(RelRunner.class);
            return runner.prepareStatement(detached);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to compile logical plan", e);
        }
    }

    /**
     * Rebuilds the plan tree in a fresh {@link RelOptCluster} with
     * {@link Convention#NONE} traits and a fully-configured {@link VolcanoPlanner}.
     */
    private static RelNode detachFromPlanner(RelNode root) {
        VolcanoPlanner freshPlanner = new VolcanoPlanner();
        freshPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        freshPlanner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        RelOptUtil.registerDefaultRules(freshPlanner, false, false);
        freshPlanner.addRule(BoundaryToEnumerableRule.INSTANCE);

        RexBuilder rexBuilder = root.getCluster().getRexBuilder();
        RelOptCluster freshCluster = RelOptCluster.create(freshPlanner, rexBuilder);
        freshCluster.setMetadataProvider(root.getCluster().getMetadataProvider());
        freshCluster.setMetadataQuerySupplier(root.getCluster().getMetadataQuerySupplier());

        return rebuild(root, freshCluster);
    }

    /**
     * Recursively rebuilds a RelNode tree in a fresh cluster with
     * {@link Convention#NONE} traits. Uses {@code copy()} for generic
     * handling of all RelNode types instead of per-type factory methods.
     */
    private static RelNode rebuild(RelNode node, RelOptCluster freshCluster) {
        // Leaf: OpenSearchBoundaryTableScan — rebuild with NONE convention
        if (node instanceof OpenSearchBoundaryTableScan) {
            OpenSearchBoundaryTableScan boundary = (OpenSearchBoundaryTableScan) node;
            RelTraitSet noneTraits = freshCluster.traitSetOf(Convention.NONE);
            return new OpenSearchBoundaryTableScan(
                freshCluster,
                noneTraits,
                boundary.getTable(),
                boundary.getLogicalFragment(),
                boundary.getEngineExecutor()
            );
        }

        // Leaf: LogicalTableScan → BindableTableScan when possible
        if (node instanceof LogicalTableScan) {
            RelOptTable table = node.getTable();
            if (Bindables.BindableTableScan.canHandle(table)) {
                return Bindables.BindableTableScan.create(freshCluster, table);
            }
            return LogicalTableScan.create(freshCluster, table, List.of());
        }

        // Non-leaf: rebuild children, then reconstruct node using factory methods
        // Factory methods derive cluster from inputs, avoiding "belongs to a different planner" errors
        List<RelNode> inputs = node.getInputs();
        if (inputs.isEmpty()) {
            return node.copy(node.getTraitSet().replace(Convention.NONE), inputs);
        }

        List<RelNode> newInputs = new ArrayList<>(inputs.size());
        for (RelNode input : inputs) {
            newInputs.add(rebuild(input, freshCluster));
        }

        if (node instanceof LogicalFilter) {
            return LogicalFilter.create(newInputs.get(0), ((LogicalFilter) node).getCondition());
        }
        if (node instanceof LogicalProject) {
            LogicalProject p = (LogicalProject) node;
            return LogicalProject.create(newInputs.get(0), p.getHints(), p.getProjects(), p.getRowType());
        }
        if (node instanceof LogicalAggregate) {
            LogicalAggregate a = (LogicalAggregate) node;
            return LogicalAggregate.create(newInputs.get(0), a.getHints(), a.getGroupSet(), a.getGroupSets(), a.getAggCallList());
        }
        if (node instanceof LogicalSort) {
            LogicalSort s = (LogicalSort) node;
            return LogicalSort.create(newInputs.get(0), s.getCollation(), s.offset, s.fetch);
        }
        return node.copy(node.getTraitSet().replace(Convention.NONE), newInputs);
    }

    /**
     * Converter rule: {@link OpenSearchBoundaryTableScan} from
     * {@link Convention#NONE} to {@link EnumerableConvention}.
     */
    private static class BoundaryToEnumerableRule extends ConverterRule {

        static final Config DEFAULT_CONFIG = Config.INSTANCE.withConversion(
            OpenSearchBoundaryTableScan.class,
            Convention.NONE,
            EnumerableConvention.INSTANCE,
            "BoundaryToEnumerableRule"
        ).withRuleFactory(BoundaryToEnumerableRule::new);

        static final BoundaryToEnumerableRule INSTANCE = new BoundaryToEnumerableRule(DEFAULT_CONFIG);

        protected BoundaryToEnumerableRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            OpenSearchBoundaryTableScan scan = (OpenSearchBoundaryTableScan) rel;
            RelTraitSet newTraits = scan.getTraitSet().replace(EnumerableConvention.INSTANCE);
            return new OpenSearchBoundaryTableScan(
                scan.getCluster(),
                newTraits,
                scan.getTable(),
                scan.getLogicalFragment(),
                scan.getEngineExecutor()
            );
        }
    }
}
