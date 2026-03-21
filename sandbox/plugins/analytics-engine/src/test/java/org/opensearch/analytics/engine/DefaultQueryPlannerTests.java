/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.engine;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.plan.DefaultQueryPlanner;
import org.opensearch.analytics.plan.FieldCapabilityResolver;
import org.opensearch.analytics.plan.ResolvedPlan;
import org.opensearch.analytics.plan.operators.AggMode;
import org.opensearch.analytics.plan.operators.OpenSearchAggregate;
import org.opensearch.analytics.plan.operators.OpenSearchTableScan;
import org.opensearch.analytics.plan.registry.BackendCapabilityRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for DefaultQueryPlanner — single-shard AggSplit skip (P15).
 */
public class DefaultQueryPlannerTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    private LogicalTableScan buildScan() {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getRowType()).thenReturn(typeFactory.builder().add("id", SqlTypeName.BIGINT).build());
        when(table.getQualifiedName()).thenReturn(List.of("t"));
        return new LogicalTableScan(cluster, cluster.traitSet(), Collections.emptyList(), table);
    }

    private LogicalAggregate buildAggregate(LogicalTableScan scan) {
        AggregateCall countStar = AggregateCall.create(
            SqlStdOperatorTable.COUNT, false, Collections.emptyList(), 0, scan, null, "cnt");
        return LogicalAggregate.create(scan, Collections.emptyList(),
            ImmutableBitSet.of(), null, List.of(countStar));
    }

    /**
     * // Feature: analytics-query-planner, Property 15: AggSplitRule skipped for single-shard index
     *
     * When shardCount == 1, every OpenSearchAggregate in the tree SHALL retain mode == UNRESOLVED.
     */
    public void testAggSplitSkippedForSingleShard() {
        for (int i = 0; i < 100; i++) {
            BackendCapabilityRegistry registry = new BackendCapabilityRegistry();
            registry.register("datafusion",
                Set.of(LogicalTableScan.class, LogicalAggregate.class,
                    OpenSearchTableScan.class, OpenSearchAggregate.class),
                Set.of());
            FieldCapabilityResolver fcr = mock(FieldCapabilityResolver.class);
            DefaultQueryPlanner planner = new DefaultQueryPlanner(registry, cluster, fcr);

            LogicalTableScan scan = buildScan();
            LogicalAggregate agg = buildAggregate(scan);

            ResolvedPlan result = planner.plan(agg, 1); // shardCount == 1

            // Walk the result tree and verify no PARTIAL or FINAL aggregates exist
            assertNoSplitAggregates(result.getRoot());
        }
    }

    private void assertNoSplitAggregates(org.apache.calcite.rel.RelNode node) {
        if (node instanceof OpenSearchAggregate) {
            AggMode mode = ((OpenSearchAggregate) node).getMode();
            assertNotEquals("AggSplit must not fire for single-shard: found PARTIAL", AggMode.PARTIAL, mode);
            assertNotEquals("AggSplit must not fire for single-shard: found FINAL", AggMode.FINAL, mode);
        }
        for (org.apache.calcite.rel.RelNode input : node.getInputs()) {
            assertNoSplitAggregates(input);
        }
    }
}
