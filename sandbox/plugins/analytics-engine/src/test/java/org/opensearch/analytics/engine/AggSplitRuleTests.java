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
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.plan.operators.AggMode;
import org.opensearch.analytics.plan.operators.OpenSearchAggregate;
import org.opensearch.analytics.plan.rules.AggSplitRule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Property-based tests for {@link AggSplitRule}.
 */
public class AggSplitRuleTests extends OpenSearchTestCase {

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

    private RelNode buildScan() {
        var table = mock(org.apache.calcite.plan.RelOptTable.class);
        var rowType = typeFactory.builder().add("id", SqlTypeName.BIGINT).build();
        when(table.getRowType()).thenReturn(rowType);
        when(table.getQualifiedName()).thenReturn(List.of("t"));
        return new LogicalTableScan(cluster, cluster.traitSet(), Collections.emptyList(), table);
    }

    private OpenSearchAggregate buildUnresolvedAgg(RelNode input, AggMode mode) {
        AggregateCall countStar = AggregateCall.create(
            SqlStdOperatorTable.COUNT, false, Collections.emptyList(), 0, input, null, "cnt");
        return new OpenSearchAggregate(cluster, cluster.traitSet(), input,
            ImmutableBitSet.of(), null, List.of(countStar), "unresolved", mode);
    }

    private RelNode applyRule(RelNode input) {
        HepProgram program = new HepProgramBuilder()
            .addMatchOrder(HepMatchOrder.BOTTOM_UP)
            .addRuleInstance(AggSplitRule.INSTANCE)
            .build();
        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(input);
        return planner.findBestExp();
    }

    /**
     * // Feature: analytics-query-planner, Property 7: AggSplit structural correctness
     *
     * For any OpenSearchAggregate with mode == UNRESOLVED, after AggSplitRule fires,
     * the result SHALL be a FINAL OpenSearchAggregate whose sole input is a PARTIAL
     * OpenSearchAggregate, both with backendTag == "unresolved".
     */
    public void testAggSplitStructure() {
        for (int i = 0; i < 100; i++) {
            RelNode scan = buildScan();
            OpenSearchAggregate unresolved = buildUnresolvedAgg(scan, AggMode.UNRESOLVED);
            RelNode result = applyRule(unresolved);

            assertInstanceOf(OpenSearchAggregate.class, result);
            OpenSearchAggregate finalAgg = (OpenSearchAggregate) result;
            assertEquals("FINAL mode expected", AggMode.FINAL, finalAgg.getMode());
            assertEquals("unresolved", finalAgg.getBackendTag());

            assertInstanceOf(OpenSearchAggregate.class, finalAgg.getInput());
            OpenSearchAggregate partialAgg = (OpenSearchAggregate) finalAgg.getInput();
            assertEquals("PARTIAL mode expected", AggMode.PARTIAL, partialAgg.getMode());
            assertEquals("unresolved", partialAgg.getBackendTag());
        }
    }

    /**
     * // Feature: analytics-query-planner, Property 8: AggSplitRule idempotence
     *
     * For any OpenSearchAggregate with mode == PARTIAL or FINAL, AggSplitRule SHALL NOT fire.
     */
    public void testAggSplitIdempotence() {
        for (int i = 0; i < 100; i++) {
            RelNode scan = buildScan();

            // PARTIAL — rule must not fire
            OpenSearchAggregate partial = buildUnresolvedAgg(scan, AggMode.PARTIAL);
            RelNode resultPartial = applyRule(partial);
            assertInstanceOf(OpenSearchAggregate.class, resultPartial);
            assertEquals("PARTIAL must stay PARTIAL", AggMode.PARTIAL,
                ((OpenSearchAggregate) resultPartial).getMode());

            // FINAL — rule must not fire
            OpenSearchAggregate finalAgg = buildUnresolvedAgg(scan, AggMode.FINAL);
            RelNode resultFinal = applyRule(finalAgg);
            assertInstanceOf(OpenSearchAggregate.class, resultFinal);
            assertEquals("FINAL must stay FINAL", AggMode.FINAL,
                ((OpenSearchAggregate) resultFinal).getMode());
        }
    }

    private static void assertInstanceOf(Class<?> type, Object obj) {
        assertTrue("Expected " + type.getSimpleName() + " but got " + obj.getClass().getSimpleName(),
            type.isInstance(obj));
    }
}
