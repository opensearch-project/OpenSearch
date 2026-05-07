/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Optional;

/**
 * Tests for {@link MultiInputShape} — verifies detection of the N-input coordinator
 * shape where all children are {@link OpenSearchExchangeReducer}s each wrapping an
 * {@link OpenSearchStageInputScan}.
 */
public class MultiInputShapeTests extends OpenSearchTestCase {

    private RelOptCluster cluster;
    private RelDataType rowType;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        rowType = typeFactory.builder().add("a", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();
    }

    /** Two ER children each wrapping a StageInputScan — detected, returns the two scans in order. */
    public void testTwoExchangeReducersWrappingStageInputScans() {
        OpenSearchStageInputScan leftScan = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 1, rowType, List.of("df"));
        OpenSearchStageInputScan rightScan = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 2, rowType, List.of("df"));
        OpenSearchExchangeReducer leftReducer = new OpenSearchExchangeReducer(cluster, cluster.traitSet(), leftScan, List.of("df"));
        OpenSearchExchangeReducer rightReducer = new OpenSearchExchangeReducer(cluster, cluster.traitSet(), rightScan, List.of("df"));
        RelNode multiInput = new StubMultiInput(cluster, List.of(leftReducer, rightReducer));

        Optional<MultiInputShape> result = MultiInputShape.detect(multiInput);

        assertTrue("expected MultiInputShape for two ER-wrapped StageInputScans", result.isPresent());
        List<OpenSearchStageInputScan> scans = result.get().getBranchScans();
        assertEquals(2, scans.size());
        assertSame(leftScan, scans.get(0));
        assertSame(rightScan, scans.get(1));
    }

    /** Single-input node — not a multi-input shape, returns empty. */
    public void testSingleInputReturnsEmpty() {
        OpenSearchStageInputScan scan = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 1, rowType, List.of("df"));
        OpenSearchExchangeReducer reducer = new OpenSearchExchangeReducer(cluster, cluster.traitSet(), scan, List.of("df"));
        RelNode singleInput = new StubMultiInput(cluster, List.of(reducer));

        Optional<MultiInputShape> result = MultiInputShape.detect(singleInput);

        assertTrue("single-input shape must not be treated as multi-input", result.isEmpty());
    }

    /** Mixed children (one ER, one non-ER leaf) — not a multi-input shape, returns empty. */
    public void testMixedChildrenReturnsEmpty() {
        OpenSearchStageInputScan scan = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 1, rowType, List.of("df"));
        OpenSearchExchangeReducer reducer = new OpenSearchExchangeReducer(cluster, cluster.traitSet(), scan, List.of("df"));
        OpenSearchStageInputScan bareScan = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 2, rowType, List.of("df"));
        RelNode mixed = new StubMultiInput(cluster, List.of(reducer, bareScan));

        Optional<MultiInputShape> result = MultiInputShape.detect(mixed);

        assertTrue("mixed-child shape must not be treated as multi-input", result.isEmpty());
    }

    /** Minimal multi-input stub — Calcite RelNode with N inputs and no side effects. */
    private static final class StubMultiInput extends AbstractRelNode {
        private final List<RelNode> inputs;

        StubMultiInput(RelOptCluster cluster, List<RelNode> inputs) {
            super(cluster, cluster.traitSet());
            this.inputs = List.copyOf(inputs);
        }

        @Override
        public List<RelNode> getInputs() {
            return inputs;
        }
    }
}
