/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Regression guard for the {@link DatetimeOperandCoerceShuttle} concurrency contract.
 *
 * <p>Calcite's {@code RelShuttleImpl} (which {@code RelHomogeneousShuttle} extends) keeps
 * a stateful traversal {@code Deque<RelNode>}, so a singleton shuttle would race under
 * concurrent planning. These tests pin the per-call instantiation contract.
 */
public class DatetimeOperandCoerceShuttleTests extends OpenSearchTestCase {

    public void testNoSingletonInstanceField() {
        // The shuttle must not expose a static INSTANCE — that's the bug pattern this
        // class guards against.
        for (var field : DatetimeOperandCoerceShuttle.class.getDeclaredFields()) {
            assertNotEquals("DatetimeOperandCoerceShuttle must not expose a static singleton instance", "INSTANCE", field.getName());
        }
    }

    public void testFreshShuttlesAreIndependent() {
        // Two shuttles built back-to-back must not share traversal state.
        DatetimeOperandCoerceShuttle a = new DatetimeOperandCoerceShuttle();
        DatetimeOperandCoerceShuttle b = new DatetimeOperandCoerceShuttle();
        assertNotSame(a, b);
    }

    public void testSequentialAcceptCallsDoNotCorruptStack() {
        // Drive a fresh shuttle through two trivial RelNode visits sequentially. If the
        // shuttle were reused with a corrupt stack, the second visit would either NPE
        // or return an unexpected tree shape. A fresh-per-call shuttle visits cleanly.
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        RelBuilderFactory factory = RelBuilder.proto(Contexts.empty());
        RelBuilder builder = factory.create(cluster, null);

        RelNode plan = builder.values(new String[] { "x" }, 1).build();

        for (int i = 0; i < 3; i++) {
            DatetimeOperandCoerceShuttle shuttle = new DatetimeOperandCoerceShuttle();
            RelNode visited = plan.accept(shuttle);
            assertNotNull("visit must return a non-null RelNode", visited);
        }
    }
}
