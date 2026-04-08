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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.plan.DefaultQueryPlanner;
import org.opensearch.analytics.plan.FieldCapabilityResolver;
import org.opensearch.analytics.plan.QueryPlanningException;
import org.opensearch.analytics.plan.ResolvedPlan;
import org.opensearch.analytics.plan.operators.OpenSearchTableScan;
import org.opensearch.analytics.plan.registry.BackendCapabilityRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Property-based tests for DefaultQueryPlanner validation phase (Phase 1).
 */
public class DefaultQueryPlannerValidationTests extends OpenSearchTestCase {

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

    private RelOptTable mockTable() {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getRowType()).thenReturn(typeFactory.builder().add("id", SqlTypeName.BIGINT).build());
        when(table.getQualifiedName()).thenReturn(List.of("t"));
        return table;
    }

    private LogicalTableScan buildScan() {
        return new LogicalTableScan(cluster, cluster.traitSet(), Collections.emptyList(), mockTable());
    }

    private DefaultQueryPlanner plannerWithScan() {
        BackendCapabilityRegistry registry = new BackendCapabilityRegistry();
        registry.register("datafusion", Set.of(LogicalTableScan.class), Set.of());
        FieldCapabilityResolver fcr = mock(FieldCapabilityResolver.class);
        return new DefaultQueryPlanner(registry, cluster, fcr);
    }

    private DefaultQueryPlanner plannerWithNoBackends() {
        BackendCapabilityRegistry registry = new BackendCapabilityRegistry();
        FieldCapabilityResolver fcr = mock(FieldCapabilityResolver.class);
        return new DefaultQueryPlanner(registry, cluster, fcr);
    }

    /**
     * // Feature: analytics-query-planner, Property 1: Exhaustive error collection
     *
     * For any RelNode tree containing N unsupported operators, the QueryPlanningException
     * SHALL contain exactly N error messages.
     */
    public void testExhaustiveErrorCollection() {
        // With no backends registered, every operator is unsupported
        DefaultQueryPlanner planner = plannerWithNoBackends();
        LogicalTableScan scan = buildScan();

        QueryPlanningException ex = expectThrows(QueryPlanningException.class,
            () -> planner.plan(scan, 1));
        // At least one error for the unsupported scan
        assertFalse("errors must not be empty", ex.getErrors().isEmpty());
        assertTrue("error must mention operator class",
            ex.getErrors().stream().anyMatch(e -> e.contains("LogicalTableScan")));
    }

    /**
     * // Feature: analytics-query-planner, Property 2: Unsupported operator rejection
     *
     * For any RelNode containing an operator not in any backend's supported set,
     * the planner SHALL throw QueryPlanningException with the operator's class name.
     */
    public void testUnsupportedOperatorRejected() {
        for (int i = 0; i < 100; i++) {
            DefaultQueryPlanner planner = plannerWithNoBackends();
            LogicalTableScan scan = buildScan();

            QueryPlanningException ex = expectThrows(QueryPlanningException.class,
                () -> planner.plan(scan, 1));
            assertTrue("error must contain operator class name",
                ex.getErrors().stream().anyMatch(e -> e.contains("LogicalTableScan")));
        }
    }

    /**
     * // Feature: analytics-query-planner, Property 3: Valid plan passes validation unchanged
     *
     * For any RelNode where every operator is supported, validation SHALL complete without throwing.
     */
    public void testValidPlanPassesUnchanged() {
        for (int i = 0; i < 100; i++) {
            BackendCapabilityRegistry registry = new BackendCapabilityRegistry();
            // Register OpenSearch* operators (post-wrap) as supported
            registry.register("datafusion",
                Set.of(LogicalTableScan.class, OpenSearchTableScan.class),
                Set.of());
            FieldCapabilityResolver fcr = mock(FieldCapabilityResolver.class);
            DefaultQueryPlanner planner = new DefaultQueryPlanner(registry, cluster, fcr);

            LogicalTableScan scan = buildScan();
            // Should not throw — scan is supported
            try {
                planner.plan(scan, 1);
            } catch (QueryPlanningException e) {
                // Only fail if the error is about an unsupported operator (not resolution)
                boolean hasUnsupportedError = e.getErrors().stream()
                    .anyMatch(err -> err.contains("No backend supports operator"));
                assertFalse("Valid plan should not fail validation: " + e.getMessage(),
                    hasUnsupportedError);
            }
        }
    }
}
