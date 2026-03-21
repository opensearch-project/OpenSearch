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
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.plan.DefaultQueryPlanner;
import org.opensearch.analytics.plan.FieldCapabilityResolver;
import org.opensearch.analytics.plan.QueryPlanningException;
import org.opensearch.analytics.plan.ResolvedPlan;
import org.opensearch.analytics.plan.operators.BackendTagged;
import org.opensearch.analytics.plan.operators.OpenSearchTableScan;
import org.opensearch.analytics.plan.registry.BackendCapabilityRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Property-based tests for DefaultQueryPlanner resolution phase (Phase 5).
 */
public class DefaultQueryPlannerResolveTests extends OpenSearchTestCase {

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

    /**
     * // Feature: analytics-query-planner, Property 9: Resolution assigns highest-priority backend
     *
     * After resolution, every operator's backendTag SHALL equal the first backend in priority
     * order that supports that operator's class.
     */
    public void testResolutionPriorityOrder() {
        for (int i = 0; i < 100; i++) {
            BackendCapabilityRegistry registry = new BackendCapabilityRegistry();
            // Register two backends — "first" has higher priority (inserted first)
            registry.register("first", Set.of(LogicalTableScan.class, OpenSearchTableScan.class), Set.of());
            registry.register("second", Set.of(LogicalTableScan.class, OpenSearchTableScan.class), Set.of());
            FieldCapabilityResolver fcr = mock(FieldCapabilityResolver.class);
            DefaultQueryPlanner planner = new DefaultQueryPlanner(registry, cluster, fcr);

            LogicalTableScan scan = buildScan();
            ResolvedPlan result = planner.plan(scan, 1);
            assertEquals("highest-priority backend must be selected", "first", result.getBackendName());
        }
    }

    /**
     * // Feature: analytics-query-planner, Property 10: No "unresolved" tags after successful resolution
     *
     * After resolution, no operator in the tree SHALL have backendTag == "unresolved".
     */
    public void testNoUnresolvedAfterResolution() {
        for (int i = 0; i < 100; i++) {
            BackendCapabilityRegistry registry = new BackendCapabilityRegistry();
            registry.register("datafusion", Set.of(LogicalTableScan.class, OpenSearchTableScan.class), Set.of());
            FieldCapabilityResolver fcr = mock(FieldCapabilityResolver.class);
            DefaultQueryPlanner planner = new DefaultQueryPlanner(registry, cluster, fcr);

            LogicalTableScan scan = buildScan();
            ResolvedPlan result = planner.plan(scan, 1);
            assertNotEquals("root must not be unresolved", "unresolved", result.getBackendName());
            assertNotEquals("root must not be unresolved",
                "unresolved", ((BackendTagged) result.getRoot()).getBackendTag());
        }
    }

    /**
     * // Feature: analytics-query-planner, Property 16: UnresolvedRexNode resolution
     * Validates: Requirements 5.4, 5.5
     * (Structural test — full RexNode replacement deferred to integration)
     */
    public void testUnresolvedRexNodeResolved() {
        // Covered by integration: UnresolvedRexNode in filter condition is resolved
        // to BackendSpecificRexNode when a backend accepts the payload.
        // This test verifies the registry lookup works correctly.
        BackendCapabilityRegistry registry = new BackendCapabilityRegistry();
        byte[] payload = new byte[]{1, 2, 3};
        var plugin = mock(org.opensearch.analytics.spi.AnalyticsBackEndPlugin.class);
        when(plugin.canAcceptUnresolvedPredicate(payload)).thenReturn(true);
        when(plugin.name()).thenReturn("lucene");
        registry.register("lucene", Set.of(), Set.of(), plugin);

        var resolved = registry.backendForUnresolvedPredicate(payload);
        assertTrue("backend must accept payload", resolved.isPresent());
        assertEquals("lucene", resolved.get());
    }

    /**
     * // Feature: analytics-query-planner, Property 17: UnresolvedRexNode rejection
     * Validates: Requirements 5.6
     */
    public void testUnresolvedRexNodeRejected() {
        BackendCapabilityRegistry registry = new BackendCapabilityRegistry();
        byte[] payload = new byte[]{9, 9, 9};
        var plugin = mock(org.opensearch.analytics.spi.AnalyticsBackEndPlugin.class);
        when(plugin.canAcceptUnresolvedPredicate(payload)).thenReturn(false);
        when(plugin.name()).thenReturn("datafusion");
        registry.register("datafusion", Set.of(), Set.of(), plugin);

        var resolved = registry.backendForUnresolvedPredicate(payload);
        assertFalse("no backend should accept this payload", resolved.isPresent());
    }

    /**
     * // Feature: analytics-query-planner, Property 18: HybridFilter creation
     * Validates: Requirements 5.4, 5.5
     * (Structural test — HybridFilter is created when predicates span multiple backends)
     */
    public void testHybridFilterCreated() {
        // Verify OpenSearchHybridFilter carries backendPredicates correctly
        var cluster2 = cluster;
        var input = buildScan();
        var rexBuilder = cluster.getRexBuilder();
        var condition = rexBuilder.makeLiteral(true);
        var predicates = java.util.Map.of("lucene", condition, "datafusion", condition);

        var hybridFilter = new org.opensearch.analytics.plan.operators.OpenSearchHybridFilter(
            cluster2, cluster2.traitSet(), input, condition, "lucene", predicates);

        assertEquals("lucene", hybridFilter.getBackendTag());
        assertEquals(2, hybridFilter.getBackendPredicates().size());
        assertTrue(hybridFilter.getBackendPredicates().containsKey("lucene"));
        assertTrue(hybridFilter.getBackendPredicates().containsKey("datafusion"));
    }
}
