/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.engine;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.opensearch.analytics.plan.registry.BackendCapabilityRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

/**
 * Property-based tests for {@link BackendCapabilityRegistry}.
 */
public class BackendCapabilityRegistryTests extends OpenSearchTestCase {

    private static final List<Class<? extends RelNode>> ALL_OPS = List.of(
        LogicalTableScan.class, LogicalFilter.class, LogicalAggregate.class, LogicalProject.class
    );

    /**
     * // Feature: analytics-query-planner, Property 11: Registry registration round-trip
     *
     * For any backend name and set of supported operator classes, after register(),
     * backendsForOperator(opClass) SHALL return a list containing that backend name.
     */
    public void testRegistrationRoundTrip() {
        for (int i = 0; i < 100; i++) {
            BackendCapabilityRegistry registry = new BackendCapabilityRegistry();
            String name = "backend-" + randomAlphaOfLength(6);

            // pick a random non-empty subset of operators
            int opCount = randomIntBetween(1, ALL_OPS.size());
            Set<Class<? extends RelNode>> ops = new java.util.HashSet<>();
            List<Class<? extends RelNode>> shuffled = new java.util.ArrayList<>(ALL_OPS);
            java.util.Collections.shuffle(shuffled, random());
            for (int j = 0; j < opCount; j++) ops.add(shuffled.get(j));

            Set<String> fns = Set.of("COUNT", "SUM");
            registry.register(name, ops, fns);

            for (Class<? extends RelNode> op : ops) {
                assertTrue("backendsForOperator must contain registered backend for " + op.getSimpleName(),
                    registry.backendsForOperator(op).contains(name));
            }
        }
    }

    /**
     * // Feature: analytics-query-planner, Property 12: Registry deregistration removes all entries
     *
     * After deregister(), backendsForOperator() SHALL NOT return the deregistered backend.
     */
    public void testDeregistrationRemovesEntries() {
        for (int i = 0; i < 100; i++) {
            BackendCapabilityRegistry registry = new BackendCapabilityRegistry();
            String name = "backend-" + randomAlphaOfLength(6);
            Set<Class<? extends RelNode>> ops = Set.of(LogicalTableScan.class, LogicalFilter.class);
            registry.register(name, ops, Set.of("COUNT"));

            registry.deregister(name);

            for (Class<? extends RelNode> op : ops) {
                assertFalse("deregistered backend must not appear in backendsForOperator",
                    registry.backendsForOperator(op).contains(name));
            }
            assertFalse("deregistered backend must not appear in backendsForFunction",
                registry.backendsForFunction("COUNT").contains(name));
        }
    }
}
