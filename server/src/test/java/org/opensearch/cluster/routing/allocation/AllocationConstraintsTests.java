/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AllocationConstraintsTests extends OpenSearchAllocationTestCase {

    public void testSettings() {
        Settings.Builder settings = Settings.builder();
        ClusterSettings service = new ClusterSettings(Settings.builder().build(),
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(settings.build(), service);

        // Changing other balancer settings should not affect constraint settings
        settings = Settings.builder();
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.2);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.3);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 2.0);
        service.applySettings(settings.build());
        assertEquals(0.2, allocator.getIndexBalance(), 0.01);
        assertEquals(0.3, allocator.getShardBalance(), 0.01);
        assertEquals(2.0, allocator.getThreshold(), 0.01);

    }

    /**
     * Test constraint evaluation logic when with different values of ConstraintMode
     * for IndexShardPerNode constraint satisfied and breached.
     */
    public void testIndexShardsPerNodeConstraint() {
        BalancedShardsAllocator.Balancer balancer = mock(BalancedShardsAllocator.Balancer.class);
        BalancedShardsAllocator.ModelNode node = mock(BalancedShardsAllocator.ModelNode.class);
        when(balancer.avgShardsPerNode(anyString())).thenReturn(3.33f);
        when(node.getNodeId()).thenReturn("test-node");

        /*
         * Constraint breached
         */

        when(node.numShards(anyString())).thenReturn(4);

        AllocationConstraints constraints = new AllocationConstraints();
        assertEquals(constraints.CONSTRAINT_WEIGHT, constraints.weight(balancer, node, "index"));

        /*
         * Constraint not breached
         */

        when(node.numShards(anyString())).thenReturn(3);

        constraints = new AllocationConstraints();
        assertEquals(0, constraints.weight(balancer, node, "index"));

    }

}
