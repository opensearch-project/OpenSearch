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

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AllocationConstraintsTests extends OpenSearchAllocationTestCase {

    public void testSettings() {
        Settings.Builder settings = Settings.builder();
        ClusterSettings service = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(settings.build(), service);

        settings = Settings.builder();
        float indexBalanceFactor = randomFloat();
        float shardBalance = randomFloat();
        float threshold = randomFloat();
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), indexBalanceFactor);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), shardBalance);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), threshold);

        service.applySettings(settings.build());

        assertEquals(indexBalanceFactor, allocator.getIndexBalance(), 0.01);
        assertEquals(shardBalance, allocator.getShardBalance(), 0.01);
        assertEquals(threshold, allocator.getThreshold(), 0.01);

    }

    /**
     * Test constraint evaluation logic when with different values of ConstraintMode
     * for IndexShardPerNode constraint satisfied and breached.
     */
    public void testIndexShardsPerNodeConstraint() {
        BalancedShardsAllocator.Balancer balancer = mock(BalancedShardsAllocator.Balancer.class);
        BalancedShardsAllocator.ModelNode node = mock(BalancedShardsAllocator.ModelNode.class);
        AllocationConstraints constraints = new AllocationConstraints();

        int shardCount = randomIntBetween(1, 500);
        float avgShardsPerNode = 1.0f + (random().nextFloat()) * 999.0f;

        when(balancer.avgShardsPerNode(anyString())).thenReturn(avgShardsPerNode);
        when(node.numShards(anyString())).thenReturn(shardCount);
        when(node.getNodeId()).thenReturn("test-node");

        long expectedWeight = (shardCount >= avgShardsPerNode) ? constraints.CONSTRAINT_WEIGHT : 0;
        assertEquals(expectedWeight, constraints.weight(balancer, node, "index"));

    }

}
