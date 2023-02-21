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
import org.opensearch.cluster.routing.allocation.allocator.LocalShardsBalancer;
import org.opensearch.cluster.routing.allocation.allocator.ShardsBalancer;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.cluster.routing.allocation.AllocationConstraints.INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID;
import static org.opensearch.cluster.routing.allocation.AllocationConstraints.INDEX_SHARD_PER_NODE_BREACH_WEIGHT;
import static org.opensearch.cluster.routing.allocation.RebalanceConstraints.PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_ID;
import static org.opensearch.cluster.routing.allocation.RebalanceConstraints.PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_WEIGHT;

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
        settings.put(BalancedShardsAllocator.PREFER_PRIMARY_SHARD_BALANCE.getKey(), true);

        service.applySettings(settings.build());

        assertEquals(indexBalanceFactor, allocator.getIndexBalance(), 0.01);
        assertEquals(shardBalance, allocator.getShardBalance(), 0.01);
        assertEquals(threshold, allocator.getThreshold(), 0.01);
        assertEquals(true, allocator.getPreferPrimaryBalance());

        settings.put(BalancedShardsAllocator.PREFER_PRIMARY_SHARD_BALANCE.getKey(), false);
        service.applySettings(settings.build());
        assertEquals(false, allocator.getPreferPrimaryBalance());
    }

    /**
     * Test constraint evaluation logic when with different values of ConstraintMode
     * for IndexShardPerNode constraint satisfied and breached.
     */
    public void testIndexShardsPerNodeConstraint() {
        ShardsBalancer balancer = mock(LocalShardsBalancer.class);
        BalancedShardsAllocator.ModelNode node = mock(BalancedShardsAllocator.ModelNode.class);
        AllocationConstraints constraints = new AllocationConstraints();
        constraints.updateAllocationConstraint(INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID, true);

        int shardCount = randomIntBetween(1, 500);
        float avgShardsPerNode = 1.0f + (random().nextFloat()) * 999.0f;

        when(balancer.avgShardsPerNode(anyString())).thenReturn(avgShardsPerNode);
        when(node.numShards(anyString())).thenReturn(shardCount);
        when(node.getNodeId()).thenReturn("test-node");

        long expectedWeight = (shardCount >= avgShardsPerNode) ? INDEX_SHARD_PER_NODE_BREACH_WEIGHT : 0;
        assertEquals(expectedWeight, constraints.weight(balancer, node, "index"));

    }

    /**
     * Test constraint evaluation logic when with different values of ConstraintMode
     * for IndexShardPerNode constraint satisfied and breached.
     */
    public void testIndexPrimaryShardsPerNodeConstraint() {
        ShardsBalancer balancer = mock(LocalShardsBalancer.class);
        BalancedShardsAllocator.ModelNode node = mock(BalancedShardsAllocator.ModelNode.class);
        AllocationConstraints constraints = new AllocationConstraints();
        constraints.updateAllocationConstraint(PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_ID, true);

        int primaryShardCount = 1;
        float avgPrimaryShardsPerNode = 2f;

        when(balancer.avgPrimaryShardsPerNode(anyString())).thenReturn(avgPrimaryShardsPerNode);
        when(node.numPrimaryShards(anyString())).thenReturn(primaryShardCount);
        when(node.getNodeId()).thenReturn("test-node");

        assertEquals(0, constraints.weight(balancer, node, "index"));

        primaryShardCount = 3;
        when(node.numPrimaryShards(anyString())).thenReturn(primaryShardCount);
        assertEquals(PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_WEIGHT, constraints.weight(balancer, node, "index"));

        constraints.updateAllocationConstraint(PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_ID, false);
        assertEquals(0, constraints.weight(balancer, node, "index"));
    }

    /**
     * Test constraint evaluation logic when with different values of ConstraintMode
     * for IndexShardPerNode constraint satisfied and breached.
     */
    public void testAllConstraint() {
        ShardsBalancer balancer = mock(LocalShardsBalancer.class);
        BalancedShardsAllocator.ModelNode node = mock(BalancedShardsAllocator.ModelNode.class);
        AllocationConstraints constraints = new AllocationConstraints();
        constraints.updateAllocationConstraint(INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID, true);
        constraints.updateAllocationConstraint(PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_ID, true);

        int shardCount = randomIntBetween(1, 500);
        int primaryShardCount = randomIntBetween(1, shardCount);
        float avgShardsPerNode = 1.0f + (random().nextFloat()) * 999.0f;
        float avgPrimaryShardsPerNode = (random().nextFloat()) * avgShardsPerNode;

        when(balancer.avgPrimaryShardsPerNode(anyString())).thenReturn(avgPrimaryShardsPerNode);
        when(node.numPrimaryShards(anyString())).thenReturn(primaryShardCount);
        when(balancer.avgShardsPerNode(anyString())).thenReturn(avgShardsPerNode);
        when(node.numShards(anyString())).thenReturn(shardCount);
        when(node.getNodeId()).thenReturn("test-node");

        long expectedWeight = (shardCount >= avgShardsPerNode) ? INDEX_SHARD_PER_NODE_BREACH_WEIGHT : 0;
        expectedWeight += primaryShardCount > avgPrimaryShardsPerNode ? PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_WEIGHT : 0;
        assertEquals(expectedWeight, constraints.weight(balancer, node, "index"));
    }

}
