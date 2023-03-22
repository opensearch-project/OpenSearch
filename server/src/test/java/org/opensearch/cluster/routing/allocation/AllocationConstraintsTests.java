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
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.CONSTRAINT_WEIGHT;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID;

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

        long expectedWeight = (shardCount >= avgShardsPerNode) ? CONSTRAINT_WEIGHT : 0;
        assertEquals(expectedWeight, constraints.weight(balancer, node, "index"));

    }

    /**
     * Test constraint evaluation logic when with different values of ConstraintMode
     * for IndexShardPerNode constraint satisfied and breached.
     */
    public void testPerIndexPrimaryShardsConstraint() {
        ShardsBalancer balancer = mock(LocalShardsBalancer.class);
        BalancedShardsAllocator.ModelNode node = mock(BalancedShardsAllocator.ModelNode.class);
        AllocationConstraints constraints = new AllocationConstraints();
        constraints.updateAllocationConstraint(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, true);

        final String indexName = "test-index";
        int perIndexPrimaryShardCount = 1;
        float avgPerIndexPrimaryShardsPerNode = 2f;

        when(balancer.avgPrimaryShardsPerNode(anyString())).thenReturn(avgPerIndexPrimaryShardsPerNode);
        when(node.numPrimaryShards(anyString())).thenReturn(perIndexPrimaryShardCount);
        when(node.getNodeId()).thenReturn("test-node");

        assertEquals(0, constraints.weight(balancer, node, indexName));

        perIndexPrimaryShardCount = 3;
        when(node.numPrimaryShards(anyString())).thenReturn(perIndexPrimaryShardCount);
        assertEquals(CONSTRAINT_WEIGHT, constraints.weight(balancer, node, indexName));

        constraints.updateAllocationConstraint(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, false);
        assertEquals(0, constraints.weight(balancer, node, indexName));
    }

    /**
     * Test constraint evaluation logic when per index primary shard count constraint is breached.
     */
    public void testGlobalPrimaryShardsConstraint() {
        ShardsBalancer balancer = mock(LocalShardsBalancer.class);
        BalancedShardsAllocator.ModelNode node = mock(BalancedShardsAllocator.ModelNode.class);
        AllocationConstraints constraints = new AllocationConstraints();
        constraints.updateAllocationConstraint(CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, true);

        final String indexName = "test-index";
        int primaryShardCount = 1;
        float avgPrimaryShardsPerNode = 2f;

        when(balancer.avgPrimaryShardsPerNode()).thenReturn(avgPrimaryShardsPerNode);
        when(node.numPrimaryShards()).thenReturn(primaryShardCount);
        when(node.getNodeId()).thenReturn("test-node");

        assertEquals(0, constraints.weight(balancer, node, indexName));

        primaryShardCount = 3;
        when(node.numPrimaryShards()).thenReturn(primaryShardCount);
        assertEquals(CONSTRAINT_WEIGHT, constraints.weight(balancer, node, indexName));

        constraints.updateAllocationConstraint(CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, false);
        assertEquals(0, constraints.weight(balancer, node, indexName));
    }

    /**
     * Test constraint evaluation logic when both per index and global primary shard count constraint is breached.
     */
    public void testPrimaryShardsConstraints() {
        ShardsBalancer balancer = mock(LocalShardsBalancer.class);
        BalancedShardsAllocator.ModelNode node = mock(BalancedShardsAllocator.ModelNode.class);
        AllocationConstraints constraints = new AllocationConstraints();
        constraints.updateAllocationConstraint(CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, true);
        constraints.updateAllocationConstraint(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, true);

        final String indexName = "test-index";
        int perIndexPrimaryShardCount = 1;
        float avgPerIndexPrimaryShardCount = 2;
        int primaryShardCount = 2;
        float avgPrimaryShardsPerNode = 4;

        when(balancer.avgPrimaryShardsPerNode(indexName)).thenReturn(avgPerIndexPrimaryShardCount);
        when(node.numPrimaryShards(indexName)).thenReturn(perIndexPrimaryShardCount);
        when(balancer.avgPrimaryShardsPerNode()).thenReturn(avgPrimaryShardsPerNode);
        when(node.numPrimaryShards()).thenReturn(primaryShardCount);
        when(node.getNodeId()).thenReturn("test-node");

        assertEquals(0, constraints.weight(balancer, node, indexName));

        // breaching global primary shard count but not per index primary shard count
        primaryShardCount = 5;
        when(node.numPrimaryShards()).thenReturn(primaryShardCount);
        assertEquals(CONSTRAINT_WEIGHT, constraints.weight(balancer, node, indexName));

        // when per index primary shard count constraint is also breached
        perIndexPrimaryShardCount = 3;
        when(node.numPrimaryShards(indexName)).thenReturn(perIndexPrimaryShardCount);
        assertEquals(2 * CONSTRAINT_WEIGHT, constraints.weight(balancer, node, indexName));

        // disable both constraints
        constraints.updateAllocationConstraint(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, false);
        constraints.updateAllocationConstraint(CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, false);
        assertEquals(0, constraints.weight(balancer, node, indexName));
    }

    /**
     * Test constraint evaluation logic when with different values of ConstraintMode
     * for IndexShardPerNode constraint satisfied and breached.
     */
    public void testAllConstraints() {
        ShardsBalancer balancer = mock(LocalShardsBalancer.class);
        BalancedShardsAllocator.ModelNode node = mock(BalancedShardsAllocator.ModelNode.class);
        AllocationConstraints constraints = new AllocationConstraints();
        constraints.updateAllocationConstraint(INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID, true);
        constraints.updateAllocationConstraint(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, true);
        constraints.updateAllocationConstraint(CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, true);

        final String indexName = "test-index";
        int shardCount = randomIntBetween(1, 500);
        float avgPerIndexShardsPerNode = 1.0f + (random().nextFloat()) * 999.0f;

        int perIndexPrimaryShardCount = randomIntBetween(1, shardCount);
        float avgPerIndexPrimaryShardsPerNode = (random().nextFloat()) * avgPerIndexShardsPerNode;

        float avgPrimaryShardsPerNode = 1.0f + (random().nextFloat()) * 999.0f;
        int primaryShardsPerNode = randomIntBetween(1, shardCount);

        when(balancer.avgPrimaryShardsPerNode(indexName)).thenReturn(avgPerIndexPrimaryShardsPerNode);
        when(node.numPrimaryShards(indexName)).thenReturn(perIndexPrimaryShardCount);

        when(balancer.avgPrimaryShardsPerNode()).thenReturn(avgPrimaryShardsPerNode);
        when(node.numPrimaryShards()).thenReturn(primaryShardsPerNode);

        when(balancer.avgShardsPerNode(indexName)).thenReturn(avgPerIndexShardsPerNode);
        when(node.numShards(indexName)).thenReturn(shardCount);
        when(node.getNodeId()).thenReturn("test-node");

        long expectedWeight = (shardCount >= avgPerIndexShardsPerNode) ? CONSTRAINT_WEIGHT : 0;
        expectedWeight += perIndexPrimaryShardCount > avgPerIndexPrimaryShardsPerNode ? CONSTRAINT_WEIGHT : 0;
        expectedWeight += primaryShardsPerNode >= avgPrimaryShardsPerNode ? CONSTRAINT_WEIGHT : 0;
        assertEquals(expectedWeight, constraints.weight(balancer, node, indexName));
    }

}
