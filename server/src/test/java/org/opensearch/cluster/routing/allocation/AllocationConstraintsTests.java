/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.CLUSTER_PRIMARY_SHARD_REBALANCE_CONSTRAINT_ID;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.CONSTRAINT_WEIGHT;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.allocator.LocalShardsBalancer;
import org.opensearch.cluster.routing.allocation.allocator.ShardsBalancer;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

public class AllocationConstraintsTests extends OpenSearchAllocationTestCase {

    long constraintWeight = 20L;

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
        assertEquals(expectedWeight, constraints.weight(balancer, node, "index", constraintWeight));

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

        assertEquals(0, constraints.weight(balancer, node, indexName, constraintWeight));

        perIndexPrimaryShardCount = 2;
        when(node.numPrimaryShards(anyString())).thenReturn(perIndexPrimaryShardCount);
        assertEquals(CONSTRAINT_WEIGHT, constraints.weight(balancer, node, indexName, constraintWeight));

        constraints.updateAllocationConstraint(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, false);
        assertEquals(0, constraints.weight(balancer, node, indexName, constraintWeight));
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

        assertEquals(0, constraints.weight(balancer, node, indexName, constraintWeight));

        primaryShardCount = 3;
        when(node.numPrimaryShards()).thenReturn(primaryShardCount);
        assertEquals(constraintWeight, constraints.weight(balancer, node, indexName, constraintWeight));

        constraints.updateAllocationConstraint(CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, false);
        assertEquals(0, constraints.weight(balancer, node, indexName, constraintWeight));
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

        assertEquals(0, constraints.weight(balancer, node, indexName, constraintWeight));

        // breaching global primary shard count but not per index primary shard count
        primaryShardCount = 5;
        when(node.numPrimaryShards()).thenReturn(primaryShardCount);
        assertEquals(constraintWeight, constraints.weight(balancer, node, indexName, constraintWeight));

        // when per index primary shard count constraint is also breached
        perIndexPrimaryShardCount = 3;
        when(node.numPrimaryShards(indexName)).thenReturn(perIndexPrimaryShardCount);
        assertEquals(CONSTRAINT_WEIGHT + constraintWeight, constraints.weight(balancer, node, indexName, constraintWeight));

        // disable both constraints
        constraints.updateAllocationConstraint(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, false);
        constraints.updateAllocationConstraint(CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, false);
        assertEquals(0, constraints.weight(balancer, node, indexName, constraintWeight));
    }

    public void testPrimaryConstraintWeightsAtAllocationAndRebalanceThresholds() {
        ShardsBalancer balancer = mock(ShardsBalancer.class);
        BalancedShardsAllocator.ModelNode node = mock(BalancedShardsAllocator.ModelNode.class);
        AllocationConstraints allocation = new AllocationConstraints();
        RebalanceConstraints rebalance = new RebalanceConstraints(new RebalanceParameter(0.0f));
        allocation.updateAllocationConstraint(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, true);
        rebalance.updateRebalanceConstraint(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, true);
        when(balancer.avgPrimaryShardsPerNode("index")).thenReturn(1.5f);

        // Three primaries across two nodes have a ceiling of two. A node with two primaries
        // should discourage another allocation, but its existing primary count is balanced.
        int[] counts = { 1, 2, 3 };
        long[] allocationWeights = { 0, CONSTRAINT_WEIGHT, CONSTRAINT_WEIGHT };
        long[] rebalanceWeights = { 0, 0, CONSTRAINT_WEIGHT };
        for (int i = 0; i < counts.length; i++) {
            when(node.numPrimaryShards("index")).thenReturn(counts[i]);
            assertEquals(
                "per-index allocation with " + counts[i] + " primaries",
                allocationWeights[i],
                allocation.weight(balancer, node, "index", constraintWeight)
            );
            assertEquals(
                "per-index rebalance with " + counts[i] + " primaries",
                rebalanceWeights[i],
                rebalance.weight(balancer, node, "index", constraintWeight)
            );
        }
        rebalance.updateRebalanceConstraint(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, false);
        assertEquals(0, rebalance.weight(balancer, node, "index", constraintWeight));

        // Use fresh constraints so the per-index penalty cannot mask the global boundary.
        allocation = new AllocationConstraints();
        rebalance = new RebalanceConstraints(new RebalanceParameter(0.5f));
        allocation.updateAllocationConstraint(CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, true);
        rebalance.updateRebalanceConstraint(CLUSTER_PRIMARY_SHARD_REBALANCE_CONSTRAINT_ID, true);
        when(balancer.avgPrimaryShardsPerNode()).thenReturn(2.5f);

        // Five primaries across two nodes: allocation penalizes counts at ceil(2.5) = 3.
        // Rebalance tolerates ceil(2.5 * 1.5) = 4 primaries, including equality at that limit.
        counts = new int[] { 2, 3, 4, 5 };
        allocationWeights = new long[] { 0, constraintWeight, constraintWeight, constraintWeight };
        rebalanceWeights = new long[] { 0, 0, 0, constraintWeight };
        for (int i = 0; i < counts.length; i++) {
            when(node.numPrimaryShards()).thenReturn(counts[i]);
            assertEquals(
                "global allocation with " + counts[i] + " primaries",
                allocationWeights[i],
                allocation.weight(balancer, node, "index", constraintWeight)
            );
            assertEquals(
                "global rebalance with " + counts[i] + " primaries",
                rebalanceWeights[i],
                rebalance.weight(balancer, node, "index", constraintWeight)
            );
        }
        rebalance.updateRebalanceConstraint(CLUSTER_PRIMARY_SHARD_REBALANCE_CONSTRAINT_ID, false);
        assertEquals(0, rebalance.weight(balancer, node, "index", constraintWeight));
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

        long expectedWeight = (shardCount >= (int) Math.ceil(avgPerIndexShardsPerNode)) ? CONSTRAINT_WEIGHT : 0;
        expectedWeight += perIndexPrimaryShardCount >= (int) Math.ceil(avgPerIndexPrimaryShardsPerNode) ? CONSTRAINT_WEIGHT : 0;
        expectedWeight += primaryShardsPerNode >= (int) Math.ceil(avgPrimaryShardsPerNode) ? constraintWeight : 0;
        assertEquals(expectedWeight, constraints.weight(balancer, node, indexName, constraintWeight));
    }

}
