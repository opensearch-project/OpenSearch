/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ExistingShardsAllocatorTests extends OpenSearchAllocationTestCase {

    public void testRunnablesExecutedForUnassignedShards() throws InterruptedException {

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(2))
            .build();
        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")))
            .build();
        RoutingAllocation allocation = new RoutingAllocation(
            yesAllocationDeciders(),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        CountDownLatch expectedStateLatch = new CountDownLatch(3);
        TestAllocator testAllocator = new TestAllocator(expectedStateLatch);
        testAllocator.allocateAllUnassignedShards(allocation, true).run();
        // if the below condition is passed, then we are sure runnable executed for all primary shards
        assertTrue(expectedStateLatch.await(30, TimeUnit.SECONDS));

        expectedStateLatch = new CountDownLatch(6);
        testAllocator = new TestAllocator(expectedStateLatch);
        testAllocator.allocateAllUnassignedShards(allocation, false).run();
        // if the below condition is passed, then we are sure runnable executed for all replica shards
        assertTrue(expectedStateLatch.await(30, TimeUnit.SECONDS));
    }

    private static class TestAllocator implements ExistingShardsAllocator {

        final CountDownLatch countDownLatch;

        TestAllocator(CountDownLatch latch) {
            this.countDownLatch = latch;
        }

        @Override
        public void beforeAllocation(RoutingAllocation allocation) {

        }

        @Override
        public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {

        }

        @Override
        public void allocateUnassigned(
            ShardRouting shardRouting,
            RoutingAllocation allocation,
            UnassignedAllocationHandler unassignedAllocationHandler
        ) {
            countDownLatch.countDown();
        }

        @Override
        public AllocateUnassignedDecision explainUnassignedShardAllocation(
            ShardRouting unassignedShard,
            RoutingAllocation routingAllocation
        ) {
            return null;
        }

        @Override
        public void cleanCaches() {

        }

        @Override
        public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {

        }

        @Override
        public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {

        }

        @Override
        public int getNumberOfInFlightFetches() {
            return 0;
        }
    }
}
