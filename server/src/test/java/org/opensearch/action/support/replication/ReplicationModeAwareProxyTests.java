/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShardTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

public class ReplicationModeAwareProxyTests extends OpenSearchTestCase {
    public void testDetermineReplicationModeTargetRoutingCurrentPrimary() {
        ShardRouting targetRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node",
            null,
            true,
            ShardRoutingState.STARTED,
            AllocationId.newInitializing("abc")
        );
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node",
            null,
            true,
            ShardRoutingState.STARTED,
            AllocationId.newInitializing("abc")
        );
        final ReplicationModeAwareProxy replicationModeAwareProxy = new ReplicationModeAwareProxy(
            ReplicationMode.NO_REPLICATION,
            DiscoveryNodes.builder().add(IndexShardTestUtils.getFakeRemoteEnabledNode("dummy-node")).build(),
            mock(TransportReplicationAction.ReplicasProxy.class),
            mock(TransportReplicationAction.ReplicasProxy.class)
        );
        assertEquals(ReplicationMode.NO_REPLICATION, replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting));
    }

    public void testDetermineReplicationModeTargetRoutingRelocatingPrimary() {
        AllocationId primaryId = AllocationId.newRelocation(AllocationId.newInitializing());
        AllocationId relocationTargetId = AllocationId.newTargetRelocation(primaryId);
        ShardRouting targetRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node-2",
            null,
            true,
            ShardRoutingState.INITIALIZING,
            relocationTargetId
        );
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node",
            "dummy-node-2",
            true,
            ShardRoutingState.RELOCATING,
            primaryId
        );
        final ReplicationModeAwareProxy replicationModeAwareProxy = new ReplicationModeAwareProxy(
            ReplicationMode.NO_REPLICATION,
            DiscoveryNodes.builder()
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(targetRouting.currentNodeId()))
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(primaryRouting.currentNodeId()))
                .build(),
            mock(TransportReplicationAction.ReplicasProxy.class),
            mock(TransportReplicationAction.ReplicasProxy.class)
        );
        assertEquals(ReplicationMode.FULL_REPLICATION, replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting));
    }

    public void testDetermineReplicationModeTargetRoutingDocrepShard() {
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node",
            false,
            ShardRoutingState.STARTED
        );
        ShardRouting targetRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node-2",
            true,
            ShardRoutingState.STARTED
        );
        final ReplicationModeAwareProxy replicationModeAwareProxy = new ReplicationModeAwareProxy(
            ReplicationMode.NO_REPLICATION,
            DiscoveryNodes.builder()
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(primaryRouting.currentNodeId()))
                .add(IndexShardTestUtils.getFakeDiscoNode(targetRouting.currentNodeId()))
                .build(),
            mock(TransportReplicationAction.ReplicasProxy.class),
            mock(TransportReplicationAction.ReplicasProxy.class)
        );
        assertEquals(ReplicationMode.FULL_REPLICATION, replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting));
    }

    public void testDetermineReplicationModeTargetRoutingRemoteShard() {
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node",
            false,
            ShardRoutingState.STARTED
        );
        ShardRouting targetRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node-2",
            true,
            ShardRoutingState.STARTED
        );
        final ReplicationModeAwareProxy replicationModeAwareProxy = new ReplicationModeAwareProxy(
            ReplicationMode.NO_REPLICATION,
            DiscoveryNodes.builder()
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(targetRouting.currentNodeId()))
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(primaryRouting.currentNodeId()))
                .build(),
            mock(TransportReplicationAction.ReplicasProxy.class),
            mock(TransportReplicationAction.ReplicasProxy.class)
        );
        assertEquals(ReplicationMode.NO_REPLICATION, replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting));
    }
}
