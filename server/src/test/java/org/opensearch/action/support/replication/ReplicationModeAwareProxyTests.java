/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShardTestUtils;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ReplicationModeAwareProxyTests extends OpenSearchTestCase {

    /*
    Replication action running on the same primary copy from which it originates.
    Action should not run and proxy should return ReplicationMode.NO_REPLICATION
     */
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
            mock(TransportReplicationAction.ReplicasProxy.class),
            randomBoolean()
        );
        assertEquals(ReplicationMode.NO_REPLICATION, replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting));
    }

    /*
     Replication action originating from failing primary to replica being promoted to primary
     Action should run and proxy should return ReplicationMode.FULL_REPLICATION
     */
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
            mock(TransportReplicationAction.ReplicasProxy.class),
            randomBoolean()
        );
        assertEquals(ReplicationMode.FULL_REPLICATION, replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting));
    }

    /*
     Replication action originating from remote enabled primary to docrep replica during remote store migration
     Action should run and proxy should return ReplicationMode.FULL_REPLICATION
     */
    public void testDetermineReplicationModeTargetRoutingDocrepShard() {
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node",
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting targetRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node-2",
            false,
            ShardRoutingState.STARTED
        );
        final ReplicationModeAwareProxy replicationModeAwareProxy = new ReplicationModeAwareProxy(
            ReplicationMode.NO_REPLICATION,
            DiscoveryNodes.builder()
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(primaryRouting.currentNodeId()))
                .add(IndexShardTestUtils.getFakeDiscoNode(targetRouting.currentNodeId()))
                .build(),
            mock(TransportReplicationAction.ReplicasProxy.class),
            mock(TransportReplicationAction.ReplicasProxy.class),
            false
        );
        assertEquals(ReplicationMode.FULL_REPLICATION, replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting));
    }

    /*
     Replication action originating from remote enabled primary to remote replica during remote store migration
     Action should not run and proxy should return ReplicationMode.NO_REPLICATION
     */
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
            mock(TransportReplicationAction.ReplicasProxy.class),
            false
        );
        assertEquals(ReplicationMode.NO_REPLICATION, replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting));
    }

    /*
     Replication action originating from remote enabled primary to remote enabled replica during remote store migration
     with an explicit replication mode specified
     Action should run and proxy should return the overridden Replication Mode
     */
    public void testDetermineReplicationWithExplicitOverrideTargetRoutingRemoteShard() {
        ReplicationMode replicationModeOverride = ReplicationMode.PRIMARY_TERM_VALIDATION;
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
            replicationModeOverride,
            DiscoveryNodes.builder()
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(targetRouting.currentNodeId()))
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(primaryRouting.currentNodeId()))
                .build(),
            mock(TransportReplicationAction.ReplicasProxy.class),
            mock(TransportReplicationAction.ReplicasProxy.class),
            false
        );
        assertEquals(replicationModeOverride, replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting));
    }

    /*
     Replication action originating from remote enabled primary with remote enabled index settings enabled
     Action should not query the DiscoveryNodes object
     */
    public void testDetermineReplicationWithRemoteIndexSettingsEnabled() {
        DiscoveryNodes mockDiscoveryNodes = mock(DiscoveryNodes.class);
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
            mockDiscoveryNodes,
            mock(TransportReplicationAction.ReplicasProxy.class),
            mock(TransportReplicationAction.ReplicasProxy.class),
            true
        );
        replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting);
        // Verify no interactions with the DiscoveryNodes object
        verify(mockDiscoveryNodes, never()).get(anyString());
    }

    /**
     * Node running with {@code remote_store.mode: segments_only}: a segment repository and no translog repository.
     * The index therefore has no remote translog, so a replica can only obtain operations over the wire.
     */
    private static DiscoveryNode segmentsOnlyNode(String id) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, "segment-test-repo");
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), attributes, DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
    }

    /**
     * Same as {@link #segmentsOnlyNode} but with a cluster state repository added. The translog still lives on local
     * disk, so the durability requirements are identical and the replication mode must not differ.
     */
    private static DiscoveryNode segmentsOnlyNodeWithClusterStateRepo(String id) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, "segment-test-repo");
        attributes.put(RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, "state-test-repo");
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), attributes, DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
    }

    private static ReplicationMode determineModeBetween(DiscoveryNode primaryNode, DiscoveryNode replicaNode) {
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            primaryNode.getId(),
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting targetRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            replicaNode.getId(),
            false,
            ShardRoutingState.STARTED
        );
        ReplicationModeAwareProxy proxy = new ReplicationModeAwareProxy(
            // what TransportShardBulkAction#getReplicationMode returns for a shard on a remote-attributed node
            ReplicationMode.PRIMARY_TERM_VALIDATION,
            DiscoveryNodes.builder().add(primaryNode).add(replicaNode).build(),
            mock(TransportReplicationAction.ReplicasProxy.class),
            mock(TransportReplicationAction.ReplicasProxy.class),
            // IndexShard#isRemoteTranslogEnabled: false, because no translog repository is configured
            false
        );
        return proxy.determineReplicationMode(targetRouting, primaryRouting);
    }

    /**
     * Sanity check for the segments-only baseline: with no cluster state repository the escape hatch in
     * determineReplicationMode fires and the replica correctly receives operations.
     */
    public void testDetermineReplicationModeSegmentsOnlyReplicatesToReplica() {
        assertEquals(ReplicationMode.FULL_REPLICATION, determineModeBetween(segmentsOnlyNode("node-1"), segmentsOnlyNode("node-2")));
    }

    /**
     * Adding a cluster state repository does not change where the translog lives, so the replica still has to receive
     * operations for an acknowledged write to survive loss of the primary. determineReplicationMode gates its
     * FULL_REPLICATION escape hatch on DiscoveryNode#isRemoteTranslogStoreNode rather than on
     * DiscoveryNode#isRemoteStoreNode, so the replica keeps receiving operations instead of being downgraded to a
     * primary term validation ping.
     */
    public void testDetermineReplicationModeSegmentsOnlyWithClusterStateRepoReplicatesToReplica() {
        assertEquals(
            ReplicationMode.FULL_REPLICATION,
            determineModeBetween(segmentsOnlyNodeWithClusterStateRepo("node-1"), segmentsOnlyNodeWithClusterStateRepo("node-2"))
        );
    }
}
