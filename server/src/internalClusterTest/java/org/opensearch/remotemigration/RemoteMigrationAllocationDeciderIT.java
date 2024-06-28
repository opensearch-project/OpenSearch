/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode.MIXED;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.Direction.REMOTE_STORE;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteMigrationAllocationDeciderIT extends RemoteStoreMigrationShardAllocationBaseTestCase {

    // When the primary is on doc rep node, existing replica copy can get allocated on excluded docrep node.
    public void testFilterAllocationSkipsReplica() throws IOException {
        addRemote = false;
        List<String> docRepNodes = internalCluster().startNodes(3);
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0")
                .build()
        );
        ensureGreen("test");

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store")
                .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        assertTrue(
            internalCluster().client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", String.join(",", docRepNodes)))
                .execute()
                .actionGet()
                .isAcknowledged()
        );
        internalCluster().stopRandomDataNode();
        ensureGreen("test");
    }

    // When the primary is on remote node, new replica copy shouldn't get allocated on an excluded docrep node.
    public void testFilterAllocationSkipsReplicaOnExcludedNode() throws IOException {
        addRemote = false;
        List<String> nodes = internalCluster().startNodes(2);
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0")
                .build()
        );
        ensureGreen("test");
        addRemote = true;

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store")
                .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        String remoteNode = internalCluster().startNode();

        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand("test", 0, primaryNodeName("test"), remoteNode))
            .execute()
            .actionGet();
        client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout(TimeValue.timeValueSeconds(60))
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();
        assertEquals(remoteNode, primaryNodeName("test"));

        assertTrue(
            internalCluster().client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", String.join(",", nodes)))
                .execute()
                .actionGet()
                .isAcknowledged()
        );
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNodeName("test")));
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setTimeout(TimeValue.timeValueSeconds(2))
            .execute()
            .actionGet();
        assertTrue(clusterHealthResponse.isTimedOut());
        ensureYellow("test");
    }

    // When under mixed mode and remote_store direction, a primary shard can only be allocated to a remote node

    public void testNewPrimaryShardAllocationForRemoteStoreMigration() throws Exception {
        logger.info("Initialize cluster");
        internalCluster().startClusterManagerOnlyNode();

        logger.info("Add non-remote data node");
        String nonRemoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);

        logger.info("Set mixed mode and remote_store direction");
        setClusterMode(MIXED.mode);
        setDirection(REMOTE_STORE.direction);

        logger.info("Verify expected decision for allocating a new primary shard on a non-remote node");
        prepareIndexWithoutReplica(Optional.empty());
        Decision decision = getDecisionForTargetNode(nonRemoteNode, true, true, false);
        assertEquals(Decision.Type.NO, decision.type());
        assertEquals(
            "[remote_store migration_direction]: primary shard copy can not be allocated to a non-remote node",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );

        logger.info("Attempt allocation on non-remote node");
        attemptAllocation(null);

        logger.info("Verify non-allocation of primary shard on non-remote node");
        assertNonAllocation(true);

        logger.info("Add remote data node");
        setAddRemote(true);
        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);

        logger.info("Verify expected decision for allocating a new primary shard on a remote node");
        excludeAllNodes();
        decision = getDecisionForTargetNode(remoteNode, true, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals(
            "[remote_store migration_direction]: primary shard copy can be allocated to a remote node",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );

        logger.info("Attempt free allocation");
        attemptAllocation(null);
        ensureGreen(TEST_INDEX);

        logger.info("Verify allocation of primary shard on remote node");
        assertAllocation(true, remoteNode);
    }

    // When under mixed mode and remote_store direction, a replica shard can only be allocated to a remote node if the primary has relocated
    // to another remote node

    public void testNewReplicaShardAllocationIfPrimaryShardOnNonRemoteNodeForRemoteStoreMigration() throws Exception {
        logger.info("Initialize cluster");
        internalCluster().startClusterManagerOnlyNode();

        logger.info("Add non-remote data node");
        String nonRemoteNodeName1 = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode1 = assertNodeInCluster(nonRemoteNodeName1);

        logger.info("Allocate primary shard on non-remote node");
        prepareIndexWithAllocatedPrimary(nonRemoteNode1, Optional.empty());

        logger.info("Add remote data node");
        setClusterMode(MIXED.mode);
        setAddRemote(true);
        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);

        logger.info("Set remote_store direction");
        setDirection(REMOTE_STORE.direction);

        logger.info("Verify expected decision for allocating a replica shard on a remote node");
        excludeAllNodes();
        Decision decision = getDecisionForTargetNode(remoteNode, false, true, false);
        assertEquals(Decision.Type.NO, decision.type());
        assertEquals(
            "[remote_store migration_direction]: replica shard copy can not be allocated to a remote node since primary shard copy is not yet migrated to remote",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );

        logger.info("Attempt free allocation of replica shard");
        attemptAllocation(null);

        logger.info("Verify non-allocation of replica shard");
        assertNonAllocation(false);

        logger.info("Add another non-remote data node");
        setAddRemote(false);
        String nonRemoteNodeName2 = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode2 = assertNodeInCluster(nonRemoteNodeName2);

        logger.info("Verify expected decision for allocating the replica shard on a non-remote node");
        excludeAllNodes();
        decision = getDecisionForTargetNode(nonRemoteNode2, false, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals(
            "[remote_store migration_direction]: replica shard copy can be allocated to a non-remote node",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );

        logger.info("Attempt free allocation of replica shard");
        attemptAllocation(null);
        ensureGreen(TEST_INDEX);

        logger.info("Verify allocation of replica shard on non-remote node");
        assertAllocation(false, nonRemoteNode2);
    }

    public void testNewReplicaShardAllocationIfPrimaryShardOnRemoteNodeForRemoteStoreMigration() throws Exception {
        logger.info("Initialize cluster");
        internalCluster().startClusterManagerOnlyNode();

        logger.info("Add non-remote data nodes");
        String nonRemoteNodeName1 = internalCluster().startDataOnlyNode();
        String nonRemoteNodeName2 = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode1 = assertNodeInCluster(nonRemoteNodeName1);
        DiscoveryNode nonRemoteNode2 = assertNodeInCluster(nonRemoteNodeName2);

        logger.info("Allocate primary and replica shard on non-remote nodes");
        createIndex(TEST_INDEX, 1);
        ensureGreen(TEST_INDEX);

        logger.info("Set mixed mode");
        setClusterMode(MIXED.mode);

        logger.info("Add remote data nodes");
        setAddRemote(true);
        String remoteNodeName1 = internalCluster().startDataOnlyNode();
        String remoteNodeName2 = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode1 = assertNodeInCluster(remoteNodeName1);
        DiscoveryNode remoteNode2 = assertNodeInCluster(remoteNodeName2);

        logger.info("Set remote_store direction");
        setDirection(REMOTE_STORE.direction);

        logger.info("Relocate primary shard to remote node");
        DiscoveryNode initialPrimaryNode = primaryNodeName(TEST_INDEX).equals(nonRemoteNodeName1) ? nonRemoteNode1 : nonRemoteNode2;
        DiscoveryNode initialReplicaNode = initialPrimaryNode.equals(nonRemoteNode1) ? nonRemoteNode2 : nonRemoteNode1;
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(TEST_INDEX, 0, initialPrimaryNode.getName(), remoteNodeName1))
                .get()
        );
        ensureGreen(TEST_INDEX);
        assertAllocation(true, remoteNode1);

        logger.info("Verify expected decision for relocating a replica shard on non-remote node");
        Decision decision = getDecisionForTargetNode(initialPrimaryNode, false, true, true);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals(
            "[remote_store migration_direction]: replica shard copy can be relocated to a non-remote node",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );

        logger.info("Attempt relocation of replica shard to non-remote node");
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(TEST_INDEX, 0, initialReplicaNode.getName(), initialPrimaryNode.getName()))
                .get()
        );

        logger.info("Verify relocation of replica shard to non-remote node");
        ensureGreen(TEST_INDEX);
        assertAllocation(false, initialPrimaryNode);

        logger.info("Verify expected decision for relocating a replica shard on remote node");
        decision = getDecisionForTargetNode(remoteNode2, false, true, true);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals(
            "[remote_store migration_direction]: replica shard copy can be relocated to a remote node since primary shard copy has been migrated to remote",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );

        logger.info("Attempt relocation of replica shard to remote node");
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(TEST_INDEX, 0, initialPrimaryNode.getName(), remoteNodeName2))
                .get()
        );

        logger.info("Verify relocation of replica shard to non-remote node");
        ensureGreen(TEST_INDEX);
        assertAllocation(false, remoteNode2);
    }

    // When under strict mode, a shard can be allocated to any node

    public void testAlwaysAllocateNewShardForStrictMode() throws Exception {
        boolean isRemoteCluster = randomBoolean();
        boolean isReplicaAllocation = randomBoolean();

        logger.info("Initialize cluster and add nodes");
        setAddRemote(isRemoteCluster);
        internalCluster().startClusterManagerOnlyNode();
        String nodeName1 = internalCluster().startDataOnlyNode();
        String nodeName2 = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode node1 = assertNodeInCluster(nodeName1);
        DiscoveryNode node2 = assertNodeInCluster(nodeName2);

        if (isReplicaAllocation) {
            prepareIndexWithAllocatedPrimary(node1, Optional.empty());
        } else {
            prepareIndexWithoutReplica(Optional.empty());
        }

        if (isRemoteCluster) {
            assertRemoteStoreBackedIndex(TEST_INDEX);
        } else {
            assertNonRemoteStoreBackedIndex(TEST_INDEX);
        }

        logger.info("Verify expected decision for allocation of a shard");
        excludeAllNodes();
        Decision decision = getDecisionForTargetNode(
            isReplicaAllocation ? node2 : randomFrom(node1, node2),
            !isReplicaAllocation,
            true,
            false
        );
        assertEquals(Decision.Type.YES, decision.type());
        String expectedReason = String.format(
            Locale.ROOT,
            "[none migration_direction]: %s shard copy can be allocated to a %s node for strict compatibility mode",
            (isReplicaAllocation ? "replica" : "primary"),
            (isRemoteCluster ? "remote" : "non-remote")
        );
        assertEquals(expectedReason, decision.getExplanation().toLowerCase(Locale.ROOT));

        logger.info("Attempt free allocation");
        attemptAllocation(null);
        ensureGreen(TEST_INDEX);

        logger.info("Verify allocation of shard");
        assertAllocation(!isReplicaAllocation, !isReplicaAllocation ? null : node2);
    }

    // When under mixed mode and remote_store direction, shard of a remote store backed index can not be allocated to a non-remote node

    public void testRemoteStoreBackedIndexShardAllocationForRemoteStoreMigration() throws Exception {
        logger.info("Initialize cluster");
        internalCluster().startClusterManagerOnlyNode();

        logger.info("Set mixed mode");
        setClusterMode(MIXED.mode);

        logger.info("Add remote and non-remote nodes");
        String nonRemoteNodeName = internalCluster().startDataOnlyNode();
        setAddRemote(true);
        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);

        logger.info("Set remote_store direction");
        setDirection(REMOTE_STORE.direction);

        boolean isReplicaAllocation = randomBoolean();
        if (isReplicaAllocation) {
            logger.info("Create index with primary allocated on remote node");
            prepareIndexWithAllocatedPrimary(remoteNode, Optional.empty());
        } else {
            logger.info("Create index with unallocated primary");
            prepareIndexWithoutReplica(Optional.empty());
        }

        logger.info("Verify remote store backed index");
        assertRemoteStoreBackedIndex(TEST_INDEX);

        logger.info("Verify expected decision for allocation of shard on a non-remote node");
        excludeAllNodes();
        Decision decision = getDecisionForTargetNode(nonRemoteNode, !isReplicaAllocation, false, false);
        assertEquals(Decision.Type.NO, decision.type());
        String expectedReason = String.format(
            Locale.ROOT,
            "[remote_store migration_direction]: %s shard copy can not be allocated to a non-remote node because a remote store backed index's shard copy can only be allocated to a remote node",
            (isReplicaAllocation ? "replica" : "primary")
        );
        assertEquals(expectedReason, decision.getExplanation().toLowerCase(Locale.ROOT));

        logger.info("Attempt allocation of shard on non-remote node");
        attemptAllocation(nonRemoteNodeName);

        logger.info("Verify non-allocation of shard");
        assertNonAllocation(!isReplicaAllocation);
    }

    // When under mixed mode and none direction, allocate shard of a remote store backed index to a remote node and shard of a non remote
    // store backed index to a non-remote node only

    public void testAllocationForNoneDirectionAndMixedMode() throws Exception {
        boolean isRemoteStoreBackedIndex = randomBoolean();
        boolean isReplicaAllocation = randomBoolean();
        logger.info(
            String.format(
                Locale.ROOT,
                "Test for allocation decisions for %s shard of a %s store backed index under NONE direction",
                (isReplicaAllocation ? "replica" : "primary"),
                (isRemoteStoreBackedIndex ? "remote" : "non remote")
            )
        );

        logger.info("Initialize cluster");
        setAddRemote(isRemoteStoreBackedIndex);
        internalCluster().startClusterManagerOnlyNode();

        logger.info("Add data nodes");
        String previousNodeName1 = internalCluster().startDataOnlyNode();
        String previousNodeName2 = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode previousNode1 = assertNodeInCluster(previousNodeName1);
        DiscoveryNode previousNode2 = assertNodeInCluster(previousNodeName2);

        logger.info("Prepare test index");
        if (isReplicaAllocation) {
            prepareIndexWithAllocatedPrimary(previousNode1, Optional.empty());
        } else {
            prepareIndexWithoutReplica(Optional.empty());
        }

        if (isRemoteStoreBackedIndex) {
            assertRemoteStoreBackedIndex(TEST_INDEX);
        } else {
            assertNonRemoteStoreBackedIndex(TEST_INDEX);
        }

        logger.info("Switch to MIXED cluster compatibility mode");
        setClusterMode(MIXED.mode);
        setAddRemote(!addRemote);
        String newNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode newNode = assertNodeInCluster(newNodeName);

        logger.info("Verify decision for allocation on the new node");
        excludeAllNodes();
        Decision decision = getDecisionForTargetNode(newNode, !isReplicaAllocation, false, false);
        assertEquals(Decision.Type.NO, decision.type());
        String expectedReason = String.format(
            Locale.ROOT,
            "[none migration_direction]: %s shard copy can not be allocated to a %s node for %s store backed index",
            (isReplicaAllocation ? "replica" : "primary"),
            (isRemoteStoreBackedIndex ? "non-remote" : "remote"),
            (isRemoteStoreBackedIndex ? "remote" : "non remote")
        );
        assertEquals(expectedReason, decision.getExplanation().toLowerCase(Locale.ROOT));

        logger.info("Attempt allocation of shard on new node");
        attemptAllocation(newNodeName);

        logger.info("Verify non-allocation of shard");
        assertNonAllocation(!isReplicaAllocation);

        logger.info("Verify decision for allocation on previous node");
        decision = getDecisionForTargetNode(previousNode2, !isReplicaAllocation, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        expectedReason = String.format(
            Locale.ROOT,
            "[none migration_direction]: %s shard copy can be allocated to a %s node for %s store backed index",
            (isReplicaAllocation ? "replica" : "primary"),
            (isRemoteStoreBackedIndex ? "remote" : "non-remote"),
            (isRemoteStoreBackedIndex ? "remote" : "non remote")
        );
        assertEquals(expectedReason, decision.getExplanation().toLowerCase(Locale.ROOT));

        logger.info("Attempt free allocation of shard");
        attemptAllocation(null);

        logger.info("Verify successful allocation of shard");
        if (!isReplicaAllocation) {
            ensureGreen(TEST_INDEX);
        } else {
            ensureYellowAndNoInitializingShards(TEST_INDEX);
        }
        assertAllocation(!isReplicaAllocation, null);
        logger.info("Verify allocation on one of the previous nodes");
        ShardRouting shardRouting = getShardRouting(!isReplicaAllocation);
        assertTrue(
            shardRouting.currentNodeId().equals(previousNode1.getId()) || shardRouting.currentNodeId().equals(previousNode2.getId())
        );
    }
}
