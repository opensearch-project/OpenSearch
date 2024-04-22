/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Locale;
import java.util.Optional;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode.MIXED;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreMigrationShardAllocationIT extends RemoteStoreMigrationShardAllocationBaseTestCase {

    public static final String NAME = "remote_store_migration";

    private Client client;

    // test for shard allocation decisions for MIXED mode and NONE direction
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
        initializeCluster(isRemoteStoreBackedIndex);

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
        addRemote = !addRemote;
        String newNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode newNode = assertNodeInCluster(newNodeName);

        logger.info("Verify decision for allocation on the new node");
        prepareDecisions();
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

    // bootstrap a cluster
    private void initializeCluster(boolean remoteClusterManager) {
        addRemote = remoteClusterManager;
        internalCluster().startClusterManagerOnlyNode();
        client = internalCluster().client();
    }

}
