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
    public void testAllocationForRemoteStoreBackedIndexForNoneDirectionAndMixedMode() throws Exception {
        logger.info("Initialize cluster");
        initializeCluster(true);

        logger.info("Add data nodes");
        String remoteNodeName1 = internalCluster().startDataOnlyNode();
        String remoteNodeName2 = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode1 = assertNodeInCluster(remoteNodeName1);
        DiscoveryNode remoteNode2 = assertNodeInCluster(remoteNodeName2);

        logger.info("Prepare test index");
        boolean isReplicaAllocation = randomBoolean();
        if (isReplicaAllocation) {
            prepareIndexWithAllocatedPrimary(remoteNode1, Optional.empty());
        } else {
            prepareIndexWithoutReplica(Optional.empty());
        }
        assertRemoteStoreBackedIndex(TEST_INDEX);

        logger.info("Switch to MIXED cluster compatibility mode");
        setClusterMode(MIXED.mode);
        addRemote = false;
        String docrepNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode docrepNode = assertNodeInCluster(docrepNodeName);

        logger.info("Verify decision for allocation on docrep node");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(docrepNode, !isReplicaAllocation, false, false);
        assertEquals(Decision.Type.NO, decision.type());
        String expectedReason = String.format(
            Locale.ROOT,
            "[none migration_direction]: %s shard copy can not be allocated to a non-remote node for remote store backed index",
            (isReplicaAllocation ? "replica" : "primary")
        );
        assertEquals(expectedReason, decision.getExplanation().toLowerCase(Locale.ROOT));

        logger.info("Attempt allocation of shard on non-remote node");
        attemptAllocation(docrepNodeName);

        logger.info("Verify non-allocation of shard");
        assertNonAllocation(!isReplicaAllocation);

        logger.info("Verify decision for allocation on remote node");
        decision = getDecisionForTargetNode(remoteNode2, !isReplicaAllocation, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        expectedReason = String.format(
            Locale.ROOT,
            "[none migration_direction]: %s shard copy can be allocated to a remote node for remote store backed index",
            (isReplicaAllocation ? "replica" : "primary")
        );
        assertEquals(expectedReason, decision.getExplanation().toLowerCase(Locale.ROOT));

        logger.info("Attempt free allocation of shard on remote node");
        attemptAllocation(null);

        logger.info("Verify successful allocation of shard");
        if (!isReplicaAllocation) {
            ensureGreen(TEST_INDEX);
        } else {
            ensureYellowAndNoInitializingShards(TEST_INDEX);
        }
        assertAllocation(!isReplicaAllocation, null);
        logger.info("Verify allocation on one of the remote nodes");
        ShardRouting shardRouting = getShardRouting(!isReplicaAllocation);
        assertTrue(shardRouting.currentNodeId().equals(remoteNode1.getId()) || shardRouting.currentNodeId().equals(remoteNode2.getId()));
    }

    // bootstrap a cluster
    private void initializeCluster(boolean remoteClusterManager) {
        addRemote = remoteClusterManager;
        internalCluster().startClusterManagerOnlyNode();
        client = internalCluster().client();
    }

}
