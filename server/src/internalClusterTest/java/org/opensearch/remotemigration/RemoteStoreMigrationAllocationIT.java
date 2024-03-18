/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.MoveDecision;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.cluster.metadata.IndexMetadata.*;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.Direction.REMOTE_STORE;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.Direction.NONE;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode.MIXED;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode.STRICT;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoteStoreMigrationAllocationIT extends MigrationBaseTestCase {

    private static final String TEST_INDEX = "test_index";
    protected static final String NAME = "remote_store_migration";

    private final ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
    private Client client;

    // tests for primary shard copy allocation with MIXED mode and REMOTE_STORE direction

    public void testDontAllocateNewPrimaryShardOnNonRemoteNodeForMixedModeAndRemoteStoreDirection() throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster(false);

        logger.info(" --> add remote and non-remote nodes");
        setClusterMode(MIXED.mode);
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);

        logger.info(" --> verify expected decision for allocating a new primary shard on a non-remote node");
        prepareIndexWithoutReplica(Optional.empty());

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE.direction);

        Decision decision = getDecisionForTargetNode(nonRemoteNode, true, true, false);
        Decision.Type type = Decision.Type.NO;
        assertEquals(type, decision.type());
        assertEquals("[remote_store migration_direction]: primary shard copy can not be allocated to a non-remote node", decision.getExplanation().toLowerCase(Locale.ROOT));

        logger.info(" --> attempt allocation");
        attemptAllocation(nonRemoteNodeName);

        logger.info(" --> verify non-allocation of primary shard");
        assertNonAllocation(true);
    }

    public void testAllocateNewPrimaryShardOnRemoteNodeForMixedModeAndRemoteStoreDirection() throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster(false);

        logger.info(" --> add remote and non-remote nodes");
        setClusterMode(MIXED.mode);
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);

        logger.info(" --> verify expected decision for allocating a new primary shard on a remote node");
        prepareIndexWithoutReplica(Optional.empty());

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE.direction);

        Decision decision = getDecisionForTargetNode(remoteNode, true, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals(
            "[remote_store migration_direction]: primary shard copy can be allocated to a remote node",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );

        logger.info(" --> attempt allocation");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", allNodesExcept(null))
                    .put("index.routing.allocation.exclude._name", "")
            )
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of primary shard");
        assertAllocation(true, remoteNode);
    }

    // tests for replica shard copy allocation with MIXED mode and REMOTE_STORE direction

    public void testDontAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnNonRemoteNodeForMixedModeAndRemoteStoreDirection()
        throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster(false);

        logger.info(" --> add remote and non-remote nodes");
        setClusterMode(MIXED.mode);
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);

        logger.info(" --> allocate primary shard on non-remote node");
        prepareIndexWithAllocatedPrimary(nonRemoteNode, Optional.empty());

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE.direction);

        logger.info(" --> verify expected decision for replica shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(remoteNode, false, true, false);
        assertEquals(Decision.Type.NO, decision.type());
        assertEquals(
            "[remote_store migration_direction]: replica shard copy can not be allocated to a remote node since primary shard copy is not yet migrated to remote",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );

        logger.info(" --> attempt allocation of replica shard on remote node");
        attemptAllocation(remoteNodeName);

        logger.info(" --> verify non-allocation of replica shard");
        assertNonAllocation(false);
    }

    public void testAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnRemoteNodeForMixedModeAndRemoteStoreDirection() throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster(false);

        logger.info(" --> set mixed cluster compatibility mode");
        setClusterMode(MIXED.mode);

        logger.info(" --> add remote and non-remote nodes");
        addRemote = true;
        String remoteNodeName1 = internalCluster().startNode();
        String remoteNodeName2 = internalCluster().startNode();
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode1 = assertNodeInCluster(remoteNodeName1);
        DiscoveryNode remoteNode2 = assertNodeInCluster(remoteNodeName2);
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);

        logger.info(" --> allocate primary shard on remote node");
        prepareIndexWithAllocatedPrimary(remoteNode1, Optional.empty());

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE.direction);

        logger.info(" --> verify expected decision for replica shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(remoteNode2, false, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals(
            "[remote_store migration_direction]: replica shard copy can be allocated to a remote node since primary shard copy has been migrated to remote",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );

        logger.info(" --> attempt allocation of replica shard the other remote node");
        attemptAllocation(remoteNodeName2);
        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of replica shard");
        assertAllocation(false, remoteNode2);
    }

    public void testAllocateNewReplicaShardOnNonRemoteNodeIfPrimaryShardOnNonRemoteNodeForMixedModeAndRemoteStoreDirection()
        throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster(false);

        logger.info(" --> add remote and non-remote nodes");
        setClusterMode(MIXED.mode);
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        addRemote = false;
        String nonRemoteNodeName1 = internalCluster().startNode();
        String nonRemoteNodeName2 = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        DiscoveryNode nonRemoteNode1 = assertNodeInCluster(nonRemoteNodeName1);
        DiscoveryNode nonRemoteNode2 = assertNodeInCluster(nonRemoteNodeName2);

        logger.info(" --> allocate primary shard on non-remote node");
        prepareIndexWithAllocatedPrimary(nonRemoteNode1, Optional.empty());

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE.direction);

        logger.info(" --> verify expected decision for replica shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(nonRemoteNode2, false, true, false);
        Decision.Type type = Decision.Type.YES;
        String reason = "[remote_store migration_direction]: replica shard copy can be allocated to a non-remote node";

        assertEquals(type, decision.type());
        assertEquals(reason, decision.getExplanation().toLowerCase(Locale.ROOT));

        logger.info(" --> allocate replica shard on the other non-remote node");
        attemptAllocation(nonRemoteNodeName2);
        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of replica shard");
        assertAllocation(false, nonRemoteNode2);
    }

    public void testAllocateNewReplicaShardOnNonRemoteNodeIfPrimaryShardOnRemoteNodeForMixedModeAndRemoteStoreDirection() throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster(false);

        logger.info(" --> set mixed cluster compatibility mode");

        logger.info(" --> add remote and non-remote nodes");
        setClusterMode(MIXED.mode);
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);

        logger.info(" --> allocate primary on remote node");
        prepareIndexWithAllocatedPrimary(remoteNode, Optional.empty());

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE.direction);

        logger.info(" --> verify expected decision for replica shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(nonRemoteNode, false, true, false);

        Decision.Type type = Decision.Type.YES;
        assertEquals(type, decision.type());
        assertEquals("[remote_store migration_direction]: replica shard copy can be allocated to a non-remote node", decision.getExplanation().toLowerCase(Locale.ROOT));

        logger.info(" --> allocate replica shard on non-remote node");
        attemptAllocation(nonRemoteNodeName);
        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of replica shard");
        assertAllocation(false, nonRemoteNode);
    }

    // test for STRICT mode

    public void testAlwaysAllocateNewShardForStrictMode() throws Exception {
        boolean isRemoteCluster = randomBoolean();
        boolean isReplicaAllocation = randomBoolean();

        logger.info(" --> initialize cluster and add nodes");
        List<DiscoveryNode> nodes = new ArrayList<>();
        if (isRemoteCluster) {
            initializeCluster(true);

            logger.info(" --> add remote nodes");
            String remoteNodeName1 = internalCluster().startNode();
            String remoteNodeName2 = internalCluster().startNode();
            internalCluster().validateClusterFormed();
            DiscoveryNode remoteNode1 = assertNodeInCluster(remoteNodeName1);
            DiscoveryNode remoteNode2 = assertNodeInCluster(remoteNodeName2);
            nodes.add(remoteNode1);
            nodes.add(remoteNode2);
        }
        else {
            initializeCluster(false);
            setClusterMode(STRICT.mode);
            addRemote = false;
            String nonRemoteNodeName1 = internalCluster().startNode();
            String nonRemoteNodeName2 = internalCluster().startNode();
            internalCluster().validateClusterFormed();
            DiscoveryNode nonRemoteNode1 = assertNodeInCluster(nonRemoteNodeName1);
            DiscoveryNode nonRemoteNode2 = assertNodeInCluster(nonRemoteNodeName2);
            nodes.add(nonRemoteNode1);
            nodes.add(nonRemoteNode2);
        }

        logger.info(" --> verify expected decision for allocating a new shard on a non-remote node");
        if (isReplicaAllocation) {
            prepareIndexWithAllocatedPrimary(nodes.get(0), Optional.empty());
        } else {
            prepareIndexWithoutReplica(Optional.empty());
        }

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE.direction);

        DiscoveryNode targetNode = isReplicaAllocation ? nodes.get(1) : nodes.get(0);

        assertEquals(
            (isRemoteCluster ? "true" : null),
            client.admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .getMetadata()
                .index(TEST_INDEX)
                .getSettings()
                .get(SETTING_REMOTE_STORE_ENABLED)
        );

        prepareDecisions();
        Decision decision = getDecisionForTargetNode(targetNode, !isReplicaAllocation, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        String reason = String.format(
            Locale.ROOT,
            "[remote_store migration_direction]: %s shard copy can be allocated to a %s node for strict compatibility mode",
            (isReplicaAllocation ? "replica" : "primary"),
            (isRemoteCluster ? "remote" : "non-remote")
        );
        assertEquals(reason, decision.getExplanation().toLowerCase(Locale.ROOT));

        logger.info(" --> attempt allocation");
        attemptAllocation(targetNode.getName());
        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of primary shard");
        assertAllocation(!isReplicaAllocation, targetNode);
    }

    // test for remote store backed index
    public void testDontAllocateToNonRemoteNodeForRemoteStoreBackedIndex() throws Exception {
        logger.info(" --> initialize cluster with remote master node");
        initializeCluster(true);

        logger.info(" --> add remote and non-remote nodes");
        String remoteNodeName = internalCluster().startNode();
        setClusterMode(MIXED.mode);
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);

        boolean isReplicaAllocation = randomBoolean();

        logger.info(" --> verify expected decision for allocating a new shard on a non-remote node");
        if (isReplicaAllocation) {
            prepareIndexWithAllocatedPrimary(remoteNode, Optional.empty());
        }
        else {
            prepareIndexWithoutReplica(Optional.empty());
        }

        assertEquals(
            "true",
            client.admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .getMetadata()
                .index(TEST_INDEX)
                .getSettings()
                .get(SETTING_REMOTE_STORE_ENABLED)
        );

        setDirection(REMOTE_STORE.direction);
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(nonRemoteNode, !isReplicaAllocation, false, false);
        assertEquals(Decision.Type.NO, decision.type());
        String reason = String.format(
            Locale.ROOT,
            "[remote_store migration_direction]: %s shard copy can not be allocated to a non-remote node because a remote store backed index's shard copy can only be allocated to a remote node",
            (isReplicaAllocation ? "replica" : "primary")
        );
        assertEquals(reason, decision.getExplanation().toLowerCase(Locale.ROOT));

        logger.info(" --> attempt allocation of shard on non-remote node");
        attemptAllocation(nonRemoteNodeName);

        logger.info(" --> verify non-allocation of shard");
        assertNonAllocation(!isReplicaAllocation);
    }

    // bootstrap a cluster
    public void initializeCluster(boolean remoteClusterManager) {
        addRemote = remoteClusterManager;
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        internalCluster().startNodes(1);
        client = internalCluster().client();
        setClusterMode(STRICT.mode);
        setDirection(NONE.direction);
    }

    // set the compatibility mode of cluster [strict, mixed]
    public void setClusterMode(String mode) {
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), mode));
        assertAcked(client.admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    // set the migration direction for cluster [remote_store, docrep, none]
    public void setDirection(String direction) {
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), direction));
        assertAcked(client.admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    // verify that the given nodeName exists in cluster
    public DiscoveryNode assertNodeInCluster(String nodeName) {
        Map<String, DiscoveryNode> nodes = client.admin().cluster().prepareState().get().getState().nodes().getNodes();
        DiscoveryNode discoveryNode = null;
        for (Map.Entry<String, DiscoveryNode> entry : nodes.entrySet()) {
            DiscoveryNode node = entry.getValue();
            if (node.getName().equals(nodeName)) {
                discoveryNode = node;
                break;
            }
        }
        assertNotNull(discoveryNode);
        return discoveryNode;
    }

    // returns a comma-separated list of node names excluding `except`
    private String allNodesExcept(String except) {
        StringBuilder exclude = new StringBuilder();
        DiscoveryNodes allNodes = client.admin().cluster().prepareState().get().getState().nodes();
        for (DiscoveryNode node : allNodes) {
            if (node.getName().equals(except) == false) {
                exclude.append(node.getName()).append(",");
            }
        }
        return exclude.toString();
    }

    // obtain decision for allocation/relocation of a shard to a given node
    private Decision getDecisionForTargetNode(
        DiscoveryNode targetNode,
        boolean isPrimary,
        boolean includeYesDecisions,
        boolean isRelocation
    ) {
        ClusterAllocationExplanation explanation = client.admin()
            .cluster()
            .prepareAllocationExplain()
            .setIndex(TEST_INDEX)
            .setShard(0)
            .setPrimary(isPrimary)
            .setIncludeYesDecisions(includeYesDecisions)
            .get()
            .getExplanation();

        Decision requiredDecision = null;
        List<NodeAllocationResult> nodeAllocationResults;
        if (isRelocation) {
            MoveDecision moveDecision = explanation.getShardAllocationDecision().getMoveDecision();
            nodeAllocationResults = moveDecision.getNodeDecisions();
        } else {
            AllocateUnassignedDecision allocateUnassignedDecision = explanation.getShardAllocationDecision().getAllocateDecision();
            nodeAllocationResults = allocateUnassignedDecision.getNodeDecisions();
        }

        for (NodeAllocationResult nodeAllocationResult : nodeAllocationResults) {
            if (nodeAllocationResult.getNode().equals(targetNode)) {
                for (Decision decision : nodeAllocationResult.getCanAllocateDecision().getDecisions()) {
                    if (decision.label().equals(NAME)) {
                        requiredDecision = decision;
                        break;
                    }
                }
            }
        }

        assertNotNull(requiredDecision);
        return requiredDecision;
    }

    // create a new test index
    public void prepareIndexWithoutReplica(Optional<String> name) {
        String indexName = name.orElse(TEST_INDEX);
        client.admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder().put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(null))
            )
            .execute()
            .actionGet();
    }

    public void prepareIndexWithAllocatedPrimary(DiscoveryNode primaryShardNode, Optional<String> name) {
        String indexName = name.orElse(TEST_INDEX);
        client.admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder().put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.routing.allocation.include._name", primaryShardNode.getName())
                    .put("index.routing.allocation.exclude._name", allNodesExcept(primaryShardNode.getName()))
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();

        ensureYellowAndNoInitializingShards(TEST_INDEX);

        logger.info(" --> verify allocation of primary shard");
        assertAllocation(true, primaryShardNode);

        logger.info(" --> verify non-allocation of replica shard");
        assertNonAllocation(false);
    }

    // get allocation and relocation decisions for all nodes
    private void prepareDecisions() {
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", allNodesExcept(null)))
            .execute()
            .actionGet();
    }

    private void attemptAllocation(String targetNodeName) {
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", targetNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(targetNodeName))
            )
            .execute()
            .actionGet();
    }

    private ShardRouting getShardRouting(boolean isPrimary) {
        IndexShardRoutingTable table = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable().index(TEST_INDEX).shard(0);
        return (isPrimary ? table.primaryShard() : table.replicaShards().get(0));
    }

    // verify that shard does not exist at targetNode
    private void assertNonAllocation(boolean isPrimary) {
        if (isPrimary) {
            ensureRed(TEST_INDEX);
        }
        else {
            ensureYellowAndNoInitializingShards(TEST_INDEX);
        }
        ShardRouting shardRouting = getShardRouting(isPrimary);
        assertFalse(shardRouting.active());
        assertNull(shardRouting.currentNodeId());
        assertEquals(ShardRoutingState.UNASSIGNED, shardRouting.state());
    }

    // verify that shard exists at targetNode
    private void assertAllocation(boolean isPrimary, DiscoveryNode targetNode) {
        ShardRouting shardRouting = getShardRouting(isPrimary);
        assertTrue(shardRouting.active());
        assertNotNull(shardRouting.currentNodeId());
        assertEquals(shardRouting.currentNodeId(), targetNode.getId());
    }

}
