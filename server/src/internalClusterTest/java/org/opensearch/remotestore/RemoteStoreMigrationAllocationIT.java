/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.MoveDecision;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;


@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoteStoreMigrationAllocationIT extends OpenSearchIntegTestCase {

    private static final String TEST_INDEX = "test_index";

    private final static String REMOTE_STORE_DIRECTION = "remote_store";
    private final static String NONE_DIRECTION = "none";

    private final static String STRICT_MODE = "strict";
    private final static String MIXED_MODE = "mixed";


    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String REPOSITORY_2_NAME = "test-remote-store-repo-2";

    protected static final String NAME = "remote_store_migration";

    protected Path segmentRepoPath;
    protected Path translogRepoPath;

    static boolean addRemote = false;
    private final ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
    private  Client client;

    protected Settings nodeSettings (int nodeOrdinal) {
        if (segmentRepoPath == null || translogRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
            translogRepoPath = randomRepoPath().toAbsolutePath();
        }
        if (addRemote) {
            logger.info("Adding remote_store_enabled node");
            return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(remoteStoreClusterSettings(REPOSITORY_NAME, segmentRepoPath, REPOSITORY_2_NAME, translogRepoPath))
                .put("discovery.initial_state_timeout", "500ms")
                .build();
        } else {
            logger.info("Adding non_remote_store_enabled node");
            return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("discovery.initial_state_timeout", "500ms")
                .build();
        }
    }

    @Override
    protected Settings featureFlagSettings () {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL, "true").build();
    }


    // test cases for primary shard copy allocation with REMOTE_STORE direction

    public void testDontAllocateNewPrimaryShardOnNonRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> add non-remote node");
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);
        assertFalse(nonRemoteNode.isRemoteStoreNode());

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE_DIRECTION);

        logger.info(" --> verify expected decision for allocating a new primary shard on a non-remote node");
        prepareIndex(1, 0);
        Decision decision = getDecisionForTargetNode(nonRemoteNode, true, false, false);
        assertEquals(Decision.Type.NO, decision.type());
        assertEquals("[REMOTE_STORE migration_direction]: primary shard copy can not be allocated to a non_remote_store node", decision.getExplanation());

        logger.info(" --> attempt allocation");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", nonRemoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(nonRemoteNodeName))
            )
            .execute()
            .actionGet();

        ensureRed(TEST_INDEX);

        logger.info(" --> verify non-allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertNonAllocation(primaryShardRouting);
    }

    public void testAllocateNewPrimaryShardOnRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> set mixed cluster compatibility mode");
        setClusterMode(MIXED_MODE);

        logger.info(" --> add remote node");
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        assertTrue(remoteNode.isRemoteStoreNode());

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE_DIRECTION);

        logger.info(" --> verify expected decision for allocating a new primary shard on a remote node");
        prepareIndex(1, 0);
        Decision decision = getDecisionForTargetNode(remoteNode, true, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("[REMOTE_STORE migration_direction]: primary shard copy can be allocated to a remote_store node", decision.getExplanation());

        logger.info(" --> attempt allocation");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", remoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(remoteNodeName))
            )
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertAllocation(primaryShardRouting, remoteNode);
    }


    // test cases for replica shard copy allocation with REMOTE_STORE direction

    public void testDontAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnNonRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> set mixed cluster compatibility mode");
        setClusterMode(MIXED_MODE);

        logger.info(" --> add remote and non-remote nodes");
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        assertFalse(nonRemoteNode.isRemoteStoreNode());
        assertTrue(remoteNode.isRemoteStoreNode());

        logger.info(" --> allocate primary shard on non-remote node");
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.routing.allocation.include._name", nonRemoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(nonRemoteNodeName))
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertAllocation(primaryShardRouting, nonRemoteNode);

        logger.info(" --> verify non-allocation of replica shard");
        ShardRouting replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertNonAllocation(replicaShardRouting);

        ensureYellowAndNoInitializingShards(TEST_INDEX);

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE_DIRECTION);

        logger.info(" --> verify expected decision for replica shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(remoteNode, false, false, false);
        assertEquals(Decision.Type.NO, decision.type());
        assertEquals("[REMOTE_STORE migration_direction]: replica shard copy can not be allocated to a remote_store node since primary shard copy is not yet migrated to remote", decision.getExplanation());

        logger.info(" --> attempt allocation of replica shard on remote node");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", remoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(remoteNodeName))
            )
            .execute()
            .actionGet();

        ensureYellowAndNoInitializingShards(TEST_INDEX);

        logger.info(" --> verify non-allocation of replica shard");
        routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertNonAllocation(replicaShardRouting);
    }

    public void testAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> set mixed cluster compatibility mode");
        setClusterMode(MIXED_MODE);

        logger.info(" --> add remote nodes");
        addRemote = true;
        String remoteNodeName1 = internalCluster().startNode();
        String remoteNodeName2 = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode1 = assertNodeInCluster(remoteNodeName1);
        DiscoveryNode remoteNode2 = assertNodeInCluster(remoteNodeName2);
        assertTrue(remoteNode1.isRemoteStoreNode());
        assertTrue(remoteNode2.isRemoteStoreNode());

        logger.info(" --> allocate primary shard on remote node");
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.routing.allocation.include._name", remoteNodeName1)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(remoteNodeName1))
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertAllocation(primaryShardRouting, remoteNode1);

        logger.info(" --> verify non-allocation of replica shard");
        ShardRouting replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertNonAllocation(replicaShardRouting);

        ensureYellowAndNoInitializingShards(TEST_INDEX);

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE_DIRECTION);

        logger.info(" --> verify expected decision for replica shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(remoteNode2, false, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("[REMOTE_STORE migration_direction]: replica shard copy can be allocated to a remote_store node since primary shard copy has been migrated to remote", decision.getExplanation());

        logger.info(" --> attempt allocation of replica shard the other remote node");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", remoteNodeName2)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(remoteNodeName2))
            )
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of replica shard");
        routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertAllocation(replicaShardRouting, remoteNode2);
    }

    public void testAllocateNewReplicaShardOnNonRemoteNodeIfPrimaryShardOnNonRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> add non-remote nodes");
        addRemote = false;
        String nonRemoteNodeName1 = internalCluster().startNode();
        String nonRemoteNodeName2 = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode1 = assertNodeInCluster(nonRemoteNodeName1);
        DiscoveryNode nonRemoteNode2 = assertNodeInCluster(nonRemoteNodeName2);
        assertFalse(nonRemoteNode1.isRemoteStoreNode());
        assertFalse(nonRemoteNode2.isRemoteStoreNode());

        logger.info(" --> allocate primary shard on non-remote node");
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.routing.allocation.include._name", nonRemoteNodeName1)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(nonRemoteNodeName1))
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertAllocation(primaryShardRouting, nonRemoteNode1);

        logger.info(" --> verify non-allocation of replica shard");
        ShardRouting replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertNonAllocation(replicaShardRouting);

        ensureYellowAndNoInitializingShards(TEST_INDEX);

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE_DIRECTION);

        logger.info(" --> verify expected decision for replica shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(nonRemoteNode2, false, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("[REMOTE_STORE migration_direction]: replica shard copy can be allocated to a non_remote_store node", decision.getExplanation());

        logger.info(" --> allocate replica shard on the other non-remote node");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", nonRemoteNodeName2)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(nonRemoteNodeName2))
            )
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of replica shard");
        routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertAllocation(replicaShardRouting, nonRemoteNode2);
    }

    public void testAllocateNewReplicaShardOnNonRemoteNodeIfPrimaryShardOnRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> set mixed cluster compatibility mode");
        setClusterMode(MIXED_MODE);

        logger.info(" --> add remote and non-remote nodes");
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        assertFalse(nonRemoteNode.isRemoteStoreNode());
        assertTrue(remoteNode.isRemoteStoreNode());

        logger.info(" --> allocate primary on remote node");
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.routing.allocation.include._name", remoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(remoteNodeName))
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertAllocation(primaryShardRouting, remoteNode);

        logger.info(" --> verify non-allocation of replica shard");
        ShardRouting replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertNonAllocation(replicaShardRouting);

        ensureYellowAndNoInitializingShards(TEST_INDEX);

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE_DIRECTION);

        logger.info(" --> verify expected decision for replica shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(nonRemoteNode, false, true, false);

        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("[REMOTE_STORE migration_direction]: replica shard copy can be allocated to a non_remote_store node", decision.getExplanation());

        logger.info(" --> allocate replica shard on non-remote node");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", nonRemoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(nonRemoteNodeName))
            )
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of replica shard");
        routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertAllocation(replicaShardRouting, nonRemoteNode);
    }


    // bootstrap a cluster
    private void initializeCluster () {
        addRemote = false;
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        internalCluster().startNodes(1);
        client = internalCluster().client();
        setClusterMode(STRICT_MODE);
        setDirection(NONE_DIRECTION);
    }

    // set the compatibility mode of cluster to one of ["strict", "mixed"]
    private void setClusterMode (String mode) {
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), mode));
        assertAcked(client.admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    // set the migration direction for cluster to one of ["remote_store", "docrep", "none"]
    private void setDirection (String direction) {
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), direction));
        assertAcked(client.admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    // verify that the given nodeName exists in cluster
    private DiscoveryNode assertNodeInCluster (String nodeName) {
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
    private String allNodesExcept (String except) {
        StringBuilder exclude = new StringBuilder();
        DiscoveryNodes allNodes = client.admin().cluster().prepareState().get().getState().nodes();
        for (DiscoveryNode node: allNodes) {
            if (!(node.getName().equals(except))) {
                exclude.append(node.getName()).append(",");
            }
        }
        return exclude.toString();
    }

    // obtain decision for allocation/relocation of a shard to a given node
    private Decision getDecisionForTargetNode (DiscoveryNode targetNode, boolean isPrimary, boolean includeYesDecisions, boolean isRelocation) {
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
        }
        else {
            AllocateUnassignedDecision allocateUnassignedDecision = explanation.getShardAllocationDecision().getAllocateDecision();
            nodeAllocationResults = allocateUnassignedDecision.getNodeDecisions();
        }

        for (NodeAllocationResult nodeAllocationResult : nodeAllocationResults) {
            if (nodeAllocationResult.getNode().equals(targetNode)) {
                for (Decision decision: nodeAllocationResult.getCanAllocateDecision().getDecisions()) {
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
    private void prepareIndex (int shardCount, int replicaCount) {
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", shardCount)
                    .put("index.number_of_replicas", replicaCount)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(null))
            )
            .execute()
            .actionGet();
    }

    // get allocation and relocation decisions for all nodes
    private void prepareDecisions () {
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.exclude._name", allNodesExcept(null))
            )
            .execute()
            .actionGet();
    }

    // verify that shard does not exist at targetNode
    private void assertNonAllocation (ShardRouting shardRouting) {
        assertFalse(shardRouting.active());
        assertNull(shardRouting.currentNodeId());
        assertEquals(ShardRoutingState.UNASSIGNED, shardRouting.state());
    }

    // verify that shard exists at targetNode
    private void assertAllocation (ShardRouting shardRouting, DiscoveryNode targetNode) {
        assertTrue(shardRouting.active());
        assertNotNull(shardRouting.currentNodeId());
        assertEquals(shardRouting.currentNodeId(), targetNode.getId());
    }

}
