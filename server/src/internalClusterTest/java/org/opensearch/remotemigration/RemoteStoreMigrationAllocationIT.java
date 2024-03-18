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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreMigrationAllocationIT extends MigrationBaseTestCase {

    public static final String TEST_INDEX = "test_index";
    public static final String NAME = "remote_store_migration";

    private static final ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
    private Client client;

    public void testSample() {

    }

    // bootstrap a cluster
    private void initializeCluster(boolean remoteClusterManager) {
        addRemote = remoteClusterManager;
        internalCluster().startClusterManagerOnlyNode();
        client = internalCluster().client();
    }

    // set the compatibility mode of cluster [strict, mixed]
    public static void setClusterMode(String mode) {
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), mode));
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    // set the migration direction for cluster [remote_store, docrep, none]
    public static void setDirection(String direction) {
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), direction));
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    // verify that the given nodeName exists in cluster
    public static DiscoveryNode assertNodeInCluster(String nodeName) {
        Map<String, DiscoveryNode> nodes = internalCluster().client().admin().cluster().prepareState().get().getState().nodes().getNodes();
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
    public static String allNodesExcept(String except) {
        StringBuilder exclude = new StringBuilder();
        DiscoveryNodes allNodes = internalCluster().client().admin().cluster().prepareState().get().getState().nodes();
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
    public static void prepareIndexWithoutReplica(Optional<String> name) {
        String indexName = name.orElse(TEST_INDEX);
        internalCluster().client()
            .admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
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
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.routing.allocation.include._name", primaryShardNode.getName())
                    .put("index.routing.allocation.exclude._name", allNodesExcept(primaryShardNode.getName()))
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();

        ensureYellowAndNoInitializingShards(TEST_INDEX);

        logger.info(" --> verify allocation of primary shard");
        assertAllocation(true, Optional.of(primaryShardNode));

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

    private void attemptAllocation(Optional<String> targetNodeName) {
        String nodeName = targetNodeName.orElse(null);
        Settings.Builder settingsBuilder;
        if (nodeName != null) {
            settingsBuilder = Settings.builder()
                .put("index.routing.allocation.include._name", nodeName)
                .put("index.routing.allocation.exclude._name", allNodesExcept(nodeName));
        } else {
            String clusterManagerNodeName = client.admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .getNodes()
                .getClusterManagerNode()
                .getName();
            // to allocate freely among all nodes other than cluster-manager node
            settingsBuilder = Settings.builder()
                .put("index.routing.allocation.include._name", allNodesExcept(clusterManagerNodeName))
                .put("index.routing.allocation.exclude._name", clusterManagerNodeName);
        }
        client.admin().indices().prepareUpdateSettings(TEST_INDEX).setSettings(settingsBuilder).execute().actionGet();
    }

    private ShardRouting getShardRouting(boolean isPrimary) {
        IndexShardRoutingTable table = client.admin()
            .cluster()
            .prepareState()
            .execute()
            .actionGet()
            .getState()
            .getRoutingTable()
            .index(TEST_INDEX)
            .shard(0);
        return (isPrimary ? table.primaryShard() : table.replicaShards().get(0));
    }

    // verify that shard does not exist at targetNode
    private void assertNonAllocation(boolean isPrimary) {
        if (isPrimary) {
            ensureRed(TEST_INDEX);
        } else {
            ensureYellowAndNoInitializingShards(TEST_INDEX);
        }
        ShardRouting shardRouting = getShardRouting(isPrimary);
        assertFalse(shardRouting.active());
        assertNull(shardRouting.currentNodeId());
        assertEquals(ShardRoutingState.UNASSIGNED, shardRouting.state());
    }

    // verify that shard exists at targetNode
    private void assertAllocation(boolean isPrimary, Optional<DiscoveryNode> targetNode) {
        ShardRouting shardRouting = getShardRouting(isPrimary);
        assertTrue(shardRouting.active());
        assertNotNull(shardRouting.currentNodeId());
        DiscoveryNode node = targetNode.orElse(null);
        if (node != null) {
            assertEquals(shardRouting.currentNodeId(), node.getId());
        }
    }

}
