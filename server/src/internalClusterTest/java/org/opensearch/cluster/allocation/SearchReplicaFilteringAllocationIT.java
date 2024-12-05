/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.allocation;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.cluster.routing.allocation.decider.SearchReplicaAllocationDecider.SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchReplicaFilteringAllocationIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL, Boolean.TRUE).build();
    }

    public void testSearchReplicaDedicatedIncludes() {
        List<String> nodesIds = internalCluster().startNodes(3);
        final String node_0 = nodesIds.get(0);
        final String node_1 = nodesIds.get(1);
        final String node_2 = nodesIds.get(2);
        assertEquals(3, cluster().size());

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", node_1 + "," + node_0)
            )
            .execute()
            .actionGet();

        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
                .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );
        ensureGreen("test");
        // ensure primary is not on node 0 or 1,
        IndexShardRoutingTable routingTable = getRoutingTable();
        assertEquals(node_2, getNodeName(routingTable.primaryShard().currentNodeId()));

        String existingSearchReplicaNode = getNodeName(routingTable.searchOnlyReplicas().get(0).currentNodeId());
        String emptyAllowedNode = existingSearchReplicaNode.equals(node_0) ? node_1 : node_0;

        // set the included nodes to the other open node, search replica should relocate to that node.
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", emptyAllowedNode))
            .execute()
            .actionGet();
        ensureGreen("test");

        routingTable = getRoutingTable();
        assertEquals(node_2, getNodeName(routingTable.primaryShard().currentNodeId()));
        assertEquals(emptyAllowedNode, getNodeName(routingTable.searchOnlyReplicas().get(0).currentNodeId()));
    }

    public void testSearchReplicaDedicatedIncludes_DoNotAssignToOtherNodes() {
        List<String> nodesIds = internalCluster().startNodes(3);
        final String node_0 = nodesIds.get(0);
        final String node_1 = nodesIds.get(1);
        final String node_2 = nodesIds.get(2);
        assertEquals(3, cluster().size());

        // set filter on 1 node and set search replica count to 2 - should leave 1 unassigned
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", node_1))
            .execute()
            .actionGet();

        logger.info("--> creating an index with no replicas");
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 2)
                .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );
        ensureYellowAndNoInitializingShards("test");
        IndexShardRoutingTable routingTable = getRoutingTable();
        assertEquals(2, routingTable.searchOnlyReplicas().size());
        List<ShardRouting> assignedSearchShards = routingTable.searchOnlyReplicas()
            .stream()
            .filter(ShardRouting::assignedToNode)
            .collect(Collectors.toList());
        assertEquals(1, assignedSearchShards.size());
        assertEquals(node_1, getNodeName(assignedSearchShards.get(0).currentNodeId()));
        assertEquals(1, routingTable.searchOnlyReplicas().stream().filter(ShardRouting::unassigned).count());
    }

    private IndexShardRoutingTable getRoutingTable() {
        IndexShardRoutingTable routingTable = getClusterState().routingTable().index("test").getShards().get(0);
        return routingTable;
    }

    private String getNodeName(String id) {
        return getClusterState().nodes().get(id).getName();
    }
}
