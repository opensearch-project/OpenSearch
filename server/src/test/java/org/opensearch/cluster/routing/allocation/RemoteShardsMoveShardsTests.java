/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterStateHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.index.IndexModule;

public class RemoteShardsMoveShardsTests extends RemoteShardsBalancerBaseTestCase {

    /**
     * Test reroute terminates gracefully if shards cannot move out of the excluded node
     */
    public void testExcludeNodeIdMoveBlocked() {
        int localOnlyNodes = 7;
        int remoteCapableNodes = 2;
        int localIndices = 10;
        int remoteIndices = 13;
        ClusterState clusterState = createInitialCluster(localOnlyNodes, remoteCapableNodes, localIndices, remoteIndices);
        AllocationService service = this.createRemoteCapableAllocationService();
        clusterState = allocateShardsAndBalance(clusterState, service);
        assertEquals(ClusterHealthStatus.GREEN, (new ClusterStateHealth(clusterState)).getStatus());

        // Exclude a node
        final String excludedNodeID = getNodeId(0, true);
        service = createRemoteCapableAllocationService(excludedNodeID);
        clusterState = allocateShardsAndBalance(clusterState, service);

        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        assertEquals(ClusterHealthStatus.GREEN, (new ClusterStateHealth(clusterState)).getStatus());
        assertEquals(0, routingNodes.unassigned().size());
        assertTrue(routingNodes.node(excludedNodeID).size() > 0);
    }

    /**
     * Test move operations for index level allocation settings.
     * Supported for local indices and remote indices.
     */
    public void testIndexLevelExclusions() throws InterruptedException {
        int localOnlyNodes = 7;
        int remoteCapableNodes = 3;
        int localIndices = 10;
        int remoteIndices = 13;
        ClusterState clusterState = createInitialCluster(localOnlyNodes, remoteCapableNodes, localIndices, remoteIndices);
        AllocationService service = this.createRemoteCapableAllocationService();
        clusterState = allocateShardsAndBalance(clusterState, service);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertEquals(ClusterHealthStatus.GREEN, (new ClusterStateHealth(clusterState)).getStatus());
        assertEquals(0, routingNodes.unassigned().size());

        final String excludedLocalOnlyNode = getNodeId(0, false);
        final String excludedRemoteCapableNode = getNodeId(0, true);
        final String localIndex = routingNodes.node(excludedLocalOnlyNode).shardsWithState(ShardRoutingState.STARTED).get(0).getIndexName();
        final String remoteIndex = routingNodes.node(excludedRemoteCapableNode)
            .shardsWithState(ShardRoutingState.STARTED)
            .stream()
            .filter(shardRouting -> shardRouting.getIndexName().startsWith(REMOTE_IDX_PREFIX))
            .findFirst()
            .get()
            .getIndexName();

        Metadata.Builder mb = Metadata.builder(clusterState.metadata());
        mb.put(
            IndexMetadata.builder(clusterState.metadata().index(localIndex))
                .settings(
                    settings(Version.CURRENT).put("index.number_of_shards", PRIMARIES)
                        .put("index.number_of_replicas", REPLICAS)
                        .put("index.routing.allocation.exclude._name", excludedLocalOnlyNode)
                        .build()
                )
        );
        mb.put(
            IndexMetadata.builder(clusterState.metadata().index(remoteIndex))
                .settings(
                    settings(Version.CURRENT).put("index.number_of_shards", PRIMARIES)
                        .put("index.number_of_replicas", REPLICAS)
                        .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
                        .put("index.routing.allocation.exclude._name", excludedRemoteCapableNode)
                        .build()
                )
        );
        clusterState = ClusterState.builder(clusterState).metadata(mb.build()).build();

        clusterState = allocateShardsAndBalance(clusterState, service);
        assertEquals(ClusterHealthStatus.GREEN, (new ClusterStateHealth(clusterState)).getStatus());
        RoutingTable routingTable = clusterState.routingTable();

        // No shard of updated local index should be on excluded local capable node
        assertTrue(routingTable.allShards(localIndex).stream().noneMatch(shard -> shard.currentNodeId().equals(excludedLocalOnlyNode)));

        // No shard of updated remote index should be on excluded remote capable node
        assertTrue(
            routingTable.allShards(remoteIndex).stream().noneMatch(shard -> shard.currentNodeId().equals(excludedRemoteCapableNode))
        );
    }
}
