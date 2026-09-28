/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.admin.tiering.status.model;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.storage.action.tiering.status.model.TieringStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TieringStatusTests extends OpenSearchTestCase {

    public void testTieringStatusConstructor() {
        String indexName = "test-index";
        String status = "RUNNING";
        String source = "hot";
        String target = "warm";
        long startTime = System.currentTimeMillis();

        TieringStatus tieringStatus = new TieringStatus(indexName, status, source, target, startTime);

        assertEquals(indexName, tieringStatus.getIndexName());
        assertEquals(status, tieringStatus.getStatus());
        assertEquals(source, tieringStatus.getSource());
        assertEquals(target, tieringStatus.getTarget());
        assertEquals(startTime, tieringStatus.getStartTime());
    }

    public void testTieringStatusSerialization() throws IOException {
        TieringStatus original = new TieringStatus("test-index", "RUNNING", "hot", "warm", 123456L);

        // Mock StreamOutput
        StreamOutput streamOutput = mock(StreamOutput.class);
        original.writeTo(streamOutput);

        verify(streamOutput).writeString("test-index");
        verify(streamOutput).writeString("RUNNING");
        verify(streamOutput).writeString("hot");
        verify(streamOutput).writeString("warm");
        verify(streamOutput).writeLong(123456L);
    }

    public void testShardLevelStatusCreation() {
        Map<String, Integer> counters = new HashMap<>();
        counters.put("pending", 2);
        counters.put("succeeded", 3);
        counters.put("running", 1);
        counters.put("total", 6);

        List<TieringStatus.OngoingShard> ongoingShards = Collections.singletonList(new TieringStatus.OngoingShard(1, "node2"));

        TieringStatus.ShardLevelStatus status = new TieringStatus.ShardLevelStatus(counters, ongoingShards);

        assertEquals(counters, status.getShardLevelCounters());
    }

    public void testFromRoutingTable() {
        // Test for WARM tier
        testForTier(true, true);
        // Test for HOT tier
        testForTier(false, true);

        // Test for detailed Flag as false
        testForTier(false, false);
    }

    private void testForTier(boolean isWarmTier, boolean isDetailedFlagEnabled) {
        // Mock ClusterState and its components
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);

        // Create node with appropriate roles
        Set<DiscoveryNodeRole> roles = new HashSet<>();
        if (isWarmTier) {
            roles.add(DiscoveryNodeRole.WARM_ROLE);
        } else {
            roles.add(DiscoveryNodeRole.DATA_ROLE);
        }

        DiscoveryNode node = new DiscoveryNode(
            "node-" + (isWarmTier ? "warm" : "hot"),
            buildNewFakeTransportAddress(),
            emptyMap(),
            roles,
            Version.CURRENT
        );

        RoutingTable.Builder routingTableBuilder = new RoutingTable.Builder().add(createRoutingTable(new Index("test-index", "na")));

        when(clusterState.routingTable()).thenReturn(routingTableBuilder.build());
        when(clusterState.getNodes()).thenReturn(nodes);
        when(nodes.get(any())).thenReturn(node);

        String targetTier = isWarmTier ? "WARM" : "HOT";
        TieringStatus.ShardLevelStatus status = TieringStatus.ShardLevelStatus.fromRoutingTable(
            clusterState,
            "test-index",
            false,
            targetTier
        );

        assertNotNull(status);
        Map<String, Integer> counters = status.getShardLevelCounters();

        // Verify counters
        assertTrue(counters.containsKey("total"));
        assertTrue(counters.containsKey("pending"));
        assertTrue(counters.containsKey("succeeded"));
        assertTrue(counters.containsKey("running"));
        if (isDetailedFlagEnabled) {
            assertFalse(counters.containsKey("ongoing_shards"));
        }

        // Additional assertions specific to tier if needed
        if (isWarmTier) {
            assertTrue(node.isWarmNode());
        } else {
            assertFalse(node.isWarmNode());
        }
    }

    public void testFromRoutingTable_WithMixedShardStates() {
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        Index index = new Index("test-index", "uuid");

        Set<DiscoveryNodeRole> warmRoles = new HashSet<>();
        warmRoles.add(DiscoveryNodeRole.WARM_ROLE);
        DiscoveryNode warmNode = new DiscoveryNode("warm-node", buildNewFakeTransportAddress(), emptyMap(), warmRoles, Version.CURRENT);

        Set<DiscoveryNodeRole> hotRoles = new HashSet<>();
        hotRoles.add(DiscoveryNodeRole.DATA_ROLE);
        DiscoveryNode hotNode = new DiscoveryNode("hot-node", buildNewFakeTransportAddress(), emptyMap(), hotRoles, Version.CURRENT);

        when(nodes.get("warm-node")).thenReturn(warmNode);
        when(nodes.get("hot-node")).thenReturn(hotNode);
        when(clusterState.getNodes()).thenReturn(nodes);

        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index);
        builder.addShard(TestShardRouting.newShardRouting(index.getName(), 0, "warm-node", true, ShardRoutingState.STARTED));
        builder.addShard(TestShardRouting.newShardRouting(index.getName(), 1, "hot-node", true, ShardRoutingState.STARTED));
        builder.addShard(TestShardRouting.newShardRouting(index.getName(), 2, null, true, ShardRoutingState.UNASSIGNED));
        builder.addShard(TestShardRouting.newShardRouting(index.getName(), 3, "hot-node", "warm-node", true, ShardRoutingState.RELOCATING));

        RoutingTable routingTable = new RoutingTable.Builder().add(builder.build()).build();
        when(clusterState.routingTable()).thenReturn(routingTable);

        TieringStatus.ShardLevelStatus status = TieringStatus.ShardLevelStatus.fromRoutingTable(clusterState, "test-index", true, "WARM");

        Map<String, Integer> counters = status.getShardLevelCounters();
        assertEquals(Integer.valueOf(1), counters.get("succeeded"));
        assertEquals(Integer.valueOf(2), counters.get("pending"));
        assertEquals(Integer.valueOf(1), counters.get("running"));
        assertEquals(Integer.valueOf(4), counters.get("total"));
        assertEquals(1, status.getOngoingShards().size());
    }

    public void testFromRoutingTable_HotTarget_StartedOnWarmIsPending() {
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        Index index = new Index("test-index", "uuid");

        Set<DiscoveryNodeRole> warmRoles = new HashSet<>();
        warmRoles.add(DiscoveryNodeRole.WARM_ROLE);
        DiscoveryNode warmNode = new DiscoveryNode("warm-node", buildNewFakeTransportAddress(), emptyMap(), warmRoles, Version.CURRENT);
        when(nodes.get("warm-node")).thenReturn(warmNode);
        when(clusterState.getNodes()).thenReturn(nodes);

        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index);
        builder.addShard(TestShardRouting.newShardRouting(index.getName(), 0, "warm-node", true, ShardRoutingState.STARTED));

        RoutingTable routingTable = new RoutingTable.Builder().add(builder.build()).build();
        when(clusterState.routingTable()).thenReturn(routingTable);

        TieringStatus.ShardLevelStatus status = TieringStatus.ShardLevelStatus.fromRoutingTable(clusterState, "test-index", false, "HOT");

        Map<String, Integer> counters = status.getShardLevelCounters();
        assertEquals(Integer.valueOf(0), counters.get("succeeded"));
        assertEquals(Integer.valueOf(1), counters.get("pending"));
    }

    public void testEquals() {
        TieringStatus s1 = new TieringStatus("idx", "RUNNING", "hot", "warm", 100L);
        TieringStatus s2 = new TieringStatus("idx", "RUNNING", "hot", "warm", 100L);
        TieringStatus s3 = new TieringStatus("other", "RUNNING", "hot", "warm", 100L);

        assertEquals(s1, s1);
        assertEquals(s1, s2);
        assertNotEquals(s1, s3);
        assertNotEquals(s1, null);
        assertNotEquals(s1, "string");
    }

    public void testHashCode() {
        TieringStatus s1 = new TieringStatus("idx", "RUNNING", "hot", "warm", 100L);
        TieringStatus s2 = new TieringStatus("idx", "RUNNING", "hot", "warm", 100L);
        assertEquals(s1.hashCode(), s2.hashCode());
    }

    public void testOngoingShardSerialization() throws IOException {
        TieringStatus.OngoingShard shard = new TieringStatus.OngoingShard(1, "node2");

        StreamOutput streamOutput = mock(StreamOutput.class);
        shard.writeTo(streamOutput);

        verify(streamOutput).writeInt(1);
        verify(streamOutput).writeString("node2");
    }

    private List<ShardRouting> createTestShards() {
        // Create and return test shard routings
        // This would be implementation-specific based on your needs
        return Collections.emptyList();
    }

    private IndexRoutingTable createRoutingTable(Index index) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index);
        ShardRouting primaryShard = TestShardRouting.newShardRouting(
            index.getName(),
            1,
            "node-2",
            "node-3",
            true,
            ShardRoutingState.RELOCATING
        );

        builder.addShard(primaryShard);
        ShardRouting replicaShard = TestShardRouting.newShardRouting(
            index.getName(),
            2,
            "node-2",
            "node-3",
            false,
            ShardRoutingState.RELOCATING
        );
        builder.addShard(replicaShard);

        return builder.build();
    }
}
