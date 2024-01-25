/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.store.ShardAttributes;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.gateway.AsyncShardFetchTestUtils.corruptShard;
import static org.opensearch.gateway.AsyncShardFetchTestUtils.prepareRequestMap;

public class TransportNodesListGatewayStartedShardsBatchIT extends OpenSearchIntegTestCase {

    public void testSingleShardFetch() throws Exception {
        String indexName = "test";
        int numOfShards = 1;
        prepareIndex(indexName, numOfShards);
        Map<ShardId, ShardAttributes> shardIdShardAttributesMap = prepareRequestMap(new String[] { indexName }, numOfShards);

        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards(indexName).get();

        TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch response;
        response = ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListGatewayStartedShardsBatch.class),
            new TransportNodesListGatewayStartedShardsBatch.Request(searchShardsResponse.getNodes(), shardIdShardAttributesMap)
        );
        final Index index = resolveIndex(indexName);
        final ShardId shardId = new ShardId(index, 0);
        TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard nodeGatewayStartedShards = response.getNodesMap()
            .get(searchShardsResponse.getNodes()[0].getId())
            .getNodeGatewayStartedShardsBatch()
            .get(shardId);
        assertNodeGatewayStartedShardsHappyCase(nodeGatewayStartedShards);
    }

    public void testShardFetchMultiNodeMultiIndexes() throws Exception {
        // start second node
        internalCluster().startNode();
        String indexName1 = "test1";
        String indexName2 = "test2";
        int numShards = internalCluster().numDataNodes();
        // assign one primary shard each to the data nodes
        prepareIndex(indexName1, numShards);
        prepareIndex(indexName2, numShards);
        Map<ShardId, ShardAttributes> shardIdShardAttributesMap = prepareRequestMap(new String[] { indexName1, indexName2 }, numShards);
        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards(indexName1, indexName2).get();
        assertEquals(internalCluster().numDataNodes(), searchShardsResponse.getNodes().length);
        TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch response;
        response = ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListGatewayStartedShardsBatch.class),
            new TransportNodesListGatewayStartedShardsBatch.Request(searchShardsResponse.getNodes(), shardIdShardAttributesMap)
        );
        for (ClusterSearchShardsGroup clusterSearchShardsGroup : searchShardsResponse.getGroups()) {
            ShardId shardId = clusterSearchShardsGroup.getShardId();
            assertEquals(1, clusterSearchShardsGroup.getShards().length);
            String nodeId = clusterSearchShardsGroup.getShards()[0].currentNodeId();
            TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard nodeGatewayStartedShards = response.getNodesMap()
                .get(nodeId)
                .getNodeGatewayStartedShardsBatch()
                .get(shardId);
            assertNodeGatewayStartedShardsHappyCase(nodeGatewayStartedShards);
        }
    }

    public void testShardFetchCorruptedShards() throws Exception {
        String indexName = "test";
        int numOfShards = 1;
        prepareIndex(indexName, numOfShards);
        Map<ShardId, ShardAttributes> shardIdShardAttributesMap = prepareRequestMap(new String[] { indexName }, numOfShards);
        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards(indexName).get();
        final Index index = resolveIndex(indexName);
        final ShardId shardId = new ShardId(index, 0);
        corruptShard(searchShardsResponse.getNodes()[0].getName(), shardId);
        TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch response;
        internalCluster().restartNode(searchShardsResponse.getNodes()[0].getName());
        response = ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListGatewayStartedShardsBatch.class),
            new TransportNodesListGatewayStartedShardsBatch.Request(getDiscoveryNodes(), shardIdShardAttributesMap)
        );
        DiscoveryNode[] discoveryNodes = getDiscoveryNodes();
        TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard nodeGatewayStartedShards = response.getNodesMap()
            .get(discoveryNodes[0].getId())
            .getNodeGatewayStartedShardsBatch()
            .get(shardId);
        assertNotNull(nodeGatewayStartedShards.storeException());
        assertNotNull(nodeGatewayStartedShards.allocationId());
        assertTrue(nodeGatewayStartedShards.primary());
    }

    private DiscoveryNode[] getDiscoveryNodes() throws ExecutionException, InterruptedException {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(false);
        clusterStateRequest.clear().nodes(true).routingTable(true).indices("*");
        ClusterStateResponse clusterStateResponse = client().admin().cluster().state(clusterStateRequest).get();
        final List<DiscoveryNode> nodes = new LinkedList<>(clusterStateResponse.getState().nodes().getDataNodes().values());
        DiscoveryNode[] disNodesArr = new DiscoveryNode[nodes.size()];
        nodes.toArray(disNodesArr);
        return disNodesArr;
    }

    private void assertNodeGatewayStartedShardsHappyCase(
        TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard nodeGatewayStartedShards
    ) {
        assertNull(nodeGatewayStartedShards.storeException());
        assertNotNull(nodeGatewayStartedShards.allocationId());
        assertTrue(nodeGatewayStartedShards.primary());
    }

    private void prepareIndex(String indexName, int numberOfPrimaryShards) {
        createIndex(
            indexName,
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numberOfPrimaryShards).put(SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        index(indexName, "type", "1");
        flush(indexName);
    }
}
