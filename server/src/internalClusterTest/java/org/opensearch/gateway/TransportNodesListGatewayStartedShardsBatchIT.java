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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

public class TransportNodesListGatewayStartedShardsBatchIT extends OpenSearchIntegTestCase {

    public void testSingleShardFetch() throws Exception {
        String indexName = "test";
        Map<ShardId, String> shardIdCustomDataPathMap = prepareRequestMap(new String[] { indexName }, 1);

        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards(indexName).get();

        TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch response;
        response = ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListGatewayStartedShardsBatch.class),
            new TransportNodesListGatewayStartedShardsBatch.Request(searchShardsResponse.getNodes(), shardIdCustomDataPathMap)
        );
        final Index index = resolveIndex(indexName);
        final ShardId shardId = new ShardId(index, 0);
        TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShards nodeGatewayStartedShards = response.getNodesMap()
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
        // assign one primary shard each to the data nodes
        Map<ShardId, String> shardIdCustomDataPathMap = prepareRequestMap(
            new String[] { indexName1, indexName2 },
            internalCluster().numDataNodes()
        );
        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards(indexName1, indexName2).get();
        assertEquals(internalCluster().numDataNodes(), searchShardsResponse.getNodes().length);
        TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch response;
        response = ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListGatewayStartedShardsBatch.class),
            new TransportNodesListGatewayStartedShardsBatch.Request(searchShardsResponse.getNodes(), shardIdCustomDataPathMap)
        );
        for (ClusterSearchShardsGroup clusterSearchShardsGroup : searchShardsResponse.getGroups()) {
            ShardId shardId = clusterSearchShardsGroup.getShardId();
            assertEquals(1, clusterSearchShardsGroup.getShards().length);
            String nodeId = clusterSearchShardsGroup.getShards()[0].currentNodeId();
            TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShards nodeGatewayStartedShards = response.getNodesMap()
                .get(nodeId)
                .getNodeGatewayStartedShardsBatch()
                .get(shardId);
            assertNodeGatewayStartedShardsHappyCase(nodeGatewayStartedShards);
        }
    }

    public void testShardFetchCorruptedShards() throws Exception {
        String indexName = "test";
        Map<ShardId, String> shardIdCustomDataPathMap = prepareRequestMap(new String[] { indexName }, 1);
        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards(indexName).get();
        final Index index = resolveIndex(indexName);
        final ShardId shardId = new ShardId(index, 0);
        corruptShard(searchShardsResponse.getNodes()[0].getName(), shardId);
        TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch response;
        internalCluster().restartNode(searchShardsResponse.getNodes()[0].getName());
        response = ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListGatewayStartedShardsBatch.class),
            new TransportNodesListGatewayStartedShardsBatch.Request(getDiscoveryNodes(), shardIdCustomDataPathMap)
        );
        DiscoveryNode[] discoveryNodes = getDiscoveryNodes();
        TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShards nodeGatewayStartedShards = response.getNodesMap()
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
        TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShards nodeGatewayStartedShards
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

    private Map<ShardId, String> prepareRequestMap(String[] indices, int primaryShardCount) {
        Map<ShardId, String> shardIdCustomDataPathMap = new HashMap<>();
        for (String indexName : indices) {
            prepareIndex(indexName, primaryShardCount);
            final Index index = resolveIndex(indexName);
            final String customDataPath = IndexMetadata.INDEX_DATA_PATH_SETTING.get(
                client().admin().indices().prepareGetSettings(indexName).get().getIndexToSettings().get(indexName)
            );
            for (int shardIdNum = 0; shardIdNum < primaryShardCount; shardIdNum++) {
                final ShardId shardId = new ShardId(index, shardIdNum);
                shardIdCustomDataPathMap.put(shardId, customDataPath);
            }
        }
        return shardIdCustomDataPathMap;
    }

    private void corruptShard(String nodeName, ShardId shardId) throws IOException, InterruptedException {
        for (Path path : internalCluster().getInstance(NodeEnvironment.class, nodeName).availableShardPaths(shardId)) {
            final Path indexPath = path.resolve(ShardPath.INDEX_FOLDER_NAME);
            if (Files.exists(indexPath)) { // multi data path might only have one path in use
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexPath)) {
                    for (Path item : stream) {
                        if (item.getFileName().toString().startsWith("segments_")) {
                            logger.info("--> deleting [{}]", item);
                            Files.delete(item);
                        }
                    }
                }
            }
        }
    }
}
