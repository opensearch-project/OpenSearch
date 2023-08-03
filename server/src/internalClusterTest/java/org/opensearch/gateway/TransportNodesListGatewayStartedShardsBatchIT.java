/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.gateway.AsyncShardFetchBatchTestUtils.corruptShard;
import static org.opensearch.gateway.AsyncShardFetchBatchTestUtils.getDiscoveryNodes;
import static org.opensearch.gateway.AsyncShardFetchBatchTestUtils.prepareRequestMap;

public class TransportNodesListGatewayStartedShardsBatchIT extends OpenSearchIntegTestCase {

    public void testSingleShardFetch() throws Exception {
        String indexName = "test";
        prepareIndices(new String[] { indexName }, 1, 0);
        DiscoveryNode[] nodes = getDiscoveryNodes();
        TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch nodesGatewayStartedShardsBatch = prepareAndSendRequest(
            new String[] { indexName },
            nodes
        );

        final Index index = resolveIndex(indexName);
        final ShardId shardId = new ShardId(index, 0);
        TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShards nodeGatewayStartedShards = nodesGatewayStartedShardsBatch
            .getNodesMap()
            .get(nodes[0].getId())
            .getNodeGatewayStartedShardsBatch()
            .get(shardId);
        assertNodeGatewayStartedShardsSuccessCase(nodeGatewayStartedShards);
    }

    public void testShardFetchMultiNodeMultiIndexes() throws Exception {
        // start second node
        internalCluster().startNode();
        String[] indices = new String[] { "index1", "index2" };
        prepareIndices(indices, 1, 0);
        DiscoveryNode[] nodes = getDiscoveryNodes();
        TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch nodesGatewayStartedShardsBatch = prepareAndSendRequest(
            indices,
            nodes
        );
        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards(indices).get();
        for (ClusterSearchShardsGroup clusterSearchShardsGroup : searchShardsResponse.getGroups()) {
            ShardId shardId = clusterSearchShardsGroup.getShardId();
            assertEquals(1, clusterSearchShardsGroup.getShards().length);
            String nodeId = clusterSearchShardsGroup.getShards()[0].currentNodeId();
            TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShards nodeGatewayStartedShards = nodesGatewayStartedShardsBatch
                .getNodesMap()
                .get(nodeId)
                .getNodeGatewayStartedShardsBatch()
                .get(shardId);
            assertNodeGatewayStartedShardsSuccessCase(nodeGatewayStartedShards);
        }
    }

    public void testShardFetchNodeNotConnected() {
        DiscoveryNode nonExistingNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        String indexName = "test";
        prepareIndices(new String[] { indexName }, 1, 0);
        TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch nodesGatewayStartedShardsBatch = prepareAndSendRequest(
            new String[] { indexName },
            new DiscoveryNode[] { nonExistingNode }
        );
        assertTrue(nodesGatewayStartedShardsBatch.hasFailures());
        assertEquals(1, nodesGatewayStartedShardsBatch.failures().size());
        assertEquals(nonExistingNode.getId(), nodesGatewayStartedShardsBatch.failures().get(0).nodeId());
    }

    public void testShardFetchCorruptedShards() throws Exception {
        String indexName = "test";
        prepareIndices(new String[] { indexName }, 1, 0);
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
        assertNodeGatewayStartedShardsFailureCase(nodeGatewayStartedShards);
    }

    private TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch prepareAndSendRequest(
        String[] indices,
        DiscoveryNode[] nodes
    ) {
        Map<ShardId, String> shardIdCustomDataPathMap = prepareRequestMap(indices, 1);
        TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch response;
        return ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListGatewayStartedShardsBatch.class),
            new TransportNodesListGatewayStartedShardsBatch.Request(nodes, shardIdCustomDataPathMap)
        );
    }

    private void assertNodeGatewayStartedShardsSuccessCase(
        TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShards nodeGatewayStartedShards
    ) {
        assertNull(nodeGatewayStartedShards.storeException());
        assertNotNull(nodeGatewayStartedShards.allocationId());
        assertTrue(nodeGatewayStartedShards.primary());
    }

    private void assertNodeGatewayStartedShardsFailureCase(
        TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShards nodeGatewayStartedShards
    ) {
        assertNotNull(nodeGatewayStartedShards.storeException());
        assertNotNull(nodeGatewayStartedShards.allocationId());
        assertTrue(nodeGatewayStartedShards.primary());
    }

    private void prepareIndices(String[] indices, int numberOfPrimaryShards, int numberOfReplicaShards) {
        for (String index : indices) {
            createIndex(
                index,
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, numberOfPrimaryShards)
                    .put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicaShards)
                    .build()
            );
            index(index, "type", "1");
            flush(index);
        }
    }

}
