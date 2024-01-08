/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.store;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.gateway.AsyncShardFetchBatchTestUtils.corruptShard;
import static org.opensearch.gateway.AsyncShardFetchBatchTestUtils.getDiscoveryNodes;
import static org.opensearch.gateway.AsyncShardFetchBatchTestUtils.prepareRequestMap;

public class TransportNodesListShardStoreMetadataBatchIT extends OpenSearchIntegTestCase {

    public void testSingleShardStoreFetch() throws ExecutionException, InterruptedException {
        String indexName = "test";
        DiscoveryNode[] nodes = getDiscoveryNodes();
        TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch response = prepareAndSendRequest(
            new String[] { indexName },
            nodes
        );
        Index index = resolveIndex(indexName);
        ShardId shardId = new ShardId(index, 0);
        TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata nodeStoreFilesMetadata = response.getNodesMap()
            .get(nodes[0].getId())
            .getNodeStoreFilesMetadataBatch()
            .get(shardId);
        assertNodeStoreFilesMetadataSuccessCase(nodeStoreFilesMetadata, shardId);
    }

    public void testShardStoreFetchMultiNodeMultiIndexes() throws Exception {
        // start second node
        internalCluster().startNode();
        String indexName1 = "test1";
        String indexName2 = "test2";
        DiscoveryNode[] nodes = getDiscoveryNodes();
        TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch response = prepareAndSendRequest(
            new String[] { indexName1, indexName2 },
            nodes
        );
        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards(indexName1, indexName2).get();
        for (ClusterSearchShardsGroup clusterSearchShardsGroup : searchShardsResponse.getGroups()) {
            ShardId shardId = clusterSearchShardsGroup.getShardId();
            ShardRouting[] shardRoutings = clusterSearchShardsGroup.getShards();
            assertEquals(2, shardRoutings.length);
            for (ShardRouting shardRouting : shardRoutings) {
                TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata nodeStoreFilesMetadata = response.getNodesMap()
                    .get(shardRouting.currentNodeId())
                    .getNodeStoreFilesMetadataBatch()
                    .get(shardId);
                assertNodeStoreFilesMetadataSuccessCase(nodeStoreFilesMetadata, shardId);
            }
        }
    }

    public void testShardStoreFetchNodeNotConnected() {
        DiscoveryNode nonExistingNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        String indexName = "test";
        TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch response = prepareAndSendRequest(
            new String[] { indexName },
            new DiscoveryNode[] { nonExistingNode }
        );
        assertTrue(response.hasFailures());
        assertEquals(1, response.failures().size());
        assertEquals(nonExistingNode.getId(), response.failures().get(0).nodeId());
    }

    public void testShardStoreFetchCorruptedIndex() throws Exception {
        // start second node
        internalCluster().startNode();
        String indexName = "test";
        prepareIndices(new String[] { indexName }, 1, 1);
        Map<ShardId, ShardAttributes> shardAttributesMap = prepareRequestMap(new String[] { indexName }, 1);
        Index index = resolveIndex(indexName);
        ShardId shardId = new ShardId(index, 0);
        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards(indexName).get();
        assertEquals(2, searchShardsResponse.getNodes().length);
        corruptShard(searchShardsResponse.getNodes()[0].getName(), shardId);
        corruptShard(searchShardsResponse.getNodes()[1].getName(), shardId);
        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(false).get();
        DiscoveryNode[] discoveryNodes = getDiscoveryNodes();
        TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch response;
        response = ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListShardStoreMetadataBatch.class),
            new TransportNodesListShardStoreMetadataBatch.Request(shardAttributesMap, discoveryNodes)
        );
        TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata nodeStoreFilesMetadata = response.getNodesMap()
            .get(discoveryNodes[0].getId())
            .getNodeStoreFilesMetadataBatch()
            .get(shardId);
        assertNodeStoreFilesMetadataFailureCase(nodeStoreFilesMetadata, shardId);
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

    private TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch prepareAndSendRequest(
        String[] indices,
        DiscoveryNode[] nodes
    ) {
        Map<ShardId, ShardAttributes> shardAttributesMap = null;
        prepareIndices(indices, 1, 1);
        shardAttributesMap = prepareRequestMap(indices, 1);
        TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch response;
        return ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListShardStoreMetadataBatch.class),
            new TransportNodesListShardStoreMetadataBatch.Request(shardAttributesMap, nodes)
        );
    }

    private void assertNodeStoreFilesMetadataFailureCase(
        TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata nodeStoreFilesMetadata,
        ShardId shardId
    ) {
        assertNotNull(nodeStoreFilesMetadata.getStoreFileFetchException());
        StoreFilesMetadata storeFileMetadata = nodeStoreFilesMetadata.storeFilesMetadata();
        assertEquals(shardId, storeFileMetadata.shardId());
        assertTrue(storeFileMetadata.peerRecoveryRetentionLeases().isEmpty());
    }

    private void assertNodeStoreFilesMetadataSuccessCase(
        TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata nodeStoreFilesMetadata,
        ShardId shardId
    ) {
        assertNull(nodeStoreFilesMetadata.getStoreFileFetchException());
        StoreFilesMetadata storeFileMetadata = nodeStoreFilesMetadata.storeFilesMetadata();
        assertFalse(storeFileMetadata.isEmpty());
        assertEquals(shardId, storeFileMetadata.shardId());
        assertNotNull(storeFileMetadata.peerRecoveryRetentionLeases());
    }
}
