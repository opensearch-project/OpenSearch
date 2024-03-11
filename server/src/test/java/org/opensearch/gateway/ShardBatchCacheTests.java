/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.gateway.TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard;
import org.opensearch.gateway.TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch;
import org.opensearch.indices.store.ShardAttributes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardBatchCacheTests extends OpenSearchAllocationTestCase {
    private static final String BATCH_ID = "b1";
    private final DiscoveryNode node1 = newNode("node1");
    private final DiscoveryNode node2 = newNode("node2");
    // Needs to be enabled once ShardsBatchGatewayAllocator is pushed
    // private final Map<ShardId, ShardsBatchGatewayAllocator.ShardEntry> batchInfo = new HashMap<>();
    private AsyncShardBatchFetch.ShardBatchCache<NodeGatewayStartedShardsBatch, NodeGatewayStartedShard> shardCache;
    private List<ShardId> shardsInBatch = new ArrayList<>();
    private static final int NUMBER_OF_SHARDS_DEFAULT = 10;

    private enum ResponseType {
        NULL,
        EMPTY,
        FAILURE,
        VALID
    }

    public void setupShardBatchCache(String batchId, int numberOfShards) {
        Map<ShardId, ShardAttributes> shardAttributesMap = new HashMap<>();
        fillShards(shardAttributesMap, numberOfShards);
        this.shardCache = new AsyncShardBatchFetch.ShardBatchCache<>(
            logger,
            "batch_shards_started",
            shardAttributesMap,
            "BatchID=[" + batchId + "]",
            NodeGatewayStartedShard.class,
            NodeGatewayStartedShardsBatch::new,
            NodeGatewayStartedShardsBatch::getNodeGatewayStartedShardsBatch,
            () -> new NodeGatewayStartedShard(null, false, null, null),
            this::removeShard
        );
    }

    public void testClearShardCache() {
        setupShardBatchCache(BATCH_ID, NUMBER_OF_SHARDS_DEFAULT);
        ShardId shard = shardsInBatch.iterator().next();
        this.shardCache.initData(node1);
        this.shardCache.markAsFetching(List.of(node1.getId()), 1);
        this.shardCache.putData(node1, new NodeGatewayStartedShardsBatch(node1, getPrimaryResponse(shardsInBatch, ResponseType.EMPTY)));
        assertTrue(
            this.shardCache.getCacheData(DiscoveryNodes.builder().add(node1).build(), null)
                .get(node1)
                .getNodeGatewayStartedShardsBatch()
                .containsKey(shard)
        );
        this.shardCache.deleteShard(shard);
        assertFalse(
            this.shardCache.getCacheData(DiscoveryNodes.builder().add(node1).build(), null)
                .get(node1)
                .getNodeGatewayStartedShardsBatch()
                .containsKey(shard)
        );
    }

    public void testGetCacheData() {
        setupShardBatchCache(BATCH_ID, NUMBER_OF_SHARDS_DEFAULT);
        ShardId shard = shardsInBatch.iterator().next();
        this.shardCache.initData(node1);
        this.shardCache.initData(node2);
        this.shardCache.markAsFetching(List.of(node1.getId(), node2.getId()), 1);
        this.shardCache.putData(node1, new NodeGatewayStartedShardsBatch(node1, getPrimaryResponse(shardsInBatch, ResponseType.EMPTY)));
        assertTrue(
            this.shardCache.getCacheData(DiscoveryNodes.builder().add(node1).build(), null)
                .get(node1)
                .getNodeGatewayStartedShardsBatch()
                .containsKey(shard)
        );
        assertTrue(
            this.shardCache.getCacheData(DiscoveryNodes.builder().add(node2).build(), null)
                .get(node2)
                .getNodeGatewayStartedShardsBatch()
                .isEmpty()
        );
    }

    public void testInitCacheData() {
        setupShardBatchCache(BATCH_ID, NUMBER_OF_SHARDS_DEFAULT);
        this.shardCache.initData(node1);
        this.shardCache.initData(node2);
        assertEquals(2, shardCache.getCache().size());

        // test getData without fetch
        assertTrue(shardCache.getData(node1).getNodeGatewayStartedShardsBatch().isEmpty());
    }

    public void testPutData() {
        // test empty and non-empty responses
        setupShardBatchCache(BATCH_ID, NUMBER_OF_SHARDS_DEFAULT);
        ShardId shard = shardsInBatch.iterator().next();
        this.shardCache.initData(node1);
        this.shardCache.initData(node2);
        this.shardCache.markAsFetching(List.of(node1.getId(), node2.getId()), 1);
        this.shardCache.putData(node1, new NodeGatewayStartedShardsBatch(node1, getPrimaryResponse(shardsInBatch, ResponseType.VALID)));
        this.shardCache.putData(node2, new NodeGatewayStartedShardsBatch(node1, getPrimaryResponse(shardsInBatch, ResponseType.EMPTY)));

        Map<DiscoveryNode, NodeGatewayStartedShardsBatch> fetchData = shardCache.getCacheData(
            DiscoveryNodes.builder().add(node1).add(node2).build(),
            null
        );
        assertEquals(2, fetchData.size());
        assertEquals(10, fetchData.get(node1).getNodeGatewayStartedShardsBatch().size());
        assertEquals("alloc-1", fetchData.get(node1).getNodeGatewayStartedShardsBatch().get(shard).allocationId());

        assertEquals(10, fetchData.get(node2).getNodeGatewayStartedShardsBatch().size());
        assertTrue(fetchData.get(node2).getNodeGatewayStartedShardsBatch().get(shard).isEmpty());

        // test GetData after fetch
        assertEquals(10, shardCache.getData(node1).getNodeGatewayStartedShardsBatch().size());
    }

    public void testNullResponses() {
        setupShardBatchCache(BATCH_ID, NUMBER_OF_SHARDS_DEFAULT);
        this.shardCache.initData(node1);
        this.shardCache.markAsFetching(List.of(node1.getId()), 1);
        this.shardCache.putData(node1, new NodeGatewayStartedShardsBatch(node1, getPrimaryResponse(shardsInBatch, ResponseType.NULL)));

        Map<DiscoveryNode, NodeGatewayStartedShardsBatch> fetchData = shardCache.getCacheData(
            DiscoveryNodes.builder().add(node1).build(),
            null
        );
        assertTrue(fetchData.get(node1).getNodeGatewayStartedShardsBatch().isEmpty());
    }

    public void testFilterFailedShards() {
        setupShardBatchCache(BATCH_ID, NUMBER_OF_SHARDS_DEFAULT);
        this.shardCache.initData(node1);
        this.shardCache.initData(node2);
        this.shardCache.markAsFetching(List.of(node1.getId(), node2.getId()), 1);
        this.shardCache.putData(node1, new NodeGatewayStartedShardsBatch(node1, getFailedPrimaryResponse(shardsInBatch, 5)));
        Map<DiscoveryNode, NodeGatewayStartedShardsBatch> fetchData = shardCache.getCacheData(
            DiscoveryNodes.builder().add(node1).add(node2).build(),
            null
        );

        // assertEquals(5, batchInfo.size());
        assertEquals(2, fetchData.size());
        assertEquals(5, fetchData.get(node1).getNodeGatewayStartedShardsBatch().size());
        assertTrue(fetchData.get(node2).getNodeGatewayStartedShardsBatch().isEmpty());
    }

    private Map<ShardId, NodeGatewayStartedShard> getPrimaryResponse(List<ShardId> shards, ResponseType responseType) {
        int allocationId = 1;
        Map<ShardId, NodeGatewayStartedShard> shardData = new HashMap<>();
        for (ShardId shard : shards) {
            switch (responseType) {
                case NULL:
                    shardData.put(shard, null);
                    break;
                case EMPTY:
                    shardData.put(shard, new NodeGatewayStartedShard(null, false, null, null));
                    break;
                case VALID:
                    shardData.put(shard, new NodeGatewayStartedShard("alloc-" + allocationId++, false, null, null));
                    break;
                default:
                    throw new AssertionError("unknown response type");
            }
        }
        return shardData;
    }

    private Map<ShardId, NodeGatewayStartedShard> getFailedPrimaryResponse(List<ShardId> shards, int failedShardsCount) {
        int allocationId = 1;
        Map<ShardId, NodeGatewayStartedShard> shardData = new HashMap<>();
        for (ShardId shard : shards) {
            if (failedShardsCount-- > 0) {
                shardData.put(
                    shard,
                    new NodeGatewayStartedShard("alloc-" + allocationId++, false, null, new OpenSearchRejectedExecutionException())
                );
            } else {
                shardData.put(shard, new NodeGatewayStartedShard("alloc-" + allocationId++, false, null, null));
            }
        }
        return shardData;
    }

    public void removeShard(ShardId shardId) {
        // batchInfo.remove(shardId);
    }

    private void fillShards(Map<ShardId, ShardAttributes> shardAttributesMap, int numberOfShards) {
        shardsInBatch = BatchTestUtil.setUpShards(numberOfShards);
        for (ShardId shardId : shardsInBatch) {
            ShardAttributes attr = new ShardAttributes("");
            shardAttributesMap.put(shardId, attr);
            // batchInfo.put(
            // shardId,
            // new ShardsBatchGatewayAllocator.ShardEntry(attr, randomShardRouting(shardId.getIndexName(), shardId.id()))
            // );
        }
    }

    private ShardRouting randomShardRouting(String index, int shard) {
        ShardRoutingState state = randomFrom(ShardRoutingState.values());
        return TestShardRouting.newShardRouting(
            index,
            shard,
            state == ShardRoutingState.UNASSIGNED ? null : "1",
            state == ShardRoutingState.RELOCATING ? "2" : null,
            state != ShardRoutingState.UNASSIGNED && randomBoolean(),
            state
        );
    }
}
