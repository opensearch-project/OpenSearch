/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.cluster.ClusterManagerMetrics;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.gateway.TransportNodesGatewayStartedShardHelper.GatewayStartedShard;
import org.opensearch.gateway.TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch;
import org.opensearch.indices.store.ShardAttributes;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardBatchCacheTests extends OpenSearchAllocationTestCase {
    private static final String BATCH_ID = "b1";
    private final DiscoveryNode node1 = newNode("node1");
    private final DiscoveryNode node2 = newNode("node2");
    private final Map<ShardId, ShardsBatchGatewayAllocator.ShardEntry> batchInfo = new HashMap<>();
    private AsyncShardBatchFetch.ShardBatchCache<NodeGatewayStartedShardsBatch, GatewayStartedShard> shardCache;
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
            GatewayStartedShard.class,
            new GatewayStartedShard(null, false, null, null),
            GatewayStartedShard::isEmpty,
            new ShardBatchResponseFactory<>(true),
            new ClusterManagerMetrics(NoopMetricsRegistry.INSTANCE)
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

        // assert that fetching is done as both node's responses are stored in cache
        assertFalse(this.shardCache.getCache().get(node1.getId()).isFetching());
        assertFalse(this.shardCache.getCache().get(node2.getId()).isFetching());

        Map<DiscoveryNode, NodeGatewayStartedShardsBatch> fetchData = shardCache.getCacheData(
            DiscoveryNodes.builder().add(node1).add(node2).build(),
            null
        );
        assertEquals(2, fetchData.size());
        assertEquals(10, fetchData.get(node1).getNodeGatewayStartedShardsBatch().size());
        assertEquals("alloc-1", fetchData.get(node1).getNodeGatewayStartedShardsBatch().get(shard).allocationId());

        assertEquals(10, fetchData.get(node2).getNodeGatewayStartedShardsBatch().size());
        assertTrue(GatewayStartedShard.isEmpty(fetchData.get(node2).getNodeGatewayStartedShardsBatch().get(shard)));

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

    public void testShardsDataWithException() {
        setupShardBatchCache(BATCH_ID, NUMBER_OF_SHARDS_DEFAULT);
        this.shardCache.initData(node1);
        this.shardCache.initData(node2);
        this.shardCache.markAsFetching(List.of(node1.getId(), node2.getId()), 1);
        this.shardCache.putData(node1, new NodeGatewayStartedShardsBatch(node1, getFailedPrimaryResponse(shardsInBatch, 5)));
        Map<DiscoveryNode, NodeGatewayStartedShardsBatch> fetchData = shardCache.getCacheData(
            DiscoveryNodes.builder().add(node1).add(node2).build(),
            null
        );

        assertEquals(10, batchInfo.size());
        assertEquals(2, fetchData.size());
        assertEquals(10, fetchData.get(node1).getNodeGatewayStartedShardsBatch().size());
        assertTrue(fetchData.get(node2).getNodeGatewayStartedShardsBatch().isEmpty());
    }

    private Map<ShardId, GatewayStartedShard> getPrimaryResponse(List<ShardId> shards, ResponseType responseType) {
        int allocationId = 1;
        Map<ShardId, GatewayStartedShard> shardData = new HashMap<>();
        for (ShardId shard : shards) {
            switch (responseType) {
                case NULL:
                    shardData.put(shard, null);
                    break;
                case EMPTY:
                    shardData.put(shard, new GatewayStartedShard(null, false, null, null));
                    break;
                case VALID:
                    shardData.put(shard, new GatewayStartedShard("alloc-" + allocationId++, false, null, null));
                    break;
                default:
                    throw new AssertionError("unknown response type");
            }
        }
        return shardData;
    }

    private Map<ShardId, GatewayStartedShard> getFailedPrimaryResponse(List<ShardId> shards, int failedShardsCount) {
        int allocationId = 1;
        Map<ShardId, GatewayStartedShard> shardData = new HashMap<>();
        for (ShardId shard : shards) {
            if (failedShardsCount-- > 0) {
                shardData.put(
                    shard,
                    new GatewayStartedShard("alloc-" + allocationId++, false, null, new OpenSearchRejectedExecutionException())
                );
            } else {
                shardData.put(shard, new GatewayStartedShard("alloc-" + allocationId++, false, null, null));
            }
        }
        return shardData;
    }

    private void fillShards(Map<ShardId, ShardAttributes> shardAttributesMap, int numberOfShards) {
        shardsInBatch = BatchTestUtil.setUpShards(numberOfShards);
        for (ShardId shardId : shardsInBatch) {
            ShardAttributes attr = new ShardAttributes("");
            shardAttributesMap.put(shardId, attr);
            batchInfo.put(
                shardId,
                new ShardsBatchGatewayAllocator.ShardEntry(attr, randomShardRouting(shardId.getIndexName(), shardId.id()))
            );
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
