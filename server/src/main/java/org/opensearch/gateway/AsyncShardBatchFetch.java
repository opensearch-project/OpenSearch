/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.store.ShardAttributes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of AsyncShardFetchAbstract with batching support.
 * cache will be created using ShardBatchCache class as that can handle the caching strategy correctly for a
 * batch of shards. Other necessary functions are also stored so cache can store or get the data for both primary
 * and replicas.
 *
 * @param <T> Response type of the transport action.
 * @param <V> Data type of shard level response.
 */
public abstract class AsyncShardBatchFetch<T extends BaseNodeResponse, V extends BaseShardResponse> extends AsyncShardFetch<T> {

    private final Consumer<ShardId> removeShardFromBatch;
    private final List<ShardId> failedShards;

    @SuppressWarnings("unchecked")
    AsyncShardBatchFetch(
        Logger logger,
        String type,
        Map<ShardId, ShardAttributes> shardAttributesMap,
        AsyncShardFetch.Lister<? extends BaseNodesResponse<T>, T> action,
        String batchId,
        Class<V> clazz,
        BiFunction<DiscoveryNode, Map<ShardId, V>, T> responseConstructor,
        Function<T, Map<ShardId, V>> shardsBatchDataGetter,
        Supplier<V> emptyResponseBuilder,
        Consumer<ShardId> handleFailedShard
    ) {
        super(logger, type, shardAttributesMap, action, batchId);
        this.removeShardFromBatch = handleFailedShard;
        this.failedShards = new ArrayList<>();
        this.cache = new ShardBatchCache<>(
            logger,
            type,
            shardAttributesMap,
            "BatchID=[" + batchId + "]",
            clazz,
            responseConstructor,
            shardsBatchDataGetter,
            emptyResponseBuilder,
            handleFailedShard
        );
    }

    /**
     * Fetch the data for a batch of shards, this uses the already written {@link AsyncShardFetch} fetchData method.
     * Based on the shards failed in last round, it makes sure to trigger a reroute for them.
     *
     * @param nodes       all the nodes where transport call should be sent
     * @param ignoreNodes nodes to update based on failures received from transport actions
     * @return data received from the transport actions
     */
    public synchronized FetchResult<T> fetchData(DiscoveryNodes nodes, Map<ShardId, Set<String>> ignoreNodes) {
        if (failedShards.isEmpty() == false) {
            // trigger a reroute if there are any shards failed, to make sure they're picked up in next run
            logger.trace("triggering another reroute for failed shards in {}", reroutingKey);
            reroute("shards-failed", "shards failed in " + reroutingKey);
        }
        return super.fetchData(nodes, ignoreNodes);
    }

    /**
     * Remove the shard from shardAttributesMap, so we don't send it in next fetching round.
     * Remove shard from the batch, so it gets picked up in a new batch in next reroute.
     *
     * @param shardId shardId to be cleaned up
     */
    private void cleanUpFailedShard(ShardId shardId) {
        shardAttributesMap.remove(shardId);
        removeShardFromBatch.accept(shardId);
        failedShards.add(shardId);
    }

    /**
     * Remove a shard from the cache maintaining a full batch of shards. This is needed to clear the shard once it's
     * assigned or failed.
     *
     * @param shardId shardId to be removed from the batch.
     */
    public void clearShard(ShardId shardId) {
        shardAttributesMap.remove(shardId);
        cache.deleteData(shardId);
    }
}
