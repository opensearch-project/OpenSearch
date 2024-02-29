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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.store.ShardAttributes;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of AsyncShardFetchAbstract with batching support.
 * @param <T> Response type of the transport action.
 * @param <V> Data type of shard level response.
 */
public abstract class AsyncShardBatchFetch<T extends BaseNodeResponse, V extends BaseShardResponse>
    extends AsyncShardFetch<T>{

    @SuppressWarnings("unchecked")
    AsyncShardBatchFetch(
        Logger logger,
        String type,
        Map<ShardId, ShardAttributes> shardToCustomDataPath,
        AsyncShardFetch.Lister<? extends BaseNodesResponse<T>, T> action,
        String batchId,
        Class<V> clazz,
        BiFunction<DiscoveryNode, Map<ShardId, V>, T> responseGetter,
        Function<T, Map<ShardId, V>> shardsBatchDataGetter,
        Supplier<V> emptyResponseBuilder
    ) {
        super(logger, type, shardToCustomDataPath, action, batchId);
        this.shardCache = new ShardBatchCache<>(logger, type, shardToCustomDataPath, "BatchID=[" + batchId+ "]"
            , clazz, responseGetter, shardsBatchDataGetter, emptyResponseBuilder);
    }

    /**
     * Remove a shard from the cache maintaining a full batch of shards. This is needed to clear the shard once it's
     * assigned or failed.
     * @param shardId shardId to be removed from the batch.
     */
    public void clearShard(ShardId shardId) {
        this.shardAttributesMap.remove(shardId);
        this.cache.clearShardCache(shardId);
    }
}
