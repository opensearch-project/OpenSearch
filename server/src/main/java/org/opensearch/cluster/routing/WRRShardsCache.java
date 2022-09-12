/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.lease.Releasable;
import org.opensearch.index.shard.ShardId;

import java.util.List;

/**
 * The WRR shards cache allows caching shard ordering returned by Weighted round-robin scheduling policy ,helping with
 * improving similar requests.
 *
 * @opensearch.internal
 */

public class WRRShardsCache implements Releasable, ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(WRRShardsCache.class);

    private final Cache<Key, List<ShardRouting>> cache;

    public WRRShardsCache(ClusterService clusterService) {

        final long sizeInBytes = 2000000;
        CacheBuilder<Key, List<ShardRouting>> cacheBuilder = CacheBuilder.<Key, List<ShardRouting>>builder()
            .removalListener(notification -> logger.info("Object" + " {} removed from cache", notification.getKey().shardId))
            .setMaximumWeight(sizeInBytes);
        cache = cacheBuilder.build();
        clusterService.addListener(this);
    }

    public long hits() {
        return cache.stats().getHits();
    }

    public long misses() {
        return cache.stats().getMisses();
    }

    public long size() {
        return cache.count();
    }

    @Override
    public void close() {
        logger.debug("Invalidating WRRShardsCache on close");
        cache.invalidateAll();
    }

    /**
     * Listens to cluster state change event and invalidate cache on such events
     *
     * @param event cluster state change event
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        logger.debug("Invalidating WRRShardsCache on ClusterChangedEvent");
        cache.invalidateAll();
    }

    public List<ShardRouting> get(Key k) {
        return cache.get(k);
    }

    public void put(Key key, List<ShardRouting> value) {
        cache.put(key, value);
    }

    /**
     * Key for the WRRShardsCache
     *
     * @opensearch.internal
     */
    public static class Key {
        public final ShardId shardId;

        Key(ShardId shardId) {
            this.shardId = shardId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WRRShardsCache.Key key = (WRRShardsCache.Key) o;
            if (!shardId.equals(key.shardId)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = shardId.hashCode();
            return result;
        }

    }
}
