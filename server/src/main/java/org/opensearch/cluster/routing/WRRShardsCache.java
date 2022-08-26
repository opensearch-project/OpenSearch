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

import java.util.ArrayList;

public class WRRShardsCache implements Releasable, ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(WRRShardsCache.class);

    private final Cache<Key, ArrayList<ShardRouting>> cache;

    public WRRShardsCache(ClusterService clusterService) {

        final long sizeInBytes = 2000000;
        CacheBuilder<Key, ArrayList<ShardRouting>> cacheBuilder = CacheBuilder.<Key, ArrayList<ShardRouting>>builder()
            .removalListener(notification -> logger.info("Object" + " {} removed from cache", notification.getKey().shardId))
            .setMaximumWeight(sizeInBytes);
        cache = cacheBuilder.build();
        clusterService.addListener(this);
    }

    @Override
    public void close() {
        logger.info("Invalidating WRRShardsCache on close");
        cache.invalidateAll();
    }

    public Cache<Key, ArrayList<ShardRouting>> getCache() {
        return cache;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        logger.info("Invalidating WRRShardsCache on ClusterChangedEvent");
        cache.invalidateAll();
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
