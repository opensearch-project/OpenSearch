/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.index.shard.ShardId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static java.util.Objects.isNull;

/**
 * Shard indexing pressure store acts as a central repository for all the shard-level tracker objects currently being
 * used at the Node level, for tracking indexing pressure requests.
 * Store manages the tracker lifecycle, from creation, access, until it is evicted to be collected.
 *
 * Trackers are maintained at two levels for access simplicity and better memory management:
 *
 * 1. shardIndexingPressureHotStore : As the name suggests, it is hot store for tracker objects which are currently live i.e. being used
 * to track an ongoing request.
 *
 * 2. shardIndexingPressureColdStore : This acts as the store for all the shard tracking objects which are currently being used
 * by the framework. In addition to hot trackers, the recently used trackers which are although not currently live, but again can be used
 * in near future, are also part of this store. To limit any memory implications, this store has an upper limit on the maximum number of
 * trackers its can hold at any given time, which is a configurable dynamic setting.
 *
 * Tracking objects when created are part of both the hot store as well as cold store. However, once the object
 * is no more live it is removed from the hot store. Objects in the cold store are evicted once the cold store
 * reaches its maximum limit. Think of it like a periodic purge when upper limit is hit.
 * During get if tracking object is not present in the hot store, a lookup is made into the cache store. If found,
 * object is brought into the hot store again, until it remains active. If not present in the either store, a fresh
 * object is instantiated and registered in both the stores for concurrent accesses.
 *
 * Note: The implementation of shardIndexingPressureColdStore methods is such that get,
 * update and evict operations can be abstracted out to support any other strategy such as LRU, if
 * discovered a need later.
 *
 * @opensearch.internal
 */
public class ShardIndexingPressureStore {

    // This represents the maximum value for the cold store size.
    public static final Setting<Integer> MAX_COLD_STORE_SIZE = Setting.intSetting(
        "shard_indexing_pressure.cache_store.max_size",
        200,
        100,
        1000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final Map<ShardId, ShardIndexingPressureTracker> shardIndexingPressureHotStore = ConcurrentCollections
        .newConcurrentMapWithAggressiveConcurrency();
    private final Map<ShardId, ShardIndexingPressureTracker> shardIndexingPressureColdStore = new HashMap<>();
    private final ShardIndexingPressureSettings shardIndexingPressureSettings;

    private volatile int maxColdStoreSize;

    public ShardIndexingPressureStore(
        ShardIndexingPressureSettings shardIndexingPressureSettings,
        ClusterSettings clusterSettings,
        Settings settings
    ) {
        this.shardIndexingPressureSettings = shardIndexingPressureSettings;
        this.maxColdStoreSize = MAX_COLD_STORE_SIZE.get(settings).intValue();
        clusterSettings.addSettingsUpdateConsumer(MAX_COLD_STORE_SIZE, this::setMaxColdStoreSize);
    }

    public ShardIndexingPressureTracker getShardIndexingPressureTracker(ShardId shardId) {
        ShardIndexingPressureTracker tracker = shardIndexingPressureHotStore.get(shardId);
        if (isNull(tracker)) {
            // Attempt from Indexing pressure cold store
            tracker = shardIndexingPressureColdStore.get(shardId);
            // If not already present in cold store instantiate a new one
            if (isNull(tracker)) {
                tracker = shardIndexingPressureHotStore.computeIfAbsent(
                    shardId,
                    (k) -> new ShardIndexingPressureTracker(
                        shardId,
                        this.shardIndexingPressureSettings.getShardPrimaryAndCoordinatingBaseLimits(),
                        this.shardIndexingPressureSettings.getShardReplicaBaseLimits()
                    )
                );
                // Write through into the cold store for future reference
                updateShardIndexingPressureColdStore(tracker);
            } else {
                // Attempt update tracker to the hot store and return the tracker which finally made to the hot store to avoid any race
                ShardIndexingPressureTracker newTracker = shardIndexingPressureHotStore.putIfAbsent(shardId, tracker);
                tracker = newTracker == null ? tracker : newTracker;
            }
        }
        return tracker;
    }

    public Map<ShardId, ShardIndexingPressureTracker> getShardIndexingPressureHotStore() {
        return Collections.unmodifiableMap(shardIndexingPressureHotStore);
    }

    public Map<ShardId, ShardIndexingPressureTracker> getShardIndexingPressureColdStore() {
        return Collections.unmodifiableMap(shardIndexingPressureColdStore);
    }

    public void tryTrackerCleanupFromHotStore(ShardIndexingPressureTracker tracker, BooleanSupplier condition) {
        if (condition.getAsBoolean()) {
            // Try inserting into cold store again in case there was an eviction triggered
            shardIndexingPressureColdStore.putIfAbsent(tracker.getShardId(), tracker);
            // Remove from the hot store
            shardIndexingPressureHotStore.remove(tracker.getShardId(), tracker);
        }
    }

    /**
     * This is used to update the reference of tracker in cold store, to be re-used later of tracker is removed from hot store upon request
     * completion. When the cold store size reaches maximum, all the tracker objects in cold store are flushed. Flush is a less frequent
     * (periodic) operation, can be sized based on workload. It is okay to not to synchronize counters being flushed, as
     * objects in the cold store are only empty references, and can be re-initialized if needed.
     */
    private void updateShardIndexingPressureColdStore(ShardIndexingPressureTracker tracker) {
        if (shardIndexingPressureColdStore.size() > maxColdStoreSize) {
            shardIndexingPressureColdStore.clear();
        }
        shardIndexingPressureColdStore.put(tracker.getShardId(), tracker);
    }

    private void setMaxColdStoreSize(int maxColdStoreSize) {
        this.maxColdStoreSize = maxColdStoreSize;
    }
}
