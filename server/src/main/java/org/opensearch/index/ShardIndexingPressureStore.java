/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.shard.ShardId;

import java.util.Collections;
import java.util.Map;

import static java.util.Objects.isNull;

/**
 * The Shard indexing pressure store acts as a central repository for all the shard-level tracking objects being
 * used at a node level in order to track indexing pressure. It manages the tracker lifecycle.
 *
 * The shardIndexingPressureHotStore is a primary (hot) store and holds all the shard tracking object which are
 * currently live i.e. they are performing request level tracking for in-flight requests.
 *
 * The shardIndexingPressureColdStore acts as the cold storage for all the shard tracking objects which were created,
 * but are not currently live i.e. they are not tracking any in-flight requests currently.
 *
 * Tracking objects when created are part of both the hot store as well as cold store. However, once the object
 * is no more live it is removed from the hot store. Objects in the cold store are evicted once the cold store
 * reaches its maximum limit. Think of it like a periodic archival purge.
 * During get if tracking object is not present in the hot store, a lookup is made into the cache store. If found,
 * object is brought into the hot store again, until it remains active. If not present in the either store, a fresh
 * object is instantiated an registered in both the stores.
 *
 * Note: The implementation of shardIndexingPressureColdStore methods is such that get,
 * update and evict are abstracted out such that LRU logic can be plugged into it, if discovered a need later.
 */
public class ShardIndexingPressureStore {

    // This represents the initial value of cold store size.
    public static final Setting<Integer> MAX_CACHE_STORE_SIZE =
        Setting.intSetting("shard_indexing_pressure.cache_store.max_size", 200, 100, 1000,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    private final Map<Long, ShardIndexingPressureTracker> shardIndexingPressureHotStore =
            ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    private final Map<Long, ShardIndexingPressureTracker> shardIndexingPressureColdStore =
            ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    private final ShardIndexingPressureSettings shardIndexingPressureSettings;

    private volatile int maxColdStoreSize;

    public ShardIndexingPressureStore(ShardIndexingPressureSettings shardIndexingPressureSettings,
                                      ClusterSettings clusterSettings, Settings settings) {
        this.shardIndexingPressureSettings = shardIndexingPressureSettings;
        this.maxColdStoreSize = MAX_CACHE_STORE_SIZE.get(settings).intValue();
        clusterSettings.addSettingsUpdateConsumer(MAX_CACHE_STORE_SIZE, this::setMaxColdStoreSize);
    }

    public ShardIndexingPressureTracker getShardIndexingPressureTracker(ShardId shardId) {
        ShardIndexingPressureTracker tracker = shardIndexingPressureHotStore.get((long)shardId.hashCode());
        if (isNull(tracker)) {
            // Attempt from Indexing pressure cold store
            tracker = shardIndexingPressureColdStore.get((long)shardId.hashCode());
            // If not present in cold store so instantiate a new one
            if (isNull(tracker)) {
                ShardIndexingPressureTracker newShardIndexingPressureTracker = new ShardIndexingPressureTracker(shardId,
                    this.shardIndexingPressureSettings.getShardPrimaryAndCoordinatingBaseLimits(),
                    this.shardIndexingPressureSettings.getShardReplicaBaseLimits());
                // Try update the new shard stat to the hot store
                tracker = shardIndexingPressureHotStore.putIfAbsent((long) shardId.hashCode(), newShardIndexingPressureTracker);
                // Update the tracker so that we use the one actual in the hot store
                tracker = tracker == null ? newShardIndexingPressureTracker : tracker;
                // Write through into the cold store for future reference
                updateIndexingPressureColdStore(tracker);
            } else {
                // Attempt update tracker to the primary store and return tracker finally in the store to avoid any race
                ShardIndexingPressureTracker newTracker = shardIndexingPressureHotStore.putIfAbsent((long) shardId.hashCode(), tracker);
                tracker = newTracker == null ? tracker : newTracker;
            }
        }
        return tracker;
    }

    public Map<Long, ShardIndexingPressureTracker> getShardIndexingPressureHotStore() {
        return Collections.unmodifiableMap(shardIndexingPressureHotStore);
    }

    public Map<Long, ShardIndexingPressureTracker> getShardIndexingPressureColdStore() {
        return Collections.unmodifiableMap(shardIndexingPressureColdStore);
    }

    public void tryIndexingPressureTrackerCleanup(ShardIndexingPressureTracker tracker) {
        if (tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().get() == 0 &&
            tracker.memory().getCurrentReplicaBytes().get() == 0) {
            // Try inserting into cache again in case there was an eviction earlier
            shardIndexingPressureColdStore.putIfAbsent((long)tracker.getShardId().hashCode(), tracker);
            // Remove from the active store
            shardIndexingPressureHotStore.remove((long)tracker.getShardId().hashCode(), tracker);
        }
    }

    private void updateIndexingPressureColdStore(ShardIndexingPressureTracker tracker) {
        if (shardIndexingPressureColdStore.size() > maxColdStoreSize) {
            shardIndexingPressureColdStore.clear();
        }
        shardIndexingPressureColdStore.put((long)tracker.getShardId().hashCode(), tracker);
    }

    private void setMaxColdStoreSize(int maxColdStoreSize) {
        this.maxColdStoreSize = maxColdStoreSize;
    }

}
