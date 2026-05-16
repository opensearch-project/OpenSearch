/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.memory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.parquet.ParquetSettings;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Plugin-scoped tracker for live {@link ArrowBufferPool} instances. Registers a cluster-settings
 * listener on {@link ParquetSettings#MAX_NATIVE_ALLOCATION} once, then fans dynamic updates out
 * to every pool created since the listener registered. The per-child allocator cap is derived
 * from the root inside {@link ArrowBufferPool}, so no second listener is needed.
 *
 * <p>Pools are held via a synchronized {@link WeakHashMap}-backed set so closed/GC'd pools drop
 * out automatically — there is no {@code removeSettingsUpdateConsumer} API to deregister
 * listeners per pool, and per-shard pools come and go.
 */
public class ArrowBufferPoolRegistry {

    private static final Logger logger = LogManager.getLogger(ArrowBufferPoolRegistry.class);

    private final Set<ArrowBufferPool> pools = Collections.newSetFromMap(Collections.synchronizedMap(new WeakHashMap<>()));

    /** Creates the registry and wires the listener for the root allocator setting. */
    public ArrowBufferPoolRegistry(ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(ParquetSettings.MAX_NATIVE_ALLOCATION, this::applyToAllPools);
    }

    /** Registers a pool to receive future limit updates. Safe to call from any thread. */
    public void register(ArrowBufferPool pool) {
        pools.add(pool);
    }

    /** Removes a pool from the registry. Idempotent; safe to call from {@code close()}. */
    public void unregister(ArrowBufferPool pool) {
        pools.remove(pool);
    }

    /** Visible for tests — returns the number of currently-tracked pools. */
    public int trackedPoolCount() {
        synchronized (pools) {
            return pools.size();
        }
    }

    private void applyToAllPools(String maxNativeAllocation) {
        // Build a Settings view from the listener-supplied value rather than ClusterSettings.get,
        // which reads lastSettingsApplied — written only after all update consumers run.
        Settings nodeSettings = Settings.builder().put(ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(), maxNativeAllocation).build();
        // Snapshot under the synchronized lock to avoid CME during iteration; applyLimits itself
        // touches Arrow allocator state and shouldn't be held under the lock.
        ArrowBufferPool[] snapshot;
        synchronized (pools) {
            snapshot = pools.toArray(new ArrowBufferPool[0]);
        }
        for (ArrowBufferPool pool : snapshot) {
            try {
                pool.applyLimits(nodeSettings);
            } catch (RuntimeException e) {
                logger.warn("Failed to apply Arrow allocator limits to a pool; continuing with others", e);
            }
        }
    }
}
