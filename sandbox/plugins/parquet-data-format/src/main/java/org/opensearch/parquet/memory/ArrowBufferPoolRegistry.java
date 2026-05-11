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
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.parquet.ParquetSettings;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Plugin-scoped tracker for live {@link ArrowBufferPool} instances. Registers cluster-settings
 * listeners on the four allocation knobs once, then fans dynamic updates out to every pool
 * created since the listener registered.
 *
 * <p>Pools are held via a synchronized {@link WeakHashMap}-backed set so closed/GC'd pools drop
 * out automatically — there is no {@code removeSettingsUpdateConsumer} API to deregister
 * listeners per pool, and per-shard pools come and go.
 */
public class ArrowBufferPoolRegistry {

    private static final Logger logger = LogManager.getLogger(ArrowBufferPoolRegistry.class);

    private final ClusterSettings clusterSettings;
    private final Set<ArrowBufferPool> pools = Collections.newSetFromMap(Collections.synchronizedMap(new WeakHashMap<>()));
    /**
     * Latest known values for the four allocation knobs. Each listener updates one field then
     * fans out an {@link ArrowBufferPool#applyLimits(Settings)} call. We track them locally
     * because {@link ClusterSettings#get(org.opensearch.common.settings.Setting)} reads from
     * {@code lastSettingsApplied} which is written only after all updaters run.
     */
    private volatile String maxNativeAllocation;
    private volatile ByteSizeValue minNativeAllocation;
    private volatile ByteSizeValue maxNativeAllocationCeiling;
    private volatile ByteSizeValue childAllocation;

    /** Creates the registry, snapshots current setting values, and wires the listeners. */
    public ArrowBufferPoolRegistry(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
        // Snapshot initial values from the registered defaults so the first PUT sees a coherent
        // settings view. Subsequent listener fires update only the field that changed.
        this.maxNativeAllocation = clusterSettings.get(ParquetSettings.MAX_NATIVE_ALLOCATION);
        this.minNativeAllocation = clusterSettings.get(ParquetSettings.MIN_NATIVE_ALLOCATION);
        this.maxNativeAllocationCeiling = clusterSettings.get(ParquetSettings.MAX_NATIVE_ALLOCATION_CEILING);
        this.childAllocation = clusterSettings.get(ParquetSettings.CHILD_ALLOCATION);

        clusterSettings.addSettingsUpdateConsumer(ParquetSettings.MAX_NATIVE_ALLOCATION, v -> {
            this.maxNativeAllocation = v;
            applyToAllPools();
        });
        clusterSettings.addSettingsUpdateConsumer(ParquetSettings.MIN_NATIVE_ALLOCATION, v -> {
            this.minNativeAllocation = v;
            applyToAllPools();
        });
        clusterSettings.addSettingsUpdateConsumer(ParquetSettings.MAX_NATIVE_ALLOCATION_CEILING, v -> {
            this.maxNativeAllocationCeiling = v;
            applyToAllPools();
        });
        clusterSettings.addSettingsUpdateConsumer(ParquetSettings.CHILD_ALLOCATION, v -> {
            this.childAllocation = v;
            applyToAllPools();
        });
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

    /** Visible for tests — returns the underlying {@link ClusterSettings}. */
    ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    private void applyToAllPools() {
        Settings nodeSettings = currentSettings();
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

    /**
     * Builds a {@link Settings} view from the locally-tracked field values rather than from
     * {@link ClusterSettings#get}, which reads {@code lastSettingsApplied} — written only after
     * all update consumers run.
     */
    private Settings currentSettings() {
        return Settings.builder()
            .put(ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(), maxNativeAllocation)
            .put(ParquetSettings.MIN_NATIVE_ALLOCATION.getKey(), minNativeAllocation.getStringRep())
            .put(ParquetSettings.MAX_NATIVE_ALLOCATION_CEILING.getKey(), maxNativeAllocationCeiling.getStringRep())
            .put(ParquetSettings.CHILD_ALLOCATION.getKey(), childAllocation.getStringRep())
            .build();
    }
}
