/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.SingleObjectCache;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM-based implementation of {@link NativeMemoryService} that collects jemalloc memory metrics
 * from the native (Rust) layer and caches them using a {@link SingleObjectCache} with a configurable TTL.
 * <p>
 * Follows the same lazy-refresh pattern as {@code OsService} and {@code JvmService}.
 * No background threads — the cache is refreshed on demand when the TTL expires.
 */
public class FfmNativeMemoryService implements NativeMemoryService {

    private static final Logger logger = LogManager.getLogger(FfmNativeMemoryService.class);

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "monitor.native_memory.refresh_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope
    );

    private static final MethodHandle ALLOCATED;
    private static final MethodHandle RESIDENT;

    static {
        SymbolLookup lookup = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        FunctionDescriptor desc = FunctionDescriptor.of(ValueLayout.JAVA_LONG);
        ALLOCATED = linker.downcallHandle(lookup.find("native_jemalloc_allocated_bytes").orElseThrow(), desc);
        RESIDENT = linker.downcallHandle(lookup.find("native_jemalloc_resident_bytes").orElseThrow(), desc);
    }

    private final SingleObjectCache<NativeMemoryStats> cache;

    public FfmNativeMemoryService(Settings settings) {
        TimeValue refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        this.cache = new NativeMemoryStatsCache(refreshInterval, fetchStats());
        logger.debug("using refresh_interval [{}]", refreshInterval);
    }

    /**
     * Returns the current native memory stats, potentially refreshing the cache if the TTL has expired.
     */
    @Override
    public NativeMemoryStats stats() {
        return cache.getOrRefresh();
    }

    /**
     * Invokes the FFM downcalls to fetch current jemalloc metrics.
     * If either call returns a negative value (error), logs a warning and returns
     * {@code NativeMemoryStats(-1, -1)}.
     */
    private NativeMemoryStats fetchStats() {
        try {
            long allocated = (long) ALLOCATED.invokeExact();
            long resident = (long) RESIDENT.invokeExact();
            if (allocated < 0 || resident < 0) {
                logger.warn("Native memory stats returned error: allocated={}, resident={}", allocated, resident);
                return new NativeMemoryStats(-1, -1);
            }
            return new NativeMemoryStats(allocated, resident);
        } catch (Throwable t) {
            logger.warn("Error fetching native memory stats", t);
            return new NativeMemoryStats(-1, -1);
        }
    }

    private class NativeMemoryStatsCache extends SingleObjectCache<NativeMemoryStats> {
        NativeMemoryStatsCache(TimeValue interval, NativeMemoryStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected NativeMemoryStats refresh() {
            return fetchStats();
        }
    }
}
