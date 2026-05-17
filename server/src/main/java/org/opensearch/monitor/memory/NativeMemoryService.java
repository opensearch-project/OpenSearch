/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor.memory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;

import java.util.function.Supplier;

/**
 * Service for native (jemalloc) memory statistics with refresh-interval caching.
 * <p>
 * Mirrors the caching pattern of {@link org.opensearch.monitor.jvm.JvmService}:
 * stats are fetched from the native layer at most once per refresh interval.
 *
 * @opensearch.internal
 */
public class NativeMemoryService {

    private static final Logger logger = LogManager.getLogger(NativeMemoryService.class);

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "monitor.native_memory.refresh_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope
    );

    private final TimeValue refreshInterval;
    @Nullable
    private volatile Supplier<AnalyticsBackendNativeMemoryStats> statsSupplier;
    private volatile AnalyticsBackendNativeMemoryStats cachedStats;
    private volatile long lastRefreshTimestamp;

    public NativeMemoryService(Settings settings) {
        this.refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        this.lastRefreshTimestamp = 0;

        logger.debug("using refresh_interval [{}]", refreshInterval);
    }

    public void setStatsSupplier(Supplier<AnalyticsBackendNativeMemoryStats> supplier) {
        this.statsSupplier = supplier;
    }

    @Nullable
    public synchronized AnalyticsBackendNativeMemoryStats stats() {
        if (statsSupplier == null) {
            return null;
        }
        if ((System.currentTimeMillis() - lastRefreshTimestamp) > refreshInterval.millis()) {
            AnalyticsBackendNativeMemoryStats fresh = statsSupplier.get();
            cachedStats = fresh != null ? fresh : new AnalyticsBackendNativeMemoryStats(-1, -1);
            lastRefreshTimestamp = System.currentTimeMillis();
        }
        return cachedStats;
    }
}
