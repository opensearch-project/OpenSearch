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
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;

import java.util.function.Supplier;

/**
 * Unified memory reporting service that provides both JVM and native memory statistics
 * with refresh-interval caching.
 *
 * @opensearch.internal
 */
public class MemoryReportingService {

    private static final Logger logger = LogManager.getLogger(MemoryReportingService.class);

    public static final Setting<TimeValue> NATIVE_MEMORY_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "monitor.native_memory.refresh_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope
    );

    private final JvmService jvmService;
    private final TimeValue nativeRefreshInterval;
    @Nullable
    private volatile Supplier<AnalyticsBackendNativeMemoryStats> nativeStatsSupplier;

    private volatile AnalyticsBackendNativeMemoryStats cachedNativeStats;
    private volatile long lastNativeRefreshTimestamp;

    public MemoryReportingService(Settings settings, JvmService jvmService) {
        this.jvmService = jvmService;
        this.nativeStatsSupplier = null;
        this.nativeRefreshInterval = NATIVE_MEMORY_REFRESH_INTERVAL_SETTING.get(settings);
        this.lastNativeRefreshTimestamp = 0;

        logger.debug("using native_memory refresh_interval [{}]", nativeRefreshInterval);
    }

    public void setNativeStatsSupplier(Supplier<AnalyticsBackendNativeMemoryStats> supplier) {
        this.nativeStatsSupplier = supplier;
    }

    public JvmStats jvmStats() {
        return jvmService.stats();
    }

    @Nullable
    public synchronized AnalyticsBackendNativeMemoryStats nativeStats() {
        if (nativeStatsSupplier == null) {
            return null;
        }
        if ((System.currentTimeMillis() - lastNativeRefreshTimestamp) > nativeRefreshInterval.millis()) {
            AnalyticsBackendNativeMemoryStats fresh = nativeStatsSupplier.get();
            cachedNativeStats = fresh != null ? fresh : new AnalyticsBackendNativeMemoryStats(-1, -1);
            lastNativeRefreshTimestamp = System.currentTimeMillis();
        }
        return cachedNativeStats;
    }
}
