/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor.memory;

import org.opensearch.common.Nullable;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;

import java.util.function.Supplier;

/**
 * Unified memory reporting service that delegates to {@link JvmService} for JVM
 * stats and {@link NativeMemoryService} for native (jemalloc) stats.
 *
 * @opensearch.internal
 */
public class MemoryReportingService {

    private final JvmService jvmService;
    private final NativeMemoryService nativeMemoryService;

    /**
     * Constructs a new MemoryReportingService.
     *
     * @param jvmService          the JVM stats service
     * @param nativeMemoryService the native memory stats service
     */
    public MemoryReportingService(JvmService jvmService, NativeMemoryService nativeMemoryService) {
        this.jvmService = jvmService;
        this.nativeMemoryService = nativeMemoryService;
    }

    /**
     * Returns the current JVM memory statistics.
     */
    public JvmStats jvmStats() {
        return jvmService.stats();
    }

    /**
     * Returns the current native (jemalloc) memory statistics, or {@code null} if unavailable.
     */
    @Nullable
    public AnalyticsBackendNativeMemoryStats nativeStats() {
        return nativeMemoryService.stats();
    }

    /**
     * Sets the supplier for native memory statistics.
     *
     * @param supplier the supplier that provides native memory stats from the backend plugin
     */
    public void setNativeStatsSupplier(Supplier<AnalyticsBackendNativeMemoryStats> supplier) {
        nativeMemoryService.setStatsSupplier(supplier);
    }
}
