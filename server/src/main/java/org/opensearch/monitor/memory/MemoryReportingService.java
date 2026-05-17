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

    public MemoryReportingService(JvmService jvmService, NativeMemoryService nativeMemoryService) {
        this.jvmService = jvmService;
        this.nativeMemoryService = nativeMemoryService;
    }

    public JvmStats jvmStats() {
        return jvmService.stats();
    }

    @Nullable
    public AnalyticsBackendNativeMemoryStats nativeStats() {
        return nativeMemoryService.stats();
    }

    public void setNativeStatsSupplier(Supplier<AnalyticsBackendNativeMemoryStats> supplier) {
        nativeMemoryService.setStatsSupplier(supplier);
    }
}
