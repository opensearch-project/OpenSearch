/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.nativebridge.spi.NativeAllocatorConfig;
import org.opensearch.nativebridge.spi.NativeArenaPurger;
import org.opensearch.nativebridge.spi.NativeHeapProfiler;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;
import org.opensearch.nativebridge.spi.NativeMemoryFetcher;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Always-loaded module that manages runtime tuning for the native (Rust/FFM) layer.
 * <p>
 * Registers dynamic cluster settings and applies changes at runtime via the FFM bridge.
 * Also registers the NativeHeapProfiler JMX MBean for on-demand heap profiling via
 * the opensearch-heap-prof CLI tool.
 */
public class NativeBridgeModule extends Plugin {

    private static final Logger logger = LogManager.getLogger(NativeBridgeModule.class);

    private volatile long currentNativeLimitBytes;
    private volatile int currentThresholdPercent;

    /** jemalloc dirty page decay time (ms). Dynamically tunable — applied to all arenas at runtime. */
    public static final Setting<Long> JEMALLOC_DIRTY_DECAY_MS = Setting.longSetting(
        "native.jemalloc.dirty_decay_ms",
        30_000L,
        -1L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** jemalloc muzzy page decay time (ms). Dynamically tunable — applied to all arenas at runtime. */
    public static final Setting<Long> JEMALLOC_MUZZY_DECAY_MS = Setting.longSetting(
        "native.jemalloc.muzzy_decay_ms",
        30_000L,
        -1L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How often the purge thread checks if resident memory exceeds the threshold.
     * Set to 0 to disable periodic purging entirely.
     */
    public static final Setting<TimeValue> JEMALLOC_PURGE_INTERVAL = Setting.timeSetting(
        "native.jemalloc.purge_interval",
        TimeValue.timeValueSeconds(5),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Purge fires only when jemalloc resident exceeds this percentage of
     * {@code node.native_memory.limit}. Default: 85%.
     */
    public static final Setting<Integer> JEMALLOC_PURGE_THRESHOLD_PERCENT = Setting.intSetting(
        "native.jemalloc.purge_threshold_percent",
        85,
        0,
        100,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private long computeThresholdBytes() {
        return currentNativeLimitBytes > 0 ? currentNativeLimitBytes * currentThresholdPercent / 100 : Long.MAX_VALUE;
    }

    public AnalyticsBackendNativeMemoryStats memoryStats() {
        if (!NativeLibraryLoader.isLoaded()) {
            return null;
        }
        return NativeMemoryFetcher.fetch();
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        Settings settings = environment.settings();

        // Compute purge threshold and start the Rust-side background purge thread
        this.currentNativeLimitBytes = ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(settings).getBytes();
        this.currentThresholdPercent = JEMALLOC_PURGE_THRESHOLD_PERCENT.get(settings);
        NativeArenaPurger.init(computeThresholdBytes(), JEMALLOC_PURGE_INTERVAL.get(settings).millis());

        // Register the heap profiler MBean first — this is pure Java and always works
        java.util.List<String> allowedDirs = new java.util.ArrayList<>();
        allowedDirs.add(environment.dataFiles()[0].toAbsolutePath().toString());
        // Additional paths configurable via system property (set in jvm.options)
        String extraPaths = System.getProperty("native.heap_prof.allowed_paths", "");
        if (!extraPaths.isEmpty()) {
            for (String p : extraPaths.split(",")) {
                if (!p.trim().isEmpty()) {
                    allowedDirs.add(p.trim());
                }
            }
        }
        NativeHeapProfiler.setAllowedDumpDirs(allowedDirs);
        NativeHeapProfiler.register();

        // Apply initial allocator values — requires native library to be loaded
        try {
            NativeAllocatorConfig.setDirtyDecayMs(JEMALLOC_DIRTY_DECAY_MS.get(settings));
            NativeAllocatorConfig.setMuzzyDecayMs(JEMALLOC_MUZZY_DECAY_MS.get(settings));

            // Register dynamic update listeners
            clusterService.getClusterSettings().addSettingsUpdateConsumer(JEMALLOC_DIRTY_DECAY_MS, NativeAllocatorConfig::setDirtyDecayMs);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(JEMALLOC_MUZZY_DECAY_MS, NativeAllocatorConfig::setMuzzyDecayMs);
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(JEMALLOC_PURGE_INTERVAL, v -> NativeArenaPurger.setCheckIntervalMs(v.millis()));
            clusterService.getClusterSettings().addSettingsUpdateConsumer(JEMALLOC_PURGE_THRESHOLD_PERCENT, percent -> {
                this.currentThresholdPercent = percent;
                NativeArenaPurger.setThresholdBytes(computeThresholdBytes());
            });
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING, limitValue -> {
                    this.currentNativeLimitBytes = limitValue.getBytes();
                    NativeArenaPurger.setThresholdBytes(computeThresholdBytes());
                });
        } catch (Throwable t) {
            logger.warn("Native allocator config unavailable — native library may not be loaded", t);
        }

        return Collections.emptyList();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(JEMALLOC_DIRTY_DECAY_MS, JEMALLOC_MUZZY_DECAY_MS, JEMALLOC_PURGE_INTERVAL, JEMALLOC_PURGE_THRESHOLD_PERCENT);
    }

    @Override
    public void close() {
        NativeArenaPurger.stop();
    }
}
