/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.nativebridge.spi.NativeAllocatorConfig;
import org.opensearch.nativebridge.spi.NativeHeapProfiler;
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

        // Apply initial values (handles config overrides of the compile-time malloc_conf defaults)
        NativeAllocatorConfig.setDirtyDecayMs(JEMALLOC_DIRTY_DECAY_MS.get(settings));
        NativeAllocatorConfig.setMuzzyDecayMs(JEMALLOC_MUZZY_DECAY_MS.get(settings));

        // Register the heap profiler MBean for JMX-based profiling via opensearch-heap-prof CLI
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

        // Register dynamic update listeners
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JEMALLOC_DIRTY_DECAY_MS, NativeAllocatorConfig::setDirtyDecayMs);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JEMALLOC_MUZZY_DECAY_MS, NativeAllocatorConfig::setMuzzyDecayMs);

        return Collections.emptyList();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(JEMALLOC_DIRTY_DECAY_MS, JEMALLOC_MUZZY_DECAY_MS);
    }
}
