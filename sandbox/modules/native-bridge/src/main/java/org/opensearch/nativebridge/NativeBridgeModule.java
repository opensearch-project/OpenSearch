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

    /** Activates/deactivates jemalloc heap profiling at runtime. Requires prof:true via _RJEM_MALLOC_CONF env var. */
    public static final Setting<Boolean> JEMALLOC_HEAP_PROF_ACTIVE = Setting.boolSetting(
        "native.jemalloc.heap_prof_active",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Triggers a heap profile dump to the specified path when updated.
     * Path must be under the node's data directory (validated by NativeHeapProfiler).
     * Analyzed with jeprof. WARNING: Heap dumps may contain sensitive in-memory data.
     */
    public static final Setting<String> JEMALLOC_HEAP_PROF_DUMP_PATH = Setting.simpleString(
        "native.jemalloc.heap_prof_dump_path",
        "",
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

        // Apply initial values (handles opensearch.yml overrides of the compile-time malloc_conf defaults)
        NativeAllocatorConfig.setDirtyDecayMs(JEMALLOC_DIRTY_DECAY_MS.get(settings));
        NativeAllocatorConfig.setMuzzyDecayMs(JEMALLOC_MUZZY_DECAY_MS.get(settings));

        // Restrict heap dump paths to the node's data directory
        NativeHeapProfiler.setAllowedDumpDir(environment.dataFiles()[0].toAbsolutePath().toString());

        // Register dynamic update listeners
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JEMALLOC_DIRTY_DECAY_MS, NativeAllocatorConfig::setDirtyDecayMs);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JEMALLOC_MUZZY_DECAY_MS, NativeAllocatorConfig::setMuzzyDecayMs);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JEMALLOC_HEAP_PROF_ACTIVE, NativeHeapProfiler::setActive);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JEMALLOC_HEAP_PROF_DUMP_PATH, path -> {
            if (path != null && !path.isEmpty()) {
                NativeHeapProfiler.dumpProfile(path);
            }
        });

        return Collections.emptyList();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(JEMALLOC_DIRTY_DECAY_MS, JEMALLOC_MUZZY_DECAY_MS, JEMALLOC_HEAP_PROF_ACTIVE, JEMALLOC_HEAP_PROF_DUMP_PATH);
    }
}
