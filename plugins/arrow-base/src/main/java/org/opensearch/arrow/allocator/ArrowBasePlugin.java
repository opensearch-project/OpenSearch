/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.opensearch.arrow.memory.ArrowAllocatorService;
import org.opensearch.arrow.memory.DefaultArrowAllocatorService;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Module;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Top-level plugin that owns the unified Arrow-backed native memory allocator.
 *
 * <p>All Arrow-consuming plugins (arrow-flight-rpc, parquet-data-format) extend
 * this plugin to share one {@link ArrowNativeAllocator} and its classloader.
 *
 * <p>Each pool has a min (guaranteed floor) and max (burst ceiling). The rebalancer
 * ensures every pool can always allocate up to its min, and distributes unused
 * capacity allowing pools to grow up to their max.
 */
public class ArrowBasePlugin extends Plugin implements ExtensiblePlugin {

    /** Creates the plugin. */
    public ArrowBasePlugin() {}

    /** Maximum bytes for the root Arrow allocator. */
    public static final Setting<Long> ROOT_LIMIT_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT,
        Long.MAX_VALUE,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Minimum guaranteed bytes for the Flight pool. */
    public static final Setting<Long> FLIGHT_MIN_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_FLIGHT_MIN,
        Long.MAX_VALUE,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Maximum bytes the Flight pool can burst to. */
    public static final Setting<Long> FLIGHT_MAX_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX,
        Long.MAX_VALUE,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Minimum guaranteed bytes for the ingest pool. */
    public static final Setting<Long> INGEST_MIN_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_INGEST_MIN,
        Long.MAX_VALUE,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Maximum bytes the ingest pool can burst to. */
    public static final Setting<Long> INGEST_MAX_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_INGEST_MAX,
        Long.MAX_VALUE,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Interval in seconds between pool rebalance cycles. 0 disables rebalancing. */
    public static final Setting<Long> REBALANCE_INTERVAL_SETTING = Setting.longSetting(
        "native.allocator.rebalance.interval_seconds",
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile ArrowNativeAllocator allocator;
    private DefaultArrowAllocatorService allocatorService;

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

        long rootLimit = ROOT_LIMIT_SETTING.get(settings);
        allocator = new ArrowNativeAllocator(rootLimit);
        allocator.setRebalanceInterval(REBALANCE_INTERVAL_SETTING.get(settings));

        long flightMin = FLIGHT_MIN_SETTING.get(settings);
        long flightMax = FLIGHT_MAX_SETTING.get(settings);
        long ingestMin = INGEST_MIN_SETTING.get(settings);
        long ingestMax = INGEST_MAX_SETTING.get(settings);

        validateMinMax(NativeAllocatorPoolConfig.POOL_FLIGHT, flightMin, flightMax);
        validateMinMax(NativeAllocatorPoolConfig.POOL_INGEST, ingestMin, ingestMax);
        validateMinSum(rootLimit, flightMin, ingestMin);

        allocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_FLIGHT, flightMin, flightMax);
        allocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_INGEST, ingestMin, ingestMax);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(ROOT_LIMIT_SETTING, allocator::setRootLimit);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REBALANCE_INTERVAL_SETTING, allocator::setRebalanceInterval);

        // ArrowBasePlugin component: node-level Arrow allocator service for cross-plugin zero-copy
        this.allocatorService = new DefaultArrowAllocatorService(allocator);

        List<Object> components = new ArrayList<>();
        components.add(allocator);
        components.add(allocatorService);
        return components;
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return List.of(new AbstractModule() {
            @Override
            protected void configure() {
                bind(ArrowAllocatorService.class).toInstance(allocatorService);
            }
        });
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            ROOT_LIMIT_SETTING,
            FLIGHT_MIN_SETTING,
            FLIGHT_MAX_SETTING,
            INGEST_MIN_SETTING,
            INGEST_MAX_SETTING,
            REBALANCE_INTERVAL_SETTING
        );
    }

    private static void validateMinMax(String poolName, long min, long max) {
        if (min > max) {
            throw new IllegalArgumentException("Pool '" + poolName + "' min (" + min + ") exceeds max (" + max + ")");
        }
    }

    private static void validateMinSum(long rootLimit, long... mins) {
        if (rootLimit == Long.MAX_VALUE) {
            return;
        }
        long sum = 0;
        for (long min : mins) {
            long prev = sum;
            sum += min;
            if (sum < prev) {
                throw new IllegalArgumentException("Sum of pool minimums overflows.");
            }
        }
        if (sum > rootLimit) {
            throw new IllegalArgumentException(
                "Sum of pool minimums ("
                    + sum
                    + " bytes) exceeds root limit ("
                    + rootLimit
                    + " bytes). "
                    + "Reduce pool minimums or increase "
                    + NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT
            );
        }
    }

    @Override
    public void close() throws IOException {
        if (allocatorService != null) {
            allocatorService.close();
        }
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }
}
