/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.arrow.spi.PoolGroup;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;
import org.opensearch.plugin.stats.NativeAllocatorStatsRegistry;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Top-level plugin that owns the unified Arrow-backed native memory allocator.
 *
 * <p>All Arrow-consuming plugins extend this plugin to share one
 * {@link ArrowNativeAllocator} and its classloader.
 */
public class ArrowBasePlugin extends Plugin implements ExtensiblePlugin, ActionPlugin {

    /** Creates the plugin. */
    public ArrowBasePlugin() {}

    // ─── Settings ────────────────────────────────────────────────────────────────

    /** Whether the NativeMemoryRebalancer is enabled. */
    public static final Setting<Boolean> REBALANCER_ENABLED_SETTING = Setting.boolSetting(
        "native.allocator.rebalancer.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Interval in seconds between pool rebalance cycles. 0 disables rebalancing. */
    public static final Setting<Long> REBALANCE_INTERVAL_SETTING = Setting.longSetting(
        "native.allocator.rebalance.interval_seconds",
        5L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Pool utilization above this triggers growth. */
    public static final Setting<Double> PRESSURE_THRESHOLD_SETTING = Setting.doubleSetting(
        "native.allocator.rebalancer.pressure_threshold",
        0.75,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Pool utilization below this means pool can give back capacity. */
    public static final Setting<Double> IDLE_THRESHOLD_SETTING = Setting.doubleSetting(
        "native.allocator.rebalancer.idle_threshold",
        0.50,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Factor to shrink idle pools by (new limit = limit * (1 - shrink_factor)). */
    public static final Setting<Double> SHRINK_FACTOR_SETTING = Setting.doubleSetting(
        "native.allocator.rebalancer.shrink_factor",
        0.10,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Minimum guaranteed bytes for the Flight pool. Default is 2% of budget. */
    public static final Setting<Long> FLIGHT_MIN_SETTING = new Setting<>(
        NativeAllocatorPoolConfig.SETTING_FLIGHT_MIN,
        s -> derivePoolMinDefault(s, 2),
        s -> parseNonNegativeLong(s, NativeAllocatorPoolConfig.SETTING_FLIGHT_MIN),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Maximum bytes the Flight pool can burst to. Default is 5% of budget. */
    public static final Setting<Long> FLIGHT_MAX_SETTING = new Setting<>(
        NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX,
        s -> derivePoolMaxDefault(s, 5),
        s -> parseNonNegativeLong(s, NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Minimum guaranteed bytes for the ingest pool. Default is 1% of budget on warm nodes, 2% otherwise. */
    public static final Setting<Long> INGEST_MIN_SETTING = new Setting<>(
        NativeAllocatorPoolConfig.SETTING_INGEST_MIN,
        s -> derivePoolMinDefault(s, DiscoveryNode.isWarmNode(s) ? 1 : 2),
        s -> parseNonNegativeLong(s, NativeAllocatorPoolConfig.SETTING_INGEST_MIN),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Maximum bytes the ingest pool can burst to. Default is 3% of budget on warm nodes, 5% otherwise. */
    public static final Setting<Long> INGEST_MAX_SETTING = new Setting<>(
        NativeAllocatorPoolConfig.SETTING_INGEST_MAX,
        s -> derivePoolMaxDefault(s, DiscoveryNode.isWarmNode(s) ? 3 : 5),
        s -> parseNonNegativeLong(s, NativeAllocatorPoolConfig.SETTING_INGEST_MAX),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Minimum guaranteed bytes for the query pool. Default is 2% of budget. */
    public static final Setting<Long> QUERY_MIN_SETTING = new Setting<>(
        NativeAllocatorPoolConfig.SETTING_QUERY_MIN,
        s -> derivePoolMinDefault(s, 2),
        s -> parseNonNegativeLong(s, NativeAllocatorPoolConfig.SETTING_QUERY_MIN),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Maximum bytes the query pool can allocate. Default is 5% of budget. */
    public static final Setting<Long> QUERY_MAX_SETTING = new Setting<>(
        NativeAllocatorPoolConfig.SETTING_QUERY_MAX,
        s -> derivePoolMaxDefault(s, 5),
        s -> parseNonNegativeLong(s, NativeAllocatorPoolConfig.SETTING_QUERY_MAX),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // ─── Instance state ──────────────────────────────────────────────────────────

    private volatile ArrowNativeAllocator allocator;
    private volatile ScheduledExecutorService rebalancerScheduler;
    private volatile ScheduledFuture<?> rebalanceTask;
    private volatile NativeMemoryRebalancer rebalancer;

    // ─── Plugin lifecycle ────────────────────────────────────────────────────────

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
        ClusterSettings cs = clusterService.getClusterSettings();
        Supplier<Long> budgetSupplier = () -> ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(clusterService.getSettings())
            .getBytes();
        ArrowNativeAllocator built = buildAllocator(settings, cs, budgetSupplier);
        this.allocator = built;

        Supplier<NativeAllocatorPoolStats> statsSupplier = () -> {
            ArrowNativeAllocator a = this.allocator;
            return a != null ? a.stats() : null;
        };
        return List.of(built, new NativeAllocatorStatsRegistry(statsSupplier));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            FLIGHT_MIN_SETTING,
            FLIGHT_MAX_SETTING,
            INGEST_MIN_SETTING,
            INGEST_MAX_SETTING,
            QUERY_MIN_SETTING,
            QUERY_MAX_SETTING,
            REBALANCE_INTERVAL_SETTING,
            REBALANCER_ENABLED_SETTING,
            PRESSURE_THRESHOLD_SETTING,
            IDLE_THRESHOLD_SETTING,
            SHRINK_FACTOR_SETTING
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        Supplier<NativeAllocatorPoolStats> statsSupplier = () -> allocator != null ? allocator.stats() : null;
        return List.of(new ArrowBaseStatsAction(statsSupplier));
    }

    @Override
    public void close() throws IOException {
        if (rebalancerScheduler != null) {
            rebalancerScheduler.shutdownNow();
        }
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }

    // ─── Package-private (visible for tests) ─────────────────────────────────────

    /**
     * Constructs the allocator and wires its pools and the rebalancer.
     */
    ArrowNativeAllocator buildAllocator(Settings settings, ClusterSettings cs, Supplier<Long> budgetSupplier) {
        ArrowNativeAllocator allocator = new ArrowNativeAllocator();

        // Set budget for validation
        long nativeBudget = ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(settings).getBytes();
        if (nativeBudget > 0) {
            allocator.setBudget(nativeBudget);
        }

        // Validate min < max for each pool
        validateMinMax(NativeAllocatorPoolConfig.POOL_FLIGHT, FLIGHT_MIN_SETTING.get(settings), FLIGHT_MAX_SETTING.get(settings));
        validateMinMax(NativeAllocatorPoolConfig.POOL_INGEST, INGEST_MIN_SETTING.get(settings), INGEST_MAX_SETTING.get(settings));
        validateMinMax(NativeAllocatorPoolConfig.POOL_QUERY, QUERY_MIN_SETTING.get(settings), QUERY_MAX_SETTING.get(settings));

        // Create pools (always start at max)
        allocator.getOrCreatePool(
            NativeAllocatorPoolConfig.POOL_FLIGHT,
            FLIGHT_MIN_SETTING.get(settings),
            FLIGHT_MAX_SETTING.get(settings),
            PoolGroup.TRANSPORT
        );
        allocator.getOrCreatePool(
            NativeAllocatorPoolConfig.POOL_INGEST,
            INGEST_MIN_SETTING.get(settings),
            INGEST_MAX_SETTING.get(settings),
            PoolGroup.INDEXING
        );
        allocator.getOrCreatePool(
            NativeAllocatorPoolConfig.POOL_QUERY,
            QUERY_MIN_SETTING.get(settings),
            QUERY_MAX_SETTING.get(settings),
            PoolGroup.SEARCH
        );

        // Register dynamic setting consumers for min/max changes
        cs.addSettingsUpdateConsumer(FLIGHT_MIN_SETTING, newMin -> allocator.setPoolMin(NativeAllocatorPoolConfig.POOL_FLIGHT, newMin));
        cs.addSettingsUpdateConsumer(FLIGHT_MAX_SETTING, newMax -> allocator.setPoolLimit(NativeAllocatorPoolConfig.POOL_FLIGHT, newMax));
        cs.addSettingsUpdateConsumer(INGEST_MIN_SETTING, newMin -> allocator.setPoolMin(NativeAllocatorPoolConfig.POOL_INGEST, newMin));
        cs.addSettingsUpdateConsumer(INGEST_MAX_SETTING, newMax -> allocator.setPoolLimit(NativeAllocatorPoolConfig.POOL_INGEST, newMax));
        cs.addSettingsUpdateConsumer(QUERY_MIN_SETTING, newMin -> allocator.setPoolMin(NativeAllocatorPoolConfig.POOL_QUERY, newMin));
        cs.addSettingsUpdateConsumer(QUERY_MAX_SETTING, newMax -> allocator.setPoolLimit(NativeAllocatorPoolConfig.POOL_QUERY, newMax));

        // Register dynamic consumer for rebalancer enable/disable
        cs.addSettingsUpdateConsumer(REBALANCER_ENABLED_SETTING, enabled -> {
            if (enabled == false) {
                cancelRebalanceTask();
                allocator.resetAllPoolsToMax();
            } else {
                startRebalancer(allocator, budgetSupplier, cs.get(REBALANCE_INTERVAL_SETTING));
            }
        });

        // Set up the rebalancer if enabled
        if (REBALANCER_ENABLED_SETTING.get(settings)) {
            startRebalancer(allocator, budgetSupplier, REBALANCE_INTERVAL_SETTING.get(settings));
        }

        // Register dynamic consumer for interval changes
        cs.addSettingsUpdateConsumer(REBALANCE_INTERVAL_SETTING, this::updateRebalanceInterval);

        // Register dynamic consumers for threshold changes
        cs.addSettingsUpdateConsumer(PRESSURE_THRESHOLD_SETTING, value -> {
            NativeMemoryRebalancer r = this.rebalancer;
            if (r != null) r.setPressureThreshold(value);
        });
        cs.addSettingsUpdateConsumer(IDLE_THRESHOLD_SETTING, value -> {
            NativeMemoryRebalancer r = this.rebalancer;
            if (r != null) r.setIdleThreshold(value);
        });
        cs.addSettingsUpdateConsumer(SHRINK_FACTOR_SETTING, value -> {
            NativeMemoryRebalancer r = this.rebalancer;
            if (r != null) r.setShrinkFactor(value);
        });

        return allocator;
    }

    // ─── Private helpers ─────────────────────────────────────────────────────────

    private synchronized void startRebalancer(ArrowNativeAllocator allocator, Supplier<Long> budgetSupplier, long intervalSeconds) {
        if (rebalancer != null || rebalancerScheduler != null) return;

        long budget = budgetSupplier.get();
        if (budget <= 0) return;
        if (intervalSeconds <= 0) return;

        NativeMemoryRebalancer nativeRebalancer = new NativeMemoryRebalancer(
            allocator,
            budgetSupplier,
            PRESSURE_THRESHOLD_SETTING.getDefault(Settings.EMPTY),
            IDLE_THRESHOLD_SETTING.getDefault(Settings.EMPTY),
            SHRINK_FACTOR_SETTING.getDefault(Settings.EMPTY)
        );
        this.rebalancer = nativeRebalancer;

        Scheduler.SafeScheduledThreadPoolExecutor executor = new Scheduler.SafeScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "native-allocator-rebalancer");
            t.setDaemon(true);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
        this.rebalancerScheduler = executor;

        rebalanceTask = rebalancerScheduler.scheduleAtFixedRate(nativeRebalancer, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    private synchronized void cancelRebalanceTask() {
        ScheduledFuture<?> existing = rebalanceTask;
        if (existing != null) {
            FutureUtils.cancel(existing);
            rebalanceTask = null;
        }
        rebalancer = null;
        if (rebalancerScheduler != null) {
            rebalancerScheduler.shutdown();
            rebalancerScheduler = null;
        }
    }

    private void updateRebalanceInterval(long newInterval) {
        cancelRebalanceTask();
        if (newInterval > 0 && rebalancerScheduler != null && rebalancer != null) {
            rebalanceTask = rebalancerScheduler.scheduleAtFixedRate(rebalancer, newInterval, newInterval, TimeUnit.SECONDS);
        }
    }

    private static void validateMinMax(String poolName, long min, long max) {
        if (min > max) {
            throw new IllegalArgumentException("Pool '" + poolName + "' min (" + min + ") exceeds max (" + max + ")");
        }
    }

    static String derivePoolMaxDefault(Settings settings, int percent) {
        ByteSizeValue nativeLimit = ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(settings);
        if (nativeLimit.getBytes() <= 0) {
            return Long.toString(Long.MAX_VALUE);
        }
        return Long.toString(Math.max(0L, nativeLimit.getBytes() * percent / 100));
    }

    static String derivePoolMinDefault(Settings settings, int percent) {
        ByteSizeValue nativeLimit = ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(settings);
        if (nativeLimit.getBytes() <= 0) {
            return "0";
        }
        return Long.toString(Math.max(0L, nativeLimit.getBytes() * percent / 100));
    }

    private static long parseNonNegativeLong(String s, String settingName) {
        long v = Long.parseLong(s);
        if (v < 0) {
            throw new IllegalArgumentException("Setting [" + settingName + "] must be >= 0, got " + v);
        }
        return v;
    }
}
