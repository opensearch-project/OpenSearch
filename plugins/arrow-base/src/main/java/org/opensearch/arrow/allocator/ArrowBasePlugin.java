/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;
import org.opensearch.plugin.stats.NativeAllocatorStatsRegistry;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
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

    /**
     * Maximum bytes for the root Arrow allocator.
     *
     * <p>When unset, the default is 20% of
     * {@link ResourceTrackerSettings#NODE_NATIVE_MEMORY_LIMIT_SETTING}; see
     * {@link #deriveRootLimitDefault}. The Arrow framework gets a small fraction of the
     * native budget because the dominant consumer of native memory in analytics workloads
     * is the DataFusion Rust runtime (~75% of {@code node.native_memory.limit}), not Arrow.
     * If AC is unconfigured (limit = 0), the default is {@link Long#MAX_VALUE}, preserving
     * pre-AC behaviour.
     */
    public static final Setting<Long> ROOT_LIMIT_SETTING = new Setting<>(
        NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT,
        ArrowBasePlugin::deriveRootLimitDefault,
        s -> {
            long v = Long.parseLong(s);
            if (v < 0) {
                throw new IllegalArgumentException("Setting [" + NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT + "] must be >= 0, got " + v);
            }
            return v;
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Computes the default for {@link #ROOT_LIMIT_SETTING} as 20% of
     * {@link ResourceTrackerSettings#NODE_NATIVE_MEMORY_LIMIT_SETTING}. The Arrow framework's
     * hard cap covers only Arrow allocations — DataFusion's Rust runtime is a sibling of
     * Arrow root and gets the larger share of the native budget (see
     * {@code DataFusionPlugin#deriveMemoryPoolLimitDefault}).
     *
     * <p>Returns the bytes-as-string representation expected by the {@link Setting} parser.
     * If the AC limit is unset (== 0), the default is {@link Long#MAX_VALUE} — unbounded —
     * preserving pre-AC behaviour.
     */
    static String deriveRootLimitDefault(Settings settings) {
        ByteSizeValue nativeLimit = ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(settings);
        if (nativeLimit.getBytes() <= 0) {
            return Long.toString(Long.MAX_VALUE);
        }
        return Long.toString(nativeLimit.getBytes() * 20 / 100);
    }

    /** Minimum guaranteed bytes for the Flight pool. */
    public static final Setting<Long> FLIGHT_MIN_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_FLIGHT_MIN,
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Maximum bytes the Flight pool can burst to. Default is 5% of
     * {@link ResourceTrackerSettings#NODE_NATIVE_MEMORY_LIMIT_SETTING}; see
     * {@link #derivePoolMaxDefault}. Falls back to {@link Long#MAX_VALUE} when AC is
     * unconfigured. Matches the partitioning model documented in PR #21732.
     */
    public static final Setting<Long> FLIGHT_MAX_SETTING = new Setting<>(
        NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX,
        s -> derivePoolMaxDefault(s, 5),
        s -> {
            long v = Long.parseLong(s);
            if (v < 0) {
                throw new IllegalArgumentException("Setting [" + NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX + "] must be >= 0, got " + v);
            }
            return v;
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Minimum guaranteed bytes for the ingest pool. */
    public static final Setting<Long> INGEST_MIN_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_INGEST_MIN,
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Maximum bytes the ingest pool can burst to. Default is 8% of
     * {@link ResourceTrackerSettings#NODE_NATIVE_MEMORY_LIMIT_SETTING}; see
     * {@link #derivePoolMaxDefault}. Falls back to {@link Long#MAX_VALUE} when AC is
     * unconfigured. Ingest gets a larger fraction than Flight/Query because parquet VSR
     * allocators dominate write-path memory usage — see partitioning model in PR #21732.
     */
    public static final Setting<Long> INGEST_MAX_SETTING = new Setting<>(
        NativeAllocatorPoolConfig.SETTING_INGEST_MAX,
        s -> derivePoolMaxDefault(s, 8),
        s -> {
            long v = Long.parseLong(s);
            if (v < 0) {
                throw new IllegalArgumentException("Setting [" + NativeAllocatorPoolConfig.SETTING_INGEST_MAX + "] must be >= 0, got " + v);
            }
            return v;
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Minimum guaranteed bytes for the query pool. Honored by the rebalancer (when
     * enabled) — sets a floor below which the rebalancer will not shrink the pool.
     * Has no effect when rebalancing is disabled.
     */
    public static final Setting<Long> QUERY_MIN_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_QUERY_MIN,
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Maximum bytes the query pool can allocate. Default is 5% of
     * {@link ResourceTrackerSettings#NODE_NATIVE_MEMORY_LIMIT_SETTING}; see
     * {@link #derivePoolMaxDefault}. Falls back to {@link Long#MAX_VALUE} when AC is
     * unconfigured. Enforced by Arrow's child-allocator limit — analytics-engine's
     * per-query allocators are children of this pool, so the sum of in-flight per-query
     * allocations is capped here.
     *
     * <p>Note: each individual analytics query is also bounded by
     * {@code analytics.exec.QueryContext} per-query limit (currently the constant
     * {@code DEFAULT_PER_QUERY_MEMORY_LIMIT = 256 MB}). Lowering {@code QUERY_MAX}
     * below {@code 256 MB × concurrent-queries} can starve queries even when each
     * individual query is within its per-query limit.
     */
    public static final Setting<Long> QUERY_MAX_SETTING = new Setting<>(
        NativeAllocatorPoolConfig.SETTING_QUERY_MAX,
        s -> derivePoolMaxDefault(s, 5),
        s -> {
            long v = Long.parseLong(s);
            if (v < 0) {
                throw new IllegalArgumentException("Setting [" + NativeAllocatorPoolConfig.SETTING_QUERY_MAX + "] must be >= 0, got " + v);
            }
            return v;
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Computes the default for a pool max as a percentage of
     * {@link ResourceTrackerSettings#NODE_NATIVE_MEMORY_LIMIT_SETTING} (the operator's
     * declared off-heap budget), falling back to {@link Long#MAX_VALUE} when AC is
     * unconfigured. Returns the bytes-as-string representation expected by the
     * {@link Setting} parser.
     *
     * <p>Pools are anchored to {@code node.native_memory.limit} rather than to
     * {@link #ROOT_LIMIT_SETTING} so the diagrammed partitioning (PR #21732) holds:
     * sum of pool maxes (5+8+5 = 18% of native_memory.limit) fits within the framework
     * root cap (20% of native_memory.limit) by default. Operator overrides of
     * {@code root.limit} that drop it below {@code sum(pool.max)} are caught by the
     * grouped validator.
     *
     * <p>The fraction is taken straight from {@code node.native_memory.limit}, not from
     * {@code limit - buffer_percent}. {@code buffer_percent} is an admission-control
     * throttle margin, not a framework budget reduction.
     *
     * @param settings node settings
     * @param percent  fraction of {@code node.native_memory.limit} the pool max defaults to
     */
    static String derivePoolMaxDefault(Settings settings, int percent) {
        ByteSizeValue nativeLimit = ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(settings);
        if (nativeLimit.getBytes() <= 0) {
            return Long.toString(Long.MAX_VALUE);
        }
        long pool = Math.max(0L, nativeLimit.getBytes() * percent / 100);
        return Long.toString(pool);
    }

    /** Interval in seconds between pool rebalance cycles. 0 disables rebalancing. */
    public static final Setting<Long> REBALANCE_INTERVAL_SETTING = Setting.longSetting(
        "native.allocator.rebalance.interval_seconds",
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile ArrowNativeAllocator allocator;

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
        ArrowNativeAllocator built = buildAllocator(settings, cs);
        this.allocator = built;
        // Publish a NativeAllocatorStatsRegistry alongside the allocator so the server-side
        // NodeService can discover the supplier via pluginComponents (instanceof filter) without
        // taking a compile-time dependency on this plugin. The lambda re-reads `this.allocator`
        // each invocation, so after close() nulls the field, the supplier returns null cleanly.
        Supplier<NativeAllocatorPoolStats> statsSupplier = () -> {
            ArrowNativeAllocator a = this.allocator;
            return a != null ? a.stats() : null;
        };
        return List.of(built, new NativeAllocatorStatsRegistry(statsSupplier));
    }

    /**
     * Constructs the allocator and wires its pools and dynamic-update consumers from
     * a pure {@code (Settings, ClusterSettings)} pair. Package-private so unit tests
     * can exercise the full wiring without a heavyweight {@link ClusterService}
     * fixture — mirrors the shape of {@link #registerSettingsUpdateConsumers} which
     * is already test-friendly for the same reason.
     */
    static ArrowNativeAllocator buildAllocator(Settings settings, ClusterSettings cs) {
        long rootLimit = ROOT_LIMIT_SETTING.get(settings);
        ArrowNativeAllocator allocator = new ArrowNativeAllocator(rootLimit);
        allocator.setRebalanceInterval(REBALANCE_INTERVAL_SETTING.get(settings));

        // Single source of truth for cross-setting invariants — same logic runs on
        // dynamic updates via the grouped consumer below.
        validateUpdate(settings);

        allocator.getOrCreatePool(
            NativeAllocatorPoolConfig.POOL_FLIGHT,
            FLIGHT_MIN_SETTING.get(settings),
            FLIGHT_MAX_SETTING.get(settings)
        );
        allocator.getOrCreatePool(
            NativeAllocatorPoolConfig.POOL_INGEST,
            INGEST_MIN_SETTING.get(settings),
            INGEST_MAX_SETTING.get(settings)
        );
        allocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_QUERY, QUERY_MIN_SETTING.get(settings), QUERY_MAX_SETTING.get(settings));

        registerSettingsUpdateConsumers(cs, allocator);
        return allocator;
    }

    /**
     * Registers cluster-settings update consumers that propagate dynamic setting changes
     * into the live {@link ArrowNativeAllocator}. Package-private so unit tests can exercise
     * the wiring with a real {@link ClusterSettings} instance — the test that asserts a PUT
     * lands on the allocator is what catches a future regression where one of these lines
     * is accidentally removed.
     */
    static void registerSettingsUpdateConsumers(ClusterSettings cs, ArrowNativeAllocator allocator) {
        cs.addSettingsUpdateConsumer(ROOT_LIMIT_SETTING, allocator::setRootLimit);
        cs.addSettingsUpdateConsumer(REBALANCE_INTERVAL_SETTING, allocator::setRebalanceInterval);
        cs.addSettingsUpdateConsumer(FLIGHT_MAX_SETTING, v -> allocator.setPoolLimit(NativeAllocatorPoolConfig.POOL_FLIGHT, v));
        cs.addSettingsUpdateConsumer(FLIGHT_MIN_SETTING, v -> allocator.setPoolMin(NativeAllocatorPoolConfig.POOL_FLIGHT, v));
        cs.addSettingsUpdateConsumer(INGEST_MAX_SETTING, v -> allocator.setPoolLimit(NativeAllocatorPoolConfig.POOL_INGEST, v));
        cs.addSettingsUpdateConsumer(INGEST_MIN_SETTING, v -> allocator.setPoolMin(NativeAllocatorPoolConfig.POOL_INGEST, v));
        cs.addSettingsUpdateConsumer(QUERY_MAX_SETTING, v -> allocator.setPoolLimit(NativeAllocatorPoolConfig.POOL_QUERY, v));
        cs.addSettingsUpdateConsumer(QUERY_MIN_SETTING, v -> allocator.setPoolMin(NativeAllocatorPoolConfig.POOL_QUERY, v));

        // Grouped validator runs across the related settings on every dynamic update so cross-setting
        // invariants (sum of pool mins ≤ root, per-pool min ≤ max) are enforced post-startup.
        cs.addSettingsUpdateConsumer(s -> {}, MIN_MAX_SETTINGS, ArrowBasePlugin::validateUpdate);
    }

    private static final List<Setting<Long>> MIN_MAX_SETTINGS = List.of(
        ROOT_LIMIT_SETTING,
        FLIGHT_MIN_SETTING,
        FLIGHT_MAX_SETTING,
        INGEST_MIN_SETTING,
        INGEST_MAX_SETTING,
        QUERY_MIN_SETTING,
        QUERY_MAX_SETTING
    );

    private static void validateUpdate(Settings settings) {
        long rootLimit = ROOT_LIMIT_SETTING.get(settings);
        long flightMin = FLIGHT_MIN_SETTING.get(settings);
        long flightMax = FLIGHT_MAX_SETTING.get(settings);
        long ingestMin = INGEST_MIN_SETTING.get(settings);
        long ingestMax = INGEST_MAX_SETTING.get(settings);
        long queryMin = QUERY_MIN_SETTING.get(settings);
        long queryMax = QUERY_MAX_SETTING.get(settings);
        validateMinMax(NativeAllocatorPoolConfig.POOL_FLIGHT, flightMin, flightMax);
        validateMinMax(NativeAllocatorPoolConfig.POOL_INGEST, ingestMin, ingestMax);
        validateMinMax(NativeAllocatorPoolConfig.POOL_QUERY, queryMin, queryMax);
        validateMinSum(rootLimit, flightMin, ingestMin, queryMin);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            ROOT_LIMIT_SETTING,
            FLIGHT_MIN_SETTING,
            FLIGHT_MAX_SETTING,
            INGEST_MIN_SETTING,
            INGEST_MAX_SETTING,
            QUERY_MIN_SETTING,
            QUERY_MAX_SETTING,
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
            try {
                sum = Math.addExact(sum, min);
            } catch (ArithmeticException overflow) {
                throw new IllegalArgumentException("Sum of pool minimums overflows.", overflow);
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
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }
}
