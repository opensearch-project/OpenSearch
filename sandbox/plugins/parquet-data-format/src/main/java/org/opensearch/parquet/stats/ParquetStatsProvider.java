/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.stats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.plugin.stats.DataFormatStatsProvider;
import org.opensearch.plugin.stats.DataFormatStatsProviderRegistry;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Parquet implementation of {@link DataFormatStatsProvider}.
 *
 * <p>Maintains a per-shard registry of {@link ParquetShardStatsTracker} instances.
 * {@code ParquetIndexingEngine} self-registers on construction and unregisters
 * on close. The plugin's REST + transport classes read stats from this provider
 * via the {@link DataFormatStatsProvider} interface.
 *
 * <p>Singleton pattern: a static {@code INSTANCE} field is set during plugin
 * construction so the engine layer (which doesn't have easy Guice injection
 * hooks) can register its tracker without DI plumbing.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ParquetStatsProvider implements DataFormatStatsProvider<ParquetShardStats> {

    /** Canonical format name, sourced from {@link ParquetDataFormat} to avoid duplicating the literal. */
    public static final String FORMAT_NAME = ParquetDataFormat.PARQUET_DATA_FORMAT_NAME;

    private static final Logger logger = LogManager.getLogger(ParquetStatsProvider.class);

    private static volatile ParquetStatsProvider INSTANCE;

    private final Map<ShardId, ParquetShardStatsTracker> trackers = new ConcurrentHashMap<>();

    // Node-level thread pool backing parquet ingestion writes. Set post-construction (the pool
    // isn't available when the provider is built in createGuiceModules). May be null in tests.
    private volatile org.opensearch.threadpool.ThreadPool threadPool;

    public ParquetStatsProvider() {
        // First instance wins. Subsequent constructions are no-ops on the singleton slot.
        if (INSTANCE == null) {
            INSTANCE = this;
        }
        // Register with the cross-plugin registry so REST handlers and transports can find us.
        DataFormatStatsProviderRegistry.INSTANCE.register(this);
    }

    /** Returns the singleton instance, or {@code null} if the plugin has not been constructed yet. */
    public static ParquetStatsProvider getInstance() {
        return INSTANCE;
    }

    /** Sets the node thread pool used to read live {@code parquet_native_write} pool stats. */
    public void setThreadPool(org.opensearch.threadpool.ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    /** Registers a tracker for a shard. Called from {@code ParquetIndexingEngine}'s constructor. */
    public void register(ShardId shardId, ParquetShardStatsTracker tracker) {
        trackers.put(shardId, tracker);
    }

    /** Unregisters a tracker. Called from the engine's {@code close()} method. */
    public void unregister(ShardId shardId) {
        trackers.remove(shardId);
    }

    /**
     * Returns the registered tracker for a shard, or {@code null} if none.
     * Used by sibling components (e.g., merger, writer) to increment counters
     * directly on the same tracker the engine registered.
     */
    public ParquetShardStatsTracker getTracker(ShardId shardId) {
        return trackers.get(shardId);
    }

    // --- DataFormatStatsProvider ---

    @Override
    public String formatName() {
        return FORMAT_NAME;
    }

    @Override
    public Optional<ParquetShardStats> shardStats(ShardId shardId) {
        ParquetShardStatsTracker tracker = trackers.get(shardId);
        return tracker == null ? Optional.empty() : Optional.of(tracker.stats());
    }

    @Override
    public Optional<ParquetShardStats> aggregateNodeStats() {
        if (trackers.isEmpty()) {
            return Optional.empty();
        }
        ParquetShardStats agg = ParquetShardStats.empty();
        for (ParquetShardStatsTracker t : trackers.values()) {
            agg = agg.add(t.stats());
        }
        // Node-level decoration: the live parquet_native_write pool snapshot + the Rust runtime
        // metrics. Both are best-effort — a null pool or an unavailable native bridge simply
        // leaves that block off rather than failing the whole stats request.
        ParquetIngestPoolStats ingestPool = collectIngestPoolStats();
        ParquetNativeRuntimeStats runtime = null;
        try {
            runtime = ParquetNativeRuntimeStats.fromArray(org.opensearch.parquet.bridge.RustBridge.collectRuntimeMetrics());
        } catch (Exception e) {
            logger.warn("Failed to collect native runtime metrics; node stats will omit the native_runtime block", e);
        }
        return Optional.of(agg.withNodeStats(runtime, ingestPool));
    }

    /**
     * Reads the live {@code parquet_native_write} pool stats from the node thread pool, or returns
     * {@code null} if the thread pool is unavailable or the pool is not present.
     */
    private ParquetIngestPoolStats collectIngestPoolStats() {
        org.opensearch.threadpool.ThreadPool tp = threadPool;
        if (tp == null) {
            return null;
        }
        try {
            for (org.opensearch.threadpool.ThreadPoolStats.Stats s : tp.stats()) {
                if (org.opensearch.parquet.ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME.equals(s.getName())) {
                    return ParquetIngestPoolStats.from(s);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to collect parquet ingest pool stats", e);
        }
        return null;
    }

    @Override
    public Writeable.Reader<ParquetShardStats> shardStatsReader() {
        return ParquetShardStats::new;
    }
}
