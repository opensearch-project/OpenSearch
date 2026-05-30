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

    public static final String FORMAT_NAME = "parquet";

    private static final Logger logger = LogManager.getLogger(ParquetStatsProvider.class);

    private static volatile ParquetStatsProvider INSTANCE;

    private final Map<ShardId, ParquetShardStatsTracker> trackers = new ConcurrentHashMap<>();

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
        // Attach native runtime metrics — only at node level. If the FFM/JNI bridge is
        // unavailable (e.g., test infra without lib loaded) we still return the per-shard
        // aggregate; missing runtime metrics shouldn't break the whole stats request.
        // Catching Exception (not Throwable) so we still propagate JVM-fatal Errors.
        try {
            ParquetNativeRuntimeStats runtime = ParquetNativeRuntimeStats.fromArray(
                org.opensearch.parquet.bridge.RustBridge.collectRuntimeMetrics()
            );
            return Optional.of(agg.withNativeRuntime(runtime));
        } catch (Exception e) {
            logger.warn("Failed to collect native runtime metrics; returning per-shard aggregate without runtime block", e);
            return Optional.of(agg);
        }
    }

    @Override
    public Writeable.Reader<ParquetShardStats> shardStatsReader() {
        return ParquetShardStats::new;
    }
}
