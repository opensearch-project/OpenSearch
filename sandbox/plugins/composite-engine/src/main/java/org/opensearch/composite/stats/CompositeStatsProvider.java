/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.composite.CompositeDataFormat;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugin.stats.DataFormatStatsProvider;
import org.opensearch.plugin.stats.DataFormatStatsProviderRegistry;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Composite-engine implementation of {@link DataFormatStatsProvider}.
 *
 * <p>Maintains a per-shard registry of {@link CompositeShardStatsTracker} instances.
 * {@code CompositeIndexingExecutionEngine} self-registers on construction and unregisters
 * on close. The plugin's REST + transport classes read stats from this provider.
 *
 * <p>Singleton pattern: a static {@code INSTANCE} is set during plugin construction so the
 * engine/writer/merger layers can reach the tracker without DI plumbing.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class CompositeStatsProvider implements DataFormatStatsProvider<CompositeShardStats> {

    public static final String FORMAT_NAME = CompositeDataFormat.COMPOSITE_FORMAT_NAME;

    private static volatile CompositeStatsProvider INSTANCE;

    private final Map<ShardId, CompositeShardStatsTracker> trackers = new ConcurrentHashMap<>();

    public CompositeStatsProvider() {
        // First instance wins. Subsequent constructions are no-ops on the singleton slot.
        if (INSTANCE == null) {
            INSTANCE = this;
        }
        DataFormatStatsProviderRegistry.INSTANCE.register(this);
    }

    /** Returns the singleton, or {@code null} if the plugin has not been constructed yet. */
    public static CompositeStatsProvider getInstance() {
        return INSTANCE;
    }

    /** Registers a tracker for a shard. Called from the composite engine's constructor. */
    public void register(ShardId shardId, CompositeShardStatsTracker tracker) {
        trackers.put(shardId, tracker);
    }

    /** Unregisters a tracker. Called from the engine's {@code close()}. */
    public void unregister(ShardId shardId) {
        trackers.remove(shardId);
    }

    /** Returns the tracker for a shard, or {@code null} if none — used by writer/merger to count. */
    public CompositeShardStatsTracker getTracker(ShardId shardId) {
        return trackers.get(shardId);
    }

    // --- DataFormatStatsProvider ---

    @Override
    public String formatName() {
        return FORMAT_NAME;
    }

    @Override
    public Optional<CompositeShardStats> shardStats(ShardId shardId) {
        CompositeShardStatsTracker tracker = trackers.get(shardId);
        return tracker == null ? Optional.empty() : Optional.of(tracker.stats());
    }

    @Override
    public Optional<CompositeShardStats> aggregateNodeStats() {
        if (trackers.isEmpty()) {
            return Optional.empty();
        }
        CompositeShardStats agg = CompositeShardStats.empty();
        for (CompositeShardStatsTracker t : trackers.values()) {
            agg = agg.add(t.stats());
        }
        return Optional.of(agg);
    }

    @Override
    public Writeable.Reader<CompositeShardStats> shardStatsReader() {
        return CompositeShardStats::new;
    }
}
