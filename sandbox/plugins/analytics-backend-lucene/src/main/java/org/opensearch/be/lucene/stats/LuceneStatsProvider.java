/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugin.stats.DataFormatStatsProvider;
import org.opensearch.plugin.stats.DataFormatStatsProviderRegistry;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lucene implementation of {@link DataFormatStatsProvider}.
 *
 * <p>Maintains a per-shard registry of {@link LuceneShardStatsTracker} instances.
 * {@code LuceneIndexingExecutionEngine} self-registers on construction and
 * unregisters on close. The composite-engine plugin reads stats from this
 * provider via the {@link DataFormatStatsProvider} interface — no direct
 * dependency on lucene's concrete classes.
 *
 * <p>Singleton pattern: a static {@code INSTANCE} field is set during plugin
 * construction so the engine layer can register its tracker without DI plumbing.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LuceneStatsProvider implements DataFormatStatsProvider<LuceneShardStats> {

    public static final String FORMAT_NAME = "lucene";

    private static volatile LuceneStatsProvider INSTANCE;

    private final Map<ShardId, LuceneShardStatsTracker> trackers = new ConcurrentHashMap<>();

    public LuceneStatsProvider() {
        if (INSTANCE == null) {
            INSTANCE = this;
        }
        DataFormatStatsProviderRegistry.INSTANCE.register(this);
    }

    /** Returns the singleton instance, or {@code null} if the plugin has not been constructed yet. */
    public static LuceneStatsProvider getInstance() {
        return INSTANCE;
    }

    /** Registers a tracker for a shard. Called from {@code LuceneIndexingExecutionEngine}'s constructor. */
    public void register(ShardId shardId, LuceneShardStatsTracker tracker) {
        trackers.put(shardId, tracker);
    }

    /** Unregisters a tracker. Called from the engine's {@code close()} method. */
    public void unregister(ShardId shardId) {
        trackers.remove(shardId);
    }

    /**
     * Returns the registered tracker for a shard, or null if none.
     * Used by {@code LuceneCommitter} and {@code LuceneDeleteExecutionEngine} to increment
     * their counters into the same tracker the engine registered.
     */
    public LuceneShardStatsTracker getTracker(ShardId shardId) {
        return trackers.get(shardId);
    }

    // --- DataFormatStatsProvider ---

    @Override
    public String formatName() {
        return FORMAT_NAME;
    }

    @Override
    public Optional<LuceneShardStats> shardStats(ShardId shardId) {
        LuceneShardStatsTracker tracker = trackers.get(shardId);
        if (tracker == null) {
            return Optional.empty();
        }
        return Optional.of(tracker.stats());
    }

    @Override
    public Optional<LuceneShardStats> aggregateNodeStats() {
        if (trackers.isEmpty()) {
            return Optional.empty();
        }
        LuceneShardStats agg = LuceneShardStats.empty();
        for (LuceneShardStatsTracker t : trackers.values()) {
            agg = agg.add(t.stats());
        }
        return Optional.of(agg);
    }

    @Override
    public Writeable.Reader<LuceneShardStats> shardStatsReader() {
        return LuceneShardStats::new;
    }
}
