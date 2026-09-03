/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.filteredstats;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Setting;
import org.opensearch.index.IndexWarmer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

/**
 * Warms {@link FilteredStatsCache} for segments that have just become visible.
 * <p>
 * The cache is per-segment, so a refresh or merge that produces a new segment leaves that segment's visible-subset
 * statistics uncomputed: the first query to touch it pays the O(visible postings) build. That is bounded (existing
 * segments stay warm) but shows up as an occasional tail-latency spike on a write-heavy index, and it repeats on every
 * node that has to build its own cache. This warmer moves that work off the query path and onto the warmer thread pool.
 * <p>
 * It only warms (filter, field) pairs the cache has actually seen in use, so it does no speculative work on an index
 * where nothing queries through a filtered alias. Warming is best-effort: on failure the query path simply recomputes.
 *
 * @opensearch.internal
 */
public final class FilteredStatsWarmer implements IndexWarmer.Listener {

    /**
     * Whether to pre-compute visible-subset statistics for newly visible segments. Off by default: it costs warmer-pool
     * work on every refresh, which is only worth paying for indices actually served through {@code pre_filter} aliases
     * with {@code filtered_stats} scoring.
     */
    public static final Setting<Boolean> INDEX_WARM_FILTERED_STATS_SETTING = Setting.boolSetting(
        "index.filter_aware_alias.warm_filtered_stats",
        false,
        Setting.Property.IndexScope
    );

    private final Executor executor;
    private final FilteredStatsCache cache;

    public FilteredStatsWarmer(ThreadPool threadPool, FilteredStatsCache cache) {
        this.executor = threadPool.executor(ThreadPool.Names.WARMER);
        this.cache = cache;
    }

    @Override
    public IndexWarmer.TerminationHandle warmReader(IndexShard indexShard, OpenSearchDirectoryReader reader) {
        if (cache == null || indexShard.indexSettings().getValue(INDEX_WARM_FILTERED_STATS_SETTING) == false) {
            return IndexWarmer.TerminationHandle.NO_WAIT;
        }
        // A plain searcher over this reader: we only need it to rewrite the filter and create weights, and we must not
        // pollute (or read) the query cache while doing so.
        final IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);

        final CountDownLatch latch = new CountDownLatch(reader.leaves().size());
        for (final LeafReaderContext ctx : reader.leaves()) {
            executor.execute(() -> {
                try {
                    cache.warm(searcher, ctx);
                } finally {
                    latch.countDown();
                }
            });
        }
        return latch::await;
    }
}
