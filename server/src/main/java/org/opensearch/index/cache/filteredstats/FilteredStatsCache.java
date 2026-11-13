/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.filteredstats;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.AbstractIndexComponent;
import org.opensearch.index.IndexSettings;
import org.opensearch.search.internal.VisibleSubsetStatistics;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One index's view of the visible-subset statistics used when relevance statistics are scoped to a filter.
 * <p>
 * Scoped statistics are computed over only the documents a filter admits: a per-segment bitset of those documents,
 * then per-field and per-term counts over it. Segments are immutable and a view's filter is stable, so the result is
 * the same for every query on the same (segment, filter) pair, and it is cached after the first.
 * <p>
 * Entries live in the node-level {@link IndicesFilteredStatsCache}, which bounds them in bytes across all indices and
 * drops a segment's entries when the segment closes. This class tracks what one index holds there, and which filters
 * and fields it uses so {@link FilteredStatsWarmer} can pre-populate new segments.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FilteredStatsCache extends AbstractIndexComponent implements Closeable {

    /**
     * A computation that produces a value to cache on the first miss for a (segment, filter[, field|term]) key.
     * Separate from {@code java.util.function.Supplier} so it can throw {@link IOException} from the postings walk.
     */
    @ExperimentalApi
    @FunctionalInterface
    public interface Computation<T> {
        T compute() throws IOException;
    }

    private final IndicesFilteredStatsCache nodeCache;
    // Bytes this index holds in the node-level cache.
    private final AtomicLong bytes = new AtomicLong();

    /**
     * Creates the view of one index into the node-level cache.
     *
     * @param indexSettings settings of the index
     * @param nodeCache the node-level cache entries are stored in
     */
    public FilteredStatsCache(IndexSettings indexSettings, IndicesFilteredStatsCache nodeCache) {
        super(indexSettings);
        this.nodeCache = Objects.requireNonNull(nodeCache);
    }

    /**
     * Returns the visible-subset statistics of a segment under a filter, building the visible bitset on a miss. Hold the
     * result for the rest of the request: lookups on it after the first do not touch the node-level cache.
     *
     * @param ctx the segment
     * @param filter the rewritten filter that admits the visible docs
     * @param compute builds the visible bitset, returning {@code null} when no doc is visible
     * @return the segment's statistics
     */
    public FilteredSegmentStats segment(LeafReaderContext ctx, Query filter, Computation<BitSet> compute) throws IOException {
        track(filter, null);
        return nodeCache.segment(this, ctx, filter, compute);
    }

    /** Returns the visible-doc bitset for {@code ctx} under {@code filter}, or {@code null} when no doc is visible. */
    public BitSet getOrComputeVisibleBitSet(LeafReaderContext ctx, Query filter, Computation<BitSet> compute) throws IOException {
        return segment(ctx, filter, compute).visibleDocs();
    }

    /** Returns the visible-subset {@code [docCount, sumTotalTermFreq, sumDocFreq]} for {@code (ctx, filter, field)}. */
    public long[] getOrComputeFieldContribution(LeafReaderContext ctx, Query filter, String field, Computation<long[]> compute)
        throws IOException {
        track(filter, field);
        return segment(ctx, filter, () -> computeVisibleBitSetFor(ctx, filter)).field(field, compute);
    }

    /**
     * Returns the visible-subset {@code [docFreq, totalTermFreq]} for {@code (ctx, filter, term)}, or {@code null} when
     * no visible doc contains the term; that answer is cached too.
     */
    public long[] getOrComputeTermContribution(LeafReaderContext ctx, Query filter, Term term, Computation<long[]> compute)
        throws IOException {
        return segment(ctx, filter, () -> computeVisibleBitSetFor(ctx, filter)).term(term, compute);
    }

    private static BitSet computeVisibleBitSetFor(LeafReaderContext ctx, Query filter) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(ctx.parent);
        searcher.setQueryCache(null);
        return VisibleSubsetStatistics.computeVisibleBitSet(
            searcher.createWeight(searcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1f),
            ctx
        );
    }

    void addBytes(long delta) {
        bytes.addAndGet(delta);
    }

    /**
     * Filters (and, per filter, the fields) this cache has actually been asked about, so a warmer can pre-populate
     * exactly what is in use on a newly visible segment rather than guessing. Bounded so an unusual workload that
     * cycles through many distinct filters cannot grow this without limit.
     */
    private static final int MAX_TRACKED_FILTERS = 32;
    private static final int MAX_TRACKED_FIELDS_PER_FILTER = 32;
    private final Map<Query, Set<String>> inUse = new ConcurrentHashMap<>();

    private void track(Query filter, String field) {
        Set<String> fields = inUse.get(filter);
        if (fields == null) {
            if (inUse.size() >= MAX_TRACKED_FILTERS) {
                return;
            }
            fields = inUse.computeIfAbsent(filter, f -> ConcurrentHashMap.newKeySet());
        }
        if (field != null && fields.size() < MAX_TRACKED_FIELDS_PER_FILTER) {
            fields.add(field);
        }
    }

    /**
     * Pre-populates the visible bitset and per-field statistics for {@code ctx} for every (filter, field) pair this
     * cache has seen in use, so the first query to touch a newly refreshed or merged segment does not pay the
     * O(visible postings) build itself. Best-effort: any failure is logged and skipped, because warming is an
     * optimization and the query path recomputes on a miss anyway.
     */
    public void warm(IndexSearcher searcher, LeafReaderContext ctx) {
        for (Map.Entry<Query, Set<String>> entry : inUse.entrySet()) {
            final Query filter = entry.getKey();
            try {
                final Weight weight = searcher.createWeight(searcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1f);
                final FilteredSegmentStats segment = segment(ctx, filter, () -> VisibleSubsetStatistics.computeVisibleBitSet(weight, ctx));
                final BitSet visible = segment.visibleDocs();
                if (visible == null) {
                    continue;
                }
                for (String field : entry.getValue()) {
                    segment.field(field, () -> VisibleSubsetStatistics.computeFieldContribution(ctx, visible, field));
                }
            } catch (IOException | RuntimeException e) {
                logger.debug(() -> "failed to warm filtered statistics for filter [" + filter + "]", e);
            }
        }
    }

    /** Bytes this index currently holds in the node-level cache. */
    public long ramBytesUsed() {
        return bytes.get();
    }

    /**
     * Drops every entry cached for this index.
     *
     * @param reason why the cache is cleared, for the log
     */
    public void clear(String reason) {
        logger.debug("clearing filtered-stats cache because [{}]", reason);
        nodeCache.clear(this);
    }

    /** Entries are invalidated as the index's segments close, so closing the index has nothing left to do. */
    @Override
    public void close() {}
}
