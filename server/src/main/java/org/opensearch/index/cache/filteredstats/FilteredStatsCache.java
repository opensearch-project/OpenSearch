/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.filteredstats;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.index.AbstractIndexComponent;
import org.opensearch.index.IndexSettings;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-index cache of the visible-subset statistics used by the filter-aware-alias {@code filtered_stats} path.
 * <p>
 * When a search runs {@code filtered_stats}, BM25 collection/term statistics are recomputed over only the documents
 * that match the alias filter. That recomputation is O(total corpus) per query: it builds a per-segment "visible"
 * bitset from the filter and then walks postings intersected with that bitset. Because Lucene segments are immutable
 * and the alias filter is stable for a given view, the result is identical across every query hitting the same
 * (segment, filter) pair -- so it is cached here and only computed on the first query per segment.
 * <p>
 * Entries are keyed by the segment core {@link IndexReader.CacheKey} and evicted via a
 * {@link IndexReader.CacheHelper#addClosedListener close listener}: when a segment is replaced on refresh or merge its
 * core closes, its entry drops, and the next query recomputes against the new segment. This gives correct freshness
 * with no explicit invalidation.
 * <p>
 * Cached bytes are reported to the fielddata circuit breaker with {@link CircuitBreaker#addWithoutBreaking}
 * (accounting only -- this cache never rejects), and returned on eviction.
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

    /** Sentinel {@code long[]} stored for a (segment, filter, term) that has no visible occurrences, so the "absent" answer is cached too. */
    private static final long[] TERM_ABSENT = new long[0];

    /**
     * Sentinel stored for a (segment, filter) with no visible docs. {@link ConcurrentHashMap} forbids {@code null}
     * values, so we cache this identity and translate it back to {@code null} on read (the "no visible docs" contract).
     */
    private static final BitSet NO_VISIBLE_DOCS = new FixedBitSet(1);

    /** Per-segment cached statistics for the visible subset, sub-keyed by the alias filter query. */
    private static final class SegmentEntry {
        // filter -> visible-doc bitset for this segment (may be null when no visible docs match in this segment).
        private final Map<Query, BitSet> visibleBitSets = new ConcurrentHashMap<>();
        // filter -> field -> [docCount, sumTotalTermFreq, sumDocFreq] over the visible subset.
        private final Map<Query, Map<String, long[]>> fieldContrib = new ConcurrentHashMap<>();
        // filter -> term -> [docFreq, totalTermFreq] over the visible subset (TERM_ABSENT when none).
        private final Map<Query, Map<Term, long[]>> termContrib = new ConcurrentHashMap<>();
        // approximate bytes this entry has reported to the breaker, returned when the segment closes.
        private final AtomicLong bytes = new AtomicLong();
    }

    private final ConcurrentHashMap<IndexReader.CacheKey, SegmentEntry> entries = new ConcurrentHashMap<>();
    private final CircuitBreaker accountingBreaker;

    public FilteredStatsCache(IndexSettings indexSettings, CircuitBreaker accountingBreaker) {
        super(indexSettings);
        this.accountingBreaker = accountingBreaker;
    }

    /**
     * Returns the visible-doc bitset for {@code ctx} under {@code filter}, computing and caching it on first access.
     * A cached {@code null} (no visible docs in this segment) is preserved and returned as {@code null}.
     */
    public BitSet getOrComputeVisibleBitSet(LeafReaderContext ctx, Query filter, Computation<BitSet> compute) throws IOException {
        final SegmentEntry entry = entryFor(ctx);
        if (entry == null) {
            return compute.compute();
        }
        final BitSet cached = entry.visibleBitSets.get(filter);
        if (cached != null) {
            return cached == NO_VISIBLE_DOCS ? null : cached;
        }
        final BitSet bitSet = compute.compute();
        entry.visibleBitSets.put(filter, bitSet == null ? NO_VISIBLE_DOCS : bitSet);
        if (bitSet != null) {
            account(entry, bitSet.ramBytesUsed());
        }
        return bitSet;
    }

    /**
     * Returns the visible-subset field contribution {@code [docCount, sumTotalTermFreq, sumDocFreq]} for
     * {@code (ctx, filter, field)}, computing and caching it on first access.
     */
    public long[] getOrComputeFieldContribution(LeafReaderContext ctx, Query filter, String field, Computation<long[]> compute)
        throws IOException {
        final SegmentEntry entry = entryFor(ctx);
        if (entry == null) {
            return compute.compute();
        }
        final Map<String, long[]> byField = entry.fieldContrib.computeIfAbsent(filter, f -> new ConcurrentHashMap<>());
        long[] contribution = byField.get(field);
        if (contribution == null) {
            contribution = compute.compute();
            byField.put(field, contribution);
            account(entry, (long) contribution.length * Long.BYTES);
        }
        return contribution;
    }

    /**
     * Returns the visible-subset term contribution {@code [docFreq, totalTermFreq]} for {@code (ctx, filter, term)},
     * or {@code null} when the term has no visible occurrences. Computed and cached on first access; the "absent"
     * answer is cached as well so repeated probes of a filtered-out term stay cheap.
     */
    public long[] getOrComputeTermContribution(LeafReaderContext ctx, Query filter, Term term, Computation<long[]> compute)
        throws IOException {
        final SegmentEntry entry = entryFor(ctx);
        if (entry == null) {
            return compute.compute();
        }
        final Map<Term, long[]> byTerm = entry.termContrib.computeIfAbsent(filter, f -> new ConcurrentHashMap<>());
        long[] contribution = byTerm.get(term);
        if (contribution == null) {
            contribution = compute.compute();
            final long[] toStore = contribution == null ? TERM_ABSENT : contribution;
            byTerm.put(term, toStore);
            account(entry, (long) toStore.length * Long.BYTES);
        }
        return contribution == TERM_ABSENT ? null : contribution;
    }

    /**
     * Returns the cache entry for a segment, creating it (and registering the eviction listener) on first use.
     * Returns {@code null} when the segment exposes no core cache helper, so the caller falls back to inline compute.
     */
    private SegmentEntry entryFor(LeafReaderContext ctx) {
        final IndexReader.CacheHelper cacheHelper = ctx.reader().getCoreCacheHelper();
        if (cacheHelper == null) {
            return null;
        }
        final IndexReader.CacheKey key = cacheHelper.getKey();
        return entries.computeIfAbsent(key, k -> {
            cacheHelper.addClosedListener(this::onSegmentClose);
            return new SegmentEntry();
        });
    }

    private void onSegmentClose(IndexReader.CacheKey key) {
        final SegmentEntry removed = entries.remove(key);
        if (removed != null && accountingBreaker != null) {
            final long bytes = removed.bytes.getAndSet(0);
            if (bytes != 0) {
                accountingBreaker.addWithoutBreaking(-bytes);
            }
        }
    }

    private void account(SegmentEntry entry, long bytes) {
        if (bytes <= 0) {
            return;
        }
        entry.bytes.addAndGet(bytes);
        if (accountingBreaker != null) {
            accountingBreaker.addWithoutBreaking(bytes);
        }
    }

    /** Total bytes this cache is currently accounting for -- for tests and stats. */
    public long ramBytesUsed() {
        long total = 0;
        for (SegmentEntry entry : entries.values()) {
            total += entry.bytes.get();
        }
        return total;
    }

    public void clear(String reason) {
        logger.debug("clearing filtered-stats cache because [{}]", reason);
        for (IndexReader.CacheKey key : entries.keySet()) {
            onSegmentClose(key);
        }
    }

    @Override
    public void close() {
        clear("close");
    }
}
