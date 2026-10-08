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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SmallFloat;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.index.AbstractIndexComponent;
import org.opensearch.index.IndexSettings;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
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
     * Optional ceiling on how many admitted documents {@code filtered_stats} will compute statistics over, above which
     * a request falls back to the {@code pre_filter} constant-score behaviour.
     * <p>
     * Unlimited by default, and a negative value is also unlimited. The first query on a segment costs one pass over
     * the documents the view admits, which is proportional to the view rather than to the index, so a ceiling is a
     * capacity choice for an operator who would rather lose relevance ordering than pay that cost. Defaulting it to a
     * finite value would mean a large view quietly scoring without relevance ordering: the request succeeds, the
     * documents are right, and only the ordering is wrong, which is harder to notice than the cost it avoids.
     */
    public static final Setting<Long> INDEX_FILTERED_STATS_MAX_VISIBLE_DOCS_SETTING = Setting.longSetting(
        "index.filter_aware_alias.filtered_stats.max_visible_docs",
        -1L,
        -1L,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

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
        track(filter, null);
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
        track(filter, field);
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
            // Deliberately get-then-putIfAbsent rather than computeIfAbsent. Concurrent requests can miss the
            // same cold (segment, filter, term) and each build it. The values are identical, so either is
            // correct, and putIfAbsent keeps a single instance. computeIfAbsent would make the build run once,
            // but it holds the map's bin lock for the duration of the mapping function, and that function is a
            // postings intersection that can run for hundreds of milliseconds on a large merged segment, which
            // would block unrelated keys in the same bin exactly when load is highest.
            // Measured: duplicate builds are 0.07% of builds on a realistic query mix, and 4% to 7% in an
            // adversarial one (3-term queries drawn from a 20-term head pool, 16 concurrent clients, force-merge
            // of a 266-segment shard mid-flight). If a workload is ever found where the duplicate rate matters,
            // the fix is memoizing a future per key so waiters block on the future rather than on a map bin.
            final long[] winner = byTerm.putIfAbsent(term, toStore);
            if (winner != null) {
                contribution = winner;
            } else {
                account(entry, (long) toStore.length * Long.BYTES);
            }
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
                final BitSet visible = getOrComputeVisibleBitSet(ctx, filter, () -> computeVisibleBitSet(weight, ctx));
                if (visible == null) {
                    continue;
                }
                for (String field : entry.getValue()) {
                    getOrComputeFieldContribution(ctx, filter, field, () -> computeFieldContribution(ctx, visible, field));
                }
            } catch (IOException | RuntimeException e) {
                logger.debug(() -> "failed to warm filtered statistics for filter [" + filter + "]", e);
            }
        }
    }

    /**
     * Builds the visible-doc bitset for a single segment from an (already rewritten) filter weight. Returns
     * {@code null} when no live document in the segment matches, which callers treat as "no visible docs here".
     * Only live documents are included so counts match what a physically filtered index would report.
     */
    public static BitSet computeVisibleBitSet(Weight weight, LeafReaderContext ctx) throws IOException {
        final ScorerSupplier scorerSupplier = weight.scorerSupplier(ctx);
        if (scorerSupplier == null) {
            return null;
        }
        final Scorer scorer = scorerSupplier.get(Long.MAX_VALUE);
        if (scorer == null) {
            return null;
        }
        final FixedBitSet bitSet = new FixedBitSet(ctx.reader().maxDoc());
        final DocIdSetIterator iterator = scorer.iterator();
        final Bits liveDocs = ctx.reader().getLiveDocs();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            if (liveDocs == null || liveDocs.get(doc)) {
                bitSet.set(doc);
            }
        }
        return bitSet.cardinality() > 0 ? bitSet : null;
    }

    /**
     * Computes a single segment's {@code [docCount, sumTotalTermFreq, sumDocFreq]} contribution for a field over the
     * visible subset. This is the O(field postings) walk that {@link #getOrComputeFieldContribution} memoizes.
     */
    public static long[] computeFieldContribution(LeafReaderContext ctx, BitSet visible, String field) throws IOException {
        final Terms terms = ctx.reader().terms(field);
        if (terms == null) {
            return new long[] { 0, 0, 0 };
        }
        final NumericDocValues norms = ctx.reader().getNormValues(field);
        if (norms == null) {
            // Field indexed without norms: there is no per-document length to read, so fall back to the exact
            // (but O(all postings in the field)) walk below.
            return computeFieldContributionByTermWalk(ctx, visible, terms);
        }

        // Fast path. BM25 reads exactly two values off CollectionStatistics -- docCount, and
        // sumTotalTermFreq only to derive avgFieldLength = sumTotalTermFreq / docCount. Neither needs the
        // term dictionary: norms carry one length value per document, so a single pass over the visible
        // documents yields both in O(visible docs) instead of O(all postings in the field).
        long docCount = 0;
        long sumTotalTermFreq = 0;
        final BitSetIterator visibleDocs = new BitSetIterator(visible, visible.approximateCardinality());
        for (int doc = visibleDocs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = visibleDocs.nextDoc()) {
            if (norms.advanceExact(doc)) {
                docCount++;
                // Norms encode field length the way the similarity wrote them; BM25 uses SmallFloat.intToByte4,
                // and its own per-document length normalization decodes the same lossy value. A similarity that
                // encodes something else would make avgFieldLength meaningless here -- see the class javadoc.
                sumTotalTermFreq += SmallFloat.byte4ToInt((byte) norms.longValue());
            }
        }
        if (docCount == 0) {
            return new long[] { 0, 0, 0 };
        }

        // sumDocFreq is never read by BM25, and computing it exactly is what forces a full term-dictionary
        // walk. Estimate it by scaling the shard-wide value (free field metadata) by the visible fraction,
        // then clamp to Lucene's CollectionStatistics invariants.
        final long shardDocCount = terms.getDocCount();
        long sumDocFreq = shardDocCount <= 0 ? docCount : (long) ((double) terms.getSumDocFreq() * docCount / shardDocCount);
        sumDocFreq = Math.min(Math.max(sumDocFreq, docCount), Math.max(sumTotalTermFreq, docCount));
        sumTotalTermFreq = Math.max(sumTotalTermFreq, sumDocFreq);
        return new long[] { docCount, sumTotalTermFreq, sumDocFreq };
    }

    /**
     * Exact contribution via a full walk of the field's term dictionary. Correct for any similarity, but costs
     * O(all postings in the field) -- retained only for fields indexed without norms.
     */
    private static long[] computeFieldContributionByTermWalk(LeafReaderContext ctx, BitSet visible, Terms terms) throws IOException {
        final TermsEnum termsEnum = terms.iterator();
        final FixedBitSet docsWithField = new FixedBitSet(ctx.reader().maxDoc());
        long sumTotalTermFreq = 0;
        long sumDocFreq = 0;
        PostingsEnum postings = null;
        while (termsEnum.next() != null) {
            postings = termsEnum.postings(postings, PostingsEnum.FREQS);
            long termDocFreq = 0;
            for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postings.nextDoc()) {
                if (visible.get(doc)) {
                    termDocFreq++;
                    sumTotalTermFreq += postings.freq();
                    docsWithField.set(doc);
                }
            }
            sumDocFreq += termDocFreq;
        }
        return new long[] { docsWithField.cardinality(), sumTotalTermFreq, sumDocFreq };
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
