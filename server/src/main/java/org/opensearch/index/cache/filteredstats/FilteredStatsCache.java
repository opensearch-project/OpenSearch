/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.filteredstats;

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
import org.opensearch.index.AbstractIndexComponent;
import org.opensearch.index.IndexSettings;

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
        return computeVisibleBitSet(searcher.createWeight(searcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1f), ctx);
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
                final FilteredSegmentStats segment = segment(ctx, filter, () -> computeVisibleBitSet(weight, ctx));
                final BitSet visible = segment.visibleDocs();
                if (visible == null) {
                    continue;
                }
                for (String field : entry.getValue()) {
                    segment.field(field, () -> computeFieldContribution(ctx, visible, field));
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
