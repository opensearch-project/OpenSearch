/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.SmallFloat;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.settings.Setting;

import java.io.IOException;
import java.util.Arrays;

/**
 * Computes relevance statistics over the documents a filter admits in one segment, and holds the setting that bounds
 * how many documents a request may scope them to.
 *
 * @opensearch.internal
 */
@InternalApi
public final class VisibleSubsetStatistics {

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
     * visible subset. Costs one pass over the visible documents when the field has norms.
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

    /**
     * Computes a single segment's {@code [docFreq, totalTermFreq]} contribution for a term over the visible subset,
     * or {@code null} when the term occurs in no visible doc in this segment.
     */
    public static long[] computeTermContribution(LeafReaderContext ctx, BitSet visible, Term term) throws IOException {
        final Terms terms = ctx.reader().terms(term.field());
        if (terms == null) {
            return null;
        }
        final TermsEnum termsEnum = terms.iterator();
        if (termsEnum.seekExact(term.bytes()) == false) {
            return null;
        }
        final PostingsEnum postings = termsEnum.postings(null, PostingsEnum.FREQS);
        // Leapfrog the term's postings against the visible bitset instead of scanning the postings linearly and
        // testing each hit: the conjunction advances whichever side is behind, so skip lists do the work and the
        // cost is O(min(visible, docFreq)) rather than O(docFreq). The more selective the view, the cheaper it gets.
        final DocIdSetIterator visibleAndTerm = ConjunctionUtils.intersectIterators(
            Arrays.asList(new BitSetIterator(visible, visible.approximateCardinality()), postings)
        );
        long docFreq = 0;
        long totalTermFreq = 0;
        for (int doc = visibleAndTerm.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = visibleAndTerm.nextDoc()) {
            docFreq++;
            totalTermFreq += postings.freq();
        }
        return docFreq == 0 ? null : new long[] { docFreq, totalTermFreq };
    }

    /** Heap bytes of the visible-doc bitset for a segment with {@code maxDoc} documents. */
    public static long visibleBitSetBytes(int maxDoc) {
        return FIXED_BIT_SET_SHALLOW_BYTES + RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) FixedBitSet.bits2words(maxDoc) * Long.BYTES
        );
    }

    private static final long FIXED_BIT_SET_SHALLOW_BYTES = RamUsageEstimator.shallowSizeOfInstance(FixedBitSet.class);

    private VisibleSubsetStatistics() {}
}
