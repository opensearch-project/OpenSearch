/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.index.LeafReaderContext;
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
     * a request falls back to the {@code no_stats} constant-score behaviour.
     * <p>
     * Unlimited by default, and a negative value is also unlimited. Scoped statistics cost a bitset of the admitted
     * documents per segment and a walk of each queried field's postings against it, so a ceiling is a capacity choice
     * for an operator who would rather lose relevance ordering than pay that cost. Defaulting it to a
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
     * Makes a nested child visible exactly when its root document is, as in an index that held only the visible root
     * documents and their children. A child that the filter matched on its own is dropped unless its root is visible.
     *
     * @param visible the documents the filter admits in this segment
     * @param parents the root (non-nested) documents of this segment
     * @return the visible root documents and their nested children, or {@code null} when there are none
     */
    public static BitSet withNestedChildren(BitSet visible, BitSet parents) {
        if (parents == null) {
            return null;
        }
        final FixedBitSet expanded = new FixedBitSet(visible.length());
        final BitSetIterator roots = new BitSetIterator(visible, visible.approximateCardinality());
        for (int doc = roots.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = roots.nextDoc()) {
            if (parents.get(doc)) {
                // A block is its children followed by the root, so the children run from just after the previous root.
                final int firstChild = doc == 0 ? 0 : parents.prevSetBit(doc - 1) + 1;
                expanded.set(firstChild, doc + 1);
            }
        }
        return expanded.cardinality() > 0 ? expanded : null;
    }

    /**
     * Computes a single segment's {@code [docCount, sumTotalTermFreq, sumDocFreq]} contribution for a field over the
     * visible subset. The values are exact, the same ones Lucene would report for an index that held only the
     * visible documents, because they are counted from the postings rather than derived from lossy norms.
     * Each term's postings are intersected with the visible set, so a small view skips most of them.
     */
    public static long[] computeFieldContribution(LeafReaderContext ctx, BitSet visible, String field) throws IOException {
        final Terms terms = ctx.reader().terms(field);
        if (terms == null) {
            return new long[] { 0, 0, 0 };
        }
        final FixedBitSet docsWithField = new FixedBitSet(ctx.reader().maxDoc());
        final long visibleCost = visible.approximateCardinality();
        final TermsEnum termsEnum = terms.iterator();
        long sumTotalTermFreq = 0;
        long sumDocFreq = 0;
        PostingsEnum postings = null;
        while (termsEnum.next() != null) {
            postings = termsEnum.postings(postings, PostingsEnum.FREQS);
            final DocIdSetIterator visibleAndTerm = ConjunctionUtils.intersectIterators(
                Arrays.asList(new BitSetIterator(visible, visibleCost), postings)
            );
            for (int doc = visibleAndTerm.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = visibleAndTerm.nextDoc()) {
                sumDocFreq++;
                sumTotalTermFreq += postings.freq();
                docsWithField.set(doc);
            }
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
