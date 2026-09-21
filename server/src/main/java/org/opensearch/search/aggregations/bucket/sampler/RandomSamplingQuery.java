/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.sampler;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.util.BitMixer;

import java.io.IOException;
import java.util.Objects;
import java.util.SplittableRandom;

/**
 * Matches a uniform random sample of the documents in each segment, including deleted documents: callers are expected
 * to intersect the result with the live documents and with whatever query they care about.
 * <p>
 * The sample is drawn by skipping documents according to the geometric distribution
 * {@code (1 - p)^(k - 1) * p}, which visits only about {@code p * maxDoc} documents per segment instead of all of
 * them. Because the skips are drawn from a stream seeded per segment, the sampled set of a segment does not depend on
 * the query it is intersected with, and repeats for a given seed as long as the segment is unchanged.
 *
 * @opensearch.internal
 */
public final class RandomSamplingQuery extends Query {

    private final double probability;
    private final int seed;

    /**
     * @param probability the probability that any given document is sampled, in {@code (0, 1]}
     * @param seed        seed for the sampling; combined with a per-segment value so that segments sample independently
     */
    public RandomSamplingQuery(double probability, int seed) {
        if (Double.isNaN(probability) || probability <= 0.0 || probability > 1.0) {
            throw new IllegalArgumentException("[probability] must be greater than 0.0 and at most 1.0, but was [" + probability + "]");
        }
        this.probability = probability;
        this.seed = seed;
    }

    /**
     * Derives the seed for one segment. The segment name is used rather than {@link LeafReaderContext#ord} because the
     * ordinal changes whenever the searcher's list of segments changes, not only when a segment itself changes, which
     * would needlessly re-draw the sample. Readers that cannot be unwrapped to a segment reader have no segment name,
     * so they fall back to the ordinal.
     */
    static long segmentSeed(int seed, LeafReaderContext ctx) {
        final long salt;
        try {
            salt = BitMixer.mix64(Lucene.segmentReader(ctx.reader()).getSegmentName().hashCode());
        } catch (IllegalStateException e) {
            return BitMixer.mix64(seed) ^ BitMixer.mix64(ctx.ord);
        }
        return BitMixer.mix64(seed) ^ salt;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final int maxDoc = context.reader().maxDoc();
                final DocIdSetIterator iterator = new RandomSamplingIterator(maxDoc, probability, segmentSeed(seed, context));
                return ConstantScoreScorerSupplier.fromIterator(iterator, score(), scoreMode, maxDoc);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // Caching would pin one sample for the lifetime of the cache entry, which is both surprising and
                // pointless: the sample is cheap to redraw and callers ask for a fresh seed when they want a fresh one.
                return false;
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "RandomSamplingQuery(probability=" + probability + ", seed=" + seed + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) {
            return false;
        }
        RandomSamplingQuery that = (RandomSamplingQuery) other;
        return Double.compare(probability, that.probability) == 0 && seed == that.seed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), probability, seed);
    }

    /**
     * Walks a segment by geometric skips. The sequence of sampled documents is fixed once the iterator is created:
     * {@link #advance(int)} moves along that sequence rather than drawing a new skip from its target, so the sampled
     * set stays independent of the documents a conjunction happens to feed it.
     *
     * @opensearch.internal
     */
    static final class RandomSamplingIterator extends DocIdSetIterator {

        private final int maxDoc;
        private final double logOneMinusProbability;
        private final SplittableRandom random;
        private final long cost;
        private int doc = -1;

        RandomSamplingIterator(int maxDoc, double probability, long seed) {
            this.maxDoc = maxDoc;
            // log1p is accurate for the small probabilities this aggregation is meant for, where 1 - p loses precision.
            this.logOneMinusProbability = Math.log1p(-probability);
            this.random = new SplittableRandom(seed);
            this.cost = Math.max(1L, (long) (probability * maxDoc));
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            if (doc == NO_MORE_DOCS) {
                return NO_MORE_DOCS;
            }
            final int next = doc + nextSkip();
            // The skip can overflow for very small probabilities, in which case there is no next document anyway.
            return doc = (next >= maxDoc || next < 0) ? NO_MORE_DOCS : next;
        }

        @Override
        public int advance(int target) {
            while (doc < target) {
                nextDoc();
            }
            return doc;
        }

        @Override
        public long cost() {
            return cost;
        }

        /**
         * Draws the gap to the next sampled document from the geometric distribution {@code (1 - p)^(k - 1) * p} by
         * inverting its cumulative distribution function. The result is at least 1.
         */
        private int nextSkip() {
            // nextDouble() is in [0, 1), so 1 - u is in (0, 1] and its log is defined.
            final double skip = Math.ceil(Math.log(1.0 - random.nextDouble()) / logOneMinusProbability);
            if (skip >= maxDoc) {
                return maxDoc;
            }
            return Math.max(1, (int) skip);
        }
    }
}
