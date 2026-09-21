/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.sampler;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.SingleBucketAggregator;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Collects a uniform random sample of the documents matching the query into a single bucket.
 * <p>
 * There are two collection paths. When this aggregation is at the top level it drives its own iteration: it intersects
 * the sample with the query and collects only what falls in both, so documents outside the sample are never visited.
 * Otherwise, documents arrive from a parent aggregation and the only thing left to do is test each one against the
 * sample. The second path produces the same results as the first, more slowly, and is what the first is tested against.
 *
 * @opensearch.internal
 */
public class RandomSamplerAggregator extends BucketsAggregator implements SingleBucketAggregator {

    private final double probability;
    private final int seed;
    /**
     * Null when {@code probability} is 1.0, where there is nothing to sample away.
     */
    private final Weight samplingWeight;
    /**
     * The query's weight, built on first use by the fast path and reused for the remaining segments. Creating it can
     * cost term statistics lookups, so it must not be rebuilt per segment. Only ever touched from the thread
     * collecting this aggregator's slice.
     */
    private Weight queryWeight;

    RandomSamplerAggregator(
        String name,
        double probability,
        int seed,
        AggregatorFactories factories,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, cardinality, metadata);
        this.probability = probability;
        this.seed = seed;
        if (probability < 1.0) {
            RandomSamplingQuery samplingQuery = new RandomSamplingQuery(probability, seed);
            this.samplingWeight = context.searcher()
                .createWeight(context.searcher().rewrite(samplingQuery), ScoreMode.COMPLETE_NO_SCORES, 1f);
        } else {
            this.samplingWeight = null;
        }
    }

    @Override
    protected boolean tryPrecomputeAggregationForLeaf(LeafReaderContext ctx) throws IOException {
        if (parent() != null || samplingWeight == null) {
            // Under a parent aggregation the documents are pushed to us, so there is no iteration to drive, and with a
            // probability of 1.0 there is nothing to skip.
            return false;
        }

        ScorerSupplier samplingScorerSupplier = samplingWeight.scorerSupplier(ctx);
        if (samplingScorerSupplier == null) {
            return true;
        }
        if (queryWeight == null) {
            queryWeight = context.searcher().createWeight(context.searcher().rewrite(context.query()), scoreMode(), 1f);
        }
        ScorerSupplier queryScorerSupplier = queryWeight.scorerSupplier(ctx);
        if (queryScorerSupplier == null) {
            // Nothing in this segment matches the query, so the sample of it is empty.
            return true;
        }

        // Normally BucketsAggregator does this from preGetSubLeafCollectors, which the precompute path skips. Without
        // it the _doc_count field of pre-aggregated documents would be ignored.
        preGetSubLeafCollectors(ctx);
        LeafBucketCollector sub = collectableSubAggregators.getLeafCollector(ctx);

        Scorer samplingScorer = samplingScorerSupplier.get(Long.MAX_VALUE);
        DocIdSetIterator samplingIterator = samplingScorer.iterator();
        // The sample is the sparse side of the conjunction, so let the query optimise for being driven by it.
        Scorer queryScorer = queryScorerSupplier.get(samplingIterator.cost());
        sub.setScorer(queryScorer);

        // Weight#scorerSupplier does not apply deleted documents; that is normally the searcher's job.
        final Bits liveDocs = ctx.reader().getLiveDocs();
        DocIdSetIterator sampledMatches = ConjunctionUtils.intersectIterators(Arrays.asList(samplingIterator, queryScorer.iterator()));
        for (int doc = sampledMatches.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = sampledMatches.nextDoc()) {
            if (liveDocs == null || liveDocs.get(doc)) {
                collectBucket(sub, doc, 0);
            }
        }
        return true;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (samplingWeight == null) {
            return new LeafBucketCollectorBase(sub, null) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    collectBucket(sub, doc, bucket);
                }
            };
        }
        final Bits sampled = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), samplingWeight.scorerSupplier(ctx));
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (sampled.get(doc)) {
                    collectBucket(sub, doc, bucket);
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(
            owningBucketOrds,
            (owningBucketOrd, subAggregationResults) -> new InternalRandomSampler(
                name,
                bucketDocCount(owningBucketOrd),
                seed,
                probability,
                subAggregationResults,
                metadata()
            )
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalRandomSampler(name, 0, seed, probability, buildEmptySubAggregations(), metadata());
    }
}
