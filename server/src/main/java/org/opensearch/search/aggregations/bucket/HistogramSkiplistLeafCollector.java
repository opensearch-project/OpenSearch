/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.Scorable;
import org.opensearch.common.Rounding;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;

import java.io.IOException;

/**
 * Histogram collection logic using skip list.
 *
 * @opensearch.internal
 */
public class HistogramSkiplistLeafCollector extends LeafBucketCollector {

    private final NumericDocValues values;
    private final DocValuesSkipper skipper;
    private final Rounding.Prepared preparedRounding;
    private final LongKeyedBucketOrds bucketOrds;
    private final LeafBucketCollector sub;
    private final BucketsAggregator aggregator;

    /**
     * Max doc ID (inclusive) up to which all docs values may map to the same
     * bucket.
     */
    private int upToInclusive = -1;

    /**
     * Whether all docs up to {@link #upToInclusive} values map to the same bucket.
     */
    private boolean upToSameBucket;

    /**
     * Index in bucketOrds for docs up to {@link #upToInclusive}.
     */
    private long upToBucketIndex;

    public HistogramSkiplistLeafCollector(
        NumericDocValues values,
        DocValuesSkipper skipper,
        Rounding.Prepared preparedRounding,
        LongKeyedBucketOrds bucketOrds,
        LeafBucketCollector sub,
        BucketsAggregator aggregator
    ) {
        this.values = values;
        this.skipper = skipper;
        this.preparedRounding = preparedRounding;
        this.bucketOrds = bucketOrds;
        this.sub = sub;
        this.aggregator = aggregator;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        if (sub != null) {
            sub.setScorer(scorer);
        }
    }

    private void advanceSkipper(int doc, long owningBucketOrd) throws IOException {
        if (doc > skipper.maxDocID(0)) {
            skipper.advance(doc);
        }
        upToSameBucket = false;

        if (skipper.minDocID(0) > doc) {
            // Corner case which happens if `doc` doesn't have a value and is between two
            // intervals of
            // the doc-value skip index.
            upToInclusive = skipper.minDocID(0) - 1;
            return;
        }

        upToInclusive = skipper.maxDocID(0);

        // Now find the highest level where all docs map to the same bucket.
        for (int level = 0; level < skipper.numLevels(); ++level) {
            int totalDocsAtLevel = skipper.maxDocID(level) - skipper.minDocID(level) + 1;
            long minBucket = preparedRounding.round(skipper.minValue(level));
            long maxBucket = preparedRounding.round(skipper.maxValue(level));

            if (skipper.docCount(level) == totalDocsAtLevel && minBucket == maxBucket) {
                // All docs at this level have a value, and all values map to the same bucket.
                upToInclusive = skipper.maxDocID(level);
                upToSameBucket = true;
                upToBucketIndex = bucketOrds.add(owningBucketOrd, maxBucket);
                if (upToBucketIndex < 0) {
                    upToBucketIndex = -1 - upToBucketIndex;
                }
            } else {
                break;
            }
        }
    }

    @Override
    public void collect(int doc, long owningBucketOrd) throws IOException {
        if (doc > upToInclusive) {
            advanceSkipper(doc, owningBucketOrd);
        }

        if (upToSameBucket) {
            aggregator.incrementBucketDocCount(upToBucketIndex, 1L);
            sub.collect(doc, upToBucketIndex);
        } else if (values.advanceExact(doc)) {
            final long value = values.longValue();
            long bucketIndex = bucketOrds.add(owningBucketOrd, preparedRounding.round(value));
            if (bucketIndex < 0) {
                bucketIndex = -1 - bucketIndex;
                aggregator.collectExistingBucket(sub, doc, bucketIndex);
            } else {
                aggregator.collectBucket(sub, doc, bucketIndex);
            }
        }
    }

    @Override
    public void collect(DocIdStream stream) throws IOException {
        // This will only be called if its the top agg
        collect(stream, 0);
    }

    @Override
    public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
        // This will only be called if its the sub aggregation
        for (;;) {
            int upToExclusive = upToInclusive + 1;
            if (upToExclusive < 0) { // overflow
                upToExclusive = Integer.MAX_VALUE;
            }

            if (upToSameBucket) {
                if (sub == NO_OP_COLLECTOR) {
                    // stream.count maybe faster when we don't need to handle sub-aggs
                    long count = stream.count(upToExclusive);
                    aggregator.incrementBucketDocCount(upToBucketIndex, count);
                } else {
                    final int[] count = { 0 };
                    stream.forEach(upToExclusive, doc -> {
                        sub.collect(doc, upToBucketIndex);
                        count[0]++;
                    });
                    aggregator.incrementBucketDocCount(upToBucketIndex, count[0]);
                }
            } else {
                stream.forEach(upToExclusive, doc -> collect(doc, owningBucketOrd));
            }

            if (stream.mayHaveRemaining()) {
                advanceSkipper(upToExclusive, owningBucketOrd);
            } else {
                break;
            }
        }
    }
}
