/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.metrics.InternalStringStats.Metrics;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregates the string values of the matching documents into their number, their lengths, and how often each character occurs, per
 * bucket. The memory for the characters is charged to the request circuit breaker, and a bucket may have at most
 * {@link InternalStringStats#MAX_DISTINCT_CHARACTERS} distinct characters.
 *
 * @opensearch.internal
 */
public class StringStatsAggregator extends NumericMetricsAggregator.MultiValue {

    private static final long BUCKET_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BucketStats.class);

    private final ValuesSource.Bytes valuesSource;
    private final boolean showDistribution;

    private ObjectArray<BucketStats> buckets;

    StringStatsAggregator(
        String name,
        @Nullable ValuesSource valuesSource,
        boolean showDistribution,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = (ValuesSource.Bytes) valuesSource;
        this.showDistribution = showDistribution;
        this.buckets = context.bigArrays().newObjectArray(1);
    }

    private boolean hasDataForBucket(long bucketOrd) {
        return bucketOrd < buckets.size() && buckets.get(bucketOrd) != null;
    }

    @Override
    public boolean hasMetric(String name) {
        try {
            Metrics.resolve(name);
            return true;
        } catch (IllegalArgumentException iae) {
            return false;
        }
    }

    @Override
    public double metric(String name, long owningBucketOrd) {
        final Metrics metric = Metrics.resolve(name);
        if (hasDataForBucket(owningBucketOrd) == false) {
            // a bucket without values has no values to count, and no lengths and no entropy to speak of
            return metric == Metrics.count ? 0.0 : Double.NaN;
        }
        final BucketStats stats = buckets.get(owningBucketOrd);
        switch (metric) {
            case count:
                return stats.count;
            case min_length:
                return stats.minLength;
            case max_length:
                return stats.maxLength;
            case avg_length:
                return (double) stats.totalCharacters / stats.count;
            case entropy:
                return stats.entropy();
            default:
                throw new AssertionError("unhandled metric [" + name + "]");
        }
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        } else {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        final BigArrays bigArrays = context.bigArrays();
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc) == false) {
                    return;
                }

                buckets = bigArrays.grow(buckets, bucket + 1);

                BucketStats stats = buckets.get(bucket);
                if (stats == null) {
                    stats = new BucketStats();
                    addRequestCircuitBreakerBytes(BUCKET_SHALLOW_SIZE + stats.accountedBytes);
                    buckets.set(bucket, stats);
                }

                final int valueCount = values.docValueCount();
                for (int i = 0; i < valueCount; i++) {
                    stats.add(values.nextValue());
                    // Checked after every value and not every document, so that a document with very many values can not take a
                    // bucket far past the limit or the breaker before either gets to refuse it. The limit is checked first, so that a
                    // bucket over it is told why instead of that the breaker tripped.
                    if (stats.characters.size() > InternalStringStats.MAX_DISTINCT_CHARACTERS) {
                        throw InternalStringStats.tooManyDistinctCharacters(name);
                    }
                    // The memory is charged once the value is in, so a single value can take the memory past what the breaker allows
                    // by as much as its own characters need, before the breaker gets to refuse it.
                    final long grown = stats.characters.ramBytesUsed() - stats.accountedBytes;
                    if (grown != 0) {
                        stats.accountedBytes += grown;
                        addRequestCircuitBreakerBytes(grown);
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        if (hasDataForBucket(bucket)) {
            final BucketStats stats = buckets.get(bucket);
            final int[] codePoints = stats.characters.sortedCodePoints();
            final long[] characterCounts = stats.characters.countsOf(codePoints);
            return new InternalStringStats(
                name,
                metadata(),
                stats.count,
                stats.minLength,
                stats.maxLength,
                codePoints,
                characterCounts,
                showDistribution
            );
        } else {
            return buildEmptyAggregation();
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalStringStats.empty(name, metadata(), showDistribution);
    }

    @Override
    public void doClose() {
        Releasables.close(buckets);
    }

    /**
     * What is known of the values of one bucket.
     */
    private static final class BucketStats {
        private final CharacterCounts characters = new CharacterCounts();
        /** The memory of the characters that is charged to the circuit breaker. */
        private long accountedBytes = characters.ramBytesUsed();
        private long count;
        private long totalCharacters;
        private int minLength = Integer.MAX_VALUE;
        private int maxLength;
        /** The entropy, which is calculated when it is asked for, and is {@code NaN} again whenever a value is added. */
        private double entropy = Double.NaN;

        void add(BytesRef value) {
            final int length = characters.addUtf8(value);
            count++;
            totalCharacters += length;
            minLength = Math.min(minLength, length);
            maxLength = Math.max(maxLength, length);
            entropy = Double.NaN;
        }

        /**
         * Calculates the entropy of the characters from their counts in ascending order of the character, the same as the result does,
         * so that both are the same to the last bit.
         */
        double entropy() {
            if (Double.isNaN(entropy)) {
                entropy = InternalStringStats.entropy(characters.countsOf(characters.sortedCodePoints()), totalCharacters);
            }
            return entropy;
        }
    }
}
