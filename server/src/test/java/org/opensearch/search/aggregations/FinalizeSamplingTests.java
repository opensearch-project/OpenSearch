/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.util.BytesRef;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.opensearch.search.aggregations.bucket.filter.InternalFilters;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.opensearch.search.aggregations.metrics.ExtendedStats;
import org.opensearch.search.aggregations.metrics.InternalExtendedStats;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalStats;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.test.OpenSearchTestCase;

import static java.util.Collections.singletonList;

/**
 * Covers {@link InternalAggregation#finalizeSampling(SamplingContext)} for the aggregations that override it, and for
 * ones that deliberately report the sample unchanged.
 */
public class FinalizeSamplingTests extends OpenSearchTestCase {

    private static final SamplingContext ONE_IN_TEN = new SamplingContext(0.1);

    public void testSingleBucketScalesDocCountAndSubAggregations() {
        InternalAggregations subAggregations = InternalAggregations.from(
            singletonList(new InternalSum("sum", 7.0, DocValueFormat.RAW, null))
        );
        TestSingleBucket bucket = new TestSingleBucket("bucket", 25L, subAggregations);

        InternalSingleBucketAggregation scaled = (InternalSingleBucketAggregation) bucket.finalizeSampling(ONE_IN_TEN);

        assertEquals(250L, scaled.getDocCount());
        assertEquals(70.0, ((InternalSum) scaled.getAggregations().get("sum")).getValue(), 1e-9);
    }

    public void testTermsScalesBucketsErrorsAndOtherDocCount() {
        StringTerms terms = new StringTerms(
            "terms",
            BucketOrder.count(false),
            BucketOrder.count(false),
            null,
            DocValueFormat.RAW,
            10,
            true,
            13L,
            singletonList(new StringTerms.Bucket(new BytesRef("a"), 20L, InternalAggregations.EMPTY, true, 5L, DocValueFormat.RAW)),
            7L,
            new TermsAggregator.BucketCountThresholds(1, 0, 10, 10)
        );

        StringTerms scaled = (StringTerms) terms.finalizeSampling(ONE_IN_TEN);

        Terms.Bucket bucket = scaled.getBuckets().get(0);
        assertEquals(200L, bucket.getDocCount());
        assertEquals(50L, bucket.getDocCountError());
        assertEquals(70L, scaled.getDocCountError());
        assertEquals(130L, scaled.getSumOfOtherDocCounts());
    }

    public void testTermsLeavesUnknownDocCountErrorAlone() {
        StringTerms terms = new StringTerms(
            "terms",
            BucketOrder.count(false),
            BucketOrder.count(false),
            null,
            DocValueFormat.RAW,
            10,
            false,
            0L,
            singletonList(new StringTerms.Bucket(new BytesRef("a"), 20L, InternalAggregations.EMPTY, false, -1L, DocValueFormat.RAW)),
            -1L,
            new TermsAggregator.BucketCountThresholds(1, 0, 10, 10)
        );

        StringTerms scaled = (StringTerms) terms.finalizeSampling(ONE_IN_TEN);

        assertEquals(-1L, scaled.getDocCountError());
    }

    /**
     * An unmapped terms aggregation must opt out: the inherited implementation would throw, because the {@code create}
     * it needs is unsupported there.
     */
    public void testUnmappedTermsIsUnchanged() {
        UnmappedTerms unmapped = new UnmappedTerms(
            "terms",
            BucketOrder.count(false),
            new TermsAggregator.BucketCountThresholds(1, 0, 10, 10),
            null
        );

        assertSame(unmapped, unmapped.finalizeSampling(ONE_IN_TEN));
    }

    public void testHistogramScalesBuckets() {
        InternalHistogram histogram = new InternalHistogram(
            "histo",
            singletonList(new InternalHistogram.Bucket(1.0, 4L, false, DocValueFormat.RAW, InternalAggregations.EMPTY)),
            BucketOrder.key(true),
            1L,
            null,
            DocValueFormat.RAW,
            false,
            null
        );

        InternalHistogram scaled = (InternalHistogram) histogram.finalizeSampling(ONE_IN_TEN);

        assertEquals(40L, scaled.getBuckets().get(0).getDocCount());
        assertEquals(1.0, (Double) scaled.getBuckets().get(0).getKey(), 0.0);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testRangeScalesBuckets() {
        InternalRange.Factory factory = new InternalRange.Factory();
        InternalRange.Bucket bucket = (InternalRange.Bucket) factory.createBucket(
            "0-10",
            0.0,
            10.0,
            6L,
            InternalAggregations.EMPTY,
            false,
            DocValueFormat.RAW
        );
        InternalRange range = new InternalRange("range", singletonList(bucket), DocValueFormat.RAW, false, null);

        InternalRange scaled = (InternalRange) range.finalizeSampling(ONE_IN_TEN);

        InternalRange.Bucket scaledBucket = (InternalRange.Bucket) scaled.getBuckets().get(0);
        assertEquals(60L, scaledBucket.getDocCount());
        assertEquals("0-10", scaledBucket.getKey());
    }

    public void testFiltersScalesBuckets() {
        InternalFilters filters = new InternalFilters(
            "filters",
            singletonList(new InternalFilters.InternalBucket("a", 8L, InternalAggregations.EMPTY, true)),
            true,
            null
        );

        InternalFilters scaled = (InternalFilters) filters.finalizeSampling(ONE_IN_TEN);

        assertEquals(80L, scaled.getBuckets().get(0).getDocCount());
        assertEquals("a", scaled.getBuckets().get(0).getKey());
    }

    public void testSumAndValueCountAreScaled() {
        InternalSum sum = new InternalSum("sum", 12.5, DocValueFormat.RAW, null);
        assertEquals(125.0, ((InternalSum) sum.finalizeSampling(ONE_IN_TEN)).getValue(), 1e-9);

        InternalValueCount count = new InternalValueCount("count", 9L, null);
        assertEquals(90L, ((InternalValueCount) count.finalizeSampling(ONE_IN_TEN)).getValue());
    }

    public void testStatsScalesCountAndSumOnly() {
        InternalStats stats = new InternalStats("stats", 10L, 50.0, 2.0, 8.0, DocValueFormat.RAW, null);

        InternalStats scaled = (InternalStats) stats.finalizeSampling(ONE_IN_TEN);

        assertEquals(100L, scaled.getCount());
        assertEquals(500.0, scaled.getSum(), 1e-9);
        assertEquals(2.0, scaled.getMin(), 0.0);
        assertEquals(8.0, scaled.getMax(), 0.0);
        // count and sum scale by the same factor, so the average is untouched
        assertEquals(stats.getAvg(), scaled.getAvg(), 1e-9);
    }

    /**
     * Scaling {@code sumOfSquares} along with {@code count} and {@code sum} is what keeps the variance and the standard
     * deviation from changing. Inheriting {@link InternalStats}'s implementation without this would leave them as
     * neither sample nor population values.
     */
    public void testExtendedStatsKeepsVarianceAndStandardDeviation() {
        InternalExtendedStats stats = new InternalExtendedStats("stats", 10L, 50.0, 2.0, 8.0, 300.0, 2.0, DocValueFormat.RAW, null);

        InternalExtendedStats scaled = (InternalExtendedStats) stats.finalizeSampling(ONE_IN_TEN);

        assertEquals(100L, scaled.getCount());
        assertEquals(500.0, scaled.getSum(), 1e-9);
        assertEquals(3000.0, scaled.getSumOfSquares(), 1e-9);
        assertEquals(stats.getVariance(), scaled.getVariance(), 1e-9);
        assertEquals(stats.getStdDeviation(), scaled.getStdDeviation(), 1e-9);
        assertEquals(stats.getStdDeviationBound(ExtendedStats.Bounds.UPPER), scaled.getStdDeviationBound(ExtendedStats.Bounds.UPPER), 1e-9);
    }

    public void testValuesThatAlreadyEstimateThePopulationAreUnchanged() {
        InternalMax max = new InternalMax("max", 42.0, DocValueFormat.RAW, null);

        assertSame(max, max.finalizeSampling(ONE_IN_TEN));
    }

    public void testNothingIsScaledWithoutSampling() {
        InternalSum sum = new InternalSum("sum", 12.5, DocValueFormat.RAW, null);

        assertEquals(12.5, ((InternalSum) sum.finalizeSampling(SamplingContext.NONE)).getValue(), 0.0);
    }

    /**
     * A minimal single-bucket aggregation, so that the base class implementation can be exercised without depending on
     * any one aggregation's package-private constructor.
     */
    private static class TestSingleBucket extends InternalSingleBucketAggregation {
        TestSingleBucket(String name, long docCount, InternalAggregations subAggregations) {
            super(name, docCount, subAggregations, null);
        }

        @Override
        protected InternalSingleBucketAggregation newAggregation(String name, long docCount, InternalAggregations subAggregations) {
            return new TestSingleBucket(name, docCount, subAggregations);
        }

        @Override
        public String getWriteableName() {
            return "test_single_bucket";
        }

        @Override
        public String getType() {
            return "test_single_bucket";
        }
    }
}
