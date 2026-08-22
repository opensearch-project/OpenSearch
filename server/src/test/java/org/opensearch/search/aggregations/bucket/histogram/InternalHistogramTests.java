/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.bucket.histogram;

import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.ParsedMultiBucketAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.InternalAggregationTestCase;
import org.opensearch.test.InternalMultiBucketAggregationTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.mockito.Mockito;

public class InternalHistogramTests extends InternalMultiBucketAggregationTestCase<InternalHistogram> {

    private boolean keyed;
    private DocValueFormat format;
    private int interval;
    private int minDocCount;
    private InternalHistogram.EmptyBucketInfo emptyBucketInfo;
    private int offset;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        keyed = randomBoolean();
        format = randomNumericDocValueFormat();
        // in order for reduction to work properly (and be realistic) we need to use the same interval, minDocCount, emptyBucketInfo
        // and offset in all randomly created aggs as part of the same test run. This is particularly important when minDocCount is
        // set to 0 as empty buckets need to be added to fill the holes.
        interval = randomIntBetween(1, 3);
        offset = randomIntBetween(0, 3);
        if (randomBoolean()) {
            minDocCount = randomIntBetween(1, 10);
            emptyBucketInfo = null;
        } else {
            minDocCount = 0;
            // it's ok if minBound and maxBound are outside the range of the generated buckets, that will just mean that
            // empty buckets won't be added before the first bucket and/or after the last one
            int minBound = randomInt(50) - 30;
            int maxBound = randomNumberOfBuckets() * interval + randomIntBetween(0, 10);
            emptyBucketInfo = new InternalHistogram.EmptyBucketInfo(interval, offset, minBound, maxBound, InternalAggregations.EMPTY);
        }
    }

    private double round(double key) {
        return Math.floor((key - offset) / interval) * interval + offset;
    }

    @Override
    protected InternalHistogram createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations) {
        final double base = round(randomInt(50) - 30);
        final int numBuckets = randomNumberOfBuckets();
        List<InternalHistogram.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < numBuckets; ++i) {
            // rarely leave some holes to be filled up with empty buckets in case minDocCount is set to 0
            if (frequently()) {
                final int docCount = TestUtil.nextInt(random(), 1, 50);
                buckets.add(new InternalHistogram.Bucket(base + i * interval, docCount, keyed, format, aggregations));
            }
        }
        BucketOrder order = BucketOrder.key(randomBoolean());
        return new InternalHistogram(name, buckets, order, minDocCount, emptyBucketInfo, format, keyed, metadata);
    }

    // issue 26787
    public void testHandlesNaN() {
        InternalHistogram histogram = createTestInstance();
        InternalHistogram histogram2 = createTestInstance();
        List<InternalHistogram.Bucket> buckets = histogram.getBuckets();
        if (buckets == null || buckets.isEmpty()) {
            return;
        }

        // Set the key of one bucket to NaN. Must be the last bucket because NaN is greater than everything else.
        List<InternalHistogram.Bucket> newBuckets = new ArrayList<>(buckets.size());
        if (buckets.size() > 1) {
            newBuckets.addAll(buckets.subList(0, buckets.size() - 1));
        }
        InternalHistogram.Bucket b = buckets.get(buckets.size() - 1);
        newBuckets.add(new InternalHistogram.Bucket(Double.NaN, b.docCount, keyed, b.format, b.aggregations));

        InternalHistogram newHistogram = histogram.create(newBuckets);
        newHistogram.reduce(
            Arrays.asList(newHistogram, histogram2),
            InternalAggregationTestCase.emptyReduceContextBuilder().forPartialReduction()
        );
    }

    public void testEmptyExtendedBoundsRespectMaxBuckets() {
        InternalHistogram histogram = createHistogram(List.of(), 1, 0, 10);

        expectThrows(
            MultiBucketConsumerService.TooManyBucketsException.class,
            () -> histogram.reduce(List.of(histogram), createReduceContext(10, Mockito.mock(CircuitBreaker.class)))
        );
    }

    public void testOneSidedExtendedBoundsRespectMaxBuckets() {
        List<InternalHistogram.Bucket> buckets = List.of(createBucket(0));
        InternalHistogram minBoundHistogram = createHistogram(buckets, 1, -10, Double.NEGATIVE_INFINITY);
        InternalHistogram maxBoundHistogram = createHistogram(buckets, 1, Double.POSITIVE_INFINITY, 10);

        expectThrows(
            MultiBucketConsumerService.TooManyBucketsException.class,
            () -> minBoundHistogram.reduce(List.of(minBoundHistogram), createReduceContext(10, Mockito.mock(CircuitBreaker.class)))
        );
        expectThrows(
            MultiBucketConsumerService.TooManyBucketsException.class,
            () -> maxBoundHistogram.reduce(List.of(maxBoundHistogram), createReduceContext(10, Mockito.mock(CircuitBreaker.class)))
        );
    }

    public void testSparseBucketsRespectMaxBucketsWithoutExtendedBounds() {
        InternalHistogram histogram = createHistogram(
            List.of(createBucket(0), createBucket(100)),
            1,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY
        );

        expectThrows(
            MultiBucketConsumerService.TooManyBucketsException.class,
            () -> histogram.reduce(List.of(histogram), createReduceContext(10, Mockito.mock(CircuitBreaker.class)))
        );
    }

    public void testExactBucketLimit() {
        InternalHistogram histogram = createHistogram(List.of(), 1, 0, 9);

        InternalHistogram reduced = (InternalHistogram) histogram.reduce(
            List.of(histogram),
            createReduceContext(10, Mockito.mock(CircuitBreaker.class))
        );

        assertEquals(10, reduced.getBuckets().size());
    }

    public void testNaNDoesNotBypassMaxBuckets() {
        InternalHistogram histogram = createHistogram(
            List.of(createBucket(0), createBucket(100), createBucket(Double.NaN)),
            1,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY
        );

        expectThrows(
            MultiBucketConsumerService.TooManyBucketsException.class,
            () -> histogram.reduce(List.of(histogram), createReduceContext(10, Mockito.mock(CircuitBreaker.class)))
        );
    }

    public void testNonAdvancingKeyFails() {
        double minBound = 1e20;
        InternalHistogram histogram = createHistogram(List.of(), 1, minBound, Math.nextUp(minBound));

        expectThrows(
            AggregationExecutionException.class,
            () -> histogram.reduce(List.of(histogram), createReduceContext(10, Mockito.mock(CircuitBreaker.class)))
        );
    }

    public void testCircuitBreakerCheckedWhileAddingEmptyBuckets() {
        CircuitBreaker breaker = Mockito.mock(CircuitBreaker.class);
        Mockito.when(breaker.addEstimateBytesAndMaybeBreak(0, "allocated_buckets")).thenThrow(CircuitBreakingException.class);
        InternalHistogram histogram = createHistogram(List.of(), 1, 0, 1024);

        expectThrows(CircuitBreakingException.class, () -> histogram.reduce(List.of(histogram), createReduceContext(2000, breaker)));
        Mockito.verify(breaker, Mockito.times(1)).addEstimateBytesAndMaybeBreak(0, "allocated_buckets");
    }

    private InternalHistogram createHistogram(
        List<InternalHistogram.Bucket> buckets,
        double bucketInterval,
        double minBound,
        double maxBound
    ) {
        InternalHistogram.EmptyBucketInfo bucketInfo = new InternalHistogram.EmptyBucketInfo(
            bucketInterval,
            0,
            minBound,
            maxBound,
            InternalAggregations.EMPTY
        );
        return new InternalHistogram(randomAlphaOfLength(5), buckets, BucketOrder.key(true), 0, bucketInfo, format, false, null);
    }

    private InternalHistogram.Bucket createBucket(double key) {
        return new InternalHistogram.Bucket(key, 1, false, format, InternalAggregations.EMPTY);
    }

    private InternalAggregation.ReduceContext createReduceContext(int maxBuckets, CircuitBreaker breaker) {
        return InternalAggregation.ReduceContext.forFinalReduction(
            null,
            null,
            new MultiBucketConsumerService.MultiBucketConsumer(maxBuckets, breaker),
            PipelineAggregator.PipelineTree.EMPTY
        );
    }

    @Override
    protected void assertReduced(InternalHistogram reduced, List<InternalHistogram> inputs) {
        TreeMap<Double, Long> expectedCounts = new TreeMap<>();
        for (Histogram histogram : inputs) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                expectedCounts.compute(
                    (Double) bucket.getKey(),
                    (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount()
                );
            }
        }
        if (minDocCount == 0) {
            double minBound = round(emptyBucketInfo.minBound);
            if (expectedCounts.isEmpty() && emptyBucketInfo.minBound <= emptyBucketInfo.maxBound) {
                expectedCounts.put(minBound, 0L);
            }
            if (expectedCounts.isEmpty() == false) {
                Double nextKey = expectedCounts.firstKey();
                while (nextKey < expectedCounts.lastKey()) {
                    expectedCounts.putIfAbsent(nextKey, 0L);
                    nextKey += interval;
                }
                while (minBound < expectedCounts.firstKey()) {
                    expectedCounts.put(expectedCounts.firstKey() - interval, 0L);
                }
                double maxBound = round(emptyBucketInfo.maxBound);
                while (expectedCounts.lastKey() < maxBound) {
                    expectedCounts.put(expectedCounts.lastKey() + interval, 0L);
                }
            }
        } else {
            expectedCounts.entrySet().removeIf(doubleLongEntry -> doubleLongEntry.getValue() < minDocCount);
        }

        Map<Double, Long> actualCounts = new TreeMap<>();
        for (Histogram.Bucket bucket : reduced.getBuckets()) {
            actualCounts.compute((Double) bucket.getKey(), (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedHistogram.class;
    }

    @Override
    protected InternalHistogram mutateInstance(InternalHistogram instance) {
        String name = instance.getName();
        List<InternalHistogram.Bucket> buckets = instance.getBuckets();
        BucketOrder order = instance.getOrder();
        long minDocCount = instance.getMinDocCount();
        Map<String, Object> metadata = instance.getMetadata();
        InternalHistogram.EmptyBucketInfo emptyBucketInfo = instance.emptyBucketInfo;
        switch (between(0, 4)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                buckets = new ArrayList<>(buckets);
                buckets.add(
                    new InternalHistogram.Bucket(
                        randomNonNegativeLong(),
                        randomIntBetween(1, 100),
                        keyed,
                        format,
                        InternalAggregations.EMPTY
                    )
                );
                break;
            case 2:
                order = BucketOrder.count(randomBoolean());
                break;
            case 3:
                minDocCount += between(1, 10);
                emptyBucketInfo = null;
                break;
            case 4:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalHistogram(name, buckets, order, minDocCount, emptyBucketInfo, format, keyed, metadata);
    }
}
