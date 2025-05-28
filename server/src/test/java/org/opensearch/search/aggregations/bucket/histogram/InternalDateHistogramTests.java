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

import org.opensearch.common.Rounding;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.ParsedMultiBucketAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.InternalMultiBucketAggregationTestCase;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.mockito.Mockito;

import static org.opensearch.common.unit.TimeValue.timeValueHours;
import static org.opensearch.common.unit.TimeValue.timeValueMinutes;
import static org.opensearch.common.unit.TimeValue.timeValueSeconds;

public class InternalDateHistogramTests extends InternalMultiBucketAggregationTestCase<InternalDateHistogram> {

    private boolean keyed;
    private DocValueFormat format;
    private long intervalMillis;
    private long baseMillis;
    private long minDocCount;
    private InternalDateHistogram.EmptyBucketInfo emptyBucketInfo;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        keyed = randomBoolean();
        format = randomNumericDocValueFormat();
        // in order for reduction to work properly (and be realistic) we need to use the same interval, minDocCount, emptyBucketInfo
        // and base in all randomly created aggs as part of the same test run. This is particularly important when minDocCount is
        // set to 0 as empty buckets need to be added to fill the holes.
        long interval = randomIntBetween(1, 3);
        intervalMillis = randomFrom(timeValueSeconds(interval), timeValueMinutes(interval), timeValueHours(interval)).getMillis();
        Rounding rounding = Rounding.builder(TimeValue.timeValueMillis(intervalMillis)).build();
        long now = System.currentTimeMillis();
        baseMillis = rounding.prepare(now, now).round(now);
        if (randomBoolean()) {
            minDocCount = randomIntBetween(1, 10);
            emptyBucketInfo = null;
        } else {
            minDocCount = 0;
            LongBounds extendedBounds = null;
            if (randomBoolean()) {
                // it's ok if min and max are outside the range of the generated buckets, that will just mean that
                // empty buckets won't be added before the first bucket and/or after the last one
                long min = baseMillis - intervalMillis * randomNumberOfBuckets();
                long max = baseMillis + randomNumberOfBuckets() * intervalMillis;
                extendedBounds = new LongBounds(min, max);
            }
            emptyBucketInfo = new InternalDateHistogram.EmptyBucketInfo(rounding, InternalAggregations.EMPTY, extendedBounds);
        }
    }

    @Override
    protected InternalDateHistogram createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations) {
        int nbBuckets = randomNumberOfBuckets();
        List<InternalDateHistogram.Bucket> buckets = new ArrayList<>(nbBuckets);
        // avoid having different random instance start from exactly the same base
        long startingDate = baseMillis - intervalMillis * randomNumberOfBuckets();
        for (int i = 0; i < nbBuckets; i++) {
            // rarely leave some holes to be filled up with empty buckets in case minDocCount is set to 0
            if (frequently()) {
                long key = startingDate + intervalMillis * i;
                buckets.add(new InternalDateHistogram.Bucket(key, randomIntBetween(1, 100), keyed, format, aggregations));
            }
        }
        BucketOrder order = BucketOrder.key(randomBoolean());
        return new InternalDateHistogram(name, buckets, order, minDocCount, 0L, emptyBucketInfo, format, keyed, metadata);
    }

    public void testTooManyBucketsExceptionWhenAddingEmptyBuckets() {
        String name = randomAlphaOfLength(5);
        Rounding rounding = Rounding.builder(TimeValue.timeValueMillis(intervalMillis)).build();
        long now = System.currentTimeMillis();
        baseMillis = rounding.prepare(now, now).round(now);
        long min = baseMillis - intervalMillis * randomNumberOfBuckets();
        long max = baseMillis + randomNumberOfBuckets() * intervalMillis;
        LongBounds extendedBounds = new LongBounds(min, max);
        InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = new InternalDateHistogram.EmptyBucketInfo(
            rounding,
            InternalAggregations.EMPTY,
            extendedBounds
        );
        List<InternalDateHistogram.Bucket> bucket1 = List.of(
            new InternalDateHistogram.Bucket(randomNonNegativeLong(), randomIntBetween(1, 100), false, format, InternalAggregations.EMPTY)
        );
        List<InternalDateHistogram.Bucket> bucket2 = List.of(
            new InternalDateHistogram.Bucket(randomNonNegativeLong(), randomIntBetween(1, 100), false, format, InternalAggregations.EMPTY)
        );
        BucketOrder order = BucketOrder.key(true);
        InternalDateHistogram histogram1 = new InternalDateHistogram(name, bucket1, order, 0, 0L, emptyBucketInfo, format, false, null);
        InternalDateHistogram histogram2 = new InternalDateHistogram(name, bucket2, order, 0, 0L, emptyBucketInfo, format, false, null);

        CircuitBreaker breaker = Mockito.mock(CircuitBreaker.class);
        Mockito.when(breaker.addEstimateBytesAndMaybeBreak(50L * 2, "empty date histogram buckets"))
            .thenThrow(CircuitBreakingException.class);

        MultiBucketConsumerService.MultiBucketConsumer bucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(0, breaker);
        InternalAggregation.ReduceContext reduceContext = InternalAggregation.ReduceContext.forFinalReduction(
            null,
            null,
            bucketConsumer,
            PipelineAggregator.PipelineTree.EMPTY,
            breaker
        );
        List<InternalDateHistogram.Bucket> reducedBuckets = histogram1.reduceBuckets(List.of(histogram1, histogram2), reduceContext);
        expectThrows(
            MultiBucketConsumerService.TooManyBucketsException.class,
            () -> histogram1.addEmptyBuckets(reducedBuckets, reduceContext)
        );
    }

    @Override
    protected void assertReduced(InternalDateHistogram reduced, List<InternalDateHistogram> inputs) {
        TreeMap<Long, Long> expectedCounts = new TreeMap<>();
        for (Histogram histogram : inputs) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                expectedCounts.compute(
                    ((ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli(),
                    (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount()
                );
            }
        }
        if (minDocCount == 0) {
            long minBound = -1;
            long maxBound = -1;
            if (emptyBucketInfo.bounds != null) {
                Rounding.Prepared prepared = emptyBucketInfo.rounding.prepare(
                    emptyBucketInfo.bounds.getMin(),
                    emptyBucketInfo.bounds.getMax()
                );
                minBound = prepared.round(emptyBucketInfo.bounds.getMin());
                maxBound = prepared.round(emptyBucketInfo.bounds.getMax());
                if (expectedCounts.isEmpty() && minBound <= maxBound) {
                    expectedCounts.put(minBound, 0L);
                }
            }
            if (expectedCounts.isEmpty() == false) {
                Long nextKey = expectedCounts.firstKey();
                while (nextKey < expectedCounts.lastKey()) {
                    expectedCounts.putIfAbsent(nextKey, 0L);
                    nextKey += intervalMillis;
                }
                if (emptyBucketInfo.bounds != null) {
                    while (minBound < expectedCounts.firstKey()) {
                        expectedCounts.put(expectedCounts.firstKey() - intervalMillis, 0L);
                    }
                    while (expectedCounts.lastKey() < maxBound) {
                        expectedCounts.put(expectedCounts.lastKey() + intervalMillis, 0L);
                    }
                }
            }
        } else {
            expectedCounts.entrySet().removeIf(doubleLongEntry -> doubleLongEntry.getValue() < minDocCount);
        }

        Map<Long, Long> actualCounts = new TreeMap<>();
        for (Histogram.Bucket bucket : reduced.getBuckets()) {
            actualCounts.compute(
                ((ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli(),
                (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount()
            );
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedDateHistogram.class;
    }

    @Override
    protected InternalDateHistogram mutateInstance(InternalDateHistogram instance) {
        String name = instance.getName();
        List<InternalDateHistogram.Bucket> buckets = instance.getBuckets();
        BucketOrder order = instance.getOrder();
        long minDocCount = instance.getMinDocCount();
        long offset = instance.getOffset();
        InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = instance.emptyBucketInfo;
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 5)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                buckets = new ArrayList<>(buckets);
                buckets.add(
                    new InternalDateHistogram.Bucket(
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
                offset += between(1, 20);
                break;
            case 5:
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
        return new InternalDateHistogram(name, buckets, order, minDocCount, offset, emptyBucketInfo, format, keyed, metadata);
    }
}
