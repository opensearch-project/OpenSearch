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

package org.opensearch.search.aggregations.metrics;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitMixer;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.search.aggregations.metrics.AbstractHyperLogLog.MAX_PRECISION;
import static org.opensearch.search.aggregations.metrics.AbstractHyperLogLog.MIN_PRECISION;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HyperLogLogPlusPlusTests extends OpenSearchTestCase {
    public void testEncodeDecode() {
        final int iters = scaledRandomIntBetween(100000, 500000);
        // random hashes
        for (int i = 0; i < iters; ++i) {
            final int p1 = randomIntBetween(4, 24);
            final long hash = randomLong();
            testEncodeDecode(p1, hash);
        }
        // special cases
        for (int p1 = MIN_PRECISION; p1 <= MAX_PRECISION; ++p1) {
            testEncodeDecode(p1, 0);
            testEncodeDecode(p1, 1);
            testEncodeDecode(p1, ~0L);
        }
    }

    private void testEncodeDecode(int p1, long hash) {
        final long index = AbstractHyperLogLog.index(hash, p1);
        final int runLen = AbstractHyperLogLog.runLen(hash, p1);
        final int encoded = AbstractLinearCounting.encodeHash(hash, p1);
        assertEquals(index, AbstractHyperLogLog.decodeIndex(encoded, p1));
        assertEquals(runLen, AbstractHyperLogLog.decodeRunLen(encoded, p1));
    }

    public void testAccuracy() {
        final long bucket = randomInt(20);
        final int numValues = randomIntBetween(1, 100000);
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000 : 100000);
        final int p = randomIntBetween(14, MAX_PRECISION);
        final Set<Integer> set = new HashSet<>();
        HyperLogLogPlusPlus e = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
        for (int i = 0; i < numValues; ++i) {
            final int n = randomInt(maxValue);
            set.add(n);
            final long hash = BitMixer.mix64(n);
            e.collect(bucket, hash);
            if (randomInt(100) == 0) {
                // System.out.println(e.cardinality(bucket) + " <> " + set.size());
                assertThat((double) e.cardinality(bucket), closeTo(set.size(), 0.1 * set.size()));
            }
        }
        assertThat((double) e.cardinality(bucket), closeTo(set.size(), 0.1 * set.size()));
    }

    public void testMerge() {
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final HyperLogLogPlusPlus single = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);
        final HyperLogLogPlusPlus[] multi = new HyperLogLogPlusPlus[randomIntBetween(2, 100)];
        final long[] bucketOrds = new long[multi.length];
        for (int i = 0; i < multi.length; ++i) {
            bucketOrds[i] = randomInt(20);
            multi[i] = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 5);
        }
        final int numValues = randomIntBetween(1, 100000);
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000 : 1000000);
        for (int i = 0; i < numValues; ++i) {
            final int n = randomInt(maxValue);
            final long hash = BitMixer.mix64(n);
            single.collect(0, hash);
            // use a gaussian so that all instances don't collect as many hashes
            final int index = (int) (Math.pow(randomDouble(), 2));
            multi[index].collect(bucketOrds[index], hash);
            if (randomInt(100) == 0) {
                HyperLogLogPlusPlus merged = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);
                for (int j = 0; j < multi.length; ++j) {
                    merged.merge(0, multi[j], bucketOrds[j]);
                }
                assertEquals(single.cardinality(0), merged.cardinality(0));
            }
        }
    }

    public void testFakeHashes() {
        // hashes with lots of leading zeros trigger different paths in the code that we try to go through here
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);

        counts.collect(0, 0);
        assertEquals(1, counts.cardinality(0));
        if (randomBoolean()) {
            counts.collect(0, 1);
            assertEquals(2, counts.cardinality(0));
        }
        counts.upgradeToHll(0);
        // all hashes felt into the same bucket so hll would expect a count of 1
        assertEquals(1, counts.cardinality(0));
    }

    public void testPrecisionFromThreshold() {
        assertEquals(4, HyperLogLogPlusPlus.precisionFromThreshold(0));
        assertEquals(6, HyperLogLogPlusPlus.precisionFromThreshold(10));
        assertEquals(10, HyperLogLogPlusPlus.precisionFromThreshold(100));
        assertEquals(13, HyperLogLogPlusPlus.precisionFromThreshold(1000));
        assertEquals(16, HyperLogLogPlusPlus.precisionFromThreshold(10000));
        assertEquals(18, HyperLogLogPlusPlus.precisionFromThreshold(100000));
        assertEquals(18, HyperLogLogPlusPlus.precisionFromThreshold(1000000));
    }

    public void testCircuitBreakerOnConstruction() {
        int whenToBreak = randomInt(10);
        AtomicLong total = new AtomicLong();
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(new NoopCircuitBreaker(CircuitBreaker.REQUEST) {
            private int countDown = whenToBreak;

            @Override
            public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                if (countDown-- == 0) {
                    throw new CircuitBreakingException("test error", bytes, Long.MAX_VALUE, Durability.TRANSIENT);
                }
                total.addAndGet(bytes);
                return total.get();
            }

            @Override
            public long addWithoutBreaking(long bytes) {
                total.addAndGet(bytes);
                return total.get();
            }
        });
        BigArrays bigArrays = new BigArrays(null, breakerService, CircuitBreaker.REQUEST).withCircuitBreaking();
        final int p = randomIntBetween(HyperLogLogPlusPlus.MIN_PRECISION, HyperLogLogPlusPlus.MAX_PRECISION);
        try {
            for (int i = 0; i < whenToBreak + 1; ++i) {
                final HyperLogLogPlusPlus subject = new HyperLogLogPlusPlus(p, bigArrays, 0);
                subject.close();
            }
            fail("Must fail");
        } catch (CircuitBreakingException e) {
            // OK
        }

        assertThat(total.get(), equalTo(0L));
    }

    public void testRetrieveCardinality() {
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
        int bucket = randomInt(100);
        counts.collect(bucket, randomLong());
        for (int i = 0; i < 1000; i++) {
            int cardinality = bucket == i ? 1 : 0;
            assertEquals(cardinality, counts.cardinality(i));
        }
    }

}
