/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitMixer;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static org.opensearch.search.aggregations.metrics.AbstractHyperLogLog.MAX_PRECISION;
import static org.opensearch.search.aggregations.metrics.AbstractHyperLogLog.MIN_PRECISION;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompactHyperLogLogPlusPlusTests extends OpenSearchTestCase {

    private static BigArrays mockBigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private static int threshold(int precision) {
        return (int) ((1 << precision) / 4 * 0.75f);
    }

    /**
     * Number of distinct values to put in a bucket: none, a few, right around the linear counting threshold or well above it.
     */
    private static int randomDistinctValues(int threshold) {
        return switch (randomInt(3)) {
            case 0 -> 0;
            case 1 -> randomIntBetween(1, 5);
            case 2 -> threshold - 1 + randomInt(2);
            default -> randomIntBetween(1, 3 * threshold);
        };
    }

    /**
     * Collects random values into random buckets, in random order and with duplicates, into both a
     * {@link HyperLogLogPlusPlus} and a {@link CompactHyperLogLogPlusPlus}, and checks that every bucket ends up with the same
     * sketch, cardinality, clone and serialized form.
     */
    public void testSameSketchesAsHyperLogLogPlusPlus() throws IOException {
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final int threshold = threshold(p);
        // Keep the number of collected values reasonable at high precisions
        final int buckets = randomIntBetween(1, Math.max(1, Math.min(200, 600_000 / (3 * threshold))));
        final List<long[]> values = new ArrayList<>();
        for (int bucket = 0; bucket < buckets; bucket++) {
            final int distinct = randomDistinctValues(threshold);
            // Buckets share values so that the same hashes end up in different buckets
            final long first = randomLongBetween(0, 1000);
            for (long value = first; value < first + distinct; value++) {
                final int copies = randomIntBetween(1, 2);
                for (int i = 0; i < copies; i++) {
                    values.add(new long[] { bucket, BitMixer.mix64(value) });
                }
            }
        }
        Collections.shuffle(values, random());

        final BigArrays bigArrays = mockBigArrays();
        final AtomicLong breaker = new AtomicLong();
        try (
            HyperLogLogPlusPlus expected = new HyperLogLogPlusPlus(p, bigArrays, 1);
            CompactHyperLogLogPlusPlus actual = new CompactHyperLogLogPlusPlus(p, bigArrays, breaker::addAndGet)
        ) {
            for (long[] value : values) {
                expected.collect(value[0], value[1]);
                actual.collect(value[0], value[1]);
                if (rarely()) {
                    // Exercise the cached HyperLogLog estimates while the sketches still change
                    final long bucket = value[0];
                    assertEquals(expected.cardinality(bucket), actual.cardinality(bucket));
                }
            }
            for (long bucket = 0; bucket < buckets + 3; bucket++) {
                assertSameSketch(p, bigArrays, expected, bucket, actual);
            }
            assertThat(Math.abs(actual.linearCountingBytes() - breaker.get()), lessThan(CompactHyperLogLogPlusPlus.BREAKER_FLUSH_BYTES));
        }
        assertThat(breaker.get(), equalTo(0L));
    }

    private static void assertSameSketch(
        int p,
        BigArrays bigArrays,
        HyperLogLogPlusPlus expected,
        long bucket,
        CompactHyperLogLogPlusPlus actual
    ) throws IOException {
        assertEquals(expected.getAlgorithm(bucket), actual.getAlgorithm(bucket));
        assertEquals(expected.cardinality(bucket), actual.cardinality(bucket));
        assertTrue(expected.equals(bucket, actual, bucket));
        assertEquals(expected.hashCode(bucket), actual.hashCode(bucket));
        if (expected.getAlgorithm(bucket) == AbstractHyperLogLogPlusPlus.LINEAR_COUNTING) {
            assertEquals(expected.getLinearCounting(bucket).size(), actual.getLinearCounting(bucket).size());
        } else {
            // equals only compares the histogram of the registers, so compare them one by one
            final AbstractHyperLogLog.RunLenIterator expectedRegisters = expected.getHyperLogLog(bucket);
            final AbstractHyperLogLog.RunLenIterator actualRegisters = actual.getHyperLogLog(bucket);
            int registers = 0;
            while (expectedRegisters.next()) {
                assertTrue(actualRegisters.next());
                assertEquals(expectedRegisters.value(), actualRegisters.value());
                registers++;
            }
            assertFalse(actualRegisters.next());
            assertEquals(1 << p, registers);
        }
        try (AbstractHyperLogLogPlusPlus clone = actual.clone(bucket, bigArrays)) {
            assertTrue(expected.equals(bucket, clone, 0));
            assertEquals(expected.cardinality(bucket), clone.cardinality(0));
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            actual.writeTo(bucket, out);
            try (AbstractHyperLogLogPlusPlus read = AbstractHyperLogLogPlusPlus.readFrom(out.bytes().streamInput(), bigArrays)) {
                assertEquals(p, read.precision());
                assertTrue(expected.equals(bucket, read, 0));
            }
        }
    }

    public void testCardinalityFollowsNewValues() {
        final int p = randomIntBetween(10, 14);
        final int values = threshold(p) * 3;
        try (
            HyperLogLogPlusPlus expected = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
            CompactHyperLogLogPlusPlus actual = new CompactHyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, bytes -> {})
        ) {
            final long bucket = randomIntBetween(0, 10);
            for (long value = 0; value < values; value++) {
                final long hash = BitMixer.mix64(value);
                expected.collect(bucket, hash);
                actual.collect(bucket, hash);
                assertEquals(expected.cardinality(bucket), actual.cardinality(bucket));
                // Asking again returns the cached estimate
                assertEquals(expected.cardinality(bucket), actual.cardinality(bucket));
            }
            assertEquals(AbstractHyperLogLogPlusPlus.HYPERLOGLOG, actual.getAlgorithm(bucket));
            assertEquals(1, actual.hyperLogLogBuckets());
        }
    }

    public void testEmptyAndUnknownBuckets() throws IOException {
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final BigArrays bigArrays = mockBigArrays();
        try (
            HyperLogLogPlusPlus expected = new HyperLogLogPlusPlus(p, bigArrays, 1);
            CompactHyperLogLogPlusPlus actual = new CompactHyperLogLogPlusPlus(p, bigArrays, bytes -> {})
        ) {
            for (long bucket = 0; bucket < 5; bucket++) {
                assertSameSketch(p, bigArrays, expected, bucket, actual);
            }
            // HyperLogLogPlusPlus reserves 2^p bytes for every bucket up to this one, so keep it to a few MB
            final long bucket = randomIntBetween(10, Math.max(10, (8 << 20) >> p));
            expected.collect(bucket, 42);
            actual.collect(bucket, 42);
            assertEquals(1, actual.cardinality(bucket));
            for (long other : new long[] { 0, bucket - 1, bucket + 1, bucket * 10 }) {
                assertEquals(0, actual.cardinality(other));
                assertSameSketch(p, bigArrays, expected, other, actual);
            }
        }
    }

    /**
     * Many buckets holding a couple of values each, the case that makes {@link HyperLogLogPlusPlus} reserve
     * {@code buckets * 2^precision} bytes.
     */
    public void testSparseBucketsUseFarLessMemory() {
        final int p = HyperLogLogPlusPlus.DEFAULT_PRECISION;
        final int buckets = randomIntBetween(2_000, 4_000);
        final long dense = bytesUsed(p, buckets, 2, true);
        final long compact = bytesUsed(p, buckets, 2, false);
        assertThat(dense, lessThanOrEqualTo(2L * buckets << p));
        assertThat(dense, greaterThanOrEqualTo((long) buckets << p));
        assertThat(compact * 100, lessThan(dense));
    }

    /**
     * When every bucket holds more values than linear counting allows, both keep {@code 2^precision} registers per bucket,
     * and the compact one must not use noticeably more.
     */
    public void testDenseBucketsUseAboutTheSameMemory() {
        final int p = randomIntBetween(CardinalityAggregator.COMPACT_SKETCHES_MIN_PRECISION, 12);
        final int buckets = randomIntBetween(50, 200);
        final int values = threshold(p) + randomIntBetween(1, 100);
        final long dense = bytesUsed(p, buckets, values, true);
        final long compact = bytesUsed(p, buckets, values, false);
        assertThat(compact, lessThanOrEqualTo(dense + dense / 20 + CompactHyperLogLogPlusPlus.BREAKER_FLUSH_BYTES));
    }

    /**
     * Bytes charged to the circuit breaker after collecting {@code values} distinct values into each of {@code buckets} buckets,
     * one bucket after the other for half of the buckets and interleaved for the rest.
     */
    private static long bytesUsed(int p, int buckets, int values, boolean dense) {
        final AtomicLong used = new AtomicLong();
        final BigArrays bigArrays = new BigArrays(null, countingBreakerService(used, Long.MAX_VALUE), CircuitBreaker.REQUEST)
            .withCircuitBreaking();
        try (
            AbstractHyperLogLogPlusPlus counts = dense
                ? new HyperLogLogPlusPlus(p, bigArrays, 1)
                : new CompactHyperLogLogPlusPlus(p, bigArrays, used::addAndGet)
        ) {
            for (int bucket = 0; bucket < buckets / 2; bucket++) {
                for (int value = 0; value < values; value++) {
                    counts.collect(bucket, BitMixer.mix64(value));
                }
            }
            for (int value = 0; value < values; value++) {
                for (int bucket = buckets / 2; bucket < buckets; bucket++) {
                    counts.collect(bucket, BitMixer.mix64(value));
                }
            }
            for (int bucket = 0; bucket < buckets; bucket++) {
                assertThat(counts.cardinality(bucket), greaterThan(0L));
            }
            return used.get();
        } finally {
            assertThat(used.get(), equalTo(0L));
        }
    }

    private static CircuitBreakerService countingBreakerService(AtomicLong used, long limit) {
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(new NoopCircuitBreaker(CircuitBreaker.REQUEST) {
            @Override
            public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                if (bytes > 0 && used.get() + bytes > limit) {
                    throw new CircuitBreakingException("test error", bytes, limit, Durability.TRANSIENT);
                }
                return used.addAndGet(bytes);
            }

            @Override
            public long addWithoutBreaking(long bytes) {
                return used.addAndGet(bytes);
            }
        });
        return breakerService;
    }

    /**
     * Trips the circuit breaker at a random point, from the registers or from the hash tables, and checks that the sketch
     * is still usable and releases everything it charged once closed.
     */
    public void testCircuitBreakerTripLeavesConsistentSketch() throws IOException {
        final int p = randomIntBetween(CardinalityAggregator.COMPACT_SKETCHES_MIN_PRECISION, 14);
        final AtomicLong used = new AtomicLong();
        final long limit = randomLongBetween(1, 4L << 20);
        final CircuitBreakerService breakerService = countingBreakerService(used, limit);
        final BigArrays bigArrays = new BigArrays(null, breakerService, CircuitBreaker.REQUEST).withCircuitBreaking();
        final LongConsumer tablesBreaker = bytes -> breakerService.getBreaker(CircuitBreaker.REQUEST)
            .addEstimateBytesAndMaybeBreak(bytes, "test");
        CompactHyperLogLogPlusPlus counts = null;
        try {
            counts = new CompactHyperLogLogPlusPlus(p, bigArrays, tablesBreaker);
            final int buckets = randomIntBetween(1, 2000);
            final int maxValues = threshold(p) * 2;
            for (int i = 0; i < 2_000_000; i++) {
                counts.collect(randomInt(buckets - 1), BitMixer.mix64(randomInt(maxValues)));
            }
        } catch (CircuitBreakingException e) {
            // expected for most limits
        }
        if (counts != null) {
            try (CompactHyperLogLogPlusPlus c = counts; BytesStreamOutput out = new BytesStreamOutput()) {
                for (long bucket = 0; bucket < c.maxOrd(); bucket++) {
                    c.cardinality(bucket);
                    assertTrue(c.equals(bucket, c, bucket));
                    c.writeTo(bucket, out);
                }
            }
        }
        assertThat(used.get(), equalTo(0L));
    }
}
