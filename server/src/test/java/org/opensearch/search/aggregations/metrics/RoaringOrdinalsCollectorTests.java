/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitArray;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.fielddata.FieldData;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.BucketOrdinalBitmaps;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.BucketOrdinalBits;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.DirectCollector;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.HybridCollector;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.MurmurHash3Values;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.OrdinalsCollector;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.RoaringOrdinalsCollector;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.VisitedRanks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.roaringbitmap.RoaringBitmap;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RoaringOrdinalsCollector} and the fallback chain of {@link HybridCollector}.
 */
public class RoaringOrdinalsCollectorTests extends AggregatorTestCase {

    /**
     * Three segments with overlapping values: X has apps [a, b, c] and Y has apps [b, c, d, e].
     */
    private static final List<List<String[]>> SEGMENTS = List.of(
        List.of(doc("X", "a"), doc("X", "b"), doc("Y", "b"), doc("Y", "c")),
        List.of(doc("X", "b"), doc("X", "c"), doc("Y", "c"), doc("Y", "d")),
        List.of(doc("X", "a"), doc("Y", "d"), doc("Y", "e"))
    );

    public void testEveryCollectorProducesTheSameResults() throws IOException {
        withIndex(SEGMENTS, searcher -> {
            for (String hint : new String[] { null, "direct", "ordinals" }) {
                // unlimited keeps BitArrays, 40 bytes switches collectors after the first bucket, 1 byte right away
                for (long memoryThreshold : new long[] { Long.MAX_VALUE, 40, 1 }) {
                    for (boolean hybrid : new boolean[] { true, false }) {
                        CardinalityAggregationContext cardinalityContext = new CardinalityAggregationContext(hybrid, memoryThreshold);
                        Terms terms = (Terms) collect(searcher, terms(card(hint)), cardinalityContext, new HashMap<>());
                        assertEquals(2, terms.getBuckets().size());
                        assertEquals(3, ((InternalCardinality) terms.getBucketByKey("X").getAggregations().get("card")).getValue());
                        assertEquals(4, ((InternalCardinality) terms.getBucketByKey("Y").getAggregations().get("card")).getValue());
                    }
                }
            }
        });
    }

    /**
     * Once BitArrays per bucket exceed the memory threshold, the hybrid collector moves on to roaring bitmaps, which only
     * pay for the ordinals each bucket holds.
     */
    public void testHybridCollectorFallsBackToRoaringBitmaps() throws IOException {
        int groups = 2500;
        List<String[]> docs = new ArrayList<>();
        for (int g = 0; g < groups; g++) {
            docs.add(doc(group(g), app(2 * g)));
            docs.add(doc(group(g), app(2 * g + 1)));
        }
        withIndex(List.of(docs), searcher -> {
            Map<String, Object> debug = new HashMap<>();
            // A BitArray per bucket costs ~670 bytes for 5000 ordinals, a two-value roaring bitmap ~150 bytes.
            Terms terms = (Terms) collect(
                searcher,
                terms(card(null)).size(groups),
                new CardinalityAggregationContext(true, 1 << 20),
                debug
            );
            assertEquals(groups, terms.getBuckets().size());
            for (Terms.Bucket bucket : terms.getBuckets()) {
                assertEquals(2, ((InternalCardinality) bucket.getAggregations().get("card")).getValue());
            }
            assertEquals(1, debug.get("hybrid_collectors_used"));
            assertEquals(1, debug.get("roaring_ordinals_collectors_used"));
            assertEquals(0, debug.get("hybrid_collectors_switched_to_direct"));
        });
    }

    public void testHybridCollectorFallsBackToDirectWhenRoaringBitmapsExceedThreshold() throws IOException {
        withIndex(SEGMENTS, searcher -> {
            Map<String, Object> debug = new HashMap<>();
            Terms terms = (Terms) collect(searcher, terms(card(null)), new CardinalityAggregationContext(true, 1), debug);
            assertEquals(2, terms.getBuckets().size());
            assertEquals(3, debug.get("roaring_ordinals_collectors_used"));
            assertEquals(3, debug.get("hybrid_collectors_switched_to_direct"));
        });
    }

    public void testHashesEveryBucket() throws IOException {
        int buckets = 300, values = 2000;
        List<String[]> docs = new ArrayList<>();
        Map<Long, Set<String>> expected = new HashMap<>();
        for (int i = 0; i < 6000; i++) {
            String app = app((i * 7919) % values);
            docs.add(doc("unused", app));
            expected.computeIfAbsent((long) (i % buckets), b -> new HashSet<>()).add(app);
        }
        withIndex(List.of(docs), searcher -> {
            LeafReaderContext leaf = searcher.getIndexReader().leaves().get(0);
            BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
            AtomicLong breaker = new AtomicLong();
            try (
                HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1);
                RoaringOrdinalsCollector collector = new RoaringOrdinalsCollector(
                    counts,
                    leaf.reader().getSortedSetDocValues("app"),
                    new BucketOrdinalBitmaps(bigArrays, breaker::addAndGet),
                    Long.MAX_VALUE
                )
            ) {
                for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                    collector.collect(doc, doc % buckets);
                }
                collector.postCollect();
                for (long bucket = 0; bucket < buckets; bucket++) {
                    assertEquals("bucket " + bucket, expected.get(bucket).size(), counts.cardinality(bucket));
                }
            }
            assertEquals("breaker must be released", 0, breaker.get());
        });
    }

    /**
     * Bulk collection through a {@link DocIdStream} or a doc range falls back through the stages like per-document collection.
     */
    public void testBulkCollectionFallsBack() throws IOException {
        List<String[]> docs = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            docs.add(doc("g", app(i)));
        }
        withIndex(List.of(docs), searcher -> {
            LeafReader reader = searcher.getIndexReader().leaves().get(0).reader();
            BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
            AtomicLong breaker = new AtomicLong();
            // A BitArray over 10000 ordinals costs ~1.3KB, a roaring bitmap of three values ~150 bytes
            for (long memoryThreshold : new long[] { 1024, 1 }) {
                for (boolean stream : new boolean[] { true, false }) {
                    try (
                        HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1);
                        HybridCollector collector = hybridCollector(counts, reader, bigArrays, breaker, memoryThreshold)
                    ) {
                        if (stream) {
                            collector.collect(docIdStream(3, 7, 11), 0);
                        } else {
                            collector.collectRange(3, 6);
                        }
                        collector.postCollect();
                        assertEquals(3, counts.cardinality(0));
                        Class<?> expected = memoryThreshold == 1 ? DirectCollector.class : RoaringOrdinalsCollector.class;
                        assertThat(collector.getActiveCollector(), instanceOf(expected));
                    }
                    assertEquals("breaker must be released", 0, breaker.get());
                }
            }
        });
    }

    public void testHybridCollectorFailsWhenLastCollectorExceedsThreshold() throws IOException {
        withIndex(SEGMENTS, searcher -> {
            LeafReader reader = searcher.getIndexReader().leaves().get(0).reader();
            BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
            try (
                HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1);
                HybridCollector collector = new HybridCollector(
                    new OrdinalsCollector(counts, reader.getSortedSetDocValues("app"), bigArrays, 1, true)
                )
            ) {
                expectThrows(IllegalStateException.class, () -> collector.collect(0, 0));
            }
        });
    }

    public void testDocsWithoutValues() throws IOException {
        withIndex(List.of(List.of(doc("g", "a"), doc("g", null), doc("g", null))), searcher -> {
            LeafReader reader = searcher.getIndexReader().leaves().get(0).reader();
            BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
            AtomicLong breaker = new AtomicLong();
            for (int[] collected : new int[][] { { 1, 2 }, { 0, 1, 2 } }) {
                try (
                    HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1);
                    RoaringOrdinalsCollector collector = new RoaringOrdinalsCollector(
                        counts,
                        reader.getSortedSetDocValues("app"),
                        new BucketOrdinalBitmaps(bigArrays, breaker::addAndGet),
                        Long.MAX_VALUE
                    )
                ) {
                    for (int doc : collected) {
                        collector.collect(doc, 0);
                    }
                    collector.postCollect();
                    assertEquals(collected.length == 2 ? 0 : 1, counts.cardinality(0));
                }
            }
            assertEquals("breaker must be released", 0, breaker.get());
        });
    }

    public void testRejectsMoreThanIntegerMaxValueOrdinals() {
        SortedSetDocValues values = mock(SortedSetDocValues.class);
        when(values.getValueCount()).thenReturn(Integer.MAX_VALUE + 1L);
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        AtomicLong breaker = new AtomicLong();
        BucketOrdinalBitmaps bitmaps = new BucketOrdinalBitmaps(bigArrays, breaker::addAndGet);
        expectThrows(IllegalArgumentException.class, () -> new RoaringOrdinalsCollector(null, values, bitmaps, Long.MAX_VALUE));
        assertEquals("breaker must be released", 0, breaker.get());
    }

    public void testBitmapsReleaseArraysWhenAllocationBreaks() {
        AtomicInteger allocations = new AtomicInteger();
        CircuitBreaker requestBreaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST) {
            @Override
            public double addEstimateBytesAndMaybeBreak(long bytes, String label) {
                if (allocations.incrementAndGet() > 1) {
                    throw new CircuitBreakingException("test", bytes, 0, CircuitBreaker.Durability.TRANSIENT);
                }
                return 0;
            }
        };
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(requestBreaker);
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService).withCircuitBreaking();
        // the first array is allocated, the second one breaks: the first must be released (checked by MockBigArrays)
        expectThrows(CircuitBreakingException.class, () -> new BucketOrdinalBitmaps(bigArrays, bytes -> {}));
    }

    public void testBitmapAccountingStaysCloseToBitmapSize() {
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        // one dense bucket, many small interleaved buckets, and values spread over many containers
        assertAccounting(bigArrays, 1, 50_000, 100_000);
        assertAccounting(bigArrays, 2_000, 20, 1_000_000);
        assertAccounting(bigArrays, 500, 40, Integer.MAX_VALUE);
    }

    /**
     * A value that opens a container costs ~25 times more than one added to an existing container. A bitmap that turns from
     * dense to sparse must still be accounted within 2x, both while its value count doubles and once it is re-measured every
     * {@value BucketOrdinalBitmaps#MAX_VALUES_BETWEEN_MEASUREMENTS} values.
     */
    public void testBitmapAccountingWhenValuesOpenNewContainers() {
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        for (int denseValues : new int[] { 2048, 5000 }) {
            AtomicLong breaker = new AtomicLong();
            try (BucketOrdinalBitmaps bitmaps = new BucketOrdinalBitmaps(bigArrays, breaker::addAndGet)) {
                for (int ord = 0; ord < denseValues; ord++) {
                    bitmaps.add(0, ord);
                }
                RoaringBitmap bitmap = bitmaps.get(0);
                for (int container = 1; container < 3000; container++) {
                    bitmaps.add(0, container << 16);
                    long actual = BucketOrdinalBitmaps.ramBytesUsed(bitmap);
                    // an array container that turned into a bitmap container since the last measurement is a few bytes smaller
                    assertThat("after " + bitmap.getCardinality() + " values", bitmaps.bytesUsed(), lessThanOrEqualTo(actual + 16));
                    assertThat("after " + bitmap.getCardinality() + " values", bitmaps.bytesUsed(), greaterThanOrEqualTo(actual / 2));
                }
            }
            assertEquals("breaker must be released", 0, breaker.get());
        }
    }

    private void assertAccounting(BigArrays bigArrays, int buckets, int valuesPerBucket, int maxOrd) {
        AtomicLong breaker = new AtomicLong();
        try (BucketOrdinalBitmaps bitmaps = new BucketOrdinalBitmaps(bigArrays, breaker::addAndGet)) {
            for (int i = 0; i < valuesPerBucket; i++) {
                for (int bucket = 0; bucket < buckets; bucket++) {
                    bitmaps.add(bucket, randomIntBetween(0, maxOrd - 1));
                }
            }
            long actual = 0;
            for (int bucket = 0; bucket < buckets; bucket++) {
                RoaringBitmap bitmap = bitmaps.get(bucket);
                actual += bitmap == null ? 0 : BucketOrdinalBitmaps.ramBytesUsed(bitmap);
            }
            assertThat(bitmaps.bytesUsed(), lessThanOrEqualTo(actual));
            assertThat(bitmaps.bytesUsed(), greaterThanOrEqualTo(actual / 2));
            bitmaps.flushBreaker();
            bitmaps.flushBreaker(); // nothing left to flush
            assertEquals(bitmaps.bytesUsed(), breaker.get());
        }
        assertEquals("breaker must be released", 0, breaker.get());
    }

    /**
     * A bucket keeps its first ordinal inline, without a bitmap or any charged bytes, until a second distinct one arrives.
     */
    public void testSingleOrdinalBucketsStayInline() {
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        AtomicLong breaker = new AtomicLong();
        try (BucketOrdinalBitmaps bitmaps = new BucketOrdinalBitmaps(bigArrays, breaker::addAndGet)) {
            int buckets = randomIntBetween(1, 5000);
            for (int bucket = 0; bucket < buckets; bucket++) {
                bitmaps.add(bucket, bucket * 7);
                bitmaps.add(bucket, bucket * 7); // a duplicate stays inline
            }
            assertEquals(0, bitmaps.bytesUsed());
            for (int bucket = 0; bucket < buckets; bucket++) {
                assertNull(bitmaps.get(bucket));
                assertEquals(1, bitmaps.cardinality(bucket));
                assertEquals(bucket * 7, bitmaps.singleOrd(bucket));
            }
            assertEquals(0, bitmaps.cardinality(buckets + 10));
            assertEquals(-1, bitmaps.singleOrd(buckets + 10));

            int bucket = randomInt(buckets - 1);
            bitmaps.add(bucket, Integer.MAX_VALUE - 1);
            RoaringBitmap bitmap = bitmaps.get(bucket);
            assertNotNull(bitmap);
            assertEquals(2, bitmaps.cardinality(bucket));
            assertTrue(bitmap.contains(bucket * 7));
            assertTrue(bitmap.contains(Integer.MAX_VALUE - 1));
            assertEquals(-1, bitmaps.singleOrd(bucket));
            assertEquals(BucketOrdinalBitmaps.ramBytesUsed(bitmap), bitmaps.bytesUsed());
        }
        assertEquals("breaker must be released", 0, breaker.get());
    }

    /**
     * Buckets with one ordinal, a few, or many, over ordinals that are mostly visited (hashes looked up by ordinal) or rarely
     * visited (looked up by rank), get exactly the sketches of their values.
     */
    public void testCollectIntoMatchesPerBucketSketches() throws IOException {
        int values = randomIntBetween(500, 20_000);
        List<String[]> docs = new ArrayList<>();
        for (int i = 0; i < values; i++) {
            docs.add(doc("unused", app(i)));
        }
        withIndex(List.of(docs), searcher -> {
            LeafReader reader = searcher.getIndexReader().leaves().get(0).reader();
            BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
            for (boolean sparse : new boolean[] { false, true }) {
                AtomicLong breaker = new AtomicLong();
                int buckets = randomIntBetween(1, 300);
                Map<Long, Set<Integer>> expected = new HashMap<>();
                try (
                    HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1);
                    BucketOrdinalBitmaps bitmaps = new BucketOrdinalBitmaps(bigArrays, breaker::addAndGet)
                ) {
                    if (sparse == false) {
                        // one bucket holding every value makes the ordinals dense, so hashes are looked up by ordinal
                        for (int ord = 0; ord < values; ord++) {
                            bitmaps.add(buckets, ord);
                            expected.computeIfAbsent((long) buckets, b -> new HashSet<>()).add(ord);
                        }
                    }
                    for (long bucket = 0; bucket < buckets; bucket++) {
                        // empty, a single value, a few, or more than a rank lookup handles
                        int size = randomFrom(0, 1, randomIntBetween(2, 64), randomIntBetween(65, 400));
                        for (int i = 0; i < size; i++) {
                            int ord = sparse ? randomInt(values / 50) * 50 % values : randomInt(values - 1);
                            bitmaps.add(bucket, ord);
                            expected.computeIfAbsent(bucket, b -> new HashSet<>()).add(ord);
                        }
                    }
                    SortedSetDocValues ords = reader.getSortedSetDocValues("app");
                    bitmaps.collectInto(counts, ords);
                    for (long bucket = 0; bucket <= buckets; bucket++) {
                        try (HyperLogLogPlusPlus sketch = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1)) {
                            MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                            for (int ord : expected.getOrDefault(bucket, Set.of())) {
                                BytesRef value = ords.lookupOrd(ord);
                                MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                                sketch.collect(0, hash.h1);
                            }
                            assertEquals("bucket " + bucket + " sparse=" + sparse, sketch.cardinality(0), counts.cardinality(bucket));
                            assertTrue("bucket " + bucket + " sparse=" + sparse, sketch.equals(0, counts, bucket));
                        }
                    }
                }
                assertEquals("breaker must be released", 0, breaker.get());
            }
        });
    }

    /**
     * Bits and bitmaps holding the same ordinals give the same counts and the same sketches, whether all buckets are hashed
     * at once or one at a time, and bits copied into bitmaps hold the same ordinals.
     */
    public void testBitsMatchBitmaps() throws IOException {
        int values = randomIntBetween(500, 5_000);
        List<String[]> docs = new ArrayList<>();
        for (int i = 0; i < values; i++) {
            docs.add(doc("unused", app(i)));
        }
        withIndex(List.of(docs), searcher -> {
            LeafReader reader = searcher.getIndexReader().leaves().get(0).reader();
            BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
            AtomicLong breaker = new AtomicLong();
            int buckets = randomIntBetween(1, 200);
            try (
                BucketOrdinalBits bits = new BucketOrdinalBits(bigArrays, values, Long.MAX_VALUE, true);
                BucketOrdinalBitmaps bitmaps = new BucketOrdinalBitmaps(bigArrays, breaker::addAndGet);
                BucketOrdinalBitmaps copied = new BucketOrdinalBitmaps(bigArrays, breaker::addAndGet)
            ) {
                for (long bucket = 0; bucket < buckets; bucket++) {
                    // empty, a single value, a few, or many, with duplicates
                    int size = randomFrom(0, 1, randomIntBetween(2, 64), randomIntBetween(65, 2 * values));
                    for (int i = 0; i < size; i++) {
                        int ord = randomInt(values - 1);
                        bits.addCounted(bucket, bits.bucket(bucket), ord);
                        bitmaps.add(bucket, ord);
                    }
                }
                bits.copyTo(copied);
                for (long bucket = 0; bucket <= buckets; bucket++) {
                    assertEquals("bucket " + bucket, bitmaps.cardinality(bucket), bits.cardinality(bucket));
                    assertEquals("bucket " + bucket, bitmaps.cardinality(bucket), copied.cardinality(bucket));
                    assertEquals("bucket " + bucket, bitmaps.singleOrd(bucket), copied.singleOrd(bucket));
                    assertEquals("bucket " + bucket, bitmaps.get(bucket), copied.get(bucket));
                }
                SortedSetDocValues ords = reader.getSortedSetDocValues("app");
                try (
                    HyperLogLogPlusPlus fromBits = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1);
                    HyperLogLogPlusPlus fromBitmaps = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1);
                    HyperLogLogPlusPlus bitsByBucket = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1);
                    HyperLogLogPlusPlus bitmapsByBucket = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1);
                    HyperLogLogPlusPlus bitsSome = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1);
                    HyperLogLogPlusPlus bitmapsSome = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1)
                ) {
                    bits.collectInto(fromBits, ords);
                    bitmaps.collectInto(fromBitmaps, ords);
                    for (long bucket = 0; bucket <= buckets; bucket++) {
                        bits.collectInto(bitsByBucket, ords, new long[] { bucket });
                        bitmaps.collectInto(bitmapsByBucket, ords, new long[] { bucket });
                    }
                    // some of the buckets at once, in any order, including buckets that were never collected
                    Set<Long> some = new HashSet<>();
                    for (long bucket = 0; bucket <= buckets + 5; bucket++) {
                        if (randomBoolean()) {
                            some.add(bucket);
                        }
                    }
                    long[] someBuckets = some.stream().mapToLong(Long::longValue).toArray();
                    bits.collectInto(bitsSome, ords, someBuckets);
                    bitmaps.collectInto(bitmapsSome, ords, someBuckets);
                    for (long bucket = 0; bucket <= buckets; bucket++) {
                        long expected = fromBitmaps.cardinality(bucket);
                        assertEquals("bucket " + bucket, expected, fromBits.cardinality(bucket));
                        assertTrue("bucket " + bucket, fromBitmaps.equals(bucket, fromBits, bucket));
                        assertTrue("bucket " + bucket, fromBitmaps.equals(bucket, bitsByBucket, bucket));
                        assertTrue("bucket " + bucket, fromBitmaps.equals(bucket, bitmapsByBucket, bucket));
                        long expectedSome = some.contains(bucket) ? expected : 0;
                        assertEquals("bucket " + bucket, expectedSome, bitsSome.cardinality(bucket));
                        assertEquals("bucket " + bucket, expectedSome, bitmapsSome.cardinality(bucket));
                        if (some.contains(bucket)) {
                            assertTrue("bucket " + bucket, fromBitmaps.equals(bucket, bitsSome, bucket));
                            assertTrue("bucket " + bucket, fromBitmaps.equals(bucket, bitmapsSome, bucket));
                        }
                    }
                }
            }
            assertEquals("breaker must be released", 0, breaker.get());
        });
    }

    /**
     * A bucket only gets bits while they fit under the threshold, and a bucket refused bits isn't charged for any.
     */
    public void testBitsStayUnderThreshold() {
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        int maxOrd = randomIntBetween(1, 100_000);
        long perBucket = OrdinalsCollector.memoryOverhead(maxOrd);
        int fit = randomIntBetween(0, 20);
        long threshold = fit * perBucket + randomLongBetween(0, perBucket - 1);
        try (BucketOrdinalBits bits = new BucketOrdinalBits(bigArrays, maxOrd, threshold, false)) {
            for (int bucket = 0; bucket < fit; bucket++) {
                BitArray bucketBits = bits.bucket(bucket);
                assertNotNull(bucketBits);
                assertSame(bucketBits, bits.bucket(bucket));
                bucketBits.set(maxOrd - 1);
            }
            assertEquals(fit * perBucket, bits.memoryUsed());
            assertNull(bits.bucket(fit + randomIntBetween(0, 1000)));
            assertEquals(fit * perBucket, bits.memoryUsed());
        }
        try (BucketOrdinalBits bits = new BucketOrdinalBits(bigArrays, maxOrd, Long.MAX_VALUE, true)) {
            BitArray bucketBits = bits.bucket(3);
            bits.addCounted(3, bucketBits, maxOrd - 1);
            bits.addCounted(3, bucketBits, maxOrd - 1);
            assertEquals(1, bits.cardinality(3));
            assertEquals(0, bits.cardinality(2));
            assertEquals(0, bits.cardinality(5000));
        }
    }

    /**
     * When counting, sparse ordinals stop getting bits past a share of the threshold, dense ones keep getting them up to it.
     */
    public void testCountedBitsStopEarlyWhenSparse() {
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        int maxOrd = randomIntBetween(1024, 100_000);
        long perBucket = OrdinalsCollector.memoryOverhead(maxOrd);
        int fit = randomIntBetween(2 * BucketOrdinalBits.SPARSE_SHARE, 100);
        long threshold = fit * perBucket;
        int share = (int) (threshold / BucketOrdinalBits.SPARSE_SHARE / perBucket);
        for (boolean dense : new boolean[] { false, true }) {
            try (BucketOrdinalBits bits = new BucketOrdinalBits(bigArrays, maxOrd, threshold, true)) {
                int bucket = 0;
                for (BitArray bucketBits; (bucketBits = bits.bucket(bucket)) != null; bucket++) {
                    // a dense bucket holds every fourth ordinal, a sparse one a single ordinal
                    int ordinals = dense ? (maxOrd + 3) / 4 : 1;
                    for (int i = 0; i < ordinals; i++) {
                        bits.addCounted(bucket, bucketBits, i * 4L);
                    }
                }
                assertEquals("dense=" + dense, dense ? fit : share, bucket);
                // a bucket that has bits keeps them
                assertNotNull(bits.bucket(0));
            }
        }
    }

    /**
     * The rank of every value of a set spread over dense and sparse blocks of 65536 ordinals is its position in the set, whether
     * ranks are asked in ascending or random order.
     */
    public void testVisitedRanks() {
        RoaringBitmap set = new RoaringBitmap();
        int blocks = randomIntBetween(1, 12);
        for (int block = 0; block < blocks; block++) {
            int base = (block * randomIntBetween(1, 3)) << 16;
            switch (randomInt(3)) {
                case 0 -> {
                } // empty block
                case 1 -> set.add(base + randomInt(65535)); // a single value
                case 2 -> { // sparse, up to the dense limit
                    for (int i = randomIntBetween(2, VisitedRanks.DENSE_VALUES); i > 0; i--) {
                        set.add(base + randomInt(65535));
                    }
                }
                default -> { // dense: random values or a range
                    if (randomBoolean()) {
                        set.add((long) base, (long) base + randomIntBetween(VisitedRanks.DENSE_VALUES + 1, 65536));
                    } else {
                        for (int i = randomIntBetween(VisitedRanks.DENSE_VALUES + 1, 40_000); i > 0; i--) {
                            set.add(base + randomInt(65535));
                        }
                    }
                }
            }
        }
        if (randomBoolean()) {
            set.runOptimize();
        }
        int[] values = set.toArray();
        VisitedRanks ranks = new VisitedRanks(set);
        for (int i = 0; i < values.length; i++) {
            assertEquals("value " + values[i], i, ranks.rank(values[i]));
        }
        for (int n = 0; n < Math.min(values.length, 10_000); n++) {
            int i = randomInt(values.length - 1);
            assertEquals("value " + values[i], i, ranks.rank(values[i]));
        }
        assertThat(
            VisitedRanks.ramBytesUsed(set),
            lessThanOrEqualTo(4096L + 4L * values.length + 16L * set.getContainerCount() + 12288L * set.getContainerCount())
        );
    }

    /**
     * Over an ordinal space more than twice as large as the visited ordinals, hashes are found by rank, here across several
     * blocks of 65536 ordinals, some dense and some sparse, and buckets still get exactly the sketches of their values.
     */
    public void testCollectIntoByRankAcrossBlocks() throws IOException {
        int values = 4 * 65536 + randomIntBetween(0, 1000);
        List<String[]> docs = new ArrayList<>(values);
        for (int i = 0; i < values; i++) {
            docs.add(doc("unused", app(i)));
        }
        withIndex(List.of(docs), searcher -> {
            LeafReader reader = searcher.getIndexReader().leaves().get(0).reader();
            SortedSetDocValues ords = reader.getSortedSetDocValues("app");
            assertEquals(values, ords.getValueCount());
            BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
            AtomicLong breaker = new AtomicLong();
            Map<Long, Set<Integer>> expected = new HashMap<>();
            try (
                HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1);
                BucketOrdinalBitmaps bitmaps = new BucketOrdinalBitmaps(bigArrays, breaker::addAndGet)
            ) {
                // a dense block from a few big buckets, then single values, small and medium buckets spread over all blocks
                int denseBlock = randomInt(3);
                for (long bucket = 0; bucket < 3; bucket++) {
                    for (int i = 0; i < 20_000; i++) {
                        int ord = (denseBlock << 16) + randomInt(65535);
                        bitmaps.add(bucket, ord);
                        expected.computeIfAbsent(bucket, b -> new HashSet<>()).add(ord);
                    }
                }
                int buckets = randomIntBetween(100, 2000);
                for (long bucket = 3; bucket < buckets; bucket++) {
                    int size = randomFrom(1, randomIntBetween(2, 64), randomIntBetween(65, 300));
                    for (int i = 0; i < size; i++) {
                        int ord = randomInt(values - 1);
                        bitmaps.add(bucket, ord);
                        expected.computeIfAbsent(bucket, b -> new HashSet<>()).add(ord);
                    }
                }
                long visited = expected.values().stream().flatMap(Set::stream).distinct().count();
                assertTrue("hashes must be found by rank", values > 2 * visited);
                bitmaps.collectInto(counts, ords);
                for (long bucket = 0; bucket < buckets; bucket++) {
                    try (HyperLogLogPlusPlus sketch = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1)) {
                        MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                        for (int ord : expected.getOrDefault(bucket, Set.of())) {
                            BytesRef value = ords.lookupOrd(ord);
                            MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                            sketch.collect(0, hash.h1);
                        }
                        assertEquals("bucket " + bucket, sketch.cardinality(0), counts.cardinality(bucket));
                        assertTrue("bucket " + bucket, sketch.equals(0, counts, bucket));
                    }
                }
            }
            assertEquals("breaker must be released", 0, breaker.get());
        });
    }

    // ---------------------------------------------------------------------------------------------------------------

    @FunctionalInterface
    private interface SearcherConsumer {
        void accept(IndexSearcher searcher) throws IOException;
    }

    private static String[] doc(String group, String app) {
        return new String[] { group, app };
    }

    private static HybridCollector hybridCollector(
        HyperLogLogPlusPlus counts,
        LeafReader reader,
        BigArrays bigArrays,
        AtomicLong breaker,
        long memoryThreshold
    ) throws IOException {
        return new HybridCollector(
            new OrdinalsCollector(counts, reader.getSortedSetDocValues("app"), bigArrays, memoryThreshold, true),
            () -> new RoaringOrdinalsCollector(
                counts,
                reader.getSortedSetDocValues("app"),
                new BucketOrdinalBitmaps(bigArrays, breaker::addAndGet),
                memoryThreshold
            ),
            () -> new DirectCollector(counts, MurmurHash3Values.hash(FieldData.toString(reader.getSortedSetDocValues("app"))))
        );
    }

    /**
     * A single-use stream of {@code docs}, like the ones bulk scorers pass to collectors.
     */
    private static DocIdStream docIdStream(int... docs) {
        return new DocIdStream() {
            private int next;

            @Override
            public void forEach(int upTo, CheckedIntConsumer<IOException> consumer) throws IOException {
                while (next < docs.length && docs[next] < upTo) {
                    consumer.accept(docs[next++]);
                }
            }

            @Override
            public int count(int upTo) {
                int count = 0;
                while (next < docs.length && docs[next] < upTo) {
                    next++;
                    count++;
                }
                return count;
            }

            @Override
            public int intoArray(int upTo, int[] array) {
                int count = 0;
                while (next < docs.length && docs[next] < upTo && count < array.length) {
                    array[count++] = docs[next++];
                }
                return count;
            }

            @Override
            public boolean mayHaveRemaining() {
                return next < docs.length;
            }
        };
    }

    private static String group(int i) {
        return String.format(Locale.ROOT, "g%05d", i);
    }

    private static String app(int i) {
        return String.format(Locale.ROOT, "a%05d", i);
    }

    private void withIndex(List<List<String[]>> segments, SearcherConsumer consumer) throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (List<String[]> segment : segments) {
                    for (String[] values : segment) {
                        Document doc = new Document();
                        doc.add(new SortedSetDocValuesField("group", new BytesRef(values[0])));
                        if (values[1] != null) {
                            doc.add(new SortedSetDocValuesField("app", new BytesRef(values[1])));
                        }
                        w.addDocument(doc);
                    }
                    w.commit();
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(segments.size(), reader.leaves().size());
                consumer.accept(newIndexSearcher(reader));
            }
        }
    }

    private static CardinalityAggregationBuilder card(String hint) {
        CardinalityAggregationBuilder builder = new CardinalityAggregationBuilder("card").field("app");
        if (hint != null) {
            builder.executionHint(hint);
        }
        return builder;
    }

    private static TermsAggregationBuilder terms(CardinalityAggregationBuilder card) {
        return new TermsAggregationBuilder("groups").field("group").size(10).subAggregation(card);
    }

    /**
     * Collects {@code builder} and returns its shard-level result, putting the debug info of the cardinality aggregator into
     * {@code cardDebug}.
     */
    private Terms collect(
        IndexSearcher searcher,
        TermsAggregationBuilder builder,
        CardinalityAggregationContext cardinalityContext,
        Map<String, Object> cardDebug
    ) throws IOException {
        Aggregator aggregator = createAggregatorWithCustomizableSearchContext(
            new MatchAllDocsQuery(),
            builder,
            searcher,
            createIndexSettings(),
            new MultiBucketConsumerService.MultiBucketConsumer(
                Integer.MAX_VALUE,
                new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
            ),
            searchContext -> when(searchContext.cardinalityAggregationContext()).thenReturn(cardinalityContext),
            new KeywordFieldMapper.KeywordFieldType("group"),
            new KeywordFieldMapper.KeywordFieldType("app")
        );
        // Look the cardinality aggregator up before collection starts, when it isn't wrapped by a breadth-first parent yet.
        Aggregator card = aggregator.subAggregator("card");
        aggregator.preCollection();
        searcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        Terms result = (Terms) aggregator.buildTopLevel();
        card.collectDebugInfo(cardDebug::put);
        return result;
    }
}
