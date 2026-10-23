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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
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
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.DirectCollector;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.HybridCollector;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.MurmurHash3Values;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.OrdinalsCollector;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.RoaringOrdinalsCollector;

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
                RoaringBitmap bitmap = bitmaps.getOrCreate(0);
                for (int ord = 0; ord < denseValues; ord++) {
                    bitmaps.add(0, bitmap, ord);
                }
                for (int container = 1; container < 3000; container++) {
                    bitmaps.add(0, bitmap, container << 16);
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
                    bitmaps.add(bucket, bitmaps.getOrCreate(bucket), randomIntBetween(0, maxOrd - 1));
                }
            }
            long actual = 0;
            for (int bucket = 0; bucket < buckets; bucket++) {
                actual += BucketOrdinalBitmaps.ramBytesUsed(bitmaps.get(bucket));
            }
            assertThat(bitmaps.bytesUsed(), lessThanOrEqualTo(actual));
            assertThat(bitmaps.bytesUsed(), greaterThanOrEqualTo(actual / 2));
            bitmaps.flushBreaker();
            bitmaps.flushBreaker(); // nothing left to flush
            assertEquals(bitmaps.bytesUsed(), breaker.get());
        }
        assertEquals("breaker must be released", 0, breaker.get());
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
