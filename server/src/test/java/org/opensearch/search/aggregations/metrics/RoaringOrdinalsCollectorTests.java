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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.BucketOrdinalBitmaps;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.RoaringOrdinalsCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RoaringOrdinalsCollector}, the roaring bitmap stage of the cardinality {@link CardinalityAggregator.HybridCollector}.
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

    public void testBitmapAccountingStaysCloseToBitmapSize() {
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        // one dense bucket, many small interleaved buckets, and values spread over many containers
        assertAccounting(bigArrays, 1, 50_000, 100_000);
        assertAccounting(bigArrays, 2_000, 20, 1_000_000);
        assertAccounting(bigArrays, 500, 40, Integer.MAX_VALUE);
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
                        doc.add(new SortedSetDocValuesField("app", new BytesRef(values[1])));
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
