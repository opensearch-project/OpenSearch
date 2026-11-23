/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitMixer;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Cardinality under a parent with many buckets keeps its sketches in a {@link CompactHyperLogLogPlusPlus}. These tests check
 * that ordering a {@code terms} aggregation by such a cardinality returns the same buckets and values as sketches built
 * one bucket at a time, whatever collector runs, and that the sketches fit under a request circuit breaker that the
 * {@code 2^precision} bytes per bucket of {@link HyperLogLogPlusPlus} would exceed.
 */
public class CompactSketchesAggregatorTests extends AggregatorTestCase {

    private static final String GROUP = "group";
    private static final String KEYWORD = "value";
    private static final String NUMBER = "number";

    private static final MappedFieldType[] FIELD_TYPES = new MappedFieldType[] {
        new KeywordFieldMapper.KeywordFieldType(GROUP),
        new KeywordFieldMapper.KeywordFieldType(KEYWORD),
        new NumberFieldMapper.NumberFieldType(NUMBER, NumberFieldMapper.NumberType.LONG) };

    /**
     * Groups and the values each one holds. A few groups hold more values than linear counting allows, most hold a few.
     */
    private static Map<String, List<Long>> randomGroups(int threshold) {
        final Map<String, List<Long>> groups = new TreeMap<>();
        final int count = randomIntBetween(50, 1500);
        final int dense = randomIntBetween(1, 4);
        for (int g = 0; g < count; g++) {
            final int distinct = g < dense ? randomIntBetween(threshold / 2, 2 * threshold) : randomIntBetween(0, 3);
            final List<Long> values = new ArrayList<>();
            // Groups share values so the same values end up in different buckets
            final long first = randomLongBetween(0, 5000);
            for (long v = first; v < first + distinct; v++) {
                values.add(v);
            }
            groups.put(String.format(Locale.ROOT, "g%05d", g), values);
        }
        return groups;
    }

    private static String keyword(long value) {
        return "v" + value;
    }

    /**
     * Writes one or more docs per value of each group, sometimes two values in a doc and sometimes a doc without value,
     * spread over a few segments.
     */
    private static void index(Directory dir, Map<String, List<Long>> groups) throws IOException {
        try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            int docs = 0;
            for (Map.Entry<String, List<Long>> group : groups.entrySet()) {
                final List<Long> values = group.getValue();
                if (values.isEmpty() || rarely()) {
                    w.addDocument(doc(group.getKey()));
                }
                for (int i = 0; i < values.size(); i++) {
                    final int copies = randomIntBetween(1, 2);
                    for (int c = 0; c < copies; c++) {
                        if (i + 1 < values.size() && randomBoolean()) {
                            w.addDocument(doc(group.getKey(), values.get(i), values.get(randomIntBetween(i + 1, values.size() - 1))));
                        } else {
                            w.addDocument(doc(group.getKey(), values.get(i)));
                        }
                        if (++docs % 4000 == 0 && randomBoolean()) {
                            w.commit();
                        }
                    }
                }
            }
        }
    }

    private static Document doc(String group, long... values) {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField(GROUP, new BytesRef(group)));
        for (long value : values) {
            doc.add(new SortedSetDocValuesField(KEYWORD, new BytesRef(keyword(value))));
            doc.add(new SortedNumericDocValuesField(NUMBER, value));
        }
        return doc;
    }

    /**
     * The cardinality of each group, from a sketch built for that group alone and hashed like the aggregator hashes.
     */
    private static Map<String, Long> expectedCardinalities(Map<String, List<Long>> groups, int precision, boolean keyword) {
        final Map<String, Long> expected = new HashMap<>();
        final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
        for (Map.Entry<String, List<Long>> group : groups.entrySet()) {
            try (HyperLogLogPlusPlus sketch = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1)) {
                for (long value : group.getValue()) {
                    if (keyword) {
                        final byte[] bytes = keyword(value).getBytes(StandardCharsets.UTF_8);
                        MurmurHash3.hash128(bytes, 0, bytes.length, 0, hash);
                        sketch.collect(0, hash.h1);
                    } else {
                        sketch.collect(0, BitMixer.mix64(value));
                    }
                }
                expected.put(group.getKey(), sketch.cardinality(0));
            }
        }
        return expected;
    }

    /**
     * The {@code size} groups with the highest (or lowest) cardinality, ties broken by key like {@code terms} does.
     */
    private static List<String> expectedTop(Map<String, Long> cardinalities, boolean asc, int size) {
        Comparator<Map.Entry<String, Long>> byCardinality = Map.Entry.comparingByValue();
        if (asc == false) {
            byCardinality = byCardinality.reversed();
        }
        return cardinalities.entrySet()
            .stream()
            .sorted(byCardinality.thenComparing(Map.Entry.comparingByKey()))
            .limit(size)
            .map(Map.Entry::getKey)
            .toList();
    }

    private static long randomPrecisionThreshold() {
        // precisions 9 (below the compact threshold), 12, 14 and 15
        return randomFrom(100L, 1000L, 3000L, 5000L);
    }

    private static TermsAggregationBuilder termsByCardinality(CardinalityAggregationBuilder card, boolean asc, int size, int shardSize) {
        return new TermsAggregationBuilder("groups").field(GROUP)
            .size(size)
            .shardSize(shardSize)
            .order(BucketOrder.aggregation("card", asc))
            .subAggregation(card);
    }

    public void testOrderByCardinalityMatchesPerBucketSketches() throws IOException {
        final long precisionThreshold = randomPrecisionThreshold();
        final int precision = HyperLogLogPlusPlus.precisionFromThreshold(precisionThreshold);
        final Map<String, List<Long>> groups = randomGroups((1 << precision) / 4 * 3 / 4);
        try (Directory dir = newDirectory()) {
            index(dir, groups);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = newIndexSearcher(reader);
                for (boolean keyword : new boolean[] { true, false }) {
                    final Map<String, Long> expected = expectedCardinalities(groups, precision, keyword);
                    for (String hint : keyword ? new String[] { null, "direct", "ordinals" } : new String[] { null }) {
                        final boolean hybrid = randomBoolean();
                        // unlimited, small enough to move to roaring bitmaps, or so small that roaring moves to direct
                        final long memoryThreshold = randomFrom(Long.MAX_VALUE, 1L << 16, 1L);
                        final boolean asc = randomBoolean();
                        final int size = randomIntBetween(1, Math.min(200, groups.size()));
                        CardinalityAggregationBuilder card = new CardinalityAggregationBuilder("card").field(keyword ? KEYWORD : NUMBER)
                            .precisionThreshold(precisionThreshold);
                        if (hint != null) {
                            card.executionHint(hint);
                        }
                        final String context = String.format(
                            Locale.ROOT,
                            "keyword=%s hint=%s hybrid=%s threshold=%d asc=%s size=%d precision=%d",
                            keyword,
                            hint,
                            hybrid,
                            memoryThreshold,
                            asc,
                            size,
                            precision
                        );
                        final Map<String, Object> debug = new HashMap<>();
                        final Terms terms = collect(
                            searcher,
                            termsByCardinality(card, asc, size, size),
                            new CardinalityAggregationContext(hybrid, memoryThreshold),
                            new NoneCircuitBreakerService(),
                            debug
                        );
                        assertEquals(
                            context,
                            precision >= CardinalityAggregator.COMPACT_SKETCHES_MIN_PRECISION,
                            debug.get("compact_sketches")
                        );
                        assertTopBuckets(context, expected, asc, size, terms, false);
                    }
                }
            }
        }
    }

    /**
     * Shards rank their buckets by their own sketches and the reduced result merges them.
     */
    public void testOrderByCardinalityAcrossShards() throws IOException {
        final long precisionThreshold = randomPrecisionThreshold();
        final int precision = HyperLogLogPlusPlus.precisionFromThreshold(precisionThreshold);
        final Map<String, List<Long>> groups = randomGroups((1 << precision) / 4 * 3 / 4);
        try (Directory dir = newDirectory()) {
            index(dir, groups);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = newIndexSearcher(reader);
                final boolean keyword = randomBoolean();
                final Map<String, Long> expected = expectedCardinalities(groups, precision, keyword);
                final boolean asc = randomBoolean();
                final int size = randomIntBetween(1, Math.min(100, groups.size()));
                CardinalityAggregationBuilder card = new CardinalityAggregationBuilder("card").field(keyword ? KEYWORD : NUMBER)
                    .precisionThreshold(precisionThreshold);
                // Every shard returns all its buckets, so the merged ranking is exact
                final Terms terms = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    termsByCardinality(card, asc, size, groups.size()),
                    true,
                    FIELD_TYPES
                );
                assertTopBuckets("keyword=" + keyword + " asc=" + asc + " size=" + size, expected, asc, size, terms, true);
            }
        }
    }

    /**
     * Checks that {@code terms} holds the expected top buckets with the expected cardinalities. Shard-level results are sorted
     * by key for the reduce, so their order is only checked once reduced.
     */
    private static void assertTopBuckets(String context, Map<String, Long> expected, boolean asc, int size, Terms terms, boolean reduced) {
        final List<String> expectedKeys = expectedTop(expected, asc, size);
        final List<String> keys = terms.getBuckets().stream().map(Terms.Bucket::getKeyAsString).toList();
        if (reduced) {
            assertEquals(context, expectedKeys, keys);
        } else {
            assertEquals(context, new TreeSet<>(expectedKeys), new TreeSet<>(keys));
        }
        for (Terms.Bucket bucket : terms.getBuckets()) {
            final Cardinality cardinality = bucket.getAggregations().get("card");
            assertEquals(
                context + " bucket " + bucket.getKeyAsString(),
                (long) expected.get(bucket.getKeyAsString()),
                cardinality.getValue()
            );
        }
    }

    /**
     * 3000 buckets of a few values each need about 47MB of {@link HyperLogLogPlusPlus} sketches at the default precision but
     * fit in a few hundred KB of compact ones, so the aggregation succeeds under a 16MB request circuit breaker and releases
     * everything once closed.
     */
    public void testManySparseBucketsFitUnderRequestBreaker() throws IOException {
        final Map<String, List<Long>> groups = new TreeMap<>();
        for (int g = 0; g < 3000; g++) {
            groups.put(String.format(Locale.ROOT, "g%05d", g), List.of((long) g, (long) g + 1));
        }
        final long limit = 16 << 20;
        final AtomicLong used = new AtomicLong();
        final AtomicLong peak = new AtomicLong();
        try (Directory dir = newDirectory()) {
            index(dir, groups);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = newIndexSearcher(reader);
                final Map<String, Long> expected = expectedCardinalities(groups, HyperLogLogPlusPlus.DEFAULT_PRECISION, true);
                for (String hint : new String[] { null, "direct", "ordinals" }) {
                    CardinalityAggregationBuilder card = new CardinalityAggregationBuilder("card").field(KEYWORD);
                    if (hint != null) {
                        card.executionHint(hint);
                    }
                    final Map<String, Object> debug = new HashMap<>();
                    final Terms terms = collect(
                        searcher,
                        termsByCardinality(card, false, 10, 10),
                        // The hybrid collector's own threshold, so that its BitArrays don't count against the 16MB
                        new CardinalityAggregationContext(true, 1 << 20),
                        countingBreakerService(used, peak, limit),
                        debug
                    );
                    assertEquals(true, debug.get("compact_sketches"));
                    assertEquals(0L, debug.get("hyperloglog_buckets"));
                    assertTopBuckets("hint=" + hint, expected, false, 10, terms, false);
                    assertThat("breaker must be released", used.get(), equalTo(0L));
                }
            }
        }
        assertThat(peak.get(), lessThan(limit / 4));
    }

    /**
     * A tripped request circuit breaker fails the aggregation and leaves nothing charged once it is closed.
     */
    public void testBreakerTripReleasesEverything() throws IOException {
        final Map<String, List<Long>> groups = randomGroups(3072);
        final AtomicLong used = new AtomicLong();
        final AtomicLong peak = new AtomicLong();
        try (Directory dir = newDirectory()) {
            index(dir, groups);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = newIndexSearcher(reader);
                final long limit = randomLongBetween(1, 512 << 10);
                CardinalityAggregationBuilder card = new CardinalityAggregationBuilder("card").field(randomFrom(KEYWORD, NUMBER));
                try {
                    collect(
                        searcher,
                        termsByCardinality(card, randomBoolean(), 10, 10),
                        new CardinalityAggregationContext(randomBoolean(), randomFrom(Long.MAX_VALUE, 1L << 16, 1L)),
                        countingBreakerService(used, peak, limit),
                        new HashMap<>()
                    );
                } catch (CircuitBreakingException e) {
                    // expected for most limits
                }
            }
        }
        assertThat("breaker must be released", used.get(), equalTo(0L));
    }

    /**
     * With compact sketches disabled the aggregation goes back to {@link HyperLogLogPlusPlus} and returns the same results.
     */
    public void testCompactSketchesCanBeDisabled() throws IOException {
        final long precisionThreshold = randomFrom(1000L, 3000L);
        final int precision = HyperLogLogPlusPlus.precisionFromThreshold(precisionThreshold);
        final Map<String, List<Long>> groups = randomGroups((1 << precision) / 4 * 3 / 4);
        try (Directory dir = newDirectory()) {
            index(dir, groups);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = newIndexSearcher(reader);
                final Map<String, Long> expected = expectedCardinalities(groups, precision, true);
                final int size = randomIntBetween(1, Math.min(50, groups.size()));
                for (boolean compact : new boolean[] { true, false }) {
                    final Map<String, Object> debug = new HashMap<>();
                    final Terms terms = collect(
                        searcher,
                        termsByCardinality(
                            new CardinalityAggregationBuilder("card").field(KEYWORD).precisionThreshold(precisionThreshold),
                            false,
                            size,
                            size
                        ),
                        new CardinalityAggregationContext(true, 1L << 16, compact),
                        new NoneCircuitBreakerService(),
                        debug
                    );
                    assertEquals(compact, debug.get("compact_sketches"));
                    assertTopBuckets("compact=" + compact, expected, false, size, terms, false);
                }
            }
        }
    }

    /**
     * Top-level cardinality and cardinality under a single-bucket parent keep using {@link HyperLogLogPlusPlus}.
     */
    public void testSingleBucketKeepsHyperLogLogPlusPlus() {
        for (int precision =
            AbstractCardinalityAlgorithm.MIN_PRECISION; precision <= AbstractCardinalityAlgorithm.MAX_PRECISION; precision++) {
            final boolean compact = precision >= CardinalityAggregator.COMPACT_SKETCHES_MIN_PRECISION;
            assertFalse(CardinalityAggregator.useCompactSketches(CardinalityUpperBound.ONE, precision));
            assertFalse(CardinalityAggregator.useCompactSketches(CardinalityUpperBound.NONE, precision));
            assertEquals(compact, CardinalityAggregator.useCompactSketches(CardinalityUpperBound.MANY, precision));
            assertEquals(compact, CardinalityAggregator.useCompactSketches(CardinalityUpperBound.ONE.multiply(2), precision));
        }
    }

    private static CircuitBreakerService countingBreakerService(AtomicLong used, AtomicLong peak, long limit) {
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        // Global ordinals are charged to the fielddata breaker, which these tests don't limit
        when(breakerService.getBreaker(anyString())).thenReturn(new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(new NoopCircuitBreaker(CircuitBreaker.REQUEST) {
            @Override
            public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                if (bytes > 0 && used.get() + bytes > limit) {
                    throw new CircuitBreakingException("test error", bytes, limit, Durability.TRANSIENT);
                }
                final long now = used.addAndGet(bytes);
                peak.accumulateAndGet(now, Math::max);
                return now;
            }

            @Override
            public long addWithoutBreaking(long bytes) {
                return used.addAndGet(bytes);
            }
        });
        return breakerService;
    }

    /**
     * Collects {@code builder} on one shard and returns its shard-level result, putting the debug info of the cardinality
     * aggregator into {@code cardDebug}. The aggregators are closed before returning.
     */
    private Terms collect(
        IndexSearcher searcher,
        TermsAggregationBuilder builder,
        CardinalityAggregationContext cardinalityContext,
        CircuitBreakerService breakerService,
        Map<String, Object> cardDebug
    ) throws IOException {
        final SearchContext searchContext = createSearchContext(
            searcher,
            createIndexSettings(),
            new MatchAllDocsQuery(),
            new MultiBucketConsumerService.MultiBucketConsumer(Integer.MAX_VALUE, breakerService.getBreaker(CircuitBreaker.REQUEST)),
            breakerService,
            FIELD_TYPES
        );
        when(searchContext.cardinalityAggregationContext()).thenReturn(cardinalityContext);
        // Release the aggregators here, even when creating them trips the breaker, so the test can check the breaker
        final List<Releasable> releasables = new ArrayList<>();
        doAnswer(invocation -> releasables.add(invocation.getArgument(0))).when(searchContext).addReleasable(any());
        try {
            final Aggregator aggregator = createAggregator(builder, searchContext);
            // Look the cardinality aggregator up before collection starts, when it isn't wrapped by a breadth-first parent yet
            final Aggregator card = aggregator.subAggregator("card");
            aggregator.preCollection();
            searcher.search(new MatchAllDocsQuery(), aggregator);
            aggregator.postCollection();
            final Terms result = (Terms) aggregator.buildTopLevel();
            card.collectDebugInfo(cardDebug::put);
            return result;
        } finally {
            Releasables.close(releasables);
        }
    }
}
