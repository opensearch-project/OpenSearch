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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Integration tests for Roaring bitmap cardinality collector selection and correctness.
 * Tests through the aggregator API with debug info assertions to verify the decision tree.
 *
 * Scenario: multi-segment index with overlapping keyword values.
 * group X: apps [a, b, c] (distinct=3)
 * group Y: apps [b, c, d, e] (distinct=4)
 */
public class RoaringCardinalityIntegTests extends AggregatorTestCase {

    /**
     * Simple caching IndexFieldDataCache for tests. Stores global ordinals so
     * isGlobalOrdinalsCached returns true after loadGlobal is called.
     */
    static class CachingFieldDataCache implements org.opensearch.index.fielddata.IndexFieldDataCache {
        private final java.util.concurrent.ConcurrentHashMap<Object, Object> cache = new java.util.concurrent.ConcurrentHashMap<>();

        private Object cacheKey(DirectoryReader reader, Object fieldData) {
            return java.util.List.of(reader.getReaderCacheHelper().getKey(), System.identityHashCode(fieldData));
        }

        @Override
        @SuppressWarnings("unchecked")
        public <
            FD extends org.opensearch.index.fielddata.LeafFieldData,
            IFD extends org.opensearch.index.fielddata.IndexFieldData<FD>> FD load(
                org.apache.lucene.index.LeafReaderContext context,
                IFD indexFieldData
            ) throws Exception {
            return indexFieldData.loadDirect(context);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <
            FD extends org.opensearch.index.fielddata.LeafFieldData,
            IFD extends org.opensearch.index.fielddata.IndexFieldData.Global<FD>> IFD load(DirectoryReader indexReader, IFD indexFieldData)
                throws Exception {
            return (IFD) cache.computeIfAbsent(cacheKey(indexReader, indexFieldData), k -> {
                try {
                    return indexFieldData.loadGlobalDirect(indexReader);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public <
            FD extends org.opensearch.index.fielddata.LeafFieldData,
            IFD extends org.opensearch.index.fielddata.IndexFieldData.Global<FD>> boolean isCached(
                DirectoryReader indexReader,
                IFD indexFieldData
            ) {
            return cache.containsKey(cacheKey(indexReader, indexFieldData));
        }

        @Override
        public void clear() {
            cache.clear();
        }

        @Override
        public void clear(String fieldName) {
            cache.clear();
        }
    }

    private final CachingFieldDataCache testCache = new CachingFieldDataCache();
    private final java.util.concurrent.ConcurrentHashMap<String, org.opensearch.index.fielddata.IndexFieldData<?>> fieldDataInstances =
        new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    protected
        org.opensearch.common.TriFunction<
            org.opensearch.index.mapper.MappedFieldType,
            String,
            java.util.function.Supplier<org.opensearch.search.lookup.SearchLookup>,
            org.opensearch.index.fielddata.IndexFieldData<?>>
        getIndexFieldDataLookup(
            org.opensearch.index.mapper.MapperService mapperService,
            org.opensearch.core.indices.breaker.CircuitBreakerService circuitBreakerService
        ) {
        return (fieldType, s, searchLookup) -> fieldDataInstances.computeIfAbsent(
            fieldType.name(),
            k -> fieldType.fielddataBuilder(mapperService.getIndexSettings().getIndex().getName(), searchLookup)
                .build(testCache, circuitBreakerService)
        );
    }

    // ======================== Correctness tests ========================

    /**
     * Single cardinality agg, multiple segments, no parent bucket agg.
     * Verifies cross-segment deduplication.
     */
    public void testSingleBucketMultiSegment() throws IOException {
        withMultiSegmentIndex((searcher) -> {
            var agg = new CardinalityAggregationBuilder("card").field("app");
            InternalCardinality result = searchAndReduce(searcher, new MatchAllDocsQuery(), agg, keywordField("app"));
            // Total distinct apps: a, b, c, d, e = 5
            assertEquals(5, result.getValue());
        });
    }

    /**
     * Cardinality nested under terms, multiple segments.
     * Each bucket's cardinality must be computed independently across segments.
     */
    public void testMultiBucketMultiSegment() throws IOException {
        withMultiSegmentIndex((searcher) -> {
            var termsAgg = termsWithCard("card", "group", "app", null);
            var result = searchAndReduce(searcher, new MatchAllDocsQuery(), termsAgg, keywordField("group"), keywordField("app"));
            assertBucketCardinalities((Terms) result, Map.of("X", 3L, "Y", 4L));
        });
    }

    /**
     * Order by cardinality desc — requires deferred_ordinals.
     * Y(4) should sort before X(3).
     */
    public void testOrderByCardinalityDesc() throws IOException {
        withMultiSegmentIndex((searcher) -> {
            var termsAgg = new TermsAggregationBuilder("groups").field("group")
                .size(10)
                .order(BucketOrder.aggregation("card", false))
                .subAggregation(new CardinalityAggregationBuilder("card").field("app").executionHint("deferred_ordinals"));

            var result = searchAndReduce(searcher, new MatchAllDocsQuery(), termsAgg, keywordField("group"), keywordField("app"));
            var terms = (Terms) result;
            var buckets = terms.getBuckets();
            assertEquals(2, buckets.size());
            assertEquals("Y", buckets.get(0).getKeyAsString());
            assertEquals(4, ((InternalCardinality) buckets.get(0).getAggregations().get("card")).getValue());
            assertEquals("X", buckets.get(1).getKeyAsString());
            assertEquals(3, ((InternalCardinality) buckets.get(1).getAggregations().get("card")).getValue());
        });
    }

    /**
     * All execution hints produce identical results across segments.
     */
    public void testAllHintsProduceSameResults() throws IOException {
        withMultiSegmentIndex((searcher) -> {
            for (String hint : new String[] { null, "direct", "ordinals", "deferred_ordinals" }) {
                var cardBuilder = new CardinalityAggregationBuilder("card").field("app");
                if (hint != null) cardBuilder.executionHint(hint);

                var termsAgg = new TermsAggregationBuilder("groups").field("group").size(10).subAggregation(cardBuilder);
                var result = searchAndReduce(searcher, new MatchAllDocsQuery(), termsAgg, keywordField("group"), keywordField("app"));
                assertBucketCardinalities((Terms) result, Map.of("X", 3L, "Y", 4L), "hint=" + hint);
            }
        });
    }

    // ======================== Debug info / decision tree tests ========================

    /**
     * Forced deferred_ordinals hint → deferred collector used.
     */
    public void testDebugDeferredOrdinals() throws IOException {
        withMultiSegmentIndex((searcher) -> {
            var debug = collectAndGetDebug(
                searcher,
                new CardinalityAggregationBuilder("card").field("app").executionHint("deferred_ordinals")
            );
            assertTrue("deferred collector used", (int) debug.get("deferred_ordinals_collectors_used") > 0);
            assertEquals("no hybrid", 0, debug.get("hybrid_collectors_used"));
            assertEquals("no string hashing", 0, debug.get("string_hashing_collectors_used"));
        });
    }

    /**
     * Forced direct hint → string hashing collector used.
     */
    public void testDebugDirect() throws IOException {
        withMultiSegmentIndex((searcher) -> {
            var debug = collectAndGetDebug(searcher, new CardinalityAggregationBuilder("card").field("app").executionHint("direct"));
            assertTrue("string hashing used", (int) debug.get("string_hashing_collectors_used") > 0);
            assertEquals("no deferred", 0, debug.get("deferred_ordinals_collectors_used"));
        });
    }

    /**
     * Forced ordinals hint → ordinals collector used.
     */
    public void testDebugOrdinals() throws IOException {
        withMultiSegmentIndex((searcher) -> {
            var debug = collectAndGetDebug(searcher, new CardinalityAggregationBuilder("card").field("app").executionHint("ordinals"));
            assertTrue("ordinals collector used", (int) debug.get("ordinals_collectors_used") > 0);
            assertEquals("no deferred", 0, debug.get("deferred_ordinals_collectors_used"));
            assertEquals("no string hashing", 0, debug.get("string_hashing_collectors_used"));
        });
    }

    /**
     * Multi-bucket, no hint, global ordinals NOT cached → should pick Roaring.
     * Global ordinals unavailable because multi-segment and no prior load.
     */
    public void testAutoSelectRoaringWhenGlobalOrdsNotCached() throws IOException {
        testCache.clear();
        withMultiSegmentIndex((searcher) -> {
            // terms on "group", cardinality on "app" — global ords for "app" not loaded
            var termsAgg = termsWithCard("card", "group", "app", null);
            var debug = collectAndGetDebugForSubAgg(searcher, termsAgg);
            assertEquals(true, debug.get("has_parent_multi_bucket_agg"));
            assertEquals("multi_bucket_roaring", debug.get("collector_selection_reason"));
            assertTrue("roaring used", (int) debug.get("roaring_ordinals_collectors_used") > 0);
        });
    }

    /**
     * Multi-bucket, no hint, global ordinals cached → should pick Deferred.
     * Parent terms on "app" loads global ordinals, cardinality on "app" finds them cached.
     */
    public void testAutoSelectDeferredWhenGlobalOrdsCached() throws IOException {
        testCache.clear();
        withMultiSegmentIndex((searcher) -> {
            var termsAgg = new TermsAggregationBuilder("by_app").field("app")
                .size(10)
                .subAggregation(new CardinalityAggregationBuilder("card").field("app"));
            var debug = collectAndGetDebugForSubAgg(searcher, termsAgg);
            assertEquals(true, debug.get("has_parent_multi_bucket_agg"));
            assertEquals("deferred_global_ordinals", debug.get("collector_selection_reason"));
            assertTrue("deferred used", (int) debug.get("deferred_ordinals_collectors_used") > 0);
        });
    }

    // ======================== Helpers ========================

    @FunctionalInterface
    interface SearcherConsumer {
        void accept(org.apache.lucene.search.IndexSearcher searcher) throws IOException;
    }

    private void withMultiSegmentIndex(SearcherConsumer consumer) throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                // Segment 1: X=[a,b], Y=[b,c]
                addDoc(w, "X", "a");
                addDoc(w, "X", "b");
                addDoc(w, "Y", "b");
                addDoc(w, "Y", "c");
                w.commit();
                // Segment 2: X=[b,c], Y=[c,d]
                addDoc(w, "X", "b");
                addDoc(w, "X", "c");
                addDoc(w, "Y", "c");
                addDoc(w, "Y", "d");
                w.commit();
                // Segment 3: X=[a], Y=[d,e]
                addDoc(w, "X", "a");
                addDoc(w, "Y", "d");
                addDoc(w, "Y", "e");
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertTrue("Need multiple segments", reader.leaves().size() >= 3);
                consumer.accept(newIndexSearcher(reader));
            }
        }
    }

    private void addDoc(IndexWriter w, String group, String app) throws IOException {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("group", new BytesRef(group)));
        doc.add(new SortedSetDocValuesField("app", new BytesRef(app)));
        w.addDocument(doc);
    }

    private TermsAggregationBuilder termsWithCard(String cardName, String termsField, String cardField, String hint) {
        var cardBuilder = new CardinalityAggregationBuilder(cardName).field(cardField);
        if (hint != null) cardBuilder.executionHint(hint);
        return new TermsAggregationBuilder("groups").field(termsField).size(10).subAggregation(cardBuilder);
    }

    private void assertBucketCardinalities(Terms terms, Map<String, Long> expected) {
        assertBucketCardinalities(terms, expected, "");
    }

    private void assertBucketCardinalities(Terms terms, Map<String, Long> expected, String context) {
        for (var bucket : terms.getBuckets()) {
            InternalCardinality card = bucket.getAggregations().get("card");
            Long expectedVal = expected.get(bucket.getKeyAsString());
            assertNotNull(context + " unexpected bucket: " + bucket.getKeyAsString(), expectedVal);
            assertEquals(context + " bucket=" + bucket.getKeyAsString(), expectedVal.longValue(), card.getValue());
        }
    }

    private Map<String, Object> collectAndGetDebug(org.apache.lucene.search.IndexSearcher searcher, CardinalityAggregationBuilder cardAgg)
        throws IOException {
        Aggregator aggregator = createAggregator(cardAgg, searcher, keywordField("group"), keywordField("app"));
        aggregator.preCollection();
        searcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        aggregator.buildTopLevel();
        Map<String, Object> debug = new HashMap<>();
        aggregator.collectDebugInfo(debug::put);
        return debug;
    }

    private Map<String, Object> collectAndGetDebugForSubAgg(
        org.apache.lucene.search.IndexSearcher searcher,
        TermsAggregationBuilder termsAgg
    ) throws IOException {
        Aggregator aggregator = createAggregatorWithCustomizableSearchContext(
            new MatchAllDocsQuery(),
            termsAgg,
            searcher,
            createIndexSettings(),
            new org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer(
                Integer.MAX_VALUE,
                new org.opensearch.core.indices.breaker.NoneCircuitBreakerService().getBreaker(
                    org.opensearch.core.common.breaker.CircuitBreaker.REQUEST
                )
            ),
            (searchContext) -> {
                var ctx = new org.opensearch.search.aggregations.metrics.CardinalityAggregationContext(true, Long.MAX_VALUE);
                org.mockito.Mockito.when(searchContext.cardinalityAggregationContext()).thenReturn(ctx);
            },
            keywordField("group"),
            keywordField("app")
        );
        aggregator.preCollection();
        searcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        aggregator.buildTopLevel();
        Aggregator cardAgg = ((org.opensearch.search.aggregations.AggregatorBase) aggregator).subAggregators()[0];
        Map<String, Object> debug = new HashMap<>();
        cardAgg.collectDebugInfo(debug::put);
        return debug;
    }
}
