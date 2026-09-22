/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.Bits;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.query.DateRangeIncludingNowQuery;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.apache.lucene.document.LongPoint.pack;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApproximateScoreQueryTests extends OpenSearchTestCase {

    public void testEqualsAndHashCode() {
        Query original = LongPoint.newRangeQuery("ts", 0L, 10L);
        ApproximateQuery approximation = approximateRange("ts", 0L, 10L);
        ApproximateScoreQuery query = new ApproximateScoreQuery(original, approximation);

        ApproximateScoreQuery same = new ApproximateScoreQuery(LongPoint.newRangeQuery("ts", 0L, 10L), approximateRange("ts", 0L, 10L));
        assertEquals(query, same);
        assertEquals(query.hashCode(), same.hashCode());

        // resolvedQuery is mutable rewrite state and must not change identity
        same.resolvedQuery = approximation;
        assertEquals(query, same);
        assertEquals(query.hashCode(), same.hashCode());

        ApproximateScoreQuery differentOriginal = new ApproximateScoreQuery(LongPoint.newRangeQuery("ts", 0L, 20L), approximation);
        assertFalse(query.equals(differentOriginal));

        ApproximateScoreQuery differentApproximation = new ApproximateScoreQuery(original, approximateRange("ts", 0L, 20L));
        assertFalse(query.equals(differentApproximation));

        assertFalse(query.equals(null));
        assertFalse(query.equals(original));
    }

    public void testApproximationScoreSupplier() throws IOException {
        long l = Long.MIN_VALUE;
        long u = Long.MAX_VALUE;
        Query originalQuery = new PointRangeQuery(
            "test-index",
            pack(new long[] { l }).bytes,
            pack(new long[] { u }).bytes,
            new long[] { l }.length
        ) {
            protected String toString(int dimension, byte[] value) {
                return Long.toString(LongPoint.decodeDimension(value, 0));
            }
        };

        ApproximateQuery approximateQuery = new ApproximatePointRangeQuery(
            "test-index",
            pack(new long[] { l }).bytes,
            pack(new long[] { u }).bytes,
            new long[] { l }.length,
            ApproximatePointRangeQuery.LONG_FORMAT
        );

        ApproximateScoreQuery query = new ApproximateScoreQuery(originalQuery, approximateQuery);
        query.resolvedQuery = approximateQuery;

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                Document document = new Document();
                document.add(new LongPoint("testPoint", Long.MIN_VALUE));
                iw.addDocument(document);
                iw.flush();
                try (IndexReader reader = iw.getReader()) {
                    try {
                        IndexSearcher searcher = new IndexSearcher(reader);
                        searcher.search(query, 10);
                        Weight weight = query.rewrite(searcher).createWeight(searcher, ScoreMode.TOP_SCORES, 1.0F);
                        Scorer scorer = weight.scorer(reader.leaves().get(0));
                        assertEquals(
                            scorer,
                            originalQuery.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0F).scorer(searcher.getLeafContexts().get(0))
                        );
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }
            }
        }
    }

    /**
     * The rewrite state of the query is published by {@link ApproximateScoreQuery#setContext} and updated by
     * {@link ApproximateScoreQuery#rewrite}, both of which run on whichever thread happens to touch the query.
     * Under concurrent segment search several slices of one shard request rewrite the same shared query instance,
     * so every state those two calls can publish has to be able to create a weight, and all of them have to
     * resolve to the same documents.
     */
    public void testCreateWeightForTheRewriteStatesPublishedByConcurrentSlices() throws IOException {
        final long lower = 10L;
        final long upper = 20L;
        final int expected = (int) (upper - lower + 1);
        final Query original = new IndexOrDocValuesQuery(
            LongPoint.newRangeQuery("ts", lower, upper),
            SortedNumericDocValuesField.newSlowRangeQuery("ts", lower, upper)
        );
        // the original query the date field mapper produces for a range that uses now
        final Query nowBasedOriginal = new DateRangeIncludingNowQuery(original);
        final ApproximateQuery approximation = approximateRange("ts", lower, upper);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                for (long value = lower - 5L; value <= upper + 5L; value++) {
                    Document document = new Document();
                    document.add(new LongPoint("ts", value));
                    document.add(new SortedNumericDocValuesField("ts", value));
                    iw.addDocument(document);
                }
                try (IndexReader reader = iw.getReader()) {
                    final IndexSearcher searcher = new IndexSearcher(reader);
                    // aggregations in the request veto the approximation, so the query resolves to its original query
                    final SearchContext contextWithAggregations = mock(SearchContext.class);
                    when(contextWithAggregations.aggregations()).thenReturn(
                        new SearchContextAggregations(
                            AggregatorFactories.EMPTY,
                            new MultiBucketConsumer(DEFAULT_MAX_BUCKETS, new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST))
                        )
                    );

                    // no rewrite has published any state yet
                    ApproximateScoreQuery untouched = new ApproximateScoreQuery(nowBasedOriginal, approximation);
                    assertEquals(expected, countMatches(untouched.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f), reader));

                    // setContext published the original query, before the rewrite unwrapped it
                    ApproximateScoreQuery published = new ApproximateScoreQuery(nowBasedOriginal, approximation);
                    published.setContext(contextWithAggregations);
                    assertSame(nowBasedOriginal, published.resolvedQuery);
                    assertEquals(expected, countMatches(published.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f), reader));

                    // the rewrite unwrapped the now marker and published the unwrapped query
                    ApproximateScoreQuery unwrapped = new ApproximateScoreQuery(nowBasedOriginal, approximation);
                    unwrapped.setContext(contextWithAggregations);
                    assertEquals(
                        expected,
                        countMatches(
                            ((ApproximateScoreQuery) unwrapped.rewrite(searcher)).createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f),
                            reader
                        )
                    );
                }
            }
        }
    }

    private static long countMatches(Weight weight, IndexReader reader) throws IOException {
        long count = 0;
        for (LeafReaderContext context : reader.leaves()) {
            final Bits bits = Lucene.asSequentialAccessBits(context.reader().maxDoc(), weight.scorerSupplier(context));
            for (int doc = 0; doc < context.reader().maxDoc(); doc++) {
                if (bits.get(doc)) {
                    count++;
                }
            }
        }
        return count;
    }

    private static ApproximateQuery approximateRange(String field, long lower, long upper) {
        return new ApproximatePointRangeQuery(
            field,
            pack(new long[] { lower }).bytes,
            pack(new long[] { upper }).bytes,
            1,
            ApproximatePointRangeQuery.LONG_FORMAT
        );
    }
}
