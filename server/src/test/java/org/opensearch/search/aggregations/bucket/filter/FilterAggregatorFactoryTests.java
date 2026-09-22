/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filter;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.Bits;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.query.DateRangeIncludingNowQuery;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.opensearch.search.approximate.ApproximateScoreQuery;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;

/**
 * Tests the lifecycle of the weight the {@link FilterAggregatorFactory} creates for its filter query.
 * <p>
 * Under concurrent segment search a shard request runs its segments in slices: every slice builds its own
 * aggregator, but all of them share the aggregator factories of the request and ask for the filter weight while
 * collecting their own segments. The factory therefore hands out its weight to several threads at once.
 */
public class FilterAggregatorFactoryTests extends AggregatorTestCase {

    private static final String FIELD = "timestamp";
    private static final int SLICE_COUNT = 8;

    public void testWeightIsCreatedOnceAndSharedByConcurrentSlices() throws Exception {
        // Documents close enough to now to fall into [now/d-1d] and documents that stay out of it, so that the
        // shared weight is checked against a range that is neither everything nor nothing. Both distances are
        // more than 23 hours away from the boundary the range is evaluated against, so the expected count cannot
        // change while the test runs.
        final long now = System.currentTimeMillis();
        final int withinADay = 5;
        final int olderThanAWeek = 7;

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                // one commit per group: concurrent segment search slices segments
                indexDocuments(indexWriter, now - TimeUnit.HOURS.toMillis(1), withinADay);
                indexDocuments(indexWriter, now - TimeUnit.DAYS.toMillis(6), olderThanAWeek);
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);
                SearchContext searchContext = createSearchContext(
                    indexSearcher,
                    createIndexSettings(),
                    new MatchAllDocsQuery(),
                    new MultiBucketConsumer(DEFAULT_MAX_BUCKETS, new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)),
                    dateField(FIELD, DateFieldMapper.Resolution.MILLISECONDS)
                );
                QueryShardContext queryShardContext = searchContext.getQueryShardContext();
                // The request carries aggregations, which is what vetoes the query approximation of the filter
                // query and leaves its rewrite state unresolved until it is rewritten.
                assertNotNull(searchContext.aggregations());
                FilterAggregatorFactory factory = (FilterAggregatorFactory) new FilterAggregationBuilder(
                    "within_a_day",
                    new RangeQueryBuilder(FIELD).gte("now/d-1d")
                ).build(queryShardContext, null);
                assertNowRangeQueryShape(queryShardContext);

                ExecutorService slices = Executors.newFixedThreadPool(SLICE_COUNT);
                try {
                    final CyclicBarrier startTogether = new CyclicBarrier(SLICE_COUNT);
                    List<Callable<Weight>> sliceTasks = new ArrayList<>(SLICE_COUNT);
                    for (int i = 0; i < SLICE_COUNT; i++) {
                        sliceTasks.add(() -> {
                            startTogether.await();
                            return factory.getWeight();
                        });
                    }
                    List<Weight> weights = new ArrayList<>(SLICE_COUNT);
                    for (Future<Weight> future : slices.invokeAll(sliceTasks)) {
                        weights.add(future.get());
                    }
                    for (int i = 1; i < weights.size(); i++) {
                        assertSame("the filter weight must be created once and shared by every slice", weights.get(0), weights.get(i));
                    }
                    assertEquals(withinADay, countMatchingDocs(weights.get(0), indexReader));
                } finally {
                    slices.shutdown();
                }
            }
        }
    }

    /**
     * A slice that finished rewriting the shared filter query must keep working when another slice republishes
     * the rewrite state of that same query instance. {@link ContextIndexSearcher#rewrite} publishes the query
     * resolved for the search context before it rewrites it, and {@link Query#createWeight} reads whatever the
     * last rewrite published, so with aggregations in the request a slice can publish the un-rewritten
     * {@code now} based query between the rewrite of another slice and its weight creation.
     */
    public void testWeightCreationIsNotBrokenByTheRewriteStateOfAnotherSlice() throws Exception {
        final long now = System.currentTimeMillis();
        final int withinADay = 5;
        final int olderThanAWeek = 7;

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                indexDocuments(indexWriter, now - TimeUnit.HOURS.toMillis(1), withinADay);
                indexDocuments(indexWriter, now - TimeUnit.DAYS.toMillis(6), olderThanAWeek);
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);
                SearchContext searchContext = createSearchContext(
                    indexSearcher,
                    createIndexSettings(),
                    new MatchAllDocsQuery(),
                    new MultiBucketConsumer(DEFAULT_MAX_BUCKETS, new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)),
                    dateField(FIELD, DateFieldMapper.Resolution.MILLISECONDS)
                );
                QueryShardContext queryShardContext = searchContext.getQueryShardContext();
                assertNotNull(searchContext.aggregations());
                Query filter = new RangeQueryBuilder(FIELD).gte("now/d-1d").toQuery(queryShardContext);
                assertTrue(filter instanceof ApproximateScoreQuery);
                assertTrue(((ApproximateScoreQuery) filter).getOriginalQuery() instanceof DateRangeIncludingNowQuery);

                ContextIndexSearcher searcher = searchContext.searcher();
                ApproximateScoreQuery query = (ApproximateScoreQuery) searcher.rewrite(filter);
                // the query resolved by another slice for the same search context, published while this slice
                // creates its weight
                query.setContext(searchContext);

                Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
                assertEquals(withinADay, countMatchingDocs(weight, indexReader));
            }
        }
    }

    private static void assertNowRangeQueryShape(QueryShardContext queryShardContext) throws IOException {
        Query filter = new RangeQueryBuilder(FIELD).gte("now/d-1d").toQuery(queryShardContext);
        assertTrue("expected an approximate score query, got " + filter, filter instanceof ApproximateScoreQuery);
        assertTrue(
            "expected the now based range query to be marked as such, got " + ((ApproximateScoreQuery) filter).getOriginalQuery(),
            ((ApproximateScoreQuery) filter).getOriginalQuery() instanceof DateRangeIncludingNowQuery
        );
    }

    private void indexDocuments(RandomIndexWriter indexWriter, long timestamp, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            Document document = new Document();
            document.add(new LongPoint(FIELD, timestamp));
            document.add(new SortedNumericDocValuesField(FIELD, timestamp));
            indexWriter.addDocument(document);
        }
        indexWriter.commit();
    }

    private static long countMatchingDocs(Weight weight, IndexReader indexReader) throws IOException {
        long count = 0;
        for (LeafReaderContext context : indexReader.leaves()) {
            final Bits bits = Lucene.asSequentialAccessBits(context.reader().maxDoc(), weight.scorerSupplier(context));
            for (int doc = 0; doc < context.reader().maxDoc(); doc++) {
                if (bits.get(doc)) {
                    count++;
                }
            }
        }
        return count;
    }
}
