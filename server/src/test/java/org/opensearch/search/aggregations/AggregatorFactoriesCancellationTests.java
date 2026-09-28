/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link AggregatorFactories#createTopLevelAggregators} checks for task cancellation
 * between aggregator factory creates.
 */
public class AggregatorFactoriesCancellationTests extends AggregatorTestCase {

    public void testCreateTopLevelAggregatorsThrowsWhenCancelled() throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("field", new BytesRef("value" + i)));
                indexWriter.addDocument(doc);
            }
            indexWriter.close();

            try (IndexReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = newIndexSearcher(reader);

                MultiBucketConsumerService.MultiBucketConsumer bucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
                    DEFAULT_MAX_BUCKETS,
                    new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                );
                SearchContext searchContext = createSearchContext(
                    searcher,
                    createIndexSettings(),
                    new MatchAllDocsQuery(),
                    bucketConsumer,
                    keywordField("field")
                );

                // Build AggregatorFactories from a builder with an actual aggregation
                TermsAggregationBuilder aggBuilder = new TermsAggregationBuilder("terms").field("field").size(10);
                QueryShardContext qsc = searchContext.getQueryShardContext();
                AggregatorFactories.Builder factoriesBuilder = new AggregatorFactories.Builder().addAggregator(aggBuilder);
                AggregatorFactories factories = factoriesBuilder.build(qsc, null);

                // Verify it works when not cancelled
                when(searchContext.isCancelled()).thenReturn(false);
                List<Aggregator> aggregators = factories.createTopLevelAggregators(searchContext);
                assertFalse(aggregators.isEmpty());

                // Now mark as cancelled — should throw TaskCancelledException
                when(searchContext.isCancelled()).thenReturn(true);
                expectThrows(TaskCancelledException.class, () -> factories.createTopLevelAggregators(searchContext));
            }
        }
    }
}
