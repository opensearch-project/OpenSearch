/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

public class AggregationCancellationTests extends AggregatorTestCase {

    public void testCancellationDuringReduce() throws IOException {
        // Create test index and data
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

            // Index enough documents to make reduce phase substantial
            int numDocs = 1000;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new NumericDocValuesField("value", i));
                document.add(new SortedSetDocValuesField("keyword", new BytesRef("value" + (i % 100))));
                indexWriter.addDocument(document);
            }
            indexWriter.close();

            try (IndexReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = newSearcher(reader);

                // Create a complex aggregation with nested terms to make reduce phase longer
                TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("keywords").field("keyword")
                    .size(100)
                    .subAggregation(
                        new TermsAggregationBuilder("nested_keywords").field("keyword")
                            .size(100)
                            .subAggregation(new StatsAggregationBuilder("stats").field("value"))
                    );

                BooleanSupplier cancellationChecker = () -> true;

                // Execute aggregation
                List<InternalAggregation> aggs = new ArrayList<>();
                MultiBucketConsumerService.MultiBucketConsumer bucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
                    100000,
                    new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                );

                Query query = new MatchAllDocsQuery();

                Aggregator agg = createAggregator(query, aggregationBuilder, searcher, bucketConsumer);

                // Collect
                agg.preCollection();
                searcher.search(query, agg);
                agg.postCollection();
                aggs.add(agg.buildTopLevel());

                // Create reduce context with cancellation checker
                InternalAggregation.ReduceContext reduceContext = InternalAggregation.ReduceContext.forFinalReduction(
                    agg.context().bigArrays(),
                    null,
                    bucketConsumer,
                    aggregationBuilder.buildPipelineTree()
                );

                reduceContext.setIsTaskCancelled(cancellationChecker);

                // Perform reduce - should throw TaskCancelledException
                expectThrows(
                    TaskCancelledException.class,
                    () -> { InternalAggregations.reduce(List.of(InternalAggregations.from(aggs)), reduceContext); }
                );
            }
        }
    }
}
