/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.MultiBucketConsumerService;

import java.util.List;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class StreamingStringTermsAggregatorTests extends AggregatorTestCase {
    public void testBuildAggregationsBatchDirectBucketCreation() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("apple")));
                document.add(new SortedSetDocValuesField("field", new BytesRef("banana")));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("apple")));
                document.add(new SortedSetDocValuesField("field", new BytesRef("cherry")));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("banana")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field")
                        .order(BucketOrder.key(true));

                    StreamingStringTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(3));

                    List<StringTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.get(0).getKeyAsString(), equalTo("apple"));
                    assertThat(buckets.get(0).getDocCount(), equalTo(2L));
                    assertThat(buckets.get(1).getKeyAsString(), equalTo("banana"));
                    assertThat(buckets.get(1).getDocCount(), equalTo(2L));
                    assertThat(buckets.get(2).getKeyAsString(), equalTo("cherry"));
                    assertThat(buckets.get(2).getDocCount(), equalTo(1L));

                    for (StringTerms.Bucket bucket : buckets) {
                        assertThat(bucket, instanceOf(StringTerms.Bucket.class));
                        assertThat(bucket.getKey(), instanceOf(String.class));
                        assertThat(bucket.getKeyAsString(), notNullValue());
                    }
                }
            }
        }
    }

    public void testBuildAggregationsBatchEmptyResults() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field");

                    StreamingStringTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(0));
                }
            }
        }
    }
}
