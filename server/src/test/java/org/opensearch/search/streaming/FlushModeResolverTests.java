/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.aggregations.MultiBucketCollector;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.terms.StreamStringTermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;

public class FlushModeResolverTests extends AggregatorTestCase {

    private static final int SMALL_BUCKET_LIMIT = 50;
    private static final double HIGH_CARDINALITY_RATIO = 0.1;
    private static final int MIN_BUCKET_THRESHOLD = 5;

    @FunctionalInterface
    private interface IOConsumer<T> {
        void accept(T t) throws IOException;
    }

    private MultiBucketConsumerService.MultiBucketConsumer createBucketConsumer() {
        return new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
    }

    private void addDocuments(IndexWriter writer, int docCount, int categoryCount) throws IOException {
        for (int i = 0; i < docCount; i++) {
            Document document = new Document();
            document.add(new SortedSetDocValuesField("category", new BytesRef("category_" + (i % categoryCount))));
            writer.addDocument(document);
        }
    }

    private void addDocumentsWithSubcategory(IndexWriter writer, int docCount, int categoryCount, int subcategoryCount) throws IOException {
        for (int i = 0; i < docCount; i++) {
            Document document = new Document();
            document.add(new SortedSetDocValuesField("category", new BytesRef("category_" + (i % categoryCount))));
            document.add(new SortedSetDocValuesField("subcategory", new BytesRef("subcategory_" + (i % subcategoryCount))));
            writer.addDocument(document);
        }
    }

    private void withIndex(IOConsumer<IndexWriter> dataSetup, IOConsumer<IndexSearcher> testLogic) throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                dataSetup.accept(writer);

                try (IndexReader reader = maybeWrapReaderEs(DirectoryReader.open(writer))) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    assertEquals("strictly single segment", 1, searcher.getIndexReader().leaves().size());
                    testLogic.accept(searcher);
                }
            }
        }
    }

    private StreamStringTermsAggregator createTermsAggregator(
        String name,
        String field,
        IndexSearcher searcher,
        MappedFieldType... fieldTypes
    ) throws IOException {
        TermsAggregationBuilder builder = new TermsAggregationBuilder(name).field(field);
        return createStreamAggregator(null, builder, searcher, createIndexSettings(), createBucketConsumer(), fieldTypes);
    }

    public void testResolveWithStreamableAggregator() throws IOException {
        withIndex(writer -> addDocuments(writer, 100, 10), searcher -> {
            MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");
            StreamStringTermsAggregator aggregator = createTermsAggregator("categories", "category", searcher, fieldType);

            assertTrue(aggregator instanceof Streamable);

            FlushMode result = FlushModeResolver.resolve(
                aggregator,
                FlushMode.PER_SHARD,
                SMALL_BUCKET_LIMIT,
                HIGH_CARDINALITY_RATIO,
                MIN_BUCKET_THRESHOLD
            );

            assertEquals(FlushMode.PER_SEGMENT, result);
        });
    }

    public void testResolveWithNonStreamableAggregator() throws IOException {
        withIndex(writer -> addDocuments(writer, 1, 1), searcher -> {
            MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");
            TopHitsAggregationBuilder builder = new TopHitsAggregationBuilder("top_docs").size(3);
            var aggregator = createAggregator(builder, searcher, fieldType);

            assertFalse(aggregator instanceof Streamable);

            FlushMode result = FlushModeResolver.resolve(
                aggregator,
                FlushMode.PER_SHARD,
                SMALL_BUCKET_LIMIT,
                HIGH_CARDINALITY_RATIO,
                MIN_BUCKET_THRESHOLD
            );

            assertEquals(FlushMode.PER_SHARD, result);
        });
    }

    public void testResolveWithHighCardinalityExceedsLimit() throws IOException {
        withIndex(writer -> addDocuments(writer, 100, 100), searcher -> {
            MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");
            StreamStringTermsAggregator aggregator = createTermsAggregator("categories", "category", searcher, fieldType);

            FlushMode result = FlushModeResolver.resolve(
                aggregator,
                FlushMode.PER_SHARD,
                SMALL_BUCKET_LIMIT,
                HIGH_CARDINALITY_RATIO,
                MIN_BUCKET_THRESHOLD
            );

            assertEquals(FlushMode.PER_SHARD, result);
        });
    }

    public void testResolveWithLowCardinalityRatio() throws IOException {
        withIndex(writer -> addDocuments(writer, 1000, 5), searcher -> {
            MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");
            StreamStringTermsAggregator aggregator = createTermsAggregator("categories", "category", searcher, fieldType);

            FlushMode result = FlushModeResolver.resolve(
                aggregator,
                FlushMode.PER_SHARD,
                SMALL_BUCKET_LIMIT,
                HIGH_CARDINALITY_RATIO,
                MIN_BUCKET_THRESHOLD
            );

            assertEquals(FlushMode.PER_SHARD, result);
        });
    }

    public void testResolveWithBelowMinBucketCount() throws IOException {
        withIndex(writer -> addDocuments(writer, 10, 2), searcher -> {
            MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");
            StreamStringTermsAggregator aggregator = createTermsAggregator("categories", "category", searcher, fieldType);

            FlushMode result = FlushModeResolver.resolve(aggregator, FlushMode.PER_SHARD, SMALL_BUCKET_LIMIT, 0.01, MIN_BUCKET_THRESHOLD);

            assertEquals(FlushMode.PER_SHARD, result);
        });
    }

    public void testResolveWithMixedAggregators() throws IOException {
        withIndex(writer -> addDocuments(writer, 50, 10), searcher -> {
            MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");

            StreamStringTermsAggregator streamableAgg = createTermsAggregator("categories", "category", searcher, fieldType);

            TopHitsAggregationBuilder topHitsBuilder = new TopHitsAggregationBuilder("top_docs").size(3);
            var nonStreamableAgg = createAggregator(topHitsBuilder, searcher, fieldType);

            List<BucketCollector> aggregators = new ArrayList<>();
            aggregators.add(streamableAgg);
            aggregators.add(nonStreamableAgg);

            BucketCollector collector = MultiBucketCollector.wrap(aggregators);

            FlushMode result = FlushModeResolver.resolve(
                collector,
                FlushMode.PER_SHARD,
                SMALL_BUCKET_LIMIT,
                HIGH_CARDINALITY_RATIO,
                MIN_BUCKET_THRESHOLD
            );

            assertEquals(FlushMode.PER_SHARD, result);
        });
    }

    public void testResolveWithNestedStreamableAggregation() throws IOException {
        withIndex(writer -> addDocumentsWithSubcategory(writer, 100, 8, 8), searcher -> {
            MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType("category");
            MappedFieldType subcategoryFieldType = new KeywordFieldMapper.KeywordFieldType("subcategory");

            TermsAggregationBuilder subAggBuilder = new TermsAggregationBuilder("sub_categories").field("subcategory");
            TermsAggregationBuilder mainAggBuilder = new TermsAggregationBuilder("categories").field("category")
                .subAggregation(subAggBuilder);

            StreamStringTermsAggregator aggregator = createStreamAggregator(
                null,
                mainAggBuilder,
                searcher,
                createIndexSettings(),
                createBucketConsumer(),
                categoryFieldType,
                subcategoryFieldType
            );

            FlushMode result = FlushModeResolver.resolve(
                aggregator,
                FlushMode.PER_SHARD,
                65, // 8*8=64 buckets, so 65 allows streaming
                0.05,
                MIN_BUCKET_THRESHOLD
            );

            assertEquals(FlushMode.PER_SEGMENT, result);
        });
    }

    public void testResolveWithNestedMixedAggregation() throws IOException {
        withIndex(writer -> addDocuments(writer, 50, 5), searcher -> {
            MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");

            TopHitsAggregationBuilder topHitsBuilder = new TopHitsAggregationBuilder("top_docs").size(3);
            TermsAggregationBuilder mainAggBuilder = new TermsAggregationBuilder("categories").field("category")
                .subAggregation(topHitsBuilder);

            StreamStringTermsAggregator aggregator = createStreamAggregator(
                null,
                mainAggBuilder,
                searcher,
                createIndexSettings(),
                createBucketConsumer(),
                fieldType
            );

            FlushMode result = FlushModeResolver.resolve(aggregator, FlushMode.PER_SHARD, SMALL_BUCKET_LIMIT, 0.05, 3);

            assertEquals(FlushMode.PER_SHARD, result);
        });
    }

    public void testSettingsDefaults() {
        assertEquals(100_000L, FlushModeResolver.STREAMING_MAX_ESTIMATED_BUCKET_COUNT.getDefault(Settings.EMPTY).longValue());
        assertEquals(0.01, FlushModeResolver.STREAMING_MIN_CARDINALITY_RATIO.getDefault(Settings.EMPTY).doubleValue(), 0.001);
        assertEquals(1000L, FlushModeResolver.STREAMING_MIN_ESTIMATED_BUCKET_COUNT.getDefault(Settings.EMPTY).longValue());
    }
}
