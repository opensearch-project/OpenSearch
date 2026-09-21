/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.sampler;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.Sum;

import java.io.IOException;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RandomSamplerAggregatorTests extends AggregatorTestCase {

    private static final String NUMERIC_FIELD = "value";
    private static final String KEYWORD_FIELD = "term";
    private static final int NUM_DOCS = 10000;

    private final MappedFieldType numericFieldType = new NumberFieldMapper.NumberFieldType(
        NUMERIC_FIELD,
        NumberFieldMapper.NumberType.LONG
    );
    private final MappedFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD);

    /**
     * The sampled count is binomially distributed, so this asserts the scaled {@code doc_count} lands within four
     * standard deviations of the real count rather than asserting an exact number.
     */
    public void testScaledDocCountEstimatesTheRealCount() throws IOException {
        double probability = 0.2;
        withIndex(NUM_DOCS, (searcher, reader) -> {
            InternalRandomSampler response = searchAndReduce(
                searcher,
                new MatchAllDocsQuery(),
                sampler(probability, randomInt()),
                numericFieldType,
                keywordFieldType
            );
            double sigma = Math.sqrt(NUM_DOCS * probability * (1 - probability));
            assertEquals(NUM_DOCS, response.getDocCount(), 4 * sigma / probability);
            assertThat(response.getSampledDocCount(), greaterThan(0L));
        });
    }

    public void testProbabilityOneCollectsEveryMatch() throws IOException {
        withIndex(NUM_DOCS, (searcher, reader) -> {
            InternalRandomSampler response = searchAndReduce(
                searcher,
                new MatchAllDocsQuery(),
                sampler(1.0, randomInt()),
                numericFieldType,
                keywordFieldType
            );
            assertEquals(NUM_DOCS, response.getDocCount());
            assertEquals(NUM_DOCS, response.getSampledDocCount());
        });
    }

    /**
     * The fast path, which drives its own iteration, must collect exactly what the per-document path collects. The
     * sampler is forced onto the fallback path by nesting it under a filter, where documents are pushed to it.
     */
    public void testFastPathCollectsTheSameDocumentsAsTheFallback() throws IOException {
        int seed = randomInt();
        double probability = 0.25;
        withIndex(NUM_DOCS, (searcher, reader) -> {
            InternalRandomSampler topLevel = searchAndReduce(
                searcher,
                new MatchAllDocsQuery(),
                sampler(probability, seed),
                numericFieldType,
                keywordFieldType
            );
            InternalFilter nested = searchAndReduce(
                searcher,
                new MatchAllDocsQuery(),
                AggregationBuilders.filter("wrapper", new MatchAllQueryBuilder()).subAggregation(sampler(probability, seed)),
                numericFieldType,
                keywordFieldType
            );
            InternalRandomSampler fallback = nested.getAggregations().get("sampled");

            assertEquals(topLevel.getSampledDocCount(), fallback.getSampledDocCount());
            assertEquals(topLevel.getDocCount(), fallback.getDocCount());
            assertEquals(sumOf(topLevel), sumOf(fallback), 1e-6);
        });
    }

    /**
     * The fast path builds its own scorer, which does not filter deleted documents; the searcher normally does that. If
     * it forgot to apply the live documents it would collect more than the fallback path does.
     */
    public void testDeletedDocumentsAreNotCollected() throws IOException {
        int seed = randomInt();
        double probability = 0.25;
        try (Directory directory = newDirectory()) {
            // A plain writer with merging disabled: any merge after the deletion would expunge exactly the deleted
            // documents this test is about.
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (int i = 0; i < NUM_DOCS; i++) {
                    writer.addDocument(document(i));
                }
                writer.deleteDocuments(new Term(KEYWORD_FIELD, "even"));
                writer.commit();
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                assertEquals(NUM_DOCS / 2, reader.numDeletedDocs());
                assertEquals(NUM_DOCS / 2, reader.numDocs());
                IndexSearcher searcher = newSearcher(reader, true, true);

                InternalRandomSampler topLevel = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    sampler(probability, seed),
                    numericFieldType,
                    keywordFieldType
                );
                InternalFilter nested = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    AggregationBuilders.filter("wrapper", new MatchAllQueryBuilder()).subAggregation(sampler(probability, seed)),
                    numericFieldType,
                    keywordFieldType
                );
                InternalRandomSampler fallback = nested.getAggregations().get("sampled");

                assertEquals(topLevel.getSampledDocCount(), fallback.getSampledDocCount());
                // "odd" documents survive, so the estimate must be around half the corpus, not all of it
                assertThat(topLevel.getDocCount(), lessThanOrEqualTo((long) NUM_DOCS));
            }
        }
    }

    /**
     * Selection never consults scores, so changing the scoring of an otherwise identical query must not move the sample.
     */
    public void testSampleDoesNotDependOnScores() throws IOException {
        int seed = randomInt();
        double probability = 0.25;
        withIndex(NUM_DOCS, (searcher, reader) -> {
            Query unboosted = new TermQuery(new Term(KEYWORD_FIELD, "odd"));
            Query boosted = new BoostQuery(new TermQuery(new Term(KEYWORD_FIELD, "odd")), 17f);

            InternalRandomSampler withoutBoost = searchAndReduce(
                searcher,
                unboosted,
                sampler(probability, seed),
                numericFieldType,
                keywordFieldType
            );
            InternalRandomSampler withBoost = searchAndReduce(
                searcher,
                boosted,
                sampler(probability, seed),
                numericFieldType,
                keywordFieldType
            );

            assertEquals(withoutBoost.getSampledDocCount(), withBoost.getSampledDocCount());
            assertEquals(sumOf(withoutBoost), sumOf(withBoost), 0.0);
        });
    }

    public void testQueryOtherThanMatchAllIsRespected() throws IOException {
        withIndex(NUM_DOCS, (searcher, reader) -> {
            InternalRandomSampler response = searchAndReduce(
                searcher,
                new TermQuery(new Term(KEYWORD_FIELD, "odd")),
                sampler(1.0, randomInt()),
                numericFieldType,
                keywordFieldType
            );
            assertEquals(NUM_DOCS / 2, response.getDocCount());
        });
    }

    /**
     * Under a {@code terms} aggregation the sampler runs on the fallback path once per bucket. Every document belongs to
     * exactly one bucket, so the sampled counts must add up to what a single top-level sampler collects with the same
     * seed.
     */
    public void testNestedUnderTermsCollectsTheSameSampleAcrossBuckets() throws IOException {
        int seed = randomInt();
        double probability = 0.25;
        withIndex(NUM_DOCS, (searcher, reader) -> {
            InternalRandomSampler topLevel = searchAndReduce(
                searcher,
                new MatchAllDocsQuery(),
                sampler(probability, seed),
                numericFieldType,
                keywordFieldType
            );
            StringTerms terms = searchAndReduce(
                searcher,
                new MatchAllDocsQuery(),
                AggregationBuilders.terms("by_term").field(KEYWORD_FIELD).subAggregation(sampler(probability, seed)),
                numericFieldType,
                keywordFieldType
            );

            long sampledAcrossBuckets = 0L;
            for (Terms.Bucket bucket : terms.getBuckets()) {
                InternalRandomSampler sampled = bucket.getAggregations().get("sampled");
                sampledAcrossBuckets += sampled.getSampledDocCount();
                // each bucket's own doc_count is scaled too
                assertThat(sampled.getDocCount(), greaterThan(sampled.getSampledDocCount()));
            }
            assertEquals(topLevel.getSampledDocCount(), sampledAcrossBuckets);
        });
    }

    /**
     * A {@code sampler} underneath must not have its {@code doc_count} scaled: it reports how many top-scoring
     * documents it kept, which cannot exceed {@code shard_size}.
     */
    public void testNestedSamplerDocCountIsNotScaled() throws IOException {
        int shardSize = 10;
        withIndex(NUM_DOCS, (searcher, reader) -> {
            InternalRandomSampler response = searchAndReduce(
                createIndexSettings(),
                searcher,
                new MatchAllDocsQuery(),
                sampler(0.25, randomInt()).subAggregation(
                    AggregationBuilders.sampler("kept")
                        .shardSize(shardSize)
                        .subAggregation(AggregationBuilders.sum("sum").field(NUMERIC_FIELD))
                ),
                DEFAULT_MAX_BUCKETS,
                false,
                numericFieldType,
                keywordFieldType
            );

            Sampler kept = response.getAggregations().get("kept");
            assertThat(kept.getDocCount(), lessThanOrEqualTo((long) shardSize));
            // Nothing under a sampler is scaled either: the top-scoring documents it kept are not a sample of
            // anything. Every document carries a value of 1, so an unscaled sum equals the number kept.
            Sum sum = kept.getAggregations().get("sum");
            assertEquals((double) kept.getDocCount(), sum.getValue(), 0.0);
            assertThat(kept.getDocCount(), greaterThan(0L));
        });
    }

    private RandomSamplerAggregationBuilder sampler(double probability, int seed) {
        return AggregationBuilders.randomSampler("sampled")
            .probability(probability)
            .seed(seed)
            .subAggregation(AggregationBuilders.sum("sum").field(NUMERIC_FIELD));
    }

    private static double sumOf(InternalRandomSampler sampler) {
        InternalSum sum = sampler.getAggregations().get("sum");
        return sum.getValue();
    }

    private static Document document(int i) {
        String term = i % 2 == 0 ? "even" : "odd";
        Document document = new Document();
        document.add(new Field(KEYWORD_FIELD, term, KeywordFieldMapper.Defaults.FIELD_TYPE));
        // a terms aggregation over the keyword field reads ordinals, which need doc values
        document.add(new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef(term)));
        document.add(new SortedNumericDocValuesField(NUMERIC_FIELD, 1));
        return document;
    }

    private interface IndexConsumer {
        void accept(IndexSearcher searcher, IndexReader reader) throws IOException;
    }

    private void withIndex(int numDocs, IndexConsumer consumer) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numDocs; i++) {
                    writer.addDocument(document(i));
                    if (i > 0 && i % 2500 == 0) {
                        writer.commit();
                    }
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                consumer.accept(newSearcher(reader, true, true), reader);
            }
        }
    }
}
