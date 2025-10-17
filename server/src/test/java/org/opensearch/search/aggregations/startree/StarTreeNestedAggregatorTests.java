/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Rounding;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite103.Composite103Codec;
import org.opensearch.index.codec.composite912.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.OrdinalDimension;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregatorTestCase;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.opensearch.search.aggregations.AggregationBuilders.count;
import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.min;
import static org.opensearch.search.aggregations.AggregationBuilders.range;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;

public class StarTreeNestedAggregatorTests extends DateHistogramAggregatorTestCase {
    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final MappedFieldType TIMESTAMP_FIELD_TYPE = new DateFieldMapper.DateFieldType(TIMESTAMP_FIELD);

    private static final String KEYWORD_FIELD = "clientip";
    MappedFieldType KEYWORD_FIELD_TYPE = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD);

    final static String STATUS = "status";
    final static String SIZE = "size";
    private static final MappedFieldType STATUS_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(
        STATUS,
        NumberFieldMapper.NumberType.LONG
    );
    private static final MappedFieldType SIZE_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(SIZE, NumberFieldMapper.NumberType.LONG);

    protected Codec getCodec() {
        final Logger testLogger = LogManager.getLogger(MetricAggregatorTests.class);
        MapperService mapperService;
        try {
            mapperService = StarTreeDocValuesFormatTests.createMapperService(NumericTermsAggregatorTests.getExpandedMapping(1, false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite103Codec(Lucene103Codec.Mode.BEST_SPEED, mapperService, testLogger);
    }

    public void testStarTreeNestedAggregations() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(getCodec());
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);

        Random random = RandomizedTest.getRandom();
        int totalDocs = 100;

        long val;
        long date;
        List<Document> docs = new ArrayList<>();
        // Index 100 random documents
        for (int i = 0; i < totalDocs; i++) {
            Document doc = new Document();
            if (randomBoolean()) {
                val = random.nextInt(100); // Random int between 0 and 99 for status
                doc.add(new SortedNumericDocValuesField(STATUS, val));
            }
            if (randomBoolean()) {
                val = random.nextInt(100);
                doc.add(new SortedNumericDocValuesField(SIZE, val));
            }
            if (randomBoolean()) {
                val = random.nextInt(10); // Random strings for int between 0 and 9 for keyword terms
                doc.add(new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef(String.valueOf(val))));
            }
            if (randomBoolean()) {
                date = random.nextInt(180) * 24 * 60 * 60 * 1000L; // Random date within 180 days
                doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, date));
                doc.add(new LongPoint(TIMESTAMP_FIELD, date));
            }

            iw.addDocument(doc);
            docs.add(doc);
        }

        if (randomBoolean()) {
            iw.forceMerge(1);
        }
        iw.close();
        DirectoryReader ir = DirectoryReader.open(directory);
        LeafReaderContext context = ir.leaves().get(0);

        SegmentReader reader = Lucene.segmentReader(context.reader());
        IndexSearcher indexSearcher = newSearcher(wrapInMockESDirectoryReader(ir), false, false);
        CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();

        List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();
        CompositeIndexFieldInfo starTree = compositeIndexFields.get(0);

        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions = new LinkedHashMap<>();
        supportedDimensions.put(new NumericDimension(STATUS), STATUS_FIELD_TYPE);
        supportedDimensions.put(new NumericDimension(SIZE), SIZE_FIELD_TYPE);
        supportedDimensions.put(
            new DateDimension(
                TIMESTAMP_FIELD,
                List.of(
                    new DateTimeUnitAdapter(Rounding.DateTimeUnit.MONTH_OF_YEAR),
                    new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH)
                ),
                DateFieldMapper.Resolution.MILLISECONDS
            ),
            new DateFieldMapper.DateFieldType(TIMESTAMP_FIELD)
        );
        supportedDimensions.put(new OrdinalDimension(KEYWORD_FIELD), KEYWORD_FIELD_TYPE);

        Query query = new MatchAllDocsQuery();
        QueryBuilder queryBuilder = null;

        ValuesSourceAggregationBuilder[] aggBuilders = {
            sum("_sum").field(STATUS),
            max("_max").field(STATUS),
            min("_min").field(STATUS),
            count("_count").field(STATUS) };

        List<Supplier<ValuesSourceAggregationBuilder<?>>> aggregationSuppliers = List.of(
            () -> terms("term_size").field(SIZE),
            () -> terms("term_status").field(STATUS),
            () -> range("range_agg").field(STATUS).addRange(10, 30).addRange(30, 50),
            () -> terms("term_keyword").field(KEYWORD_FIELD).collectMode(Aggregator.SubAggCollectionMode.DEPTH_FIRST),
            () -> terms("term_keyword").field(KEYWORD_FIELD).collectMode(Aggregator.SubAggCollectionMode.BREADTH_FIRST),
            () -> dateHistogram("by_day").field(TIMESTAMP_FIELD).calendarInterval(DateHistogramInterval.DAY)
        );
        // 3-LEVELS [BUCKET -> BUCKET -> METRIC]
        for (ValuesSourceAggregationBuilder aggregationBuilder : aggBuilders) {
            query = new MatchAllDocsQuery();
            queryBuilder = null;
            for (Supplier<ValuesSourceAggregationBuilder<?>> outerSupplier : aggregationSuppliers) {
                for (Supplier<ValuesSourceAggregationBuilder<?>> innerSupplier : aggregationSuppliers) {

                    ValuesSourceAggregationBuilder<?> inner = innerSupplier.get().subAggregation(aggregationBuilder);
                    ValuesSourceAggregationBuilder<?> outer = outerSupplier.get().subAggregation(inner);

                    // Skipping [DateHistogramAggregationBuilder + RangeAggregationBuilder] combinations for a ReducedMultiBucketConsumer
                    // assertion in
                    // searchAndReduceStarTree
                    boolean skipReducedMultiBucketConsumerAssertion = (inner instanceof RangeAggregationBuilder
                        && outer instanceof DateHistogramAggregationBuilder);
                    testCase(
                        indexSearcher,
                        query,
                        queryBuilder,
                        outer,
                        starTree,
                        supportedDimensions,
                        skipReducedMultiBucketConsumerAssertion
                    );

                    // Numeric-terms query with numeric terms aggregation
                    for (int cases = 0; cases < 5; cases++) {
                        String queryField;
                        long queryValue;
                        if (randomBoolean()) {
                            queryField = STATUS;
                        } else {
                            queryField = SIZE;
                        }
                        queryValue = random.nextInt(10);
                        query = SortedNumericDocValuesField.newSlowExactQuery(queryField, queryValue);
                        queryBuilder = new TermQueryBuilder(queryField, queryValue);
                        testCase(
                            indexSearcher,
                            query,
                            queryBuilder,
                            outer,
                            starTree,
                            supportedDimensions,
                            skipReducedMultiBucketConsumerAssertion
                        );
                    }
                }
            }
        }

        // 4-LEVELS [BUCKET -> BUCKET -> BUCKET -> METRIC]
        for (ValuesSourceAggregationBuilder aggregationBuilder : aggBuilders) {
            query = new MatchAllDocsQuery();
            queryBuilder = null;
            for (Supplier<ValuesSourceAggregationBuilder<?>> outermostSupplier : aggregationSuppliers) {
                for (Supplier<ValuesSourceAggregationBuilder<?>> middleSupplier : aggregationSuppliers) {
                    for (Supplier<ValuesSourceAggregationBuilder<?>> innerSupplier : aggregationSuppliers) {

                        ValuesSourceAggregationBuilder<?> innermost = innerSupplier.get().subAggregation(aggregationBuilder);
                        ValuesSourceAggregationBuilder<?> middle = middleSupplier.get().subAggregation(innermost);
                        ValuesSourceAggregationBuilder<?> outermost = outermostSupplier.get().subAggregation(middle);

                        // Skipping [DateHistogramAggregationBuilder + RangeAggregationBuilder] combinations for a
                        // ReducedMultiBucketConsumer assertion in
                        // searchAndReduceStarTree
                        boolean skipReducedMultiBucketConsumerAssertion = (middle instanceof RangeAggregationBuilder
                            && outermost instanceof DateHistogramAggregationBuilder)
                            || (middle instanceof DateHistogramAggregationBuilder && innermost instanceof RangeAggregationBuilder);
                        testCase(
                            indexSearcher,
                            query,
                            queryBuilder,
                            outermost,
                            starTree,
                            supportedDimensions,
                            skipReducedMultiBucketConsumerAssertion
                        );

                        // Numeric-terms query with numeric terms aggregation
                        for (int cases = 0; cases < 5; cases++) {
                            String queryField;
                            long queryValue;
                            if (randomBoolean()) {
                                queryField = STATUS;
                            } else {
                                queryField = SIZE;
                            }
                            queryValue = random.nextInt(10);
                            query = SortedNumericDocValuesField.newSlowExactQuery(queryField, queryValue);
                            queryBuilder = new TermQueryBuilder(queryField, queryValue);
                            testCase(
                                indexSearcher,
                                query,
                                queryBuilder,
                                outermost,
                                starTree,
                                supportedDimensions,
                                skipReducedMultiBucketConsumerAssertion
                            );
                        }

                    }
                }
            }
        }

        ir.close();
        directory.close();

    }

    private void testCase(
        IndexSearcher indexSearcher,
        Query query,
        QueryBuilder queryBuilder,
        ValuesSourceAggregationBuilder<?> aggregationBuilder,
        CompositeIndexFieldInfo starTree,
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions,
        boolean skipReducedMultiBucketConsumerAssertion
    ) throws IOException {

        if (aggregationBuilder instanceof TermsAggregationBuilder) {
            assertEqualStarTreeAggregation(
                InternalTerms.class,
                InternalTerms::getBuckets,
                aggregationBuilder,
                indexSearcher,
                query,
                queryBuilder,
                starTree,
                supportedDimensions,
                skipReducedMultiBucketConsumerAssertion,
                STATUS_FIELD_TYPE,
                SIZE_FIELD_TYPE,
                TIMESTAMP_FIELD_TYPE,
                KEYWORD_FIELD_TYPE
            );

        } else if (aggregationBuilder instanceof DateHistogramAggregationBuilder) {
            assertEqualStarTreeAggregation(
                InternalDateHistogram.class,
                InternalDateHistogram::getBuckets,
                aggregationBuilder,
                indexSearcher,
                query,
                queryBuilder,
                starTree,
                supportedDimensions,
                skipReducedMultiBucketConsumerAssertion,
                STATUS_FIELD_TYPE,
                TIMESTAMP_FIELD_TYPE,
                SIZE_FIELD_TYPE,
                KEYWORD_FIELD_TYPE
            );

        } else if (aggregationBuilder instanceof RangeAggregationBuilder) {
            assertEqualStarTreeAggregation(
                InternalRange.class,
                InternalRange::getBuckets,
                aggregationBuilder,
                indexSearcher,
                query,
                queryBuilder,
                starTree,
                supportedDimensions,
                skipReducedMultiBucketConsumerAssertion,
                STATUS_FIELD_TYPE,
                SIZE_FIELD_TYPE,
                TIMESTAMP_FIELD_TYPE,
                KEYWORD_FIELD_TYPE
            );
        }
    }

    private <T extends InternalAggregation> void assertEqualStarTreeAggregation(
        Class<T> clazz,
        Function<T, List<?>> getBuckets,
        ValuesSourceAggregationBuilder<?> aggregationBuilder,
        IndexSearcher indexSearcher,
        Query query,
        QueryBuilder queryBuilder,
        CompositeIndexFieldInfo starTree,
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions,
        boolean skipReducedMultiBucketConsumerAssertion,
        MappedFieldType... fieldTypes
    ) throws IOException {

        T defaultAgg = clazz.cast(
            searchAndReduceStarTree(
                createIndexSettings(),
                indexSearcher,
                query,
                queryBuilder,
                aggregationBuilder,
                null,
                null,
                null,
                DEFAULT_MAX_BUCKETS,
                false,
                null,
                false,
                skipReducedMultiBucketConsumerAssertion,
                fieldTypes
            )
        );

        T starTreeAgg = clazz.cast(
            searchAndReduceStarTree(
                createIndexSettings(),
                indexSearcher,
                query,
                queryBuilder,
                aggregationBuilder,
                starTree,
                supportedDimensions,
                null,
                DEFAULT_MAX_BUCKETS,
                false,
                null,
                true,
                skipReducedMultiBucketConsumerAssertion,
                fieldTypes
            )
        );

        List<?> defaultBuckets = getBuckets.apply(defaultAgg);
        List<?> starTreeBuckets = getBuckets.apply(starTreeAgg);

        assertEquals(defaultBuckets.size(), starTreeBuckets.size());
        assertEquals(defaultBuckets, starTreeBuckets);
    }

}
