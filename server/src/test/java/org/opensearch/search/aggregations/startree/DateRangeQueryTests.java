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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.Rounding;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite103.Composite103Codec;
import org.opensearch.index.codec.composite912.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.opensearch.search.aggregations.AggregationBuilders.range;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;

/**
 * Tests for aggregations with date-range query
 */
public class DateRangeQueryTests extends AggregatorTestCase {
    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final MappedFieldType TIMESTAMP_FIELD_TYPE = new DateFieldMapper.DateFieldType(TIMESTAMP_FIELD);

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
            mapperService = StarTreeDocValuesFormatTests.createMapperService(DateHistogramAggregatorTests.getExpandedMapping(1, false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite103Codec(Lucene103Codec.Mode.BEST_SPEED, mapperService, testLogger);
    }

    public void testStarTreeValidDateRangeQuery() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(getCodec());
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);

        Random random = RandomizedTest.getRandom();
        int totalDocs = 100;

        int val;
        long date;

        final long nowMillis = System.currentTimeMillis();
        Instant now = Instant.ofEpochMilli(nowMillis);

        final long duration180DaysMillis = TimeUnit.DAYS.toMillis(180);

        List<Document> docs = new ArrayList<>();
        // Index 100 random documents
        for (int i = 0; i < totalDocs; i++) {
            Document doc = new Document();
            if (random.nextBoolean()) {
                val = random.nextInt(10); // Random int between 0 and 9 for status
                doc.add(new SortedNumericDocValuesField(STATUS, val));
            }
            if (random.nextBoolean()) {
                val = random.nextInt(100); // Random int between 0 and 99 for size
                doc.add(new SortedNumericDocValuesField(SIZE, val));
            }
            date = nowMillis - random.nextLong(duration180DaysMillis); // Random date within last 180 days
            doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, date));
            doc.add(new LongPoint(TIMESTAMP_FIELD, date));
            iw.addDocument(doc);
            docs.add(doc);
        }

        if (randomBoolean()) {
            iw.forceMerge(1);
        }
        iw.close();

        DirectoryReader ir = DirectoryReader.open(directory);
        initValuesSourceRegistry();
        LeafReaderContext context = ir.leaves().get(0);

        SegmentReader reader = Lucene.segmentReader(context.reader());
        IndexSearcher indexSearcher = newSearcher(reader, false, false);
        CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();

        List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();
        CompositeIndexFieldInfo starTree = compositeIndexFields.get(0);

        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions = new LinkedHashMap<>();
        supportedDimensions.put(
            new NumericDimension(STATUS),
            new NumberFieldMapper.NumberFieldType(STATUS, NumberFieldMapper.NumberType.INTEGER)
        );
        supportedDimensions.put(
            new NumericDimension(SIZE),
            new NumberFieldMapper.NumberFieldType(SIZE, NumberFieldMapper.NumberType.INTEGER)
        );
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

        DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
        Instant startOfTodayUTC = now.truncatedTo(ChronoUnit.DAYS);

        for (int i = 0; i < 100; i++) {
            int randomFrom = random.nextInt(80) + 120;
            int randomTo = random.nextInt(80);

            long from = startOfTodayUTC.toEpochMilli() - TimeUnit.DAYS.toMillis(randomFrom);
            long to = startOfTodayUTC.toEpochMilli() - TimeUnit.DAYS.toMillis(randomTo);

            String fromDateString = formatter.format(Instant.ofEpochMilli(from));
            String toDateString = formatter.format(Instant.ofEpochMilli(to));

            String fromDateStringNow = "now-" + randomFrom + "d/d";
            String toDateStringNow = "now-" + randomTo + "d/d";

            Query query = new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery(TIMESTAMP_FIELD, from, to - 1),
                SortedNumericDocValuesField.newSlowRangeQuery(TIMESTAMP_FIELD, from, to - 1)
            );

            // corresponding queryBuilder for the above query
            QueryBuilder queryBuilderAbsolute = new RangeQueryBuilder(TIMESTAMP_FIELD).from(fromDateString)
                .includeLower(true)
                .to(toDateString)
                .includeUpper(false)
                .format("strict_date_optional_time||epoch_millis");

            QueryBuilder queryBuilderRelative = new RangeQueryBuilder(TIMESTAMP_FIELD).from(fromDateStringNow)
                .includeLower(true)
                .to(toDateStringNow)
                .includeUpper(false)
                .format("strict_date_optional_time||epoch_millis");

            AggregationBuilder sumAggregationBuilder = sum("max").field(SIZE);
            testCase(indexSearcher, query, queryBuilderAbsolute, sumAggregationBuilder, starTree, supportedDimensions);
            testCase(indexSearcher, query, queryBuilderRelative, sumAggregationBuilder, starTree, supportedDimensions);

            AggregationBuilder rangeAggregationBuilder = range("range_agg").field(STATUS).addRange(10, 30).addRange(30, 50);
            testCase(indexSearcher, query, queryBuilderAbsolute, rangeAggregationBuilder, starTree, supportedDimensions);
            testCase(indexSearcher, query, queryBuilderRelative, rangeAggregationBuilder, starTree, supportedDimensions);

            rangeAggregationBuilder = range("range_agg").field(STATUS)
                .addRange(10, 30)
                .addRange(30, 50)
                .subAggregation(sumAggregationBuilder);
            testCase(indexSearcher, query, queryBuilderAbsolute, rangeAggregationBuilder, starTree, supportedDimensions);
            testCase(indexSearcher, query, queryBuilderRelative, rangeAggregationBuilder, starTree, supportedDimensions);

            AggregationBuilder termAggregationBuilder = terms("terms_agg").field(STATUS).subAggregation(sumAggregationBuilder);
            testCase(indexSearcher, query, queryBuilderAbsolute, termAggregationBuilder, starTree, supportedDimensions);
            testCase(indexSearcher, query, queryBuilderRelative, termAggregationBuilder, starTree, supportedDimensions);

            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.must(queryBuilderAbsolute).must(queryBuilderAbsolute);
            BooleanQuery booleanQuery = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
                .add(query, BooleanClause.Occur.MUST)
                .build();
            testCaseForQuery(indexSearcher, booleanQuery, boolQueryBuilder, sumAggregationBuilder, starTree, supportedDimensions);

            boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.should(queryBuilderAbsolute).should(queryBuilderAbsolute);
            booleanQuery = new BooleanQuery.Builder().add(query, BooleanClause.Occur.SHOULD).add(query, BooleanClause.Occur.SHOULD).build();
            testCaseForQuery(indexSearcher, booleanQuery, boolQueryBuilder, sumAggregationBuilder, starTree, supportedDimensions);
        }
        ir.close();
        directory.close();
    }

    /**
     * Test case for the given query and aggregation builder.
     * Also, wraps the query in boolean query to test for the same results.
     */
    private void testCase(
        IndexSearcher indexSearcher,
        Query query,
        QueryBuilder queryBuilder,
        AggregationBuilder aggregationBuilder,
        CompositeIndexFieldInfo starTree,
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions
    ) throws IOException {
        testCaseForQuery(indexSearcher, query, queryBuilder, aggregationBuilder, starTree, supportedDimensions);

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(queryBuilder);
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
            .add(query, BooleanClause.Occur.MUST)
            .build();
        testCaseForQuery(indexSearcher, booleanQuery, boolQueryBuilder, aggregationBuilder, starTree, supportedDimensions);
    }

    private void testCaseForQuery(
        IndexSearcher indexSearcher,
        Query query,
        QueryBuilder queryBuilder,
        AggregationBuilder aggregationBuilder,
        CompositeIndexFieldInfo starTree,
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions
    ) throws IOException {
        InternalAggregation starTreeAggregation = searchAndReduceStarTree(
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
            TIMESTAMP_FIELD_TYPE,
            SIZE_FIELD_TYPE,
            STATUS_FIELD_TYPE
        );

        InternalAggregation defaultAggregation = searchAndReduceStarTree(
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
            TIMESTAMP_FIELD_TYPE,
            SIZE_FIELD_TYPE,
            STATUS_FIELD_TYPE
        );

        assertEquals(defaultAggregation, starTreeAggregation);
    }
}
