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
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.Rounding;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite101.Composite101Codec;
import org.opensearch.index.codec.composite912.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregatorTestCase;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;

import static org.opensearch.index.codec.composite912.datacube.startree.AbstractStarTreeDVFormatTests.topMapping;
import static org.opensearch.search.aggregations.AggregationBuilders.avg;
import static org.opensearch.search.aggregations.AggregationBuilders.count;
import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.min;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;

public class DateHistogramAggregatorTests extends DateHistogramAggregatorTestCase {
    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final MappedFieldType TIMESTAMP_FIELD_TYPE = new DateFieldMapper.DateFieldType(TIMESTAMP_FIELD);

    private static final String FIELD_NAME = "status";
    private static final MappedFieldType NUMBER_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(
        FIELD_NAME,
        NumberFieldMapper.NumberType.LONG
    );

    @Before
    public void setup() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(FeatureFlags.STAR_TREE_INDEX, true).build());
    }

    @After
    public void teardown() throws IOException {
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    protected Codec getCodec() {
        final Logger testLogger = LogManager.getLogger(MetricAggregatorTests.class);
        MapperService mapperService;
        try {
            mapperService = StarTreeDocValuesFormatTests.createMapperService(getExpandedMapping(1, false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite101Codec(Lucene101Codec.Mode.BEST_SPEED, mapperService, testLogger);
    }

    public void testStarTreeDateHistogram() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(getCodec());
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);

        Random random = RandomizedTest.getRandom();
        int totalDocs = 100;
        final String STATUS = "status";
        final String SIZE = "size";
        int val;
        long date;

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
            date = random.nextInt(180) * 24 * 60 * 60 * 1000L; // Random date within 180 days
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

        ValuesSourceAggregationBuilder[] agggBuilders = {
            sum("_name").field(FIELD_NAME),
            max("_name").field(FIELD_NAME),
            min("_name").field(FIELD_NAME),
            count("_name").field(FIELD_NAME),
            avg("_name").field(FIELD_NAME) };

        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions = new LinkedHashMap<>();
        supportedDimensions.put(
            new NumericDimension(STATUS),
            new NumberFieldMapper.NumberFieldType(STATUS, NumberFieldMapper.NumberType.INTEGER)
        );
        supportedDimensions.put(
            new NumericDimension(SIZE),
            new NumberFieldMapper.NumberFieldType(STATUS, NumberFieldMapper.NumberType.INTEGER)
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

        for (ValuesSourceAggregationBuilder aggregationBuilder : agggBuilders) {
            Query query = new MatchAllDocsQuery();
            QueryBuilder queryBuilder = null;

            DateHistogramAggregationBuilder dateHistogramAggregationBuilder = dateHistogram("by_day").field(TIMESTAMP_FIELD)
                .calendarInterval(DateHistogramInterval.DAY)
                .subAggregation(aggregationBuilder);
            testCase(indexSearcher, query, queryBuilder, dateHistogramAggregationBuilder, starTree, supportedDimensions);

            dateHistogramAggregationBuilder = dateHistogram("by_month").field(TIMESTAMP_FIELD)
                .calendarInterval(DateHistogramInterval.MONTH)
                .subAggregation(aggregationBuilder);
            testCase(indexSearcher, query, queryBuilder, dateHistogramAggregationBuilder, starTree, supportedDimensions);

            // year not present in star-tree, but should be able to compute using @timestamp_day dimension
            dateHistogramAggregationBuilder = dateHistogram("by_year").field(TIMESTAMP_FIELD)
                .calendarInterval(DateHistogramInterval.YEAR)
                .subAggregation(aggregationBuilder);
            testCase(indexSearcher, query, queryBuilder, dateHistogramAggregationBuilder, starTree, supportedDimensions);

            // Numeric-terms query with date histogram
            for (int cases = 0; cases < 100; cases++) {
                String queryField;
                long queryValue;
                if (randomBoolean()) {
                    queryField = STATUS;
                    queryValue = random.nextInt(10);
                } else {
                    queryField = SIZE;
                    queryValue = random.nextInt(20) - 15;
                }
                dateHistogramAggregationBuilder = dateHistogram("by_month").field(TIMESTAMP_FIELD)
                    .calendarInterval(DateHistogramInterval.MONTH)
                    .subAggregation(aggregationBuilder);
                query = SortedNumericDocValuesField.newSlowExactQuery(queryField, queryValue);
                queryBuilder = new TermQueryBuilder(queryField, queryValue);
                testCase(indexSearcher, query, queryBuilder, dateHistogramAggregationBuilder, starTree, supportedDimensions);

                // year not present in star-tree, but should be able to compute using @timestamp_day dimension
                dateHistogramAggregationBuilder = dateHistogram("by_year").field(TIMESTAMP_FIELD)
                    .calendarInterval(DateHistogramInterval.YEAR)
                    .subAggregation(aggregationBuilder);
                testCase(indexSearcher, query, queryBuilder, dateHistogramAggregationBuilder, starTree, supportedDimensions);
            }
        }
        ir.close();
        directory.close();
    }

    private void testCase(
        IndexSearcher indexSearcher,
        Query query,
        QueryBuilder queryBuilder,
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder,
        CompositeIndexFieldInfo starTree,
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions
    ) throws IOException {
        InternalDateHistogram starTreeAggregation = searchAndReduceStarTree(
            createIndexSettings(),
            indexSearcher,
            query,
            queryBuilder,
            dateHistogramAggregationBuilder,
            starTree,
            supportedDimensions,
            null,
            DEFAULT_MAX_BUCKETS,
            false,
            null,
            true,
            TIMESTAMP_FIELD_TYPE,
            NUMBER_FIELD_TYPE
        );

        InternalDateHistogram defaultAggregation = searchAndReduceStarTree(
            createIndexSettings(),
            indexSearcher,
            query,
            queryBuilder,
            dateHistogramAggregationBuilder,
            null,
            null,
            null,
            DEFAULT_MAX_BUCKETS,
            false,
            null,
            false,
            TIMESTAMP_FIELD_TYPE,
            NUMBER_FIELD_TYPE
        );

        assertEquals(defaultAggregation.getBuckets().size(), starTreeAggregation.getBuckets().size());
        assertEquals(defaultAggregation.getBuckets(), starTreeAggregation.getBuckets());
    }

    public static XContentBuilder getExpandedMapping(int maxLeafDocs, boolean skipStarNodeCreationForStatusDimension) throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree1"); // Use the same name as the provided mapping
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("max_leaf_docs", maxLeafDocs);
            if (skipStarNodeCreationForStatusDimension) {
                b.startArray("skip_star_node_creation_for_dimensions");
                b.value("status"); // Skip for "status" dimension
                b.endArray();
            }
            b.startArray("ordered_dimensions");
            b.startObject();
            b.field("name", "status");
            b.endObject();
            b.startObject();
            b.field("name", "size");
            b.endObject();
            b.startObject();
            b.field("name", TIMESTAMP_FIELD);
            b.startArray("calendar_intervals");
            b.value("month");
            b.value("day");
            b.endArray();
            b.endObject();
            b.endArray();
            b.startArray("metrics");
            b.startObject();
            b.field("name", "size");
            b.startArray("stats");
            b.value("sum");
            b.value("value_count");
            b.value("min");
            b.value("max");
            b.endArray();
            b.endObject();
            b.startObject();
            b.field("name", "status");
            b.startArray("stats");
            b.value("sum");
            b.value("value_count");
            b.value("min");
            b.value("max");
            b.endArray();
            b.endObject();
            b.endArray();
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("@timestamp");
            b.field("type", "date");
            b.field("format", "strict_date_optional_time||epoch_second");
            b.endObject();
            b.startObject("message");
            b.field("type", "keyword");
            b.field("index", false);
            b.field("doc_values", false);
            b.endObject();
            b.startObject("clientip");
            b.field("type", "ip");
            b.endObject();
            b.startObject("request");
            b.field("type", "text");
            b.startObject("fields");
            b.startObject("raw");
            b.field("type", "keyword");
            b.field("ignore_above", 256);
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("status");
            b.field("type", "integer");
            b.endObject();
            b.startObject("size");
            b.field("type", "integer");
            b.endObject();
            b.startObject("geoip");
            b.startObject("properties");
            b.startObject("country_name");
            b.field("type", "keyword");
            b.endObject();
            b.startObject("city_name");
            b.field("type", "keyword");
            b.endObject();
            b.startObject("location");
            b.field("type", "geo_point");
            b.endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        });
    }
}
