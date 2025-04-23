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
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite101.Composite101Codec;
import org.opensearch.index.codec.composite912.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;

import static org.opensearch.search.aggregations.AggregationBuilders.avg;
import static org.opensearch.search.aggregations.AggregationBuilders.count;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.min;
import static org.opensearch.search.aggregations.AggregationBuilders.range;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;

public class RangeAggregatorTests extends AggregatorTestCase {
    final static String STATUS = "status";
    final static String SIZE = "size";
    private static final MappedFieldType STATUS_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(
        STATUS,
        NumberFieldMapper.NumberType.LONG
    );
    private static final MappedFieldType SIZE_FIELD_NAME = new NumberFieldMapper.NumberFieldType(SIZE, NumberFieldMapper.NumberType.FLOAT);

    @Before
    public void setup() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(FeatureFlags.STAR_TREE_INDEX, true).build());
    }

    @After
    public void teardown() throws IOException {
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    protected Codec getCodec() {
        final Logger testLogger = LogManager.getLogger(NumericTermsAggregatorTests.class);
        MapperService mapperService;
        try {
            mapperService = StarTreeDocValuesFormatTests.createMapperService(NumericTermsAggregatorTests.getExpandedMapping(1, false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite101Codec(Lucene101Codec.Mode.BEST_SPEED, mapperService, testLogger);
    }

    public void testRangeAggregation() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(getCodec());
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);

        Random random = RandomizedTest.getRandom();
        int totalDocs = 100;
        List<Document> docs = new ArrayList<>();
        long val;

        // Index 100 random documents
        for (int i = 0; i < totalDocs; i++) {
            Document doc = new Document();
            if (random.nextBoolean()) {
                val = random.nextInt(100); // Random int between 0 and 99 for status
                doc.add(new SortedNumericDocValuesField(STATUS, val));
            }
            if (random.nextBoolean()) {
                val = NumericUtils.doubleToSortableLong(random.nextInt(100) + 0.5f);
                doc.add(new SortedNumericDocValuesField(SIZE, val));
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
        IndexSearcher indexSearcher = newSearcher(reader, false, false);
        CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();

        List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();
        CompositeIndexFieldInfo starTree = compositeIndexFields.get(0);

        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions = new LinkedHashMap<>();
        supportedDimensions.put(new NumericDimension(STATUS), STATUS_FIELD_TYPE);
        supportedDimensions.put(new NumericDimension(SIZE), SIZE_FIELD_NAME);

        Query query = new MatchAllDocsQuery();
        QueryBuilder queryBuilder = null;
        RangeAggregationBuilder rangeAggregationBuilder = range("range_agg").field(STATUS).addRange(10, 30).addRange(30, 50);
        // no sub-aggregation
        testCase(indexSearcher, query, queryBuilder, rangeAggregationBuilder, starTree, supportedDimensions);

        ValuesSourceAggregationBuilder[] aggBuilders = {
            sum("_sum").field(SIZE),
            max("_max").field(SIZE),
            min("_min").field(SIZE),
            count("_count").field(SIZE),
            avg("_avg").field(SIZE) };

        for (ValuesSourceAggregationBuilder aggregationBuilder : aggBuilders) {
            query = new MatchAllDocsQuery();
            queryBuilder = null;
            rangeAggregationBuilder = range("range_agg").field(STATUS).addRange(10, 30).addRange(30, 50).subAggregation(aggregationBuilder);
            // sub-aggregation, no top level query
            testCase(indexSearcher, query, queryBuilder, rangeAggregationBuilder, starTree, supportedDimensions);

            // Numeric-terms query with range aggregation
            for (int cases = 0; cases < 100; cases++) {
                // term query of status field
                String queryField = SIZE;
                long queryValue = NumericUtils.floatToSortableInt(random.nextInt(50) + 0.5f);
                query = SortedNumericDocValuesField.newSlowExactQuery(queryField, queryValue);
                queryBuilder = new TermQueryBuilder(queryField, queryValue);
                testCase(indexSearcher, query, queryBuilder, rangeAggregationBuilder, starTree, supportedDimensions);

                // range query on same field as aggregation field
                query = SortedNumericDocValuesField.newSlowRangeQuery(STATUS, 15, 35);
                queryBuilder = new RangeQueryBuilder(STATUS).from(15).to(35);
                testCase(indexSearcher, query, queryBuilder, rangeAggregationBuilder, starTree, supportedDimensions);
            }
        }

        ir.close();
        reader.close();
        directory.close();
    }

    private void testCase(
        IndexSearcher indexSearcher,
        Query query,
        QueryBuilder queryBuilder,
        RangeAggregationBuilder rangeAggregationBuilder,
        CompositeIndexFieldInfo starTree,
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions
    ) throws IOException {
        InternalRange starTreeAggregation = searchAndReduceStarTree(
            createIndexSettings(),
            indexSearcher,
            query,
            queryBuilder,
            rangeAggregationBuilder,
            starTree,
            supportedDimensions,
            null,
            DEFAULT_MAX_BUCKETS,
            false,
            null,
            true,
            STATUS_FIELD_TYPE,
            SIZE_FIELD_NAME
        );

        InternalRange defaultAggregation = searchAndReduceStarTree(
            createIndexSettings(),
            indexSearcher,
            query,
            queryBuilder,
            rangeAggregationBuilder,
            null,
            null,
            null,
            DEFAULT_MAX_BUCKETS,
            false,
            null,
            false,
            STATUS_FIELD_TYPE,
            SIZE_FIELD_NAME
        );

        assertEquals(defaultAggregation.getBuckets().size(), starTreeAggregation.getBuckets().size());
        assertEquals(defaultAggregation.getBuckets(), starTreeAggregation.getBuckets());
    }
}
