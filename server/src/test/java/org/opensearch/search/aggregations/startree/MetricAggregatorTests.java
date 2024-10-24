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
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
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
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite912.Composite912Codec;
import org.opensearch.index.codec.composite912.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.opensearch.search.aggregations.AggregationBuilders.avg;
import static org.opensearch.search.aggregations.AggregationBuilders.count;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.min;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;

public class MetricAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";
    private static final NumberFieldMapper.NumberType DEFAULT_FIELD_TYPE = NumberFieldMapper.NumberType.LONG;
    private static final MappedFieldType DEFAULT_MAPPED_FIELD = new NumberFieldMapper.NumberFieldType(FIELD_NAME, DEFAULT_FIELD_TYPE);

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
            mapperService = StarTreeDocValuesFormatTests.createMapperService(StarTreeFilterTests.getExpandedMapping(1, false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite912Codec(Lucene912Codec.Mode.BEST_SPEED, mapperService, testLogger);
    }

    public void testStarTreeDocValues() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(getCodec());
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);

        Random random = RandomizedTest.getRandom();
        int totalDocs = 100;
        final String SNDV = "sndv";
        final String DV = "dv";
        int val;

        List<Document> docs = new ArrayList<>();
        // Index 100 random documents
        for (int i = 0; i < totalDocs; i++) {
            Document doc = new Document();
            if (random.nextBoolean()) {
                val = random.nextInt(10) - 5; // Random long between -5 and 4
                doc.add(new SortedNumericDocValuesField(SNDV, val));
            }
            if (random.nextBoolean()) {
                val = random.nextInt(20) - 10; // Random long between -10 and 9
                doc.add(new SortedNumericDocValuesField(DV, val));
            }
            if (random.nextBoolean()) {
                val = random.nextInt(50); // Random long between 0 and 49
                doc.add(new SortedNumericDocValuesField(FIELD_NAME, val));
            }
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

        SumAggregationBuilder sumAggregationBuilder = sum("_name").field(FIELD_NAME);
        MaxAggregationBuilder maxAggregationBuilder = max("_name").field(FIELD_NAME);
        MinAggregationBuilder minAggregationBuilder = min("_name").field(FIELD_NAME);
        ValueCountAggregationBuilder valueCountAggregationBuilder = count("_name").field(FIELD_NAME);
        AvgAggregationBuilder avgAggregationBuilder = avg("_name").field(FIELD_NAME);

        List<Dimension> supportedDimensions = new LinkedList<>();
        supportedDimensions.add(new NumericDimension(SNDV));
        supportedDimensions.add(new NumericDimension(DV));

        Query query = new MatchAllDocsQuery();
        // match-all query
        QueryBuilder queryBuilder = null; // no predicates
        testCase(
            indexSearcher,
            query,
            queryBuilder,
            sumAggregationBuilder,
            starTree,
            supportedDimensions,
            verifyAggregation(InternalSum::getValue)
        );
        testCase(
            indexSearcher,
            query,
            queryBuilder,
            maxAggregationBuilder,
            starTree,
            supportedDimensions,
            verifyAggregation(InternalMax::getValue)
        );
        testCase(
            indexSearcher,
            query,
            queryBuilder,
            minAggregationBuilder,
            starTree,
            supportedDimensions,
            verifyAggregation(InternalMin::getValue)
        );
        testCase(
            indexSearcher,
            query,
            queryBuilder,
            valueCountAggregationBuilder,
            starTree,
            supportedDimensions,
            verifyAggregation(InternalValueCount::getValue)
        );
        testCase(
            indexSearcher,
            query,
            queryBuilder,
            avgAggregationBuilder,
            starTree,
            supportedDimensions,
            verifyAggregation(InternalAvg::getValue)
        );

        // Numeric-terms query
        for (int cases = 0; cases < 100; cases++) {
            String queryField;
            long queryValue;
            if (randomBoolean()) {
                queryField = SNDV;
                queryValue = random.nextInt(10);
            } else {
                queryField = DV;
                queryValue = random.nextInt(20) - 15;
            }

            query = SortedNumericDocValuesField.newSlowExactQuery(queryField, queryValue);
            queryBuilder = new TermQueryBuilder(queryField, queryValue);

            testCase(
                indexSearcher,
                query,
                queryBuilder,
                sumAggregationBuilder,
                starTree,
                supportedDimensions,
                verifyAggregation(InternalSum::getValue)
            );
            testCase(
                indexSearcher,
                query,
                queryBuilder,
                maxAggregationBuilder,
                starTree,
                supportedDimensions,
                verifyAggregation(InternalMax::getValue)
            );
            testCase(
                indexSearcher,
                query,
                queryBuilder,
                minAggregationBuilder,
                starTree,
                supportedDimensions,
                verifyAggregation(InternalMin::getValue)
            );
            testCase(
                indexSearcher,
                query,
                queryBuilder,
                valueCountAggregationBuilder,
                starTree,
                supportedDimensions,
                verifyAggregation(InternalValueCount::getValue)
            );
            testCase(
                indexSearcher,
                query,
                queryBuilder,
                avgAggregationBuilder,
                starTree,
                supportedDimensions,
                verifyAggregation(InternalAvg::getValue)
            );
        }

        ir.close();
        directory.close();
    }

    <T, R extends Number> BiConsumer<T, T> verifyAggregation(Function<T, R> valueExtractor) {
        return (expectedAggregation, actualAggregation) -> assertEquals(
            valueExtractor.apply(expectedAggregation).doubleValue(),
            valueExtractor.apply(actualAggregation).doubleValue(),
            0.0f
        );
    }

    private <T extends AggregationBuilder, V extends InternalAggregation> void testCase(
        IndexSearcher searcher,
        Query query,
        QueryBuilder queryBuilder,
        T aggBuilder,
        CompositeIndexFieldInfo starTree,
        List<Dimension> supportedDimensions,
        BiConsumer<V, V> verify
    ) throws IOException {
        V starTreeAggregation = searchAndReduceStarTree(
            createIndexSettings(),
            searcher,
            query,
            queryBuilder,
            aggBuilder,
            starTree,
            supportedDimensions,
            DEFAULT_MAX_BUCKETS,
            false,
            DEFAULT_MAPPED_FIELD
        );
        V expectedAggregation = searchAndReduceStarTree(
            createIndexSettings(),
            searcher,
            query,
            queryBuilder,
            aggBuilder,
            null,
            null,
            DEFAULT_MAX_BUCKETS,
            false,
            DEFAULT_MAPPED_FIELD
        );
        verify.accept(expectedAggregation, starTreeAggregation);
    }
}
