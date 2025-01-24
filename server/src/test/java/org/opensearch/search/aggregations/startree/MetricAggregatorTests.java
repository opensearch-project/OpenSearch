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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
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
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite101.Composite101Codec;
import org.opensearch.index.codec.composite912.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.OrdinalDimension;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MetricAggregatorFactory;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
            mapperService = StarTreeDocValuesFormatTests.createMapperService(StarTreeQueryTests.getExpandedMapping(1, false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite101Codec(Lucene101Codec.Mode.BEST_SPEED, mapperService, testLogger);
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
        final String KEYWORD = "keyword_field";
        int val;

        List<Document> docs = new ArrayList<>();
        // Index 100 random documents
        for (int i = 0; i < totalDocs; i++) {
            Document doc = new Document();
            if (random.nextBoolean()) {
                val = random.nextInt(10) - 5; // Random long between -5 and 4
                doc.add(new LongField(SNDV, val, Field.Store.YES));
            }
            if (random.nextBoolean()) {
                val = random.nextInt(20) - 10; // Random long between -10 and 9
                doc.add(new LongField(DV, val, Field.Store.YES));
            }
            if (random.nextBoolean()) {
                val = random.nextInt(50); // Random long between 0 and 49
                doc.add(new SortedNumericDocValuesField(FIELD_NAME, val));
            }
            if (random.nextBoolean()) {
                val = random.nextInt(50);
                doc.add(new KeywordField(KEYWORD, String.valueOf(val), Field.Store.YES));
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

        MapperService mapperService = mapperServiceMock();
        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        when(mapperService.fieldType(SNDV)).thenReturn(new NumberFieldMapper.NumberFieldType(SNDV, NumberFieldMapper.NumberType.LONG));
        when(mapperService.fieldType(DV)).thenReturn(new NumberFieldMapper.NumberFieldType(DV, NumberFieldMapper.NumberType.LONG));
        when(mapperService.fieldType(KEYWORD)).thenReturn(new KeywordFieldMapper.KeywordFieldType(KEYWORD));
        QueryShardContext queryShardContext = queryShardContextMock(
            indexSearcher,
            mapperService,
            createIndexSettings(),
            circuitBreakerService,
            new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), circuitBreakerService).withCircuitBreaking()
        );
        when(queryShardContext.fieldMapper(SNDV)).thenReturn(
            new NumberFieldMapper.NumberFieldType(SNDV, NumberFieldMapper.NumberType.LONG)
        );
        when(queryShardContext.fieldMapper(DV)).thenReturn(new NumberFieldMapper.NumberFieldType(DV, NumberFieldMapper.NumberType.LONG));
        when(queryShardContext.fieldMapper(KEYWORD)).thenReturn(new KeywordFieldMapper.KeywordFieldType(KEYWORD));

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
        supportedDimensions.add(new OrdinalDimension(KEYWORD));

        Query query = new MatchAllDocsQuery();
        // match-all query
        QueryBuilder numericTermQueryBuilder = null; // no predicates
        QueryBuilder numericRangeQueryBuilder = null;
        QueryBuilder keywordTermQueryBuilder = null;
        QueryBuilder keywordRangeQueryBuilder = null;
        testCase(
            indexSearcher,
            query,
            numericTermQueryBuilder,
            sumAggregationBuilder,
            starTree,
            supportedDimensions,
            verifyAggregation(InternalSum::getValue)
        );
        testCase(
            indexSearcher,
            query,
            numericTermQueryBuilder,
            maxAggregationBuilder,
            starTree,
            supportedDimensions,
            verifyAggregation(InternalMax::getValue)
        );
        testCase(
            indexSearcher,
            query,
            numericTermQueryBuilder,
            minAggregationBuilder,
            starTree,
            supportedDimensions,
            verifyAggregation(InternalMin::getValue)
        );
        testCase(
            indexSearcher,
            query,
            numericTermQueryBuilder,
            valueCountAggregationBuilder,
            starTree,
            supportedDimensions,
            verifyAggregation(InternalValueCount::getValue)
        );
        testCase(
            indexSearcher,
            query,
            numericTermQueryBuilder,
            avgAggregationBuilder,
            starTree,
            supportedDimensions,
            verifyAggregation(InternalAvg::getValue)
        );

        // Numeric-terms query
        for (int cases = 0; cases < 100; cases++) {
            String queryField;
            long queryLow, queryHigh;
            if (randomBoolean()) {
                queryField = SNDV;
                queryLow = random.nextInt(10);
            } else {
                queryField = DV;
                queryLow = random.nextInt(20) - 15;
            }
            queryHigh = random.nextInt(10);

            numericTermQueryBuilder = new TermQueryBuilder(queryField, queryLow);
            numericRangeQueryBuilder = getRandomRangeQuery(queryField, queryLow, queryHigh);
            long keywordLow = randomIntBetween(1, 50);
            long keywordHigh = randomIntBetween(1, 50);
            keywordTermQueryBuilder = new TermQueryBuilder(KEYWORD, String.valueOf(keywordLow));
            keywordRangeQueryBuilder = getRandomRangeQuery(KEYWORD, String.valueOf(keywordLow), String.valueOf(keywordHigh));

            for (QueryBuilder qb : new QueryBuilder[] {
                numericTermQueryBuilder,
                numericRangeQueryBuilder,
                keywordTermQueryBuilder,
                keywordRangeQueryBuilder }) {
                query = qb.toQuery(queryShardContext);
                testCase(
                    indexSearcher,
                    query,
                    qb,
                    sumAggregationBuilder,
                    starTree,
                    supportedDimensions,
                    verifyAggregation(InternalSum::getValue)
                );
                testCase(
                    indexSearcher,
                    query,
                    qb,
                    maxAggregationBuilder,
                    starTree,
                    supportedDimensions,
                    verifyAggregation(InternalMax::getValue)
                );
                testCase(
                    indexSearcher,
                    query,
                    qb,
                    minAggregationBuilder,
                    starTree,
                    supportedDimensions,
                    verifyAggregation(InternalMin::getValue)
                );
                testCase(
                    indexSearcher,
                    query,
                    qb,
                    valueCountAggregationBuilder,
                    starTree,
                    supportedDimensions,
                    verifyAggregation(InternalValueCount::getValue)
                );
                testCase(
                    indexSearcher,
                    query,
                    qb,
                    avgAggregationBuilder,
                    starTree,
                    supportedDimensions,
                    verifyAggregation(InternalAvg::getValue)
                );
            }
        }

        MetricAggregatorFactory aggregatorFactory = mock(MetricAggregatorFactory.class);
        when(aggregatorFactory.getSubFactories()).thenReturn(AggregatorFactories.EMPTY);
        when(aggregatorFactory.getField()).thenReturn(FIELD_NAME);
        when(aggregatorFactory.getMetricStat()).thenReturn(MetricStat.SUM);

        query = numericRangeQueryBuilder.toQuery(queryShardContext);
        // Case when field and metric type in aggregation are fully supported by star tree.
        testCase(
            indexSearcher,
            query,
            numericRangeQueryBuilder,
            sumAggregationBuilder,
            starTree,
            supportedDimensions,
            List.of(new Metric(FIELD_NAME, List.of(MetricStat.SUM, MetricStat.MAX, MetricStat.MIN, MetricStat.AVG))),
            verifyAggregation(InternalSum::getValue),
            aggregatorFactory,
            true
        );

        // Case when the field is not supported by star tree
        SumAggregationBuilder invalidFieldSumAggBuilder = sum("_name").field("hello");
        testCase(
            indexSearcher,
            query,
            numericRangeQueryBuilder,
            invalidFieldSumAggBuilder,
            starTree,
            supportedDimensions,
            Collections.emptyList(),
            verifyAggregation(InternalSum::getValue),
            invalidFieldSumAggBuilder.build(queryShardContext, null),
            false // Invalid fields will return null StarTreeQueryContext which will not cause early termination by leaf collector
        );

        // Case when metric type in aggregation is not supported by star tree but the field is supported.
        testCase(
            indexSearcher,
            query,
            numericRangeQueryBuilder,
            sumAggregationBuilder,
            starTree,
            supportedDimensions,
            List.of(new Metric(FIELD_NAME, List.of(MetricStat.MAX, MetricStat.MIN, MetricStat.AVG))),
            verifyAggregation(InternalSum::getValue),
            aggregatorFactory,
            false
        );

        // Case when field is not present in supported metrics
        testCase(
            indexSearcher,
            query,
            numericRangeQueryBuilder,
            sumAggregationBuilder,
            starTree,
            supportedDimensions,
            List.of(new Metric("hello", List.of(MetricStat.MAX, MetricStat.MIN, MetricStat.AVG))),
            verifyAggregation(InternalSum::getValue),
            aggregatorFactory,
            false
        );

        AggregatorFactories aggregatorFactories = mock(AggregatorFactories.class);
        when(aggregatorFactories.getFactories()).thenReturn(new AggregatorFactory[] { mock(MetricAggregatorFactory.class) });
        when(aggregatorFactory.getSubFactories()).thenReturn(aggregatorFactories);

        // Case when sub aggregations are present
        testCase(
            indexSearcher,
            query,
            numericRangeQueryBuilder,
            sumAggregationBuilder,
            starTree,
            supportedDimensions,
            List.of(new Metric("hello", List.of(MetricStat.MAX, MetricStat.MIN, MetricStat.AVG))),
            verifyAggregation(InternalSum::getValue),
            aggregatorFactory,
            false
        );

        // Case when aggregation factory is not metric aggregation
        testCase(
            indexSearcher,
            query,
            numericRangeQueryBuilder,
            sumAggregationBuilder,
            starTree,
            supportedDimensions,
            List.of(new Metric("hello", List.of(MetricStat.MAX, MetricStat.MIN, MetricStat.AVG))),
            verifyAggregation(InternalSum::getValue),
            mock(ValuesSourceAggregatorFactory.class),
            false
        );

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
        testCase(searcher, query, queryBuilder, aggBuilder, starTree, supportedDimensions, Collections.emptyList(), verify, null, true);
    }

    private <T extends AggregationBuilder, V extends InternalAggregation> void testCase(
        IndexSearcher searcher,
        Query query,
        QueryBuilder queryBuilder,
        T aggBuilder,
        CompositeIndexFieldInfo starTree,
        List<Dimension> supportedDimensions,
        List<Metric> supportedMetrics,
        BiConsumer<V, V> verify,
        AggregatorFactory aggregatorFactory,
        boolean assertCollectorEarlyTermination
    ) throws IOException {
        V starTreeAggregation = searchAndReduceStarTree(
            createIndexSettings(),
            searcher,
            query,
            queryBuilder,
            aggBuilder,
            starTree,
            supportedDimensions,
            supportedMetrics,
            DEFAULT_MAX_BUCKETS,
            false,
            aggregatorFactory,
            assertCollectorEarlyTermination,
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
            null,
            DEFAULT_MAX_BUCKETS,
            false,
            aggregatorFactory,
            assertCollectorEarlyTermination,
            DEFAULT_MAPPED_FIELD
        );
        if (!expectedAggregation.equals(starTreeAggregation)) {
            System.out.println("Assertion Failed for Query " + query + " Builder " + queryBuilder);
            System.out.println("StarTreeResult: " + starTreeAggregation + " Should have been: " + expectedAggregation);
        }
        verify.accept(expectedAggregation, starTreeAggregation);
    }

    private RangeQueryBuilder getRandomRangeQuery(String fieldName, Object from, Object to) {
        if (randomBoolean()) {
            from = null;
        }
        if (randomBoolean()) {
            to = null;
        }
        return QueryBuilders.rangeQuery(fieldName).from(from).to(to).includeLower(randomBoolean()).includeUpper(randomBoolean());
    }
}
