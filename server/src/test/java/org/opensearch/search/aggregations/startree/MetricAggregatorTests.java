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
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.IndexSearcher;
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
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    protected Codec getCodec(
        Supplier<Integer> maxLeafDocsSupplier,
        LinkedHashMap<String, String> dimensionAndType,
        Map<String, String> metricFieldAndType
    ) {
        final Logger testLogger = LogManager.getLogger(MetricAggregatorTests.class);
        MapperService mapperService;
        try {
            mapperService = StarTreeDocValuesFormatTests.createMapperService(
                StarTreeFilterTests.getExpandedMapping(maxLeafDocsSupplier.get(), false, dimensionAndType, metricFieldAndType)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite101Codec(Lucene101Codec.Mode.BEST_SPEED, mapperService, testLogger);
    }

    public void testStarTreeDocValues() throws IOException {
        final List<Supplier<Integer>> MAX_LEAF_DOC_VARIATIONS = List.of(
            () -> 1,
            () -> randomIntBetween(2, 100),
            () -> randomIntBetween(101, 10_000)
        );
        final List<DimensionFieldData> dimensionFieldDatum = List.of(
            new DimensionFieldData("sndv", () -> random().nextInt(10) - 5, DimensionTypes.INTEGER),
            new DimensionFieldData("dv", () -> random().nextInt(20) - 10, DimensionTypes.INTEGER),
            new DimensionFieldData("keyword_field", () -> random().nextInt(50), DimensionTypes.KEYWORD),
            new DimensionFieldData("long_field", () -> random().nextInt(50), DimensionTypes.LONG),
            new DimensionFieldData("half_float_field", () -> random().nextFloat(50), DimensionTypes.HALF_FLOAT),
            new DimensionFieldData("float_field", () -> random().nextFloat(50), DimensionTypes.FLOAT),
            new DimensionFieldData("double_field", () -> random().nextDouble(50), DimensionTypes.DOUBLE)
        );
        for (Supplier<Integer> maxLeafDocsSupplier : MAX_LEAF_DOC_VARIATIONS) {
            testStarTreeDocValuesInternal(
                getCodec(
                    maxLeafDocsSupplier,
                    dimensionFieldDatum.stream()
                        .collect(
                            Collectors.toMap(
                                df -> df.getDimension().getField(),
                                DimensionFieldData::getFieldType,
                                (v1, v2) -> v1,
                                LinkedHashMap::new
                            )
                        ),
                    StarTreeFilterTests.METRIC_TYPE_MAP
                ),
                dimensionFieldDatum
            );
        }
    }

    private void testStarTreeDocValuesInternal(Codec codec, List<DimensionFieldData> dimensionFieldData) throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(codec);
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);

        Random random = RandomizedTest.getRandom();
        int totalDocs = 100;
        int val;

        // Index 100 random documents
        for (int i = 0; i < totalDocs; i++) {
            Document doc = new Document();
            for (DimensionFieldData fieldData : dimensionFieldData) {
                // FIXME: Reduce the frequency of nulls to be with at least some non-null docs like after every 1-2 ?
                if (random.nextBoolean()) {
                    doc.add(fieldData.getField());
                }
            }
            if (random.nextBoolean()) {
                val = random.nextInt(50); // Random long between 0 and 49
                doc.add(new SortedNumericDocValuesField(FIELD_NAME, val));
            }
            iw.addDocument(doc);
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
        for (DimensionFieldData fieldData : dimensionFieldData) {
            when(mapperService.fieldType(fieldData.fieldName)).thenReturn(fieldData.getMappedField());
        }
        QueryShardContext queryShardContext = queryShardContextMock(
            indexSearcher,
            mapperService,
            createIndexSettings(),
            circuitBreakerService,
            new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), circuitBreakerService).withCircuitBreaking()
        );
        for (DimensionFieldData fieldData : dimensionFieldData) {
            when(mapperService.fieldType(fieldData.fieldName)).thenReturn(fieldData.getMappedField());
            when(queryShardContext.fieldMapper(fieldData.fieldName)).thenReturn(fieldData.getMappedField());
        }

        List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();
        CompositeIndexFieldInfo starTree = compositeIndexFields.get(0);

        SumAggregationBuilder sumAggregationBuilder = sum("_name").field(FIELD_NAME);
        MaxAggregationBuilder maxAggregationBuilder = max("_name").field(FIELD_NAME);
        MinAggregationBuilder minAggregationBuilder = min("_name").field(FIELD_NAME);
        ValueCountAggregationBuilder valueCountAggregationBuilder = count("_name").field(FIELD_NAME);
        AvgAggregationBuilder avgAggregationBuilder = avg("_name").field(FIELD_NAME);

        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions = dimensionFieldData.stream()
            .collect(
                Collectors.toMap(DimensionFieldData::getDimension, DimensionFieldData::getMappedField, (v1, v2) -> v1, LinkedHashMap::new)
            );

        Query query = null;
        QueryBuilder queryBuilder = null;

        for (int cases = 0; cases < 15; cases++) {
            // Get all types of queries (Term/Terms/Range) for all the given dimensions.
            List<QueryBuilder> allFieldQueries = dimensionFieldData.stream()
                .flatMap(x -> Stream.of(x.getTermQueryBuilder(), x.getTermsQueryBuilder(), x.getRangeQueryBuilder()))
                .toList();

            for (QueryBuilder qb : allFieldQueries) {
                query = qb.toQuery(queryShardContext);
                queryBuilder = qb;
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

        // Case when field and metric type in aggregation are fully supported by star tree.
        testCase(
            indexSearcher,
            query,
            queryBuilder,
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
            queryBuilder,
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
            queryBuilder,
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
            queryBuilder,
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
            queryBuilder,
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
            queryBuilder,
            sumAggregationBuilder,
            starTree,
            supportedDimensions,
            List.of(new Metric("hello", List.of(MetricStat.MAX, MetricStat.MIN, MetricStat.AVG))),
            verifyAggregation(InternalSum::getValue),
            mock(ValuesSourceAggregatorFactory.class),
            false
        );

        // Keyword Range query with missing Low Ordinal
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("keyword_field");
        rangeQueryBuilder.from(Long.MAX_VALUE).includeLower(random().nextBoolean());
        testCase(
            indexSearcher,
            rangeQueryBuilder.toQuery(queryShardContext),
            rangeQueryBuilder,
            sumAggregationBuilder,
            starTree,
            supportedDimensions,
            List.of(new Metric(FIELD_NAME, List.of(MetricStat.SUM, MetricStat.MAX, MetricStat.MIN, MetricStat.AVG))),
            verifyAggregation(InternalSum::getValue),
            null,
            true
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
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions,
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
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions, // FIXME : Merge with the same input that goes to generating the
        // codec.
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
        verify.accept(expectedAggregation, starTreeAggregation);
    }

    private interface DimensionFieldDataSupplier {
        IndexableField getField(String fieldName, Supplier<Object> valueSupplier);

        MappedFieldType getMappedField(String fieldName);

        Dimension getDimension(String fieldName);
    }

    private abstract static class NumericDimensionFieldDataSupplier implements DimensionFieldDataSupplier {

        @Override
        public Dimension getDimension(String fieldName) {
            return new NumericDimension(fieldName);
        }

        @Override
        public MappedFieldType getMappedField(String fieldName) {
            return new NumberFieldMapper.NumberFieldType(fieldName, numberType());
        }

        abstract NumberFieldMapper.NumberType numberType();
    }

    private static class DimensionFieldData {
        private final String fieldName;
        private final Supplier<Object> valueSupplier;
        private final DimensionFieldDataSupplier dimensionFieldDataSupplier;
        private final String fieldType;

        DimensionFieldData(String fieldName, Supplier<Object> valueSupplier, DimensionTypes dimensionType) {
            this.fieldName = fieldName;
            this.valueSupplier = valueSupplier;
            this.dimensionFieldDataSupplier = dimensionType.getFieldDataSupplier();
            this.fieldType = dimensionType.name().toLowerCase(Locale.ROOT);
        }

        public Dimension getDimension() {
            return dimensionFieldDataSupplier.getDimension(fieldName);
        }

        public MappedFieldType getMappedField() {
            return dimensionFieldDataSupplier.getMappedField(fieldName);
        }

        public IndexableField getField() {
            return dimensionFieldDataSupplier.getField(fieldName, valueSupplier);
        }

        public QueryBuilder getTermQueryBuilder() {
            return new TermQueryBuilder(fieldName, valueSupplier.get());
        }

        public QueryBuilder getTermsQueryBuilder() {
            int limit = randomIntBetween(1, 20);
            List<Object> values = new ArrayList<>(limit);
            for (int i = 0; i < limit; i++) {
                values.add(valueSupplier.get());
            }
            return new TermsQueryBuilder(fieldName, values);
        }

        public QueryBuilder getRangeQueryBuilder() {
            return new RangeQueryBuilder(fieldName).from(valueSupplier.get())
                .to(valueSupplier.get())
                .includeLower(randomBoolean())
                .includeUpper(randomBoolean());
        }

        public String getFieldType() {
            return fieldType;
        }
    }

    private enum DimensionTypes {

        INTEGER(new NumericDimensionFieldDataSupplier() {
            @Override
            NumberFieldMapper.NumberType numberType() {
                return NumberFieldMapper.NumberType.INTEGER;
            }

            @Override
            public IndexableField getField(String fieldName, Supplier<Object> valueSupplier) {
                return new IntField(fieldName, (Integer) valueSupplier.get(), Field.Store.YES);
            }
        }),
        LONG(new NumericDimensionFieldDataSupplier() {
            @Override
            NumberFieldMapper.NumberType numberType() {
                return NumberFieldMapper.NumberType.LONG;
            }

            @Override
            public IndexableField getField(String fieldName, Supplier<Object> valueSupplier) {
                return new LongField(fieldName, (Integer) valueSupplier.get(), Field.Store.YES);
            }
        }),
        HALF_FLOAT(new NumericDimensionFieldDataSupplier() {
            @Override
            public IndexableField getField(String fieldName, Supplier<Object> valueSupplier) {
                return new SortedNumericDocValuesField(fieldName, HalfFloatPoint.halfFloatToSortableShort((Float) valueSupplier.get()));
            }

            @Override
            NumberFieldMapper.NumberType numberType() {
                return NumberFieldMapper.NumberType.HALF_FLOAT;
            }
        }),
        FLOAT(new NumericDimensionFieldDataSupplier() {
            @Override
            public IndexableField getField(String fieldName, Supplier<Object> valueSupplier) {
                return new FloatField(fieldName, (Float) valueSupplier.get(), Field.Store.YES);
            }

            @Override
            NumberFieldMapper.NumberType numberType() {
                return NumberFieldMapper.NumberType.FLOAT;
            }
        }),
        DOUBLE(new NumericDimensionFieldDataSupplier() {
            @Override
            public IndexableField getField(String fieldName, Supplier<Object> valueSupplier) {
                return new DoubleField(fieldName, (Double) valueSupplier.get(), Field.Store.YES);
            }

            @Override
            NumberFieldMapper.NumberType numberType() {
                return NumberFieldMapper.NumberType.DOUBLE;
            }
        }),
        KEYWORD(new DimensionFieldDataSupplier() {
            @Override
            public IndexableField getField(String fieldName, Supplier<Object> valueSupplier) {
                return new KeywordField(fieldName, String.valueOf(valueSupplier.get()), Field.Store.YES);
            }

            @Override
            public MappedFieldType getMappedField(String fieldName) {
                return new KeywordFieldMapper.KeywordFieldType(fieldName, Lucene.STANDARD_ANALYZER);
            }

            @Override
            public Dimension getDimension(String fieldName) {
                return new OrdinalDimension(fieldName);
            }
        });

        private final DimensionFieldDataSupplier dimensionFieldDataSupplier;

        DimensionTypes(DimensionFieldDataSupplier dimensionFieldDataSupplier) {
            this.dimensionFieldDataSupplier = dimensionFieldDataSupplier;
        }

        public DimensionFieldDataSupplier getFieldDataSupplier() {
            return dimensionFieldDataSupplier;
        }

    }

}
