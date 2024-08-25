/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite99.Composite99Codec;
import org.opensearch.index.codec.composite99.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
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
import org.opensearch.search.startree.OriginalOrStarTreeQuery;
import org.opensearch.search.startree.StarTreeQuery;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

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
            mapperService = StarTreeDocValuesFormatTests.createMapperService(StarTreeDocValuesFormatTests.getExpandedMapping());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite99Codec(Lucene99Codec.Mode.BEST_SPEED, mapperService, testLogger);
    }

    public void testStarTreeDocValues() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(getCodec());
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        doc.add(new SortedNumericDocValuesField("dv", 1));
        doc.add(new SortedNumericDocValuesField("field", 1));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        doc.add(new SortedNumericDocValuesField("dv", 2));
        doc.add(new SortedNumericDocValuesField("field", 3));
        iw.addDocument(doc);
        doc = new Document();
        iw.forceMerge(1);
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedNumericDocValuesField("dv", 4));
        doc.add(new SortedNumericDocValuesField("field", 6));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 3));
        doc.add(new SortedNumericDocValuesField("dv", 6));
        doc.add(new SortedNumericDocValuesField("field", 9));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();

        DirectoryReader ir = DirectoryReader.open(directory);
        initValuesSourceRegistry();
        assertEquals(ir.leaves().size(), 1);
        LeafReaderContext context = ir.leaves().get(0);

        SegmentReader reader = Lucene.segmentReader(context.reader());
        IndexSearcher indexSearcher = newSearcher(reader, true, true);
        CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();
        List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();

        CompositeIndexFieldInfo starTree = compositeIndexFields.get(0);

        SumAggregationBuilder sumAggregationBuilder = sum("_name").field(FIELD_NAME);
        MaxAggregationBuilder maxAggregationBuilder = max("_name").field(FIELD_NAME);
        MinAggregationBuilder minAggregationBuilder = min("_name").field(FIELD_NAME);
        ValueCountAggregationBuilder valueCountAggregationBuilder = count("_name").field(FIELD_NAME);
        AvgAggregationBuilder avgAggregationBuilder = avg("_name").field(FIELD_NAME);

        // match-all query
        Query defaultQeury = new MatchAllDocsQuery();
        StarTreeQuery starTreeQuery = new StarTreeQuery(starTree, null); // no predicates
        testCase(indexSearcher, defaultQeury, starTreeQuery, sumAggregationBuilder, verifyAggregation(InternalSum::getValue), 19);
        testCase(indexSearcher, defaultQeury, starTreeQuery, maxAggregationBuilder, verifyAggregation(InternalMax::getValue), 9);
        testCase(indexSearcher, defaultQeury, starTreeQuery, minAggregationBuilder, verifyAggregation(InternalMin::getValue), 1);
        testCase(
            indexSearcher,
            defaultQeury,
            starTreeQuery,
            valueCountAggregationBuilder,
            verifyAggregation(InternalValueCount::getValue),
            4
        );
        testCase(indexSearcher, defaultQeury, starTreeQuery, avgAggregationBuilder, verifyAggregation(InternalAvg::getValue), 4.75);

        // numeric-terms query
        defaultQeury = new TermQuery(new Term("sndv", "1"));
        Map<String, List<Predicate<Long>>> compositePredicateMap = new HashMap<>();
        compositePredicateMap.put("sndv", List.of(dimVal -> dimVal == 1));
        starTreeQuery = new StarTreeQuery(starTree, compositePredicateMap);
        testCase(indexSearcher, defaultQeury, starTreeQuery, sumAggregationBuilder, verifyAggregation(InternalSum::getValue), 4);
        testCase(indexSearcher, defaultQeury, starTreeQuery, maxAggregationBuilder, verifyAggregation(InternalMax::getValue), 3);
        testCase(indexSearcher, defaultQeury, starTreeQuery, minAggregationBuilder, verifyAggregation(InternalMin::getValue), 1);
        testCase(
            indexSearcher,
            defaultQeury,
            starTreeQuery,
            valueCountAggregationBuilder,
            verifyAggregation(InternalValueCount::getValue),
            2
        );
        testCase(indexSearcher, defaultQeury, starTreeQuery, avgAggregationBuilder, verifyAggregation(InternalAvg::getValue), 2);

        ir.close();
        directory.close();
    }

    private <T extends AggregationBuilder, V extends InternalAggregation, N extends Number> void testCase(
        IndexSearcher searcher,
        Query defaultQuery,
        StarTreeQuery starTreeQuery,
        T builder,
        BiConsumer<V, N> verify,
        N expectedValue
    ) throws IOException {
        OriginalOrStarTreeQuery originalOrStarTreeQuery = new OriginalOrStarTreeQuery(starTreeQuery, defaultQuery);
        V aggregation = searchAndReduceStarTree(
            createIndexSettings(),
            searcher,
            originalOrStarTreeQuery,
            builder,
            DEFAULT_MAX_BUCKETS,
            false,
            DEFAULT_MAPPED_FIELD
        );
        verify.accept(aggregation, expectedValue);
    }

    <T, R extends Number> BiConsumer<T, R> verifyAggregation(Function<T, R> valueExtractor) {
        return (aggregation, expectedValue) -> assertEquals(
            expectedValue.doubleValue(),
            valueExtractor.apply(aggregation).doubleValue(),
            0f
        );
    }
}
