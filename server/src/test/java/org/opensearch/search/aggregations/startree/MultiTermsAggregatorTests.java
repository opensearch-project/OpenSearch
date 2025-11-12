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
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
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
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite103.Composite103Codec;
import org.opensearch.index.codec.composite912.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.OrdinalDimension;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.bucket.terms.InternalMultiTerms;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.opensearch.index.codec.composite912.datacube.startree.AbstractStarTreeDVFormatTests.topMapping;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.MultiBucketConsumerService.DEFAULT_MAX_BUCKETS;

public class MultiTermsAggregatorTests extends AggregatorTestCase {

    private static final String KEYWORD_FIELD = "keyword";
    private static final String INT_FIELD = "int";
    private static final String FLOAT_FIELD = "float";
    private static final String METRIC_FIELD = "metric";

    private static final MappedFieldType KEYWORD_FIELD_TYPE = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD);
    private static final MappedFieldType INT_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(
        INT_FIELD,
        NumberFieldMapper.NumberType.INTEGER
    );
    private static final MappedFieldType FLOAT_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(
        FLOAT_FIELD,
        NumberFieldMapper.NumberType.FLOAT
    );
    private static final MappedFieldType METRIC_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(
        METRIC_FIELD,
        NumberFieldMapper.NumberType.DOUBLE
    );

    protected Codec getCodec() {
        final Logger testLogger = LogManager.getLogger(MultiTermsAggregatorTests.class);
        MapperService mapperService;
        try {
            mapperService = StarTreeDocValuesFormatTests.createMapperService(getMapping());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite103Codec(Lucene103Codec.Mode.BEST_SPEED, mapperService, testLogger);
    }

    public void testMultiTermsWithStarTree() throws IOException {
        // Setup index with star-tree codec
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(getCodec());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);

        // Index documents with values for our dimensions and metrics
        iw.addDocument(doc("a", 1, 10.0f, 100.0));
        iw.addDocument(doc("a", 1, 10.0f, 100.0));
        iw.addDocument(doc("a", 2, 20.0f, 200.0));
        iw.addDocument(doc("b", 1, 10.0f, 100.0));
        iw.addDocument(doc("b", 3, 30.0f, 300.0));
        iw.addDocument(doc("c", 2, 20.0f, 200.0));

        // Force merge to ensure the star-tree structure is built
        iw.forceMerge(1);
        iw.close();

        DirectoryReader ir = DirectoryReader.open(directory);
        LeafReaderContext context = ir.leaves().get(0);
        SegmentReader reader = Lucene.segmentReader(context.reader());
        IndexSearcher indexSearcher = newSearcher(wrapInMockESDirectoryReader(ir), false, false);

        // Extract the star-tree metadata from the segment
        CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();
        CompositeIndexFieldInfo starTree = starTreeDocValuesReader.getCompositeIndexFields().get(0);

        // Define the dimensions we expect our star-tree to have
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions = new LinkedHashMap<>();
        supportedDimensions.put(new OrdinalDimension(KEYWORD_FIELD), KEYWORD_FIELD_TYPE);
        supportedDimensions.put(new NumericDimension(INT_FIELD), INT_FIELD_TYPE);
        supportedDimensions.put(new NumericDimension(FLOAT_FIELD), FLOAT_FIELD_TYPE);
        supportedDimensions.put(new NumericDimension(METRIC_FIELD), METRIC_FIELD_TYPE);

        // Test Case 1: Simple 2-term aggregation
        testCase(
            indexSearcher,
            new MatchAllDocsQuery(),
            null,
            new MultiTermsAggregationBuilder("_name").terms(getSourceConfig(KEYWORD_FIELD, INT_FIELD)),
            starTree,
            supportedDimensions
        );

        // Test Case 2: 3-term aggregation
        testCase(
            indexSearcher,
            new MatchAllDocsQuery(),
            null,
            new MultiTermsAggregationBuilder("_name").terms(getSourceConfig(KEYWORD_FIELD, INT_FIELD, FLOAT_FIELD)),
            starTree,
            supportedDimensions
        );

        // Test Case 3: Aggregation with a query that filters on a dimension
        testCase(
            indexSearcher,
            SortedSetDocValuesField.newSlowExactQuery(KEYWORD_FIELD, newBytesRef("a")),
            new TermQueryBuilder(KEYWORD_FIELD, "a"),
            new MultiTermsAggregationBuilder("_name").terms(getSourceConfig(INT_FIELD, FLOAT_FIELD)),
            starTree,
            supportedDimensions
        );

        // Test Case 4: Aggregation with a sub-aggregation
        testCase(
            indexSearcher,
            new MatchAllDocsQuery(),
            null,
            new MultiTermsAggregationBuilder("_name").terms(getSourceConfig(KEYWORD_FIELD, FLOAT_FIELD))
                .subAggregation(max("max_metric").field(METRIC_FIELD)),
            starTree,
            supportedDimensions
        );

        ir.close();
        directory.close();
    }

    /**
     * Executes an aggregation twice: once with star-tree enabled and once with it disabled (default path),
     * then asserts that the results are identical.
     */
    private void testCase(
        IndexSearcher indexSearcher,
        Query query,
        QueryBuilder queryBuilder,
        MultiTermsAggregationBuilder aggregationBuilder,
        CompositeIndexFieldInfo starTree,
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions
    ) throws IOException {
        InternalMultiTerms starTreeAggregation = searchAndReduceStarTree(
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
            KEYWORD_FIELD_TYPE,
            INT_FIELD_TYPE,
            FLOAT_FIELD_TYPE,
            METRIC_FIELD_TYPE
        );

        InternalMultiTerms defaultAggregation = searchAndReduceStarTree(
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
            KEYWORD_FIELD_TYPE,
            INT_FIELD_TYPE,
            FLOAT_FIELD_TYPE,
            METRIC_FIELD_TYPE
        );

        assertEquals(defaultAggregation.getBuckets().size(), starTreeAggregation.getBuckets().size());
        assertEquals(defaultAggregation, starTreeAggregation);
    }

    private List<MultiTermsValuesSourceConfig> getSourceConfig(String... fieldNames) {
        return Arrays.stream(fieldNames).map(name -> new MultiTermsValuesSourceConfig.Builder().setFieldName(name).build()).toList();
    }

    private Document doc(String keyword, int integer, float f, double metric) {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef(keyword)));
        doc.add(new StringField(KEYWORD_FIELD, keyword, Field.Store.NO));
        doc.add(new SortedNumericDocValuesField(INT_FIELD, integer));
        doc.add(new SortedNumericDocValuesField(FLOAT_FIELD, NumericUtils.floatToSortableInt(f)));
        doc.add(new SortedNumericDocValuesField(METRIC_FIELD, NumericUtils.doubleToSortableLong(metric)));
        return doc;
    }

    private static XContentBuilder getMapping() throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("my_star_tree");
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("max_leaf_docs", 1);
            b.startArray("ordered_dimensions");
            b.startObject().field("name", KEYWORD_FIELD).endObject();
            b.startObject().field("name", INT_FIELD).endObject();
            b.startObject().field("name", FLOAT_FIELD).endObject();
            b.endArray();
            b.startArray("metrics");
            b.startObject();
            b.field("name", METRIC_FIELD);
            b.startArray("stats");
            b.value("max");
            b.value("value_count");
            b.endArray();
            b.endObject();
            b.endArray();
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject(KEYWORD_FIELD).field("type", "keyword").endObject();
            b.startObject(INT_FIELD).field("type", "integer").endObject();
            b.startObject(FLOAT_FIELD).field("type", "float").endObject();
            b.startObject(METRIC_FIELD).field("type", "double").endObject();
            b.endObject();
        });
    }
}
