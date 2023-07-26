/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index.mapper;

import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.FieldExistsQuery;
import org.junit.Assert;
import org.mockito.Mockito;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Explicit;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.plugin.correlation.core.index.CorrelationParamsContext;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for correlation vector field mapper
 */
public class CorrelationVectorFieldMapperTests extends OpenSearchTestCase {

    private static final String CORRELATION_VECTOR_TYPE = "correlation_vector";
    private static final String DIMENSION_FIELD_NAME = "dimension";
    private static final String TYPE_FIELD_NAME = "type";

    /**
     * test builder construction from parse of correlation params context
     * @throws IOException IOException
     */
    public void testBuilder_parse_fromCorrelationParamsContext() throws IOException {
        String fieldName = "test-field-name";
        String indexName = "test-index-name";
        Settings settings = Settings.builder().put(settings(Version.CURRENT).build()).build();

        VectorFieldMapper.TypeParser typeParser = new VectorFieldMapper.TypeParser();

        int efConstruction = 321;
        int m = 12;
        int dimension = 10;
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field(TYPE_FIELD_NAME, CORRELATION_VECTOR_TYPE)
            .field(DIMENSION_FIELD_NAME, dimension)
            .startObject("correlation_ctx")
            .field("similarityFunction", VectorSimilarityFunction.EUCLIDEAN.name())
            .startObject("parameters")
            .field("m", m)
            .field("ef_construction", efConstruction)
            .endObject()
            .endObject()
            .endObject();

        VectorFieldMapper.Builder builder = (VectorFieldMapper.Builder) typeParser.parse(
            fieldName,
            XContentHelper.convertToMap(BytesReference.bytes(xContentBuilder), true, xContentBuilder.contentType()).v2(),
            buildParserContext(indexName, settings)
        );
        Mapper.BuilderContext builderContext = new Mapper.BuilderContext(settings, new ContentPath());
        builder.build(builderContext);

        Assert.assertEquals(VectorSimilarityFunction.EUCLIDEAN, builder.correlationParamsContext.getValue().getSimilarityFunction());
        Assert.assertEquals(321, builder.correlationParamsContext.getValue().getParameters().get("ef_construction"));

        XContentBuilder xContentBuilderEmptyParams = XContentFactory.jsonBuilder()
            .startObject()
            .field(TYPE_FIELD_NAME, CORRELATION_VECTOR_TYPE)
            .field(DIMENSION_FIELD_NAME, dimension)
            .startObject("correlation_ctx")
            .field("similarityFunction", VectorSimilarityFunction.EUCLIDEAN.name())
            .endObject()
            .endObject();

        VectorFieldMapper.Builder builderEmptyParams = (VectorFieldMapper.Builder) typeParser.parse(
            fieldName,
            XContentHelper.convertToMap(BytesReference.bytes(xContentBuilderEmptyParams), true, xContentBuilderEmptyParams.contentType())
                .v2(),
            buildParserContext(indexName, settings)
        );

        Assert.assertEquals(
            VectorSimilarityFunction.EUCLIDEAN,
            builderEmptyParams.correlationParamsContext.getValue().getSimilarityFunction()
        );
        Assert.assertTrue(builderEmptyParams.correlationParamsContext.getValue().getParameters().isEmpty());
    }

    /**
     * test type parser construction throw error for invalid dimension of correlation vectors
     * @throws IOException IOException
     */
    public void testTypeParser_parse_fromCorrelationParamsContext_InvalidDimension() throws IOException {
        String fieldName = "test-field-name";
        String indexName = "test-index-name";
        Settings settings = Settings.builder().put(settings(Version.CURRENT).build()).build();

        VectorFieldMapper.TypeParser typeParser = new VectorFieldMapper.TypeParser();

        int efConstruction = 321;
        int m = 12;
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field(TYPE_FIELD_NAME, CORRELATION_VECTOR_TYPE)
            .field(DIMENSION_FIELD_NAME, 2000)
            .startObject("correlation_ctx")
            .field("similarityFunction", VectorSimilarityFunction.EUCLIDEAN.name())
            .startObject("parameters")
            .field("m", m)
            .field("ef_construction", efConstruction)
            .endObject()
            .endObject()
            .endObject();

        VectorFieldMapper.Builder builder = (VectorFieldMapper.Builder) typeParser.parse(
            fieldName,
            XContentHelper.convertToMap(BytesReference.bytes(xContentBuilder), true, xContentBuilder.contentType()).v2(),
            buildParserContext(indexName, settings)
        );

        expectThrows(IllegalArgumentException.class, () -> builder.build(new Mapper.BuilderContext(settings, new ContentPath())));
    }

    /**
     * test type parser construction error for invalid vector similarity function
     * @throws IOException IOException
     */
    public void testTypeParser_parse_fromCorrelationParamsContext_InvalidVectorSimilarityFunction() throws IOException {
        String fieldName = "test-field-name";
        String indexName = "test-index-name";
        Settings settings = Settings.builder().put(settings(Version.CURRENT).build()).build();

        VectorFieldMapper.TypeParser typeParser = new VectorFieldMapper.TypeParser();

        int efConstruction = 321;
        int m = 12;
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field(TYPE_FIELD_NAME, CORRELATION_VECTOR_TYPE)
            .field(DIMENSION_FIELD_NAME, 2000)
            .startObject("correlation_ctx")
            .field("similarityFunction", "invalid")
            .startObject("parameters")
            .field("m", m)
            .field("ef_construction", efConstruction)
            .endObject()
            .endObject()
            .endObject();

        expectThrows(
            MapperParsingException.class,
            () -> typeParser.parse(
                fieldName,
                XContentHelper.convertToMap(BytesReference.bytes(xContentBuilder), true, xContentBuilder.contentType()).v2(),
                buildParserContext(indexName, settings)
            )
        );
    }

    /**
     * test parseCreateField in CorrelationVectorFieldMapper
     * @throws IOException
     */
    public void testCorrelationVectorFieldMapper_parseCreateField() throws IOException {
        String fieldName = "test-field-name";
        int dimension = 10;
        float[] testVector = createInitializedFloatArray(dimension, 1.0f);
        CorrelationParamsContext correlationParamsContext = new CorrelationParamsContext(VectorSimilarityFunction.EUCLIDEAN, Map.of());

        VectorFieldMapper.CorrelationVectorFieldType correlationVectorFieldType = new VectorFieldMapper.CorrelationVectorFieldType(
            fieldName,
            Map.of(),
            dimension,
            correlationParamsContext
        );

        CorrelationVectorFieldMapper.CreateLuceneFieldMapperInput input = new CorrelationVectorFieldMapper.CreateLuceneFieldMapperInput(
            fieldName,
            correlationVectorFieldType,
            FieldMapper.MultiFields.empty(),
            FieldMapper.CopyTo.empty(),
            new Explicit<>(true, true),
            false,
            false,
            correlationParamsContext
        );

        ParseContext.Document document = new ParseContext.Document();
        ContentPath contentPath = new ContentPath();
        ParseContext parseContext = mock(ParseContext.class);
        when(parseContext.doc()).thenReturn(document);
        when(parseContext.path()).thenReturn(contentPath);

        CorrelationVectorFieldMapper correlationVectorFieldMapper = Mockito.spy(new CorrelationVectorFieldMapper(input));
        doReturn(Optional.of(testVector)).when(correlationVectorFieldMapper).getFloatsFromContext(parseContext, dimension);

        correlationVectorFieldMapper.parseCreateField(parseContext, dimension);

        List<IndexableField> fields = document.getFields();
        assertEquals(1, fields.size());
        IndexableField field = fields.get(0);

        Assert.assertTrue(field instanceof KnnFloatVectorField);
        KnnFloatVectorField knnFloatVectorField = (KnnFloatVectorField) field;
        Assert.assertArrayEquals(testVector, knnFloatVectorField.vectorValue(), 0.001f);
    }

    /**
     * test CorrelationVectorFieldType subclass
     */
    public void testCorrelationVectorFieldType() {
        String fieldName = "test-field-name";
        int dimension = 10;
        QueryShardContext context = mock(QueryShardContext.class);
        SearchLookup searchLookup = mock(SearchLookup.class);

        VectorFieldMapper.CorrelationVectorFieldType correlationVectorFieldType = new VectorFieldMapper.CorrelationVectorFieldType(
            fieldName,
            Map.of(),
            dimension
        );
        Assert.assertThrows(QueryShardException.class, () -> { correlationVectorFieldType.termQuery(new Object(), context); });
        Assert.assertThrows(
            UnsupportedOperationException.class,
            () -> { correlationVectorFieldType.valueFetcher(context, searchLookup, ""); }
        );
        Assert.assertTrue(correlationVectorFieldType.existsQuery(context) instanceof FieldExistsQuery);
        Assert.assertEquals(VectorFieldMapper.CONTENT_TYPE, correlationVectorFieldType.typeName());
    }

    /**
     * test constants in VectorFieldMapper
     */
    public void testVectorFieldMapperConstants() {
        Assert.assertNotNull(VectorFieldMapper.Defaults.IGNORE_MALFORMED);
        Assert.assertNotNull(VectorFieldMapper.Names.IGNORE_MALFORMED);
    }

    private IndexMetadata buildIndexMetaData(String index, Settings settings) {
        return IndexMetadata.builder(index)
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .version(7)
            .mappingVersion(0)
            .settingsVersion(0)
            .aliasesVersion(0)
            .creationDate(0)
            .build();
    }

    private Mapper.TypeParser.ParserContext buildParserContext(String index, Settings settings) {
        IndexSettings indexSettings = new IndexSettings(
            buildIndexMetaData(index, settings),
            Settings.EMPTY,
            new IndexScopedSettings(Settings.EMPTY, new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS))
        );

        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);

        return new Mapper.TypeParser.ParserContext(
            null,
            mapperService,
            type -> new VectorFieldMapper.TypeParser(),
            Version.CURRENT,
            null,
            null,
            null
        );
    }

    private static float[] createInitializedFloatArray(int dimension, float value) {
        float[] array = new float[dimension];
        Arrays.fill(array, value);
        return array;
    }
}
