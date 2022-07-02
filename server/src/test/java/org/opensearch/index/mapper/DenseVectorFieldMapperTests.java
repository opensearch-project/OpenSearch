/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.IndexableField;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.mapper.DenseVectorFieldMapper.DenseVectorFieldType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.index.mapper.KnnAlgorithmContext.Method.HNSW;
import static org.opensearch.index.mapper.KnnAlgorithmContextFactory.HNSW_PARAMETER_BEAM_WIDTH;
import static org.opensearch.index.mapper.KnnAlgorithmContextFactory.HNSW_PARAMETER_MAX_CONNECTIONS;

public class DenseVectorFieldMapperTests extends FieldMapperTestCase2<DenseVectorFieldMapper.Builder> {

    private static final float[] VECTOR = { 2.0f, 4.5f };

    public void testValueDisplay() {
        KnnAlgorithmContext knnMethodContext = new KnnAlgorithmContext(
            HNSW,
            Map.of(HNSW_PARAMETER_MAX_CONNECTIONS, 16, HNSW_PARAMETER_BEAM_WIDTH, 100)
        );
        KnnContext knnContext = new KnnContext(Metric.L2, knnMethodContext);
        MappedFieldType ft = new DenseVectorFieldType("field", 1, knnContext);
        Object actualFloatArray = ft.valueForDisplay(VECTOR);
        assertTrue(actualFloatArray instanceof float[]);
        assertArrayEquals(VECTOR, (float[]) actualFloatArray, 0.0f);
    }

    public void testSerializationWithoutKnn() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "dense_vector").field("dimension", 2)));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertTrue(fieldMapper instanceof DenseVectorFieldMapper);
        DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) fieldMapper;
        assertEquals(2, denseVectorFieldMapper.fieldType().getDimension());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", VECTOR)));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertTrue(fields[0] instanceof KnnVectorField);
        float[] actualVector = ((KnnVectorField) fields[0]).vectorValue();
        assertArrayEquals(VECTOR, actualVector, 0.0f);
    }

    public void testSerializationWithKnn() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dimension", 2)
                    .field(
                        "knn",
                        Map.of(
                            "metric",
                            "L2",
                            "algorithm",
                            Map.of("name", "HNSW", "parameters", Map.of("max_connections", 16, "beam_width", 100))
                        )
                    )
            )
        );

        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertTrue(fieldMapper instanceof DenseVectorFieldMapper);
        DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) fieldMapper;
        assertEquals(2, denseVectorFieldMapper.fieldType().getDimension());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", VECTOR)));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertTrue(fields[0] instanceof KnnVectorField);
        float[] actualVector = ((KnnVectorField) fields[0]).vectorValue();
        assertArrayEquals(VECTOR, actualVector, 0.0f);
    }

    @Override
    protected DenseVectorFieldMapper.Builder newBuilder() {
        return new DenseVectorFieldMapper.Builder("dense_vector");
    }

    public void testDeprecatedBoost() throws IOException {
        createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("boost", 2.0);
        }));
        String type = typeName();
        String[] warnings = new String[] {
            "Parameter [boost] on field [field] is deprecated and will be removed in 8.0",
            "Parameter [boost] has no effect on type [" + type + "] and will be removed in future" };
        allowedWarnings(warnings);
    }

    public void testIfMinimalSerializesToItself() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, ToXContent.EMPTY_PARAMS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, ToXContent.EMPTY_PARAMS);
        parsedFromOrig.endObject();
        assertEquals(Strings.toString(orig), Strings.toString(parsedFromOrig));
    }

    public void testForEmptyName() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
            b.startObject("");
            minimalMapping(b);
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    protected void writeFieldValue(XContentBuilder b) throws IOException {
        b.value(new float[] { 2.5f });
    }

    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "dense_vector");
        b.field("dimension", 1);
    }

    protected void registerParameters(MapperTestCase.ParameterChecker checker) throws IOException {}

    @Override
    protected Set<String> unsupportedProperties() {
        return org.opensearch.common.collect.Set.of("analyzer", "similarity", "doc_values", "store", "index");
    }

    protected String typeName() throws IOException {
        MapperService ms = createMapperService(fieldMapping(this::minimalMapping));
        return ms.fieldType("field").typeName();
    }

    @Override
    protected boolean supportsMeta() {
        return false;
    }

    public void testCosineMetric() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dimension", 2)
                    .field(
                        "knn",
                        Map.of(
                            "metric",
                            "cosine",
                            "algorithm",
                            Map.of("name", "HNSW", "parameters", Map.of("max_connections", 16, "beam_width", 100))
                        )
                    )
            )
        );

        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertTrue(fieldMapper instanceof DenseVectorFieldMapper);
        DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) fieldMapper;
        assertEquals(2, denseVectorFieldMapper.fieldType().getDimension());
    }

    public void testDotProductMetric() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dimension", 2)
                    .field(
                        "knn",
                        Map.of(
                            "metric",
                            "dot_product",
                            "algorithm",
                            Map.of("name", "HNSW", "parameters", Map.of("max_connections", 16, "beam_width", 100))
                        )
                    )
            )
        );

        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertTrue(fieldMapper instanceof DenseVectorFieldMapper);
        DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) fieldMapper;
        assertEquals(2, denseVectorFieldMapper.fieldType().getDimension());
    }

    public void testHNSWAlgorithmParametersInvalidInput() throws Exception {
        XContentBuilder mappingInvalidMaxConnections = fieldMapping(
            b -> b.field("type", "dense_vector")
                .field("dimension", 2)
                .field(
                    "knn",
                    Map.of(
                        "metric",
                        "dot_product",
                        "algorithm",
                        Map.of("name", "HNSW", "parameters", Map.of("max_connections", 256, "beam_width", 50))
                    )
                )
        );
        final MapperParsingException mapperExceptionInvalidMaxConnections = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(mappingInvalidMaxConnections)
        );
        assertEquals("max_connections value cannot be greater than 16", mapperExceptionInvalidMaxConnections.getRootCause().getMessage());

        XContentBuilder mappingInvalidBeamWidth = fieldMapping(
            b -> b.field("type", "dense_vector")
                .field("dimension", 2)
                .field(
                    "knn",
                    Map.of(
                        "metric",
                        "dot_product",
                        "algorithm",
                        Map.of("name", "HNSW", "parameters", Map.of("max_connections", 6, "beam_width", 1024))
                    )
                )
        );
        final MapperParsingException mapperExceptionInvalidmBeamWidth = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(mappingInvalidBeamWidth)
        );
        assertEquals("beam_width value cannot be greater than 512", mapperExceptionInvalidmBeamWidth.getRootCause().getMessage());

        XContentBuilder mappingUnsupportedParam = fieldMapping(
            b -> b.field("type", "dense_vector")
                .field("dimension", 2)
                .field(
                    "knn",
                    Map.of(
                        "metric",
                        "dot_product",
                        "algorithm",
                        Map.of("name", "HNSW", "parameters", Map.of("max_connections", 6, "beam_width", 256, "some_param", 23))
                    )
                )
        );
        final MapperParsingException mapperExceptionUnsupportedParam = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(mappingUnsupportedParam)
        );
        assertEquals("Algorithm parameter [some_param] is not supported", mapperExceptionUnsupportedParam.getRootCause().getMessage());
    }

    public void testInvalidMetric() throws Exception {
        XContentBuilder mappingInvalidMetric = fieldMapping(
            b -> b.field("type", "dense_vector")
                .field("dimension", 2)
                .field("knn", Map.of("metric", "LAMBDA", "algorithm", Map.of("name", "HNSW")))
        );
        final MapperParsingException mapperExceptionInvalidMetric = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(mappingInvalidMetric)
        );
        assertEquals("[metric] value [LAMBDA] is invalid", mapperExceptionInvalidMetric.getRootCause().getMessage());
    }

    public void testInvalidAlgorithm() throws Exception {
        XContentBuilder mappingInvalidAlgorithm = fieldMapping(
            b -> b.field("type", "dense_vector")
                .field("dimension", 2)
                .field("knn", Map.of("metric", "dot_product", "algorithm", Map.of("name", "MY_ALGORITHM")))
        );
        final MapperParsingException mapperExceptionInvalidAlgorithm = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(mappingInvalidAlgorithm)
        );
        assertEquals(
            "[algorithm name] value [MY_ALGORITHM] is invalid or not supported",
            mapperExceptionInvalidAlgorithm.getRootCause().getMessage()
        );
    }

    public void testInvalidParams() throws Exception {
        XContentBuilder mapping = fieldMapping(
            b -> b.field("type", "dense_vector").field("dimension", 2).field("my_field", "some_value").field("knn", Map.of())
        );
        final MapperParsingException mapperParsingException = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(mapping)
        );
        assertEquals(
            "Mapping definition for [field] has unsupported parameters:  [my_field : some_value]",
            mapperParsingException.getRootCause().getMessage()
        );
    }

    public void testExceedMaxNumberOfAlgorithmParams() throws Exception {
        Map<String, Integer> algorithmParams = new HashMap<>();
        IntStream.range(0, 100).forEach(number -> algorithmParams.put("param" + number, randomInt(Integer.MAX_VALUE)));
        XContentBuilder mapping = fieldMapping(
            b -> b.field("type", "dense_vector")
                .field("dimension", 2)
                .field("knn", Map.of("metric", "dot_product", "algorithm", Map.of("name", "HNSW", "parameters", algorithmParams)))
        );
        final MapperParsingException mapperParsingException = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(mapping)
        );
        assertEquals(
            "Invalid number of parameters for [algorithm], max allowed is [50] but given [100]",
            mapperParsingException.getRootCause().getMessage()
        );
    }

    public void testInvalidVectorNumberFormat() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dimension", 2)
                    .field(
                        "knn",
                        Map.of(
                            "metric",
                            "L2",
                            "algorithm",
                            Map.of("name", "HNSW", "parameters", Map.of("max_connections", 16, "beam_width", 100))
                        )
                    )
            )
        );
        final MapperParsingException mapperExceptionStringAsVectorValue = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("field", "some malicious script content")))
        );
        assertEquals(
            mapperExceptionStringAsVectorValue.getMessage(),
            "failed to parse field [field] of type [dense_vector] in document with id '1'. Preview of field's value: 'some malicious script content'"
        );

        final MapperParsingException mapperExceptionInfinityVectorValue = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("field", new Float[] { Float.POSITIVE_INFINITY })))
        );
        assertEquals(
            mapperExceptionInfinityVectorValue.getMessage(),
            "failed to parse field [field] of type [dense_vector] in document with id '1'. Preview of field's value: 'Infinity'"
        );

        final MapperParsingException mapperExceptionNullVectorValue = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("field", new Float[] { null })))
        );
        assertEquals(
            mapperExceptionNullVectorValue.getMessage(),
            "failed to parse field [field] of type [dense_vector] in document with id '1'. Preview of field's value: 'null'"
        );
    }

    public void testNullVectorValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dimension", 2)
                    .field(
                        "knn",
                        Map.of(
                            "metric",
                            "L2",
                            "algorithm",
                            Map.of("name", "HNSW", "parameters", Map.of("max_connections", 16, "beam_width", 100))
                        )
                    )
            )
        );
        mapper.parse(source(b -> b.field("field", (Float) null)));
        mapper.parse(source(b -> b.field("field", VECTOR)));
        mapper.parse(source(b -> b.field("field", (Float) null)));
    }
}
