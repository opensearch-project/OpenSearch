/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.opensearch.index.mapper;

import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.IndexService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.index.mapper.FieldTypeTestCase.MOCK_QSC_DISALLOW_EXPENSIVE;
import static org.opensearch.index.mapper.KnnAlgorithmContext.Method.HNSW;
import static org.opensearch.index.mapper.KnnAlgorithmContextFactory.HNSW_PARAMETER_BEAM_WIDTH;
import static org.opensearch.index.mapper.KnnAlgorithmContextFactory.HNSW_PARAMETER_MAX_CONNECTIONS;

public class DenseVectorFieldTypeTests extends OpenSearchSingleNodeTestCase {
    private static final String ALGORITHM_HNSW = "HNSW";
    private static final String DENSE_VECTOR_TYPE_NAME = "dense_vector";
    private static final int DIMENSION = 2;
    private static final String FIELD_NAME = "field";
    private static final String METRIC_L2 = "L2";
    private static final float[] VECTOR = { 2.0f, 4.5f };

    private IndexService indexService;
    private DocumentMapperParser parser;
    private MappedFieldType fieldType;

    @Before
    public void setup() throws Exception {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();

        KnnAlgorithmContext knnMethodContext = new KnnAlgorithmContext(
            HNSW,
            Map.of(HNSW_PARAMETER_MAX_CONNECTIONS, 10, HNSW_PARAMETER_BEAM_WIDTH, 100)
        );
        KnnContext knnContext = new KnnContext(Metric.L2, knnMethodContext);
        fieldType = new DenseVectorFieldMapper.DenseVectorFieldType(FIELD_NAME, 1, knnContext);
    }

    public void testIndexingWithoutEnablingKnn() throws IOException {
        XContentBuilder mappingAllDefaults = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", DENSE_VECTOR_TYPE_NAME)
            .field("dimension", DIMENSION)
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        parser.parse("type", new CompressedXContent(Strings.toString(mappingAllDefaults))).parse(source(b -> b.field(FIELD_NAME, VECTOR)));
    }

    public void testIndexingWithDefaultParams() throws IOException {
        XContentBuilder mappingAllDefaults = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", DENSE_VECTOR_TYPE_NAME)
            .field("dimension", DIMENSION)
            .field("knn", Map.of())
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        parser.parse("type", new CompressedXContent(Strings.toString(mappingAllDefaults))).parse(source(b -> b.field(FIELD_NAME, VECTOR)));
    }

    public void testIndexingWithAlgorithmParameters() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", DENSE_VECTOR_TYPE_NAME)
            .field("dimension", DIMENSION)
            .field(
                "knn",
                Map.of(
                    "metric",
                    METRIC_L2,
                    "algorithm",
                    Map.of("name", ALGORITHM_HNSW, "parameters", Map.of("beam_width", 256, "max_connections", 16))
                )
            )
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        parser.parse("type", new CompressedXContent(Strings.toString(mapping)));
    }

    public void testCosineMetric() throws IOException {
        XContentBuilder mappingCosineMetric = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", DENSE_VECTOR_TYPE_NAME)
            .field("dimension", DIMENSION)
            .field("knn", Map.of("metric", "cosine", "algorithm", Map.of("name", ALGORITHM_HNSW)))
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        parser.parse("type", new CompressedXContent(Strings.toString(mappingCosineMetric))).parse(source(b -> b.field(FIELD_NAME, VECTOR)));
    }

    public void testDotProductMetric() throws IOException {
        XContentBuilder mappingDotProductMetric = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", DENSE_VECTOR_TYPE_NAME)
            .field("dimension", DIMENSION)
            .field("knn", Map.of("metric", "dot_product", "algorithm", Map.of("name", ALGORITHM_HNSW)))
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        parser.parse("type", new CompressedXContent(Strings.toString(mappingDotProductMetric)))
            .parse(source(b -> b.field(FIELD_NAME, VECTOR)));
    }

    public void testHNSWAlgorithmParametersInvalidInput() throws Exception {
        XContentBuilder mappingInvalidMaxConnections = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", DENSE_VECTOR_TYPE_NAME)
            .field("dimension", DIMENSION)
            .field(
                "knn",
                Map.of(
                    "metric",
                    METRIC_L2,
                    "algorithm",
                    Map.of("name", ALGORITHM_HNSW, "parameters", Map.of("beam_width", 256, "max_connections", 50))
                )
            )
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        final MapperParsingException mapperExceptionInvalidMaxConnections = expectThrows(
            MapperParsingException.class,
            () -> parser.parse("type", new CompressedXContent(Strings.toString(mappingInvalidMaxConnections)))
        );
        org.hamcrest.MatcherAssert.assertThat(
            mapperExceptionInvalidMaxConnections.getMessage(),
            containsString("max_connections value cannot be greater than")
        );

        XContentBuilder mappingInvalidBeamWidth = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", DENSE_VECTOR_TYPE_NAME)
            .field("dimension", DIMENSION)
            .field(
                "knn",
                Map.of(
                    "metric",
                    METRIC_L2,
                    "algorithm",
                    Map.of("name", ALGORITHM_HNSW, "parameters", Map.of("beam_width", 1024, "max_connections", 6))
                )
            )
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        final MapperParsingException mapperExceptionInvalidmBeamWidth = expectThrows(
            MapperParsingException.class,
            () -> parser.parse("type", new CompressedXContent(Strings.toString(mappingInvalidBeamWidth)))
        );
        org.hamcrest.MatcherAssert.assertThat(
            mapperExceptionInvalidmBeamWidth.getMessage(),
            containsString("beam_width value cannot be greater than")
        );

        XContentBuilder mappingUnsupportedParam = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", DENSE_VECTOR_TYPE_NAME)
            .field("dimension", DIMENSION)
            .field(
                "knn",
                Map.of(
                    "metric",
                    METRIC_L2,
                    "algorithm",
                    Map.of("name", ALGORITHM_HNSW, "parameters", Map.of("beam_width", 256, "max_connections", 6, "some_param", 23))
                )
            )
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        final IllegalArgumentException mapperExceptionUnsupportedParam = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(Strings.toString(mappingUnsupportedParam)))
        );
        assertEquals(mapperExceptionUnsupportedParam.getMessage(), "Algorithm parameter [some_param] is not supported");
    }

    public void testInvalidVectorDimension() throws Exception {
        XContentBuilder mappingMissingDimension = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", DENSE_VECTOR_TYPE_NAME)
            .field("knn", Map.of())
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        final MapperParsingException mapperExceptionMissingDimension = expectThrows(
            MapperParsingException.class,
            () -> parser.parse("type", new CompressedXContent(Strings.toString(mappingMissingDimension)))
        );
        org.hamcrest.MatcherAssert.assertThat(
            mapperExceptionMissingDimension.getMessage(),
            containsString("[dimension] property must be specified for field")
        );

        XContentBuilder mappingInvalidDimension = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", DENSE_VECTOR_TYPE_NAME)
            .field("dimension", 1200)
            .field("knn", Map.of())
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        final IllegalArgumentException exceptionInvalidDimension = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(Strings.toString(mappingInvalidDimension)))
        );
        org.hamcrest.MatcherAssert.assertThat(
            exceptionInvalidDimension.getMessage(),
            containsString("[dimension] value cannot be greater than 1024 for vector")
        );

        XContentBuilder mappingDimentionsMismatch = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", DENSE_VECTOR_TYPE_NAME)
            .field("dimension", DIMENSION)
            .field("knn", Map.of())
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        final MapperParsingException mapperExceptionIDimentionsMismatch = expectThrows(
            MapperParsingException.class,
            () -> parser.parse("type", new CompressedXContent(Strings.toString(mappingDimentionsMismatch)))
                .parse(source(b -> b.field(FIELD_NAME, new float[] { 2.0f, 4.5f, 5.6f })))
        );
        org.hamcrest.MatcherAssert.assertThat(
            mapperExceptionIDimentionsMismatch.getMessage(),
            containsString("failed to parse field [field] of type [dense_vector]")
        );
    }

    public void testInvalidMetric() throws Exception {
        XContentBuilder mappingInvalidMetric = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", DENSE_VECTOR_TYPE_NAME)
            .field("dimension", DIMENSION)
            .field("knn", Map.of("metric", "LAMBDA", "algorithm", Map.of("name", ALGORITHM_HNSW)))
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        final MapperParsingException mapperExceptionInvalidMetric = expectThrows(
            MapperParsingException.class,
            () -> parser.parse("type", new CompressedXContent(Strings.toString(mappingInvalidMetric)))
        );
        org.hamcrest.MatcherAssert.assertThat(
            mapperExceptionInvalidMetric.getMessage(),
            containsString("[metric] value [LAMBDA] is invalid")
        );
    }

    public void testInvalidAlgorithm() throws Exception {
        XContentBuilder mappingInvalidAlgorithm = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", DENSE_VECTOR_TYPE_NAME)
            .field("dimension", DIMENSION)
            .field("knn", Map.of("metric", METRIC_L2, "algorithm", Map.of("name", "MY_ALGORITHM")))
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        final MapperParsingException mapperExceptionInvalidAlgorithm = expectThrows(
            MapperParsingException.class,
            () -> parser.parse("type", new CompressedXContent(Strings.toString(mappingInvalidAlgorithm)))
        );
        assertEquals(mapperExceptionInvalidAlgorithm.getMessage(), "[algorithm name] value [MY_ALGORITHM] is invalid or not supported");
    }

    public void testInvalidParams() throws Exception {
        XContentBuilder mappingInvalidMaxConnections = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", DENSE_VECTOR_TYPE_NAME)
            .field("dimension", DIMENSION)
            .field("my_field", "some_value")
            .field("knn", Map.of())
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        final MapperParsingException mapperExceptionInvalidMaxConnections = expectThrows(
            MapperParsingException.class,
            () -> parser.parse("type", new CompressedXContent(Strings.toString(mappingInvalidMaxConnections)))
        );
        assertEquals(
            mapperExceptionInvalidMaxConnections.getMessage(),
            "unknown parameter [my_field] on mapper [field] of type [dense_vector]"
        );
    }

    public void testValueDisplay() {
        Object actualFloatArray = fieldType.valueForDisplay(VECTOR);
        assertTrue(actualFloatArray instanceof float[]);
        assertArrayEquals(VECTOR, (float[]) actualFloatArray, 0.0f);

        KnnContext knnContextDEfaultAlgorithmContext = new KnnContext(
            Metric.L2,
            KnnAlgorithmContextFactory.defaultContext(KnnAlgorithmContext.Method.HNSW)
        );
        MappedFieldType ftDefaultAlgorithmContext = new DenseVectorFieldMapper.DenseVectorFieldType(
            FIELD_NAME,
            1,
            knnContextDEfaultAlgorithmContext
        );
        Object actualFloatArrayDefaultAlgorithmContext = ftDefaultAlgorithmContext.valueForDisplay(VECTOR);
        assertTrue(actualFloatArrayDefaultAlgorithmContext instanceof float[]);
        assertArrayEquals(VECTOR, (float[]) actualFloatArrayDefaultAlgorithmContext, 0.0f);
    }

    public void testTermQueryNotSupported() {
        QueryShardContext context = Mockito.mock(QueryShardContext.class);
        QueryShardException exception = expectThrows(QueryShardException.class, () -> fieldType.termsQuery(Arrays.asList(VECTOR), context));
        assertEquals(exception.getMessage(), "Dense_vector does not support exact searching, use KNN queries instead [field]");
    }

    public void testPrefixQueryNotSupported() {
        QueryShardException ee = expectThrows(
            QueryShardException.class,
            () -> fieldType.prefixQuery("foo*", null, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "Can only use prefix queries on keyword, text and wildcard fields - not on [field] which is of type [dense_vector]",
            ee.getMessage()
        );
    }

    public void testRegexpQueryNotSupported() {
        QueryShardException ee = expectThrows(
            QueryShardException.class,
            () -> fieldType.regexpQuery("foo?", randomInt(10), 0, randomInt(10) + 1, null, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "Can only use regexp queries on keyword and text fields - not on [field] which is of type [dense_vector]",
            ee.getMessage()
        );
    }

    public void testWildcardQueryNotSupported() {
        QueryShardException ee = expectThrows(
            QueryShardException.class,
            () -> fieldType.wildcardQuery("valu*", null, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "Can only use wildcard queries on keyword, text and wildcard fields - not on [field] which is of type [dense_vector]",
            ee.getMessage()
        );
    }

    private final SourceToParse source(CheckedConsumer<XContentBuilder, IOException> build) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        build.accept(builder);
        builder.endObject();
        return new SourceToParse("test", "1", BytesReference.bytes(builder), XContentType.JSON);
    }
}
