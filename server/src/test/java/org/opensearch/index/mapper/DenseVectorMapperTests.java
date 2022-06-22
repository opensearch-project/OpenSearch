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

import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.mapper.DenseVectorFieldMapper.DenseVectorFieldType;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.index.mapper.KnnAlgorithmContext.Method.HNSW;
import static org.opensearch.index.mapper.KnnAlgorithmContextFactory.HNSW_PARAMETER_BEAM_WIDTH;
import static org.opensearch.index.mapper.KnnAlgorithmContextFactory.HNSW_PARAMETER_MAX_CONNECTIONS;

public class DenseVectorMapperTests extends MapperServiceTestCase {

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
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper mapper = defaultMapper.mappers().getMapper("field");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\"field\":{\"type\":\"dense_vector\",\"dimension\":2}}", Strings.toString(builder));
    }

    public void testSerializationWithKnn() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("knn", Map.of());
        }));
        Mapper mapper = defaultMapper.mappers().getMapper("field");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertTrue(
            Set.of(
                "{\"field\":{\"type\":\"dense_vector\","
                    + "\"dimension\":2,"
                    + "\"knn\":"
                    + "{\"metric\":\"L2\","
                    + "\"algorithm\":{"
                    + "\"name\":\"HNSW\","
                    + "\"parameters\":{\"beam_width\":100,\"max_connections\":16}}}}}",
                "{\"field\":{\"type\":\"dense_vector\","
                    + "\"dimension\":2,"
                    + "\"knn\":"
                    + "{\"metric\":\"L2\","
                    + "\"algorithm\":{"
                    + "\"name\":\"HNSW\","
                    + "\"parameters\":{\"max_connections\":16,\"beam_width\":100}}}}}"
            ).contains(Strings.toString(builder))
        );
    }

    public void testMinimalToMaximal() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, INCLUDE_DEFAULTS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, INCLUDE_DEFAULTS);
        parsedFromOrig.endObject();
        assertEquals(Strings.toString(orig), Strings.toString(parsedFromOrig));
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
        b.field("dimension", 2);
        // b.field("knn", Map.of());
    }

    protected void registerParameters(MapperTestCase.ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", false));
    }

    protected String typeName() throws IOException {
        MapperService ms = createMapperService(fieldMapping(this::minimalMapping));
        return ms.fieldType("field").typeName();
    }
}
