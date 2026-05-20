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

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.mapper;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.index.mapper.ObjectMapper.Dynamic;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.Script;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.mockito.Mockito;

import static org.opensearch.index.mapper.ObjectMapper.Nested.isParent;
import static org.hamcrest.Matchers.containsString;

public class ObjectMapperTests extends OpenSearchSingleNodeTestCase {
    public void testDifferentInnerObjectTokenFailure() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();

        DocumentMapper defaultMapper = createIndex("test").mapperService()
            .documentMapperParser()
            .parse("type", new CompressedXContent(mapping));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            defaultMapper.parse(
                new SourceToParse(
                    "test",
                    "1",
                    new BytesArray(
                        " {\n"
                            + "      \"object\": {\n"
                            + "        \"array\":[\n"
                            + "        {\n"
                            + "          \"object\": { \"value\": \"value\" }\n"
                            + "        },\n"
                            + "        {\n"
                            + "          \"object\":\"value\"\n"
                            + "        }\n"
                            + "        ]\n"
                            + "      },\n"
                            + "      \"value\":\"value\"\n"
                            + "    }"
                    ),
                    MediaTypeRegistry.JSON
                )
            );
        });
        assertTrue(e.getMessage(), e.getMessage().contains("cannot be changed from type"));
    }

    public void testEmptyArrayProperties() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startArray("properties")
            .endArray()
            .endObject()
            .endObject()
            .toString();
        createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
    }

    public void testEmptyFieldsArrayMultiFields() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("tweet")
            .startObject("properties")
            .startObject("name")
            .field("type", "text")
            .startArray("fields")
            .endArray()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        createIndex("test").mapperService().documentMapperParser().parse("tweet", new CompressedXContent(mapping));
    }

    public void testFieldsArrayMultiFieldsShouldThrowException() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("tweet")
            .startObject("properties")
            .startObject("name")
            .field("type", "text")
            .startArray("fields")
            .startObject()
            .field("test", "string")
            .endObject()
            .startObject()
            .field("test2", "string")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        try {
            createIndex("test").mapperService().documentMapperParser().parse("tweet", new CompressedXContent(mapping));
            fail("Expected MapperParsingException");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), containsString("expected map for property [fields]"));
            assertThat(e.getMessage(), containsString("but got a class java.util.ArrayList"));
        }
    }

    public void testEmptyFieldsArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("tweet")
            .startObject("properties")
            .startArray("fields")
            .endArray()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        createIndex("test").mapperService().documentMapperParser().parse("tweet", new CompressedXContent(mapping));
    }

    public void testFieldsWithFilledArrayShouldThrowException() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("tweet")
            .startObject("properties")
            .startArray("fields")
            .startObject()
            .field("test", "string")
            .endObject()
            .startObject()
            .field("test2", "string")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        try {
            createIndex("test").mapperService().documentMapperParser().parse("tweet", new CompressedXContent(mapping));
            fail("Expected MapperParsingException");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), containsString("Expected map for property [fields]"));
        }
    }

    public void testDotAsFieldName() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(".")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .toString();

        try {
            createIndex("test").mapperService().documentMapperParser().parse("tweet", new CompressedXContent(mapping));
            fail("Expected MapperParsingException");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), containsString("Invalid field name"));
        }
    }

    public void testFieldPropertiesArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("tweet")
            .startObject("properties")
            .startObject("name")
            .field("type", "text")
            .startObject("fields")
            .startObject("raw")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        createIndex("test").mapperService().documentMapperParser().parse("tweet", new CompressedXContent(mapping));
    }

    public void testMerge() throws IOException {
        MergeReason reason = randomFrom(MergeReason.values());
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject("foo")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), reason);
        assertNull(mapper.root().dynamic());
        String update = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("dynamic", "strict")
            .endObject()
            .endObject()
            .toString();
        mapper = mapperService.merge("type", new CompressedXContent(update), reason);
        assertEquals(Dynamic.STRICT, mapper.root().dynamic());
    }

    public void testMergeEnabledForIndexTemplates() throws IOException {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("object")
            .field("type", "object")
            .field("enabled", false)
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), MergeReason.INDEX_TEMPLATE);
        assertNull(mapper.root().dynamic());

        // If we don't explicitly set 'enabled', then the mapping should not change.
        String update = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("object")
            .field("type", "object")
            .field("dynamic", false)
            .endObject()
            .endObject()
            .endObject()
            .toString();
        mapper = mapperService.merge("type", new CompressedXContent(update), MergeReason.INDEX_TEMPLATE);

        ObjectMapper objectMapper = mapper.objectMappers().get("object");
        assertNotNull(objectMapper);
        assertFalse(objectMapper.isEnabled());

        // Setting 'enabled' to true is allowed, and updates the mapping.
        update = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("object")
            .field("type", "object")
            .field("enabled", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();
        mapper = mapperService.merge("type", new CompressedXContent(update), MergeReason.INDEX_TEMPLATE);

        objectMapper = mapper.objectMappers().get("object");
        assertNotNull(objectMapper);
        assertTrue(objectMapper.isEnabled());
    }

    public void testFieldReplacementForIndexTemplates() throws IOException {
        MapperService mapperService = createIndex("test").mapperService();
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("object")
            .startObject("properties")
            .startObject("field1")
            .field("type", "keyword")
            .endObject()
            .startObject("field2")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MergeReason.INDEX_TEMPLATE);

        String update = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("object")
            .startObject("properties")
            .startObject("field2")
            .field("type", "integer")
            .endObject()
            .startObject("field3")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        DocumentMapper mapper = mapperService.merge(
            MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent(update),
            MergeReason.INDEX_TEMPLATE
        );

        String expected = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .startObject("properties")
            .startObject("object")
            .startObject("properties")
            .startObject("field1")
            .field("type", "keyword")
            .endObject()
            .startObject("field2")
            .field("type", "integer")
            .endObject()
            .startObject("field3")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertEquals(expected, mapper.mappingSource().toString());
    }

    public void testDisallowFieldReplacementForIndexTemplates() throws IOException {
        MapperService mapperService = createIndex("test").mapperService();
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("object")
            .startObject("properties")
            .startObject("field1")
            .field("type", "object")
            .endObject()
            .startObject("field2")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MergeReason.INDEX_TEMPLATE);

        String firstUpdate = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("object")
            .startObject("properties")
            .startObject("field2")
            .field("type", "nested")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(firstUpdate), MergeReason.INDEX_TEMPLATE)
        );
        assertThat(e.getMessage(), containsString("can't merge a non object mapping [object.field2] with an object mapping"));

        String secondUpdate = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("object")
            .startObject("properties")
            .startObject("field1")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        e = expectThrows(
            IllegalArgumentException.class,
            () -> mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(secondUpdate), MergeReason.INDEX_TEMPLATE)
        );
        assertThat(e.getMessage(), containsString("can't merge a non object mapping [object.field1] with an object mapping"));
    }

    public void testEmptyName() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("")
            .startObject("properties")
            .startObject("name")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        // Empty name not allowed in index created after 5.0
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            createIndex("test").mapperService().documentMapperParser().parse("", new CompressedXContent(mapping));
        });
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    public void testDerivedFields() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("tweet")
            .startObject("derived")
            .startObject("derived_field_name1")
            .field("type", "boolean")
            .endObject()
            .startObject("derived_field_name2")
            .field("type", "keyword")
            .startObject("script")
            .field("source", "doc['test'].value")
            .endObject()
            .endObject()
            .endObject()
            .startObject("properties")
            .startObject("field_name")
            .field("type", "date")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        DocumentMapper documentMapper = createIndex("test").mapperService()
            .documentMapperParser()
            .parse("tweet", new CompressedXContent(mapping));

        Mapper mapper = documentMapper.root().getMapper("derived_field_name1");
        assertTrue(mapper instanceof DerivedFieldMapper);
        DerivedFieldMapper derivedFieldMapper = (DerivedFieldMapper) mapper;
        assertEquals("boolean", derivedFieldMapper.getType());
        assertNull(derivedFieldMapper.getScript());

        mapper = documentMapper.root().getMapper("derived_field_name2");
        assertTrue(mapper instanceof DerivedFieldMapper);
        derivedFieldMapper = (DerivedFieldMapper) mapper;
        assertEquals("keyword", derivedFieldMapper.getType());
        assertEquals(Script.parse("doc['test'].value"), derivedFieldMapper.getScript());

        // Check that field in properties was parsed correctly as well
        mapper = documentMapper.root().getMapper("field_name");
        assertNotNull(mapper);
        assertEquals("date", mapper.typeName());
    }

    public void testCompositeFields() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("tweet")
            .startObject("composite")
            .startObject("startree")
            .field("type", "star_tree")
            .startObject("config")
            .startObject("date_dimension")
            .field("name", "@timestamp")
            .endObject()
            .startArray("ordered_dimensions")
            .startObject()
            .field("name", "status")
            .endObject()
            .endArray()
            .startArray("metrics")
            .startObject()
            .field("name", "status")
            .endObject()
            .startObject()
            .field("name", "metric_field")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject()
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject("status")
            .field("type", "integer")
            .endObject()
            .startObject("metric_field")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        Settings settings = Settings.builder()
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .put(StarTreeIndexSettings.STAR_TREE_SEARCH_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
            .build();

        DocumentMapper documentMapper = createIndex("test", settings).mapperService()
            .documentMapperParser()
            .parse("tweet", new CompressedXContent(mapping));

        Mapper mapper = documentMapper.root().getMapper("startree");
        assertTrue(mapper instanceof StarTreeMapper);
        StarTreeMapper starTreeMapper = (StarTreeMapper) mapper;
        assertEquals("star_tree", starTreeMapper.fieldType().typeName());
        // Check that field in properties was parsed correctly as well
        mapper = documentMapper.root().getMapper("@timestamp");
        assertNotNull(mapper);
        assertEquals("date", mapper.typeName());
    }

    public void testNestedIsParent() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("a")
            .field("type", "nested")
            .startObject("properties")
            .field("b1", Collections.singletonMap("type", "keyword"))
            .startObject("b2")
            .field("type", "nested")
            .startObject("properties")
            .startObject("c")
            .field("type", "nested")
            .startObject("properties")
            .field("d", Collections.singletonMap("type", "keyword"))
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        DocumentMapper documentMapper = createIndex("test").mapperService()
            .documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        MapperService mapperService = Mockito.mock(MapperService.class);
        Mockito.when(mapperService.getObjectMapper(("a"))).thenReturn(documentMapper.objectMappers().get("a"));
        Mockito.when(mapperService.getObjectMapper(("a.b2"))).thenReturn(documentMapper.objectMappers().get("a.b2"));
        Mockito.when(mapperService.getObjectMapper(("a.b2.c"))).thenReturn(documentMapper.objectMappers().get("a.b2.c"));

        assertTrue(isParent(documentMapper.objectMappers().get("a"), documentMapper.objectMappers().get("a.b2.c"), mapperService));
        assertTrue(isParent(documentMapper.objectMappers().get("a"), documentMapper.objectMappers().get("a.b2"), mapperService));
        assertTrue(isParent(documentMapper.objectMappers().get("a.b2"), documentMapper.objectMappers().get("a.b2.c"), mapperService));

        assertFalse(isParent(documentMapper.objectMappers().get("a.b2.c"), documentMapper.objectMappers().get("a"), mapperService));
        assertFalse(isParent(documentMapper.objectMappers().get("a.b2"), documentMapper.objectMappers().get("a"), mapperService));
        assertFalse(isParent(documentMapper.objectMappers().get("a.b2.c"), documentMapper.objectMappers().get("a.b2"), mapperService));
    }

    public void testDeriveSourceMapperValidation() throws IOException {
        // Test 1: Validate basic derive source mapping
        String basicMapping = """
            {
                "properties": {
                    "numeric_field": {
                        "type": "long"
                    },
                    "keyword_field": {
                        "type": "keyword"
                    }
                }
            }""";

        // Should succeed with derive source enabled and doc values enabled (default)
        DocumentMapperParser basicMapperParser = createIndex(
            "test_derive_1",
            Settings.builder().put("index.derived_source.enabled", true).build()
        ).mapperService().documentMapperParser();

        // This should go through without issue
        basicMapperParser.parse("type", new CompressedXContent(basicMapping));

        // Test 2: Validate mapping with doc values disabled
        String docValuesDisabledMapping = """
            {
                "properties": {
                    "numeric_field": {
                        "type": "long",
                        "doc_values": false
                    }
                }
            }""";

        final DocumentMapperParser docValuesDisabledMapperParser = createIndex(
            "test_derive_2",
            Settings.builder().put("index.derived_source.enabled", true).build()
        ).mapperService().documentMapperParser();

        // Should fail because doc values and stored are both disabled
        expectThrows(
            UnsupportedOperationException.class,
            () -> docValuesDisabledMapperParser.parse("type", new CompressedXContent(docValuesDisabledMapping))
        );

        // Test 3: Validate mapping with stored enabled but doc values disabled
        String storedEnabledMapping = """
            {
                "properties": {
                    "numeric_field": {
                        "type": "long",
                        "doc_values": false,
                        "store": true
                    }
                }
            }""";

        // Should succeed because stored is enabled
        DocumentMapperParser storedEnabledMapperParser = createIndex(
            "test_derive_3",
            Settings.builder().put("index.derived_source.enabled", true).build()
        ).mapperService().documentMapperParser();
        storedEnabledMapperParser.parse("type", new CompressedXContent(storedEnabledMapping));

        // Test 4: Validate keyword field with normalizer
        String normalizerMapping = """
            {
                "properties": {
                    "keyword_field": {
                        "type": "keyword",
                        "normalizer": "lowercase"
                    }
                }
            }""";

        // Should fail because normalizer is not supported with derive source
        DocumentMapperParser normalizerMapperParser = createIndex(
            "test_derive_4",
            Settings.builder()
                .put("index.derived_source.enabled", true)
                .put("analysis.normalizer.lowercase.type", "custom")
                .putList("analysis.normalizer.lowercase.filter", "lowercase")
                .build()
        ).mapperService().documentMapperParser();
        expectThrows(
            UnsupportedOperationException.class,
            () -> normalizerMapperParser.parse("type", new CompressedXContent(normalizerMapping))
        );

        // Test 5: Validate keyword field with ignore_above
        String ignoreAboveMapping = """
            {
                "properties": {
                    "keyword_field": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            }""";

        // Should fail because ignore_above is not supported with derive source
        DocumentMapperParser ignoreAboveMapperParser = createIndex(
            "test_derive_5",
            Settings.builder().put("index.derived_source.enabled", true).build()
        ).mapperService().documentMapperParser();
        expectThrows(
            UnsupportedOperationException.class,
            () -> ignoreAboveMapperParser.parse("type", new CompressedXContent(ignoreAboveMapping))
        );

        // Test 6: Validate object field with nested enabled
        String nestedMapping = """
            {
                "properties": {
                    "nested_field": {
                        "type": "nested",
                        "properties": {
                            "inner_field": {
                                "type": "keyword"
                            }
                        }
                    }
                }
            }""";

        // Should fail because nested fields are not supported with derive source
        DocumentMapperParser nestedMapperParser = createIndex(
            "test_derive_6",
            Settings.builder().put("index.derived_source.enabled", true).build()
        ).mapperService().documentMapperParser();
        expectThrows(UnsupportedOperationException.class, () -> nestedMapperParser.parse("type", new CompressedXContent(nestedMapping)));

        // Test 7: Validate field with copy_to
        String copyToMapping = """
            {
                "properties": {
                    "field1": {
                        "type": "keyword",
                        "copy_to": "field2"
                    },
                    "field2": {
                        "type": "keyword"
                    }
                }
            }""";

        // Should fail because copy_to is not supported with derive source
        DocumentMapperParser copyToMapperParser = createIndex(
            "test_derive_7",
            Settings.builder().put("index.derived_source.enabled", true).build()
        ).mapperService().documentMapperParser();
        expectThrows(UnsupportedOperationException.class, () -> copyToMapperParser.parse("type", new CompressedXContent(copyToMapping)));

        // Test 8: Validate multiple field types
        String multiTypeMapping = """
            {
                "properties": {
                    "keyword_field": {
                        "type": "keyword"
                    },
                    "numeric_field": {
                        "type": "long"
                    },
                    "date_field": {
                        "type": "date"
                    },
                    "date_nanos_field": {
                        "type": "date_nanos"
                    },
                    "boolean_field": {
                        "type": "boolean"
                    },
                    "ip_field": {
                        "type": "ip"
                    },
                    "constant_keyword": {
                        "type": "constant_keyword",
                        "value": "1"
                    },
                    "geo_point_field": {
                        "type": "geo_point"
                    },
                    "text_field": {
                        "type": "text",
                        "store": true
                    },
                    "wildcard_field": {
                        "type": "wildcard",
                        "doc_values": true
                    }
                }
            }""";

        // Should succeed because all field types support derive source
        DocumentMapperParser multiTypeMapperParser = createIndex(
            "test_derive_8",
            Settings.builder().put("index.derived_source.enabled", true).build()
        ).mapperService().documentMapperParser();
        multiTypeMapperParser.parse("type", new CompressedXContent(multiTypeMapping));

        // Test 9: Validate with both doc_values and stored disabled
        String bothDisabledMapping = """
            {
                "properties": {
                    "keyword_field": {
                        "type": "keyword",
                        "doc_values": false,
                        "store": false
                    }
                }
            }""";

        // Should fail because both doc_values and stored are disabled
        DocumentMapperParser bothDisabledMapperParser = createIndex(
            "test_derive_9",
            Settings.builder().put("index.derived_source.enabled", true).build()
        ).mapperService().documentMapperParser();
        expectThrows(
            UnsupportedOperationException.class,
            () -> bothDisabledMapperParser.parse("type", new CompressedXContent(bothDisabledMapping))
        );

        // Test 10: Validate for the field type, for which derived source is not implemented
        String unsupportedFieldType = """
            {
                "properties": {
                    "geo_shape_field": {
                        "type": "geo_shape",
                        "doc_values": true
                    }
                }
            }""";

        // Should fail because for geo_shape, derived source feature is not implemented for it
        DocumentMapperParser unsupportedFieldMapperParser = createIndex(
            "test_derive_10",
            Settings.builder().put("index.derived_source.enabled", true).build()
        ).mapperService().documentMapperParser();
        expectThrows(
            MapperParsingException.class,
            () -> unsupportedFieldMapperParser.parse("type", new CompressedXContent(unsupportedFieldType))
        );
    }

    public void testValidateDisableObjectsImmutability() throws Exception {
        // Test that disable_objects cannot be changed after index creation
        String initialMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("metrics")
            .field("type", "object")
            .field("disable_objects", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();

        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("_doc", new CompressedXContent(initialMapping), MergeReason.MAPPING_UPDATE);

        // Try to change disable_objects from true to false
        String updateMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("metrics")
            .field("type", "object")
            .field("disable_objects", false)
            .endObject()
            .endObject()
            .endObject()
            .toString();

        MapperException e = expectThrows(MapperException.class, () -> {
            mapperService.merge("_doc", new CompressedXContent(updateMapping), MergeReason.MAPPING_UPDATE);
        });

        assertThat(e.getMessage(), containsString("Cannot update parameter [disable_objects] from [true] to [false]"));
        assertThat(e.getMessage(), containsString("for object mapping [metrics]"));
        assertThat(e.getMessage(), containsString("The disable_objects setting is immutable after index creation"));
    }

    public void testValidateNestedObjectRejection() throws Exception {
        // Test that nested objects cannot be added when disable_objects is enabled
        String initialMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("metrics")
            .field("type", "object")
            .field("disable_objects", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();

        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("_doc", new CompressedXContent(initialMapping), MergeReason.MAPPING_UPDATE);

        // Try to add a nested object when disable_objects is true
        String updateMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("metrics")
            .field("type", "object")
            .field("disable_objects", true)
            .startObject("properties")
            .startObject("cpu")
            .field("type", "object")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        MapperException e = expectThrows(MapperException.class, () -> {
            mapperService.merge("_doc", new CompressedXContent(updateMapping), MergeReason.MAPPING_UPDATE);
        });

        // Test the specific validation lines: newMapper.name() and name() interpolation
        assertThat(e.getMessage(), containsString("Field mapping conflict: Cannot add nested object field [metrics.cpu]"));
        assertThat(e.getMessage(), containsString("when disable_objects is enabled for [metrics]"));
        assertThat(e.getMessage(), containsString("all fields must use flat field notation"));
    }

    public void testValidateFlatFieldCompatibility() throws Exception {
        // Test that nested objects are rejected during initial mapping creation when disable_objects is true
        String mappingWithNestedObject = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("metrics")
            .field("type", "object")
            .field("disable_objects", true)
            .startObject("properties")
            .startObject("cpu")
            .field("type", "object")
            .startObject("properties")
            .startObject("usage")
            .field("type", "double")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        MapperService mapperService = createIndex("test").mapperService();

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            mapperService.merge("_doc", new CompressedXContent(mappingWithNestedObject), MergeReason.MAPPING_UPDATE);
        });

        // Test the specific validation lines: childMapper.name() and name() interpolation
        assertThat(e.getMessage(), containsString("Field mapping conflict: Cannot add nested object field [metrics.cpu]"));
        assertThat(e.getMessage(), containsString("when disable_objects is enabled for [metrics]"));
        assertThat(e.getMessage(), containsString("all fields must use flat field notation"));

        // Test that validation only triggers for ObjectMapper instances, not FieldMapper instances
        String mappingWithFieldMappers = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("allowed_container")
            .field("type", "object")
            .field("disable_objects", true)
            .startObject("properties")
            .startObject("text_field")  // FieldMapper, not ObjectMapper - should be allowed
            .field("type", "text")
            .endObject()
            .startObject("keyword_field")  // Another FieldMapper - should be allowed
            .field("type", "keyword")
            .endObject()
            .startObject("number_field")  // Another FieldMapper - should be allowed
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        MapperService mapperService2 = createIndex("test2").mapperService();

        // This should NOT throw an exception because the child mappers are FieldMappers, not ObjectMappers
        mapperService2.merge("_doc", new CompressedXContent(mappingWithFieldMappers), MergeReason.MAPPING_UPDATE);

        // Test with deeply nested structure to ensure the name interpolation works correctly
        String deeplyNestedMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("level1")
            .field("type", "object")
            .startObject("properties")
            .startObject("level2")
            .field("type", "object")
            .field("disable_objects", true)  // disable_objects at level2
            .startObject("properties")
            .startObject("level3_object")  // This should be rejected
            .field("type", "object")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        MapperService mapperService3 = createIndex("test3").mapperService();

        MapperParsingException e2 = expectThrows(MapperParsingException.class, () -> {
            mapperService3.merge("_doc", new CompressedXContent(deeplyNestedMapping), MergeReason.MAPPING_UPDATE);
        });

        assertThat(e2.getMessage(), containsString("Cannot add nested object field [level1.level2.level3_object]"));
        assertThat(e2.getMessage(), containsString("when disable_objects is enabled for [level2]"));
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }
}
