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

import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
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

import static org.opensearch.common.util.FeatureFlags.STAR_TREE_INDEX;
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
            .startArray("ordered_dimensions")
            .startObject()
            .field("name", "node")
            .endObject()
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
            .startObject("node")
            .field("type", "integer")
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

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex("invalid").mapperService().documentMapperParser().parse("tweet", new CompressedXContent(mapping))
        );
        assertEquals(
            "star tree index is under an experimental feature and can be activated only by enabling opensearch.experimental.feature.composite_index.star_tree.enabled feature flag in the JVM options",
            ex.getMessage()
        );

        final Settings starTreeEnabledSettings = Settings.builder().put(STAR_TREE_INDEX, "true").build();
        FeatureFlags.initializeFeatureFlags(starTreeEnabledSettings);

        Settings settings = Settings.builder()
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
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
        mapper = documentMapper.root().getMapper("node");
        assertNotNull(mapper);
        assertEquals("integer", mapper.typeName());

        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }
}
