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

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class RootObjectMapperTests extends OpenSearchSingleNodeTestCase {

    public void testNumericDetection() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("numeric_detection", false)
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), reason);
        assertEquals(mapping, mapper.mappingSource().toString());

        // update with a different explicit value
        String mapping2 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("numeric_detection", true)
            .endObject()
            .endObject()
            .toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), reason);
        assertEquals(mapping2, mapper.mappingSource().toString());

        // update with an implicit value: no change
        String mapping3 = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), reason);
        assertEquals(mapping2, mapper.mappingSource().toString());
    }

    public void testDateDetection() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("date_detection", true)
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), reason);
        assertEquals(mapping, mapper.mappingSource().toString());

        // update with a different explicit value
        String mapping2 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("date_detection", false)
            .endObject()
            .endObject()
            .toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), reason);
        assertEquals(mapping2, mapper.mappingSource().toString());

        // update with an implicit value: no change
        String mapping3 = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), reason);
        assertEquals(mapping2, mapper.mappingSource().toString());
    }

    public void testDateFormatters() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("dynamic_date_formats", Arrays.asList("yyyy-MM-dd"))
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), reason);
        assertEquals(mapping, mapper.mappingSource().toString());

        // no update if formatters are not set explicitly
        String mapping2 = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), reason);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping3 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("dynamic_date_formats", Arrays.asList())
            .endObject()
            .endObject()
            .toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), reason);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }

    public void testDynamicTemplates() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startArray("dynamic_templates")
            .startObject()
            .startObject("my_template")
            .field("match_mapping_type", "string")
            .startObject("mapping")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        // no update if templates are not set explicitly
        String mapping2 = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping3 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("dynamic_templates", Arrays.asList())
            .endObject()
            .endObject()
            .toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }

    public void testDynamicProperties() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("*_i")
            .field("type", "long")
            .endObject()
            .startObject("*_s")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        RootObjectMapper root = mapper.root();
        DynamicProperty[] dynProps = root.dynamicProperties();
        assertEquals(2, dynProps.length);
        assertEquals("*_i", dynProps[0].getPattern());
        assertEquals("*_s", dynProps[1].getPattern());

        assertNotNull(root.findDynamicProperty("count_i"));
        assertNotNull(root.findDynamicProperty("foo_s"));
        assertNull(root.findDynamicProperty("other"));
        assertEquals("*_i", root.findDynamicProperty("count_i").getPattern());
    }

    public void testDynamicPropertiesAmbiguousPatternsRejected() throws Exception {
        // *_i and i_* both match "i_i" -> must be rejected
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("*_i")
            .field("type", "long")
            .endObject()
            .startObject("i_*")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), containsString("overlap"));
        assertThat(e.getMessage(), containsString("*_i"));
        assertThat(e.getMessage(), containsString("i_*"));
    }

    public void testDynamicPropertiesRejectsOverlappingGlobPatterns() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("*_i*")
            .field("type", "long")
            .endObject()
            .startObject("*abchdi*")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), containsString("overlap"));
    }

    public void testDynamicPropertiesAllowsNonOverlappingPatterns() throws Exception {
        // a* and b* have empty language intersection — distinct field name prefixes.
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("a*")
            .field("type", "long")
            .endObject()
            .startObject("b*")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        RootObjectMapper root = mapperService.documentMapper().root();
        assertEquals(2, root.dynamicProperties().length);
    }

    public void testDynamicPropertiesExplicitFieldMatchesPatternRejected() throws Exception {
        // Cannot add explicit property "count_i" when dynamic_property "*_i" exists
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("*_i")
            .field("type", "long")
            .endObject()
            .endObject()
            .startObject("properties")
            .startObject("count_i")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), containsString("count_i"));
        assertThat(e.getMessage(), containsString("*_i"));
        assertThat(e.getMessage(), containsString("Explicit fields cannot overlap"));
    }

    /**
     * Partial {@link MergeReason#MAPPING_UPDATE} must not drop prior {@code dynamic_properties}:
     * entries merge by pattern key (same behavior family as {@code dynamic_templates} name-based merge).
     */
    public void testDynamicPropertiesMappingUpdateMergesByPattern() throws Exception {
        String first = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("*_i")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        String second = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("*_s")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("type", new CompressedXContent(first), MergeReason.MAPPING_UPDATE);
        mapperService.merge("type", new CompressedXContent(second), MergeReason.MAPPING_UPDATE);
        RootObjectMapper root = mapperService.documentMapper().root();
        assertEquals(2, root.dynamicProperties().length);
        assertNotNull(root.findDynamicProperty("x_i"));
        assertNotNull(root.findDynamicProperty("y_s"));
        Set<String> patterns = new HashSet<>();
        for (DynamicProperty dp : root.dynamicProperties()) {
            patterns.add(dp.getPattern());
        }
        assertEquals(Set.of("*_i", "*_s"), patterns);
    }

    /** Same pattern key in a later update replaces the previous mapping for that pattern only. */
    public void testDynamicPropertiesMappingUpdateReplacesSamePattern() throws Exception {
        String first = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("*_i")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        String second = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("*_i")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("type", new CompressedXContent(first), MergeReason.MAPPING_UPDATE);
        mapperService.merge("type", new CompressedXContent(second), MergeReason.MAPPING_UPDATE);
        DynamicProperty[] dps = mapperService.documentMapper().root().dynamicProperties();
        assertEquals(1, dps.length);
        assertEquals("*_i", dps[0].getPattern());
        assertEquals("integer", dps[0].getMapping().get("type"));
    }

    public void testDynamicPropertiesTemplateMergeMergesSecondPattern() throws Exception {
        String first = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("*_i")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        String second = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("*_s")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("type", new CompressedXContent(first), MergeReason.INDEX_TEMPLATE);
        mapperService.merge("type", new CompressedXContent(second), MergeReason.INDEX_TEMPLATE);
        DynamicProperty[] dps = mapperService.documentMapper().root().dynamicProperties();
        assertEquals(2, dps.length);
        Set<String> patterns = new HashSet<>();
        for (DynamicProperty dp : dps) {
            patterns.add(dp.getPattern());
        }
        assertEquals(Set.of("*_i", "*_s"), patterns);
    }

    public void testDynamicPropertiesRejectsBareStarPattern() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("*")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), containsString("pattern [*] is not allowed"));
    }

    public void testDynamicPropertiesMustBeObject() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startArray("dynamic_properties")
            .endArray()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), containsString("must be an object"));
    }

    public void testDynamicPropertiesPatternRequiresWildcard() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("count_i")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), containsString("must contain a wildcard"));
    }

    public void testDynamicPropertiesEntryMustBeMappingObject() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .field("*_i", "long")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), containsString("must have a mapping object"));
    }

    public void testDynamicPropertiesRequiresType() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("*_i")
            .field("index", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), containsString("No type specified"));
    }

    public void testDynamicPropertiesRejectsUnknownType() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("*_i")
            .field("type", "not_a_real_mapper_type_xyz")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), containsString("No handler for type"));
    }

    /** Empty {@code dynamic_properties: {}} is valid and exercises merge validation on an empty rule list. */
    public void testDynamicPropertiesEmptyObject() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .endObject()
            .startObject("properties")
            .startObject("title")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(0, mapperService.documentMapper().root().dynamicProperties().length);
    }

    public void testDynamicPropertiesTemplateMergeRejectsExplicitOverlap() throws Exception {
        String explicitFirst = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject("count_i")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        String dynamicSecond = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_properties")
            .startObject("*_i")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("type", new CompressedXContent(explicitFirst), MergeReason.INDEX_TEMPLATE);
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.merge("type", new CompressedXContent(dynamicSecond), MergeReason.INDEX_TEMPLATE)
        );
        assertThat(e.getMessage(), containsString("count_i"));
    }

    public void testDynamicTemplatesForIndexTemplate() throws IOException {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("dynamic_templates")
            .startObject()
            .startObject("first_template")
            .field("path_match", "first")
            .startObject("mapping")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .startObject()
            .startObject("second_template")
            .field("path_match", "second")
            .startObject("mapping")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MergeReason.INDEX_TEMPLATE);

        // There should be no update if templates are not set.
        mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        DocumentMapper mapper = mapperService.merge(
            MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent(mapping),
            MergeReason.INDEX_TEMPLATE
        );

        DynamicTemplate[] templates = mapper.root().dynamicTemplates();
        assertEquals(2, templates.length);
        assertEquals("first_template", templates[0].name());
        assertEquals("first", templates[0].pathMatch());
        assertEquals("second_template", templates[1].name());
        assertEquals("second", templates[1].pathMatch());

        // Dynamic templates should be appended and deduplicated.
        mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("dynamic_templates")
            .startObject()
            .startObject("third_template")
            .field("path_match", "third")
            .startObject("mapping")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject()
            .startObject()
            .startObject("second_template")
            .field("path_match", "second_updated")
            .startObject("mapping")
            .field("type", "double")
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .toString();
        mapper = mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MergeReason.INDEX_TEMPLATE);

        templates = mapper.root().dynamicTemplates();
        assertEquals(3, templates.length);
        assertEquals("first_template", templates[0].name());
        assertEquals("first", templates[0].pathMatch());
        assertEquals("second_template", templates[1].name());
        assertEquals("second_updated", templates[1].pathMatch());
        assertEquals("third_template", templates[2].name());
        assertEquals("third", templates[2].pathMatch());
    }

    public void testIllegalFormatField() throws Exception {
        String dynamicMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startArray("dynamic_date_formats")
            .startArray()
            .value("test_format")
            .endArray()
            .endArray()
            .endObject()
            .endObject()
            .toString();
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startArray("date_formats")
            .startArray()
            .value("test_format")
            .endArray()
            .endArray()
            .endObject()
            .endObject()
            .toString();

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        for (String m : Arrays.asList(mapping, dynamicMapping)) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(m))
            );
            assertEquals("Invalid format: [[test_format]]: expected string value", e.getMessage());
        }
    }

    public void testIllegalDynamicTemplates() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_templates")
            .endObject()
            .endObject()
            .endObject()
            .toString();

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("Dynamic template syntax error. An array of named objects is expected.", e.getMessage());
    }

    public void testIllegalDynamicTemplateUnknownFieldType() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("type");
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template1");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "string");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping.toString()), MergeReason.MAPPING_UPDATE);
        assertThat(mapper.mappingSource().toString(), containsString("\"type\":\"string\""));
        assertWarnings(
            "dynamic template [my_template1] has invalid content [{\"match_mapping_type\":\"string\",\"mapping\":{\"type\":"
                + "\"string\"}}], caused by [No mapper found for type [string]]"
        );
    }

    public void testIllegalDynamicTemplateUnknownAttribute() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("type");
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template2");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "keyword");
                mapping.field("foo", "bar");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping.toString()), MergeReason.MAPPING_UPDATE);
        assertThat(mapper.mappingSource().toString(), containsString("\"foo\":\"bar\""));
        assertWarnings(
            "dynamic template [my_template2] has invalid content [{\"match_mapping_type\":\"string\",\"mapping\":{"
                + "\"foo\":\"bar\",\"type\":\"keyword\"}}], "
                + "caused by [unknown parameter [foo] on mapper [__dynamic__my_template2] of type [keyword]]"
        );
    }

    public void testIllegalDynamicTemplateInvalidAttribute() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("type");
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template3");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "text");
                mapping.field("analyzer", "foobar");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping.toString()), MergeReason.MAPPING_UPDATE);
        assertThat(mapper.mappingSource().toString(), containsString("\"analyzer\":\"foobar\""));
        assertWarnings(
            "dynamic template [my_template3] has invalid content [{\"match_mapping_type\":\"string\",\"mapping\":{"
                + "\"analyzer\":\"foobar\",\"type\":\"text\"}}], caused by [analyzer [foobar] has not been configured in mappings]"
        );
    }

    public void testIllegalDynamicTemplateNoMappingType() throws Exception {
        MapperService mapperService;

        {
            XContentBuilder mapping = XContentFactory.jsonBuilder();
            mapping.startObject();
            {
                mapping.startObject("type");
                mapping.startArray("dynamic_templates");
                {
                    mapping.startObject();
                    mapping.startObject("my_template4");
                    if (randomBoolean()) {
                        mapping.field("match_mapping_type", "*");
                    } else {
                        mapping.field("match", "string_*");
                    }
                    mapping.startObject("mapping");
                    mapping.field("type", "{dynamic_type}");
                    mapping.field("index_phrases", true);
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                }
                mapping.endArray();
                mapping.endObject();
            }
            mapping.endObject();
            mapperService = createIndex("test").mapperService();
            DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping.toString()), MergeReason.MAPPING_UPDATE);
            assertThat(mapper.mappingSource().toString(), containsString("\"index_phrases\":true"));
        }
        {
            boolean useMatchMappingType = randomBoolean();
            XContentBuilder mapping = XContentFactory.jsonBuilder();
            mapping.startObject();
            {
                mapping.startObject("type");
                mapping.startArray("dynamic_templates");
                {
                    mapping.startObject();
                    mapping.startObject("my_template4");
                    if (useMatchMappingType) {
                        mapping.field("match_mapping_type", "*");
                    } else {
                        mapping.field("match", "string_*");
                    }
                    mapping.startObject("mapping");
                    mapping.field("type", "{dynamic_type}");
                    mapping.field("foo", "bar");
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                }
                mapping.endArray();
                mapping.endObject();
            }
            mapping.endObject();

            DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping.toString()), MergeReason.MAPPING_UPDATE);
            assertThat(mapper.mappingSource().toString(), containsString("\"foo\":\"bar\""));
            if (useMatchMappingType) {
                assertWarnings(
                    "dynamic template [my_template4] has invalid content [{\"match_mapping_type\":\"*\",\"mapping\":{"
                        + "\"foo\":\"bar\",\"type\":\"{dynamic_type}\"}}], "
                        + "caused by [unknown parameter [foo] on mapper [__dynamic__my_template4] of type [binary]]"
                );
            } else {
                assertWarnings(
                    "dynamic template [my_template4] has invalid content [{\"match\":\"string_*\",\"mapping\":{"
                        + "\"foo\":\"bar\",\"type\":\"{dynamic_type}\"}}], "
                        + "caused by [unknown parameter [foo] on mapper [__dynamic__my_template4] of type [binary]]"
                );
            }
        }
    }

    /** Registering more than {@link RootObjectMapper#MAX_DYNAMIC_PROPERTIES} patterns must be rejected. */
    public void testDynamicPropertiesPatternCountLimit() throws IOException {
        MapperService mapperService = createIndex("test").mapperService();
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("dynamic_properties");
        for (int i = 0; i <= RootObjectMapper.MAX_DYNAMIC_PROPERTIES; i++) {
            mapping.startObject("*_field" + i).field("type", "keyword").endObject();
        }
        mapping.endObject().endObject().endObject();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.merge("type", new CompressedXContent(mapping.toString()), MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), containsString("exceeds the maximum allowed [" + RootObjectMapper.MAX_DYNAMIC_PROPERTIES + "]"));
    }

    /** Registering exactly {@link RootObjectMapper#MAX_DYNAMIC_PROPERTIES} patterns must succeed. */
    public void testDynamicPropertiesPatternCountAtLimit() throws IOException {
        MapperService mapperService = createIndex("test").mapperService();
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("dynamic_properties");
        for (int i = 1; i <= RootObjectMapper.MAX_DYNAMIC_PROPERTIES; i++) {
            mapping.startObject("*_field" + i).field("type", "keyword").endObject();
        }
        mapping.endObject().endObject().endObject();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping.toString()), MergeReason.MAPPING_UPDATE);
        assertEquals(RootObjectMapper.MAX_DYNAMIC_PROPERTIES, mapper.root().dynamicProperties().length);
    }

    // -----------------------------------------------------------------------
    // Lucene field-count limit tests
    // -----------------------------------------------------------------------

    /** Helper: build a dynamic_properties mapping for pattern {@code *_kw} → keyword. */
    private static String dynKwMapping() throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("dynamic_properties")
            .startObject("*_kw")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
    }

    /** Helper: build a SourceToParse with a single keyword field matching *_kw. */
    private static SourceToParse kwSource(String fieldName, String value) {
        return new SourceToParse("index", "1", new BytesArray("{\"" + fieldName + "\": \"" + value + "\"}"), MediaTypeRegistry.JSON);
    }

    /** Helper: pre-populate the tracker so it looks like N fields have been refreshed from the shard. */
    private static void populateTracker(LuceneFieldTracker tracker, int fieldCount) {
        FieldInfo[] infos = new FieldInfo[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            infos[i] = makeFieldInfo("__lucene_field_" + i, i);
        }
        tracker.setFieldInfos(new FieldInfos(infos));
    }

    /** Helper: create a minimal {@link FieldInfo} with the given name and field number. */
    private static FieldInfo makeFieldInfo(String name, int number) {
        return new FieldInfo(
            name,
            number,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NONE,
            DocValuesSkipIndexType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }

    /**
     * When the tracker is empty (no refresh yet), the field-count limit is not applied and
     * dynamic-property fields can be created freely.
     */
    public void testDynamicPropertiesLuceneFieldLimitNotEnforcedWhenTrackerEmpty() throws Exception {
        long limit = 3L;
        MapperService mapperService = createIndex(
            "idx_empty",
            Settings.builder()
                .put(MapperService.INDEX_MAPPING_DYNAMIC_PROPERTIES_LUCENE_FIELD_LIMIT_SETTING.getKey(), limit)
                .build()
        ).mapperService();
        DocumentMapper mapper = mapperService.merge("_doc", new CompressedXContent(dynKwMapping()), MergeReason.MAPPING_UPDATE);

        // Tracker starts with FieldInfos.EMPTY (no engine in unit-test context) → size is 0.
        assertEquals(0, mapperService.getLuceneFieldTracker().getFieldInfos().size());

        // Should succeed: empty tracker means the limit check is skipped.
        ParsedDocument doc = mapper.parse(kwSource("new_kw", "hello"));
        assertNotNull(doc);
    }

    /**
     * When the tracker snapshot is at the configured limit and the parsed field is new (not in the
     * tracker), the parse must throw {@link MapperParsingException}.
     */
    public void testDynamicPropertiesLuceneFieldLimitRejectsNewField() throws Exception {
        long limit = 3L;
        MapperService mapperService = createIndex(
            "idx_at_limit",
            Settings.builder()
                .put(MapperService.INDEX_MAPPING_DYNAMIC_PROPERTIES_LUCENE_FIELD_LIMIT_SETTING.getKey(), limit)
                .build()
        ).mapperService();
        DocumentMapper mapper = mapperService.merge("_doc", new CompressedXContent(dynKwMapping()), MergeReason.MAPPING_UPDATE);

        // Simulate a refresh: tracker has exactly `limit` field names, none matching *_kw.
        populateTracker(mapperService.getLuceneFieldTracker(), (int) limit);

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(kwSource("brand_new_kw", "value"))
        );
        assertThat(e.getMessage(), containsString("limit [" + limit + "]"));
        assertThat(e.getMessage(), containsString(MapperService.INDEX_MAPPING_DYNAMIC_PROPERTIES_LUCENE_FIELD_LIMIT_SETTING.getKey()));
    }

    /**
     * When the tracker snapshot is below the limit, a new dynamic-property field is allowed.
     */
    public void testDynamicPropertiesLuceneFieldLimitAllowsFieldBelowLimit() throws Exception {
        long limit = 5L;
        MapperService mapperService = createIndex(
            "idx_below_limit",
            Settings.builder()
                .put(MapperService.INDEX_MAPPING_DYNAMIC_PROPERTIES_LUCENE_FIELD_LIMIT_SETTING.getKey(), limit)
                .build()
        ).mapperService();
        DocumentMapper mapper = mapperService.merge("_doc", new CompressedXContent(dynKwMapping()), MergeReason.MAPPING_UPDATE);

        // Tracker has limit-1 fields → one slot still available.
        populateTracker(mapperService.getLuceneFieldTracker(), (int) limit - 1);

        ParsedDocument doc = mapper.parse(kwSource("ok_kw", "value"));
        assertNotNull(doc);
    }

    /**
     * When the tracker snapshot is at the limit but the field being parsed is already tracked
     * (i.e. it already exists in Lucene), it must not be rejected — existing fields reuse their slot.
     */
    public void testDynamicPropertiesLuceneFieldLimitAllowsExistingTrackedField() throws Exception {
        long limit = 3L;
        MapperService mapperService = createIndex(
            "idx_existing",
            Settings.builder()
                .put(MapperService.INDEX_MAPPING_DYNAMIC_PROPERTIES_LUCENE_FIELD_LIMIT_SETTING.getKey(), limit)
                .build()
        ).mapperService();
        DocumentMapper mapper = mapperService.merge("_doc", new CompressedXContent(dynKwMapping()), MergeReason.MAPPING_UPDATE);

        // Tracker has exactly `limit` fields — but the field we are about to parse is among them.
        String existingField = "already_kw";
        FieldInfo[] infos = new FieldInfo[(int) limit];
        infos[0] = makeFieldInfo(existingField, 0);
        for (int i = 1; i < limit; i++) {
            infos[i] = makeFieldInfo("__lucene_field_" + i, i);
        }
        mapperService.getLuceneFieldTracker().setFieldInfos(new FieldInfos(infos));

        // Parsing a field already in the tracker must succeed (no new slot needed).
        ParsedDocument doc = mapper.parse(kwSource(existingField, "reused"));
        assertNotNull(doc);
    }

    /**
     * Setting the limit to 0 disables the check entirely; even a full tracker must not block parsing.
     */
    public void testDynamicPropertiesLuceneFieldLimitZeroDisablesCheck() throws Exception {
        MapperService mapperService = createIndex(
            "idx_zero_limit",
            Settings.builder()
                .put(MapperService.INDEX_MAPPING_DYNAMIC_PROPERTIES_LUCENE_FIELD_LIMIT_SETTING.getKey(), 0L)
                .build()
        ).mapperService();
        DocumentMapper mapper = mapperService.merge("_doc", new CompressedXContent(dynKwMapping()), MergeReason.MAPPING_UPDATE);

        // Even with a large tracker snapshot, limit=0 means the check is disabled.
        populateTracker(mapperService.getLuceneFieldTracker(), 50_000);

        ParsedDocument doc = mapper.parse(kwSource("any_kw", "no_limit"));
        assertNotNull(doc);
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }
}
