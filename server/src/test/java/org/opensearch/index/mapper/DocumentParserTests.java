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

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.ParseContext.Document;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.opensearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class DocumentParserTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new MockMetadataMapperPlugin());
    }

    public void testFieldDisabled() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("foo").field("enabled", false).endObject();
            b.startObject("bar").field("type", "integer").endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.field("foo", "1234");
            b.field("bar", 10);
        }));
        assertNull(doc.rootDoc().getField("foo"));
        assertNotNull(doc.rootDoc().getField("bar"));
        assertNotNull(doc.rootDoc().getField(IdFieldMapper.NAME));
    }

    public void testDotsWithFieldDisabled() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("enabled", false)));
        {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field.bar", 111)));
            assertNull(doc.rootDoc().getField("field"));
            assertNull(doc.rootDoc().getField("bar"));
            assertNull(doc.rootDoc().getField("field.bar"));
        }
        {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field.bar", new int[] { 1, 2, 3 })));
            assertNull(doc.rootDoc().getField("field"));
            assertNull(doc.rootDoc().getField("bar"));
            assertNull(doc.rootDoc().getField("field.bar"));
        }
        {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field.bar", Collections.singletonMap("key", "value"))));
            assertNull(doc.rootDoc().getField("field"));
            assertNull(doc.rootDoc().getField("bar"));
            assertNull(doc.rootDoc().getField("field.bar"));
        }
        {
            ParsedDocument doc = mapper.parse(source(b -> {
                b.field("field.bar", "string value");
                b.field("blub", 222);
            }));
            assertNull(doc.rootDoc().getField("field"));
            assertNull(doc.rootDoc().getField("bar"));
            assertNull(doc.rootDoc().getField("field.bar"));
            assertNotNull(doc.rootDoc().getField("blub"));
        }
    }

    public void testDotsWithExistingMapper() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("foo");
            {
                b.startObject("properties");
                {
                    b.startObject("bar");
                    {
                        b.startObject("properties");
                        {
                            b.startObject("baz").field("type", "integer").endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.field("foo.bar.baz", 123);
            b.startObject("foo");
            {
                b.field("bar.baz", 456);
            }
            b.endObject();
            b.startObject("foo.bar");
            {
                b.field("baz", 789);
            }
            b.endObject();
        }));
        assertNull(doc.dynamicMappingsUpdate()); // no update!

        IndexableField[] fields = doc.rootDoc().getFields("foo.bar.baz");
        assertEquals(6, fields.length);
        assertEquals(123, fields[0].numericValue());
        assertEquals("123", fields[1].stringValue());
        assertEquals(456, fields[2].numericValue());
        assertEquals("456", fields[3].stringValue());
        assertEquals(789, fields[4].numericValue());
        assertEquals("789", fields[5].stringValue());
    }

    public void testDotsWithExistingNestedMapper() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "nested");
            b.startObject("properties");
            {
                b.startObject("bar").field("type", "integer").endObject();
            }
            b.endObject();
        }));

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("field.bar", 123))));
        assertEquals(
            "Cannot add a value for field [field.bar] since one of the intermediate objects is mapped as a nested object: [field]",
            e.getMessage()
        );
    }

    public void testUnexpectedFieldMappingType() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("foo").field("type", "long").endObject();
            b.startObject("bar").field("type", "boolean").endObject();
        }));
        {
            MapperException exception = expectThrows(MapperException.class, () -> mapper.parse(source(b -> b.field("foo", true))));
            assertThat(exception.getMessage(), containsString("failed to parse field [foo] of type [long] in document with id '1'"));
        }
        {
            MapperException exception = expectThrows(MapperException.class, () -> mapper.parse(source(b -> b.field("bar", "bar"))));
            assertThat(exception.getMessage(), containsString("failed to parse field [bar] of type [boolean] in document with id '1'"));
        }
    }

    public void testDotsWithDynamicNestedMapper() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("objects_as_nested");
                    {
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "nested").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("foo.bar", 42))));
        assertEquals("It is forbidden to create dynamic nested objects ([foo]) through `copy_to` or dots in field names", e.getMessage());
    }

    public void testNestedHaveIdAndTypeFields() throws Exception {

        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("foo");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("bar").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("baz").field("type", "keyword").endObject();
        }));

        // Verify in the case where only a single type is allowed that the _id field is added to nested documents:
        ParsedDocument result = mapper.parse(source(b -> {
            b.startArray("foo");
            {
                b.startObject().field("bar", "value1").endObject();
            }
            b.endArray();
            b.field("baz", "value2");
        }));
        assertEquals(2, result.docs().size());
        // Nested document:
        assertNotNull(result.docs().get(0).getField(IdFieldMapper.NAME));
        assertEquals(Uid.encodeId("1"), result.docs().get(0).getField(IdFieldMapper.NAME).binaryValue());
        assertEquals(IdFieldMapper.Defaults.NESTED_FIELD_TYPE, result.docs().get(0).getField(IdFieldMapper.NAME).fieldType());
        assertNotNull(result.docs().get(0).getField(NestedPathFieldMapper.NAME));
        assertEquals("foo", result.docs().get(0).getField(NestedPathFieldMapper.NAME).stringValue());
        assertEquals("value1", result.docs().get(0).getField("foo.bar").binaryValue().utf8ToString());
        // Root document:
        assertNotNull(result.docs().get(1).getField(IdFieldMapper.NAME));
        assertEquals(Uid.encodeId("1"), result.docs().get(1).getField(IdFieldMapper.NAME).binaryValue());
        assertEquals(IdFieldMapper.Defaults.FIELD_TYPE, result.docs().get(1).getField(IdFieldMapper.NAME).fieldType());
        assertNull(result.docs().get(1).getField(NestedPathFieldMapper.NAME));
        assertEquals("value2", result.docs().get(1).getField("baz").binaryValue().utf8ToString());
    }

    public void testPropagateDynamicWithExistingMapper() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties");
            {
                b.startObject("foo");
                {
                    b.field("type", "object");
                    b.field("dynamic", true);
                    b.startObject("properties").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo").field("bar", "something").endObject()));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertNotNull(doc.rootDoc().getField("foo.bar"));
    }

    public void testPropagateDynamicWithDynamicMapper() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties");
            {
                b.startObject("foo");
                {
                    b.field("type", "object");
                    b.field("dynamic", true);
                    b.startObject("properties").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("foo");
            {
                b.startObject("bar").field("baz", "something").endObject();
            }
            b.endObject();
        }));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertNotNull(doc.rootDoc().getField("foo.bar.baz"));
    }

    public void testDynamicRootFallback() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties");
            {
                b.startObject("foo");
                {
                    b.field("type", "object");
                    b.startObject("properties").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo").field("bar", "something").endObject()));
        assertNull(doc.dynamicMappingsUpdate());
        assertNull(doc.rootDoc().getField("foo.bar"));
    }

    DocumentMapper createDummyMapping() throws Exception {
        return createMapperService().documentMapper();
    }

    MapperService createMapperService() throws Exception {
        return createMapperService(mapping(b -> {
            b.startObject("y").field("type", "object").endObject();
            b.startObject("x");
            {
                b.startObject("properties");
                {
                    b.startObject("subx");
                    {
                        b.field("type", "object");
                        b.startObject("properties");
                        {
                            b.startObject("subsubx").field("type", "object").endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
    }

    // creates an object mapper, which is about 100x harder than it should be....
    ObjectMapper createObjectMapper(MapperService mapperService, String name) {
        IndexMetadata build = IndexMetadata.builder("")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings settings = new IndexSettings(build, Settings.EMPTY);
        ParseContext context = new ParseContext.InternalParseContext(
            settings,
            mapperService.documentMapperParser(),
            mapperService.documentMapper(),
            null,
            null
        );
        String[] nameParts = name.split("\\.");
        for (int i = 0; i < nameParts.length - 1; ++i) {
            context.path().add(nameParts[i]);
        }
        Mapper.Builder<?> builder = new ObjectMapper.Builder<>(nameParts[nameParts.length - 1]).enabled(true);
        Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings().getSettings(), context.path());
        return (ObjectMapper) builder.build(builderContext);
    }

    public void testEmptyMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        assertNull(DocumentParser.createDynamicUpdate(docMapper.mapping(), docMapper, Collections.emptyList()));
    }

    public void testSingleMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        List<Mapper> updates = Collections.singletonList(new MockFieldMapper("foo"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mapping(), docMapper, updates);
        assertNotNull(mapping);
        assertNotNull(mapping.root().getMapper("foo"));
    }

    public void testSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        List<Mapper> updates = Collections.singletonList(new MockFieldMapper("x.foo"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mapping(), docMapper, updates);
        assertNotNull(mapping);
        Mapper xMapper = mapping.root().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper) xMapper).getMapper("foo"));
        assertNull(((ObjectMapper) xMapper).getMapper("subx"));
    }

    public void testMultipleSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        List<Mapper> updates = new ArrayList<>();
        updates.add(new MockFieldMapper("x.foo"));
        updates.add(new MockFieldMapper("x.bar"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mapping(), docMapper, updates);
        assertNotNull(mapping);
        Mapper xMapper = mapping.root().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper) xMapper).getMapper("foo"));
        assertNotNull(((ObjectMapper) xMapper).getMapper("bar"));
        assertNull(((ObjectMapper) xMapper).getMapper("subx"));
    }

    public void testDeepSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        List<Mapper> updates = Collections.singletonList(new MockFieldMapper("x.subx.foo"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mapping(), docMapper, updates);
        assertNotNull(mapping);
        Mapper xMapper = mapping.root().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        Mapper subxMapper = ((ObjectMapper) xMapper).getMapper("subx");
        assertTrue(subxMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper) subxMapper).getMapper("foo"));
        assertNull(((ObjectMapper) subxMapper).getMapper("subsubx"));
    }

    public void testDeepSubfieldAfterSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        List<Mapper> updates = new ArrayList<>();
        updates.add(new MockFieldMapper("x.a"));
        updates.add(new MockFieldMapper("x.subx.b"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mapping(), docMapper, updates);
        assertNotNull(mapping);
        Mapper xMapper = mapping.root().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper) xMapper).getMapper("a"));
        Mapper subxMapper = ((ObjectMapper) xMapper).getMapper("subx");
        assertTrue(subxMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper) subxMapper).getMapper("b"));
    }

    public void testObjectMappingUpdate() throws Exception {
        MapperService mapperService = createMapperService();
        DocumentMapper docMapper = mapperService.documentMapper();
        List<Mapper> updates = new ArrayList<>();
        updates.add(createObjectMapper(mapperService, "foo"));
        updates.add(createObjectMapper(mapperService, "foo.bar"));
        updates.add(new MockFieldMapper("foo.bar.baz"));
        updates.add(new MockFieldMapper("foo.field"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mapping(), docMapper, updates);
        assertNotNull(mapping);
        Mapper fooMapper = mapping.root().getMapper("foo");
        assertNotNull(fooMapper);
        assertTrue(fooMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper) fooMapper).getMapper("field"));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertTrue(barMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper) barMapper).getMapper("baz"));
    }

    public void testDynamicGeoPointArrayWithTemplate() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("georule");
                    {
                        b.field("match", "foo*");
                        b.startObject("mapping");
                        {
                            b.field("type", "geo_point");
                            b.field("doc_values", false);
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("foo");
            {
                b.startArray().value(0).value(0).endArray();
                b.startArray().value(1).value(1).endArray();
            }
            b.endArray();
        }));
        assertEquals(2, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicLongArrayWithTemplate() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("georule");
                    {
                        b.field("match", "foo*");
                        b.startObject("mapping").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()));
        assertEquals(4, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()));
        assertEquals(4, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicFalseLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicStrictLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        StrictDynamicMappingException exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()))
        );
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testMappedGeoPointArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_point");
            b.field("doc_values", false);
        }));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startArray().value(0).value(0).endArray();
                b.startArray().value(1).value(1).endArray();
            }
            b.endArray();
        }));
        assertEquals(2, doc.rootDoc().getFields("field").length);
    }

    public void testMappedLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field").value(0).value(1).endArray()));
        assertEquals(4, doc.rootDoc().getFields("field").length);
    }

    public void testDynamicObjectWithTemplate() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("georule");
                    {
                        b.field("match", "foo*");
                        b.startObject("mapping");
                        {
                            b.field("type", "object");
                            b.startObject("properties");
                            {
                                b.startObject("bar").field("type", "keyword").endObject();
                            }
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo").field("bar", "baz").endObject()));
        assertEquals(2, doc.rootDoc().getFields("foo.bar").length);
    }

    public void testDynamicFalseObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo").field("bar", "baz").endObject()));
        assertEquals(0, doc.rootDoc().getFields("foo.bar").length);
    }

    public void testDynamicStrictObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        StrictDynamicMappingException exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> mapper.parse(source(b -> b.startObject("foo").field("bar", "baz").endObject()))
        );
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDynamicFalseValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("bar", "baz")));
        assertEquals(0, doc.rootDoc().getFields("bar").length);
    }

    public void testDynamicStrictValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        StrictDynamicMappingException exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> mapper.parse(source(b -> b.field("bar", "baz")))
        );
        assertEquals("mapping set to strict, dynamic introduction of [bar] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDynamicFalseNull() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("bar")));
        assertEquals(0, doc.rootDoc().getFields("bar").length);
    }

    public void testDynamicStrictNull() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        StrictDynamicMappingException exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> mapper.parse(source(b -> b.nullField("bar")))
        );
        assertEquals("mapping set to strict, dynamic introduction of [bar] within [_doc] is not allowed", exception.getMessage());
    }

    public void testMappedNullValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("foo")));
        assertEquals(0, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicBigInteger() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("big-integer-to-keyword");
                    {
                        b.field("match", "big-*");
                        b.field("match_mapping_type", "long");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        BigInteger value = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        ParsedDocument doc = mapper.parse(source(b -> b.field("big-integer", value)));

        IndexableField[] fields = doc.rootDoc().getFields("big-integer");
        assertEquals(2, fields.length);
        assertEquals(new BytesRef(value.toString()), fields[0].binaryValue());
    }

    public void testDynamicBigDecimal() throws Exception {

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("big-decimal-to-scaled-float");
                    {
                        b.field("match", "big-*");
                        b.field("match_mapping_type", "double");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        BigDecimal value = BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.valueOf(10.1));
        ParsedDocument doc = mapper.parse(source(b -> b.field("big-decimal", value)));

        IndexableField[] fields = doc.rootDoc().getFields("big-decimal");
        assertEquals(2, fields.length);
        assertEquals(new BytesRef(value.toString()), fields[0].binaryValue());
    }

    public void testDynamicDottedFieldNameLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo.bar.baz").value(0).value(1).endArray()));
        assertEquals(4, doc.rootDoc().getFields("foo.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameLongArrayWithParentTemplate() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("georule");
                    {
                        b.field("match", "foo*");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo.bar.baz").value(0).value(1).endArray()));
        assertEquals(4, doc.rootDoc().getFields("foo.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameLongArrayWithExistingParent() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "object")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field.bar.baz").value(0).value(1).endArray()));

        assertEquals(4, doc.rootDoc().getFields("field.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("field");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameLongArrayWithExistingParentWrongType() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startArray("field.bar.baz").value(0).value(1).endArray()))
        );
        assertEquals(
            "Could not dynamically add mapping for field [field.bar.baz]. "
                + "Existing mapping for [field] must be of type object but found [long].",
            exception.getMessage()
        );
    }

    public void testDynamicFalseDottedFieldNameLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo.bar.baz").value(0).value(1).endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz").length);
    }

    public void testDynamicStrictDottedFieldNameLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        StrictDynamicMappingException exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> mapper.parse(source(b -> b.startArray("foo.bar.baz").value(0).value(1).endArray()))
        );
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDynamicDottedFieldNameLong() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo.bar.baz", 0)));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameLongWithParentTemplate() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("georule");
                    {
                        b.field("match", "foo*");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("foo.bar.baz", 0)));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameLongWithExistingParent() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "object")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field.bar.baz", 0)));
        assertEquals(2, doc.rootDoc().getFields("field.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("field");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameLongWithExistingParentWrongType() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("field.bar.baz", 0)))
        );
        assertEquals(
            "Could not dynamically add mapping for field [field.bar.baz]. "
                + "Existing mapping for [field] must be of type object but found [long].",
            exception.getMessage()
        );
    }

    public void testDynamicFalseDottedFieldNameLong() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo.bar.baz", 0)));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz").length);
    }

    public void testDynamicStrictDottedFieldNameLong() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        StrictDynamicMappingException exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> mapper.parse(source(b -> b.field("foo.bar.baz", 0)))
        );
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDynamicStrictAllowTemplatesDottedFieldNameLong() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict_allow_templates")));
        StrictDynamicMappingException exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> documentMapper.parse(source(b -> b.field("foo.bar.baz", 0)))
        );
        assertEquals(
            "mapping set to strict_allow_templates, dynamic introduction of [foo] within [_doc] is not allowed",
            exception.getMessage()
        );

        DocumentMapper documentMapperWithDynamicTemplates = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "strict_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("path_match", "foo.bar.baz");
                        b.startObject("mapping").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> documentMapperWithDynamicTemplates.parse(source(b -> b.field("foo.bar.baz", 0)))
        );
        assertEquals(
            "mapping set to strict_allow_templates, dynamic introduction of [foo] within [_doc] is not allowed",
            exception.getMessage()
        );

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "strict_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "foo");
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            {
                b.startObject();
                {
                    b.startObject("test1");
                    {
                        b.field("match", "bar");
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            {
                b.startObject();
                {
                    b.startObject("test2");
                    {
                        b.field("path_match", "foo.bar.baz");
                        b.startObject("mapping").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("foo.bar.baz", 0)));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz").length);
    }

    public void testDynamicAllowTemplatesStrictLongArray() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict_allow_templates")));
        StrictDynamicMappingException exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> documentMapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()))
        );
        assertEquals(
            "mapping set to strict_allow_templates, dynamic introduction of [foo] within [_doc] is not allowed",
            exception.getMessage()
        );

        DocumentMapper documentMapperWithDynamicTemplates = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "strict_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "test");
                        b.startObject("mapping").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> documentMapperWithDynamicTemplates.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()))
        );
        assertEquals(
            "mapping set to strict_allow_templates, dynamic introduction of [foo] within [_doc] is not allowed",
            exception.getMessage()
        );

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "strict_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "foo");
                        b.startObject("mapping").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()));
        assertEquals(4, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicStrictAllowTemplatesDottedFieldNameObject() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict_allow_templates")));
        StrictDynamicMappingException exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> documentMapper.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()))
        );
        assertEquals(
            "mapping set to strict_allow_templates, dynamic introduction of [foo] within [_doc] is not allowed",
            exception.getMessage()
        );

        DocumentMapper documentMapperWithDynamicTemplates = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "strict_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "test");
                        b.startObject("mapping").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> documentMapperWithDynamicTemplates.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()))
        );
        assertEquals(
            "mapping set to strict_allow_templates, dynamic introduction of [foo] within [_doc] is not allowed",
            exception.getMessage()
        );

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "strict_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "foo");
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            {
                b.startObject();
                {
                    b.startObject("test1");
                    {
                        b.field("match", "bar");
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            {
                b.startObject();
                {
                    b.startObject("test2");
                    {
                        b.field("match", "baz");
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            {
                b.startObject();
                {
                    b.startObject("test3");
                    {
                        b.field("path_match", "foo.bar.baz.a");
                        b.startObject("mapping").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz.a").length);
    }

    public void testDynamicStrictAllowTemplatesObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "strict_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "test");
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            {
                b.startObject();
                {
                    b.startObject("test1");
                    {
                        b.field("match", "test1");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }

        ));
        StrictDynamicMappingException exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> mapper.parse(source(b -> b.startObject("foo").field("bar", "baz").endObject()))
        );
        assertEquals(
            "mapping set to strict_allow_templates, dynamic introduction of [foo] within [_doc] is not allowed",
            exception.getMessage()
        );

        ParsedDocument doc = mapper.parse(source(b -> b.startObject("test").field("test1", "baz").endObject()));
        assertEquals(2, doc.rootDoc().getFields("test.test1").length);
    }

    public void testDynamicStrictAllowTemplatesValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "strict_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "test*");
                        b.field("match_mapping_type", "string");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }

        ));
        StrictDynamicMappingException exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> mapper.parse(source(b -> b.field("bar", "baz")))
        );
        assertEquals(
            "mapping set to strict_allow_templates, dynamic introduction of [bar] within [_doc] is not allowed",
            exception.getMessage()
        );

        ParsedDocument doc = mapper.parse(source(b -> b.field("test1", "baz")));
        assertEquals(2, doc.rootDoc().getFields("test1").length);
    }

    public void testDynamicStrictAllowTemplatesNull() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict_allow_templates")));
        StrictDynamicMappingException exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> mapper.parse(source(b -> b.nullField("bar")))
        );
        assertEquals(
            "mapping set to strict_allow_templates, dynamic introduction of [bar] within [_doc] is not allowed",
            exception.getMessage()
        );
    }

    public void testDynamicDottedFieldNameObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz.a").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(ObjectMapper.class));
        Mapper aMapper = ((ObjectMapper) bazMapper).getMapper("a");
        assertNotNull(aMapper);
        assertThat(aMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameObjectWithParentTemplate() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("georule");
                    {
                        b.field("match", "foo*");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()));

        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz.a").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(ObjectMapper.class));
        Mapper aMapper = ((ObjectMapper) bazMapper).getMapper("a");
        assertNotNull(aMapper);
        assertThat(aMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameObjectWithExistingParent() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "object")));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("field.bar.baz").field("a", 0).endObject()));
        assertEquals(2, doc.rootDoc().getFields("field.bar.baz.a").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("field");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(ObjectMapper.class));
        Mapper aMapper = ((ObjectMapper) bazMapper).getMapper("a");
        assertNotNull(aMapper);
        assertThat(aMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameObjectWithExistingParentWrongType() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("field.bar.baz").field("a", 0).endObject()))
        );
        assertEquals(
            "Could not dynamically add mapping for field [field.bar.baz]. "
                + "Existing mapping for [field] must be of type object but found [long].",
            exception.getMessage()
        );
    }

    public void testDynamicFalseDottedFieldNameObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz.a").length);
    }

    public void testDynamicStrictDottedFieldNameObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        StrictDynamicMappingException exception = expectThrows(
            StrictDynamicMappingException.class,
            () -> mapper.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()))
        );
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDocumentContainsMetadataField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("_field_names", 0))));
        assertTrue(
            e.getCause().getMessage(),
            e.getCause().getMessage().contains("Field [_field_names] is a metadata field and cannot be added inside a document.")
        );

        mapper.parse(source(b -> b.field("foo._field_names", 0))); // parses without error
    }

    public void testDocumentContainsAllowedMetadataField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        {
            // A metadata field that parses a value fails to parse a null value
            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> mapper.parse(source(b -> b.nullField(MockMetadataMapperPlugin.MockMetadataMapper.CONTENT_TYPE)))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("failed to parse field [_mock_metadata]"));
        }
        {
            // A metadata field that parses a value fails to parse an object
            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> mapper.parse(
                    source(
                        b -> b.field(MockMetadataMapperPlugin.MockMetadataMapper.CONTENT_TYPE)
                            .startObject()
                            .field("sub-field", "true")
                            .endObject()
                    )
                )
            );
            assertTrue(e.getMessage(), e.getMessage().contains("failed to parse field [_mock_metadata]"));
        }
        {
            ParsedDocument doc = mapper.parse(
                source(b -> b.field(MockMetadataMapperPlugin.MockMetadataMapper.CONTENT_TYPE, "mock-metadata-field-value"))
            );
            IndexableField field = doc.rootDoc().getField(MockMetadataMapperPlugin.MockMetadataMapper.CONTENT_TYPE);
            assertEquals("mock-metadata-field-value", field.stringValue());
        }
    }

    public void testSimpleMapper() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("name");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("first");
                    {
                        b.field("type", "text");
                        b.field("store", "true");
                        b.field("index", false);
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        Document doc = docMapper.parse(source(b -> {
            b.startObject("name");
            {
                b.field("first", "fred");
                b.field("last", "quxx");
            }
            b.endObject();
        })).rootDoc();

        assertThat(doc.get(docMapper.mappers().getMapper("name.first").name()), equalTo("fred"));
    }

    public void testParseToJsonAndParse() throws Exception {
        String mapping = copyToStringFromClasspath("/org/opensearch/index/mapper/simple/test-mapping.json");
        MapperService mapperService = createMapperService(mapping(b -> {}));
        merge(MapperService.SINGLE_MAPPING_NAME, mapperService, mapping);
        String builtMapping = mapperService.documentMapper().mappingSource().string();
        // reparse it
        DocumentMapper builtDocMapper = createDocumentMapper(MapperService.SINGLE_MAPPING_NAME, builtMapping);
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/opensearch/index/mapper/simple/test1.json"));
        Document doc = builtDocMapper.parse(new SourceToParse("test", "1", json, MediaTypeRegistry.JSON)).rootDoc();
        assertThat(doc.getBinaryValue(builtDocMapper.idFieldMapper().name()), equalTo(Uid.encodeId("1")));
        assertThat(doc.get(builtDocMapper.mappers().getMapper("name.first").name()), equalTo("fred"));
    }

    public void testSimpleParser() throws Exception {
        String mapping = copyToStringFromClasspath("/org/opensearch/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = createDocumentMapper(MapperService.SINGLE_MAPPING_NAME, mapping);

        assertThat((String) docMapper.meta().get("param1"), equalTo("value1"));

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/opensearch/index/mapper/simple/test1.json"));
        Document doc = docMapper.parse(new SourceToParse("test", "1", json, MediaTypeRegistry.JSON)).rootDoc();
        assertThat(doc.getBinaryValue(docMapper.idFieldMapper().name()), equalTo(Uid.encodeId("1")));
        assertThat(doc.get(docMapper.mappers().getMapper("name.first").name()), equalTo("fred"));
    }

    public void testSimpleParserNoTypeNoId() throws Exception {
        String mapping = copyToStringFromClasspath("/org/opensearch/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = createDocumentMapper(MapperService.SINGLE_MAPPING_NAME, mapping);
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/opensearch/index/mapper/simple/test1-notype-noid.json"));
        Document doc = docMapper.parse(new SourceToParse("test", "1", json, MediaTypeRegistry.JSON)).rootDoc();
        assertThat(doc.getBinaryValue(docMapper.idFieldMapper().name()), equalTo(Uid.encodeId("1")));
        assertThat(doc.get(docMapper.mappers().getMapper("name.first").name()), equalTo("fred"));
    }

    public void testAttributes() throws Exception {
        String mapping = copyToStringFromClasspath("/org/opensearch/index/mapper/simple/test-mapping.json");

        DocumentMapper docMapper = createDocumentMapper(MapperService.SINGLE_MAPPING_NAME, mapping);

        assertThat((String) docMapper.meta().get("param1"), equalTo("value1"));

        String builtMapping = docMapper.mappingSource().string();
        DocumentMapper builtDocMapper = createDocumentMapper(MapperService.SINGLE_MAPPING_NAME, builtMapping);
        assertThat((String) builtDocMapper.meta().get("param1"), equalTo("value1"));
    }

    public void testNoDocumentSent() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        BytesReference json = new BytesArray("".getBytes(StandardCharsets.UTF_8));
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> docMapper.parse(new SourceToParse("test", "1", json, MediaTypeRegistry.JSON))
        );
        assertThat(e.getMessage(), equalTo("failed to parse, document is empty"));
    }

    public void testNoLevel() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.field("test1", "value1");
            b.field("test2", "value2");
            b.startObject("inner").field("inner_field", "inner_value").endObject();
        }));
        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("inner.inner_field"), equalTo("inner_value"));
    }

    // TODO do we still need all this tests for 'type' at the bottom level now that
    // we no longer have types?

    public void testTypeLevel() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("type");
            {
                b.field("test1", "value1");
                b.field("test2", "value2");
                b.startObject("inner").field("inner_field", "inner_value").endObject();
            }
            b.endObject();
        }));

        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testNoLevelWithFieldTypeAsValue() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.field("type", "value_type");
            b.field("test1", "value1");
            b.field("test2", "value2");
            b.startObject("inner").field("inner_field", "inner_value").endObject();
        }));

        assertThat(doc.rootDoc().get("type"), equalTo("value_type"));
        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("inner.inner_field"), equalTo("inner_value"));
    }

    public void testTypeLevelWithFieldTypeAsValue() throws Exception {

        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("type");
            {
                b.field("type", "value_type");
                b.field("test1", "value1");
                b.field("test2", "value2");
                b.startObject("inner").field("inner_field", "inner_value").endObject();
            }
            b.endObject();
        }));

        assertThat(doc.rootDoc().get("type.type"), equalTo("value_type"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testNoLevelWithFieldTypeAsObject() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("type").field("type_field", "type_value").endObject();
            b.field("test1", "value1");
            b.field("test2", "value2");
            b.startObject("inner").field("inner_field", "inner_value").endObject();
        }));

        // in this case, we analyze the type object as the actual document, and ignore the other same level fields
        assertThat(doc.rootDoc().get("type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
    }

    public void testTypeLevelWithFieldTypeAsObject() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("type");
            {
                b.startObject("type").field("type_field", "type_value").endObject();
                b.field("test1", "value1");
                b.field("test2", "value2");
                b.startObject("inner").field("inner_field", "inner_value").endObject();
            }
            b.endObject();
        }));

        assertThat(doc.rootDoc().get("type.type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testNoLevelWithFieldTypeAsValueNotFirst() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("type");
            {
                b.field("test1", "value1");
                b.field("test2", "value2");
                b.field("type", "type_value");
                b.startObject("inner").field("inner_field", "inner_value").endObject();
            }
            b.endObject();
        }));

        assertThat(doc.rootDoc().get("type.type"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testTypeLevelWithFieldTypeAsValueNotFirst() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("type");
            {
                b.field("test1", "value1");
                b.field("type", "type_value");
                b.field("test2", "value2");
                b.startObject("inner").field("inner_field", "inner_value").endObject();
            }
            b.endObject();
        }));

        assertThat(doc.rootDoc().get("type.type"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testNoLevelWithFieldTypeAsObjectNotFirst() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.field("test1", "value1");
            b.startObject("type").field("type_field", "type_value").endObject();
            b.field("test2", "value2");
            b.startObject("inner").field("inner_field", "inner_value").endObject();
        }));

        // when the type is not the first one, we don't confuse it...
        assertThat(doc.rootDoc().get("type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("inner.inner_field"), equalTo("inner_value"));
    }

    public void testTypeLevelWithFieldTypeAsObjectNotFirst() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("type");
            {
                b.field("test1", "value1");
                b.startObject("type").field("type_field", "type_value").endObject();
                b.field("test2", "value2");
                b.startObject("inner").field("inner_field", "inner_value").endObject();
            }
            b.endObject();
        }));

        assertThat(doc.rootDoc().get("type.type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testDynamicDateDetectionDisabledOnNumbers() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.startArray("dynamic_date_formats").value("yyyy").endArray()));

        // Even though we matched the dynamic format, we do not match on numbers,
        // which are too likely to be false positives
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo", "2016")));
        Mapping update = doc.dynamicMappingsUpdate();
        assertNotNull(update);
        Mapper dateMapper = update.root().getMapper("foo");
        assertNotNull(dateMapper);
        assertThat(dateMapper, not(instanceOf(DateFieldMapper.class)));
    }

    public void testDynamicDateDetectionEnabledWithNoSpecialCharacters() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.startArray("dynamic_date_formats").value("yyyy MM").endArray()));

        // We should have generated a date field
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo", "2016 12")));
        Mapping update = doc.dynamicMappingsUpdate();
        assertNotNull(update);
        Mapper dateMapper = update.root().getMapper("foo");
        assertNotNull(dateMapper);
        assertThat(dateMapper, instanceOf(DateFieldMapper.class));
    }

    public void testDynamicFieldsStartingAndEndingWithDot() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        merge(mapperService, dynamicMapping(mapperService.documentMapper().parse(source(b -> {
            b.startArray("top.");
            {
                b.startObject();
                {
                    b.startArray("foo.");
                    {
                        b.startObject().field("thing", "bah").endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }
            b.endArray();
        })).dynamicMappingsUpdate()));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> mapperService.documentMapper().parse(source(b -> {
            b.startArray("top.");
            {
                b.startObject();
                {
                    b.startArray("foo.");
                    {
                        b.startObject();
                        {
                            b.startObject("bar.");
                            {
                                b.startObject("aoeu").field("a", 1).field("b", 2).endObject();
                            }
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }
            b.endArray();
        })));

        assertThat(
            e.getMessage(),
            containsString("object field starting or ending with a [.] makes object resolution ambiguous: [top..foo..bar]")
        );
    }

    public void testDynamicFieldsWithOnlyDot() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("top");
            {
                b.startObject();
                {
                    b.startObject("inner").field(".", 2).endObject();
                }
                b.endObject();
            }
            b.endArray();
        })));

        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause().getMessage(), containsString("field name cannot contain only the character [.]"));

        e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> { b.startObject("..").field("foo", 2).endObject(); }))
        );

        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause().getMessage(), containsString("field name cannot contain only the character [.]"));

        e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field(".", "1234"))));

        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause().getMessage(), containsString("field name cannot contain only the character [.]"));
    }

    public void testDynamicFieldsEmptyName() throws Exception {

        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));

        IllegalArgumentException emptyFieldNameException = expectThrows(IllegalArgumentException.class, () -> mapper.parse(source(b -> {
            b.startArray("top.");
            {
                b.startObject();
                {
                    b.startObject("aoeu").field("a", 1).field(" ", 2).endObject();
                }
                b.endObject();
            }
            b.endArray();
        })));

        assertThat(emptyFieldNameException.getMessage(), containsString("object field cannot contain only whitespace: ['top.aoeu. ']"));
    }

    public void testBlankFieldNames() throws Exception {

        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        MapperParsingException err = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("", "foo"))));
        assertThat(err.getCause(), notNullValue());
        assertThat(err.getCause().getMessage(), containsString("field name cannot be an empty string"));

        err = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("foo").field("", "bar").endObject()))
        );
        assertThat(err.getCause(), notNullValue());
        assertThat(err.getCause().getMessage(), containsString("field name cannot be an empty string"));
    }

    public void testWriteToFieldAlias() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("alias-field");
            {
                b.field("type", "alias");
                b.field("path", "concrete-field");
            }
            b.endObject();
            b.startObject("concrete-field").field("type", "keyword").endObject();
        }));

        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("alias-field", "value")))
        );

        assertEquals("Cannot write to a field alias [alias-field].", exception.getCause().getMessage());
    }

    public void testCopyToFieldAlias() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("alias-field");
            {
                b.field("type", "alias");
                b.field("path", "concrete-field");
            }
            b.endObject();
            b.startObject("concrete-field").field("type", "keyword").endObject();
            b.startObject("text-field");
            {
                b.field("type", "text");
                b.field("copy_to", "alias-field");
            }
            b.endObject();
        }));

        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("text-field", "value")))
        );

        assertEquals("Cannot copy to a field alias [alias-field].", exception.getCause().getMessage());
    }

    public void testDynamicDottedFieldNameWithFieldAlias() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("alias-field");
            {
                b.field("type", "alias");
                b.field("path", "concrete-field");
            }
            b.endObject();
            b.startObject("concrete-field").field("type", "keyword").endObject();
        }));

        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("alias-field.dynamic-field").field("type", "keyword").endObject()))
        );

        assertEquals(
            "Could not dynamically add mapping for field [alias-field.dynamic-field]. "
                + "Existing mapping for [alias-field] must be of type object but found [alias].",
            exception.getMessage()
        );
    }

    public void testTypeless() throws IOException {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .startObject("properties")
            .startObject("foo")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        DocumentMapper mapper = createDocumentMapper(MapperService.SINGLE_MAPPING_NAME, mapping);

        ParsedDocument doc = mapper.parse(source(b -> b.field("foo", "1234")));
        assertNull(doc.dynamicMappingsUpdate()); // no update since we reused the existing type
    }

    public void testDocumentContainsDeepNestedFieldParsing() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("inner1");
            {
                b.field("inner1_field1", "inner1_value1");
                b.startObject("inner2");
                {
                    b.startObject("inner3");
                    {
                        b.field("inner3_field1", "inner3_value1");
                        b.field("inner3_field2", "inner3_value2");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        Mapping update = doc.dynamicMappingsUpdate();
        assertNotNull(update); // dynamic mapping update

        Mapper objMapper = update.root().getMapper("inner1");
        Mapper inner1_field1_mapper = ((ObjectMapper) objMapper).getMapper("inner1_field1");
        assertNotNull(inner1_field1_mapper);
        Mapper inner2_mapper = ((ObjectMapper) objMapper).getMapper("inner2");
        assertNotNull(inner2_mapper);
        Mapper inner3_mapper = ((ObjectMapper) inner2_mapper).getMapper("inner3");
        assertNotNull(inner3_mapper);
        assertThat(doc.rootDoc().get("inner1.inner2.inner3.inner3_field1"), equalTo("inner3_value1"));
    }

    public void testDocumentContainsDeepNestedFieldParsingFail() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        long depth_limit = MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getDefault(Settings.EMPTY);
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> {
            for (int i = 1; i <= depth_limit; i++) {
                b.startObject("inner" + i);
            }
            b.field("inner_field", "inner_value");
            for (int i = 1; i <= depth_limit; i++) {
                b.endObject();
            }
        })));

        // check that parsing succeeds with valid doc
        // after throwing exception
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("inner1");
            {
                b.startObject("inner2");
                {
                    b.startObject("inner3");
                    {
                        b.field("inner3_field1", "inner3_value1");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        Mapping update = doc.dynamicMappingsUpdate();
        assertNotNull(update); // dynamic mapping update
        Mapper objMapper = update.root().getMapper("inner1");
        Mapper inner2_mapper = ((ObjectMapper) objMapper).getMapper("inner2");
        assertNotNull(inner2_mapper);
        Mapper inner3_mapper = ((ObjectMapper) inner2_mapper).getMapper("inner3");
        assertNotNull(inner3_mapper);
        assertThat(doc.rootDoc().get("inner1.inner2.inner3.inner3_field1"), equalTo("inner3_value1"));
    }

    public void testDocumentContainsDeepNestedFieldParsingShouldFail() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> { b.field("type", "nested"); }));
        long depth_limit = MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getDefault(Settings.EMPTY);
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> {
            b.startObject("field");
            b.startArray("inner");
            for (int i = 1; i <= depth_limit; i++) {
                b.startArray();
            }
            b.startArray().value(0).value(0).endArray();
            for (int i = 1; i <= depth_limit; i++) {
                b.endArray();
            }
            b.endArray();
            b.endObject();
        })));
        // check parsing success for nested array within allowed depth limit
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("field");
            b.startArray("inner");
            for (int i = 1; i < depth_limit - 1; i++) {
                b.startArray();
            }
            b.startArray().value(0).value(0).endArray();
            for (int i = 1; i < depth_limit - 1; i++) {
                b.endArray();
            }
            b.endArray();
            b.endObject();
        }

        ));
        Mapping update = doc.dynamicMappingsUpdate();
        assertNotNull(update); // dynamic mapping update

    }

    // Test nesting upto max allowed depth with combination of nesting in object and array
    // object -> array -> object -> array ....
    public void testDocumentDeepNestedObjectAndArrayCombination() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        long depth_limit = MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getDefault(Settings.EMPTY);
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> {
            for (int i = 1; i < depth_limit; i++) {
                b.startArray("foo" + 1);
                b.startObject();
            }
            b.startArray("bar");
            b.startArray().value(0).value(0).endArray();
            b.endArray();
            for (int i = 1; i < depth_limit; i++) {
                b.endObject();
                b.endArray();
            }
        })));

        // check parsing success for nested array within allowed depth limit
        ParsedDocument doc = mapper.parse(source(b -> {
            for (int i = 1; i < depth_limit - 1; i++) {
                b.startArray("foo" + 1);
                b.startObject();
            }
            b.startArray("bar");
            b.startArray().value(0).value(0).endArray();
            b.endArray();
            for (int i = 1; i < depth_limit - 1; i++) {
                b.endObject();
                b.endArray();
            }
        }

        ));
        Mapping update = doc.dynamicMappingsUpdate();
        assertNotNull(update); // dynamic mapping update

    }

    public void testDynamicFalseAllowTemplatesLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false_allow_templates")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo").length);

        DocumentMapper mapperWithTemplate = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "foo");
                        b.startObject("mapping").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument docWithTemplate = mapperWithTemplate.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()));
        assertEquals(4, docWithTemplate.rootDoc().getFields("foo").length);
    }

    public void testDynamicFalseAllowTemplatesObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "test");
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            {
                b.startObject();
                {
                    b.startObject("test1");
                    {
                        b.field("match", "test1");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }

        ));

        ParsedDocument doc = mapper.parse(source(b -> b.startObject("test").field("test1", "foo").endObject()));
        assertEquals(2, doc.rootDoc().getFields("test.test1").length);
    }

    public void testDynamicFalseAllowTemplatesValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false_allow_templates")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("bar", "baz")));
        assertEquals(0, doc.rootDoc().getFields("bar").length);

        DocumentMapper mapperWithTemplate = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "bar");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument docWithTemplate = mapperWithTemplate.parse(source(b -> b.field("bar", "baz")));
        assertEquals(2, docWithTemplate.rootDoc().getFields("bar").length);
    }

    public void testDynamicFalseAllowTemplatesNull() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false_allow_templates")));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("bar")));
        assertEquals(0, doc.rootDoc().getFields("bar").length);

        DocumentMapper mapperWithTemplate = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "bar");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument docWithTemplate = mapperWithTemplate.parse(source(b -> b.nullField("bar")));
        assertEquals(0, docWithTemplate.rootDoc().getFields("bar").length); // null fields don't create mappings
    }

    public void testDynamicFalseAllowTemplatesDottedFieldNameLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false_allow_templates")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo.bar.baz").value(0).value(1).endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz").length);

        DocumentMapper mapperWithTemplate = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "foo");
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            {
                b.startObject();
                {
                    b.startObject("test1");
                    {
                        b.field("match", "bar");
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            {
                b.startObject();
                {
                    b.startObject("test2");
                    {
                        b.field("path_match", "foo.bar.baz");
                        b.startObject("mapping").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument docWithTemplate = mapperWithTemplate.parse(source(b -> b.startArray("foo.bar.baz").value(0).value(1).endArray()));
        assertEquals(4, docWithTemplate.rootDoc().getFields("foo.bar.baz").length);
    }

    public void testDynamicFalseAllowTemplatesDottedFieldNameLong() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false_allow_templates")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo.bar.baz", 0)));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz").length);

        DocumentMapper mapperWithTemplate = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "foo");
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            {
                b.startObject();
                {
                    b.startObject("test1");
                    {
                        b.field("match", "bar");
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            {
                b.startObject();
                {
                    b.startObject("test2");
                    {
                        b.field("path_match", "foo.bar.baz");
                        b.startObject("mapping").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument docWithTemplate = mapperWithTemplate.parse(source(b -> b.field("foo.bar.baz", 0)));
        assertEquals(2, docWithTemplate.rootDoc().getFields("foo.bar.baz").length);
    }

    public void testDynamicFalseAllowTemplatesDottedFieldNameObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false_allow_templates")));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz.a").length);

        DocumentMapper mapperWithTemplate = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "foo");
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            {
                b.startObject();
                {
                    b.startObject("test1");
                    {
                        b.field("match", "bar");
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            {
                b.startObject();
                {
                    b.startObject("test2");
                    {
                        b.field("path_match", "foo.bar.baz");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument docWithTemplate = mapperWithTemplate.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()));
        assertEquals(2, docWithTemplate.rootDoc().getFields("foo.bar.baz.a").length);
    }

    public void testDynamicFalseAllowTemplatesWithEmbeddedObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false_allow_templates")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("embedded_field", "value")));
        assertEquals(0, doc.rootDoc().getFields("embedded_field").length);

        DocumentMapper mapperWithTemplate = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("embedded_template");
                    {
                        b.field("match", "embedded_*");
                        b.field("match_mapping_type", "string");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument docWithTemplate = mapperWithTemplate.parse(source(b -> b.field("embedded_field", "value")));
        assertEquals(2, docWithTemplate.rootDoc().getFields("embedded_field").length);
    }

    public void testDynamicFalseAllowTemplatesWithDateDetection() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_date_formats").value("yyyy-MM-dd").endArray();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("date_field", "2023-12-25")));
        assertEquals(0, doc.rootDoc().getFields("date_field").length);

        DocumentMapper mapperWithTemplate = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_date_formats").value("yyyy-MM-dd").endArray();
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("date_template");
                    {
                        b.field("match", "date_*");
                        b.field("match_mapping_type", "date");
                        b.startObject("mapping").field("type", "date").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument docWithTemplate = mapperWithTemplate.parse(source(b -> b.field("date_field", "2023-12-25")));
        assertEquals(2, docWithTemplate.rootDoc().getFields("date_field").length);
    }

    public void testDynamicFalseAllowTemplatesWithNumericDetection() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false_allow_templates")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("numeric_field", "123")));
        assertEquals(0, doc.rootDoc().getFields("numeric_field").length);

        DocumentMapper mapperWithTemplate = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("numeric_template");
                    {
                        b.field("match", "numeric_*");
                        b.field("match_mapping_type", "long");
                        b.startObject("mapping").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument docWithTemplate = mapperWithTemplate.parse(source(b -> b.field("numeric_field", "123")));
        assertEquals(0, docWithTemplate.rootDoc().getFields("numeric_field").length);
    }

    public void testDynamicFalseAllowTemplatesWithFloatDetection() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false_allow_templates")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("float_field", "123.45")));
        assertEquals(0, doc.rootDoc().getFields("float_field").length);

        DocumentMapper mapperWithTemplate = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("float_template");
                    {
                        b.field("match", "float_*");
                        b.field("match_mapping_type", "double");
                        b.startObject("mapping").field("type", "float").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument docWithTemplate = mapperWithTemplate.parse(source(b -> b.field("float_field", "123.45")));
        assertEquals(0, docWithTemplate.rootDoc().getFields("float_field").length);
    }

    public void testDynamicFalseAllowTemplatesWithBooleanDetection() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false_allow_templates")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("bool_field", true)));
        assertEquals(0, doc.rootDoc().getFields("bool_field").length);

        DocumentMapper mapperWithTemplate = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("boolean_template");
                    {
                        b.field("match", "bool_*");
                        b.field("match_mapping_type", "boolean");
                        b.startObject("mapping").field("type", "boolean").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument docWithTemplate = mapperWithTemplate.parse(source(b -> b.field("bool_field", true)));
        assertEquals(2, docWithTemplate.rootDoc().getFields("bool_field").length);
    }

    public void testDynamicFalseAllowTemplatesWithBigInteger() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false_allow_templates")));
        BigInteger bigInt = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        ParsedDocument doc = mapper.parse(source(b -> b.field("bigint_field", bigInt)));
        assertEquals(0, doc.rootDoc().getFields("bigint_field").length);

        DocumentMapper mapperWithTemplate = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("bigint_template");
                    {
                        b.field("match", "bigint_*");
                        b.field("match_mapping_type", "long");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument docWithTemplate = mapperWithTemplate.parse(source(b -> b.field("bigint_field", bigInt)));
        assertEquals(2, docWithTemplate.rootDoc().getFields("bigint_field").length);
    }

    public void testDynamicFalseAllowTemplatesWithBigDecimal() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false_allow_templates")));
        BigDecimal bigDecimal = BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.valueOf(10.1));
        ParsedDocument doc = mapper.parse(source(b -> b.field("bigdecimal_field", bigDecimal)));
        assertEquals(0, doc.rootDoc().getFields("bigdecimal_field").length);

        DocumentMapper mapperWithTemplate = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("bigdecimal_template");
                    {
                        b.field("match", "bigdecimal_*");
                        b.field("match_mapping_type", "double");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument docWithTemplate = mapperWithTemplate.parse(source(b -> b.field("bigdecimal_field", bigDecimal)));
        assertEquals(2, docWithTemplate.rootDoc().getFields("bigdecimal_field").length);
    }

    public void testDynamicFalseAllowTemplatesWithCopyTo() throws Exception {
        DocumentMapper mapperWithDynamic = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("target_template");
                    {
                        b.field("match", "target_*");
                        b.field("match_mapping_type", "string");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("source_field");
            {
                b.field("type", "text");
                b.field("copy_to", "target_field");
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = mapperWithDynamic.parse(source(b -> b.field("source_field", "test value")));
        if (parsedDoc.dynamicMappingsUpdate() != null) {
            merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));
        }

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("source_field", "test value")));
        assertEquals(1, doc.rootDoc().getFields("source_field").length);
        assertEquals(1, doc.rootDoc().getFields("target_field").length);
    }

    public void testDynamicFalseAllowTemplatesWithNestedCopyTo() throws Exception {
        DocumentMapper mapperWithDynamic = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("nested_template");
                    {
                        b.field("match", "nested");
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            {
                b.startObject();
                {
                    b.startObject("target_template");
                    {
                        b.field("match", "target_*");
                        b.field("match_mapping_type", "string");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("nested");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("source_field");
                    {
                        b.field("type", "text");
                        b.field("copy_to", "nested.target_field");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = mapperWithDynamic.parse(source(b -> {
            b.startObject("nested");
            {
                b.field("source_field", "test value");
            }
            b.endObject();
        }));
        if (parsedDoc.dynamicMappingsUpdate() != null) {
            merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));
        }

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.startObject("nested");
            {
                b.field("source_field", "test value");
            }
            b.endObject();
        }));
        assertEquals(1, doc.rootDoc().getFields("nested.source_field").length);
        // Copying to a field that is not in the mapping should not create a field in the document
        assertEquals(0, doc.rootDoc().getFields("nested.target_field").length);
    }

    public void testGenerateGroupingCriteriaFromScript() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            contextAwareGrouping("status_code").accept(b);
            properties(x -> {
                x.startObject("status_code");
                b.field("type", "integer");
                b.endObject();
            }).accept(b);
        }));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.field("status_code", "300");
            b.field("bar", 10);
        }));

        assertEquals("300", doc.docs().getFirst().getGroupingCriteria());
    }

    // Helper method to create a mapper with disable_objects configuration
    private DocumentMapper createDisableObjectsMapper(boolean rootDisableObjects, String objectPath, boolean objectDisableObjects)
        throws IOException {
        return createDocumentMapper(topMapping(b -> {
            if (rootDisableObjects) {
                b.field("disable_objects", true);
            }
            b.field("dynamic", "true");
            if (objectPath != null) {
                b.startObject("properties");
                {
                    b.startObject(objectPath);
                    {
                        b.field("type", "object");
                        b.field("disable_objects", objectDisableObjects);
                    }
                    b.endObject();
                }
                b.endObject();
            }
        }));
    }

    public void testDisableObjectsBasicFunctionality() throws Exception {
        // Comprehensive test for basic disable_objects functionality

        // Test 1: Flat fields with disable_objects=true
        DocumentMapper flatFieldMapper = createDocumentMapper(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("metrics");
                {
                    b.field("type", "object");
                    b.field("disable_objects", true);
                    b.startObject("properties");
                    {
                        b.startObject("cpu.usage");
                        {
                            b.field("type", "float");
                        }
                        b.endObject();
                        b.startObject("memory.used");
                        {
                            b.field("type", "long");
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument flatDoc = flatFieldMapper.parse(source(b -> {
            b.startObject("metrics");
            {
                b.field("cpu.usage", 75.5);
                b.field("memory.used", 8589934592L);
            }
            b.endObject();
        }));

        // Verify flat field storage
        assertNotNull("Field 'metrics.cpu.usage' should exist", flatDoc.rootDoc().getField("metrics.cpu.usage"));
        assertEquals(75.5f, flatDoc.rootDoc().getField("metrics.cpu.usage").numericValue().floatValue(), 0.001f);
        assertNotNull("Field 'metrics.memory.used' should exist", flatDoc.rootDoc().getField("metrics.memory.used"));
        assertEquals(8589934592L, flatDoc.rootDoc().getField("metrics.memory.used").numericValue().longValue());
        assertNull("No intermediate objects should exist", flatDoc.rootDoc().getField("metrics.cpu"));

        // Test 2: Dynamic fields with disable_objects=true
        DocumentMapper dynamicMapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "true");
            b.startObject("properties");
            {
                b.startObject("metrics");
                {
                    b.field("type", "object");
                    b.field("disable_objects", true);
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument dynamicDoc = dynamicMapper.parse(source(b -> {
            b.startObject("metrics");
            {
                b.field("cpu.usage", 75.5);
                b.field("memory.used", 8589934592L);
            }
            b.endObject();
        }));

        assertNotNull("Dynamic field 'metrics.cpu.usage' should exist", dynamicDoc.rootDoc().getField("metrics.cpu.usage"));
        assertNotNull("Dynamic field 'metrics.memory.used' should exist", dynamicDoc.rootDoc().getField("metrics.memory.used"));

        // Test 3: Default behavior (disable_objects=false)
        DocumentMapper defaultMapper = createDocumentMapper(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("user");
                {
                    b.field("type", "object");
                    b.startObject("properties");
                    {
                        b.startObject("name");
                        {
                            b.field("type", "text");
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument defaultDoc = defaultMapper.parse(source(b -> {
            b.startObject("user");
            {
                b.field("name", "Jane Doe");
            }
            b.endObject();
        }));

        assertNotNull("Field 'user.name' should exist in nested structure", defaultDoc.rootDoc().getField("user.name"));
    }

    public void testRootLevelDisableObjects() throws Exception {
        // Comprehensive test for root-level disable_objects functionality

        // Test 1: Root-level disable_objects with dynamic fields
        DocumentMapper rootMapper = createDocumentMapper(topMapping(b -> {
            b.field("disable_objects", true);
            b.field("dynamic", "true");
            b.startObject("properties");
            {
                b.startObject("metrics.cpu.usage");
                {
                    b.field("type", "float");
                }
                b.endObject();
            }
            b.endObject();
        }));

        // Test predefined field
        ParsedDocument doc1 = rootMapper.parse(source(b -> { b.field("metrics.cpu.usage", 55.5); }));
        assertNotNull("Field 'metrics.cpu.usage' should exist", doc1.rootDoc().getField("metrics.cpu.usage"));

        // Test new dynamic field with shared prefix
        ParsedDocument doc2 = rootMapper.parse(source(b -> { b.field("metrics.cpu", 99.1); }));
        assertNotNull("Dynamic field 'metrics.cpu' should exist as flat field", doc2.rootDoc().getField("metrics.cpu"));

        Mapping mappingUpdate = doc2.dynamicMappingsUpdate();
        assertNotNull("Dynamic mapping update should be created", mappingUpdate);
        Mapper cpuMapper = mappingUpdate.root().getMapper("metrics.cpu");
        assertNotNull("Mapper for 'metrics.cpu' should exist", cpuMapper);
        assertThat("Mapper should be a FieldMapper", cpuMapper, instanceOf(FieldMapper.class));
    }

    public void testDisableObjectsAutomaticFlattening() throws Exception {
        // Test comprehensive automatic flattening scenarios with disable_objects=true
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("disable_objects", true);
            b.field("dynamic", "true");
        }));

        // Test deeply nested objects, multiple fields, and mixed scenarios
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("user");
            {
                b.field("name", "John");
                b.field("age", 30);
                b.startObject("address");
                {
                    b.field("city", "NYC");
                    b.field("zip", "10001");
                    b.startObject("coordinates");
                    {
                        b.field("lat", 40.7128);
                        b.field("lon", -74.0060);
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        // Verify all fields were flattened correctly
        assertNotNull("Field 'user.name' should exist", doc.rootDoc().getField("user.name"));
        assertEquals("John", doc.rootDoc().getField("user.name").stringValue());

        assertNotNull("Field 'user.age' should exist", doc.rootDoc().getField("user.age"));
        assertEquals(30L, doc.rootDoc().getField("user.age").numericValue().longValue());

        assertNotNull("Field 'user.address.city' should exist", doc.rootDoc().getField("user.address.city"));
        assertEquals("NYC", doc.rootDoc().getField("user.address.city").stringValue());

        assertNotNull("Field 'user.address.zip' should exist", doc.rootDoc().getField("user.address.zip"));
        assertEquals("10001", doc.rootDoc().getField("user.address.zip").stringValue());
    }

    public void testDisableObjectsWithStrictDynamicMapping() throws Exception {
        // Test that strict dynamic mapping works correctly with disable_objects
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("disable_objects", true);
            b.field("dynamic", "strict");
        }));

        // Should throw exception when trying to add new field with strict mapping
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class, () -> {
            mapper.parse(source(b -> b.field("new.field", "value")));
        });

        assertThat(exception.getMessage(), containsString("strict"));
    }

    public void testDisableObjectsWithExistingFieldMappers() throws Exception {
        // Test that existing field mappers work correctly with disable_objects
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("disable_objects", true);
            b.startObject("properties");
            {
                b.startObject("user.name");
                {
                    b.field("type", "text");
                }
                b.endObject();
                b.startObject("user.age");
                {
                    b.field("type", "integer");
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.field("user.name", "John Doe");
            b.field("user.age", 30);
        }));

        // Both fields should be properly mapped using existing field mappers
        assertNotNull("Field 'user.name' should exist", doc.rootDoc().getField("user.name"));
        assertEquals("John Doe", doc.rootDoc().getField("user.name").stringValue());

        assertNotNull("Field 'user.age' should exist", doc.rootDoc().getField("user.age"));
        assertEquals(30, doc.rootDoc().getField("user.age").numericValue().intValue());
    }

    public void testExistingMapperLookupWithDisableObjectsParent() throws Exception {
        // Test scenario with disable_objects parent to ensure correct flattening behavior
        // disable_objects should only be set at ONE place, not multiple levels

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "true");
            b.startObject("properties");
            {
                b.startObject("outer");
                {
                    b.field("type", "object");
                    b.field("disable_objects", true);
                    // Note: No nested object mappers defined when disable_objects=true
                    // All nested structures will be flattened during document parsing
                }
                b.endObject();
            }
            b.endObject();
        }));

        // Test that nested object structures are automatically flattened
        ParsedDocument doc1 = mapper.parse(source(b -> {
            b.startObject("outer");
            {
                b.startObject("inner");
                {
                    b.field("test", "value");
                }
                b.endObject();
            }
            b.endObject();
        }));

        // Should be flattened to outer.inner.test
        assertNotNull("Field 'outer.inner.test' should exist", doc1.rootDoc().getField("outer.inner.test"));
        assertEquals("value", doc1.rootDoc().getField("outer.inner.test").stringValue());

        // Test that a deeply nested field is also flattened correctly
        ParsedDocument doc2 = mapper.parse(source(b -> {
            b.startObject("outer");
            {
                b.startObject("deep");
                {
                    b.startObject("nested");
                    {
                        b.field("field", "testvalue");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        // Should be flattened to outer.deep.nested.field
        assertNotNull("Field 'outer.deep.nested.field' should exist", doc2.rootDoc().getField("outer.deep.nested.field"));
        assertEquals("testvalue", doc2.rootDoc().getField("outer.deep.nested.field").stringValue());

        // Test that dotted field names are treated as literal field names
        ParsedDocument doc3 = mapper.parse(source(b -> {
            b.startObject("outer");
            {
                b.field("literal.field.name", "literalvalue");
            }
            b.endObject();
        }));

        assertNotNull("Field 'outer.literal.field.name' should exist", doc3.rootDoc().getField("outer.literal.field.name"));
        assertEquals("literalvalue", doc3.rootDoc().getField("outer.literal.field.name").stringValue());
    }

    public void testRootMapperNameVariations() throws Exception {
        // Test to ensure both empty and "_doc" root mapper names are handled correctly
        // This comprehensively tests the condition: parentPath.isEmpty() || parentPath.equals("_doc")

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("disable_objects", true);
            b.field("dynamic", "true");
        }));

        // Test with array of objects containing dotted field names
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("items");
            {
                b.startObject();
                {
                    b.field("item.id", "item1");
                    b.field("item.category.name", "electronics");
                }
                b.endObject();
                b.startObject();
                {
                    b.field("item.id", "item2");
                    b.field("item.category.name", "books");
                }
                b.endObject();
            }
            b.endArray();
        }));

        // All fields should be flattened
        IndexableField[] itemIdFields = doc.rootDoc().getFields("items.item.id");
        assertTrue("Should have item.id fields", itemIdFields.length > 0);

        IndexableField[] categoryFields = doc.rootDoc().getFields("items.item.category.name");
        assertTrue("Should have item.category.name fields", categoryFields.length > 0);

        // Test edge case with deeply nested structure
        ParsedDocument doc2 = mapper.parse(source(b -> {
            b.startObject("a");
            {
                b.startObject("b");
                {
                    b.startObject("c");
                    {
                        b.startObject("d");
                        {
                            b.field("e.f.g", "deepvalue");
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        assertNotNull("Field 'a.b.c.d.e.f.g' should exist", doc2.rootDoc().getField("a.b.c.d.e.f.g"));
        assertEquals("deepvalue", doc2.rootDoc().getField("a.b.c.d.e.f.g").stringValue());
    }

    public void testPathSplittingAndReconstructionInExistingMapperLookup() throws Exception {
        // Test the path splitting logic: String[] pathParts = fieldPath.split("\\.");
        // and reconstruction: String.join(".", java.util.Arrays.copyOf(pathParts, i))
        // in the existing mapper lookup loop

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "true");
            b.startObject("properties");
            {
                b.startObject("a");
                {
                    b.field("type", "object");
                    b.startObject("properties");
                    {
                        b.startObject("b");
                        {
                            b.field("type", "object");
                            b.startObject("properties");
                            {
                                b.startObject("c");
                                {
                                    b.field("type", "object");
                                    b.field("disable_objects", true);
                                }
                                b.endObject();
                            }
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        // First establish the existing mapper structure
        ParsedDocument doc1 = mapper.parse(source(b -> {
            b.startObject("a");
            {
                b.startObject("b");
                {
                    b.startObject("c");
                    {
                        b.field("existing", "test");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        // Test various field paths that should trigger the path splitting logic
        // Field: "a.b.c.d.e.f" should find parent "a.b.c" with disable_objects=true
        ParsedDocument doc2 = mapper.parse(source(b -> { b.field("a.b.c.d.e.f", "value1"); }));

        assertNotNull("Field 'a.b.c.d.e.f' should exist", doc2.rootDoc().getField("a.b.c.d.e.f"));
        assertEquals("value1", doc2.rootDoc().getField("a.b.c.d.e.f").stringValue());

        // Test with single level under disable_objects parent
        ParsedDocument doc3 = mapper.parse(source(b -> { b.field("a.b.c.single", "value2"); }));

        assertNotNull("Field 'a.b.c.single' should exist", doc3.rootDoc().getField("a.b.c.single"));
        assertEquals("value2", doc3.rootDoc().getField("a.b.c.single").stringValue());

        // Test with very deep nesting to exercise the path reconstruction loop
        ParsedDocument doc4 = mapper.parse(source(b -> { b.field("a.b.c.very.deep.nested.field.with.many.levels", "deepvalue"); }));

        assertNotNull(
            "Field 'a.b.c.very.deep.nested.field.with.many.levels' should exist",
            doc4.rootDoc().getField("a.b.c.very.deep.nested.field.with.many.levels")
        );
        assertEquals("deepvalue", doc4.rootDoc().getField("a.b.c.very.deep.nested.field.with.many.levels").stringValue());

        // Test edge case where field path exactly matches the disable_objects parent
        ParsedDocument doc5 = mapper.parse(source(b -> { b.field("a.b.c", "exactmatch"); }));

        assertNotNull("Field 'a.b.c' should exist", doc5.rootDoc().getField("a.b.c"));
        assertEquals("exactmatch", doc5.rootDoc().getField("a.b.c").stringValue());
    }

    public void testGetMapperFlatteningLogic() throws Exception {
        // Test the specific flattening logic in getMapper function
        // This covers the StringBuilder flatFieldName code that flattens remaining path segments
        // when encountering an ObjectMapper with disable_objects=true

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "true");
            b.startObject("properties");
            {
                b.startObject("level1");
                {
                    b.field("type", "object");
                    b.startObject("properties");
                    {
                        b.startObject("level2");
                        {
                            b.field("type", "object");
                            b.startObject("properties");
                            {
                                b.startObject("level3");
                                {
                                    b.field("type", "object");
                                    b.field("disable_objects", true);
                                    // level3 has disable_objects=true, so any deeper paths should be flattened
                                }
                                b.endObject();
                            }
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        // First establish the mapping structure
        ParsedDocument doc1 = mapper.parse(source(b -> {
            b.startObject("level1");
            {
                b.startObject("level2");
                {
                    b.startObject("level3");
                    {
                        b.field("simple.field", "test1");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        // Verify the simple case works
        assertNotNull(
            "Field 'level1.level2.level3.simple.field' should exist",
            doc1.rootDoc().getField("level1.level2.level3.simple.field")
        );
        assertEquals("test1", doc1.rootDoc().getField("level1.level2.level3.simple.field").stringValue());

        // Test the getMapper flattening logic: when looking up "level1.level2.level3.deep.nested.field.name"
        // the getMapper function should:
        // 1. Traverse: level1 → level2 → level3 (which has disable_objects=true)
        // 2. Flatten remaining path ["deep", "nested", "field", "name"] into "deep.nested.field.name"
        // 3. Look for field "deep.nested.field.name" under level3 mapper
        ParsedDocument doc2 = mapper.parse(source(b -> { b.field("level1.level2.level3.deep.nested.field.name", "flattened_value"); }));

        // This should create a field with the flattened name
        assertNotNull(
            "Field 'level1.level2.level3.deep.nested.field.name' should exist",
            doc2.rootDoc().getField("level1.level2.level3.deep.nested.field.name")
        );
        assertEquals("flattened_value", doc2.rootDoc().getField("level1.level2.level3.deep.nested.field.name").stringValue());

        // Test another case with multiple segments to flatten
        ParsedDocument doc3 = mapper.parse(source(b -> { b.field("level1.level2.level3.very.long.dotted.field.path", "another_value"); }));

        assertNotNull(
            "Field 'level1.level2.level3.very.long.dotted.field.path' should exist",
            doc3.rootDoc().getField("level1.level2.level3.very.long.dotted.field.path")
        );
        assertEquals("another_value", doc3.rootDoc().getField("level1.level2.level3.very.long.dotted.field.path").stringValue());

        // Test edge case where the path exactly matches the disable_objects parent
        ParsedDocument doc4 = mapper.parse(source(b -> { b.field("level1.level2.level3", "exact_match"); }));

        assertNotNull("Field 'level1.level2.level3' should exist", doc4.rootDoc().getField("level1.level2.level3"));
        assertEquals("exact_match", doc4.rootDoc().getField("level1.level2.level3").stringValue());
    }

    public void testRootLevelGetMapperFlattening() throws Exception {
        // Test the root-level flattening logic in getMapper function
        // This covers the first StringBuilder flatFieldName code block

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("disable_objects", true);  // Root level has disable_objects=true
            b.field("dynamic", "true");
        }));

        // When root has disable_objects=true, any dotted field name should be treated as a flat field
        // The getMapper function should flatten the entire path into a single field name
        ParsedDocument doc1 = mapper.parse(source(b -> { b.field("root.level.dotted.field", "root_flattened"); }));

        // This should create a single flat field
        assertNotNull("Field 'root.level.dotted.field' should exist", doc1.rootDoc().getField("root.level.dotted.field"));
        assertEquals("root_flattened", doc1.rootDoc().getField("root.level.dotted.field").stringValue());

        // Test with very long dotted path
        ParsedDocument doc2 = mapper.parse(source(b -> { b.field("a.b.c.d.e.f.g.h.i.j", "long_path"); }));

        assertNotNull("Field 'a.b.c.d.e.f.g.h.i.j' should exist", doc2.rootDoc().getField("a.b.c.d.e.f.g.h.i.j"));
        assertEquals("long_path", doc2.rootDoc().getField("a.b.c.d.e.f.g.h.i.j").stringValue());

        // Test single level field (should work normally)
        ParsedDocument doc3 = mapper.parse(source(b -> { b.field("simple", "simple_value"); }));

        assertNotNull("Field 'simple' should exist", doc3.rootDoc().getField("simple"));
        assertEquals("simple_value", doc3.rootDoc().getField("simple").stringValue());
    }

    public void testProcessFlattenedTokenNullValueForDisableObjects() throws Exception {
        // Test the specific code path: processFlattenedTokenForDisableObjects -> processNullValueForDisableObjects
        // This happens when we have null values within flattened objects/arrays in disable_objects mode

        // Test 1: Only null field in flattened object with strict dynamic mapping should throw exception
        DocumentMapper strictMapper = createDocumentMapper(topMapping(b -> {
            b.field("disable_objects", true);
            b.field("dynamic", "strict");
        }));

        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class, () -> {
            strictMapper.parse(source(b -> {
                b.startObject("user");
                {
                    b.nullField("age"); // Only null field - should trigger the exception via processFlattenedTokenForDisableObjects
                }
                b.endObject();
            }));
        });
        assertThat(exception.getMessage(), containsString("strict"));
        assertThat(exception.getMessage(), containsString("user.age"));

        // Test 2: Only null field in flattened array with strict dynamic mapping should throw exception
        StrictDynamicMappingException exception2 = expectThrows(StrictDynamicMappingException.class, () -> {
            strictMapper.parse(source(b -> {
                b.startArray("items");
                {
                    b.startObject();
                    {
                        b.nullField("category"); // Only null field - should trigger the exception via
                                                 // processFlattenedTokenForDisableObjects
                    }
                    b.endObject();
                }
                b.endArray();
            }));
        });
        assertThat(exception2.getMessage(), containsString("strict"));
        assertThat(exception2.getMessage(), containsString("items.category"));

        // Test 3: Null value in flattened object with default dynamic mapping should be handled gracefully
        DocumentMapper defaultMapper = createDocumentMapper(topMapping(b -> {
            b.field("disable_objects", true);
            b.field("dynamic", "true");
        }));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("product");
            {
                b.field("name", "laptop");
                b.nullField("description"); // This null field should not create a field via processFlattenedTokenForDisableObjects
                b.field("price", 999);
            }
            b.endObject();
        }));

        // Regular fields should be flattened and exist
        assertNotNull("Field 'product.name' should exist", doc.rootDoc().getField("product.name"));
        assertEquals("laptop", doc.rootDoc().getField("product.name").stringValue());
        assertNotNull("Field 'product.price' should exist", doc.rootDoc().getField("product.price"));

        // Null field should not be created
        assertNull("Null field 'product.description' should not be created", doc.rootDoc().getField("product.description"));

        // Test 4: Test with pre-existing field mapping to ensure we only test dynamic null handling
        DocumentMapper mapperWithExistingField = createDocumentMapper(topMapping(b -> {
            b.field("disable_objects", true);
            b.field("dynamic", "strict");
            b.startObject("properties");
            {
                b.startObject("user.name");
                {
                    b.field("type", "text");
                }
                b.endObject();
            }
            b.endObject();
        }));

        // This should work because user.name has existing mapping, but user.age should still throw exception
        StrictDynamicMappingException exception3 = expectThrows(StrictDynamicMappingException.class, () -> {
            mapperWithExistingField.parse(source(b -> {
                b.startObject("user");
                {
                    b.field("name", "john"); // This has existing mapping, should work
                    b.nullField("age"); // This should trigger the exception for null value
                }
                b.endObject();
            }));
        });
        assertThat(exception3.getMessage(), containsString("strict"));
        assertThat(exception3.getMessage(), containsString("user.age"));
    }

    public void testParseNonDynamicArrayWithDisableObjects() throws Exception {
        // Test the specific array flattening logic in parseNonDynamicArray when disable_objects=true
        // This tests the code path: parseNonDynamicArray -> processFlattenedTokenForDisableObjects

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("disable_objects", true);
            b.field("dynamic", "true");
            b.startObject("properties");
            {
                b.startObject("tags");
                {
                    b.field("type", "text");
                }
                b.endObject();
                b.startObject("numbers");
                {
                    b.field("type", "integer");
                }
                b.endObject();
            }
            b.endObject();
        }));

        // Test 1: Simple array with existing field mapping
        ParsedDocument doc1 = mapper.parse(source(b -> { b.startArray("tags").value("tag1").value("tag2").value("tag3").endArray(); }));

        assertNotNull(doc1);
        // In disable_objects mode, array elements should be flattened using the same field name
        assertEquals(3, doc1.rootDoc().getFields("tags").length);

        // Verify the values are correctly stored
        IndexableField[] tagFields = doc1.rootDoc().getFields("tags");
        assertEquals("tag1", tagFields[0].stringValue());
        assertEquals("tag2", tagFields[1].stringValue());
        assertEquals("tag3", tagFields[2].stringValue());

        // Test 2: Array with numeric values
        ParsedDocument doc2 = mapper.parse(source(b -> { b.startArray("numbers").value(10).value(20).value(30).endArray(); }));

        assertNotNull(doc2);
        assertEquals(6, doc2.rootDoc().getFields("numbers").length); // 3 values * 2 fields each (numeric + stored)

        // Test 3: Array with mixed content (objects within arrays)
        ParsedDocument doc3 = mapper.parse(source(b -> {
            b.startArray("mixed_array");
            {
                b.startObject();
                {
                    b.field("name", "item1");
                    b.field("value", 100);
                }
                b.endObject();
                b.startObject();
                {
                    b.field("name", "item2");
                    b.field("value", 200);
                }
                b.endObject();
            }
            b.endArray();
        }));

        assertNotNull(doc3);
        // Objects within arrays should be flattened to dotted field names
        assertNotNull("Field 'mixed_array.name' should exist", doc3.rootDoc().getField("mixed_array.name"));
        assertNotNull("Field 'mixed_array.value' should exist", doc3.rootDoc().getField("mixed_array.value"));

        // Should have 2 instances of each field (one for each object in the array)
        assertEquals(2, doc3.rootDoc().getFields("mixed_array.name").length);
        assertEquals(4, doc3.rootDoc().getFields("mixed_array.value").length); // 2 values * 2 fields each
    }

    public void testParseNonDynamicArrayNestedArraysWithDisableObjects() throws Exception {
        // Test nested arrays within disable_objects mode
        // This tests the recursive array handling in processFlattenedTokenForDisableObjects

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("disable_objects", true);
            b.field("dynamic", "true");
        }));

        // Test nested arrays
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("nested_arrays");
            {
                b.startArray().value("a1").value("a2").endArray();
                b.startArray().value("b1").value("b2").endArray();
                b.startArray().value("c1").value("c2").endArray();
            }
            b.endArray();
        }));

        assertNotNull(doc);
        // All array elements should be flattened to the same field name
        assertEquals(6, doc.rootDoc().getFields("nested_arrays").length);

        // Verify all values are present
        IndexableField[] fields = doc.rootDoc().getFields("nested_arrays");
        String[] expectedValues = { "a1", "a2", "b1", "b2", "c1", "c2" };
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], fields[i].stringValue());
        }
    }

    public void testParseNonDynamicArrayWithComplexObjectsDisableObjects() throws Exception {
        // Test complex nested objects within arrays in disable_objects mode

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("disable_objects", true);
            b.field("dynamic", "true");
        }));

        // Test array containing complex nested objects
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("products");
            {
                b.startObject();
                {
                    b.field("id", "prod1");
                    b.startObject("details");
                    {
                        b.field("name", "Product 1");
                        b.field("price", 99.99);
                        b.startObject("category");
                        {
                            b.field("main", "electronics");
                            b.field("sub", "phones");
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
                b.startObject();
                {
                    b.field("id", "prod2");
                    b.startObject("details");
                    {
                        b.field("name", "Product 2");
                        b.field("price", 149.99);
                        b.startObject("category");
                        {
                            b.field("main", "electronics");
                            b.field("sub", "tablets");
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        assertNotNull(doc);

        // Verify all nested fields are flattened correctly
        assertNotNull("Field 'products.id' should exist", doc.rootDoc().getField("products.id"));
        assertNotNull("Field 'products.details.name' should exist", doc.rootDoc().getField("products.details.name"));
        assertNotNull("Field 'products.details.price' should exist", doc.rootDoc().getField("products.details.price"));
        assertNotNull("Field 'products.details.category.main' should exist", doc.rootDoc().getField("products.details.category.main"));
        assertNotNull("Field 'products.details.category.sub' should exist", doc.rootDoc().getField("products.details.category.sub"));

        // Verify we have the correct number of instances (2 products)
        assertEquals(2, doc.rootDoc().getFields("products.id").length);
        assertEquals(2, doc.rootDoc().getFields("products.details.name").length);
        assertEquals(2, doc.rootDoc().getFields("products.details.category.main").length);
        assertEquals(2, doc.rootDoc().getFields("products.details.category.sub").length);

        // Verify the actual values
        IndexableField[] idFields = doc.rootDoc().getFields("products.id");
        assertEquals("prod1", idFields[0].stringValue());
        assertEquals("prod2", idFields[1].stringValue());

        IndexableField[] nameFields = doc.rootDoc().getFields("products.details.name");
        assertEquals("Product 1", nameFields[0].stringValue());
        assertEquals("Product 2", nameFields[1].stringValue());
    }

    public void testParseNonDynamicArrayWithNullValuesDisableObjects() throws Exception {
        // Test array containing null values in disable_objects mode

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("disable_objects", true);
            b.field("dynamic", "true");
        }));

        // Test array with null values mixed with regular values
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("mixed_nulls");
            {
                b.value("value1");
                b.nullValue();
                b.value("value2");
                b.startObject();
                {
                    b.field("nested", "value3");
                    b.nullField("null_field");
                }
                b.endObject();
            }
            b.endArray();
        }));

        assertNotNull(doc);

        // Non-null values should be stored
        assertNotNull("Field 'mixed_nulls' should exist", doc.rootDoc().getField("mixed_nulls"));
        assertNotNull("Field 'mixed_nulls.nested' should exist", doc.rootDoc().getField("mixed_nulls.nested"));

        // Verify the non-null values
        IndexableField[] mixedFields = doc.rootDoc().getFields("mixed_nulls");
        assertEquals("value1", mixedFields[0].stringValue());
        assertEquals("value2", mixedFields[1].stringValue());

        assertEquals("value3", doc.rootDoc().getField("mixed_nulls.nested").stringValue());

        // Null fields should not be created
        assertNull("Null field should not be created", doc.rootDoc().getField("mixed_nulls.null_field"));
    }
}
