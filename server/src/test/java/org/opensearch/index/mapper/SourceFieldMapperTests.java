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
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collection;
import java.util.Map;

import static org.opensearch.index.MapperTestUtils.assertConflicts;
import static org.hamcrest.Matchers.equalTo;

public class SourceFieldMapperTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testNoFormat() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .endObject()
            .endObject()
            .endObject()
            .toString();

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper documentMapper = parser.parse("type", new CompressedXContent(mapping));
        ParsedDocument doc = documentMapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject()),
                MediaTypeRegistry.JSON
            )
        );

        assertThat(MediaTypeRegistry.xContent(doc.source().toBytesRef().bytes), equalTo(MediaTypeRegistry.JSON));

        documentMapper = parser.parse("type", new CompressedXContent(mapping));
        doc = documentMapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(XContentFactory.smileBuilder().startObject().field("field", "value").endObject()),
                XContentType.SMILE
            )
        );
        final IndexableField recoverySourceIndexableField = doc.rootDoc().getField("_recovery_source");
        assertNull(recoverySourceIndexableField);
        assertThat(MediaTypeRegistry.xContentType(doc.source()), equalTo(XContentType.SMILE));
    }

    public void testIncludes() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .array("includes", new String[] { "path1*" })
            .endObject()
            .endObject()
            .endObject()
            .toString();

        DocumentMapper documentMapper = createIndex("test").mapperService()
            .documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = documentMapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("path1")
                        .field("field1", "value1")
                        .endObject()
                        .startObject("path2")
                        .field("field2", "value2")
                        .endObject()
                        .endObject()
                ),
                MediaTypeRegistry.JSON
            )
        );

        IndexableField sourceField = doc.rootDoc().getField("_source");
        Map<String, Object> sourceAsMap;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(sourceField.binaryValue()))) {
            sourceAsMap = parser.map();
        }
        final IndexableField recoverySourceIndexableField = doc.rootDoc().getField("_recovery_source");
        assertNotNull(recoverySourceIndexableField);
        assertThat(sourceAsMap.containsKey("path1"), equalTo(true));
        assertThat(sourceAsMap.containsKey("path2"), equalTo(false));
    }

    public void testIncludesForRecoverySource() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .array("includes", new String[] { "path1*" })
            .array("recovery_source_includes", new String[] { "path2*" })
            .endObject()
            .endObject()
            .endObject()
            .toString();

        DocumentMapper documentMapper = createIndex("test").mapperService()
            .documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = documentMapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("path1")
                        .field("field1", "value1")
                        .endObject()
                        .startObject("path2")
                        .field("field2", "value2")
                        .endObject()
                        .endObject()
                ),
                MediaTypeRegistry.JSON
            )
        );

        IndexableField sourceField = doc.rootDoc().getField("_source");
        Map<String, Object> sourceAsMap;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(sourceField.binaryValue()))) {
            sourceAsMap = parser.map();
        }
        assertThat(sourceAsMap.containsKey("path1"), equalTo(true));
        assertThat(sourceAsMap.containsKey("path2"), equalTo(false));

        final IndexableField recoverySourceIndexableField = doc.rootDoc().getField("_recovery_source");
        assertNotNull(recoverySourceIndexableField);
        Map<String, Object> recoverySourceAsMap;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(recoverySourceIndexableField.binaryValue()))) {
            recoverySourceAsMap = parser.map();
        }

        assertThat(recoverySourceAsMap.containsKey("path1"), equalTo(false));
        assertThat(recoverySourceAsMap.containsKey("path2"), equalTo(true));
    }

    public void testNoRecoverySourceAndNoSource_whenBothAreDisabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .field("enabled", "false")
            .field("recovery_source_enabled", "false")
            .endObject()
            .endObject()
            .endObject()
            .toString();

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper documentMapper = parser.parse("type", new CompressedXContent(mapping));
        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject());
        ParsedDocument doc = documentMapper.parse(new SourceToParse("test", "1", source, MediaTypeRegistry.JSON));

        final IndexableField sourceIndexableField = doc.rootDoc().getField("_source");
        final IndexableField recoverySourceIndexableField = doc.rootDoc().getField("_recovery_source");
        assertNull(recoverySourceIndexableField);
        assertNull(sourceIndexableField);
    }

    public void testExcludes() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .array("excludes", "path1*")
            .endObject()
            .endObject()
            .endObject()
            .toString();

        DocumentMapper documentMapper = createIndex("test").mapperService()
            .documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = documentMapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("path1")
                        .field("field1", "value1")
                        .endObject()
                        .startObject("path2")
                        .field("field2", "value2")
                        .endObject()
                        .endObject()
                ),
                MediaTypeRegistry.JSON
            )
        );

        IndexableField sourceField = doc.rootDoc().getField("_source");
        Map<String, Object> sourceAsMap;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(sourceField.binaryValue()))) {
            sourceAsMap = parser.map();
        }
        final IndexableField recoverySourceIndexableField = doc.rootDoc().getField("_recovery_source");
        assertNotNull(recoverySourceIndexableField);
        assertThat(sourceAsMap.containsKey("path1"), equalTo(false));
        assertThat(sourceAsMap.containsKey("path2"), equalTo(true));
    }

    public void testExcludesForRecoverySource() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .array("excludes", "path1*")
            .array("recovery_source_excludes", "path2*")
            .endObject()
            .endObject()
            .endObject()
            .toString();

        DocumentMapper documentMapper = createIndex("test").mapperService()
            .documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = documentMapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("path1")
                        .field("field1", "value1")
                        .endObject()
                        .startObject("path2")
                        .field("field2", "value2")
                        .endObject()
                        .endObject()
                ),
                MediaTypeRegistry.JSON
            )
        );

        IndexableField sourceField = doc.rootDoc().getField("_source");
        Map<String, Object> sourceAsMap;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(sourceField.binaryValue()))) {
            sourceAsMap = parser.map();
        }
        assertThat(sourceAsMap.containsKey("path1"), equalTo(false));
        assertThat(sourceAsMap.containsKey("path2"), equalTo(true));

        final IndexableField recoverySourceIndexableField = doc.rootDoc().getField("_recovery_source");
        assertNotNull(recoverySourceIndexableField);
        Map<String, Object> recoverySourceAsMap;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(recoverySourceIndexableField.binaryValue()))) {
            recoverySourceAsMap = parser.map();
        }
        assertThat(recoverySourceAsMap.containsKey("path1"), equalTo(true));
        assertThat(recoverySourceAsMap.containsKey("path2"), equalTo(false));
    }

    public void testEnabledNotUpdateable() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        // using default of true
        String mapping1 = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();
        String mapping2 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .field("enabled", false)
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertConflicts(mapping1, mapping2, parser, "Cannot update parameter [enabled] from [true] to [false]");

        // not changing is ok
        String mapping3 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .field("enabled", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertConflicts(mapping1, mapping3, parser);
    }

    public void testIncludesNotUpdateable() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();
        String mapping1 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .array("includes", "foo.*")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertConflicts(defaultMapping, mapping1, parser, "Cannot update parameter [includes] from [[]] to [[foo.*]]");
        assertConflicts(mapping1, defaultMapping, parser, "Cannot update parameter [includes] from [[foo.*]] to [[]]");

        String mapping2 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .array("includes", "foo.*", "bar.*")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertConflicts(mapping1, mapping2, parser, "Cannot update parameter [includes] from [[foo.*]] to [[foo.*, bar.*]]");

        // not changing is ok
        assertConflicts(mapping1, mapping1, parser);
    }

    public void testExcludesNotUpdateable() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();
        String mapping1 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .array("excludes", "foo.*")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertConflicts(defaultMapping, mapping1, parser, "Cannot update parameter [excludes] from [[]] to [[foo.*]]");
        assertConflicts(mapping1, defaultMapping, parser, "Cannot update parameter [excludes] from [[foo.*]] to [[]]");

        String mapping2 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .array("excludes", "foo.*", "bar.*")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertConflicts(mapping1, mapping2, parser, "Cannot update parameter [excludes]");

        // not changing is ok
        assertConflicts(mapping1, mapping1, parser);
    }

    public void testComplete() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();
        assertTrue(parser.parse("type", new CompressedXContent(mapping)).sourceMapper().isComplete());

        mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .field("enabled", false)
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertFalse(parser.parse("type", new CompressedXContent(mapping)).sourceMapper().isComplete());

        mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .array("includes", "foo.*")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertFalse(parser.parse("type", new CompressedXContent(mapping)).sourceMapper().isComplete());

        mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_source")
            .array("excludes", "foo.*")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertFalse(parser.parse("type", new CompressedXContent(mapping)).sourceMapper().isComplete());
    }

    public void testSourceObjectContainsExtraTokens() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();
        DocumentMapper documentMapper = createIndex("test").mapperService()
            .documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        try {
            documentMapper.parse(new SourceToParse("test", "1", new BytesArray("{}}"), MediaTypeRegistry.JSON)); // extra end object
            // (invalid JSON)
            fail("Expected parse exception");
        } catch (MapperParsingException e) {
            assertNotNull(e.getRootCause());
            String message = e.getRootCause().getMessage();
            assertTrue(message, message.contains("Unexpected close marker '}'"));
        }
    }
}
