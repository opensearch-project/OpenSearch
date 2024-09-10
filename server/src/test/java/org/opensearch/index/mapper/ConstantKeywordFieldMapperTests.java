/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class ConstantKeywordFieldMapperTests extends MapperTestCase {

    public void testDefaultDisabledIndexMapper() throws Exception {

        // test value
        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> b.field("type", "constant_keyword").field("index", false).field("value", "default_value"))
            );
            {
                MapperParsingException e = expectThrows(
                    MapperParsingException.class,
                    () -> mapper.parse(source(b -> { b.field("field", "sdf"); }))
                );
                assertThat(
                    e.getMessage(),
                    containsString(
                        "failed to parse field [field] of type [constant_keyword] in document with id '1'. Preview of field's value: 'sdf'"
                    )
                );
            }

            // constantKeywordField should not be stored
            {
                final ParsedDocument doc = mapper.parse(source(b -> { b.field("field", "default_value"); }));
                final IndexableField field = doc.rootDoc().getField("field");
                assertNull(field);
            }
        }

        // test values
        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> b.field("type", "constant_keyword").field("index", true).array("values", "value1", "value2"))
            );
            {
                MapperParsingException e = expectThrows(
                    MapperParsingException.class,
                    () -> mapper.parse(source(b -> { b.field("field", "sdf"); }))
                );
                assertThat(
                    e.getMessage(),
                    containsString(
                        "failed to parse field [field] of type [constant_keyword] in document with id '1'. Preview of field's value: 'sdf'"
                    )
                );
            }

            {
                final ParsedDocument doc = mapper.parse(source(b -> { b.field("field", "value1"); }));
                final IndexableField[] fields = doc.rootDoc().getFields("field");
                assertEquals(2, fields.length);
                assertEquals(new BytesRef(new byte[] { Integer.valueOf(0).byteValue() }), fields[0].binaryValue());
                assertEquals(KeywordFieldMapper.KeywordField.class, fields[0].getClass());
                assertEquals(SortedSetDocValuesField.class, fields[1].getClass());
            }
        }
    }

    public void testMissingDefaultIndexMapper() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "constant_keyword").field("index", "false")))
        );
        assertThat(e.getMessage(), containsString("Field [field] is missing required parameter [value or values]"));
    }

    public void testBuilderToXContent() throws IOException {
        ConstantKeywordFieldMapper.Builder builder = new ConstantKeywordFieldMapper.Builder(null);
        builder.setValue("value1");
        builder.setIndexed(false);
        builder.setHasDocValues(false);
        XContentBuilder xContentBuilder = JsonXContent.contentBuilder().startObject();
        builder.toXContent(xContentBuilder, false);
        xContentBuilder.endObject();
        assertEquals("{\"value\":\"value1\",\"index\":false,\"doc_values\":false}", xContentBuilder.toString());

        builder = new ConstantKeywordFieldMapper.Builder(null);
        builder.setValues(List.of("1", "2"));
        builder.setIndexed(false);
        builder.setHasDocValues(true);
        xContentBuilder = JsonXContent.contentBuilder().startObject();
        builder.toXContent(xContentBuilder, false);
        xContentBuilder.endObject();
        assertEquals("{\"values\":[\"1\",\"2\"],\"index\":false}", xContentBuilder.toString());
    }

    public void testIndex() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "constant_keyword").array("values", "a", "b").field("index", "false"))
        );
        String json = XContentFactory.jsonBuilder().startObject().field("field", "a").endObject().toString();
        ParsedDocument doc = mapper.parse(source(json));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals(SortedSetDocValuesField.class, fields[0].getClass());
        assertEquals(new BytesRef(new byte[] { Integer.valueOf(0).byteValue() }), fields[0].binaryValue());

        mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "constant_keyword").array("values", "a", "b").field("index", "true"))
        );
        json = XContentFactory.jsonBuilder().startObject().field("field", "a").endObject().toString();
        doc = mapper.parse(source(json));
        fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(KeywordFieldMapper.KeywordField.class, fields[0].getClass());
        assertEquals(SortedSetDocValuesField.class, fields[1].getClass());
        assertEquals(new BytesRef(new byte[] { Integer.valueOf(0).byteValue() }), fields[0].binaryValue());
    }

    public void testDocValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "constant_keyword").array("values", "a", "b").field("doc_values", "false"))
        );
        String json = XContentFactory.jsonBuilder().startObject().field("field", "a").endObject().toString();
        ParsedDocument doc = mapper.parse(source(json));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals(KeywordFieldMapper.KeywordField.class, fields[0].getClass());
        assertEquals(new BytesRef(new byte[] { Integer.valueOf(0).byteValue() }), fields[0].binaryValue());
    }

    public void testValuesNotBiggerThen127() {
        String[] values = new String[130];
        for (int i = 0; i < values.length; i++) {
            values[i] = String.valueOf(i);
        }
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "constant_keyword").array("values", values)))
        );
        assertThat(
            e.getMessage(),
            containsString("Failed to parse mapping [_doc]: the values of constant_keyword field [field] must be small than 127 values")
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "constant_keyword");
        b.array("values", "a", "b");
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value("a");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
    }

    @Override
    protected boolean supportsMeta() {
        return false;
    }

    @Override
    protected boolean supportsOrIgnoresBoost() {
        return false;
    }
}
