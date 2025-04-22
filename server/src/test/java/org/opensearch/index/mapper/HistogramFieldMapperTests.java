/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.geometry.utils.Geohash.stringEncode;

/**
 * Basic unit test for {@link HistogramFieldMapper}.
 */
public class HistogramFieldMapperTests extends MapperTestCase {

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "histogram");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.startObject()
            .array("values", 0.1, 0.2, 0.3)
            .array("counts", 3L, 7L, 23L)
            .endObject();
    }

    public void testParserPresent() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        Mapper.TypeParser parser = mapperService.mapperRegistry.getMapperParsers().get("histogram");
        assertNotNull("Histogram type parser should be registered", parser);
        assertTrue("Type parser should be an instance of HistogramFieldMapper.TypeParser",
            parser instanceof HistogramFieldMapper.TypeParser);
    }

    public void testHistogramValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        XContentBuilder source = JsonXContent.contentBuilder()
            .startObject()
            .startObject("field")
            .array("values", 0.1, 0.2, 0.3)
            .array("counts", 3L, 7L, 23L)
            .endObject()
            .endObject();

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(source), XContentType.JSON));

        IndexableField field = doc.rootDoc().getField("field");
        assertThat("Histogram field should not be null", field, notNullValue());
        assertTrue("Field should be a BinaryDocValuesField", field instanceof BinaryDocValuesField);

        BytesRef bytesRef = field.binaryValue();
        ByteBuffer buffer = ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length);

        int size = buffer.getInt();
        assertEquals("Wrong number of values", 3, size);
        assertEquals("First value wrong", 0.1, buffer.getDouble(), 0.001);
        assertEquals("First count wrong", 3L, buffer.getLong());
        assertEquals("Second value wrong", 0.2, buffer.getDouble(), 0.001);
        assertEquals("Second count wrong", 7L, buffer.getLong());
        assertEquals("Third value wrong", 0.3, buffer.getDouble(), 0.001);
        assertEquals("Third count wrong", 23L, buffer.getLong());
    }

    public void testBothArraysEmpty() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.startObject("field")
                .startArray("values").endArray()
                .startArray("counts").endArray()
                .endObject()));
        });

        assertThat(e.getMessage(),
            containsString("failed to parse field [field] of type [histogram]"));

        Throwable cause = e.getCause();
        assertNotNull("Cause should not be null", cause);
        assertThat(cause.getMessage(), containsString("values array cannot be empty"));
    }

    public void testEmptyValuesArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.startObject("field")
                .startArray("values").endArray()
                .array("counts", 1L, 2L, 3L)
                .endObject()));
        });

        assertThat(e.getMessage(),
            containsString("failed to parse field [field] of type [histogram]"));

        Throwable cause = e.getCause();
        assertNotNull("Cause should not be null", cause);
        assertThat(cause.getMessage(), containsString("values array cannot be empty"));
    }

    public void testEmptyCountsArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.startObject("field")
                .array("values", 0.1, 0.2, 0.3)
                .startArray("counts").endArray()
                .endObject()));
        });

        assertThat(e.getMessage(),
            containsString("failed to parse field [field] of type [histogram]"));

        Throwable cause = e.getCause();
        assertNotNull("Cause should not be null", cause);
        assertThat(cause.getMessage(), containsString("counts array cannot be empty"));
    }

    public void testMissingArrays() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.startObject("field")
                .endObject()));
        });

        assertThat(e.getMessage(),
            containsString("failed to parse field [field] of type [histogram]"));

        Throwable cause = e.getCause();
        assertNotNull("Cause should not be null", cause);
        assertThat(cause.getMessage(), containsString("values array cannot be empty"));
    }

    public void testMultipleHistogramFields() throws IOException {
        // Test document with multiple histogram fields
        XContentBuilder mapping = XContentBuilder.builder(JsonXContent.jsonXContent)
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("histogram1")
            .field("type", "histogram")
            .endObject()
            .startObject("histogram2")
            .field("type", "histogram")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        DocumentMapper mapper = createDocumentMapper(mapping);

        ParsedDocument doc = mapper.parse(source(b -> b
            .startObject("histogram1")
            .array("values", 0.1, 0.2, 0.3)
            .array("counts", 3, 7, 5)
            .endObject()
            .startObject("histogram2")
            .array("values", 1.0, 2.0, 3.0)
            .array("counts", 10, 20, 30)
            .endObject()));

        assertNotNull(doc.rootDoc().getField("histogram1"));
        assertNotNull(doc.rootDoc().getField("histogram2"));
    }

    public void testHistogramWithLargeValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(source(b -> b.startObject("field")
            .array("values", 1000.0, 2000.0, 3000.0)
            .array("counts", 1000000L, 2000000L, 3000000L)
            .endObject()));

        IndexableField field = doc.rootDoc().getField("field");
        assertNotNull(field);

        BytesRef bytesRef = field.binaryValue();
        ByteBuffer buffer = ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length);

        assertEquals(3, buffer.getInt());
        assertEquals(1000.0, buffer.getDouble(), 0.001);
        assertEquals(1000000L, buffer.getLong());
        assertEquals(2000.0, buffer.getDouble(), 0.001);
        assertEquals(2000000L, buffer.getLong());
        assertEquals(3000.0, buffer.getDouble(), 0.001);
        assertEquals(3000000L, buffer.getLong());
    }

    public void testHistogramWithSingleValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(source(b -> b.startObject("field")
            .array("values", 1.0)
            .array("counts", 100L)
            .endObject()));

        IndexableField field = doc.rootDoc().getField("field");
        assertNotNull(field);

        BytesRef bytesRef = field.binaryValue();
        ByteBuffer buffer = ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length);

        assertEquals(1, buffer.getInt());
        assertEquals(1.0, buffer.getDouble(), 0.001);
        assertEquals(100L, buffer.getLong());
    }

    public void testHistogramUpdate() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        // First document
        ParsedDocument doc1 = mapper.parse(source(b -> b.startObject("field")
            .array("values", 0.1, 0.2, 0.3)
            .array("counts", 3, 7, 5)
            .endObject()));

        // Updated document
        ParsedDocument doc2 = mapper.parse(source(b -> b.startObject("field")
            .array("values", 0.1, 0.2, 0.3)
            .array("counts", 4, 8, 6)
            .endObject()));

        assertNotNull(doc1.rootDoc().getField("field"));
        assertNotNull(doc2.rootDoc().getField("field"));
        assertNotEquals(
            doc1.rootDoc().getField("field").binaryValue(),
            doc2.rootDoc().getField("field").binaryValue()
        );
    }

    public void testHistogramWithVariousInvalidCounts() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        // Test decimal numbers
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.startObject("field")
                .array("values", 0.1, 0.2, 0.3)
                .startArray("counts")
                .value(3.5d)
                .endArray()
                .endObject()));
        });
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [histogram]"));
        assertThat(e.getCause().getMessage(), containsString("counts must be integers"));

        // Test non-numbers
        e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.startObject("field")
                .array("values", 0.1, 0.2, 0.3)
                .startArray("counts")
                .value("not a number")
                .endArray()
                .endObject()));
        });
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [histogram]"));
        assertThat(e.getCause().getMessage(), containsString("counts must be numbers"));

        // Test negative numbers
        e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.startObject("field")
                .array("values", 0.1, 0.2, 0.3)
                .startArray("counts")
                .value(-1)
                .value(-2)
                .value(-3)
                .endArray()
                .endObject()));
        });
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [histogram]"));
        assertThat(e.getCause().getMessage(), containsString("counts must be non-negative"));
    }

    public void testHistogramWithVariousInvalidValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        // Test non-numeric values
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.startObject("field")
                .startArray("values")
                .value("not a number")
                .endArray()
                .array("counts", 1)
                .endObject()));
        });
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [histogram]"));
        assertThat(e.getCause().getMessage(), containsString("values must be numbers"));

        // Test infinite values
        e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.startObject("field")
                .startArray("values")
                .value(Double.POSITIVE_INFINITY)
                .value(0.2)
                .value(0.3)
                .endArray()
                .array("counts", 1, 2, 3)
                .endObject()));
        });
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [histogram]"));
        assertThat(e.getCause().getMessage(), containsString("values must be numbers"));

        // Test NaN values
        e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.startObject("field")
                .startArray("values")
                .value(Double.NaN)
                .value(0.2)
                .value(0.3)
                .endArray()
                .array("counts", 1, 2, 3)
                .endObject()));
        });
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [histogram]"));
        assertThat(e.getCause().getMessage(), containsString("values must be numbers"));

        // Test non-ascending values
        e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.startObject("field")
                .array("values", 0.3, 0.2, 0.1)  // Descending order
                .array("counts", 1, 2, 3)
                .endObject()));
        });
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [histogram]"));
        assertThat(e.getCause().getMessage(), containsString("values must be in strictly ascending order"));
    }

    public void testHistogramArraysNotSameLength() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        // Test when counts array is shorter than values
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.startObject("field")
                .array("values", 0.1, 0.2, 0.3)
                .array("counts", 1, 2)  // One count missing
                .endObject()));
        });
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [histogram]"));
        assertThat(e.getCause().getMessage(), containsString("values and counts arrays must have the same length"));

        // Test when counts array is longer than values
        e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.startObject("field")
                .array("values", 0.1, 0.2)
                .array("counts", 1, 2, 3)  // Extra count
                .endObject()));
        });
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [histogram]"));
        assertThat(e.getCause().getMessage(), containsString("values and counts arrays must have the same length"));

        // Test valid case with same length
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("field")
            .array("values", 0.1, 0.2, 0.3)
            .array("counts", 1, 2, 3)
            .endObject()));
        assertNotNull(doc.rootDoc().getField("field"));
    }

    public void testHistogramFieldSerialization() throws IOException {
        XContentBuilder mapping = XContentBuilder.builder(JsonXContent.jsonXContent)
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("histogram_field")
            .field("type", "histogram")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        DocumentMapper mapper = createDocumentMapper(mapping);
        assertNotNull(mapper);

        Mapper fieldMapper = mapper.mappers().getMapper("histogram_field");
        assertNotNull("Histogram field mapper should not be null", fieldMapper);

        XContentBuilder builder = jsonBuilder().startObject();
        fieldMapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        assertEquals("{\"histogram_field\":{\"type\":\"histogram\"}}", builder.toString());
    }
}
