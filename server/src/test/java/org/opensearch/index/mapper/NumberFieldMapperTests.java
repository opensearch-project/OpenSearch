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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.NumberFieldMapper.NumberType;
import org.opensearch.index.mapper.NumberFieldTypeTests.OutOfRangeSpec;
import org.opensearch.index.termvectors.TermVectorsService;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;

public class NumberFieldMapperTests extends AbstractNumericFieldMapperTestCase {

    private static final String FIELD_NAME = "field";

    @Override
    protected Set<String> types() {
        return Set.of("byte", "short", "integer", "long", "float", "double", "half_float", "unsigned_long");
    }

    @Override
    protected Set<String> wholeTypes() {
        return Set.of("byte", "short", "integer", "long", "unsigned_long");
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "long");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", 1));
        checker.registerUpdateCheck(b -> b.field("coerce", false), m -> assertFalse(((NumberFieldMapper) m).coerce()));
        checker.registerUpdateCheck(
            b -> b.field("ignore_malformed", true),
            m -> assertTrue(((NumberFieldMapper) m).ignoreMalformed().value())
        );
    }

    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value(123);
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    @Override
    public void doTestDefaults(String type) throws Exception {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", type));
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(mapping.toString(), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(DocValuesSkipIndexType.NONE, dvField.fieldType().docValuesSkipIndexType());
        assertFalse(dvField.fieldType().stored());

    }

    @Override
    public void doTestNotIndexed(String type) throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type).field("index", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(DocValuesSkipIndexType.NONE, dvField.fieldType().docValuesSkipIndexType());
    }

    @Override
    public void doTestNoDocValues(String type) throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type).field("doc_values", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
    }

    @Override
    public void doTestStore(String type) throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type).field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(DocValuesSkipIndexType.NONE, dvField.fieldType().docValuesSkipIndexType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        // The 'unsigned_long' is stored as a string
        if (type.equalsIgnoreCase("unsigned_long")) {
            assertEquals(123, new BigInteger(storedField.stringValue()).longValue());
        } else {
            assertEquals(123, storedField.numericValue().doubleValue(), 0d);
        }
    }

    @Override
    public void doTestCoerce(String type) throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "123")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());

        DocumentMapper mapper2 = createDocumentMapper(fieldMapping(b -> b.field("type", type).field("coerce", false)));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper2.parse(source(b -> b.field("field", "123"))));
        assertThat(e.getCause().getMessage(), containsString("passed as String"));
    }

    @Override
    protected void doTestDecimalCoerce(String type) throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "7.89")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        IndexableField pointField = fields[0];
        assertEquals(7, pointField.numericValue().doubleValue(), 0d);
    }

    public void testIgnoreMalformed() throws Exception {
        for (String type : types()) {
            DocumentMapper notIgnoring = createDocumentMapper(fieldMapping(b -> b.field("type", type)));
            DocumentMapper ignoring = createDocumentMapper(fieldMapping(b -> b.field("type", type).field("ignore_malformed", true)));
            for (Object malformedValue : new Object[] { "a", Boolean.FALSE }) {
                SourceToParse source = source(b -> b.field("field", malformedValue));
                MapperParsingException e = expectThrows(MapperParsingException.class, () -> notIgnoring.parse(source));
                if (malformedValue instanceof String) {
                    assertThat(e.getCause().getMessage(), containsString("For input string: \"a\""));
                } else {
                    assertThat(e.getCause().getMessage(), containsString("Current token"));
                    assertThat(e.getCause().getMessage(), containsString("not numeric, can not use numeric value accessors"));
                }

                ParsedDocument doc = ignoring.parse(source);
                IndexableField[] fields = doc.rootDoc().getFields("field");
                assertEquals(0, fields.length);
                assertArrayEquals(new String[] { "field" }, TermVectorsService.getValues(doc.rootDoc().getFields("_ignored")));
            }
        }
    }

    @Override
    protected void doTestNullValue(String type) throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type)));
        SourceToParse source = source(b -> b.nullField("field"));
        ParsedDocument doc = mapper.parse(source);
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        Object missing = Arrays.asList("float", "double", "half_float").contains(type) ? 123d : 123L;
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type).field("null_value", missing)));
        doc = mapper.parse(source);
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
    }

    public void testOutOfRangeValues() throws IOException {
        final List<OutOfRangeSpec> inputs = Arrays.asList(
            OutOfRangeSpec.of(NumberType.BYTE, "128", "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.SHORT, "32768", "is out of range for a short"),
            OutOfRangeSpec.of(NumberType.INTEGER, "2147483648", "is out of range for an integer"),
            OutOfRangeSpec.of(NumberType.LONG, "9223372036854775808", "out of range for a long"),
            OutOfRangeSpec.of(NumberType.LONG, "1e999999999", "out of range for a long"),
            OutOfRangeSpec.of(NumberType.UNSIGNED_LONG, "18446744073709551616", "out of range for an unsigned long"),

            OutOfRangeSpec.of(NumberType.BYTE, "-129", "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.SHORT, "-32769", "is out of range for a short"),
            OutOfRangeSpec.of(NumberType.INTEGER, "-2147483649", "is out of range for an integer"),
            OutOfRangeSpec.of(NumberType.LONG, "-9223372036854775809", "out of range for a long"),
            OutOfRangeSpec.of(NumberType.LONG, "-1e999999999", "out of range for a long"),
            OutOfRangeSpec.of(NumberType.UNSIGNED_LONG, "-1", "out of range for an unsigned long"),

            OutOfRangeSpec.of(NumberType.BYTE, 128, "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.SHORT, 32768, "out of range of Java short"),
            OutOfRangeSpec.of(NumberType.INTEGER, 2147483648L, " out of range of int"),
            OutOfRangeSpec.of(NumberType.LONG, new BigInteger("9223372036854775808"), "out of range of long"),
            OutOfRangeSpec.of(NumberType.UNSIGNED_LONG, new BigInteger("18446744073709551616"), "out of range for an unsigned long"),

            OutOfRangeSpec.of(NumberType.BYTE, -129, "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.SHORT, -32769, "out of range of Java short"),
            OutOfRangeSpec.of(NumberType.INTEGER, -2147483649L, " out of range of int"),
            OutOfRangeSpec.of(NumberType.LONG, new BigInteger("-9223372036854775809"), "out of range of long"),
            OutOfRangeSpec.of(NumberType.UNSIGNED_LONG, new BigInteger("-1"), "out of range for an unsigned long"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, "65520", "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, "3.4028235E39", "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, "1.7976931348623157E309", "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, "-65520", "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, "-3.4028235E39", "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, "-1.7976931348623157E309", "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, Float.NaN, "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, Float.NaN, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, Double.NaN, "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, Float.POSITIVE_INFINITY, "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, Float.POSITIVE_INFINITY, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, Double.POSITIVE_INFINITY, "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, Float.NEGATIVE_INFINITY, "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, Float.NEGATIVE_INFINITY, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, Double.NEGATIVE_INFINITY, "[double] supports only finite values")
        );

        for (OutOfRangeSpec item : inputs) {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", item.type.typeName())));
            try {
                mapper.parse(source(item::write));
                fail("Mapper parsing exception expected for [" + item.type + "] with value [" + item.value + "]");
            } catch (MapperParsingException e) {
                assertThat(
                    "Incorrect error message for [" + item.type + "] with value [" + item.value + "]",
                    e.getCause().getMessage(),
                    containsString(item.message)
                );
            }
        }

        // the following two strings are in-range for a long after coercion
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "9223372036854775807.9")));
        assertThat(doc.rootDoc().getFields("field"), arrayWithSize(2));
        doc = mapper.parse(source(b -> b.field("field", "-9223372036854775808.9")));
        assertThat(doc.rootDoc().getFields("field"), arrayWithSize(2));
    }

    public void testLongIndexingOutOfRange() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long").field("ignore_malformed", true)));
        ParsedDocument doc = mapper.parse(
            source(b -> b.rawField("field", new BytesArray("9223372036854775808").streamInput(), MediaTypeRegistry.JSON))
        );
        assertEquals(0, doc.rootDoc().getFields("field").length);
    }

    public void testPossibleToDeriveSource_WhenDocValuesAndStoredDisabled() throws IOException {
        NumberFieldMapper mapper = getMapper(NumberFieldMapper.NumberType.HALF_FLOAT, FieldMapper.CopyTo.empty(), false, false);
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testPossibleToDeriveSource_WhenCopyToPresent() throws IOException {
        FieldMapper.CopyTo copyTo = new FieldMapper.CopyTo.Builder().add("copy_to_field").build();
        NumberFieldMapper mapper = getMapper(NumberFieldMapper.NumberType.HALF_FLOAT, copyTo, true, true);
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testFloatFieldDerivedValueFetching_DocValues() throws IOException {
        NumberType[] floatTypes = { NumberType.FLOAT, NumberType.HALF_FLOAT, NumberType.DOUBLE };
        for (NumberType type : floatTypes) {
            try (Directory directory = newDirectory()) {
                NumberFieldMapper mapper = getMapper(type, FieldMapper.CopyTo.empty(), true, false);
                float value = 1.5f;
                try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                    iw.addDocument(createDocument(type, List.of(value), true));
                }

                try (DirectoryReader reader = DirectoryReader.open(directory)) {
                    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                    mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
                    builder.endObject();
                    String source = builder.toString();
                    assertEquals("{\"" + FIELD_NAME + "\":" + value + "}", source);
                }
            }
        }
    }

    public void testFloatFieldDerivedValueFetching_StoredField() throws IOException {
        NumberType[] floatTypes = { NumberType.FLOAT, NumberType.HALF_FLOAT, NumberType.DOUBLE };
        for (NumberType type : floatTypes) {
            try (Directory directory = newDirectory()) {
                NumberFieldMapper mapper = getMapper(type, FieldMapper.CopyTo.empty(), false, true);
                float value = 1.5f;
                try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                    iw.addDocument(createDocument(type, List.of(value), false));
                }

                try (DirectoryReader reader = DirectoryReader.open(directory)) {
                    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                    mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
                    builder.endObject();
                    String source = builder.toString();
                    assertEquals("{\"" + FIELD_NAME + "\":" + value + "}", source);
                }
            }
        }
    }

    public void testIntFieldDerivedValueFetching_DocValues() throws IOException {
        NumberType[] fieldTypes = { NumberType.INTEGER, NumberType.SHORT, NumberType.BYTE };
        for (NumberType type : fieldTypes) {
            try (Directory directory = newDirectory()) {
                NumberFieldMapper mapper = getMapper(type, FieldMapper.CopyTo.empty(), true, false);
                int value = 123;
                try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                    iw.addDocument(createDocument(type, List.of(value), true));
                }

                try (DirectoryReader reader = DirectoryReader.open(directory)) {
                    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                    mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
                    builder.endObject();
                    String source = builder.toString();
                    assertEquals("{\"" + FIELD_NAME + "\":" + value + "}", source);
                }
            }
        }
    }

    public void testLongFieldDerivedValueFetching_DocValues() throws IOException {
        NumberType[] fieldTypes = { NumberType.LONG, NumberType.UNSIGNED_LONG };
        for (NumberType type : fieldTypes) {
            try (Directory directory = newDirectory()) {
                NumberFieldMapper mapper = getMapper(type, FieldMapper.CopyTo.empty(), true, false);
                long value = (1L << 53) + randomLongBetween(0L, 1L << 20);
                try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                    iw.addDocument(createDocument(type, List.of(value), true));
                }

                try (DirectoryReader reader = DirectoryReader.open(directory)) {
                    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                    mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
                    builder.endObject();
                    String source = builder.toString();
                    assertEquals("{\"" + FIELD_NAME + "\":" + value + "}", source);
                }
            }
        }
    }

    public void testIntFieldDerivedValueFetching_StoredField() throws IOException {
        NumberType[] floatTypes = { NumberType.INTEGER, NumberType.LONG, NumberType.UNSIGNED_LONG, NumberType.SHORT, NumberType.BYTE };
        for (NumberType type : floatTypes) {
            try (Directory directory = newDirectory()) {
                NumberFieldMapper mapper = getMapper(type, FieldMapper.CopyTo.empty(), false, true);
                int value = 123;
                try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                    iw.addDocument(createDocument(type, List.of(value), false));
                }

                try (DirectoryReader reader = DirectoryReader.open(directory)) {
                    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                    mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
                    builder.endObject();
                    String source = builder.toString();
                    assertEquals("{\"" + FIELD_NAME + "\":" + value + "}", source);
                }
            }
        }
    }

    public void testLongFieldDerivedValueFetchingMultiValue_DocValues() throws IOException {
        try (Directory directory = newDirectory()) {
            NumberFieldMapper mapper = getMapper(NumberType.LONG, FieldMapper.CopyTo.empty(), true, false);
            long value1 = Integer.MAX_VALUE;
            long value2 = Long.MIN_VALUE;
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                iw.addDocument(createDocument(NumberType.LONG, List.of(value1, value2, value1), true));
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
                builder.endObject();
                String source = builder.toString();
                assertEquals("{\"" + FIELD_NAME + "\":[" + value2 + "," + value1 + "," + value1 + "]}", source);
            }
        }
    }

    public void testUnsignedLongFieldDerivedValueFetchingMultiValue_DocValues() throws IOException {
        try (Directory directory = newDirectory()) {
            NumberFieldMapper mapper = getMapper(NumberType.UNSIGNED_LONG, FieldMapper.CopyTo.empty(), true, false);
            long value1 = Integer.MAX_VALUE;
            BigInteger value2 = new BigInteger("9223372036854775808");
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                iw.addDocument(createDocument(NumberType.UNSIGNED_LONG, List.of(value2.longValue(), value1, value2.longValue()), true));
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
                builder.endObject();
                String source = builder.toString();
                assertEquals("{\"" + FIELD_NAME + "\":[" + value1 + "," + value2 + "," + value2 + "]}", source);
            }
        }
    }

    public void testSkipList() throws IOException {
        for (String type : types()) {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> b.field("type", type).field("index", false).field("skip_list", true))
            );
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

            IndexableField[] fields = doc.rootDoc().getFields("field");
            assertEquals(1, fields.length);
            IndexableField dvField = fields[0];
            assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
            assertEquals(DocValuesSkipIndexType.RANGE, dvField.fieldType().docValuesSkipIndexType());
        }
    }

    private NumberFieldMapper getMapper(NumberType numberType, FieldMapper.CopyTo copyTo, boolean hasDocValues, boolean isStored)
        throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", numberType.typeName()).field("store", isStored).field("doc_values", hasDocValues))
        );
        NumberFieldMapper mapper = (NumberFieldMapper) mapperService.documentMapper().mappers().getMapper(FIELD_NAME);
        mapper.copyTo = copyTo;
        return mapper;
    }

    /**
     * Helper method to create a document with both doc values and stored fields
     */
    private Document createDocument(NumberFieldMapper.NumberType type, List<Number> values, boolean hasDocValues) {
        Document doc = new Document();

        // Add doc values field
        if (hasDocValues) {
            for (final Number value : values) {
                switch (type) {
                    case HALF_FLOAT:
                        doc.add(new SortedNumericDocValuesField(FIELD_NAME, HalfFloatPoint.halfFloatToSortableShort(value.floatValue())));
                        break;
                    case FLOAT:
                        doc.add(new SortedNumericDocValuesField(FIELD_NAME, NumericUtils.floatToSortableInt(value.floatValue())));
                        break;
                    case DOUBLE:
                        doc.add(new SortedNumericDocValuesField(FIELD_NAME, NumericUtils.doubleToSortableLong(value.doubleValue())));
                        break;
                    case BYTE:
                    case SHORT:
                    case INTEGER:
                        doc.add(new SortedNumericDocValuesField(FIELD_NAME, value.intValue()));
                        break;
                    case LONG:
                        doc.add(new SortedNumericDocValuesField(FIELD_NAME, value.longValue()));
                        break;
                    case UNSIGNED_LONG:
                        doc.add(
                            new SortedNumericDocValuesField(
                                FIELD_NAME,
                                NumberFieldMapper.NumberType.objectToUnsignedLong(value, false).longValue()
                            )
                        );
                        break;
                }
            }
            return doc;
        }

        // Add stored field
        for (final Number value : values) {
            switch (type) {
                case HALF_FLOAT:
                case FLOAT:
                    doc.add(new StoredField(FIELD_NAME, value.floatValue()));
                    break;
                case DOUBLE:
                    doc.add(new StoredField(FIELD_NAME, value.doubleValue()));
                    break;
                case BYTE:
                case SHORT:
                case INTEGER:
                    doc.add(new StoredField(FIELD_NAME, value.intValue()));
                    break;
                case LONG:
                    doc.add(new StoredField(FIELD_NAME, value.longValue()));
                    break;
                case UNSIGNED_LONG:
                    doc.add(new StoredField(FIELD_NAME, value.toString()));
                    break;
            }
        }
        return doc;
    }
}
