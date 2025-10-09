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
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;

public class ScaledFloatFieldMapperTests extends MapperTestCase {

    private static final String FIELD_NAME = "field";

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new MapperExtrasModulePlugin());
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value(123);
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "scaled_float").field("scaling_factor", 10.0);
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("scaling_factor", fieldMapping(this::minimalMapping), fieldMapping(b -> {
            b.field("type", "scaled_float");
            b.field("scaling_factor", 5.0);
        }));
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("skip_list", b -> b.field("skip_list", true));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", 1));
        checker.registerUpdateCheck(b -> b.field("coerce", false), m -> assertFalse(((ScaledFloatFieldMapper) m).coerce()));
        checker.registerUpdateCheck(
            b -> b.field("ignore_malformed", true),
            m -> assertTrue(((ScaledFloatFieldMapper) m).ignoreMalformed().value())
        );
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testScaledFloatWithStarTree() throws Exception {

        double scalingFactorField1 = randomDouble() * 100;
        double scalingFactorField2 = randomDouble() * 100;
        double scalingFactorField3 = randomDouble() * 100;

        XContentBuilder mapping = getStarTreeMappingWithScaledFloat(scalingFactorField1, scalingFactorField2, scalingFactorField3);
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertTrue(mapping.toString().contains("startree"));

        long randomLongField1 = randomLong();
        long randomLongField2 = randomLong();
        long randomLongField3 = randomLong();
        ParsedDocument doc = mapper.parse(
            source(b -> b.field("field1", randomLongField1).field("field2", randomLongField2).field("field3", randomLongField3))
        );
        validateScaledFloatFields(doc, "field1", randomLongField1, scalingFactorField1);
        validateScaledFloatFields(doc, "field2", randomLongField2, scalingFactorField2);
        validateScaledFloatFields(doc, "field3", randomLongField3, scalingFactorField3);
    }

    @Override
    protected Settings getIndexSettings() {
        return Settings.builder()
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .put(super.getIndexSettings())
            .build();
    }

    private static void validateScaledFloatFields(ParsedDocument doc, String field, long value, double scalingFactor) {
        IndexableField[] fields = doc.rootDoc().getFields(field);
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals((long) (value * scalingFactor), pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals((long) (value * scalingFactor), dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());
    }

    private XContentBuilder getStarTreeMappingWithScaledFloat(
        double scalingFactorField1,
        double scalingFactorField2,
        double scalingFactorField3
    ) throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("max_leaf_docs", 100);
            b.startArray("ordered_dimensions");
            b.startObject();
            b.field("name", "field1");
            b.endObject();
            b.startObject();
            b.field("name", "field2");
            b.endObject();
            b.endArray();
            b.startArray("metrics");
            b.startObject();
            b.field("name", "field3");
            b.startArray("stats");
            b.value("sum");
            b.value("value_count");
            b.endArray();
            b.endObject();
            b.endArray();
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("field1");
            b.field("type", "scaled_float").field("scaling_factor", scalingFactorField1);
            b.endObject();
            b.startObject("field2");
            b.field("type", "scaled_float").field("scaling_factor", scalingFactorField2);
            b.endObject();
            b.startObject("field3");
            b.field("type", "scaled_float").field("scaling_factor", scalingFactorField3);
            b.endObject();
            b.endObject();
        });
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 10.0));
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(mapping.toString(), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));
        validateScaledFloatFields(doc, "field", 123, 10.0);
    }

    public void testMissingScalingFactor() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "scaled_float")))
        );
        assertThat(e.getMessage(), containsString("Failed to parse mapping [_doc]: Field [scaling_factor] is required"));
    }

    public void testIllegalScalingFactor() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", -1)))
        );
        assertThat(e.getMessage(), containsString("[scaling_factor] must be a positive number, got [-1.0]"));
    }

    public void testNotIndexed() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("index", false).field("scaling_factor", 10.0))
        );

        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", 123).endObject()),
                MediaTypeRegistry.JSON
            )
        );

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1230, dvField.numericValue().longValue());
    }

    public void testNoDocValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("doc_values", false).field("scaling_factor", 10.0))
        );

        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", 123).endObject()),
                MediaTypeRegistry.JSON
            )
        );

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(1230, pointField.numericValue().longValue());
    }

    public void testStore() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("store", true).field("scaling_factor", 10.0))
        );

        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", 123).endObject()),
                MediaTypeRegistry.JSON
            )
        );

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(1230, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(1230, storedField.numericValue().longValue());
    }

    public void testCoerce() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "123").endObject()),
                MediaTypeRegistry.JSON
            )
        );
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(1230, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());

        DocumentMapper mapper2 = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 10.0).field("coerce", false))
        );
        ThrowingRunnable runnable = () -> mapper2.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "123").endObject()),
                MediaTypeRegistry.JSON
            )
        );
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString("passed as String"));
    }

    public void testIgnoreMalformed() throws Exception {
        doTestIgnoreMalformed("a", "For input string: \"a\"");
        doTestIgnoreMalformed(true, "Current token (VALUE_TRUE) not numeric");

        List<String> values = Arrays.asList("NaN", "Infinity", "-Infinity");
        for (String value : values) {
            doTestIgnoreMalformed(value, "[scaled_float] only supports finite values, but got [" + value + "]");
        }
    }

    private void doTestIgnoreMalformed(Object value, String exceptionMessageContains) throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ThrowingRunnable runnable = () -> mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", value).endObject()),
                MediaTypeRegistry.JSON
            )
        );
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString(exceptionMessageContains));

        DocumentMapper mapper2 = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 10.0).field("ignore_malformed", true))
        );
        ParsedDocument doc = mapper2.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", value).endObject()),
                MediaTypeRegistry.JSON
            )
        );

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

    public void testNullValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().nullField("field").endObject()),
                MediaTypeRegistry.JSON
            )
        );
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 10.0).field("null_value", 2.5))
        );
        doc = mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().nullField("field").endObject()),
                MediaTypeRegistry.JSON
            )
        );
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(25, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
    }

    public void testPossibleToDeriveSource_WhenDocValuesAndStoredDisabled() throws IOException {
        ScaledFloatFieldMapper mapper = getMapper(FieldMapper.CopyTo.empty(), false, false);
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testPossibleToDeriveSource_WhenCopyToPresent() throws IOException {
        FieldMapper.CopyTo copyTo = new FieldMapper.CopyTo.Builder().add("copy_to_field").build();
        ScaledFloatFieldMapper mapper = getMapper(copyTo, true, true);
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testDerivedValueFetching_DocValues() throws IOException {
        try (Directory directory = newDirectory()) {
            ScaledFloatFieldMapper mapper = getMapper(FieldMapper.CopyTo.empty(), true, false);
            float value = 11.523f;
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                iw.addDocument(createDocument(value, true));
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
                builder.endObject();
                String source = builder.toString();
                assertEquals("{\"" + FIELD_NAME + "\":" + 11.52 + "}", source);
            }
        }
    }

    public void testDerivedValueFetching_StoredField() throws IOException {
        try (Directory directory = newDirectory()) {
            ScaledFloatFieldMapper mapper = getMapper(FieldMapper.CopyTo.empty(), false, true);
            float value = 11.523f;
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                iw.addDocument(createDocument(value, false));
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
                builder.endObject();
                String source = builder.toString();
                assertEquals("{\"" + FIELD_NAME + "\":" + 11.52 + "}", source);
            }
        }
    }

    private ScaledFloatFieldMapper getMapper(FieldMapper.CopyTo copyTo, boolean hasDocValues, boolean isStored) throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "scaled_float").field("store", isStored).field("doc_values", hasDocValues).field("scaling_factor", 100)
            )
        );
        ScaledFloatFieldMapper mapper = (ScaledFloatFieldMapper) mapperService.documentMapper().mappers().getMapper(FIELD_NAME);
        mapper.copyTo = copyTo;
        return mapper;
    }

    /**
     * Helper method to create a document with both doc values and stored fields
     */
    private Document createDocument(double value, boolean hasDocValues) {
        long scaledValue = Math.round(value * 100);
        Document doc = new Document();
        if (hasDocValues) {
            doc.add(new SortedNumericDocValuesField(FIELD_NAME, scaledValue));
        } else {
            doc.add(new StoredField(FIELD_NAME, scaledValue));
        }
        return doc;
    }

    /**
     * `index_options` was deprecated and is rejected as of 7.0
     */
    public void testRejectIndexOptions() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "scaled_float").field("index_options", randomIndexOptions())))
        );
        assertThat(e.getMessage(), containsString("Failed to parse mapping [_doc]: Field [scaling_factor] is required"));
        assertWarnings("Parameter [index_options] has no effect on type [scaled_float] and will be removed in future");
    }

    public void testScaledFloatEncodePoint() {
        double scalingFactor = 100.0;
        ScaledFloatFieldMapper.ScaledFloatFieldType fieldType = new ScaledFloatFieldMapper.ScaledFloatFieldType(
            "test_field",
            scalingFactor
        );
        double originalValue = 10.5;
        byte[] encodedRoundUp = fieldType.encodePoint(originalValue, true);
        byte[] encodedRoundDown = fieldType.encodePoint(originalValue, false);
        long decodedUp = LongPoint.decodeDimension(encodedRoundUp, 0);
        long decodedDown = LongPoint.decodeDimension(encodedRoundDown, 0);
        assertEquals(1051, decodedUp); // 10.5 scaled = 1050, then +1 = 1051 (represents 10.51)
        assertEquals(1049, decodedDown); // 10.5 scaled = 1050, then -1 = 1049 (represents 10.49)
    }

    public void testSkiplistParameter() throws IOException {
        // Test default value (none)
        DocumentMapper defaultMapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 100))
        );
        ParsedDocument doc = defaultMapper.parse(source(b -> b.field("field", 123.45)));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length); // point field + doc values field
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(DocValuesSkipIndexType.NONE, dvField.fieldType().docValuesSkipIndexType());

        // Test skiplist = "skip_list"
        DocumentMapper skiplistMapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 100).field("skip_list", "true"))
        );
        doc = skiplistMapper.parse(source(b -> b.field("field", 123.45)));
        fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(DocValuesSkipIndexType.RANGE, dvField.fieldType().docValuesSkipIndexType());

        // Test invalid value
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 100).field("skip_list", "invalid"))
            )
        );
        assertThat(e.getMessage(), containsString("Failed to parse value [invalid] as only [true] or [false] are allowed"));
    }
}
