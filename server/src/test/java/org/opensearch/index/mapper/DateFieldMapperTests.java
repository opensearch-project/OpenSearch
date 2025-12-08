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

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IndexSortConfig;
import org.opensearch.index.fieldvisitor.SingleFieldsVisitor;
import org.opensearch.index.termvectors.TermVectorsService;
import org.opensearch.search.DocValueFormat;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DateFieldMapperTests extends MapperTestCase {

    private static final long TEST_TIMESTAMP = 1739858400000L;

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value("2016-03-11");
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "date");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("skip_list", b -> b.field("skip_list", true));
        checker.registerConflictCheck("format", b -> b.field("format", "yyyy-MM-dd"));
        checker.registerConflictCheck("print_format", b -> b.field("print_format", "yyyy-MM-dd"));
        checker.registerConflictCheck("locale", b -> b.field("locale", "es"));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", "34500000"));
        checker.registerUpdateCheck(
            b -> b.field("ignore_malformed", true),
            m -> assertTrue(((DateFieldMapper) m).ignoreMalformed().value())
        );
        checker.registerUpdateCheck(b -> b.field("boost", 2.0), m -> assertEquals(m.fieldType().boost(), 2.0, 0));
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
    protected void assertParseMaximalWarnings() {
        assertWarnings("Parameter [boost] on field [field] is deprecated and will be removed in 3.0");
    }

    public void testWithContextAwareGroupingMapper() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            contextAwareGrouping("field").accept(b);
            properties(x -> {
                x.startObject("field");
                minimalMapping(x);
                b.endObject();
            }).accept(b);
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "2016-03-11")));

        // Assert date field
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(8, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(1457654400000L, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1457654400000L, dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());

        // Assert groupingcriteria is correct
        assertEquals("2016-03-11", doc.docs().getFirst().getGroupingCriteria());
    }

    public void testDefaults() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "2016-03-11")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(8, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(1457654400000L, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1457654400000L, dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());
    }

    public void testNotIndexed() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("index", false)));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "2016-03-11")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
    }

    public void testNoDocValues() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("doc_values", false)));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "2016-03-11")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
    }

    public void testStore() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("store", true)));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "2016-03-11")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(1457654400000L, storedField.numericValue().longValue());
    }

    public void testIgnoreMalformedLegacy() throws IOException {
        assumeThat("Using legacy datetime format as default", FeatureFlags.isEnabled(FeatureFlags.DATETIME_FORMATTER_CACHING), is(false));
        testIgnoreMalformedForValue(
            "2016-03-99",
            "failed to parse date field [2016-03-99] with format [strict_date_optional_time||epoch_millis]"
        );
        testIgnoreMalformedForValue("-2147483648", "Invalid value for Year (valid values -999999999 - 999999999): -2147483648");
    }

    public void testIgnoreMalformed() throws IOException {
        assumeThat(
            "Using experimental datetime format as default",
            FeatureFlags.isEnabled(FeatureFlags.DATETIME_FORMATTER_CACHING),
            is(true)
        );
        testIgnoreMalformedForValue(
            "2016-03-99",
            "failed to parse date field [2016-03-99] with format [strict_date_time_no_millis||strict_date_optional_time||epoch_millis]"
        );
        testIgnoreMalformedForValue("-2147483648", "Invalid value for Year (valid values -999999999 - 999999999): -2147483648");
    }

    private void testIgnoreMalformedForValue(String value, String expectedCause) throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("field", value))));
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [date]"));
        assertThat(e.getMessage(), containsString("Preview of field's value: '" + value + "'"));
        assertThat(e.getCause().getMessage(), containsString(expectedCause));

        DocumentMapper mapper2 = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("ignore_malformed", true)));

        ParsedDocument doc = mapper2.parse(source(b -> b.field("field", value)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
        assertArrayEquals(new String[] { "field" }, TermVectorsService.getValues(doc.rootDoc().getFields("_ignored")));
    }

    public void testChangeFormat() throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("format", "epoch_second")));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 1457654400)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1457654400000L, pointField.numericValue().longValue());
    }

    public void testChangeLocale() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "date").field("format", "E, d MMM yyyy HH:mm:ss Z").field("locale", "de"))
        );

        mapper.parse(source(b -> b.field("field", "Mi., 06 Dez. 2000 02:55:00 -0800")));
    }

    public void testNullValue() throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("null_value", "2016-03-11")));

        doc = mapper.parse(source(b -> b.nullField("field")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(8, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(1457654400000L, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1457654400000L, dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());
    }

    public void testNanosNullValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "date_nanos").field("null_value", "2016-03-11"))
        );

        DateFieldMapper.DateFieldType ft = (DateFieldMapper.DateFieldType) mapperService.fieldType("field");
        long expectedNullValue = ft.parse("2016-03-11");

        doc = mapperService.documentMapper().parse(source(b -> b.nullField("field")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(8, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(expectedNullValue, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(expectedNullValue, dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());
    }

    public void testBadNullValue() throws IOException {
        createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("null_value", "foo")));

        assertWarnings("Error parsing [foo] as date in [null_value] on field [field]); [null_value] will be ignored");
    }

    public void testNullConfigValuesFail() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "date").nullField("format")))
        );
        assertThat(e.getMessage(), containsString("[format] on mapper [field] of type [date] must not have a [null] value"));
    }

    public void testTimeZoneParsing() throws Exception {
        final String timeZonePattern = "yyyy-MM-dd" + randomFrom("XXX", "[XXX]", "'['XXX']'");

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("format", timeZonePattern)));

        DateFormatter formatter = DateFormatter.forPattern(timeZonePattern);
        final ZoneId randomTimeZone = randomBoolean() ? ZoneId.of(randomFrom("UTC", "CET")) : randomZone();
        final ZonedDateTime randomDate = ZonedDateTime.of(2016, 3, 11, 0, 0, 0, 0, randomTimeZone);

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", formatter.format(randomDate))));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        long millis = randomDate.withZoneSameInstant(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEquals(millis, fields[0].numericValue().longValue());
    }

    public void testMergeDate() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "date").field("format", "yyyy/MM/dd")));

        assertThat(mapperService.fieldType("field"), notNullValue());
        assertFalse(mapperService.fieldType("field").isStored());

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> b.field("type", "date").field("format", "epoch_millis")))
        );
        assertThat(e.getMessage(), containsString("parameter [format] from [yyyy/MM/dd] to [epoch_millis]"));
    }

    public void testMergeText() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> b.field("type", "text")))
        );
        assertEquals("mapper [field] cannot be changed from type [date] to [text]", e.getMessage());
    }

    public void testIllegalFormatField() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("format", "test_format")))
        );
        assertThat(e.getMessage(), containsString("Invalid format: [test_format]: Unknown pattern letter: t"));
        assertThat(e.getMessage(), containsString("Error parsing [format] on field [field]: Invalid"));
    }

    public void testFetchDocValuesMillis() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "date").field("format", "strict_date_time||epoch_millis"))
        );
        MappedFieldType ft = mapperService.fieldType("field");
        DocValueFormat format = ft.docValueFormat(null, null);
        String date = "2020-05-15T21:33:02.123Z";
        assertEquals(List.of(date), fetchFromDocValues(mapperService, ft, format, date));
        assertEquals(List.of(date), fetchFromDocValues(mapperService, ft, format, 1589578382123L));
    }

    public void testFetchDocValuesNanos() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "date_nanos").field("format", "strict_date_time||epoch_millis"))
        );
        MappedFieldType ft = mapperService.fieldType("field");
        DocValueFormat format = ft.docValueFormat(null, null);
        String date = "2020-05-15T21:33:02.123456789Z";
        assertEquals(List.of(date), fetchFromDocValues(mapperService, ft, format, date));
        assertEquals(List.of("2020-05-15T21:33:02.123Z"), fetchFromDocValues(mapperService, ft, format, 1589578382123L));
    }

    public void testPossibleToDeriveSource_WhenDerivedSourceDisabled() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "date_nanos").field("format", "strict_date_time||epoch_millis").field("copy_to", "a"))
        );
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThrows(UnsupportedOperationException.class, dateFieldMapper::canDeriveSource);
    }

    public void testPossibleToDeriveSource_WhenCopyToPresent() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "date_nanos").field("format", "strict_date_time||epoch_millis").field("copy_to", "a"))
        );
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThrows(UnsupportedOperationException.class, dateFieldMapper::canDeriveSource);
    }

    public void testPossibleToDeriveSource_WhenDocValuesAndStoreFieldDisabled() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "date").field("doc_values", false).field("store", false))
        );
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThrows(UnsupportedOperationException.class, dateFieldMapper::canDeriveSource);
    }

    public void testDeriveSource_WhenStoredFieldEnabledAndDateType() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "date").field("format", "strict_date_time_no_millis").field("doc_values", false).field("store", true)
            )
        );
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        LeafReader leafReader = mock(LeafReader.class);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

        StoredFields storedFields = mock(StoredFields.class);
        when(leafReader.storedFields()).thenReturn(storedFields);

        FieldInfo mockFieldInfo = new FieldInfo(
            "field",
            1,
            false,
            false,
            true,
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

        doAnswer(invocation -> {
            SingleFieldsVisitor visitor = invocation.getArgument(1);
            visitor.longField(mockFieldInfo, TEST_TIMESTAMP);
            return null;
        }).when(storedFields).document(anyInt(), any(StoredFieldVisitor.class));

        dateFieldMapper.deriveSource(builder, leafReader, 0);
        builder.endObject();
        String source = builder.toString();
        assertTrue(source.contains("\"field\":\"2025-02-18T06:00:00Z\""));
    }

    public void testDeriveSource_WhenStoredFieldEnabledAndDateNanosType() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "date_nanos")
                    .field("format", "strict_date_optional_time_nanos")
                    .field("doc_values", false)
                    .field("store", true)
            )
        );
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        LeafReader leafReader = mock(LeafReader.class);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

        StoredFields storedFields = mock(StoredFields.class);
        when(leafReader.storedFields()).thenReturn(storedFields);

        FieldInfo mockFieldInfo = new FieldInfo(
            "field",
            1,
            false,
            false,
            true,
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

        doAnswer(invocation -> {
            SingleFieldsVisitor visitor = invocation.getArgument(1);
            visitor.longField(mockFieldInfo, TEST_TIMESTAMP * 1000000L + 111111111L);
            return null;
        }).when(storedFields).document(anyInt(), any(StoredFieldVisitor.class));

        dateFieldMapper.deriveSource(builder, leafReader, 0);
        builder.endObject();
        String source = builder.toString();
        assertTrue(source.contains("\"field\":\"2025-02-18T06:00:00.111111111Z\""));
    }

    public void testDeriveSource_WhenStoredFieldEnabledWithMultiValue() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "date").field("format", "strict_date_time_no_millis").field("doc_values", false).field("store", true)
            )
        );
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        LeafReader leafReader = mock(LeafReader.class);
        StoredFields storedFields = mock(StoredFields.class);
        when(leafReader.storedFields()).thenReturn(storedFields);

        FieldInfo mockFieldInfo = new FieldInfo(
            "field",
            1,
            false,
            false,
            true,
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
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        doAnswer(invocation -> {
            SingleFieldsVisitor visitor = invocation.getArgument(1);
            visitor.longField(mockFieldInfo, TEST_TIMESTAMP);
            visitor.longField(mockFieldInfo, TEST_TIMESTAMP + 3600000); // One hour later
            return null;
        }).when(storedFields).document(anyInt(), any(StoredFieldVisitor.class));

        dateFieldMapper.deriveSource(builder, leafReader, 0);
        builder.endObject();
        String source = builder.toString();
        assertTrue(source.contains("\"field\":[\"2025-02-18T06:00:00Z\",\"2025-02-18T07:00:00Z\"]"));
    }

    public void testDeriveSource_WhenDocValuesEnabledAndDateType() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "date").field("format", "strict_date_time_no_millis").field("store", false).field("doc_values", true)
            )
        );
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        LeafReader leafReader = mock(LeafReader.class);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

        SortedNumericDocValues docValues = mock(SortedNumericDocValues.class);
        when(leafReader.getSortedNumericDocValues("field")).thenReturn(docValues);
        when(docValues.advanceExact(0)).thenReturn(true);
        when(docValues.docValueCount()).thenReturn(1);
        when(docValues.nextValue()).thenReturn(TEST_TIMESTAMP);

        dateFieldMapper.deriveSource(builder, leafReader, 0);
        builder.endObject();
        String source = builder.toString();
        assertTrue(source.contains("\"field\":\"2025-02-18T06:00:00Z\""));
    }

    public void testDeriveSource_WhenDocValuesEnabledAndDateNanosType() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "date_nanos")
                    .field("format", "strict_date_optional_time_nanos")
                    .field("store", false)
                    .field("doc_values", true)
            )
        );
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        LeafReader leafReader = mock(LeafReader.class);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

        SortedNumericDocValues docValues = mock(SortedNumericDocValues.class);
        when(leafReader.getSortedNumericDocValues("field")).thenReturn(docValues);
        when(docValues.advanceExact(0)).thenReturn(true);
        when(docValues.docValueCount()).thenReturn(1);
        when(docValues.nextValue()).thenReturn(TEST_TIMESTAMP * 1000000L + 222222222L);

        dateFieldMapper.deriveSource(builder, leafReader, 0);
        builder.endObject();
        String source = builder.toString();
        assertTrue(source.contains("\"field\":\"2025-02-18T06:00:00.222222222Z\""));
    }

    public void testDeriveSource_WhenDocValuesEnabledWithMultiValue() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "date").field("format", "strict_date_time_no_millis").field("store", false).field("doc_values", true)
            )
        );
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        LeafReader leafReader = mock(LeafReader.class);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

        SortedNumericDocValues docValues = mock(SortedNumericDocValues.class);
        when(leafReader.getSortedNumericDocValues("field")).thenReturn(docValues);
        when(docValues.advanceExact(0)).thenReturn(true);
        when(docValues.docValueCount()).thenReturn(2);
        when(docValues.nextValue()).thenReturn(TEST_TIMESTAMP).thenReturn(TEST_TIMESTAMP + 3600000L); // One Hour Later

        dateFieldMapper.deriveSource(builder, leafReader, 0);
        builder.endObject();
        String source = builder.toString();
        assertTrue(source.contains("\"field\":[\"2025-02-18T06:00:00Z\",\"2025-02-18T07:00:00Z\"]"));
    }

    public void testDeriveSource_NoValue() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "date").field("format", "strict_date_time_no_millis").field("store", false).field("doc_values", true)
            )
        );
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        LeafReader leafReader = mock(LeafReader.class);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

        SortedNumericDocValues docValues = mock(SortedNumericDocValues.class);
        when(leafReader.getSortedNumericDocValues("field")).thenReturn(docValues);
        when(docValues.advanceExact(0)).thenReturn(false);

        dateFieldMapper.deriveSource(builder, leafReader, 0);
        builder.endObject();
        String source = builder.toString();
        assertEquals("{}", source);
    }

    public void testDateEncodePoint() {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType("test_field");
        // Test basic roundUp
        long baseTime = fieldType.parse("2024-01-15T10:30:00Z");
        byte[] encoded = fieldType.encodePoint("2024-01-15T10:30:00Z", true);
        long decoded = LongPoint.decodeDimension(encoded, 0);
        assertEquals(baseTime + 1, decoded);
        // Test basic roundDown
        encoded = fieldType.encodePoint("2024-01-15T10:30:00Z", false);
        decoded = LongPoint.decodeDimension(encoded, 0);
        assertEquals(baseTime - 1, decoded);
        // Test with extreme long values,
        long largeEpoch = 253402300799999L;
        String largeEpochStr = String.valueOf(largeEpoch);
        encoded = fieldType.encodePoint(largeEpochStr, true);
        decoded = LongPoint.decodeDimension(encoded, 0);
        assertEquals("Should increment epoch millis", largeEpoch + 1, decoded);
        long negativeEpoch = -377705116800000L;
        String negativeEpochStr = String.valueOf(negativeEpoch);
        encoded = fieldType.encodePoint(negativeEpochStr, false);
        decoded = LongPoint.decodeDimension(encoded, 0);
        assertEquals("Should decrement epoch millis", negativeEpoch - 1, decoded);
    }

    public void testSkipListParameterValidBooleanValues() throws IOException {
        // Test skip_list=true
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("skip_list", true)));
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapper.mappers().getMapper("field");
        assertTrue("skip_list should be true", dateFieldMapper.skiplist());

        // Test skip_list=false
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("skip_list", false)));
        dateFieldMapper = (DateFieldMapper) mapper.mappers().getMapper("field");
        assertFalse("skip_list should be false", dateFieldMapper.skiplist());
    }

    public void testSkipListParameterDefaultBehavior() throws IOException {
        // Test default behavior when skip_list parameter is omitted
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date")));
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapper.mappers().getMapper("field");
        assertFalse("skip_list should default to false", dateFieldMapper.skiplist());
    }

    public void testSkipListParameterInvalidValues() throws IOException {
        // Test invalid string value
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("skip_list", "invalid")))
        );
        assertThat(e.getMessage(), containsString("Failed to parse mapping"));

        // Test invalid numeric value
        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("skip_list", 123)))
        );
        assertThat(e.getMessage(), containsString("Failed to parse mapping"));

        // Test null value
        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "date").nullField("skip_list")))
        );
        assertThat(e.getMessage(), containsString("Failed to parse mapping"));
    }

    public void testSkipListParameterDateNanos() throws IOException {
        // Test skip_list=true with date_nanos type
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date_nanos").field("skip_list", true)));
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapper.mappers().getMapper("field");
        assertTrue("skip_list should be true for date_nanos", dateFieldMapper.skiplist());

        // Test skip_list=false with date_nanos type
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date_nanos").field("skip_list", false)));
        dateFieldMapper = (DateFieldMapper) mapper.mappers().getMapper("field");
        assertFalse("skip_list should be false for date_nanos", dateFieldMapper.skiplist());

        // Test default behavior for date_nanos
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date_nanos")));
        dateFieldMapper = (DateFieldMapper) mapper.mappers().getMapper("field");
        assertFalse("skip_list should default to false for date_nanos", dateFieldMapper.skiplist());
    }

    // Integration tests for end-to-end skip_list functionality

    public void testSkipListIntegrationDateFieldWithIndexedDocValues() throws IOException {
        // Test creating index with skip_list=true for date field
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("skip_list", true)));

        // Parse a document with date field
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "2016-03-11T10:30:00Z")));

        // Verify the field structure
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals("Should have 2 fields (point and doc values)", 2, fields.length);

        // Verify point field
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(8, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(1457692200000L, pointField.numericValue().longValue());

        // Verify doc values field - when skip_list=true, should use indexed doc values
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1457692200000L, dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());

        // Verify the mapper has skip_list enabled
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapper.mappers().getMapper("field");
        assertTrue("skip_list should be enabled", dateFieldMapper.skiplist());
    }

    public void testSkipListIntegrationDateNanosFieldWithIndexedDocValues() throws IOException {
        // Test creating index with skip_list=true for date_nanos field
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date_nanos").field("skip_list", true)));

        // Parse a document with date_nanos field
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "2016-03-11T10:30:00.123456789Z")));

        // Verify the field structure
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals("Should have 2 fields (point and doc values)", 2, fields.length);

        // Verify point field
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(8, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());

        // Verify doc values field - when skip_list=true, should use indexed doc values
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());

        // Verify the mapper has skip_list enabled
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapper.mappers().getMapper("field");
        assertTrue("skip_list should be enabled", dateFieldMapper.skiplist());
    }

    public void testSkipListIntegrationRegularDocValuesWhenDisabled() throws IOException {
        // Test creating index with skip_list=false for date field
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("skip_list", false)));

        // Parse a document with date field
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "2016-03-11T10:30:00Z")));

        // Verify the field structure
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals("Should have 2 fields (point and doc values)", 2, fields.length);

        // Verify point field
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(8, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(1457692200000L, pointField.numericValue().longValue());

        // Verify doc values field - when skip_list=false, should use regular doc values
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1457692200000L, dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());

        // Verify the mapper has skip_list disabled
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapper.mappers().getMapper("field");
        assertFalse("skip_list should be disabled", dateFieldMapper.skiplist());
    }

    public void testSkipListIntegrationWithMultipleResolutions() throws IOException {
        // Test both MILLISECONDS and NANOSECONDS resolution with skip_list enabled

        // Test MILLISECONDS resolution (date type)
        DocumentMapper mapperMillis = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("skip_list", true)));
        ParsedDocument docMillis = mapperMillis.parse(source(b -> b.field("field", "2016-03-11T10:30:00Z")));

        IndexableField[] fieldsMillis = docMillis.rootDoc().getFields("field");
        assertEquals("Date field should have 2 fields", 2, fieldsMillis.length);
        assertEquals(1457692200000L, fieldsMillis[0].numericValue().longValue());
        assertEquals(1457692200000L, fieldsMillis[1].numericValue().longValue());

        DateFieldMapper dateMapperMillis = (DateFieldMapper) mapperMillis.mappers().getMapper("field");
        assertTrue("Date field skip_list should be enabled", dateMapperMillis.skiplist());

        // Test NANOSECONDS resolution (date_nanos type)
        DocumentMapper mapperNanos = createDocumentMapper(fieldMapping(b -> b.field("type", "date_nanos").field("skip_list", true)));
        ParsedDocument docNanos = mapperNanos.parse(source(b -> b.field("field", "2016-03-11T10:30:00.123456789Z")));

        IndexableField[] fieldsNanos = docNanos.rootDoc().getFields("field");
        assertEquals("Date_nanos field should have 2 fields", 2, fieldsNanos.length);

        DateFieldMapper dateMapperNanos = (DateFieldMapper) mapperNanos.mappers().getMapper("field");
        assertTrue("Date_nanos field skip_list should be enabled", dateMapperNanos.skiplist());
    }

    public void testSkipListIntegrationMappingDefinitionSerialization() throws IOException {
        // Test that skip_list parameter appears correctly in mapping definitions

        // Create mapper with skip_list=true
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "date").field("skip_list", true)));

        // Get the field type and verify skip_list is set
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertTrue("skip_list should be true in mapper", dateFieldMapper.skiplist());

        // Test with date_nanos as well
        MapperService mapperServiceNanos = createMapperService(fieldMapping(b -> b.field("type", "date_nanos").field("skip_list", true)));
        DateFieldMapper dateFieldMapperNanos = (DateFieldMapper) mapperServiceNanos.documentMapper().mappers().getMapper("field");
        assertTrue("skip_list should be true in date_nanos mapper", dateFieldMapperNanos.skiplist());
    }

    public void testIsSkiplistDefaultEnabled() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date")));
        testIsSkiplistEnabled(mapper, true);

    }

    public void testIsSkiplistDefaultDisabledInOlderVersions() throws IOException {
        DocumentMapper mapper = createDocumentMapper(Version.V_3_2_0, fieldMapping(b -> b.field("type", "date")));
        testIsSkiplistEnabled(mapper, false);
    }

    private void testIsSkiplistEnabled(DocumentMapper mapper, boolean expectedValue) throws IOException {
        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapper.mappers().getMapper("field");

        // Test with no index sort and non-timestamp field
        IndexMetadata noSortindexMetadata = new IndexMetadata.Builder("index").settings(getIndexSettings()).build();
        IndexSortConfig noSortConfig = new IndexSortConfig(new IndexSettings(noSortindexMetadata, getIndexSettings()));
        assertFalse(dateFieldMapper.isSkiplistDefaultEnabled(noSortConfig, "field"));

        // timestamp field
        assertEquals(expectedValue, dateFieldMapper.isSkiplistDefaultEnabled(noSortConfig, "@timestamp"));

        // Create index settings with an index sort.
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .putList("index.sort.field", "field")
            .build();

        // Test with timestamp field
        IndexMetadata indexMetadata = new IndexMetadata.Builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        IndexSortConfig sortConfig = new IndexSortConfig(indexSettings);
        assertEquals(expectedValue, dateFieldMapper.isSkiplistDefaultEnabled(sortConfig, "field"));
        assertEquals(expectedValue, dateFieldMapper.isSkiplistDefaultEnabled(sortConfig, "@timestamp"));
    }

    public void testSkipListIntegrationFieldBehaviorConsistency() throws IOException {
        // Test that field behavior is consistent between skip_list enabled and disabled

        String dateValue = "2016-03-11T10:30:00Z";
        long expectedTimestamp = 1457692200000L;

        // Test with skip_list=true
        DocumentMapper mapperEnabled = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("skip_list", true)));
        ParsedDocument docEnabled = mapperEnabled.parse(source(b -> b.field("field", dateValue)));

        // Test with skip_list=false
        DocumentMapper mapperDisabled = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("skip_list", false)));
        ParsedDocument docDisabled = mapperDisabled.parse(source(b -> b.field("field", dateValue)));

        // Both should have the same field structure and values
        IndexableField[] fieldsEnabled = docEnabled.rootDoc().getFields("field");
        IndexableField[] fieldsDisabled = docDisabled.rootDoc().getFields("field");

        assertEquals("Both should have same number of fields", fieldsEnabled.length, fieldsDisabled.length);
        assertEquals(
            "Point field values should match",
            fieldsEnabled[0].numericValue().longValue(),
            fieldsDisabled[0].numericValue().longValue()
        );
        assertEquals(
            "Doc values field values should match",
            fieldsEnabled[1].numericValue().longValue(),
            fieldsDisabled[1].numericValue().longValue()
        );
        assertEquals("Expected timestamp should match", expectedTimestamp, fieldsEnabled[0].numericValue().longValue());
    }
}
