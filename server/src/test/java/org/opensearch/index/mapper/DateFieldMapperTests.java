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

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.termvectors.TermVectorsService;
import org.opensearch.search.DocValueFormat;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assume.assumeThat;

public class DateFieldMapperTests extends MapperTestCase {

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
        checker.registerConflictCheck("format", b -> b.field("format", "yyyy-MM-dd"));
        checker.registerConflictCheck("print_format", b -> b.field("print_format", "yyyy-MM-dd"));
        checker.registerConflictCheck("locale", b -> b.field("locale", "es"));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", "34500000"));
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true), m -> assertTrue(((DateFieldMapper) m).getIgnoreMalformed()));
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
        testIgnoreMalformedForValue("-522000000", "long overflow");
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
        testIgnoreMalformedForValue("-522000000", "long overflow");
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
}
