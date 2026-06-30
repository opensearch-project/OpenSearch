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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoUtils;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.geometry.utils.Geohash.stringEncode;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GeoPointFieldMapperTests extends FieldMapperTestCase2<GeoPointFieldMapper.Builder> {

    private static final String FIELD_NAME = "field";

    @Override
    protected Set<String> unsupportedProperties() {
        return Set.of("analyzer", "similarity", "doc_values");
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "geo_point");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true), m -> {
            GeoPointFieldMapper gpfm = (GeoPointFieldMapper) m;
            assertTrue(gpfm.ignoreMalformed.value());
        });
        checker.registerUpdateCheck(b -> b.field("ignore_z_value", false), m -> {
            GeoPointFieldMapper gpfm = (GeoPointFieldMapper) m;
            assertFalse(gpfm.ignoreZValue.value());
        });
        GeoPoint point = GeoUtils.parseFromString("41.12,-71.34");
        // TODO this should not be updateable!
        checker.registerUpdateCheck(b -> b.field("null_value", "41.12,-71.34"), m -> {
            GeoPointFieldMapper gpfm = (GeoPointFieldMapper) m;
            assertEquals(gpfm.nullValue, point);
        });
    }

    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value(stringEncode(1.3, 1.2));
    }

    public final void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testGeoHashValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", stringEncode(1.3, 1.2))));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testWKT() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "POINT (2 3)")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLatLonValuesStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("field").field("lat", 1.2).field("lon", 1.3).endObject()));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testArrayLatLonValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_point").field("doc_values", false).field("store", true))
        );
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("field");
            b.startObject().field("lat", 1.2).field("lon", 1.3).endObject();
            b.startObject().field("lat", 1.4).field("lon", 1.5).endObject();
            b.endArray();
        }));

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields("field"), notNullValue());
        assertThat(doc.rootDoc().getFields("field").length, equalTo(4));
    }

    public void testLatLonInOneValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.2,1.3")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLatLonStringWithZValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_z_value", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.2,1.3,10.0")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLatLonStringWithZValueException() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_z_value", false)));
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("field", "1.2,1.3,10.0"))));
        assertThat(e.getCause().getMessage(), containsString("but [ignore_z_value] parameter is [false]"));
    }

    public void testLatLonInOneValueStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.2,1.3")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLatLonInOneValueArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_point").field("doc_values", false).field("store", true))
        );
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field").value("1.2,1.3").value("1.4,1.5").endArray()));

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields("field"), notNullValue());
        assertThat(doc.rootDoc().getFields("field"), arrayWithSize(4));
    }

    public void testLonLatArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field").value(1.3).value(1.2).endArray()));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLonLatArrayDynamic() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc").startArray("dynamic_templates");
        {
            mapping.startObject().startObject("point");
            {
                mapping.field("match", "point*");
                mapping.startObject("mapping").field("type", "geo_point").endObject();
            }
            mapping.endObject().endObject();
        }
        mapping.endArray().endObject().endObject();
        DocumentMapper mapper = createDocumentMapper(mapping);

        ParsedDocument doc = mapper.parse(source(b -> b.startArray("point").value(1.3).value(1.2).endArray()));
        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testLonLatArrayStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field").value(1.3).value(1.2).endArray()));
        assertThat(doc.rootDoc().getFields("field").length, equalTo(3));
    }

    public void testLonLatArrayArrayStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_point").field("store", true).field("doc_values", false))
        );
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("field");
            b.startArray().value(1.3).value(1.2).endArray();
            b.startArray().value(1.5).value(1.4).endArray();
            b.endArray();
        }));
        assertThat(doc.rootDoc().getFields("field"), notNullValue());
        assertThat(doc.rootDoc().getFields("field").length, CoreMatchers.equalTo(4));
    }

    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_z_value", true)));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));
        boolean ignoreZValue = ((GeoPointFieldMapper) fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_z_value", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));
        ignoreZValue = ((GeoPointFieldMapper) fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(false));
    }

    public void testMultiField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_point").field("doc_values", false);
            b.startObject("fields");
            {
                b.startObject("geohash").field("type", "keyword").field("doc_values", false).endObject();  // test geohash as keyword
                b.startObject("latlon").field("type", "text").endObject();  // test geohash as text
            }
            b.endObject();
        }));
        ParseContext.Document doc = mapper.parse(source(b -> b.field("field", "POINT (2 3)"))).rootDoc();
        assertThat(doc.getFields("field"), arrayWithSize(1));
        assertThat(doc.getField("field"), hasToString(both(containsString("field:2.999")).and(containsString("1.999"))));
        assertThat(doc.getFields("field.geohash"), arrayWithSize(1));
        assertThat(doc.getField("field.geohash").binaryValue().utf8ToString(), equalTo("s093jd0k72s1"));
        assertThat(doc.getFields("field.latlon"), arrayWithSize(1));
        assertThat(doc.getField("field.latlon").stringValue(), equalTo("s093jd0k72s1"));
    }

    public void testNullValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point")));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));

        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertThat(doc.rootDoc().getField("field"), nullValue());
        assertThat(doc.rootDoc().getFields(FieldNamesFieldMapper.NAME).length, equalTo(0));

        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("doc_values", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));

        doc = mapper.parse(source(b -> b.nullField("field")));
        assertThat(doc.rootDoc().getField("field"), nullValue());
        assertThat(doc.rootDoc().getFields(FieldNamesFieldMapper.NAME).length, equalTo(0));

        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("null_value", "1,2")));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));

        AbstractPointGeometryFieldMapper.ParsedPoint nullValue = ((GeoPointFieldMapper) fieldMapper).nullValue;
        assertThat(nullValue, equalTo(new GeoPoint(1, 2)));

        doc = mapper.parse(source(b -> b.nullField("field")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
        BytesRef defaultValue = doc.rootDoc().getBinaryValue("field");

        // Shouldn't matter if we specify the value explicitly or use null value
        doc = mapper.parse(source(b -> b.field("field", "1, 2")));
        assertThat(defaultValue, equalTo(doc.rootDoc().getBinaryValue("field")));

        doc = mapper.parse(source(b -> b.field("field", "3, 4")));
        assertThat(defaultValue, not(equalTo(doc.rootDoc().getBinaryValue("field"))));
    }

    /**
     * Test the fix for a bug that would read the value of field "ignore_z_value" for "ignore_malformed"
     * when setting the "null_value" field. See PR https://github.com/elastic/elasticsearch/pull/49645
     */
    public void testNullValueWithIgnoreMalformed() throws Exception {
        // Set ignore_z_value = false and ignore_malformed = true and test that a malformed point for null_value is normalized.
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "geo_point")
                    .field("ignore_z_value", false)
                    .field("ignore_malformed", true)
                    .field("null_value", "91,181")
            )
        );
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));

        AbstractPointGeometryFieldMapper.ParsedPoint nullValue = ((GeoPointFieldMapper) fieldMapper).nullValue;
        // geo_point [91, 181] should have been normalized to [89, 1]
        assertThat(nullValue, equalTo(new GeoPoint(89, 1)));
    }

    public void testInvalidGeohashIgnored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_malformed", "true")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234.333")));
        assertThat(doc.rootDoc().getField("field"), nullValue());
    }

    public void testInvalidGeohashNotIgnored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("field", "1234.333")))
        );
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [geo_point]"));
        assertThat(e.getRootCause().getMessage(), containsString("unsupported symbol [.] in geohash [1234.333]"));
    }

    public void testInvalidGeopointValuesIgnored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_malformed", "true")));

        assertThat(mapper.parse(source(b -> b.field("field", "1234.333"))).rootDoc().getField("field"), nullValue());
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", "-").field("lon", 1.3).endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", 1.3).field("lon", "-").endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(mapper.parse(source(b -> b.field("field", "-,1.3"))).rootDoc().getField("field"), nullValue());
        assertThat(mapper.parse(source(b -> b.field("field", "1.3,-"))).rootDoc().getField("field"), nullValue());
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", "NaN").field("lon", 1.2).endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", 1.2).field("lon", "NaN").endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(mapper.parse(source(b -> b.field("field", "1.3,NaN"))).rootDoc().getField("field"), nullValue());
        assertThat(mapper.parse(source(b -> b.field("field", "NaN,1.3"))).rootDoc().getField("field"), nullValue());
        assertThat(
            mapper.parse(source(b -> b.startObject("field").nullField("lat").field("lon", 1.2).endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", 1.2).nullField("lon").endObject())).rootDoc().getField("field"),
            nullValue()
        );
    }

    public void testGeoJsonSingleValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject("field").field("type", "Point").array("coordinates", new double[] { 1.1, 1.2 }).endObject())
        );
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testGeoJsonArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            source(
                b -> b.startArray("field")
                    .startObject()
                    .field("type", "Point")
                    .array("coordinates", new double[] { 1.1, 1.2 })
                    .endObject()
                    .startObject()
                    .field("type", "Point")
                    .array("coordinates", new double[] { 1.3, 1.4 })
                    .endObject()
                    .endArray()
            )
        );
        assertThat(doc.rootDoc().getField("field"), notNullValue());
        assertThat(doc.rootDoc().getFields("field"), arrayWithSize(4));
    }

    public void testGeoJsonIgnoreZValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_z_value", true)));
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject("field").field("type", "Point").array("coordinates", new double[] { 1.1, 1.2, 1.3 }).endObject())
        );
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testGeoJsonZValueException() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_z_value", false)));
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(
                source(b -> b.startObject("field").field("type", "Point").array("coordinates", new double[] { 1.1, 1.2, 1.3 }).endObject())
            )
        );
        assertThat(e.getCause().getMessage(), containsString("but [ignore_z_value] parameter is [false]"));
    }

    public void testGeoJsonIgnoreInvalidForm() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_malformed", "true")));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("field").array("coordinates", new double[] { 1.1, 1.2 }).endObject()));
        assertThat(doc.rootDoc().getField("field"), nullValue());
    }

    @Override
    protected GeoPointFieldMapper.Builder newBuilder() {
        return new GeoPointFieldMapper.Builder("geo");
    }

    public void testPossibleToDeriveSource_WhenDocValuesAndStoredDisabled() throws IOException {
        GeoPointFieldMapper mapper = getMapper(getMapperService(false, false), FieldMapper.CopyTo.empty());
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testPossibleToDeriveSource_WhenCopyToPresent() throws IOException {
        FieldMapper.CopyTo copyTo = new FieldMapper.CopyTo.Builder().add("copy_to_field").build();
        GeoPointFieldMapper mapper = getMapper(getMapperService(true, false), copyTo);
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testDerivedValueFetching_DocValues_GeoHash() throws IOException {
        try (Directory directory = newDirectory()) {
            MapperService mapperService = getMapperService(true, false); // doc values
            GeoPointFieldMapper mapper = getMapper(mapperService, FieldMapper.CopyTo.empty());
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, stringEncode(1.3, 1.2))));
                iw.addDocument(doc.rootDoc());
            }
            validateDerivedSource(mapper, directory);
        }
    }

    public void testDerivedValueFetching_StoredField_GeoHash() throws IOException {
        try (Directory directory = newDirectory()) {
            MapperService mapperService = getMapperService(false, true); // doc values
            GeoPointFieldMapper mapper = getMapper(mapperService, FieldMapper.CopyTo.empty());
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, stringEncode(1.3, 1.2))));
                iw.addDocument(doc.rootDoc());
            }
            validateDerivedSource(mapper, directory);
        }
    }

    public void testDerivedValueFetching_DocValues_Point() throws IOException {
        try (Directory directory = newDirectory()) {
            MapperService mapperService = getMapperService(true, false); // doc values
            GeoPointFieldMapper mapper = getMapper(mapperService, FieldMapper.CopyTo.empty());
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                ParsedDocument doc = mapperService.documentMapper()
                    .parse(source(b -> b.startObject(FIELD_NAME).field("lat", 1.2).field("lon", 1.3).endObject()));
                iw.addDocument(doc.rootDoc());
            }
            validateDerivedSource(mapper, directory);
        }
    }

    public void testDerivedValueFetching_StoredField_Point() throws IOException {
        try (Directory directory = newDirectory()) {
            MapperService mapperService = getMapperService(false, true); // doc values
            GeoPointFieldMapper mapper = getMapper(mapperService, FieldMapper.CopyTo.empty());
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                ParsedDocument doc = mapperService.documentMapper()
                    .parse(source(b -> b.startObject(FIELD_NAME).field("lat", 1.2).field("lon", 1.3).endObject()));
                iw.addDocument(doc.rootDoc());
            }
            validateDerivedSource(mapper, directory);
        }
    }

    public void testDerivedValueFetching_DocValues_String() throws IOException {
        try (Directory directory = newDirectory()) {
            MapperService mapperService = getMapperService(true, false); // doc values
            GeoPointFieldMapper mapper = getMapper(mapperService, FieldMapper.CopyTo.empty());
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, "1.2,1.3")));
                iw.addDocument(doc.rootDoc());
            }
            validateDerivedSource(mapper, directory);
        }
    }

    public void testDerivedValueFetching_StoredField_String() throws IOException {
        try (Directory directory = newDirectory()) {
            MapperService mapperService = getMapperService(false, true); // doc values
            GeoPointFieldMapper mapper = getMapper(mapperService, FieldMapper.CopyTo.empty());
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, "1.2,1.3")));
                iw.addDocument(doc.rootDoc());
            }
            validateDerivedSource(mapper, directory);
        }
    }

    public void testDerivedValueFetching_DocValues_Array() throws IOException {
        try (Directory directory = newDirectory()) {
            MapperService mapperService = getMapperService(true, false); // doc values
            GeoPointFieldMapper mapper = getMapper(mapperService, FieldMapper.CopyTo.empty());
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, new double[] { 1.3, 1.2 })));
                iw.addDocument(doc.rootDoc());
            }
            validateDerivedSource(mapper, directory);
        }
    }

    public void testDerivedValueFetching_StoredField_Array() throws IOException {
        try (Directory directory = newDirectory()) {
            MapperService mapperService = getMapperService(false, true); // doc values
            GeoPointFieldMapper mapper = getMapper(mapperService, FieldMapper.CopyTo.empty());
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, new double[] { 1.3, 1.2 })));
                iw.addDocument(doc.rootDoc());
            }
            validateDerivedSource(mapper, directory);
        }
    }

    public void testDerivedValueFetching_DocValues_WKT() throws IOException {
        try (Directory directory = newDirectory()) {
            MapperService mapperService = getMapperService(true, false); // doc values
            GeoPointFieldMapper mapper = getMapper(mapperService, FieldMapper.CopyTo.empty());
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, "POINT (1.3 1.2)")));
                iw.addDocument(doc.rootDoc());
            }
            validateDerivedSource(mapper, directory);
        }
    }

    public void testDerivedValueFetching_StoredField_WKT() throws IOException {
        try (Directory directory = newDirectory()) {
            MapperService mapperService = getMapperService(false, true); // doc values
            GeoPointFieldMapper mapper = getMapper(mapperService, FieldMapper.CopyTo.empty());
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, "POINT (1.3 1.2)")));
                iw.addDocument(doc.rootDoc());
            }
            validateDerivedSource(mapper, directory);
        }
    }

    public void testDerivedValueFetching_DocValues_Coordinates() throws IOException {
        try (Directory directory = newDirectory()) {
            MapperService mapperService = getMapperService(true, false); // doc values
            GeoPointFieldMapper mapper = getMapper(mapperService, FieldMapper.CopyTo.empty());
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                ParsedDocument doc = mapperService.documentMapper()
                    .parse(
                        source(
                            b -> b.startObject(FIELD_NAME)
                                .field("type", "Point")
                                .field("coordinates", new double[] { 1.3, 1.2 })
                                .endObject()
                        )
                    );
                iw.addDocument(doc.rootDoc());
            }
            validateDerivedSource(mapper, directory);
        }
    }

    public void testDerivedValueFetching_StoredField_Coordinates() throws IOException {
        try (Directory directory = newDirectory()) {
            MapperService mapperService = getMapperService(false, true); // doc values
            GeoPointFieldMapper mapper = getMapper(mapperService, FieldMapper.CopyTo.empty());
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                ParsedDocument doc = mapperService.documentMapper()
                    .parse(
                        source(
                            b -> b.startObject(FIELD_NAME)
                                .field("type", "Point")
                                .field("coordinates", new double[] { 1.3, 1.2 })
                                .endObject()
                        )
                    );
                iw.addDocument(doc.rootDoc());
            }
            validateDerivedSource(mapper, directory);
        }
    }

    public void testDerivedValueFetching_DocValues_Multi() throws IOException {
        try (Directory directory = newDirectory()) {
            MapperService mapperService = getMapperService(true, false); // doc values
            GeoPointFieldMapper mapper = getMapper(mapperService, FieldMapper.CopyTo.empty());
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                ParsedDocument doc = mapperService.documentMapper()
                    .parse(source(b -> b.field(FIELD_NAME, new String[] { stringEncode(1.3, 1.2), stringEncode(1.2, 1.1) })));
                iw.addDocument(doc.rootDoc());
            }
            validateDerivedSourceMultiField(mapper, directory, false);
        }
    }

    public void testDerivedValueFetching_StoredField_Multi() throws IOException {
        try (Directory directory = newDirectory()) {
            MapperService mapperService = getMapperService(false, true); // doc values
            GeoPointFieldMapper mapper = getMapper(mapperService, FieldMapper.CopyTo.empty());
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                ParsedDocument doc = mapperService.documentMapper()
                    .parse(source(b -> b.field(FIELD_NAME, new String[] { stringEncode(1.3, 1.2), stringEncode(1.2, 1.1) })));
                iw.addDocument(doc.rootDoc());
            }
            validateDerivedSourceMultiField(mapper, directory, true);
        }
    }

    private void validateDerivedSource(GeoPointFieldMapper mapper, Directory directory) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
            builder.endObject();
            Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            assertTrue(jsonObject.containsKey(FIELD_NAME));
            Map<String, Object> latLon = (Map<String, Object>) jsonObject.get(FIELD_NAME);
            assertEquals(1.2, (Double) latLon.get("lat"), 0.001);
            assertEquals(1.3, (Double) latLon.get("lon"), 0.001);
        }
    }

    private void validateDerivedSourceMultiField(GeoPointFieldMapper mapper, Directory directory, boolean isStored) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
            builder.endObject();
            Map<String, Object> jsonObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            assertTrue(jsonObject.containsKey(FIELD_NAME));
            List<Map<String, Object>> points = (List<Map<String, Object>>) jsonObject.get(FIELD_NAME);
            assertEquals(2, points.size());
            if (isStored) {
                Map<String, Object> latLon1 = points.get(0);
                assertEquals(1.2, (Double) latLon1.get("lat"), 0.001);
                assertEquals(1.3, (Double) latLon1.get("lon"), 0.001);
                Map<String, Object> latLon2 = points.get(1);
                assertEquals(1.1, (Double) latLon2.get("lat"), 0.001);
                assertEquals(1.2, (Double) latLon2.get("lon"), 0.001);
            } else {
                Map<String, Object> latLon1 = points.get(0);
                assertEquals(1.1, (Double) latLon1.get("lat"), 0.001);
                assertEquals(1.2, (Double) latLon1.get("lon"), 0.001);
                Map<String, Object> latLon2 = points.get(1);
                assertEquals(1.2, (Double) latLon2.get("lat"), 0.001);
                assertEquals(1.3, (Double) latLon2.get("lon"), 0.001);
            }
        }
    }

    private MapperService getMapperService(boolean hasDocValues, boolean isStored) throws IOException {
        return createMapperService(
            fieldMapping(b -> b.field("type", "geo_point").field("store", isStored).field("doc_values", hasDocValues))
        );
    }

    private GeoPointFieldMapper getMapper(MapperService mapperService, FieldMapper.CopyTo copyTo) throws IOException {
        GeoPointFieldMapper mapper = (GeoPointFieldMapper) mapperService.documentMapper().mappers().getMapper(FIELD_NAME);
        mapper.copyTo = copyTo;
        return mapper;
    }
}
