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

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.opensearch.Version;
import org.opensearch.common.Explicit;
import org.opensearch.common.geo.builders.ShapeBuilder;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.TestGeoShapeFieldMapperPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class GeoShapeFieldMapperTests extends FieldMapperTestCase2<GeoShapeFieldMapper.Builder> {

    @Override
    protected Set<String> unsupportedProperties() {
        return Set.of("analyzer", "similarity", "store");
    }

    @Override
    protected boolean supportsOrIgnoresBoost() {
        return false;
    }

    @Override
    protected GeoShapeFieldMapper.Builder newBuilder() {
        return new GeoShapeFieldMapper.Builder("geoshape");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(b -> b.field("orientation", "right"), m -> {
            GeoShapeFieldMapper gsfm = (GeoShapeFieldMapper) m;
            assertEquals(ShapeBuilder.Orientation.RIGHT, gsfm.orientation());
        });
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true), m -> {
            GeoShapeFieldMapper gpfm = (GeoShapeFieldMapper) m;
            assertTrue(gpfm.ignoreMalformed.value());
        });
        checker.registerUpdateCheck(b -> b.field("ignore_z_value", false), m -> {
            GeoShapeFieldMapper gpfm = (GeoShapeFieldMapper) m;
            assertFalse(gpfm.ignoreZValue.value());
        });
        checker.registerUpdateCheck(b -> b.field("coerce", true), m -> {
            GeoShapeFieldMapper gpfm = (GeoShapeFieldMapper) m;
            assertTrue(gpfm.coerce.value());
        });
    }

    @Before
    public void addModifiers() {
        addModifier("orientation", true, (a, b) -> {
            a.orientation(ShapeBuilder.Orientation.LEFT);
            b.orientation(ShapeBuilder.Orientation.RIGHT);
        });
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new TestGeoShapeFieldMapperPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "geo_shape");
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value("POINT (14.0 15.0)");
    }

    public void testDefaultConfiguration() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(GeoShapeFieldMapper.Defaults.ORIENTATION.value()));
        assertThat(geoShapeFieldMapper.fieldType().hasDocValues(), equalTo(true));
    }

    public void testExistsQueryForIndexCreatedBeforeDocValuesSupport() throws IOException {
        assertGeoShapeExistsQuery(Version.V_2_8_0);
    }

    public void testExistsQueryForIndexCreatedWithDocValuesSupport() throws IOException {
        assertGeoShapeExistsQuery(Version.V_2_9_0);
        assertGeoShapeExistsQuery(Version.CURRENT);
    }

    public void testOldGeoShapeMappingReloadAndMerge() throws IOException {
        assertGeoShapeMappingReloadAndMerge(Version.V_2_8_0, null, true);
        assertGeoShapeMappingReloadAndMerge(Version.V_2_8_0, false, false);
        assertGeoShapeMappingReloadAndMerge(Version.V_2_8_0, true, true);
    }

    public void testOldGeoShapeMappingPreservesDocValuesDeclaration() throws IOException {
        for (Boolean configuredDocValues : new Boolean[] { null, false, true }) {
            XContentBuilder input = fieldMapping(b -> {
                minimalMapping(b);
                if (configuredDocValues != null) {
                    b.field("doc_values", configuredDocValues);
                }
            });
            MapperService old = createMapperService(Version.V_2_8_0, input);
            // The declaration must stay compatible with nodes that do not have this fix.
            MapperService existing = createMapperService(input);
            assertEquals(existing.documentMapper().mappingSource(), old.documentMapper().mappingSource());
            try (XContentBuilder serialized = JsonXContent.contentBuilder().startObject()) {
                old.documentMapper().mapping().toXContent(serialized, ToXContent.EMPTY_PARAMS);
                serialized.endObject();
                merge(existing, MapperService.MergeReason.MAPPING_RECOVERY, serialized);
                MapperService copied = createMapperService(serialized);
                assertEquals(configuredDocValues != Boolean.FALSE, copied.fieldType("field").hasDocValues());
            }
        }
    }

    public void testCurrentGeoShapeMappingReloadAndMerge() throws IOException {
        assertGeoShapeMappingReloadAndMerge(Version.V_2_9_0, false, false);
        assertGeoShapeMappingReloadAndMerge(Version.V_2_9_0, true, true);
        assertGeoShapeMappingReloadAndMerge(Version.CURRENT, null, true);
        assertGeoShapeMappingReloadAndMerge(Version.CURRENT, false, false);
        assertGeoShapeMappingReloadAndMerge(Version.CURRENT, true, true);
    }

    public void testUnindexedGeoShapeSkipsBkdFallback() throws IOException {
        MapperService mapperService = createMapperService(Version.V_2_8_0, fieldMapping(this::minimalMapping));
        for (boolean declaredDocValues : new boolean[] { false, true }) {
            MappedFieldType fieldType = new GeoShapeFieldMapper.GeoShapeFieldType(
                "field",
                false,
                false,
                declaredDocValues,
                Collections.emptyMap()
            );
            assertEquals(
                new TermQuery(new Term(FieldNamesFieldMapper.NAME, "field")),
                fieldType.existsQuery(createQueryShardContext(mapperService))
            );
        }
    }

    private void assertGeoShapeMappingReloadAndMerge(Version version, Boolean configuredDocValues, boolean expectedDocValues)
        throws IOException {
        MapperService mapperService = createMapperService(version, fieldMapping(b -> {
            minimalMapping(b);
            if (configuredDocValues != null) {
                b.field("doc_values", configuredDocValues);
            }
        }));
        assertEquals(expectedDocValues, mapperService.fieldType("field").hasDocValues());
        Query originalExists = mapperService.fieldType("field").existsQuery(createQueryShardContext(mapperService));
        if (version.onOrAfter(Version.V_2_9_0)) {
            assertEquals(
                expectedDocValues ? new FieldExistsQuery("field") : new TermQuery(new Term(FieldNamesFieldMapper.NAME, "field")),
                originalExists
            );
        }
        try (XContentBuilder serialized = JsonXContent.contentBuilder().startObject()) {
            mapperService.documentMapper().mapping().toXContent(serialized, ToXContent.EMPTY_PARAMS);
            serialized.endObject();
            MapperService reloaded = createMapperService(version, serialized);
            assertEquals(mapperService.documentMapper().mappingSource(), reloaded.documentMapper().mappingSource());
            merge(reloaded, fieldMapping(b -> {
                minimalMapping(b);
                b.field("orientation", "left");
                if (configuredDocValues != null) {
                    b.field("doc_values", configuredDocValues);
                }
            }));
            assertEquals(expectedDocValues, reloaded.fieldType("field").hasDocValues());
            assertEquals(expectedDocValues, reloaded.fieldType("field").isAggregatable());
            assertEquals(originalExists, reloaded.fieldType("field").existsQuery(createQueryShardContext(reloaded)));
            ParseContext.Document doc = reloaded.documentMapper().parse(source(this::writeField)).rootDoc();
            if (expectedDocValues && version.onOrAfter(Version.V_2_9_0)) {
                assertDocValuesField(doc, "field");
                assertNoFieldNamesField(doc);
            } else {
                assertNoDocValuesField(doc, "field");
                if (expectedDocValues) {
                    assertNoFieldNamesField(doc);
                } else {
                    assertEquals("field", doc.getField(FieldNamesFieldMapper.NAME).stringValue());
                }
            }
        }
    }

    private void assertGeoShapeExistsQuery(Version indexVersion) throws IOException {
        MapperService mapperService = createMapperService(indexVersion, fieldMapping(this::minimalMapping));
        boolean hasDocValues = indexVersion.onOrAfter(Version.V_2_9_0);
        // Keep the declaration unchanged for mapping compatibility; only the write path is version gated.
        assertTrue(mapperService.fieldType("field").hasDocValues());
        withLuceneIndex(mapperService, writer -> {
            ParseContext.Document doc = mapperService.documentMapper().parse(source(this::writeField)).rootDoc();
            if (hasDocValues) {
                assertDocValuesField(doc, "field");
                assertNoFieldNamesField(doc);
            } else {
                assertNoDocValuesField(doc, "field");
                assertNoFieldNamesField(doc);
            }
            writer.addDocument(doc);
            writer.addDocument(mapperService.documentMapper().parse(source(b -> {})).rootDoc());
            writer.addDocument(mapperService.documentMapper().parse(source(b -> b.nullField("field"))).rootDoc());
        }, reader -> {
            Query exists = ExistsQueryBuilder.newFilter(createQueryShardContext(mapperService), "field", true);
            assertEquals(1, newSearcher(reader).count(exists));
            assertEquals(2, newSearcher(reader).count(Queries.not(exists)));
        });
    }

    public void testGeoShapeUpgradeStageBoundaries() throws IOException {
        assertGeoShapeUpgradeStageBoundaries(false);
    }

    public void testGeoShapeUpgradeStageBoundariesAfterMerge() throws IOException {
        assertGeoShapeUpgradeStageBoundaries(true);
    }

    private void assertGeoShapeUpgradeStageBoundaries(boolean forceMerge) throws IOException {
        MapperService mapperService = createMapperService(Version.V_2_8_0, fieldMapping(this::minimalMapping));
        // Explicitly disabling doc values models the 2.8 write path.
        // These are current-code representations, not segments generated by a running 2.8 cluster.
        MapperService noDocValuesMapper = createMapperService(
            Version.V_2_8_0,
            fieldMapping(b -> b.field("type", "geo_shape").field("doc_values", false))
        );
        ParseContext.Document beforeUpgrade = noDocValuesMapper.documentMapper().parse(source(this::writeField)).rootDoc();
        ParseContext.Document affected = parseWithoutFieldNames(mapperService, "POINT (14 15)");
        ParseContext.Document afterCorrection = mapperService.documentMapper().parse(source(this::writeField)).rootDoc();
        assertFalse(noDocValuesMapper.fieldType("field").hasDocValues());
        assertTrue(mapperService.fieldType("field").hasDocValues());
        for (ParseContext.Document doc : List.of(beforeUpgrade, affected, afterCorrection)) {
            assertNoDocValuesField(doc, "field");
            assertEquals(7, doc.getField("field").fieldType().pointDimensionCount());
        }
        assertEquals("field", beforeUpgrade.getField(FieldNamesFieldMapper.NAME).stringValue());
        assertNoFieldNamesField(affected);
        assertNoFieldNamesField(afterCorrection);
        beforeUpgrade.add(new StoredField("stage", "before_upgrade"));
        affected.add(new StoredField("stage", "affected"));
        afterCorrection.add(new StoredField("stage", "after_correction"));
        ParseContext.Document missing = mapperService.documentMapper().parse(source(b -> {})).rootDoc();
        missing.add(new StoredField("stage", "missing"));
        ParseContext.Document nullValue = mapperService.documentMapper().parse(source(b -> b.nullField("field"))).rootDoc();
        nullValue.add(new StoredField("stage", "null"));

        withLuceneIndex(mapperService, writer -> {
            for (ParseContext.Document doc : List.of(beforeUpgrade, affected, afterCorrection, missing, nullValue)) {
                writer.addDocument(doc);
                writer.flush();
            }
            if (forceMerge) {
                writer.forceMerge(1);
            }
        }, reader -> {
            IndexSearcher searcher = newSearcher(reader);
            Query exists = ExistsQueryBuilder.newFilter(createQueryShardContext(mapperService), "field", true);
            assertGeoShapeMatches(searcher, exists, "before_upgrade", "affected", "after_correction");
            assertGeoShapeMatches(searcher, Queries.not(exists), "missing", "null");
        });
    }

    public void testGeoShapeUpgradeBkdQueryWithoutExistenceMarkers() throws IOException {
        MapperService mapperService = createMapperService(Version.V_2_8_0, fieldMapping(this::minimalMapping));
        List<String> geometries = List.of(
            "POINT (14 15)",
            "POINT (-180 -90)",
            "POINT (180 90)",
            "LINESTRING (-10 -10, 10 10)",
            "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
            "POLYGON ((170 -10, -170 -10, -170 10, 170 10, 170 -10))",
            "MULTIPOINT (-10 -10, 10 10)",
            "GEOMETRYCOLLECTION (POINT (20 20), LINESTRING (0 0, 1 1))"
        );
        withLuceneIndex(mapperService, writer -> {
            for (String geometry : geometries) {
                ParseContext.Document doc = parseWithoutFieldNames(mapperService, geometry);
                assertNoDocValuesField(doc, "field");
                assertNoFieldNamesField(doc);
                doc.add(new StoredField("stage", geometry));
                writer.addDocument(doc);
            }
            ParseContext.Document missing = mapperService.documentMapper().parse(source(b -> {})).rootDoc();
            missing.add(new StoredField("stage", "missing"));
            writer.addDocument(missing);
        }, reader -> {
            IndexSearcher searcher = newSearcher(reader);
            Query exists = ExistsQueryBuilder.newFilter(createQueryShardContext(mapperService), "field", true);
            assertGeoShapeMatches(searcher, exists, geometries.toArray(String[]::new));
            assertGeoShapeMatches(searcher, Queries.not(exists), "missing");
        });
    }

    public void testGeoShapeUpgradeEmptyGeometryHasNoBkdFallback() throws IOException {
        MapperService affectedMapper = createMapperService(Version.V_2_8_0, fieldMapping(this::minimalMapping));
        MapperService noDocValuesMapper = createMapperService(
            Version.V_2_8_0,
            fieldMapping(b -> b.field("type", "geo_shape").field("doc_values", false))
        );
        ParseContext.Document beforeUpgrade = noDocValuesMapper.documentMapper()
            .parse(source(b -> b.field("field", "GEOMETRYCOLLECTION EMPTY")))
            .rootDoc();
        ParseContext.Document affected = parseWithoutFieldNames(affectedMapper, "GEOMETRYCOLLECTION EMPTY");
        ParseContext.Document afterCorrection = affectedMapper.documentMapper()
            .parse(source(b -> b.field("field", "GEOMETRYCOLLECTION EMPTY")))
            .rootDoc();
        assertNull(beforeUpgrade.getField("field"));
        assertNull(affected.getField("field"));
        assertEquals("field", beforeUpgrade.getField(FieldNamesFieldMapper.NAME).stringValue());
        assertNoFieldNamesField(affected);
        beforeUpgrade.add(new StoredField("stage", "before_upgrade_empty"));
        affected.add(new StoredField("stage", "affected_empty"));
        afterCorrection.add(new StoredField("stage", "after_correction_empty"));
        ParseContext.Document missing = affectedMapper.documentMapper().parse(source(b -> {})).rootDoc();
        missing.add(new StoredField("stage", "missing"));
        withLuceneIndex(affectedMapper, writer -> {
            writer.addDocument(beforeUpgrade);
            writer.addDocument(affected);
            writer.addDocument(afterCorrection);
            writer.addDocument(missing);
        }, reader -> {
            IndexSearcher searcher = newSearcher(reader);
            Query exists = ExistsQueryBuilder.newFilter(createQueryShardContext(affectedMapper), "field", true);
            assertGeoShapeMatches(searcher, exists, "before_upgrade_empty");
            // Historical empty geometries without an existence marker have no indexed shape to recover.
            assertGeoShapeMatches(searcher, Queries.not(exists), "affected_empty", "after_correction_empty", "missing");
        });
    }

    private ParseContext.Document parseWithoutFieldNames(MapperService mapperService, String geometry) throws IOException {
        ParseContext.Document doc = mapperService.documentMapper().parse(source(b -> b.field("field", geometry))).rootDoc();
        // Reproduce the representation written by affected versions, which omitted both doc values and _field_names.
        assertNoDocValuesField(doc, "field");
        doc.getFields().removeIf(field -> FieldNamesFieldMapper.NAME.equals(field.name()));
        return doc;
    }

    private static void assertGeoShapeMatches(IndexSearcher searcher, Query query, String... expectedStages) throws IOException {
        Set<String> actualStages = new HashSet<>();
        for (ScoreDoc hit : searcher.search(query, 20).scoreDocs) {
            actualStages.add(searcher.storedFields().document(hit.doc).get("stage"));
        }
        assertEquals(expectedStages.length, searcher.count(query));
        assertEquals(Set.of(expectedStages), actualStages);
    }

    /**
     * Test that orientation parameter correctly parses
     */
    public void testOrientationParsing() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("orientation", "left")));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        ShapeBuilder.Orientation orientation = ((GeoShapeFieldMapper) fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.LEFT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CW));

        // explicit right orientation test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("orientation", "right")));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        orientation = ((GeoShapeFieldMapper) fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.RIGHT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CCW));
    }

    /**
     * Test that coerce parameter correctly parses
     */
    public void testCoerceParsing() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("coerce", true)));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        boolean coerce = ((GeoShapeFieldMapper) fieldMapper).coerce().value();
        assertThat(coerce, equalTo(true));

        // explicit false coerce test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("coerce", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        coerce = ((GeoShapeFieldMapper) fieldMapper).coerce().value();
        assertThat(coerce, equalTo(false));
        assertFieldWarnings("tree");
    }

    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("ignore_z_value", true)));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        boolean ignoreZValue = ((GeoShapeFieldMapper) fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("ignore_z_value", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        ignoreZValue = ((GeoShapeFieldMapper) fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(false));
    }

    /**
     * Test that ignore_malformed parameter correctly parses
     */
    public void testIgnoreMalformedParsing() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("ignore_malformed", true)));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        Explicit<Boolean> ignoreMalformed = ((GeoShapeFieldMapper) fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed.value(), equalTo(true));

        // explicit false ignore_malformed test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("ignore_malformed", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        ignoreMalformed = ((GeoShapeFieldMapper) fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed.explicit(), equalTo(true));
        assertThat(ignoreMalformed.value(), equalTo(false));
    }

    private void assertFieldWarnings(String... fieldNames) {
        String[] warnings = new String[fieldNames.length];
        for (int i = 0; i < fieldNames.length; ++i) {
            warnings[i] = "Field parameter [" + fieldNames[i] + "] " + "is deprecated and will be removed in a future version.";
        }
    }

    public void testGeoShapeMapperMerge() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "geo_shape").field("orientation", "ccw")));
        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CCW));

        // change mapping; orientation
        merge(mapperService, fieldMapping(b -> b.field("type", "geo_shape").field("orientation", "cw")));
        fieldMapper = mapperService.documentMapper().mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
        geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CW));
    }

    public void testSerializeDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertThat(
            Strings.toString(
                MediaTypeRegistry.JSON,
                mapper.mappers().getMapper("field"),
                new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true"))
            ),
            containsString("\"orientation\":\"" + AbstractShapeGeometryFieldMapper.Defaults.ORIENTATION.value() + "\"")
        );
    }

    public void testGeoShapeArrayParsing() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument document = mapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject().field("type", "Point").startArray("coordinates").value(176.0).value(15.0).endArray().endObject();
                b.startObject().field("type", "Point").startArray("coordinates").value(76.0).value(-15.0).endArray().endObject();
            }
            b.endArray();
        }));
        assertThat(document.docs(), hasSize(1));
        assertThat(document.docs().get(0).getFields("field").length, equalTo(4));
    }

    @Override
    protected boolean supportsMeta() {
        return false;
    }

    public void testPluggableDataFormatGeoShapeThrows() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        GeoShapeFieldMapper geoMapper = (GeoShapeFieldMapper) mapper.mappers().getMapper("field");
        expectThrows(UnsupportedOperationException.class, () -> geoMapper.parseCreateFieldForPluggableFormat(null));
    }
}
