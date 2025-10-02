/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.protobufs.BuiltinScriptLanguage;
import org.opensearch.protobufs.FieldSort;
import org.opensearch.protobufs.FieldSortMap;
import org.opensearch.protobufs.GeoDistanceSort;
import org.opensearch.protobufs.GeoLocation;
import org.opensearch.protobufs.GeoLocationArray;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.LatLonGeoLocation;
import org.opensearch.protobufs.ScoreSort;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.ScriptLanguage;
import org.opensearch.protobufs.ScriptSort;
import org.opensearch.protobufs.ScriptSortType;
import org.opensearch.protobufs.SortCombinations;
import org.opensearch.protobufs.SortOptions;
import org.opensearch.protobufs.SortOrder;
import org.opensearch.protobufs.SortOrderMap;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.GeoDistanceSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.ScriptSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverterRegistryImpl;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

import java.util.ArrayList;
import java.util.List;

public class SortBuilderProtoUtilsTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Set up the registry with all built-in converters
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testFromProtoWithEmptyList() {
        // Create an empty list of SortCombinations
        List<SortCombinations> sortProto = new ArrayList<>();

        // This should return an empty list
        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull(result);
        assertEquals(0, result.size());
    }

    public void testFromProtoWithUnsetSortCombinations() {
        // Create a list with a SortCombination that has SORTCOMBINATIONS_NOT_SET
        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().build());

        // This should return an empty list since SORTCOMBINATIONS_NOT_SET is skipped
        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull(result);
        assertEquals(0, result.size());
    }

    public void testFieldOrScoreSortWithScoreField() {
        // Test with "_score" field
        SortBuilder<?> sortBuilder = SortBuilderProtoUtils.fieldOrScoreSort("_score");

        assertTrue("Should return ScoreSortBuilder for _score field", sortBuilder instanceof ScoreSortBuilder);
    }

    public void testFieldOrScoreSortWithRegularField() {
        // Test with regular field name
        SortBuilder<?> sortBuilder = SortBuilderProtoUtils.fieldOrScoreSort("username");

        assertTrue("Should return FieldSortBuilder for regular field", sortBuilder instanceof FieldSortBuilder);
        FieldSortBuilder fieldSortBuilder = (FieldSortBuilder) sortBuilder;
        assertEquals("Field name should match", "username", fieldSortBuilder.getFieldName());
    }

    public void testFieldOrScoreSortWithEmptyField() {
        // Test with empty field name
        SortBuilder<?> sortBuilder = SortBuilderProtoUtils.fieldOrScoreSort("");

        assertTrue("Should return FieldSortBuilder for empty field", sortBuilder instanceof FieldSortBuilder);
        FieldSortBuilder fieldSortBuilder = (FieldSortBuilder) sortBuilder;
        assertEquals("Field name should be empty", "", fieldSortBuilder.getFieldName());
    }

    public void testFieldOrScoreSortWithNullField() {
        // Test with null field name - should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SortBuilderProtoUtils.fieldOrScoreSort(null)
        );

        // Verify the exception is thrown with the correct message
        assertEquals("fieldName must not be null", exception.getMessage());
    }

    public void testFromProtoWithFieldSortCombination() {
        // Test FIELD case
        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setField("username").build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.get(0) instanceof FieldSortBuilder);
        assertEquals("username", ((FieldSortBuilder) result.get(0)).getFieldName());
    }

    public void testFromProtoWithFieldWithDirectionSortCombination() {
        // Test FIELD_WITH_DIRECTION case
        SortOrderMap sortOrderMap = SortOrderMap.newBuilder().putSortOrderMap("username", SortOrder.SORT_ORDER_DESC).build();

        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setFieldWithDirection(sortOrderMap).build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.get(0) instanceof FieldSortBuilder);
        assertEquals("username", ((FieldSortBuilder) result.get(0)).getFieldName());
        assertEquals(org.opensearch.search.sort.SortOrder.DESC, result.get(0).order());
    }

    public void testFromProtoWithFieldWithOrderSortCombination() {
        // Test FIELD_WITH_ORDER case
        FieldSort fieldSort = FieldSort.newBuilder().setOrder(SortOrder.SORT_ORDER_ASC).build();

        FieldSortMap fieldSortMap = FieldSortMap.newBuilder().putFieldSortMap("username", fieldSort).build();

        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setFieldWithOrder(fieldSortMap).build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.get(0) instanceof FieldSortBuilder);
        assertEquals("username", ((FieldSortBuilder) result.get(0)).getFieldName());
    }

    public void testFromProtoWithScoreSortOptions() {
        // Test OPTIONS case with score sort
        ScoreSort scoreSort = ScoreSort.newBuilder().setOrder(SortOrder.SORT_ORDER_DESC).build();

        SortOptions sortOptions = SortOptions.newBuilder().setXScore(scoreSort).build();

        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setOptions(sortOptions).build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.get(0) instanceof ScoreSortBuilder);
        assertEquals(org.opensearch.search.sort.SortOrder.DESC, result.get(0).order());
    }

    public void testFromProtoWithGeoDistanceSortOptions() {
        // Test OPTIONS case with geo distance sort
        GeoLocationArray geoLocationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder().setLatlon(LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-74.0).build()).build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder().putLocation("location", geoLocationArray).build();

        SortOptions sortOptions = SortOptions.newBuilder().setXGeoDistance(geoDistanceSort).build();

        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setOptions(sortOptions).build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.get(0) instanceof GeoDistanceSortBuilder);
    }

    public void testFromProtoWithScriptSortOptions() {
        // Test OPTIONS case with script sort
        ScriptSort scriptSort = ScriptSort.newBuilder()
            .setScript(
                Script.newBuilder()
                    .setInline(
                        InlineScript.newBuilder()
                            .setSource("Math.random()")
                            .setLang(ScriptLanguage.newBuilder().setBuiltin(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS))
                            .build()
                    )
                    .build()
            )
            .setType(ScriptSortType.SCRIPT_SORT_TYPE_NUMBER)
            .build();

        SortOptions sortOptions = SortOptions.newBuilder().setXScript(scriptSort).build();

        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setOptions(sortOptions).build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.get(0) instanceof ScriptSortBuilder);
    }

    public void testFromProtoWithUnknownSortOptions() {
        // Test OPTIONS case with no valid sort type set
        SortOptions sortOptions = SortOptions.newBuilder().build();

        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setOptions(sortOptions).build());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SortBuilderProtoUtils.fromProto(sortProto, registry)
        );
        assertEquals("Unknown sort options type", exception.getMessage());
    }

    public void testFromProtoWithEmptySortOrderMap() {
        // Test FIELD_WITH_DIRECTION case with empty SortOrderMap
        SortOrderMap sortOrderMap = SortOrderMap.newBuilder().build();

        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setFieldWithDirection(sortOrderMap).build());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SortBuilderProtoUtils.fromProto(sortProto, registry)
        );
        assertEquals("SortOrderMap cannot be empty or contain multiple entries", exception.getMessage());
    }

    public void testFromProtoWithMultipleSortOrderMapEntries() {
        // Test FIELD_WITH_DIRECTION case with multiple entries
        SortOrderMap sortOrderMap = SortOrderMap.newBuilder()
            .putSortOrderMap("field1", SortOrder.SORT_ORDER_ASC)
            .putSortOrderMap("field2", SortOrder.SORT_ORDER_DESC)
            .build();

        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setFieldWithDirection(sortOrderMap).build());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SortBuilderProtoUtils.fromProto(sortProto, registry)
        );
        assertEquals("SortOrderMap cannot be empty or contain multiple entries", exception.getMessage());
    }

    public void testFromProtoWithEmptyFieldSortMap() {
        // Test FIELD_WITH_ORDER case with empty FieldSortMap
        FieldSortMap fieldSortMap = FieldSortMap.newBuilder().build();

        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setFieldWithOrder(fieldSortMap).build());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SortBuilderProtoUtils.fromProto(sortProto, registry)
        );
        assertEquals("FieldSortMap cannot be empty or contain multiple entries", exception.getMessage());
    }

    public void testFromProtoWithMultipleFieldSortMapEntries() {
        // Test FIELD_WITH_ORDER case with multiple entries
        FieldSort fieldSort1 = FieldSort.newBuilder().setOrder(SortOrder.SORT_ORDER_ASC).build();
        FieldSort fieldSort2 = FieldSort.newBuilder().setOrder(SortOrder.SORT_ORDER_DESC).build();

        FieldSortMap fieldSortMap = FieldSortMap.newBuilder()
            .putFieldSortMap("field1", fieldSort1)
            .putFieldSortMap("field2", fieldSort2)
            .build();

        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setFieldWithOrder(fieldSortMap).build());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SortBuilderProtoUtils.fromProto(sortProto, registry)
        );
        assertEquals("FieldSortMap cannot be empty or contain multiple entries", exception.getMessage());
    }

    public void testParseSortOrderAsc() {
        // Test parsing ASC sort order - accessed through fromProto
        SortOrderMap sortOrderMap = SortOrderMap.newBuilder().putSortOrderMap("field", SortOrder.SORT_ORDER_ASC).build();

        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setFieldWithDirection(sortOrderMap).build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(org.opensearch.search.sort.SortOrder.ASC, result.get(0).order());
    }

    public void testParseSortOrderDesc() {
        // Test parsing DESC sort order - accessed through fromProto
        SortOrderMap sortOrderMap = SortOrderMap.newBuilder().putSortOrderMap("field", SortOrder.SORT_ORDER_DESC).build();

        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setFieldWithDirection(sortOrderMap).build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(org.opensearch.search.sort.SortOrder.DESC, result.get(0).order());
    }
}
