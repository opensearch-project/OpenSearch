/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.protobufs.FieldSort;
import org.opensearch.protobufs.FieldSortMap;
import org.opensearch.protobufs.GeoDistanceSort;
import org.opensearch.protobufs.GeoLocation;
import org.opensearch.protobufs.GeoLocationArray;
import org.opensearch.protobufs.ScoreSort;
import org.opensearch.protobufs.SortCombinations;
import org.opensearch.protobufs.SortOptions;
import org.opensearch.protobufs.SortOrder;
import org.opensearch.protobufs.SortOrderMap;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
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
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testFromProtoEmpty() {
        List<SortCombinations> sortProto = new ArrayList<>();

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull("Result should not be null", result);
        assertEquals("Result should be empty", 0, result.size());
    }

    public void testFromProtoWithUnsetCombination() {
        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull("Result should not be null", result);
        assertEquals("Result should be empty for unset combination", 0, result.size());
    }

    public void testFromProtoWithFieldType() {
        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setField("username").build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull("Result should not be null", result);
        assertEquals("Result should have one builder", 1, result.size());
        assertTrue("Should be FieldSortBuilder", result.get(0) instanceof FieldSortBuilder);
    }

    public void testFromProtoWithScoreField() {
        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setField("_score").build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull("Result should not be null", result);
        assertEquals("Result should have one builder", 1, result.size());
        assertTrue("Should be ScoreSortBuilder for _score", result.get(0) instanceof ScoreSortBuilder);
    }

    public void testFromProtoWithFieldWithDirection() {
        List<SortCombinations> sortProto = new ArrayList<>();

        SortOrderMap sortOrderMap = SortOrderMap.newBuilder().putSortOrderMap("message", SortOrder.SORT_ORDER_DESC).build();

        sortProto.add(SortCombinations.newBuilder().setFieldWithDirection(sortOrderMap).build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull("Result should not be null", result);
        assertEquals("Result should have one builder", 1, result.size());
        assertTrue("Should be SortBuilder", result.get(0) instanceof SortBuilder);
    }

    public void testFromProtoWithFieldWithOrder() {
        List<SortCombinations> sortProto = new ArrayList<>();

        FieldSort fieldSort = FieldSort.newBuilder().build();
        FieldSortMap fieldSortMap = FieldSortMap.newBuilder().putFieldSortMap("timestamp", fieldSort).build();

        sortProto.add(SortCombinations.newBuilder().setFieldWithOrder(fieldSortMap).build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull("Result should not be null", result);
        assertEquals("Result should have one builder", 1, result.size());
        assertTrue("Should be FieldSortBuilder", result.get(0) instanceof FieldSortBuilder);
    }

    public void testFromProtoWithScoreOptions() {
        List<SortCombinations> sortProto = new ArrayList<>();

        ScoreSort scoreSort = ScoreSort.newBuilder().build();
        SortOptions sortOptions = SortOptions.newBuilder().setXScore(scoreSort).build();

        sortProto.add(SortCombinations.newBuilder().setOptions(sortOptions).build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull("Result should not be null", result);
        assertEquals("Result should have one builder", 1, result.size());
        assertTrue("Should be ScoreSortBuilder", result.get(0) instanceof ScoreSortBuilder);
    }

    public void testFromProtoWithMultipleSortCombinations() {
        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().setField("_score").build());
        sortProto.add(SortCombinations.newBuilder().setField("timestamp").build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull("Result should not be null", result);
        assertEquals("Result should have two builders", 2, result.size());
        assertTrue("First should be ScoreSortBuilder", result.get(0) instanceof ScoreSortBuilder);
        assertTrue("Second should be FieldSortBuilder", result.get(1) instanceof FieldSortBuilder);
    }

    public void testFromProtoWithEmptySortOrderMap() {
        List<SortCombinations> sortProto = new ArrayList<>();
        SortOrderMap sortOrderMap = SortOrderMap.newBuilder().build();
        sortProto.add(SortCombinations.newBuilder().setFieldWithDirection(sortOrderMap).build());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SortBuilderProtoUtils.fromProto(sortProto, registry)
        );
        assertEquals("SortOrderMap cannot be empty or contain multiple entries", exception.getMessage());
    }

    public void testFromProtoWithUnknownSortOptions() {
        List<SortCombinations> sortProto = new ArrayList<>();
        SortOptions sortOptions = SortOptions.newBuilder().build();
        sortProto.add(SortCombinations.newBuilder().setOptions(sortOptions).build());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SortBuilderProtoUtils.fromProto(sortProto, registry)
        );
        assertEquals("Unknown sort options type", exception.getMessage());
    }

    public void testFieldOrScoreSortWithScoreField() {
        SortBuilder<?> sortBuilder = SortBuilderProtoUtils.fieldOrScoreSort("_score");

        assertTrue("Should return ScoreSortBuilder for _score field", sortBuilder instanceof ScoreSortBuilder);
    }

    public void testFieldOrScoreSortWithRegularField() {
        SortBuilder<?> sortBuilder = SortBuilderProtoUtils.fieldOrScoreSort("username");

        assertTrue("Should return FieldSortBuilder for regular field", sortBuilder instanceof FieldSortBuilder);
        FieldSortBuilder fieldSortBuilder = (FieldSortBuilder) sortBuilder;
        assertEquals("Field name should match", "username", fieldSortBuilder.getFieldName());
    }

    public void testFieldOrScoreSortWithEmptyField() {
        SortBuilder<?> sortBuilder = SortBuilderProtoUtils.fieldOrScoreSort("");

        assertTrue("Should return FieldSortBuilder for empty field", sortBuilder instanceof FieldSortBuilder);
        FieldSortBuilder fieldSortBuilder = (FieldSortBuilder) sortBuilder;
        assertEquals("Field name should be empty", "", fieldSortBuilder.getFieldName());
    }

    public void testFromProtoWithGeoDistanceOptions() {
        List<SortCombinations> sortProto = new ArrayList<>();

        GeoLocationArray locationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder()
                    .setLatlon(org.opensearch.protobufs.LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-70.0).build())
                    .build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder().putLocation("location", locationArray).build();
        SortOptions sortOptions = SortOptions.newBuilder().setXGeoDistance(geoDistanceSort).build();

        sortProto.add(SortCombinations.newBuilder().setOptions(sortOptions).build());

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto, registry);

        assertNotNull("Result should not be null", result);
        assertEquals("Result should have one builder", 1, result.size());
        assertNotNull("Should have a builder for GeoDistanceSort", result.get(0));
    }

    public void testFromProtoWithMultipleSortOrderMapEntries() {
        List<SortCombinations> sortProto = new ArrayList<>();

        SortOrderMap sortOrderMap = SortOrderMap.newBuilder()
            .putSortOrderMap("field1", SortOrder.SORT_ORDER_DESC)
            .putSortOrderMap("field2", SortOrder.SORT_ORDER_ASC)
            .build();

        sortProto.add(SortCombinations.newBuilder().setFieldWithDirection(sortOrderMap).build());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SortBuilderProtoUtils.fromProto(sortProto, registry)
        );
        assertEquals("SortOrderMap cannot be empty or contain multiple entries", exception.getMessage());
    }

    public void testFromProtoWithEmptyFieldSortMap() {
        List<SortCombinations> sortProto = new ArrayList<>();
        FieldSortMap fieldSortMap = FieldSortMap.newBuilder().build();
        sortProto.add(SortCombinations.newBuilder().setFieldWithOrder(fieldSortMap).build());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SortBuilderProtoUtils.fromProto(sortProto, registry)
        );
        assertEquals("FieldSortMap cannot be empty or contain multiple entries", exception.getMessage());
    }

    public void testFromProtoWithMultipleFieldSortMapEntries() {
        List<SortCombinations> sortProto = new ArrayList<>();

        FieldSort fieldSort = FieldSort.newBuilder().build();
        FieldSortMap fieldSortMap = FieldSortMap.newBuilder()
            .putFieldSortMap("field1", fieldSort)
            .putFieldSortMap("field2", fieldSort)
            .build();

        sortProto.add(SortCombinations.newBuilder().setFieldWithOrder(fieldSortMap).build());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SortBuilderProtoUtils.fromProto(sortProto, registry)
        );
        assertEquals("FieldSortMap cannot be empty or contain multiple entries", exception.getMessage());
    }
}
