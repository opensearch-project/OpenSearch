/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.protobufs.GeoDistanceSort;
import org.opensearch.protobufs.GeoDistanceType;
import org.opensearch.protobufs.GeoLocation;
import org.opensearch.protobufs.GeoLocationArray;
import org.opensearch.protobufs.LatLonGeoLocation;
import org.opensearch.protobufs.NestedSortValue;
import org.opensearch.search.sort.GeoDistanceSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverterRegistryImpl;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

/**
 * Tests for {@link GeoDistanceSortProtoUtils}
 */
public class GeoDistanceSortProtoUtilsTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testFromProtoWithNullInput() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GeoDistanceSortProtoUtils.fromProto(null, registry)
        );
        assertEquals("GeoDistanceSort cannot be null", exception.getMessage());
    }

    public void testFromProtoWithNoLocations() {
        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder().build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry)
        );
        assertEquals("GeoDistanceSort must contain at least one geo location", exception.getMessage());
    }

    public void testFromProtoWithMultipleLocations() {
        GeoLocationArray geoLocationArray1 = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder().setLatlon(LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-74.0).build()).build()
            )
            .build();
        GeoLocationArray geoLocationArray2 = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder().setLatlon(LatLonGeoLocation.newBuilder().setLat(41.0).setLon(-73.0).build()).build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("field1", geoLocationArray1)
            .putLocation("field2", geoLocationArray2)
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry)
        );
        assertEquals("GeoDistanceSort supports only one field, but found 2 fields", exception.getMessage());
    }

    public void testFromProtoBasicConversion() {
        GeoLocationArray geoLocationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder().setLatlon(LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-74.0).build()).build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder().putLocation("location", geoLocationArray).build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);

        assertNotNull(result);
        // Basic validation - the builder should be created successfully
        assertEquals(SortOrder.ASC, result.order()); // Default order
    }

    public void testFromProtoWithAllOptions() {
        GeoLocationArray geoLocationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder().setLatlon(LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-74.0).build()).build()
            )
            .addGeoLocationArray(
                GeoLocation.newBuilder().setLatlon(LatLonGeoLocation.newBuilder().setLat(41.0).setLon(-73.0).build()).build()
            )
            .build();

        NestedSortValue nestedSort = NestedSortValue.newBuilder().setPath("nested_path").build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("location", geoLocationArray)
            .setOrder(org.opensearch.protobufs.SortOrder.SORT_ORDER_DESC)
            .setUnit(org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_KM)
            .setDistanceType(GeoDistanceType.GEO_DISTANCE_TYPE_PLANE)
            .setValidationMethod(org.opensearch.protobufs.GeoValidationMethod.GEO_VALIDATION_METHOD_IGNORE_MALFORMED)
            .setMode(org.opensearch.protobufs.SortMode.SORT_MODE_MIN)
            .setIgnoreUnmapped(true)
            .setNested(nestedSort)
            .build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);

        assertNotNull(result);
        assertEquals(SortOrder.DESC, result.order());
        // Additional validations would require access to private fields or specific getters
    }

    public void testFromProtoWithUnspecifiedEnums() {
        GeoLocationArray geoLocationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder().setLatlon(LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-74.0).build()).build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("location", geoLocationArray)
            .setUnit(org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_UNSPECIFIED)
            .setDistanceType(GeoDistanceType.GEO_DISTANCE_TYPE_UNSPECIFIED)
            .setValidationMethod(org.opensearch.protobufs.GeoValidationMethod.GEO_VALIDATION_METHOD_UNSPECIFIED)
            .setMode(org.opensearch.protobufs.SortMode.SORT_MODE_UNSPECIFIED)
            .build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);

        assertNotNull(result);
        // Should use defaults when unspecified enums are provided
    }

    public void testFromProtoWithDifferentUnits() {
        GeoLocationArray geoLocationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder().setLatlon(LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-74.0).build()).build()
            )
            .build();

        // Test with MILES
        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("location", geoLocationArray)
            .setUnit(org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_MI)
            .build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);
        assertNotNull(result);
    }

    public void testFromProtoWithDifferentDistanceTypes() {
        GeoLocationArray geoLocationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder().setLatlon(LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-74.0).build()).build()
            )
            .build();

        // Test with ARC distance type
        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("location", geoLocationArray)
            .setDistanceType(GeoDistanceType.GEO_DISTANCE_TYPE_ARC)
            .build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);
        assertNotNull(result);
    }

    public void testFromProtoWithDifferentValidationMethods() {
        GeoLocationArray geoLocationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder().setLatlon(LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-74.0).build()).build()
            )
            .build();

        // Test with STRICT validation
        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("location", geoLocationArray)
            .setValidationMethod(org.opensearch.protobufs.GeoValidationMethod.GEO_VALIDATION_METHOD_STRICT)
            .build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);
        assertNotNull(result);
    }

    public void testFromProtoWithDifferentSortModes() {
        GeoLocationArray geoLocationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder().setLatlon(LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-74.0).build()).build()
            )
            .build();

        // Test with MAX sort mode
        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("location", geoLocationArray)
            .setMode(org.opensearch.protobufs.SortMode.SORT_MODE_MAX)
            .build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);
        assertNotNull(result);
    }

    public void testFromProtoWithIgnoreUnmappedFalse() {
        GeoLocationArray geoLocationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder().setLatlon(LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-74.0).build()).build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("location", geoLocationArray)
            .setIgnoreUnmapped(false)
            .build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);
        assertNotNull(result);
    }

    public void testExtractGeoPointsWithEmptyArray() {
        GeoLocationArray emptyArray = GeoLocationArray.newBuilder().build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder().putLocation("location", emptyArray).build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry)
        );
        assertEquals("No valid geo locations found in field 'location'", exception.getMessage());
    }

    public void testExtractGeoPointsWithInvalidLocation() {
        // Create a malformed geo location (invalid text format)
        GeoLocationArray geoLocationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(GeoLocation.newBuilder().setText("invalid_geo_format").build())
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder().putLocation("location", geoLocationArray).build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry)
        );
        assertTrue(exception.getMessage().contains("Invalid geo location in field 'location'"));
    }
}
