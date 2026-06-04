/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.common.geo.GeoDistance;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.protobufs.GeoDistanceSort;
import org.opensearch.protobufs.GeoDistanceType;
import org.opensearch.protobufs.GeoLocation;
import org.opensearch.protobufs.GeoLocationArray;
import org.opensearch.protobufs.SortMode;
import org.opensearch.search.sort.GeoDistanceSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverterRegistryImpl;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

public class GeoDistanceSortProtoUtilsTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testFromProtoWithGeoDistanceSort() {
        // Create a GeoDistanceSort with basic parameters
        GeoLocationArray locationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder()
                    .setLatlon(org.opensearch.protobufs.LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-70.0).build())
                    .build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("location_field", locationArray)
            .setOrder(org.opensearch.protobufs.SortOrder.SORT_ORDER_DESC)
            .setUnit(org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_KM)
            .setDistanceType(GeoDistanceType.GEO_DISTANCE_TYPE_PLANE)
            .build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);

        assertNotNull("GeoDistanceSortBuilder should not be null", result);
        assertEquals("Field name should match", "location_field", result.fieldName());
        assertEquals("Order should match", SortOrder.DESC, result.order());
        assertEquals("Distance type should match", GeoDistance.PLANE, result.geoDistance());
        assertEquals("Unit should match", DistanceUnit.KILOMETERS, result.unit());
    }

    public void testFromProtoWithDefaultValues() {
        GeoLocationArray locationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder()
                    .setLatlon(org.opensearch.protobufs.LatLonGeoLocation.newBuilder().setLat(51.5).setLon(-0.1).build())
                    .build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder().putLocation("geo_field", locationArray).build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);

        assertNotNull("GeoDistanceSortBuilder should not be null", result);
        assertEquals("Field name should match", "geo_field", result.fieldName());
        assertEquals("Default order should be ASC", SortOrder.ASC, result.order());
        assertEquals("Default distance type should be ARC", GeoDistance.ARC, result.geoDistance());
        assertEquals("Default unit should be DEFAULT", DistanceUnit.DEFAULT, result.unit());
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
        GeoLocationArray locationArray1 = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder()
                    .setLatlon(org.opensearch.protobufs.LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-70.0).build())
                    .build()
            )
            .build();

        GeoLocationArray locationArray2 = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder()
                    .setLatlon(org.opensearch.protobufs.LatLonGeoLocation.newBuilder().setLat(51.5).setLon(-0.1).build())
                    .build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("field1", locationArray1)
            .putLocation("field2", locationArray2)
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry)
        );

        assertEquals("GeoDistanceSort supports only one field, but found 2 fields", exception.getMessage());
    }

    public void testFromProtoWithMultipleGeoPointsInOneField() {
        GeoLocationArray locationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder()
                    .setLatlon(org.opensearch.protobufs.LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-70.0).build())
                    .build()
            )
            .addGeoLocationArray(
                GeoLocation.newBuilder()
                    .setLatlon(org.opensearch.protobufs.LatLonGeoLocation.newBuilder().setLat(51.5).setLon(-0.1).build())
                    .build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder().putLocation("location_field", locationArray).build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);

        assertNotNull("GeoDistanceSortBuilder should not be null", result);
        assertEquals("Field name should match", "location_field", result.fieldName());
    }

    public void testFromProtoWithUnspecifiedEnums() {
        GeoLocationArray locationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder()
                    .setLatlon(org.opensearch.protobufs.LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-70.0).build())
                    .build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("location_field", locationArray)
            .setUnit(org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_UNSPECIFIED)
            .setDistanceType(GeoDistanceType.GEO_DISTANCE_TYPE_UNSPECIFIED)
            .setValidationMethod(org.opensearch.protobufs.GeoValidationMethod.GEO_VALIDATION_METHOD_UNSPECIFIED)
            .setMode(SortMode.SORT_MODE_UNSPECIFIED)
            .build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);

        assertNotNull("GeoDistanceSortBuilder should not be null", result);
        assertEquals("Default unit should be used for UNSPECIFIED", DistanceUnit.DEFAULT, result.unit());
        assertEquals("Default distance type should be used for UNSPECIFIED", GeoDistance.ARC, result.geoDistance());
    }

    public void testFromProtoWithIgnoreUnmapped() {
        GeoLocationArray locationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder()
                    .setLatlon(org.opensearch.protobufs.LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-70.0).build())
                    .build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("location_field", locationArray)
            .setIgnoreUnmapped(true)
            .build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);

        assertNotNull("GeoDistanceSortBuilder should not be null", result);
        assertTrue("IgnoreUnmapped should be true", result.ignoreUnmapped());
    }

    public void testFromProtoWithSortMode() {
        GeoLocationArray locationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder()
                    .setLatlon(org.opensearch.protobufs.LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-70.0).build())
                    .build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("location_field", locationArray)
            .setMode(SortMode.SORT_MODE_AVG)
            .build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);

        assertNotNull("GeoDistanceSortBuilder should not be null", result);
    }

    public void testFromProtoWithValidationMethod() {
        GeoLocationArray locationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder()
                    .setLatlon(org.opensearch.protobufs.LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-70.0).build())
                    .build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("location_field", locationArray)
            .setValidationMethod(org.opensearch.protobufs.GeoValidationMethod.GEO_VALIDATION_METHOD_STRICT)
            .build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);

        assertNotNull("GeoDistanceSortBuilder should not be null", result);
    }

    public void testFromProtoWithNestedSort() {
        GeoLocationArray locationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder()
                    .setLatlon(org.opensearch.protobufs.LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-70.0).build())
                    .build()
            )
            .build();

        org.opensearch.protobufs.NestedSortValue nestedSort = org.opensearch.protobufs.NestedSortValue.newBuilder()
            .setPath("nested_field")
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("location_field", locationArray)
            .setNested(nestedSort)
            .build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);

        assertNotNull("GeoDistanceSortBuilder should not be null", result);
        assertEquals("Field name should match", "location_field", result.fieldName());
    }

    public void testFromProtoWithAllParameters() {
        GeoLocationArray locationArray = GeoLocationArray.newBuilder()
            .addGeoLocationArray(
                GeoLocation.newBuilder()
                    .setLatlon(org.opensearch.protobufs.LatLonGeoLocation.newBuilder().setLat(40.0).setLon(-70.0).build())
                    .build()
            )
            .addGeoLocationArray(
                GeoLocation.newBuilder()
                    .setLatlon(org.opensearch.protobufs.LatLonGeoLocation.newBuilder().setLat(38.0).setLon(-68.0).build())
                    .build()
            )
            .build();

        GeoDistanceSort geoDistanceSort = GeoDistanceSort.newBuilder()
            .putLocation("location_field", locationArray)
            .setOrder(org.opensearch.protobufs.SortOrder.SORT_ORDER_ASC)
            .setUnit(org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_MI)
            .setDistanceType(GeoDistanceType.GEO_DISTANCE_TYPE_PLANE)
            .setValidationMethod(org.opensearch.protobufs.GeoValidationMethod.GEO_VALIDATION_METHOD_IGNORE_MALFORMED)
            .setMode(SortMode.SORT_MODE_MAX)
            .setIgnoreUnmapped(true)
            .build();

        GeoDistanceSortBuilder result = GeoDistanceSortProtoUtils.fromProto(geoDistanceSort, registry);

        assertNotNull("GeoDistanceSortBuilder should not be null", result);
        assertEquals("Field name should match", "location_field", result.fieldName());
        assertEquals("Order should match", SortOrder.ASC, result.order());
        assertEquals("Unit should match", DistanceUnit.MILES, result.unit());
        assertEquals("Distance type should match", GeoDistance.PLANE, result.geoDistance());
        assertTrue("IgnoreUnmapped should be true", result.ignoreUnmapped());
    }
}
