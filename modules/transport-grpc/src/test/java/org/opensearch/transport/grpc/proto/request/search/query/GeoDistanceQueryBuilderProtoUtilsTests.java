/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.index.query.GeoDistanceQueryBuilder;
import org.opensearch.index.query.GeoValidationMethod;
import org.opensearch.protobufs.DoubleArray;
import org.opensearch.protobufs.GeoDistanceQuery;
import org.opensearch.protobufs.GeoDistanceType;
import org.opensearch.protobufs.GeoLocation;
import org.opensearch.protobufs.LatLonGeoLocation;
import org.opensearch.test.OpenSearchTestCase;

public class GeoDistanceQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithLatLon() {
        LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7).setLon(-74.0).build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("10km")
            .putLocation("location", geoLocation)
            .build();

        GeoDistanceQueryBuilder query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertEquals("Field name should match", "location", query.fieldName());
        assertEquals("Latitude should match", 40.7, query.point().getLat(), 0.001);
        assertEquals("Longitude should match", -74.0, query.point().getLon(), 0.001);
        // Distance is returned in meters, so 10km = 10000m
        assertEquals("Distance should match", 10000.0, query.distance(), 0.001);
    }

    public void testFromProtoWithGeohash() {
        org.opensearch.protobufs.GeoHashLocation geohashLocation = org.opensearch.protobufs.GeoHashLocation.newBuilder()
            .setGeohash("drm3btev3e86")
            .build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setGeohash(geohashLocation).build();

        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("5mi")
            .putLocation("location", geoLocation)
            .build();

        GeoDistanceQueryBuilder query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertEquals("Field name should match", "location", query.fieldName());
        // Distance is returned in meters, so 5mi = 8046.72m (approximately)
        assertEquals("Distance should match", 8046.72, query.distance(), 0.1);
    }

    public void testFromProtoWithEmptyFieldName() {
        expectThrows(IllegalArgumentException.class, () -> {
            LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7).setLon(-74.0).build();

            GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

            GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
                .setXName("")
                .setDistance("10km")
                .putLocation("", geoLocation)
                .build();

            GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);
        });
    }

    public void testFromProtoWithEmptyDistance() {
        expectThrows(IllegalArgumentException.class, () -> {
            LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7).setLon(-74.0).build();

            GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

            GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
                .setXName("location")
                .setDistance("")
                .putLocation("location", geoLocation)
                .build();

            GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);
        });
    }

    public void testFromProtoWithEmptyLocationMap() {
        expectThrows(IllegalArgumentException.class, () -> {
            GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder().setXName("location").setDistance("10km").build(); // No
                                                                                                                                // location
                                                                                                                                // added

            GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);
        });
    }

    public void testFromProtoWithNullFieldName() {
        expectThrows(IllegalArgumentException.class, () -> {
            LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7).setLon(-74.0).build();
            GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

            // This should trigger the null field name check
            GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
                .setXName("location")
                .setDistance("10km")
                .putLocation("", geoLocation) // Empty field name
                .build();

            GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);
        });
    }

    public void testFromProtoWithNullDistance() {
        expectThrows(IllegalArgumentException.class, () -> {
            LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7).setLon(-74.0).build();
            GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

            GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
                .setXName("location")
                // No distance set - should be null
                .putLocation("location", geoLocation)
                .build();

            GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);
        });
    }

    public void testFromProtoWithDoubleArrayGeoLocation() {
        // Test with DoubleArray format [lon, lat]
        DoubleArray doubleArray = DoubleArray.newBuilder()
            .addDoubleArray(-74.0) // lon
            .addDoubleArray(40.7)  // lat
            .build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setCoords(doubleArray).build();

        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("5mi")
            .putLocation("location", geoLocation)
            .build();

        GeoDistanceQueryBuilder query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertEquals("Field name should match", "location", query.fieldName());
        assertEquals("Latitude should match", 40.7, query.point().getLat(), 0.001);
        assertEquals("Longitude should match", -74.0, query.point().getLon(), 0.001);
    }

    public void testFromProtoWithTextGeoLocation() {
        // Test with text format "lat,lon"
        GeoLocation geoLocation = GeoLocation.newBuilder().setText("40.7,-74.0").build();

        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("2km")
            .putLocation("location", geoLocation)
            .build();

        GeoDistanceQueryBuilder query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertEquals("Field name should match", "location", query.fieldName());
        assertEquals("Latitude should match", 40.7, query.point().getLat(), 0.001);
        assertEquals("Longitude should match", -74.0, query.point().getLon(), 0.001);
    }

    public void testFromProtoWithDistanceUnits() {
        LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7).setLon(-74.0).build();
        GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

        // Test different distance units
        org.opensearch.protobufs.DistanceUnit[] units = {
            org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_CM,
            org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_FT,
            org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_IN,
            org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_KM,
            org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_M,
            org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_MI,
            org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_MM,
            org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_NMI,
            org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_YD,
            org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_UNSPECIFIED };

        DistanceUnit[] expectedUnits = {
            DistanceUnit.CENTIMETERS,
            DistanceUnit.FEET,
            DistanceUnit.INCH,
            DistanceUnit.KILOMETERS,
            DistanceUnit.METERS,
            DistanceUnit.MILES,
            DistanceUnit.MILLIMETERS,
            DistanceUnit.NAUTICALMILES,
            DistanceUnit.YARD,
            DistanceUnit.METERS // Default
        };

        for (int i = 0; i < units.length; i++) {
            GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
                .setXName("location")
                .setDistance("100")
                .setUnit(units[i])
                .putLocation("location", geoLocation)
                .build();

            GeoDistanceQueryBuilder query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

            assertNotNull("Query should not be null for unit " + units[i], query);
        }
    }

    public void testFromProtoWithDistanceTypes() {
        LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7).setLon(-74.0).build();
        GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

        // Test PLANE distance type
        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("10km")
            .setDistanceType(GeoDistanceType.GEO_DISTANCE_TYPE_PLANE)
            .putLocation("location", geoLocation)
            .build();

        GeoDistanceQueryBuilder query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertEquals("Distance type should be PLANE", org.opensearch.common.geo.GeoDistance.PLANE, query.geoDistance());

        // Test ARC distance type
        geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("10km")
            .setDistanceType(GeoDistanceType.GEO_DISTANCE_TYPE_ARC)
            .putLocation("location", geoLocation)
            .build();

        query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertEquals("Distance type should be ARC", org.opensearch.common.geo.GeoDistance.ARC, query.geoDistance());
    }

    public void testFromProtoWithValidationMethods() {
        LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7).setLon(-74.0).build();
        GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

        // Test COERCE validation method
        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("10km")
            .setValidationMethod(org.opensearch.protobufs.GeoValidationMethod.GEO_VALIDATION_METHOD_COERCE)
            .putLocation("location", geoLocation)
            .build();

        GeoDistanceQueryBuilder query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertEquals("Validation method should be COERCE", GeoValidationMethod.COERCE, query.getValidationMethod());

        // Test IGNORE_MALFORMED validation method
        geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("10km")
            .setValidationMethod(org.opensearch.protobufs.GeoValidationMethod.GEO_VALIDATION_METHOD_IGNORE_MALFORMED)
            .putLocation("location", geoLocation)
            .build();

        query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertEquals("Validation method should be IGNORE_MALFORMED", GeoValidationMethod.IGNORE_MALFORMED, query.getValidationMethod());

        // Test STRICT validation method
        geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("10km")
            .setValidationMethod(org.opensearch.protobufs.GeoValidationMethod.GEO_VALIDATION_METHOD_STRICT)
            .putLocation("location", geoLocation)
            .build();

        query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertEquals("Validation method should be STRICT", GeoValidationMethod.STRICT, query.getValidationMethod());
    }

    public void testFromProtoWithIgnoreUnmapped() {
        LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7).setLon(-74.0).build();
        GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

        // Test ignoreUnmapped = true
        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("10km")
            .setIgnoreUnmapped(true)
            .putLocation("location", geoLocation)
            .build();

        GeoDistanceQueryBuilder query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertTrue("Ignore unmapped should be true", query.ignoreUnmapped());

        // Test ignoreUnmapped = false
        geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("10km")
            .setIgnoreUnmapped(false)
            .putLocation("location", geoLocation)
            .build();

        query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertFalse("Ignore unmapped should be false", query.ignoreUnmapped());
    }

    public void testFromProtoWithBoost() {
        LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7).setLon(-74.0).build();
        GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("10km")
            .setBoost(2.5f)
            .putLocation("location", geoLocation)
            .build();

        GeoDistanceQueryBuilder query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertEquals("Boost should match", 2.5f, query.boost(), 0.001f);
    }

    public void testFromProtoWithQueryName() {
        LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7).setLon(-74.0).build();
        GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("my_geo_distance_query")
            .setDistance("10km")
            .putLocation("location", geoLocation)
            .build();

        GeoDistanceQueryBuilder query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertEquals("Query name should match", "my_geo_distance_query", query.queryName());
    }

    public void testFromProtoWithNumericDistance() {
        LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7).setLon(-74.0).build();
        GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

        // Test with numeric distance (this tests the Number vs String branch in the code)
        // Note: The proto uses string for distance, but internally it may be treated as a Number
        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("1000") // Numeric string
            .setUnit(org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_M)
            .putLocation("location", geoLocation)
            .build();

        GeoDistanceQueryBuilder query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertEquals("Distance should be 1000 meters", 1000.0, query.distance(), 0.001);
    }

    public void testFromProtoWithAllParameters() {
        LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7589).setLon(-73.9851).build();
        GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("comprehensive_geo_distance_query")
            .setDistance("5")
            .setUnit(org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_MI)
            .setDistanceType(GeoDistanceType.GEO_DISTANCE_TYPE_PLANE)
            .setValidationMethod(org.opensearch.protobufs.GeoValidationMethod.GEO_VALIDATION_METHOD_COERCE)
            .setIgnoreUnmapped(true)
            .setBoost(1.8f)
            .putLocation("geo_field", geoLocation)
            .build();

        GeoDistanceQueryBuilder query = GeoDistanceQueryBuilderProtoUtils.fromProto(geoDistanceQuery);

        assertNotNull("Query should not be null", query);
        assertEquals("Field name should match", "geo_field", query.fieldName());
        assertEquals("Query name should match", "comprehensive_geo_distance_query", query.queryName());
        assertEquals("Latitude should match", 40.7589, query.point().getLat(), 0.0001);
        assertEquals("Longitude should match", -73.9851, query.point().getLon(), 0.0001);
        assertEquals("Distance should be 5 miles in meters", 8046.72, query.distance(), 0.1);
        assertEquals("Distance type should be PLANE", org.opensearch.common.geo.GeoDistance.PLANE, query.geoDistance());
        assertEquals("Validation method should be COERCE", GeoValidationMethod.COERCE, query.getValidationMethod());
        assertTrue("Ignore unmapped should be true", query.ignoreUnmapped());
        assertEquals("Boost should match", 1.8f, query.boost(), 0.001f);
    }
}
