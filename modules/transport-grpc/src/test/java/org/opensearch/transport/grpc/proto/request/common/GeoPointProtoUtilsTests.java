/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.common;

import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoUtils;
import org.opensearch.protobufs.DoubleArray;
import org.opensearch.protobufs.GeoHashLocation;
import org.opensearch.protobufs.GeoLocation;
import org.opensearch.protobufs.LatLonGeoLocation;
import org.opensearch.test.OpenSearchTestCase;

public class GeoPointProtoUtilsTests extends OpenSearchTestCase {

    public void testParseGeoPointWithLatLon() {
        // Create a LatLon GeoLocation
        LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7589).setLon(-73.9851).build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

        // Test the simple parseGeoPoint method
        GeoPoint result = GeoPointProtoUtils.parseGeoPoint(geoLocation);

        assertNotNull("GeoPoint should not be null", result);
        assertEquals("Latitude should match", 40.7589, result.getLat(), 0.0001);
        assertEquals("Longitude should match", -73.9851, result.getLon(), 0.0001);
    }

    public void testParseGeoPointWithLatLonInPlace() {
        // Create a LatLon GeoLocation
        LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(37.7749).setLon(-122.4194).build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

        // Test the in-place parseGeoPoint method
        GeoPoint point = new GeoPoint();
        GeoPoint result = GeoPointProtoUtils.parseGeoPoint(geoLocation, point, false, GeoUtils.EffectivePoint.BOTTOM_LEFT);

        assertSame("Should return the same instance", point, result);
        assertEquals("Latitude should match", 37.7749, result.getLat(), 0.0001);
        assertEquals("Longitude should match", -122.4194, result.getLon(), 0.0001);
    }

    public void testParseGeoPointWithDoubleArrayTwoDimensions() {
        // Create a DoubleArray GeoLocation with [lon, lat]
        DoubleArray doubleArray = DoubleArray.newBuilder()
            .addDoubleArray(-122.4194)  // lon
            .addDoubleArray(37.7749)    // lat
            .build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setDoubleArray(doubleArray).build();

        GeoPoint result = GeoPointProtoUtils.parseGeoPoint(geoLocation);

        assertNotNull("GeoPoint should not be null", result);
        assertEquals("Latitude should match", 37.7749, result.getLat(), 0.0001);
        assertEquals("Longitude should match", -122.4194, result.getLon(), 0.0001);
    }

    public void testParseGeoPointWithDoubleArrayThreeDimensions() {
        // Create a DoubleArray GeoLocation with [lon, lat, z]
        DoubleArray doubleArray = DoubleArray.newBuilder()
            .addDoubleArray(-73.9851)   // lon
            .addDoubleArray(40.7589)    // lat
            .addDoubleArray(100.0)      // z (elevation)
            .build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setDoubleArray(doubleArray).build();

        // Test with ignoreZValue = true
        GeoPoint point = new GeoPoint();
        GeoPoint result = GeoPointProtoUtils.parseGeoPoint(geoLocation, point, true, GeoUtils.EffectivePoint.BOTTOM_LEFT);

        assertNotNull("GeoPoint should not be null", result);
        assertEquals("Latitude should match", 40.7589, result.getLat(), 0.0001);
        assertEquals("Longitude should match", -73.9851, result.getLon(), 0.0001);
    }

    public void testParseGeoPointWithDoubleArrayThreeDimensionsNoIgnore() {
        // Create a DoubleArray GeoLocation with [lon, lat, z] with z=0 and ignoreZValue = false
        // According to OpenSearch, even z=0.0 throws an exception when ignoreZValue is false
        DoubleArray doubleArray = DoubleArray.newBuilder()
            .addDoubleArray(-73.9851)   // lon
            .addDoubleArray(40.7589)    // lat
            .addDoubleArray(0.0)        // z (elevation)
            .build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setDoubleArray(doubleArray).build();

        // Test with ignoreZValue = false and z = 0.0 (should throw OpenSearchParseException)
        GeoPoint point = new GeoPoint();

        org.opensearch.OpenSearchParseException exception = expectThrows(
            org.opensearch.OpenSearchParseException.class,
            () -> GeoPointProtoUtils.parseGeoPoint(geoLocation, point, false, GeoUtils.EffectivePoint.BOTTOM_LEFT)
        );

        assertTrue(
            "Exception message should mention z value",
            exception.getMessage().contains("found Z value [0.0] but [ignore_z_value] parameter is [false]")
        );
    }

    public void testParseGeoPointWithText() {
        // Create a text-based GeoLocation
        GeoLocation geoLocation = GeoLocation.newBuilder().setText("40.7589,-73.9851").build();

        GeoPoint result = GeoPointProtoUtils.parseGeoPoint(geoLocation);

        assertNotNull("GeoPoint should not be null", result);
        assertEquals("Latitude should match", 40.7589, result.getLat(), 0.0001);
        assertEquals("Longitude should match", -73.9851, result.getLon(), 0.0001);
    }

    public void testParseGeoPointWithWKTPoint() {
        // Create a WKT POINT GeoLocation
        GeoLocation geoLocation = GeoLocation.newBuilder().setText("POINT(-73.9851 40.7589)").build();

        GeoPoint result = GeoPointProtoUtils.parseGeoPoint(geoLocation);

        assertNotNull("GeoPoint should not be null", result);
        assertEquals("Latitude should match", 40.7589, result.getLat(), 0.0001);
        assertEquals("Longitude should match", -73.9851, result.getLon(), 0.0001);
    }

    public void testParseGeoPointWithGeohash() {
        // Create a geohash GeoLocation
        GeoHashLocation geohashLocation = GeoHashLocation.newBuilder()
            .setGeohash("dr5regy")  // Approximate geohash for NYC area
            .build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setGeohash(geohashLocation).build();

        GeoPoint result = GeoPointProtoUtils.parseGeoPoint(geoLocation);

        assertNotNull("GeoPoint should not be null", result);
        // Geohash dr5regy corresponds approximately to the NYC area
        // We'll just check that the values are reasonable
        assertTrue("Latitude should be reasonable", result.getLat() > 40.0 && result.getLat() < 41.0);
        assertTrue("Longitude should be reasonable", result.getLon() > -75.0 && result.getLon() < -73.0);
    }

    public void testParseGeoPointWithEmptyGeoLocation() {
        // Create an empty GeoLocation (no location type set)
        GeoLocation geoLocation = GeoLocation.newBuilder().build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GeoPointProtoUtils.parseGeoPoint(geoLocation)
        );

        assertTrue("Exception message should mention 'geo_point expected'", exception.getMessage().contains("geo_point expected"));
    }

    public void testParseGeoPointWithDoubleArrayTooFewDimensions() {
        // Create a DoubleArray GeoLocation with only one dimension
        DoubleArray doubleArray = DoubleArray.newBuilder()
            .addDoubleArray(-122.4194)  // Only lon, missing lat
            .build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setDoubleArray(doubleArray).build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GeoPointProtoUtils.parseGeoPoint(geoLocation)
        );

        assertTrue(
            "Exception message should mention 'at least two dimensions'",
            exception.getMessage().contains("at least two dimensions")
        );
    }

    public void testParseGeoPointWithDoubleArrayTooManyDimensions() {
        // Create a DoubleArray GeoLocation with four dimensions
        DoubleArray doubleArray = DoubleArray.newBuilder()
            .addDoubleArray(-122.4194)  // lon
            .addDoubleArray(37.7749)    // lat
            .addDoubleArray(100.0)      // z
            .addDoubleArray(200.0)      // extra dimension
            .build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setDoubleArray(doubleArray).build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GeoPointProtoUtils.parseGeoPoint(geoLocation)
        );

        assertTrue(
            "Exception message should mention 'does not accept more than 3 values'",
            exception.getMessage().contains("does not accept more than 3 values")
        );
    }

    public void testParseGeoPointWithDoubleArrayThreeDimensionsWithNonZeroZ() {
        // Create a DoubleArray GeoLocation with [lon, lat, z] with non-zero z and ignoreZValue = false
        DoubleArray doubleArray = DoubleArray.newBuilder()
            .addDoubleArray(-73.9851)   // lon
            .addDoubleArray(40.7589)    // lat
            .addDoubleArray(100.0)      // non-zero z (elevation)
            .build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setDoubleArray(doubleArray).build();

        // Test with ignoreZValue = false and non-zero z (should throw OpenSearchParseException)
        GeoPoint point = new GeoPoint();

        org.opensearch.OpenSearchParseException exception = expectThrows(
            org.opensearch.OpenSearchParseException.class,
            () -> GeoPointProtoUtils.parseGeoPoint(geoLocation, point, false, GeoUtils.EffectivePoint.BOTTOM_LEFT)
        );

        assertTrue(
            "Exception message should mention z value",
            exception.getMessage().contains("but [ignore_z_value] parameter is [false]")
        );
    }
}
