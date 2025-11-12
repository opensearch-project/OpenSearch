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
import org.opensearch.protobufs.GeoLocation;

/**
 * Utility class for parsing Protocol Buffer GeoLocation objects into OpenSearch GeoPoint objects.
 * This class provides shared functionality for converting protobuf geo location representations
 * into their corresponding OpenSearch GeoPoint implementations.
 *
 * @opensearch.internal
 */
public class GeoPointProtoUtils {

    private GeoPointProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Parses a Protocol Buffer GeoLocation into an OpenSearch GeoPoint.
     * Supports multiple geo location formats:
     * <ul>
     *   <li>Lat/Lon objects: {@code {lat: 37.7749, lon: -122.4194}}</li>
     *   <li>Geohash: {@code "9q8yyk0"}</li>
     *   <li>Double arrays: {@code [lon, lat]} or {@code [lon, lat, z]}</li>
     *   <li>Text formats: {@code "37.7749, -122.4194"} or {@code "POINT(-122.4194 37.7749)"}</li>
     * </ul>
     *
     * @param geoLocation The Protocol Buffer GeoLocation to parse
     * @return A GeoPoint object representing the parsed location
     * @throws IllegalArgumentException if the geo location format is invalid or unsupported
     */
    public static GeoPoint parseGeoPoint(GeoLocation geoLocation) {
        GeoPoint point = new GeoPoint();
        return parseGeoPoint(geoLocation, point, false, GeoUtils.EffectivePoint.BOTTOM_LEFT);
    }

    /**
     * Parses a GeoLocation protobuf into a GeoPoint, following the same pattern as GeoUtils.parseGeoPoint().
     * This method modifies the provided GeoPoint in-place and returns it.
     *
     * @param geoLocation the protobuf GeoLocation to parse
     * @param point the GeoPoint to modify in-place
     * @param ignoreZValue whether to ignore Z values (elevation)
     * @param effectivePoint the effective point interpretation for coordinate ordering
     * @return the same GeoPoint instance that was passed in (modified)
     * @throws IllegalArgumentException if the GeoLocation format is invalid
     */
    public static GeoPoint parseGeoPoint(
        GeoLocation geoLocation,
        GeoPoint point,
        boolean ignoreZValue,
        GeoUtils.EffectivePoint effectivePoint
    ) {

        if (geoLocation.hasLatlon()) {
            org.opensearch.protobufs.LatLonGeoLocation latLon = geoLocation.getLatlon();
            point.resetLat(latLon.getLat());
            point.resetLon(latLon.getLon());

        } else if (geoLocation.hasDoubleArray()) {
            org.opensearch.protobufs.DoubleArray doubleArray = geoLocation.getDoubleArray();
            int count = doubleArray.getDoubleArrayCount();
            if (count < 2) {
                throw new IllegalArgumentException("[geo_point] field type should have at least two dimensions");
            } else if (count > 3) {
                throw new IllegalArgumentException("[geo_point] field type does not accept more than 3 values");
            } else {
                double lon = doubleArray.getDoubleArray(0);
                double lat = doubleArray.getDoubleArray(1);
                point.resetLat(lat);
                point.resetLon(lon);
                if (count == 3 && !ignoreZValue) {
                    // Z value is ignored as GeoPoint doesn't support elevation
                    GeoPoint.assertZValue(ignoreZValue, doubleArray.getDoubleArray(2));
                }
            }

        } else if (geoLocation.hasText()) {
            // String format: "lat,lon", WKT, or geohash
            String val = geoLocation.getText();
            point.resetFromString(val, ignoreZValue, effectivePoint);

        } else if (geoLocation.hasGeohash()) {
            org.opensearch.protobufs.GeoHashLocation geohashLocation = geoLocation.getGeohash();
            point.resetFromGeoHash(geohashLocation.getGeohash());

        } else {
            throw new IllegalArgumentException("geo_point expected");
        }

        return point;
    }
}
