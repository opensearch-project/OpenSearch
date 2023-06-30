/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.bucket.geogrid.util;

import org.opensearch.common.geo.GeoShapeDocValue;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geometry.utils.Geohash;

import java.util.ArrayList;
import java.util.List;

/**
 * We have a {@link Geohash} class present at the libs level, not using that because while encoding the shapes we need
 * {@link GeoShapeDocValue}. This class provided the utilities encode the shape as GeoHashes
 */
public class GeoShapeHashUtil {

    /**
     * The function encodes the shape provided as {@link GeoShapeDocValue} to a {@link List} of {@link Long} values
     * (representing the GeoHashes) which are intersecting with the shapes at a given precision.
     *
     * @param geoShapeDocValue {@link GeoShapeDocValue}
     * @param precision int
     * @return {@link List} containing encoded {@link Long} values
     */
    public static List<Long> encodeShape(final GeoShapeDocValue geoShapeDocValue, final int precision) {
        final List<Long> encodedValues = new ArrayList<>();
        final GeoShapeDocValue.BoundingRectangle boundingRectangle = geoShapeDocValue.getBoundingRectangle();
        long topLeftGeoHash = Geohash.longEncode(boundingRectangle.getMinX(), boundingRectangle.getMaxY(), precision);
        long topRightGeoHash = Geohash.longEncode(boundingRectangle.getMaxX(), boundingRectangle.getMaxY(), precision);
        long bottomRightGeoHash = Geohash.longEncode(boundingRectangle.getMaxX(), boundingRectangle.getMinY(), precision);

        long currentValue = topLeftGeoHash;
        long rightMax = topRightGeoHash;
        long tempCurrent = currentValue;
        while (true) {
            // check if this currentValue intersect with shape.
            final Rectangle geohashRectangle = Geohash.toBoundingBox(Geohash.stringEncode(tempCurrent));
            if (geoShapeDocValue.isIntersectingRectangle(geohashRectangle)) {
                encodedValues.add(tempCurrent);
            }

            // Breaking condition
            if (tempCurrent == bottomRightGeoHash) {
                break;
            }
            // now change the iterator => tempCurrent
            if (tempCurrent == rightMax) {
                // move to next row
                tempCurrent = Geohash.longEncode(Geohash.getNeighbor(Geohash.stringEncode(currentValue), precision, 0, -1));
                currentValue = tempCurrent;
                // update right max
                rightMax = Geohash.longEncode(Geohash.getNeighbor(Geohash.stringEncode(rightMax), precision, 0, -1));
            } else {
                // move to next column
                tempCurrent = Geohash.longEncode(Geohash.getNeighbor(Geohash.stringEncode(tempCurrent), precision, 1, 0));
            }
        }
        return encodedValues;
    }
}
