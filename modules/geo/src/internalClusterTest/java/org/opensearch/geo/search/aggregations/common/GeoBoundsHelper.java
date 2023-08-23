/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.common;

import org.opensearch.common.geo.GeoPoint;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.GeometryCollection;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.MultiLine;
import org.opensearch.geometry.MultiPoint;
import org.opensearch.geometry.MultiPolygon;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geometry.ShapeType;
import org.junit.Assert;

import java.util.Locale;

/**
 * A helper class for finding the geo bounds for a shape or a point.
 */
public final class GeoBoundsHelper {

    /**
     * Updates the GeoBounds for the input GeoPoint in topLeft and bottomRight GeoPoints.
     *
     * @param geoPoint {@link GeoPoint}
     * @param topLeft {@link GeoPoint}
     * @param bottomRight {@link GeoPoint}
     */
    public static void updateBoundsForGeoPoint(final GeoPoint geoPoint, final GeoPoint topLeft, final GeoPoint bottomRight) {
        updateBoundsBottomRight(geoPoint, bottomRight);
        updateBoundsTopLeft(geoPoint, topLeft);
    }

    /**
     * Find the bottom right for a point and put it in the currentBounds param.
     *
     * @param geoPoint {@link GeoPoint}
     * @param currentBound {@link GeoPoint}
     */
    public static void updateBoundsBottomRight(final GeoPoint geoPoint, final GeoPoint currentBound) {
        if (geoPoint.lat() < currentBound.lat()) {
            currentBound.resetLat(geoPoint.lat());
        }
        if (geoPoint.lon() > currentBound.lon()) {
            currentBound.resetLon(geoPoint.lon());
        }
    }

    /**
     * Find the top left for a point and put it in the currentBounds param.
     *
     * @param geoPoint {@link GeoPoint}
     * @param currentBound {@link GeoPoint}
     */
    public static void updateBoundsTopLeft(final GeoPoint geoPoint, final GeoPoint currentBound) {
        if (geoPoint.lat() > currentBound.lat()) {
            currentBound.resetLat(geoPoint.lat());
        }
        if (geoPoint.lon() < currentBound.lon()) {
            currentBound.resetLon(geoPoint.lon());
        }
    }

    /**
     * Find the bounds for an input shape.
     *
     * @param geometry {@link Geometry}
     * @param geoShapeTopLeft {@link GeoPoint}
     * @param geoShapeBottomRight {@link GeoPoint}
     */
    public static void updateBoundsForGeometry(
        final Geometry geometry,
        final GeoPoint geoShapeTopLeft,
        final GeoPoint geoShapeBottomRight
    ) {
        final ShapeType shapeType = geometry.type();
        switch (shapeType) {
            case POINT:
                updateBoundsTopLeft((Point) geometry, geoShapeTopLeft);
                updateBoundsBottomRight((Point) geometry, geoShapeBottomRight);
                return;
            case MULTIPOINT:
                ((MultiPoint) geometry).getAll().forEach(p -> updateBoundsTopLeft(p, geoShapeTopLeft));
                ((MultiPoint) geometry).getAll().forEach(p -> updateBoundsBottomRight(p, geoShapeBottomRight));
                return;
            case POLYGON:
                updateBoundsTopLeft((Polygon) geometry, geoShapeTopLeft);
                updateBoundsBottomRight((Polygon) geometry, geoShapeBottomRight);
                return;
            case LINESTRING:
                updateBoundsTopLeft((Line) geometry, geoShapeTopLeft);
                updateBoundsBottomRight((Line) geometry, geoShapeBottomRight);
                return;
            case MULTIPOLYGON:
                ((MultiPolygon) geometry).getAll().forEach(p -> updateBoundsTopLeft(p, geoShapeTopLeft));
                ((MultiPolygon) geometry).getAll().forEach(p -> updateBoundsBottomRight(p, geoShapeBottomRight));
                return;
            case GEOMETRYCOLLECTION:
                ((GeometryCollection<?>) geometry).getAll()
                    .forEach(geo -> updateBoundsForGeometry(geo, geoShapeTopLeft, geoShapeBottomRight));
                return;
            case MULTILINESTRING:
                ((MultiLine) geometry).getAll().forEach(line -> updateBoundsTopLeft(line, geoShapeTopLeft));
                ((MultiLine) geometry).getAll().forEach(line -> updateBoundsBottomRight(line, geoShapeBottomRight));
                return;
            case ENVELOPE:
                updateBoundsTopLeft((Rectangle) geometry, geoShapeTopLeft);
                updateBoundsBottomRight((Rectangle) geometry, geoShapeBottomRight);
                return;
            default:
                Assert.fail(String.format(Locale.ROOT, "The shape type %s is not supported", shapeType));
        }
    }

    private static void updateBoundsTopLeft(final Point p, final GeoPoint currentBound) {
        final GeoPoint geoPoint = new GeoPoint(p.getLat(), p.getLon());
        updateBoundsTopLeft(geoPoint, currentBound);
    }

    private static void updateBoundsTopLeft(final Polygon polygon, final GeoPoint currentBound) {
        for (int i = 0; i < polygon.getPolygon().length(); i++) {
            double lat = polygon.getPolygon().getLats()[i];
            double lon = polygon.getPolygon().getLons()[i];
            final GeoPoint geoPoint = new GeoPoint(lat, lon);
            updateBoundsTopLeft(geoPoint, currentBound);
        }
    }

    private static void updateBoundsTopLeft(final Line line, final GeoPoint currentBound) {
        for (int i = 0; i < line.length(); i++) {
            double lat = line.getLats()[i];
            double lon = line.getLons()[i];
            final GeoPoint geoPoint = new GeoPoint(lat, lon);
            updateBoundsTopLeft(geoPoint, currentBound);
        }
    }

    private static void updateBoundsTopLeft(final Rectangle rectangle, final GeoPoint currentBound) {
        if (rectangle.getMaxLat() > currentBound.lat()) {
            currentBound.resetLat(rectangle.getMaxLat());
        }
        if (rectangle.getMinLon() < currentBound.lon()) {
            currentBound.resetLon(rectangle.getMinLon());
        }
    }

    private static void updateBoundsBottomRight(final Point p, final GeoPoint currentBound) {
        final GeoPoint geoPoint = new GeoPoint(p.getLat(), p.getLon());
        updateBoundsBottomRight(geoPoint, currentBound);
    }

    private static void updateBoundsBottomRight(final Polygon polygon, final GeoPoint currentBound) {
        for (int i = 0; i < polygon.getPolygon().length(); i++) {
            double lat = polygon.getPolygon().getLats()[i];
            double lon = polygon.getPolygon().getLons()[i];
            final GeoPoint geoPoint = new GeoPoint(lat, lon);
            updateBoundsBottomRight(geoPoint, currentBound);
        }
    }

    private static void updateBoundsBottomRight(final Line line, final GeoPoint currentBound) {
        for (int i = 0; i < line.length(); i++) {
            double lat = line.getLats()[i];
            double lon = line.getLons()[i];
            final GeoPoint geoPoint = new GeoPoint(lat, lon);
            updateBoundsBottomRight(geoPoint, currentBound);
        }
    }

    private static void updateBoundsBottomRight(final Rectangle rectangle, final GeoPoint currentBound) {
        if (rectangle.getMinLat() < currentBound.lat()) {
            currentBound.resetLat(rectangle.getMinLat());
        }
        if (rectangle.getMaxLon() > currentBound.lon()) {
            currentBound.resetLon(rectangle.getMaxLon());
        }
    }
}
