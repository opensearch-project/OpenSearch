/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.geo;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.LatLonShapeDocValues;
import org.apache.lucene.document.LatLonShapeDocValuesField;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.GeometryVisitor;
import org.opensearch.geometry.Rectangle;
import org.opensearch.index.mapper.GeoShapeIndexer;

import java.io.IOException;
import java.util.List;

/**
 * This class is an OpenSearch Internal representation of lucene {@link LatLonShapeDocValuesField} for GeoShape.
 *
 * @opensearch.internal
 */
public class GeoShapeDocValue extends ShapeDocValue {
    private static final String FIELD_NAME = "missingField";
    private final LatLonShapeDocValues shapeDocValues;

    public GeoShapeDocValue(final String fieldName, final BytesRef bytesRef) {
        this(LatLonShape.createDocValueField(fieldName, bytesRef));
    }

    public GeoShapeDocValue(final LatLonShapeDocValuesField shapeDocValuesField) {
        centroid = new Centroid(shapeDocValuesField.getCentroid().getLat(), shapeDocValuesField.getCentroid().getLon());
        highestDimensionType = ShapeType.fromShapeFieldType(shapeDocValuesField.getHighestDimensionType());
        boundingRectangle = new BoundingRectangle(
            shapeDocValuesField.getBoundingBox().maxLon,
            shapeDocValuesField.getBoundingBox().maxLat,
            shapeDocValuesField.getBoundingBox().minLon,
            shapeDocValuesField.getBoundingBox().minLat
        );
        this.shapeDocValues = LatLonShape.createLatLonShapeDocValues(shapeDocValuesField.binaryValue());
    }

    /**
     * This function takes a {@link Geometry} and creates the {@link GeoShapeDocValue}. The function uses the
     * {@link GeoShapeIndexer} to first convert the {@link Geometry} to {@link IndexableField}s and then convert it
     * to the DocValue. This is very expensive function and should not be used on the Geometry Objects which are
     * already converted to {@link IndexableField}s as it does the Tessellation internally which is already done on the
     * {@link IndexableField}s.
     *
     * @param geometry {@link Geometry}
     * @return {@link GeoShapeDocValue}
     */
    public static GeoShapeDocValue createGeometryDocValue(final Geometry geometry) {
        // Setting the orientation to CCW, which will make the holes to CW. This is the default value which we will
        // be using. This is in conjunction with what we take as a default value for geoshape in WKT format.
        final GeoShapeIndexer shapeIndex = new GeoShapeIndexer(true, FIELD_NAME);
        final List<IndexableField> indexableFields = shapeIndex.indexShape(null, shapeIndex.prepareForIndexing(geometry));
        Field[] fieldsArray = new Field[indexableFields.size()];
        fieldsArray = indexableFields.toArray(fieldsArray);
        final LatLonShapeDocValuesField latLonShapeDocValuesField = LatLonShape.createDocValueField(FIELD_NAME, fieldsArray);
        return new GeoShapeDocValue(latLonShapeDocValuesField);
    }

    public Centroid getCentroid() {
        return (Centroid) centroid;
    }

    public BoundingRectangle getBoundingRectangle() {
        return (BoundingRectangle) boundingRectangle;
    }

    @Override
    public String toString() {
        return "BoundingRectangle(" + boundingRectangle + "), Centroid(" + centroid + "), HighestDimension(" + highestDimensionType + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GeoShapeDocValue object = (GeoShapeDocValue) o;
        boolean isEqual = true;
        if (boundingRectangle != null) {
            isEqual = boundingRectangle.equals(object.getBoundingRectangle());
        }
        if (centroid != null) {
            isEqual = isEqual && centroid.equals(object.getCentroid());
        }
        if (highestDimensionType != null) {
            isEqual = isEqual && highestDimensionType == object.getHighestDimensionType();
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = boundingRectangle != null ? boundingRectangle.hashCode() : 0L;
        result = Long.hashCode(temp);
        temp = centroid != null ? centroid.hashCode() : 0L;
        result = 31 * result + Long.hashCode(temp);
        temp = highestDimensionType != null ? highestDimensionType.hashCode() : 0L;

        result = 31 * result + Long.hashCode(temp);
        return result;
    }

    /**
     * An extension for {@link ShapeDocValue.Centroid} which make easy to read the values when centroid is on the
     * EarthSurface
     */
    public static final class Centroid extends ShapeDocValue.Centroid {

        Centroid(final double lat, final double lon) {
            super(lat, lon);
        }

        public double getLatitude() {
            return getY();
        }

        public double getLongitude() {
            return getX();
        }

        @Override
        public String toString() {
            return getY() + ", " + getX();
        }

    }

    /**
     * An extension for {@link ShapeDocValue.BoundingRectangle} which make easy to read the values when BB is on the
     * EarthSurface
     */
    public static final class BoundingRectangle extends ShapeDocValue.BoundingRectangle {

        BoundingRectangle(final double maxLon, final double maxLat, final double minLon, final double minLat) {
            super(maxLon, maxLat, minLon, minLat);
        }

        public double getMaxLongitude() {
            return getMaxX();
        }

        public double getMaxLatitude() {
            return getMaxY();
        }

        public double getMinLatitude() {
            return getMinY();
        }

        public double getMinLongitude() {
            return getMinX();
        }

        @Override
        public String toString() {
            return "maxLatitude: "
                + getMaxY()
                + ", minLatitude: "
                + getMinY()
                + ", maxLongitude: "
                + getMaxX()
                + ", minLongitude: "
                + getMinX();

        }
    }

    /**
     * Checks if the input {@link Rectangle} is intersecting with the shape represented as {@link GeoShapeDocValue}.
     * We could have used the {@link GeometryVisitor} here and added the functionality to check the intersection with
     * other {@link Geometry} also, but that will be an overkill for now, if required we can easily create a
     * {@link GeometryVisitor} to check the intersection with this Shape represented as {@link GeoShapeDocValue}.
     * @return boolean
     */
    public boolean isIntersectingRectangle(final Rectangle rectangle) {
        final org.apache.lucene.geo.Rectangle luceneRectangle = GeoShapeUtils.toLuceneRectangle(rectangle);
        try {
            final PointValues.Relation relation = shapeDocValues.relate(LatLonGeometry.create(luceneRectangle));
            return relation != PointValues.Relation.CELL_OUTSIDE_QUERY;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
