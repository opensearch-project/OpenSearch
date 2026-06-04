/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.geo;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.document.XYShapeDocValuesField;

import java.util.Arrays;
import java.util.Locale;

/**
 * This class is an OpenSearch Internal representation of lucene {@link XYShapeDocValuesField} for GeoShape.
 *
 * @opensearch.internal
 */
public class ShapeDocValue {
    protected Centroid centroid;
    protected BoundingRectangle boundingRectangle;
    protected ShapeType highestDimensionType;

    public Centroid getCentroid() {
        return centroid;
    }

    public BoundingRectangle getBoundingRectangle() {
        return boundingRectangle;
    }

    public ShapeType getHighestDimensionType() {
        return highestDimensionType;
    }

    /**
     * Provides the centroid of the field(Shape) which has been indexed.
     */
    public static class Centroid {
        private final double y;
        private final double x;

        Centroid(final double y, final double x) {
            this.y = y;
            this.x = x;
        }

        public double getY() {
            return y;
        }

        public double getX() {
            return x;
        }

        @Override
        public String toString() {
            return y + ", " + x;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Centroid centroid = (Centroid) o;

            if (Double.compare(centroid.y, y) != 0) return false;
            if (Double.compare(centroid.x, x) != 0) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            temp = y != +0.0d ? Double.doubleToLongBits(y) : 0L;
            result = Long.hashCode(temp);
            temp = x != +0.0d ? Double.doubleToLongBits(x) : 0L;
            result = 31 * result + Long.hashCode(temp);
            return result;
        }
    }

    /**
     * Provides the BoundingBox of the field(Shape) which has been indexed.
     */
    public static class BoundingRectangle {
        private final double maxX, maxY, minY, minX;

        BoundingRectangle(final double maxLon, final double maxLat, final double minLon, final double minLat) {
            maxY = maxLat;
            maxX = maxLon;
            minY = minLat;
            minX = minLon;
        }

        public double getMaxX() {
            return maxX;
        }

        public double getMaxY() {
            return maxY;
        }

        public double getMinY() {
            return minY;
        }

        public double getMinX() {
            return minX;
        }

        @Override
        public String toString() {
            return "maxY: " + maxY + "minY: " + minY + "maxX: " + maxX + "minX: " + minX;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BoundingRectangle boundingRectangle = (BoundingRectangle) o;

            if (Double.compare(boundingRectangle.maxY, maxY) != 0) return false;
            if (Double.compare(boundingRectangle.maxX, minX) != 0) return false;
            if (Double.compare(boundingRectangle.minY, minY) != 0) return false;
            if (Double.compare(boundingRectangle.minX, minX) != 0) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            temp = maxY != +0.0d ? Double.doubleToLongBits(maxY) : 0L;
            result = Long.hashCode(temp);

            temp = maxX != +0.0d ? Double.doubleToLongBits(maxX) : 0L;
            result = 31 * result + Long.hashCode(temp);

            temp = minY != +0.0d ? Double.doubleToLongBits(minY) : 0L;
            result = 31 * result + Long.hashCode(temp);

            temp = minX != +0.0d ? Double.doubleToLongBits(minX) : 0L;
            result = 31 * result + Long.hashCode(temp);

            return result;
        }
    }

    /**
     * An Enum class defining the highest type of Geometry present in this doc value.
     */
    public enum ShapeType {
        POINT,
        LINE,
        TRIANGLE;

        public static ShapeType fromShapeFieldType(final ShapeField.DecodedTriangle.TYPE type) {
            switch (type) {
                case POINT:
                    return POINT;
                case LINE:
                    return LINE;
                case TRIANGLE:
                    return TRIANGLE;
            }
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "No correct mapped type found for the value %s in the list of values : %s",
                    type,
                    Arrays.toString(ShapeType.values())
                )
            );
        }

        @Override
        public String toString() {
            return name();
        }
    }
}
