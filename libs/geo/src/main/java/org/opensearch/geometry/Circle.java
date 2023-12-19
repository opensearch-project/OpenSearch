/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.geometry;

import org.opensearch.geometry.utils.WellKnownText;

/**
 * Circle geometry (not part of WKT standard, but used in opensearch) defined by lat/lon coordinates of the center in degrees
 * and optional altitude in meters.
 */
public class Circle implements Geometry {

    /** Empty circle : x=0, y=0, z=NaN radius=-1 */
    public static final Circle EMPTY = new Circle();
    /** Latitude of the center of the circle in degrees */
    private final double y;
    /** Longitude of the center of the circle in degrees */
    private final double x;
    /** Altitude of the center of the circle in meters (NaN if irrelevant) */
    private final double z;
    /** Radius of the circle in meters */
    private final double radiusMeters;

    /** Create an {@link #EMPTY} circle */
    private Circle() {
        y = 0;
        x = 0;
        z = Double.NaN;
        radiusMeters = -1;
    }

    /**
     * Create a circle with no altitude.
     * @param x Longitude of the center of the circle in degrees
     * @param y Latitude of the center of the circle in degrees
     * @param radiusMeters Radius of the circle in meters
     */
    public Circle(final double x, final double y, final double radiusMeters) {
        this(x, y, Double.NaN, radiusMeters);
    }

    /**
     * Create a circle with altitude.
     * @param x Longitude of the center of the circle in degrees
     * @param y Latitude of the center of the circle in degrees
     * @param z Altitude of the center of the circle in meters
     * @param radiusMeters Radius of the circle in meters
     */
    public Circle(final double x, final double y, final double z, final double radiusMeters) {
        this.y = y;
        this.x = x;
        this.radiusMeters = radiusMeters;
        this.z = z;
        if (radiusMeters < 0) {
            throw new IllegalArgumentException("Circle radius [" + radiusMeters + "] cannot be negative");
        }
    }

    /**
     * @return The type of this geometry (always {@link ShapeType#CIRCLE})
     */
    @Override
    public ShapeType type() {
        return ShapeType.CIRCLE;
    }

    /**
     * @return The y (latitude) of the center of the circle in degrees
     */
    public double getY() {
        return y;
    }

    /**
     * @return The x (longitude) of the center of the circle in degrees
     */
    public double getX() {
        return x;
    }

    /**
     * @return The radius of the circle in meters
     */
    public double getRadiusMeters() {
        return radiusMeters;
    }

    /**
     * @return The altitude of the center of the circle in meters (NaN if irrelevant)
     */
    public double getZ() {
        return z;
    }

    /**
     * @return The latitude (y) of the center of the circle in degrees
     */
    public double getLat() {
        return y;
    }

    /**
     * @return The longitude (x) of the center of the circle in degrees
     */
    public double getLon() {
        return x;
    }

    /**
     * @return The altitude (z) of the center of the circle in meters (NaN if irrelevant)
     */
    public double getAlt() {
        return z;
    }

    /**
     * Compare this circle to another circle.
     * @param o The other circle
     * @return True if the two circles are equal in all their properties. False if null or different.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Circle circle = (Circle) o;
        if (Double.compare(circle.y, y) != 0) return false;
        if (Double.compare(circle.x, x) != 0) return false;
        if (Double.compare(circle.radiusMeters, radiusMeters) != 0) return false;
        return (Double.compare(circle.z, z) == 0);
    }

    /**
     * @return The hashcode of this circle.
     */
    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(y);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(x);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(radiusMeters);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(z);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    /**
     * Visit this circle with a {@link GeometryVisitor}.
     *
     * @param visitor The visitor
     * @param <T> The return type of the visitor
     * @param <E> The exception type of the visitor
     * @return The result of the visitor
     * @throws E The exception thrown by the visitor
     */
    @Override
    public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
        return visitor.visit(this);
    }

    /**
     * @return True if this circle is empty (radius less than 0)
     */
    @Override
    public boolean isEmpty() {
        return radiusMeters < 0;
    }

    @Override
    public String toString() {
        return WellKnownText.INSTANCE.toWKT(this);
    }

    /**
     * @return True if this circle has an altitude. False if NaN.
     */
    @Override
    public boolean hasZ() {
        return Double.isNaN(z) == false;
    }
}
