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

package org.opensearch.geometry.utils;

import org.opensearch.geometry.Circle;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.GeometryCollection;
import org.opensearch.geometry.GeometryVisitor;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.LinearRing;
import org.opensearch.geometry.MultiLine;
import org.opensearch.geometry.MultiPoint;
import org.opensearch.geometry.MultiPolygon;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;

/**
 * Validator that checks that lats are between -90 and +90 and lons are between -180 and +180 and altitude is present only if
 * ignoreZValue is set to true
 */
public class GeographyValidator implements GeometryValidator {

    /**
     * Minimum longitude value.
     */
    private static final double MIN_LON_INCL = -180.0D;

    /**
     * Maximum longitude value.
     */
    private static final double MAX_LON_INCL = 180.0D;

    /**
     * Minimum latitude value.
     */
    private static final double MIN_LAT_INCL = -90.0D;

    /**
     * Maximum latitude value.
     */
    private static final double MAX_LAT_INCL = 90.0D;

    private final boolean ignoreZValue;

    public GeographyValidator(boolean ignoreZValue) {
        this.ignoreZValue = ignoreZValue;
    }

    /**
     * validates latitude value is within standard +/-90 coordinate bounds
     */
    protected void checkLatitude(double latitude) {
        if (Double.isNaN(latitude) || latitude < MIN_LAT_INCL || latitude > MAX_LAT_INCL) {
            throw new IllegalArgumentException(
                "invalid latitude " + latitude + "; must be between " + MIN_LAT_INCL + " and " + MAX_LAT_INCL
            );
        }
    }

    /**
     * validates longitude value is within standard +/-180 coordinate bounds
     */
    protected void checkLongitude(double longitude) {
        if (Double.isNaN(longitude) || longitude < MIN_LON_INCL || longitude > MAX_LON_INCL) {
            throw new IllegalArgumentException(
                "invalid longitude " + longitude + "; must be between " + MIN_LON_INCL + " and " + MAX_LON_INCL
            );
        }
    }

    protected void checkAltitude(double zValue) {
        if (ignoreZValue == false && Double.isNaN(zValue) == false) {
            throw new IllegalArgumentException(
                "found Z value [" + zValue + "] but [ignore_z_value] " + "parameter is [" + ignoreZValue + "]"
            );
        }
    }

    @Override
    public void validate(Geometry geometry) {
        geometry.visit(new GeometryVisitor<Void, RuntimeException>() {

            @Override
            public Void visit(Circle circle) throws RuntimeException {
                checkLatitude(circle.getY());
                checkLongitude(circle.getX());
                checkAltitude(circle.getZ());
                return null;
            }

            @Override
            public Void visit(GeometryCollection<?> collection) throws RuntimeException {
                for (Geometry g : collection) {
                    g.visit(this);
                }
                return null;
            }

            @Override
            public Void visit(Line line) throws RuntimeException {
                for (int i = 0; i < line.length(); i++) {
                    checkLatitude(line.getY(i));
                    checkLongitude(line.getX(i));
                    checkAltitude(line.getZ(i));
                }
                return null;
            }

            @Override
            public Void visit(LinearRing ring) throws RuntimeException {
                for (int i = 0; i < ring.length(); i++) {
                    checkLatitude(ring.getY(i));
                    checkLongitude(ring.getX(i));
                    checkAltitude(ring.getZ(i));
                }
                return null;
            }

            @Override
            public Void visit(MultiLine multiLine) throws RuntimeException {
                return visit((GeometryCollection<?>) multiLine);
            }

            @Override
            public Void visit(MultiPoint multiPoint) throws RuntimeException {
                return visit((GeometryCollection<?>) multiPoint);
            }

            @Override
            public Void visit(MultiPolygon multiPolygon) throws RuntimeException {
                return visit((GeometryCollection<?>) multiPolygon);
            }

            @Override
            public Void visit(Point point) throws RuntimeException {
                checkLatitude(point.getY());
                checkLongitude(point.getX());
                checkAltitude(point.getZ());
                return null;
            }

            @Override
            public Void visit(Polygon polygon) throws RuntimeException {
                polygon.getPolygon().visit(this);
                for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                    polygon.getHole(i).visit(this);
                }
                return null;
            }

            @Override
            public Void visit(Rectangle rectangle) throws RuntimeException {
                checkLatitude(rectangle.getMinY());
                checkLatitude(rectangle.getMaxY());
                checkLongitude(rectangle.getMinX());
                checkLongitude(rectangle.getMaxX());
                checkAltitude(rectangle.getMinZ());
                checkAltitude(rectangle.getMaxZ());
                return null;
            }
        });
    }
}
