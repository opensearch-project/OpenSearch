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

package org.opensearch.index.mapper;

import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.index.IndexableField;
import org.opensearch.common.geo.GeoLineDecomposer;
import org.opensearch.common.geo.GeoPolygonDecomposer;
import org.opensearch.common.geo.GeoShapeType;
import org.opensearch.common.geo.GeoShapeUtils;
import org.opensearch.common.geo.GeoUtils;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.common.geo.GeoUtils.normalizePoint;

/**
 * Utility class that converts geometries into Lucene-compatible form for indexing in a geo_shape field.
 *
 * @opensearch.internal
 */
public class GeoShapeIndexer implements AbstractGeometryFieldMapper.Indexer<Geometry, Geometry> {

    private final boolean orientation;
    private final String name;

    public GeoShapeIndexer(boolean orientation, String name) {
        this.orientation = orientation;
        this.name = name;
    }

    public Geometry prepareForIndexing(Geometry geometry) {
        if (geometry == null) {
            return null;
        }

        return geometry.visit(new GeometryVisitor<Geometry, RuntimeException>() {
            @Override
            public Geometry visit(Circle circle) {
                throw new UnsupportedOperationException(GeoShapeType.CIRCLE + " geometry is not supported");
            }

            @Override
            public Geometry visit(GeometryCollection<?> collection) {
                if (collection.isEmpty()) {
                    return GeometryCollection.EMPTY;
                }
                List<Geometry> shapes = new ArrayList<>(collection.size());

                // Flatten collection and convert each geometry to Lucene-friendly format
                for (Geometry shape : collection) {
                    shapes.add(shape.visit(this));
                }

                if (shapes.size() == 1) {
                    return shapes.get(0);
                } else {
                    return new GeometryCollection<>(shapes);
                }
            }

            @Override
            public Geometry visit(Line line) {
                // decompose linestrings crossing dateline into array of Lines
                List<Line> lines = new ArrayList<>();
                GeoLineDecomposer.decomposeLine(line, lines);
                if (lines.isEmpty()) {
                    return GeometryCollection.EMPTY;
                } else if (lines.size() == 1) {
                    return lines.get(0);
                } else {
                    return new MultiLine(lines);
                }
            }

            @Override
            public Geometry visit(LinearRing ring) {
                throw new UnsupportedOperationException("cannot index linear ring [" + ring + "] directly");
            }

            @Override
            public Geometry visit(MultiLine multiLine) {
                List<Line> lines = new ArrayList<>();
                GeoLineDecomposer.decomposeMultiLine(multiLine, lines);
                if (lines.isEmpty()) {
                    return GeometryCollection.EMPTY;
                } else if (lines.size() == 1) {
                    return lines.get(0);
                } else {
                    return new MultiLine(lines);
                }
            }

            @Override
            public Geometry visit(MultiPoint multiPoint) {
                if (multiPoint.isEmpty()) {
                    return MultiPoint.EMPTY;
                } else if (multiPoint.size() == 1) {
                    return multiPoint.get(0).visit(this);
                } else {
                    List<Point> points = new ArrayList<>();
                    for (Point point : multiPoint) {
                        points.add((Point) point.visit(this));
                    }
                    return new MultiPoint(points);
                }
            }

            @Override
            public Geometry visit(MultiPolygon multiPolygon) {
                List<Polygon> polygons = new ArrayList<>();
                GeoPolygonDecomposer.decomposeMultiPolygon(multiPolygon, orientation, polygons);
                if (polygons.isEmpty()) {
                    return GeometryCollection.EMPTY;
                } else if (polygons.size() == 1) {
                    return polygons.get(0);
                } else {
                    return new MultiPolygon(polygons);
                }
            }

            @Override
            public Geometry visit(Point point) {
                double[] latlon = new double[] { point.getX(), point.getY() };
                normalizePoint(latlon);
                return new Point(latlon[0], latlon[1]);
            }

            @Override
            public Geometry visit(Polygon polygon) {
                List<Polygon> polygons = new ArrayList<>();
                GeoPolygonDecomposer.decomposePolygon(polygon, orientation, polygons);
                if (polygons.isEmpty()) {
                    return GeometryCollection.EMPTY;
                } else if (polygons.size() == 1) {
                    return polygons.get(0);
                } else {
                    return new MultiPolygon(polygons);
                }
            }

            @Override
            public Geometry visit(Rectangle rectangle) {
                return rectangle;
            }
        });
    }

    @Override
    public Class<Geometry> processedClass() {
        return Geometry.class;
    }

    @Override
    public List<IndexableField> indexShape(ParseContext context, Geometry shape) {
        LuceneGeometryIndexer visitor = new LuceneGeometryIndexer(name);
        shape.visit(visitor);
        return visitor.fields();
    }

    /**
     * The shape indexer
     *
     * @opensearch.internal
     */
    private static class LuceneGeometryIndexer implements GeometryVisitor<Void, RuntimeException> {
        private List<IndexableField> fields = new ArrayList<>();
        private final String name;

        private LuceneGeometryIndexer(String name) {
            this.name = name;
        }

        List<IndexableField> fields() {
            return fields;
        }

        @Override
        public Void visit(Circle circle) {
            throw new IllegalArgumentException("invalid shape type found [Circle] while indexing shape");
        }

        @Override
        public Void visit(GeometryCollection<?> collection) {
            for (Geometry geometry : collection) {
                geometry.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(Line line) {
            addFields(LatLonShape.createIndexableFields(name, GeoShapeUtils.toLuceneLine(line)));
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new IllegalArgumentException("invalid shape type found [LinearRing] while indexing shape");
        }

        @Override
        public Void visit(MultiLine multiLine) {
            for (Line line : multiLine) {
                visit(line);
            }
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            for (Point point : multiPoint) {
                visit(point);
            }
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            for (Polygon polygon : multiPolygon) {
                visit(polygon);
            }
            return null;
        }

        @Override
        public Void visit(Point point) {
            addFields(LatLonShape.createIndexableFields(name, point.getY(), point.getX()));
            return null;
        }

        @Override
        public Void visit(Polygon polygon) {
            addFields(LatLonShape.createIndexableFields(name, GeoShapeUtils.toLucenePolygon(polygon)));
            return null;
        }

        @Override
        public Void visit(Rectangle r) {
            if (r.getMinLon() > r.getMaxLon()) {
                Rectangle left = new Rectangle(r.getMinLon(), GeoUtils.MAX_LON, r.getMaxLat(), r.getMinLat());
                addFields(LatLonShape.createIndexableFields(name, GeoShapeUtils.toLucenePolygon(left)));
                Rectangle right = new Rectangle(GeoUtils.MIN_LON, r.getMaxLon(), r.getMaxLat(), r.getMinLat());
                addFields(LatLonShape.createIndexableFields(name, GeoShapeUtils.toLucenePolygon(right)));

            } else {
                addFields(LatLonShape.createIndexableFields(name, GeoShapeUtils.toLucenePolygon(r)));
            }
            return null;
        }

        private void addFields(IndexableField[] fields) {
            this.fields.addAll(Arrays.asList(fields));
        }
    }

}
