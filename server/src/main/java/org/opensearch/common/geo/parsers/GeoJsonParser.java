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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.geo.parsers;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.Explicit;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoShapeType;
import org.opensearch.common.geo.builders.CircleBuilder;
import org.opensearch.common.geo.builders.GeometryCollectionBuilder;
import org.opensearch.common.geo.builders.ShapeBuilder;
import org.opensearch.common.geo.builders.ShapeBuilder.Orientation;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentSubParser;
import org.opensearch.index.mapper.AbstractShapeGeometryFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.locationtech.jts.geom.Coordinate;

/**
 * Parses shape geometry represented in geojson
 *
 * complies with geojson specification: https://tools.ietf.org/html/rfc7946
 *
 * @opensearch.internal
 */
abstract class GeoJsonParser {
    protected static ShapeBuilder parse(XContentParser parser, AbstractShapeGeometryFieldMapper shapeMapper) throws IOException {
        GeoShapeType shapeType = null;
        DistanceUnit.Distance radius = null;
        CoordinateNode coordinateNode = null;
        GeometryCollectionBuilder geometryCollections = null;

        Orientation orientation = (shapeMapper == null)
            ? AbstractShapeGeometryFieldMapper.Defaults.ORIENTATION.value()
            : shapeMapper.orientation();
        Explicit<Boolean> coerce = (shapeMapper == null) ? AbstractShapeGeometryFieldMapper.Defaults.COERCE : shapeMapper.coerce();
        Explicit<Boolean> ignoreZValue = (shapeMapper == null)
            ? AbstractShapeGeometryFieldMapper.Defaults.IGNORE_Z_VALUE
            : shapeMapper.ignoreZValue();

        String malformedException = null;

        XContentParser.Token token;
        try (XContentParser subParser = new XContentSubParser(parser)) {
            while ((token = subParser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = subParser.currentName();

                    if (ShapeParser.FIELD_TYPE.match(fieldName, subParser.getDeprecationHandler())) {
                        subParser.nextToken();
                        final GeoShapeType type = GeoShapeType.forName(subParser.text());
                        if (shapeType != null && shapeType.equals(type) == false) {
                            malformedException = ShapeParser.FIELD_TYPE
                                + " already parsed as ["
                                + shapeType
                                + "] cannot redefine as ["
                                + type
                                + "]";
                        } else {
                            shapeType = type;
                        }
                    } else if (ShapeParser.FIELD_COORDINATES.match(fieldName, subParser.getDeprecationHandler())) {
                        subParser.nextToken();
                        CoordinateNode tempNode = parseCoordinates(subParser, ignoreZValue.value());
                        if (coordinateNode != null && tempNode.numDimensions() != coordinateNode.numDimensions()) {
                            throw new OpenSearchParseException("Exception parsing coordinates: " + "number of dimensions do not match");
                        }
                        coordinateNode = tempNode;
                    } else if (ShapeParser.FIELD_GEOMETRIES.match(fieldName, subParser.getDeprecationHandler())) {
                        if (shapeType == null) {
                            shapeType = GeoShapeType.GEOMETRYCOLLECTION;
                        } else if (shapeType.equals(GeoShapeType.GEOMETRYCOLLECTION) == false) {
                            malformedException = "cannot have [" + ShapeParser.FIELD_GEOMETRIES + "] with type set to [" + shapeType + "]";
                        }
                        subParser.nextToken();
                        geometryCollections = parseGeometries(subParser, shapeMapper);
                    } else if (CircleBuilder.FIELD_RADIUS.match(fieldName, subParser.getDeprecationHandler())) {
                        if (shapeType == null) {
                            shapeType = GeoShapeType.CIRCLE;
                        } else if (shapeType != null && shapeType.equals(GeoShapeType.CIRCLE) == false) {
                            malformedException = "cannot have [" + CircleBuilder.FIELD_RADIUS + "] with type set to [" + shapeType + "]";
                        }
                        subParser.nextToken();
                        radius = DistanceUnit.Distance.parseDistance(subParser.text());
                    } else if (ShapeParser.FIELD_ORIENTATION.match(fieldName, subParser.getDeprecationHandler())) {
                        if (shapeType != null
                            && (shapeType.equals(GeoShapeType.POLYGON) || shapeType.equals(GeoShapeType.MULTIPOLYGON)) == false) {
                            malformedException = "cannot have [" + ShapeParser.FIELD_ORIENTATION + "] with type set to [" + shapeType + "]";
                        }
                        subParser.nextToken();
                        orientation = ShapeBuilder.Orientation.fromString(subParser.text());
                    } else {
                        subParser.nextToken();
                        subParser.skipChildren();
                    }
                }
            }
        }

        if (malformedException != null) {
            throw new OpenSearchParseException(malformedException);
        } else if (shapeType == null) {
            throw new OpenSearchParseException("shape type not included");
        } else if (coordinateNode == null && GeoShapeType.GEOMETRYCOLLECTION != shapeType) {
            throw new OpenSearchParseException("coordinates not included");
        } else if (geometryCollections == null && GeoShapeType.GEOMETRYCOLLECTION == shapeType) {
            throw new OpenSearchParseException("geometries not included");
        } else if (radius != null && GeoShapeType.CIRCLE != shapeType) {
            throw new OpenSearchParseException("field [{}] is supported for [{}] only", CircleBuilder.FIELD_RADIUS, CircleBuilder.TYPE);
        }

        if (shapeType.equals(GeoShapeType.GEOMETRYCOLLECTION)) {
            return geometryCollections;
        }

        return shapeType.getBuilder(coordinateNode, radius, orientation, coerce.value());
    }

    /**
     * Recursive method which parses the arrays of coordinates used to define
     * Shapes
     *
     * @param parser
     *            Parser that will be read from
     * @return CoordinateNode representing the start of the coordinate tree
     * @throws IOException
     *             Thrown if an error occurs while reading from the
     *             XContentParser
     */
    private static CoordinateNode parseCoordinates(XContentParser parser, boolean ignoreZValue) throws IOException {
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.skipChildren();
            parser.nextToken();
            throw new OpenSearchParseException("coordinates cannot be specified as objects");
        }

        XContentParser.Token token = parser.nextToken();
        // Base cases
        if (token != XContentParser.Token.START_ARRAY
            && token != XContentParser.Token.END_ARRAY
            && token != XContentParser.Token.VALUE_NULL) {
            return new CoordinateNode(parseCoordinate(parser, ignoreZValue));
        } else if (token == XContentParser.Token.VALUE_NULL) {
            throw new IllegalArgumentException("coordinates cannot contain NULL values)");
        }

        List<CoordinateNode> nodes = new ArrayList<>();
        while (token != XContentParser.Token.END_ARRAY) {
            CoordinateNode node = parseCoordinates(parser, ignoreZValue);
            if (nodes.isEmpty() == false && nodes.get(0).numDimensions() != node.numDimensions()) {
                throw new OpenSearchParseException("Exception parsing coordinates: number of dimensions do not match");
            }
            nodes.add(node);
            token = parser.nextToken();
        }

        return new CoordinateNode(nodes);
    }

    private static Coordinate parseCoordinate(XContentParser parser, boolean ignoreZValue) throws IOException {
        if (parser.currentToken() != XContentParser.Token.VALUE_NUMBER) {
            throw new OpenSearchParseException("geo coordinates must be numbers");
        }
        double lon = parser.doubleValue();
        if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
            throw new OpenSearchParseException("geo coordinates must be numbers");
        }
        double lat = parser.doubleValue();
        XContentParser.Token token = parser.nextToken();
        // alt (for storing purposes only - future use includes 3d shapes)
        double alt = Double.NaN;
        if (token == XContentParser.Token.VALUE_NUMBER) {
            alt = GeoPoint.assertZValue(ignoreZValue, parser.doubleValue());
            parser.nextToken();
        }
        // do not support > 3 dimensions
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            throw new OpenSearchParseException("geo coordinates greater than 3 dimensions are not supported");
        }
        return new Coordinate(lon, lat, alt);
    }

    /**
     * Parse the geometries array of a GeometryCollection
     *
     * @param parser Parser that will be read from
     * @return Geometry[] geometries of the GeometryCollection
     * @throws IOException Thrown if an error occurs while reading from the XContentParser
     */
    static GeometryCollectionBuilder parseGeometries(XContentParser parser, AbstractShapeGeometryFieldMapper mapper) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new OpenSearchParseException("geometries must be an array of geojson objects");
        }

        XContentParser.Token token = parser.nextToken();
        GeometryCollectionBuilder geometryCollection = new GeometryCollectionBuilder();
        while (token != XContentParser.Token.END_ARRAY) {
            ShapeBuilder shapeBuilder = ShapeParser.parse(parser);
            geometryCollection.shape(shapeBuilder);
            token = parser.nextToken();
        }

        return geometryCollection;
    }
}
