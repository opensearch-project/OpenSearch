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

package org.opensearch.common.geo.builders;

import org.opensearch.common.geo.GeoShapeType;
import org.opensearch.common.geo.XShapeCollection;
import org.opensearch.common.geo.parsers.ShapeParser;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.geometry.MultiPoint;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Point;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds a multi point geometry
 *
 * @opensearch.internal
 */
public class MultiPointBuilder extends ShapeBuilder<XShapeCollection<Point>, MultiPoint, MultiPointBuilder> {

    public static final GeoShapeType TYPE = GeoShapeType.MULTIPOINT;

    /**
     * Create a new {@link MultiPointBuilder}.
     * @param coordinates needs at least two coordinates to be valid, otherwise will throw an exception
     */
    public MultiPointBuilder(List<Coordinate> coordinates) {
        super(coordinates);
    }

    /**
     * Creates a new empty MultiPoint builder
     */
    public MultiPointBuilder() {
        super();
    }

    /**
     * Read from a stream.
     */
    public MultiPointBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ShapeParser.FIELD_TYPE.getPreferredName(), TYPE.shapeName());
        builder.field(ShapeParser.FIELD_COORDINATES.getPreferredName());
        super.coordinatesToXcontent(builder, false);
        builder.endObject();
        return builder;
    }

    @Override
    public XShapeCollection<Point> buildS4J() {
        // Could wrap JtsGeometry but probably slower due to conversions to/from JTS in relate()
        // MultiPoint geometry = FACTORY.createMultiPoint(points.toArray(new Coordinate[points.size()]));
        List<Point> shapes = new ArrayList<>(coordinates.size());
        for (Coordinate coord : coordinates) {
            shapes.add(SPATIAL_CONTEXT.makePoint(coord.x, coord.y));
        }
        XShapeCollection<Point> multiPoints = new XShapeCollection<>(shapes, SPATIAL_CONTEXT);
        multiPoints.setPointsOnly(true);
        return multiPoints;
    }

    @Override
    public MultiPoint buildGeometry() {
        if (coordinates.isEmpty()) {
            return MultiPoint.EMPTY;
        }
        return new MultiPoint(
            coordinates.stream().map(coord -> new org.opensearch.geometry.Point(coord.x, coord.y)).collect(Collectors.toList())
        );
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    @Override
    public int numDimensions() {
        if (coordinates == null || coordinates.isEmpty()) {
            throw new IllegalStateException("unable to get number of dimensions, " + "LineString has not yet been initialized");
        }
        return Double.isNaN(coordinates.get(0).z) ? 2 : 3;
    }
}
