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
import org.opensearch.common.geo.parsers.GeoWKTParser;
import org.opensearch.common.geo.parsers.ShapeParser;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.MultiLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

/**
 * Builds a multi line string geometry
 *
 * @opensearch.internal
 */
public class MultiLineStringBuilder extends ShapeBuilder<JtsGeometry, org.opensearch.geometry.Geometry, MultiLineStringBuilder> {

    public static final GeoShapeType TYPE = GeoShapeType.MULTILINESTRING;

    private final ArrayList<LineStringBuilder> lines = new ArrayList<>();

    /**
     * Read from a stream.
     */
    public MultiLineStringBuilder(StreamInput in) throws IOException {
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            linestring(new LineStringBuilder(in));
        }
    }

    public MultiLineStringBuilder() {
        super();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(lines.size());
        for (LineStringBuilder line : lines) {
            line.writeTo(out);
        }
    }

    public MultiLineStringBuilder linestring(LineStringBuilder line) {
        this.lines.add(line);
        return this;
    }

    public Coordinate[][] coordinates() {
        Coordinate[][] result = new Coordinate[lines.size()][];
        for (int i = 0; i < result.length; i++) {
            result[i] = lines.get(i).coordinates(false);
        }
        return result;
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    @Override
    protected StringBuilder contentToWKT() {
        final StringBuilder sb = new StringBuilder();
        if (lines.isEmpty()) {
            sb.append(GeoWKTParser.EMPTY);
        } else {
            sb.append(GeoWKTParser.LPAREN);
            if (lines.size() > 0) {
                sb.append(ShapeBuilder.coordinateListToWKT(lines.get(0).coordinates));
            }
            for (int i = 1; i < lines.size(); ++i) {
                sb.append(GeoWKTParser.COMMA);
                sb.append(ShapeBuilder.coordinateListToWKT(lines.get(i).coordinates));
            }
            sb.append(GeoWKTParser.RPAREN);
        }
        return sb;
    }

    public int numDimensions() {
        if (lines == null || lines.isEmpty()) {
            throw new IllegalStateException("unable to get number of dimensions, " + "LineStrings have not yet been initialized");
        }
        return lines.get(0).numDimensions();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ShapeParser.FIELD_TYPE.getPreferredName(), TYPE.shapeName());
        builder.field(ShapeParser.FIELD_COORDINATES.getPreferredName());
        builder.startArray();
        for (LineStringBuilder line : lines) {
            line.coordinatesToXcontent(builder, false);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public JtsGeometry buildS4J() {
        final Geometry geometry;
        if (wrapdateline) {
            ArrayList<LineString> parts = new ArrayList<>();
            for (LineStringBuilder line : lines) {
                LineStringBuilder.decomposeS4J(FACTORY, line.coordinates(false), parts);
            }
            if (parts.size() == 1) {
                geometry = parts.get(0);
            } else {
                LineString[] lineStrings = parts.toArray(new LineString[0]);
                geometry = FACTORY.createMultiLineString(lineStrings);
            }
        } else {
            LineString[] lineStrings = new LineString[lines.size()];
            Iterator<LineStringBuilder> iterator = lines.iterator();
            for (int i = 0; iterator.hasNext(); i++) {
                lineStrings[i] = FACTORY.createLineString(iterator.next().coordinates(false));
            }
            geometry = FACTORY.createMultiLineString(lineStrings);
        }
        return jtsGeometry(geometry);
    }

    @Override
    public org.opensearch.geometry.Geometry buildGeometry() {
        if (lines.isEmpty()) {
            return MultiLine.EMPTY;
        }
        List<Line> linestrings = new ArrayList<>(lines.size());
        for (int i = 0; i < lines.size(); ++i) {
            LineStringBuilder lsb = lines.get(i);
            linestrings.add(
                new Line(lsb.coordinates.stream().mapToDouble(c -> c.x).toArray(), lsb.coordinates.stream().mapToDouble(c -> c.y).toArray())
            );
        }
        return new MultiLine(linestrings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lines);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MultiLineStringBuilder other = (MultiLineStringBuilder) obj;
        return Objects.equals(lines, other.lines);
    }
}
