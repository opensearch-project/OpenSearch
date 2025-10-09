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

package org.opensearch.common.geo;

import org.opensearch.OpenSearchParseException;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geometry.ShapeType;
import org.opensearch.geometry.utils.StandardValidator;
import org.opensearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;
import java.util.Locale;
import java.util.Objects;

/**
 * A class representing a Geo-Bounding-Box for use by Geo queries and aggregations
 * that deal with extents/rectangles representing rectangular areas of interest.
 *
 * @opensearch.internal
 */
public class GeoBoundingBox implements ToXContentObject, Writeable {
    private static final WellKnownText WKT_PARSER = new WellKnownText(true, new StandardValidator(true));
    static final ParseField TOP_RIGHT_FIELD = new ParseField("top_right");
    static final ParseField BOTTOM_LEFT_FIELD = new ParseField("bottom_left");
    static final ParseField TOP_FIELD = new ParseField("top");
    static final ParseField BOTTOM_FIELD = new ParseField("bottom");
    static final ParseField LEFT_FIELD = new ParseField("left");
    static final ParseField RIGHT_FIELD = new ParseField("right");
    static final ParseField WKT_FIELD = new ParseField("wkt");
    public static final ParseField BOUNDS_FIELD = new ParseField("bounds");
    public static final ParseField LAT_FIELD = new ParseField("lat");
    public static final ParseField LON_FIELD = new ParseField("lon");
    public static final ParseField TOP_LEFT_FIELD = new ParseField("top_left");
    public static final ParseField BOTTOM_RIGHT_FIELD = new ParseField("bottom_right");

    private final GeoPoint topLeft;
    private final GeoPoint bottomRight;

    public GeoBoundingBox(GeoPoint topLeft, GeoPoint bottomRight) {
        this.topLeft = topLeft;
        this.bottomRight = bottomRight;
    }

    public GeoBoundingBox(StreamInput input) throws IOException {
        this.topLeft = new GeoPoint(input);
        this.bottomRight = new GeoPoint(input);
    }

    public boolean isUnbounded() {
        return Double.isNaN(topLeft.lon())
            || Double.isNaN(topLeft.lat())
            || Double.isNaN(bottomRight.lon())
            || Double.isNaN(bottomRight.lat());
    }

    public GeoPoint topLeft() {
        return topLeft;
    }

    public GeoPoint bottomRight() {
        return bottomRight;
    }

    public double top() {
        return topLeft.lat();
    }

    public double bottom() {
        return bottomRight.lat();
    }

    public double left() {
        return topLeft.lon();
    }

    public double right() {
        return bottomRight.lon();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(BOUNDS_FIELD.getPreferredName());
        toXContentFragment(builder, true);
        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentFragment(XContentBuilder builder, boolean buildLatLonFields) throws IOException {
        if (buildLatLonFields) {
            builder.startObject(TOP_LEFT_FIELD.getPreferredName());
            builder.field(LAT_FIELD.getPreferredName(), topLeft.lat());
            builder.field(LON_FIELD.getPreferredName(), topLeft.lon());
            builder.endObject();
        } else {
            builder.array(TOP_LEFT_FIELD.getPreferredName(), topLeft.lon(), topLeft.lat());
        }
        if (buildLatLonFields) {
            builder.startObject(BOTTOM_RIGHT_FIELD.getPreferredName());
            builder.field(LAT_FIELD.getPreferredName(), bottomRight.lat());
            builder.field(LON_FIELD.getPreferredName(), bottomRight.lon());
            builder.endObject();
        } else {
            builder.array(BOTTOM_RIGHT_FIELD.getPreferredName(), bottomRight.lon(), bottomRight.lat());
        }
        return builder;
    }

    /**
     * If the bounding box crosses the date-line (left greater-than right) then the
     * longitude of the point need only to be higher than the left or lower
     * than the right. Otherwise, it must be both.
     *
     * @param lon the longitude of the point
     * @param lat the latitude of the point
     * @return whether the point (lon, lat) is in the specified bounding box
     */
    public boolean pointInBounds(double lon, double lat) {
        if (lat >= bottom() && lat <= top()) {
            if (left() <= right()) {
                return lon >= left() && lon <= right();
            } else {
                return lon >= left() || lon <= right();
            }
        }
        return false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        topLeft.writeTo(out);
        bottomRight.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeoBoundingBox that = (GeoBoundingBox) o;
        return topLeft.equals(that.topLeft) && bottomRight.equals(that.bottomRight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topLeft, bottomRight);
    }

    @Override
    public String toString() {
        return "BBOX (" + topLeft.lon() + ", " + bottomRight.lon() + ", " + topLeft.lat() + ", " + bottomRight.lat() + ")";
    }

    /**
     * Parses the bounding box and returns bottom, top, left, right coordinates
     */
    public static GeoBoundingBox parseBoundingBox(XContentParser parser) throws IOException, OpenSearchParseException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new OpenSearchParseException("failed to parse bounding box. Expected start object but found [{}]", token);
        }

        double top = Double.NaN;
        double bottom = Double.NaN;
        double left = Double.NaN;
        double right = Double.NaN;

        String currentFieldName;
        GeoPoint sparse = new GeoPoint();
        Rectangle envelope = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                token = parser.nextToken();
                if (WKT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        Geometry geometry = WKT_PARSER.fromWKT(parser.text());
                        if (ShapeType.ENVELOPE.equals(geometry.type()) == false) {
                            throw new OpenSearchParseException(
                                String.format(Locale.ROOT, GeoUtils.WKT_BOUNDING_BOX_TYPE_ERROR, geometry.type(), ShapeType.ENVELOPE)
                            );
                        }
                        envelope = (Rectangle) geometry;
                    } catch (ParseException | IllegalArgumentException e) {
                        throw new OpenSearchParseException(GeoUtils.WKT_BOUNDING_BOX_PARSE_ERROR, e);
                    }
                } else if (TOP_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    top = parser.doubleValue();
                } else if (BOTTOM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    bottom = parser.doubleValue();
                } else if (LEFT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    left = parser.doubleValue();
                } else if (RIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    right = parser.doubleValue();
                } else {
                    if (TOP_LEFT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        GeoUtils.parseGeoPoint(parser, sparse, false, GeoUtils.EffectivePoint.TOP_LEFT);
                        top = sparse.getLat();
                        left = sparse.getLon();
                    } else if (BOTTOM_RIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        GeoUtils.parseGeoPoint(parser, sparse, false, GeoUtils.EffectivePoint.BOTTOM_RIGHT);
                        bottom = sparse.getLat();
                        right = sparse.getLon();
                    } else if (TOP_RIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        GeoUtils.parseGeoPoint(parser, sparse, false, GeoUtils.EffectivePoint.TOP_RIGHT);
                        top = sparse.getLat();
                        right = sparse.getLon();
                    } else if (BOTTOM_LEFT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        GeoUtils.parseGeoPoint(parser, sparse, false, GeoUtils.EffectivePoint.BOTTOM_LEFT);
                        bottom = sparse.getLat();
                        left = sparse.getLon();
                    } else {
                        throw new OpenSearchParseException("failed to parse bounding box. unexpected field [{}]", currentFieldName);
                    }
                }
            } else {
                throw new OpenSearchParseException("failed to parse bounding box. field name expected but [{}] found", token);
            }
        }
        if (envelope != null) {
            if (Double.isNaN(top) == false
                || Double.isNaN(bottom) == false
                || Double.isNaN(left) == false
                || Double.isNaN(right) == false) {
                throw new OpenSearchParseException(
                    "failed to parse bounding box. Conflicting definition found " + "using well-known text and explicit corners."
                );
            }
            GeoPoint topLeft = new GeoPoint(envelope.getMaxLat(), envelope.getMinLon());
            GeoPoint bottomRight = new GeoPoint(envelope.getMinLat(), envelope.getMaxLon());
            return new GeoBoundingBox(topLeft, bottomRight);
        }
        GeoPoint topLeft = new GeoPoint(top, left);
        GeoPoint bottomRight = new GeoPoint(bottom, right);
        return new GeoBoundingBox(topLeft, bottomRight);
    }

}
