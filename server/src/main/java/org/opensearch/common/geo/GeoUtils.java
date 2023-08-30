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

import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.util.SloppyMath;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.xcontent.MapXContentParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentSubParser;
import org.opensearch.geometry.ShapeType;
import org.opensearch.index.fielddata.FieldData;
import org.opensearch.index.fielddata.GeoPointValues;
import org.opensearch.index.fielddata.MultiGeoPointValues;
import org.opensearch.index.fielddata.NumericDoubleValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.index.fielddata.SortingNumericDoubleValues;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

/**
 * Useful geo utilities
 *
 * @opensearch.internal
 */
public class GeoUtils {
    private static final String ERR_MSG_INVALID_TOKEN = "token [{}] not allowed";
    private static final String ERR_MSG_INVALID_FIELDS = "field must be either [lon|lat], [type|coordinates], or [geohash]";
    /** Maximum valid latitude in degrees. */
    public static final double MAX_LAT = 90.0;
    /** Minimum valid latitude in degrees. */
    public static final double MIN_LAT = -90.0;
    /** Maximum valid longitude in degrees. */
    public static final double MAX_LON = 180.0;
    /** Minimum valid longitude in degrees. */
    public static final double MIN_LON = -180.0;

    public static final String LATITUDE = "lat";
    public static final String LONGITUDE = "lon";
    public static final String GEOHASH = "geohash";

    public static final String GEOJSON_TYPE = "type";
    public static final String GEOJSON_COORDS = "coordinates";
    /** Earth ellipsoid major axis defined by WGS 84 in meters */
    public static final double EARTH_SEMI_MAJOR_AXIS = 6378137.0;      // meters (WGS 84)

    /** Earth ellipsoid minor axis defined by WGS 84 in meters */
    public static final double EARTH_SEMI_MINOR_AXIS = 6356752.314245; // meters (WGS 84)

    /** Earth mean radius defined by WGS 84 in meters */
    public static final double EARTH_MEAN_RADIUS = 6371008.7714D;      // meters (WGS 84)

    /** Earth axis ratio defined by WGS 84 (0.996647189335) */
    public static final double EARTH_AXIS_RATIO = EARTH_SEMI_MINOR_AXIS / EARTH_SEMI_MAJOR_AXIS;

    /** Earth ellipsoid equator length in meters */
    public static final double EARTH_EQUATOR = 2 * Math.PI * EARTH_SEMI_MAJOR_AXIS;

    /** Earth ellipsoid polar distance in meters */
    public static final double EARTH_POLAR_DISTANCE = Math.PI * EARTH_SEMI_MINOR_AXIS;

    /** rounding error for quantized latitude and longitude values */
    public static final double TOLERANCE = 1E-6;

    /** Returns true if latitude is actually a valid latitude value.*/
    public static boolean isValidLatitude(double latitude) {
        if (Double.isNaN(latitude) || Double.isInfinite(latitude) || latitude < GeoUtils.MIN_LAT || latitude > GeoUtils.MAX_LAT) {
            return false;
        }
        return true;
    }

    /** Returns true if longitude is actually a valid longitude value. */
    public static boolean isValidLongitude(double longitude) {
        if (Double.isNaN(longitude) || Double.isInfinite(longitude) || longitude < GeoUtils.MIN_LON || longitude > GeoUtils.MAX_LON) {
            return false;
        }
        return true;
    }

    /**
     * Calculate the width (in meters) of geohash cells at a specific level
     * @param level geohash level must be greater or equal to zero
     * @return the width of cells at level in meters
     */
    public static double geoHashCellWidth(int level) {
        assert level >= 0;
        // Geohash cells are split into 32 cells at each level. the grid
        // alternates at each level between a 8x4 and a 4x8 grid
        return EARTH_EQUATOR / (1L << ((((level + 1) / 2) * 3) + ((level / 2) * 2)));
    }

    /**
     * Calculate the width (in meters) of quadtree cells at a specific level
     * @param level quadtree level must be greater or equal to zero
     * @return the width of cells at level in meters
     */
    public static double quadTreeCellWidth(int level) {
        assert level >= 0;
        return EARTH_EQUATOR / (1L << level);
    }

    /**
     * Calculate the height (in meters) of geohash cells at a specific level
     * @param level geohash level must be greater or equal to zero
     * @return the height of cells at level in meters
     */
    public static double geoHashCellHeight(int level) {
        assert level >= 0;
        // Geohash cells are split into 32 cells at each level. the grid
        // alternates at each level between a 8x4 and a 4x8 grid
        return EARTH_POLAR_DISTANCE / (1L << ((((level + 1) / 2) * 2) + ((level / 2) * 3)));
    }

    /**
     * Calculate the height (in meters) of quadtree cells at a specific level
     * @param level quadtree level must be greater or equal to zero
     * @return the height of cells at level in meters
     */
    public static double quadTreeCellHeight(int level) {
        assert level >= 0;
        return EARTH_POLAR_DISTANCE / (1L << level);
    }

    /**
     * Calculate the size (in meters) of geohash cells at a specific level
     * @param level geohash level must be greater or equal to zero
     * @return the size of cells at level in meters
     */
    public static double geoHashCellSize(int level) {
        assert level >= 0;
        final double w = geoHashCellWidth(level);
        final double h = geoHashCellHeight(level);
        return Math.sqrt(w * w + h * h);
    }

    /**
     * Calculate the size (in meters) of quadtree cells at a specific level
     * @param level quadtree level must be greater or equal to zero
     * @return the size of cells at level in meters
     */
    public static double quadTreeCellSize(int level) {
        assert level >= 0;
        return Math.sqrt(EARTH_POLAR_DISTANCE * EARTH_POLAR_DISTANCE + EARTH_EQUATOR * EARTH_EQUATOR) / (1L << level);
    }

    /**
     * Calculate the number of levels needed for a specific precision. Quadtree
     * cells will not exceed the specified size (diagonal) of the precision.
     * @param meters Maximum size of cells in meters (must greater than zero)
     * @return levels need to achieve precision
     */
    public static int quadTreeLevelsForPrecision(double meters) {
        assert meters >= 0;
        if (meters == 0) {
            return QuadPrefixTree.MAX_LEVELS_POSSIBLE;
        } else {
            final double ratio = 1 + (EARTH_POLAR_DISTANCE / EARTH_EQUATOR); // cell ratio
            final double width = Math.sqrt((meters * meters) / (ratio * ratio)); // convert to cell width
            final long part = Math.round(Math.ceil(EARTH_EQUATOR / width));
            final int level = Long.SIZE - Long.numberOfLeadingZeros(part) - 1; // (log_2)
            return (part <= (1L << level)) ? level : (level + 1); // adjust level
        }
    }

    /**
     * Calculate the number of levels needed for a specific precision. QuadTree
     * cells will not exceed the specified size (diagonal) of the precision.
     * @param distance Maximum size of cells as unit string (must greater or equal to zero)
     * @return levels need to achieve precision
     */
    public static int quadTreeLevelsForPrecision(String distance) {
        return quadTreeLevelsForPrecision(DistanceUnit.METERS.parse(distance, DistanceUnit.DEFAULT));
    }

    /**
     * Calculate the number of levels needed for a specific precision. GeoHash
     * cells will not exceed the specified size (diagonal) of the precision.
     * @param meters Maximum size of cells in meters (must greater or equal to zero)
     * @return levels need to achieve precision
     */
    public static int geoHashLevelsForPrecision(double meters) {
        assert meters >= 0;

        if (meters == 0) {
            return GeohashPrefixTree.getMaxLevelsPossible();
        } else {
            final double ratio = 1 + (EARTH_POLAR_DISTANCE / EARTH_EQUATOR); // cell ratio
            final double width = Math.sqrt((meters * meters) / (ratio * ratio)); // convert to cell width
            final double part = Math.ceil(EARTH_EQUATOR / width);
            if (part == 1) return 1;
            final int bits = (int) Math.round(Math.ceil(Math.log(part) / Math.log(2)));
            final int full = bits / 5;                // number of 5 bit subdivisions
            final int left = bits - full * 5;           // bit representing the last level
            final int even = full + (left > 0 ? 1 : 0);     // number of even levels
            final int odd = full + (left > 3 ? 1 : 0);      // number of odd levels
            return even + odd;
        }
    }

    /**
     * Calculate the number of levels needed for a specific precision. GeoHash
     * cells will not exceed the specified size (diagonal) of the precision.
     * @param distance Maximum size of cells as unit string (must greater or equal to zero)
     * @return levels need to achieve precision
     */
    public static int geoHashLevelsForPrecision(String distance) {
        return geoHashLevelsForPrecision(DistanceUnit.METERS.parse(distance, DistanceUnit.DEFAULT));
    }

    /**
     * Normalize longitude to lie within the -180 (exclusive) to 180 (inclusive) range.
     *
     * @param lon Longitude to normalize
     * @return The normalized longitude.
     */
    public static double normalizeLon(double lon) {
        if (lon > 180d || lon <= -180d) {
            lon = centeredModulus(lon, 360);
        }
        // avoid -0.0
        return lon + 0d;
    }

    /**
     * Normalize latitude to lie within the -90 to 90 (both inclusive) range.
     * <p>
     * Note: You should not normalize longitude and latitude separately,
     * because when normalizing latitude it may be necessary to
     * add a shift of 180&deg; in the longitude.
     * For this purpose, you should call the
     * {@link #normalizePoint(GeoPoint)} function.
     *
     * @param lat Latitude to normalize
     * @return The normalized latitude.
     * @see #normalizePoint(GeoPoint)
     */
    public static double normalizeLat(double lat) {
        if (lat > 90d || lat < -90d) {
            lat = centeredModulus(lat, 360);
            if (lat < -90) {
                lat = -180 - lat;
            } else if (lat > 90) {
                lat = 180 - lat;
            }
        }
        // avoid -0.0
        return lat + 0d;
    }

    /**
     * Normalize the geo {@code Point} for its coordinates to lie within their
     * respective normalized ranges.
     * <p>
     * Note: A shift of 180&deg; is applied in the longitude if necessary,
     * in order to normalize properly the latitude.
     *
     * @param point The point to normalize in-place.
     */
    public static void normalizePoint(GeoPoint point) {
        normalizePoint(point, true, true);
    }

    /**
     * Normalize the geo {@code Point} for the given coordinates to lie within
     * their respective normalized ranges.
     * <p>
     * You can control which coordinate gets normalized with the two flags.
     * <p>
     * Note: A shift of 180&deg; is applied in the longitude if necessary,
     * in order to normalize properly the latitude.
     * If normalizing latitude but not longitude, it is assumed that
     * the longitude is in the form x+k*360, with x in ]-180;180],
     * and k is meaningful to the application.
     * Therefore x will be adjusted while keeping k preserved.
     *
     * @param point   The point to normalize in-place.
     * @param normLat Whether to normalize latitude or leave it as is.
     * @param normLon Whether to normalize longitude.
     */
    public static void normalizePoint(GeoPoint point, boolean normLat, boolean normLon) {
        double[] pt = { point.lon(), point.lat() };
        normalizePoint(pt, normLon, normLat);
        point.reset(pt[1], pt[0]);
    }

    public static void normalizePoint(double[] lonLat) {
        normalizePoint(lonLat, true, true);
    }

    public static void normalizePoint(double[] lonLat, boolean normLon, boolean normLat) {
        assert lonLat != null && lonLat.length == 2;

        normLat = normLat && (lonLat[1] > 90 || lonLat[1] < -90);
        normLon = normLon && (lonLat[0] > 180 || lonLat[0] < -180 || normLat);

        if (normLat) {
            lonLat[1] = centeredModulus(lonLat[1], 360);
            boolean shift = true;
            if (lonLat[1] < -90) {
                lonLat[1] = -180 - lonLat[1];
            } else if (lonLat[1] > 90) {
                lonLat[1] = 180 - lonLat[1];
            } else {
                // No need to shift the longitude, and the latitude is normalized
                shift = false;
            }
            if (shift) {
                if (normLon) {
                    lonLat[0] += 180;
                } else {
                    // Longitude won't be normalized,
                    // keep it in the form x+k*360 (with x in ]-180;180])
                    // by only changing x, assuming k is meaningful for the user application.
                    lonLat[0] += normalizeLon(lonLat[0]) > 0 ? -180 : 180;
                }
            }
        }
        if (normLon) {
            lonLat[0] = centeredModulus(lonLat[0], 360);
        }
    }

    public static double centeredModulus(double dividend, double divisor) {
        double rtn = dividend % divisor;
        if (rtn <= 0) {
            rtn += divisor;
        }
        if (rtn > divisor / 2) {
            rtn -= divisor;
        }
        return rtn;
    }

    /**
     * Parse a {@link GeoPoint} with a {@link XContentParser}:
     *
     * @param parser {@link XContentParser} to parse the value from
     * @return new {@link GeoPoint} parsed from the parse
     */
    public static GeoPoint parseGeoPoint(XContentParser parser) throws IOException, OpenSearchParseException {
        return parseGeoPoint(parser, new GeoPoint());
    }

    public static GeoPoint parseGeoPoint(XContentParser parser, GeoPoint point) throws IOException, OpenSearchParseException {
        return parseGeoPoint(parser, point, false);
    }

    /**
     * Parses the value as a geopoint. The following types of values are supported:
     * <p>
     * Object: has to contain either lat and lon or geohash fields
     * <p>
     * String: expected to be in "latitude, longitude" format or a geohash
     * <p>
     * Array: two or more elements, the first element is longitude, the second is latitude, the rest is ignored if ignoreZValue is true
     */
    public static GeoPoint parseGeoPoint(Object value, final boolean ignoreZValue) throws OpenSearchParseException {
        return parseGeoPoint(value, new GeoPoint(), ignoreZValue);
    }

    /**
     * Parses the value as a geopoint. The following types of values are supported:
     * <p>
     * Object: has to contain either lat and lon or geohash fields
     * <p>
     * String: expected to be in "latitude, longitude" format or a geohash
     * <p>
     * Array: two or more elements, the first element is longitude, the second is latitude, the rest is ignored if ignoreZValue is true
     */
    public static GeoPoint parseGeoPoint(Object value, GeoPoint point, final boolean ignoreZValue) throws OpenSearchParseException {
        try (
            XContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                Collections.singletonMap("null_value", value),
                null
            )
        ) {
            parser.nextToken(); // start object
            parser.nextToken(); // field name
            parser.nextToken(); // field value
            return parseGeoPoint(parser, point, ignoreZValue);
        } catch (IOException ex) {
            throw new OpenSearchParseException("error parsing geopoint", ex);
        }
    }

    /**
     * Represents the point of the geohash cell that should be used as the value of geohash
     *
     * @opensearch.internal
     */
    public enum EffectivePoint {
        TOP_LEFT,
        TOP_RIGHT,
        BOTTOM_LEFT,
        BOTTOM_RIGHT
    }

    /**
     * Parse a geopoint represented as an object, string or an array. If the geopoint is represented as a geohash,
     * the left bottom corner of the geohash cell is used as the geopoint coordinates.GeoBoundingBoxQueryBuilder.java
     */
    public static GeoPoint parseGeoPoint(XContentParser parser, GeoPoint point, final boolean ignoreZValue) throws IOException,
        OpenSearchParseException {
        return parseGeoPoint(parser, point, ignoreZValue, EffectivePoint.BOTTOM_LEFT);
    }

    /**
     * Parse a {@link GeoPoint} with a {@link XContentParser}. A geopoint has one of the following forms:
     *
     * <ul>
     *     <li>Object: <pre>{@code {"lat": <latitude>, "lon": <longitude}}</pre></li>
     *     <li>String: <pre>{@code "<latitude>,<longitude>"}</pre></li>
     *     <li>GeoHash: <pre>{@code "<geohash>"}</pre></li>
     *     <li>WKT: <pre>{@code "POINT (<longitude> <latitude>)"}</pre></li>
     *     <li>Array: <pre>{@code [<longitude>, <latitude>]}</pre></li>
     *     <li>GeoJson: <pre>{@code {"type": "Point", "coordinates": [<longitude>, <latitude>]}}</pre><li>
     * </ul>
     *
     *
     * @param parser {@link XContentParser} to parse the value from
     * @param point A {@link GeoPoint} that will be reset by the values parsed
     * @param ignoreZValue tells to ignore z value or throw exception when there is a z value
     * @param effectivePoint tells which point to use for GeoHash form
     * @return new {@link GeoPoint} parsed from the parse
     */
    public static GeoPoint parseGeoPoint(
        final XContentParser parser,
        final GeoPoint point,
        final boolean ignoreZValue,
        final EffectivePoint effectivePoint
    ) throws IOException, OpenSearchParseException {
        switch (parser.currentToken()) {
            case START_OBJECT:
                parseGeoPointObject(parser, point, ignoreZValue, effectivePoint);
                break;
            case START_ARRAY:
                parseGeoPointArray(parser, point, ignoreZValue);
                break;
            case VALUE_STRING:
                String val = parser.text();
                point.resetFromString(val, ignoreZValue, effectivePoint);
                break;
            default:
                throw new OpenSearchParseException("geo_point expected");
        }
        return point;
    }

    private static GeoPoint parseGeoPointObject(
        final XContentParser parser,
        final GeoPoint point,
        final boolean ignoreZValue,
        final GeoUtils.EffectivePoint effectivePoint
    ) throws IOException {
        try (XContentSubParser subParser = new XContentSubParser(parser)) {
            if (subParser.nextToken() != XContentParser.Token.FIELD_NAME) {
                throw new OpenSearchParseException(ERR_MSG_INVALID_TOKEN, subParser.currentToken());
            }

            String field = subParser.currentName();
            if (LONGITUDE.equals(field) || LATITUDE.equals(field)) {
                parseGeoPointObjectBasicFields(subParser, point);
            } else if (GEOHASH.equals(field)) {
                parseGeoHashFields(subParser, point, effectivePoint);
            } else if (GEOJSON_TYPE.equals(field) || GEOJSON_COORDS.equals(field)) {
                parseGeoJsonFields(subParser, point, ignoreZValue);
            } else {
                throw new OpenSearchParseException(ERR_MSG_INVALID_FIELDS);
            }

            if (subParser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw new OpenSearchParseException(ERR_MSG_INVALID_FIELDS);
            }

            return point;
        }
    }

    private static GeoPoint parseGeoPointObjectBasicFields(final XContentParser parser, final GeoPoint point) throws IOException {
        HashMap<String, Double> data = new HashMap<>();
        for (int i = 0; i < 2; i++) {
            if (i != 0) {
                parser.nextToken();
            }

            if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                break;
            }

            String field = parser.currentName();
            if (LONGITUDE.equals(field) == false && LATITUDE.equals(field) == false) {
                throw new OpenSearchParseException(ERR_MSG_INVALID_FIELDS);
            }
            switch (parser.nextToken()) {
                case VALUE_NUMBER:
                case VALUE_STRING:
                    try {
                        data.put(field, parser.doubleValue(true));
                    } catch (NumberFormatException e) {
                        throw new OpenSearchParseException("[{}] and [{}] must be valid double values", e, LONGITUDE, LATITUDE);
                    }
                    break;
                default:
                    throw new OpenSearchParseException("{} must be a number", field);
            }
        }

        if (data.get(LONGITUDE) == null) {
            throw new OpenSearchParseException("field [{}] missing", LONGITUDE);
        }
        if (data.get(LATITUDE) == null) {
            throw new OpenSearchParseException("field [{}] missing", LATITUDE);
        }

        return point.reset(data.get(LATITUDE), data.get(LONGITUDE));
    }

    private static GeoPoint parseGeoHashFields(
        final XContentParser parser,
        final GeoPoint point,
        final GeoUtils.EffectivePoint effectivePoint
    ) throws IOException {
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new OpenSearchParseException(ERR_MSG_INVALID_TOKEN, parser.currentToken());
        }

        if (GEOHASH.equals(parser.currentName()) == false) {
            throw new OpenSearchParseException(ERR_MSG_INVALID_FIELDS);
        }

        if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
            throw new OpenSearchParseException("{} must be a string", GEOHASH);
        }

        return point.parseGeoHash(parser.text(), effectivePoint);
    }

    private static GeoPoint parseGeoJsonFields(final XContentParser parser, final GeoPoint point, final boolean ignoreZValue)
        throws IOException {
        boolean hasTypePoint = false;
        boolean hasCoordinates = false;
        for (int i = 0; i < 2; i++) {
            if (i != 0) {
                parser.nextToken();
            }

            if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                if (hasTypePoint == false) {
                    throw new OpenSearchParseException("field [{}] missing", GEOJSON_TYPE);
                }
                if (hasCoordinates == false) {
                    throw new OpenSearchParseException("field [{}] missing", GEOJSON_COORDS);
                }
            }

            if (GEOJSON_TYPE.equals(parser.currentName())) {
                if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                    throw new OpenSearchParseException("{} must be a string", GEOJSON_TYPE);
                }

                // To be consistent with geo_shape parsing, ignore case here as well.
                if (ShapeType.POINT.name().equalsIgnoreCase(parser.text()) == false) {
                    throw new OpenSearchParseException("{} must be Point", GEOJSON_TYPE);
                }
                hasTypePoint = true;
            } else if (GEOJSON_COORDS.equals(parser.currentName())) {
                if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                    throw new OpenSearchParseException("{} must be an array", GEOJSON_COORDS);
                }
                parseGeoPointArray(parser, point, ignoreZValue);
                hasCoordinates = true;
            } else {
                throw new OpenSearchParseException(ERR_MSG_INVALID_FIELDS);
            }
        }

        return point;
    }

    private static GeoPoint parseGeoPointArray(final XContentParser parser, final GeoPoint point, final boolean ignoreZValue)
        throws IOException {
        try (XContentSubParser subParser = new XContentSubParser(parser)) {
            double x = Double.NaN;
            double y = Double.NaN;

            int element = 0;
            while (subParser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (parser.currentToken() != XContentParser.Token.VALUE_NUMBER) {
                    throw new OpenSearchParseException("numeric value expected");
                }
                element++;
                if (element == 1) {
                    x = parser.doubleValue();
                } else if (element == 2) {
                    y = parser.doubleValue();
                } else if (element == 3) {
                    GeoPoint.assertZValue(ignoreZValue, parser.doubleValue());
                } else {
                    throw new OpenSearchParseException("[geo_point] field type does not accept more than 3 values");
                }
            }

            if (element < 2) {
                throw new OpenSearchParseException("[geo_point] field type should have at least two dimensions");
            }
            return point.reset(y, x);
        }
    }

    /**
     * Parse a {@link GeoPoint} from a string. The string must have one of the following forms:
     *
     * <ul>
     *     <li>Latitude, Longitude form: <pre>&quot;<i>&lt;latitude&gt;</i>,<i>&lt;longitude&gt;</i>&quot;</pre></li>
     *     <li>Geohash form:: <pre>&quot;<i>&lt;geohash&gt;</i>&quot;</pre></li>
     * </ul>
     *
     * @param val a String to parse the value from
     * @return new parsed {@link GeoPoint}
     */
    public static GeoPoint parseFromString(String val) {
        GeoPoint point = new GeoPoint();
        return point.resetFromString(val, false, EffectivePoint.BOTTOM_LEFT);
    }

    /**
     * Parse a precision that can be expressed as an integer or a distance measure like "1km", "10m".
     *
     * The precision is expressed as a number between 1 and 12 and indicates the length of geohash
     * used to represent geo points.
     *
     * @param parser {@link XContentParser} to parse the value from
     * @return int representing precision
     */
    public static int parsePrecision(XContentParser parser) throws IOException, OpenSearchParseException {
        XContentParser.Token token = parser.currentToken();
        if (token.equals(XContentParser.Token.VALUE_NUMBER)) {
            return XContentMapValues.nodeIntegerValue(parser.intValue());
        } else {
            String precision = parser.text();
            try {
                // we want to treat simple integer strings as precision levels, not distances
                return XContentMapValues.nodeIntegerValue(precision);
            } catch (NumberFormatException e) {
                // try to parse as a distance value
                final int parsedPrecision = GeoUtils.geoHashLevelsForPrecision(precision);
                try {
                    return checkPrecisionRange(parsedPrecision);
                } catch (IllegalArgumentException e2) {
                    // this happens when distance too small, so precision > 12. We'd like to see the original string
                    throw new IllegalArgumentException("precision too high [" + precision + "]", e2);
                }
            }
        }
    }

    /**
     * Checks that the precision is within range supported by opensearch - between 1 and 12
     *
     * Returns the precision value if it is in the range and throws an IllegalArgumentException if it
     * is outside the range.
     */
    public static int checkPrecisionRange(int precision) {
        if ((precision < 1) || (precision > 12)) {
            throw new IllegalArgumentException("Invalid geohash aggregation precision of " + precision + ". Must be between 1 and 12.");
        }
        return precision;
    }

    /** Returns the maximum distance/radius (in meters) from the point 'center' before overlapping */
    public static double maxRadialDistanceMeters(final double centerLat, final double centerLon) {
        if (Math.abs(centerLat) == MAX_LAT) {
            return SloppyMath.haversinMeters(centerLat, centerLon, 0, centerLon);
        }
        return SloppyMath.haversinMeters(centerLat, centerLon, centerLat, (MAX_LON + centerLon) % 360);
    }

    /** Return the distance (in meters) between 2 lat,lon geo points using the haversine method implemented by lucene */
    public static double arcDistance(double lat1, double lon1, double lat2, double lon2) {
        return SloppyMath.haversinMeters(lat1, lon1, lat2, lon2);
    }

    /**
     * Return the distance (in meters) between 2 lat,lon geo points using a simple tangential plane
     * this provides a faster alternative to {@link GeoUtils#arcDistance} but is inaccurate for distances greater than
     * 4 decimal degrees
     */
    public static double planeDistance(double lat1, double lon1, double lat2, double lon2) {
        double x = Math.toRadians(lon2 - lon1) * Math.cos(Math.toRadians((lat2 + lat1) / 2.0d));
        double y = Math.toRadians(lat2 - lat1);
        return Math.sqrt(x * x + y * y) * EARTH_MEAN_RADIUS;
    }

    /**
     * Return a {@link SortedNumericDoubleValues} instance that returns the distances to a list of geo-points
     * for each document.
     */
    public static SortedNumericDoubleValues distanceValues(
        final GeoDistance distance,
        final DistanceUnit unit,
        final MultiGeoPointValues geoPointValues,
        final GeoPoint... fromPoints
    ) {
        final GeoPointValues singleValues = FieldData.unwrapSingleton(geoPointValues);
        if (singleValues != null && fromPoints.length == 1) {
            return FieldData.singleton(new NumericDoubleValues() {

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return singleValues.advanceExact(doc);
                }

                @Override
                public double doubleValue() throws IOException {
                    final GeoPoint from = fromPoints[0];
                    final GeoPoint to = singleValues.geoPointValue();
                    return distance.calculate(from.lat(), from.lon(), to.lat(), to.lon(), unit);
                }

            });
        } else {
            return new SortingNumericDoubleValues() {
                @Override
                public boolean advanceExact(int target) throws IOException {
                    if (geoPointValues.advanceExact(target)) {
                        resize(geoPointValues.docValueCount() * fromPoints.length);
                        int v = 0;
                        for (int i = 0; i < geoPointValues.docValueCount(); ++i) {
                            final GeoPoint point = geoPointValues.nextValue();
                            for (GeoPoint from : fromPoints) {
                                values[v] = distance.calculate(from.lat(), from.lon(), point.lat(), point.lon(), unit);
                                v++;
                            }
                        }
                        sort();
                        return true;
                    } else {
                        return false;
                    }
                }
            };
        }
    }

    private GeoUtils() {}
}
