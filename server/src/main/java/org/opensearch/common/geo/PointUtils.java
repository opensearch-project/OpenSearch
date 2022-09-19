/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.common.geo;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.geometry.Point;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;

/**
 * A utility class with a point parser methods supporting different point representation formats
 *
 * @opensearch.internal
 */
public class PointUtils {
    private static final String FN_GEOJSON_TYPE = "type";
    private static final String FV_GEOJSON_TYPE_POINT = "Point";
    private static final String FN_GEOJSON_COORDS = "coordinates";

    private static final String FN_GEOHASH = "geohash";
    private static final String ERR_MSG_INVALID_TOKEN = "token [{}] not allowed";

    private String fieldNameX;
    private String fieldNameY;
    private boolean supportGeoHash;

    private String invalidFieldErrMsg;

    public PointUtils(final String fieldNameX, final String fieldNameY, final boolean supportGeoHash) {
        this.fieldNameX = fieldNameX;
        this.fieldNameY = fieldNameY;
        this.supportGeoHash = supportGeoHash;
        this.invalidFieldErrMsg = supportGeoHash
            ? String.format(
                Locale.ROOT,
                "field must be either %s/%s, %s/%s, or %s",
                fieldNameX,
                fieldNameY,
                FN_GEOJSON_TYPE,
                FN_GEOJSON_COORDS,
                FN_GEOHASH
            )
            : String.format(
                Locale.ROOT,
                "field must be either %s/%s, or %s/%s",
                fieldNameX,
                fieldNameY,
                FN_GEOJSON_TYPE,
                FN_GEOJSON_COORDS
            );
    }

    public Point parseObject(final XContentParser parser, final boolean ignoreZValue, final GeoUtils.EffectivePoint effectivePoint)
        throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new OpenSearchParseException("object is expected");
        }

        parser.nextToken();

        if (parser.currentToken() == XContentParser.Token.END_OBJECT) {
            throw new OpenSearchParseException(invalidFieldErrMsg);
        }

        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new OpenSearchParseException(ERR_MSG_INVALID_TOKEN, parser.currentToken());
        }

        Point point = null;
        String field = parser.currentName();
        if (fieldNameX.equals(field) || fieldNameY.equals(field)) {
            point = parseBasicFields(parser);
        } else if (supportGeoHash && FN_GEOHASH.equals(field)) {
            point = parseGeoHashFields(parser, effectivePoint);
        } else if (FN_GEOJSON_TYPE.equals(field) || FN_GEOJSON_COORDS.equals(field)) {
            point = parseGeoJsonFields(parser, ignoreZValue);
        } else {
            throw new OpenSearchParseException(invalidFieldErrMsg);
        }

        if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            throw new OpenSearchParseException(invalidFieldErrMsg);
        }

        return point;
    }

    private Point parseGeoHashFields(final XContentParser parser, final GeoUtils.EffectivePoint effectivePoint) throws IOException {
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new OpenSearchParseException(ERR_MSG_INVALID_TOKEN, parser.currentToken());
        }

        String field = parser.currentName();
        if (!FN_GEOHASH.equals(field)) {
            throw new OpenSearchParseException(invalidFieldErrMsg);
        }

        parser.nextToken();

        if (parser.currentToken() != XContentParser.Token.VALUE_STRING) {
            throw new OpenSearchParseException("{} must be a string", FN_GEOHASH);
        }

        String geoHash = parser.text();
        GeoPoint geoPoint = new GeoPoint();
        geoPoint = geoPoint.parseGeoHash(geoHash, effectivePoint);

        parser.nextToken();
        return new Point(geoPoint.lon(), geoPoint.lat());
    }

    private Point parseBasicFields(final XContentParser parser) throws IOException {
        HashMap<String, Double> xy = new HashMap<>();
        for (int i = 0; i < 2; i++) {
            if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                break;
            }

            String field = parser.currentName();
            if (!fieldNameX.equals(field) && !fieldNameY.equals(field)) {
                throw new OpenSearchParseException(invalidFieldErrMsg);
            }
            parser.nextToken();
            switch (parser.currentToken()) {
                case VALUE_NUMBER:
                case VALUE_STRING:
                    try {
                        xy.put(field, parser.doubleValue(true));
                    } catch (NumberFormatException e) {
                        throw new OpenSearchParseException("[{}] and [{}] must be valid double values", e, fieldNameX, fieldNameY);
                    }
                    break;
                default:
                    throw new OpenSearchParseException("{} must be a number", field);
            }
            parser.nextToken();
        }

        if (xy.get(fieldNameX) == null) {
            throw new OpenSearchParseException("field [{}] missing", fieldNameX);
        }
        if (xy.get(fieldNameY) == null) {
            throw new OpenSearchParseException("field [{}] missing", fieldNameY);
        }

        return new Point(xy.get(fieldNameX), xy.get(fieldNameY));
    }

    private Point parseGeoJsonFields(final XContentParser parser, final boolean ignoreZValue) throws IOException {
        boolean isTypePoint = false;
        Point point = null;
        for (int i = 0; i < 2; i++) {
            if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                throw new OpenSearchParseException(ERR_MSG_INVALID_TOKEN, parser.currentToken());
            }

            String field = parser.currentName();
            parser.nextToken();
            if (FN_GEOJSON_TYPE.equals(field)) {
                if (parser.currentToken() != XContentParser.Token.VALUE_STRING) {
                    throw new OpenSearchParseException("{} must be a string", FN_GEOJSON_TYPE);
                }

                if (!FV_GEOJSON_TYPE_POINT.equals(parser.text())) {
                    throw new OpenSearchParseException("{} must be {}", FN_GEOJSON_TYPE, FV_GEOJSON_TYPE_POINT);
                }
                isTypePoint = true;
            } else if (FN_GEOJSON_COORDS.equals(field)) {
                if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                    throw new OpenSearchParseException("{} must be an array", FN_GEOJSON_COORDS);
                }
                point = parseFromArray(parser, ignoreZValue, FN_GEOJSON_COORDS);
            } else {
                throw new OpenSearchParseException(invalidFieldErrMsg);
            }
            parser.nextToken();
        }

        if (!isTypePoint) {
            throw new OpenSearchParseException("field [{}] missing", FN_GEOJSON_TYPE);
        }

        if (point == null) {
            throw new OpenSearchParseException("field [{}] missing", FN_GEOJSON_COORDS);
        }

        return point;
    }

    public Point parseFromArray(final XContentParser parser, final boolean ignoreZValue, final String fieldName) throws IOException {
        double x = Double.NaN;
        double y = Double.NaN;

        if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            parser.nextToken();
        }

        int element = 0;
        while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                element++;
                if (element == 1) {
                    x = parser.doubleValue();
                } else if (element == 2) {
                    y = parser.doubleValue();
                } else if (element == 3) {
                    GeoPoint.assertZValue(ignoreZValue, parser.doubleValue());
                } else {
                    throw new OpenSearchParseException("[{}] field type does not accept > 3 dimensions", fieldName);
                }
            } else {
                throw new OpenSearchParseException("numeric value expected");
            }
            parser.nextToken();
        }
        if (element < 2) {
            throw new OpenSearchParseException("[{}] field type should have at least two dimensions", fieldName);
        }
        return new Point(x, y);
    }
}
