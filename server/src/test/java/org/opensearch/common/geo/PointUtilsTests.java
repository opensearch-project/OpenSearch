/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.common.geo;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.geometry.Point;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Locale;

public class PointUtilsTests extends OpenSearchTestCase {
    private static final PointUtils PARSER;
    private static final PointUtils PARSER_WITHOUT_GEOHASH;
    private static final String FIELD_NAME_X = "x";
    private static final String FIELD_NAME_Y = "y";
    private static final String FN_GEOHASH = "geohash";
    private static final String FN_GEOJSON_TYPE = "type";
    private static final String FV_GEOJSON_TYPE_POINT = "Point";
    private static final String FN_GEOJSON_COORDS = "coordinates";

    private static final String FN_IN_ERR_MSG = "test error message";

    static {
        PARSER = new PointUtils(FIELD_NAME_X, FIELD_NAME_Y, true);
        PARSER_WITHOUT_GEOHASH = new PointUtils(FIELD_NAME_X, FIELD_NAME_Y, false);
    }

    public void testParseFromObjectOfBasicForm() throws IOException {
        double x = 74.00;
        double y = 40.71;
        XContentBuilder pointInJson = XContentFactory.jsonBuilder().startObject().field(FIELD_NAME_X, x).field(FIELD_NAME_Y, y).endObject();
        try (XContentParser parser = createParser(pointInJson)) {
            parser.nextToken();
            Point point = PARSER.parseObject(parser, true, GeoUtils.EffectivePoint.BOTTOM_LEFT);
            assertEquals(x, point.getX(), 0.001);
            assertEquals(y, point.getY(), 0.001);
        }
    }

    public void testParseFromObjectOfBasicFormMissingValue() throws IOException {
        double x = 74.00;
        XContentBuilder pointInJson = XContentFactory.jsonBuilder().startObject().field(FIELD_NAME_X, x).endObject();
        try (XContentParser parser = createParser(pointInJson)) {
            parser.nextToken();
            OpenSearchParseException ex = expectThrows(
                OpenSearchParseException.class,
                () -> PARSER.parseObject(parser, true, GeoUtils.EffectivePoint.BOTTOM_LEFT)
            );
            assertEquals("field [y] missing", ex.getMessage());
        }
    }

    public void testParseFromObjectOfBasicFormAdditionalField() throws IOException {
        double x = 74.00;
        double y = 40.71;
        XContentBuilder pointInJson = XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_NAME_X, x)
            .field(FIELD_NAME_Y, y)
            .field("InvalidField", 10.1)
            .endObject();
        try (XContentParser parser = createParser(pointInJson)) {
            parser.nextToken();
            OpenSearchParseException ex = expectThrows(
                OpenSearchParseException.class,
                () -> PARSER.parseObject(parser, true, GeoUtils.EffectivePoint.BOTTOM_LEFT)
            );
            assertEquals("field must be either x/y, type/coordinates, or geohash", ex.getMessage());
        }
    }

    public void testParseFromObjectOfGeoHash() throws IOException {
        double longitude = 74.00;
        double latitude = 40.71;
        String geohash = "txhxegj0uyp3";
        XContentBuilder pointInJson = XContentFactory.jsonBuilder().startObject().field(FN_GEOHASH, geohash).endObject();
        try (XContentParser parser = createParser(pointInJson)) {
            parser.nextToken();
            Point point = PARSER.parseObject(parser, true, GeoUtils.EffectivePoint.BOTTOM_LEFT);
            assertEquals(longitude, point.getLon(), 0.001);
            assertEquals(latitude, point.getLat(), 0.001);
        }
    }

    public void testParseFromObjectOfGeoHashWithNoGeoHashSupport() throws IOException {
        String geohash = "txhxegj0uyp3";
        XContentBuilder pointInJson = XContentFactory.jsonBuilder().startObject().field(FN_GEOHASH, geohash).endObject();
        try (XContentParser parser = createParser(pointInJson)) {
            parser.nextToken();
            OpenSearchParseException ex = expectThrows(
                OpenSearchParseException.class,
                () -> PARSER_WITHOUT_GEOHASH.parseObject(parser, true, GeoUtils.EffectivePoint.BOTTOM_LEFT)
            );
            assertEquals("field must be either x/y, or type/coordinates", ex.getMessage());
        }
    }

    public void testParseFromObjectOfGeoJson() throws IOException {
        double x = 41.12;
        double y = -71.34;
        XContentBuilder pointInJson = XContentFactory.jsonBuilder()
            .startObject()
            .field(FN_GEOJSON_TYPE, FV_GEOJSON_TYPE_POINT)
            .array(FN_GEOJSON_COORDS, new double[] { x, y })
            .endObject();
        try (XContentParser parser = createParser(pointInJson)) {
            parser.nextToken();
            Point point = PARSER_WITHOUT_GEOHASH.parseObject(parser, true, GeoUtils.EffectivePoint.BOTTOM_LEFT);
            assertEquals(x, point.getX(), 0.001);
            assertEquals(y, point.getY(), 0.001);
        }
    }

    public void testParseFromArrayOne() throws IOException {
        double x = 41.12;
        XContentBuilder pointArray = XContentFactory.jsonBuilder().startArray().value(x).endArray();
        try (XContentParser parser = createParser(pointArray)) {
            parser.nextToken();
            // parser -> "[41.12]"
            OpenSearchParseException ex = expectThrows(
                OpenSearchParseException.class,
                () -> PARSER.parseFromArray(parser, true, FN_IN_ERR_MSG)
            );
            assertEquals(String.format(Locale.ROOT, "[%s] field type should have at least two dimensions", FN_IN_ERR_MSG), ex.getMessage());
        }
    }

    public void testParseFromArrayTwo() throws IOException {
        double x = 41.12;
        double y = -71.34;
        XContentBuilder pointArray = XContentFactory.jsonBuilder().startArray().value(x).value(y).endArray();
        try (XContentParser parser = createParser(pointArray)) {
            parser.nextToken();
            // parser -> "[41.12, -71.34]"
            Point point = PARSER.parseFromArray(parser, true, FN_IN_ERR_MSG);
            assertEquals(x, point.getX(), 0.001);
            assertEquals(y, point.getY(), 0.001);
        }
    }

    public void testParseFromArrayThree() throws IOException {
        double x = 41.12;
        double y = -71.34;
        double z = 12.12;
        XContentBuilder pointArray = XContentFactory.jsonBuilder().startArray().value(x).value(y).value(z).endArray();
        try (XContentParser parser = createParser(pointArray)) {
            parser.nextToken();
            // parser -> "[41.12, -71.34, 12.12]"
            OpenSearchParseException ex = expectThrows(
                OpenSearchParseException.class,
                () -> PARSER.parseFromArray(parser, false, FN_IN_ERR_MSG)
            );
            assertEquals(
                String.format(
                    Locale.ROOT,
                    "Exception parsing coordinates: found Z value [%.2f] but [ignore_z_value] parameter is [false]",
                    z
                ),
                ex.getMessage()
            );
        }
    }

    public void testParseFromArrayThreeIgnore() throws IOException {
        double x = 41.12;
        double y = -71.34;
        double z = 12.12;
        XContentBuilder pointArray = XContentFactory.jsonBuilder().startArray().value(x).value(y).value(z).endArray();
        try (XContentParser parser = createParser(pointArray)) {
            parser.nextToken();
            // parser -> "[41.12, -71.34, 12.12]"
            Point point = PARSER.parseFromArray(parser, true, FN_IN_ERR_MSG);
            assertEquals(x, point.getX(), 0.001);
            assertEquals(y, point.getY(), 0.001);
        }
    }

    public void testParseFromArrayFourIgnore() throws IOException {
        double x = 41.12;
        double y = -71.34;
        double z = 12.12;
        double a = 33.12;
        XContentBuilder pointArray = XContentFactory.jsonBuilder().startArray().value(x).value(y).value(z).value(a).endArray();
        try (XContentParser parser = createParser(pointArray)) {
            parser.nextToken();
            // parser -> "[41.12, -71.34, 12.12, 33.12]"
            OpenSearchParseException ex = expectThrows(
                OpenSearchParseException.class,
                () -> PARSER.parseFromArray(parser, true, FN_IN_ERR_MSG)
            );
            assertEquals(String.format(Locale.ROOT, "[%s] field type does not accept > 3 dimensions", FN_IN_ERR_MSG), ex.getMessage());
        }
    }

    // "41.12, -71.34]"
    public void testParseFromArrayPartialForm() throws IOException {
        double x = 41.12;
        double y = -71.34;
        XContentBuilder pointArray = XContentFactory.jsonBuilder().startArray().value(x).value(y).endArray();
        try (XContentParser parser = createParser(pointArray)) {
            parser.nextToken();
            parser.nextToken();
            // parser -> "41.12, -71.34]"
            Point point = PARSER.parseFromArray(parser, true, FN_IN_ERR_MSG);
            assertEquals(x, point.getX(), 0.001);
            assertEquals(y, point.getY(), 0.001);
        }
    }
}
