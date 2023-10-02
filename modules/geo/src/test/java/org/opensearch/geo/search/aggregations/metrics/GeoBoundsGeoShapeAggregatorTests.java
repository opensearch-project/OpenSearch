/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeDocValuesField;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.geo.GeoBoundingBox;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoShapeUtils;
import org.opensearch.geo.GeoModulePlugin;
import org.opensearch.geo.tests.common.AggregationInspectionHelper;
import org.opensearch.geo.tests.common.RandomGeoGeometryGenerator;
import org.opensearch.geometry.Circle;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.ShapeType;
import org.opensearch.index.mapper.GeoShapeFieldMapper;
import org.opensearch.index.mapper.GeoShapeIndexer;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.hamcrest.Matchers.closeTo;

public class GeoBoundsGeoShapeAggregatorTests extends AggregatorTestCase {
    private static final Logger LOG = LogManager.getLogger(GeoBoundsGeoShapeAggregatorTests.class);
    private static final double GEOHASH_TOLERANCE = 1E-5D;
    private static final String AGGREGATION_NAME = "my_agg";
    private static final String FIELD_NAME = "field";

    /**
     * Overriding the Search Plugins list with {@link GeoModulePlugin} so that the testcase will know that this plugin is
     * to be loaded during the tests.
     *
     * @return List of {@link SearchPlugin}
     */
    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new GeoModulePlugin());
    }

    /**
     * Testing Empty aggregator results.
     *
     * @throws Exception if an error occurs accessing the index
     */
    public void testEmpty() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            final GeoBoundsAggregationBuilder aggBuilder = new GeoBoundsAggregationBuilder(AGGREGATION_NAME).field(FIELD_NAME)
                .wrapLongitude(false);

            final MappedFieldType fieldType = new GeoShapeFieldMapper.GeoShapeFieldType(FIELD_NAME);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalGeoBounds bounds = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertTrue(Double.isInfinite(bounds.top));
                assertTrue(Double.isInfinite(bounds.bottom));
                assertTrue(Double.isInfinite(bounds.posLeft));
                assertTrue(Double.isInfinite(bounds.posRight));
                assertTrue(Double.isInfinite(bounds.negLeft));
                assertTrue(Double.isInfinite(bounds.negRight));
                assertFalse(AggregationInspectionHelper.hasValue(bounds));
            }
        }
    }

    /**
     * Testing GeoBoundAggregator for random shapes which are indexed.
     *
     * @throws Exception if an error occurs accessing the index
     */
    public void testRandom() throws Exception {
        final int numDocs = randomIntBetween(50, 100);
        final List<Double> Y = new ArrayList<>();
        final List<Double> X = new ArrayList<>();
        final Random random = random();
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random, dir)) {
            for (int i = 0; i < numDocs; i++) {
                final Document document = new Document();
                final Geometry geometry = randomLuceneGeometry(random);
                LOG.debug("Random Geometry created for Indexing : {}", geometry);
                document.add(createShapeDocValue(geometry));
                w.addDocument(document);
                getAllXAndYPoints(geometry, X, Y);
            }
            final GeoBoundsAggregationBuilder aggBuilder = new GeoBoundsAggregationBuilder(AGGREGATION_NAME).field(FIELD_NAME)
                .wrapLongitude(false);
            final MappedFieldType fieldType = new GeoShapeFieldMapper.GeoShapeFieldType(FIELD_NAME);
            try (IndexReader reader = w.getReader()) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                final InternalGeoBounds actualBounds = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                final GeoBoundingBox expectedGeoBounds = getExpectedGeoBounds(X, Y);
                MatcherAssert.assertThat(
                    actualBounds.bottomRight().getLat(),
                    closeTo(expectedGeoBounds.bottomRight().getLat(), GEOHASH_TOLERANCE)
                );
                MatcherAssert.assertThat(
                    actualBounds.bottomRight().getLon(),
                    closeTo(expectedGeoBounds.bottomRight().getLon(), GEOHASH_TOLERANCE)
                );
                MatcherAssert.assertThat(actualBounds.topLeft().getLat(), closeTo(expectedGeoBounds.topLeft().getLat(), GEOHASH_TOLERANCE));
                MatcherAssert.assertThat(actualBounds.topLeft().getLon(), closeTo(expectedGeoBounds.topLeft().getLon(), GEOHASH_TOLERANCE));
                assertTrue(AggregationInspectionHelper.hasValue(actualBounds));
            }
        }
    }

    private GeoBoundingBox getExpectedGeoBounds(final List<Double> X, final List<Double> Y) {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double posLeft = Double.POSITIVE_INFINITY;
        double posRight = Double.NEGATIVE_INFINITY;
        double negLeft = Double.POSITIVE_INFINITY;
        double negRight = Double.NEGATIVE_INFINITY;
        // Finding the bounding box for the shapes.
        for (final Double lon : X) {
            if (lon >= 0 && lon < posLeft) {
                posLeft = lon;
            }
            if (lon >= 0 && lon > posRight) {
                posRight = lon;
            }
            if (lon < 0 && lon < negLeft) {
                negLeft = lon;
            }
            if (lon < 0 && lon > negRight) {
                negRight = lon;
            }
        }
        for (final Double lat : Y) {
            if (lat > top) {
                top = lat;
            }
            if (lat < bottom) {
                bottom = lat;
            }
        }
        if (Double.isInfinite(posLeft)) {
            return new GeoBoundingBox(new GeoPoint(top, negLeft), new GeoPoint(bottom, negRight));
        } else if (Double.isInfinite(negLeft)) {
            return new GeoBoundingBox(new GeoPoint(top, posLeft), new GeoPoint(bottom, posRight));
        } else {
            return new GeoBoundingBox(new GeoPoint(top, negLeft), new GeoPoint(bottom, posRight));
        }
    }

    private void getAllXAndYPoints(final Geometry geometry, final List<Double> X, final List<Double> Y) {
        if (geometry instanceof Point) {
            final Point point = (Point) geometry;
            X.add(point.getX());
            Y.add(point.getY());
            return;
        } else if (geometry instanceof Polygon) {
            final Polygon polygon = (Polygon) geometry;
            for (int i = 0; i < polygon.getPolygon().getX().length; i++) {
                X.add(polygon.getPolygon().getX(i));
                Y.add(polygon.getPolygon().getY(i));
            }
            return;
        } else if (geometry instanceof Line) {
            final Line line = (Line) geometry;
            for (int i = 0; i < line.getX().length; i++) {
                X.add(line.getX(i));
                Y.add(line.getY(i));
            }
            return;
        }
        Assert.fail(
            String.format(Locale.ROOT, "Error cannot convert the %s to a valid indexable format[POINT, POLYGON, LINE]", geometry.getClass())
        );
    }

    private ShapeDocValuesField createShapeDocValue(final Geometry geometry) {
        if (geometry instanceof Point) {
            final Point point = (Point) geometry;
            return LatLonShape.createDocValueField(FIELD_NAME, point.getLat(), point.getLon());
        } else if (geometry instanceof Polygon) {
            return LatLonShape.createDocValueField(FIELD_NAME, GeoShapeUtils.toLucenePolygon((Polygon) geometry));
        } else if (geometry instanceof Line) {
            return LatLonShape.createDocValueField(FIELD_NAME, GeoShapeUtils.toLuceneLine((Line) geometry));
        }
        Assert.fail(
            String.format(Locale.ROOT, "Error cannot convert the %s to a valid indexable format[POINT, POLYGON, LINE]", geometry.getClass())
        );
        return null;
    }

    /**
     * Random function to generate a {@link LatLonGeometry}. Now for indexing of GeoShape field, we index all the
     * different Geometry shapes that we support({@link ShapeType}) in OpenSearch are broken down into 3 shapes only.
     * Hence, we are generating only 3 shapes : {@link org.apache.lucene.geo.Point},
     * {@link org.apache.lucene.geo.Line}, {@link org.apache.lucene.geo.Polygon}. {@link Circle} is not supported.
     * Check {@link GeoShapeIndexer#prepareForIndexing(org.opensearch.geometry.Geometry)}
     *
     * @return {@link LatLonGeometry}
     */
    private static Geometry randomLuceneGeometry(final Random r) {
        int shapeNumber = OpenSearchTestCase.randomIntBetween(0, 2);
        if (shapeNumber == 0) {
            // Point
            return RandomGeoGeometryGenerator.randomPoint(r);
        } else if (shapeNumber == 1) {
            // LineString
            return RandomGeoGeometryGenerator.randomLine(r);
        } else {
            // Polygon
            return RandomGeoGeometryGenerator.randomPolygon(r);
        }
    }

}
