/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.settings.Settings;
import org.opensearch.geo.GeoModulePluginIntegTestCase;
import org.opensearch.geo.search.aggregations.common.GeoBoundsHelper;
import org.opensearch.geo.search.aggregations.metrics.GeoBounds;
import org.opensearch.geo.tests.common.AggregationBuilders;
import org.opensearch.geo.tests.common.RandomGeoGenerator;
import org.opensearch.geo.tests.common.RandomGeoGeometryGenerator;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.utils.WellKnownText;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Before;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;

/**
 * Tests to validate if user specified a missingValue in the input while doing the aggregation
 */
@OpenSearchIntegTestCase.SuiteScopeTestCase
public class MissingValueIT extends GeoModulePluginIntegTestCase {

    private static final String INDEX_NAME = "idx";
    private static final String GEO_SHAPE_FIELD_NAME = "myshape";
    private static final String GEO_SHAPE_FIELD_TYPE = "type=geo_shape";
    private static final String AGGREGATION_NAME = "bounds";
    private static final String NON_EXISTENT_FIELD = "non_existing_field";
    private static final WellKnownText WKT = WellKnownText.INSTANCE;
    private static Geometry indexedGeometry;
    private static GeoPoint indexedGeoPoint;
    private GeoPoint bottomRight;
    private GeoPoint topLeft;

    public MissingValueIT(Settings staticSettings) {
        super(staticSettings);
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        assertAcked(
            prepareCreate(INDEX_NAME).setMapping(
                "date",
                "type=date",
                "location",
                "type=geo_point",
                "str",
                "type=keyword",
                GEO_SHAPE_FIELD_NAME,
                GEO_SHAPE_FIELD_TYPE
            ).get()
        );
        indexedGeometry = RandomGeoGeometryGenerator.randomGeometry(random());
        indexedGeoPoint = RandomGeoGenerator.randomPoint(random());
        assert indexedGeometry != null;
        indexRandom(
            true,
            client().prepareIndex(INDEX_NAME).setId("1").setSource(),
            client().prepareIndex(INDEX_NAME)
                .setId("2")
                .setSource(
                    "str",
                    "foo",
                    "long",
                    3L,
                    "double",
                    5.5,
                    "date",
                    "2015-05-07",
                    "location",
                    indexedGeoPoint.toString(),
                    GEO_SHAPE_FIELD_NAME,
                    WKT.toWKT(indexedGeometry)
                )
        );
    }

    @Before
    public void runBeforeEachTest() {
        bottomRight = new GeoPoint(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        topLeft = new GeoPoint(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    public void testUnmappedGeoBounds() {
        final GeoPoint missingGeoPoint = RandomGeoGenerator.randomPoint(random());
        GeoBoundsHelper.updateBoundsBottomRight(missingGeoPoint, bottomRight);
        GeoBoundsHelper.updateBoundsTopLeft(missingGeoPoint, topLeft);
        SearchResponse response = client().prepareSearch(INDEX_NAME)
            .addAggregation(
                AggregationBuilders.geoBounds(AGGREGATION_NAME)
                    .field(NON_EXISTENT_FIELD)
                    .wrapLongitude(false)
                    .missing(missingGeoPoint.toString())
            )
            .get();
        assertSearchResponse(response);
        validateResult(response.getAggregations().get(AGGREGATION_NAME));
    }

    public void testGeoBounds() {
        GeoBoundsHelper.updateBoundsForGeoPoint(indexedGeoPoint, topLeft, bottomRight);
        final GeoPoint missingGeoPoint = RandomGeoGenerator.randomPoint(random());
        GeoBoundsHelper.updateBoundsForGeoPoint(missingGeoPoint, topLeft, bottomRight);
        SearchResponse response = client().prepareSearch(INDEX_NAME)
            .addAggregation(
                AggregationBuilders.geoBounds(AGGREGATION_NAME).field("location").wrapLongitude(false).missing(missingGeoPoint.toString())
            )
            .get();
        assertSearchResponse(response);
        validateResult(response.getAggregations().get(AGGREGATION_NAME));
    }

    public void testGeoBoundsWithMissingShape() {
        // create GeoBounds for the indexed Field
        GeoBoundsHelper.updateBoundsForGeometry(indexedGeometry, topLeft, bottomRight);
        final Geometry missingGeometry = RandomGeoGeometryGenerator.randomGeometry(random());
        assert missingGeometry != null;
        GeoBoundsHelper.updateBoundsForGeometry(missingGeometry, topLeft, bottomRight);
        final SearchResponse response = client().prepareSearch(INDEX_NAME)
            .addAggregation(
                AggregationBuilders.geoBounds(AGGREGATION_NAME)
                    .wrapLongitude(false)
                    .field(GEO_SHAPE_FIELD_NAME)
                    .missing(WKT.toWKT(missingGeometry))
            )
            .get();
        assertSearchResponse(response);
        validateResult(response.getAggregations().get(AGGREGATION_NAME));
    }

    public void testUnmappedGeoBoundsOnGeoShape() {
        // We cannot useGeometry other than Point as for GeoBoundsAggregation as the Default Value for the
        // CoreValueSourceType is GeoPoint hence we need to use Point here.
        final Geometry missingGeometry = RandomGeoGeometryGenerator.randomPoint(random());
        final SearchResponse response = client().prepareSearch(INDEX_NAME)
            .addAggregation(AggregationBuilders.geoBounds(AGGREGATION_NAME).field(NON_EXISTENT_FIELD).missing(WKT.toWKT(missingGeometry)))
            .get();
        GeoBoundsHelper.updateBoundsForGeometry(missingGeometry, topLeft, bottomRight);
        assertSearchResponse(response);
        validateResult(response.getAggregations().get(AGGREGATION_NAME));
    }

    private void validateResult(final GeoBounds bounds) {
        MatcherAssert.assertThat(bounds.bottomRight().lat(), closeTo(bottomRight.lat(), GEOHASH_TOLERANCE));
        MatcherAssert.assertThat(bounds.bottomRight().lon(), closeTo(bottomRight.lon(), GEOHASH_TOLERANCE));
        MatcherAssert.assertThat(bounds.topLeft().lat(), closeTo(topLeft.lat(), GEOHASH_TOLERANCE));
        MatcherAssert.assertThat(bounds.topLeft().lon(), closeTo(topLeft.lon(), GEOHASH_TOLERANCE));
    }
}
