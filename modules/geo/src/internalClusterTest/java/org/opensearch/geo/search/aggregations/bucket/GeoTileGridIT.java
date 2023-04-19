/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.bucket;

import org.hamcrest.MatcherAssert;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.geo.GeoBoundingBox;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoShapeDocValue;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoGrid;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.opensearch.geo.search.aggregations.common.GeoBoundsHelper;
import org.opensearch.geo.tests.common.AggregationBuilders;
import org.opensearch.geometry.Geometry;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.GeoTileUtils;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class GeoTileGridIT extends AbstractGeoBucketAggregationIntegTest {

    private static final int GEOPOINT_MAX_PRECISION = 17;

    private static final String AGG_NAME = "geotilegrid";

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        final Random random = random();
        // Creating a BB for limiting the number buckets generated during aggregation
        boundingRectangleForGeoShapesAgg = getGridAggregationBoundingBox(random);
        prepareSingleValueGeoPointIndex(random);
        prepareMultiValuedGeoPointIndex(random);
        prepareGeoShapeIndexForAggregations(random);
        ensureSearchable();
    }

    public void testGeoShapes() {
        final GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(boundingRectangleForGeoShapesAgg.getMaxLat(), boundingRectangleForGeoShapesAgg.getMinLon()),
            new GeoPoint(boundingRectangleForGeoShapesAgg.getMinLat(), boundingRectangleForGeoShapesAgg.getMaxLon())
        );
        for (int precision = 1; precision <= MAX_PRECISION_FOR_GEO_SHAPES_AGG_TESTING; precision++) {
            final GeoGridAggregationBuilder builder = AggregationBuilders.geotileGrid(AGG_NAME)
                .field(GEO_SHAPE_FIELD_NAME)
                .precision(precision);
            // This makes sure that for only higher precision we are providing the GeoBounding Box. This also ensures
            // that we are able to test both bounded and unbounded aggregations
            if (precision > MIN_PRECISION_WITHOUT_BB_AGGS) {
                builder.setGeoBoundingBox(boundingBox);
            }
            final SearchResponse response = client().prepareSearch(GEO_SHAPE_INDEX_NAME).addAggregation(builder).get();
            final GeoGrid geoGrid = response.getAggregations().get(AGG_NAME);
            final List<? extends GeoGrid.Bucket> buckets = geoGrid.getBuckets();
            final Object[] propertiesKeys = (Object[]) ((InternalAggregation) geoGrid).getProperty("_key");
            final Object[] propertiesDocCounts = (Object[]) ((InternalAggregation) geoGrid).getProperty("_count");
            for (int i = 0; i < buckets.size(); i++) {
                final GeoGrid.Bucket cell = buckets.get(i);
                final String geoTile = cell.getKeyAsString();

                final long bucketCount = cell.getDocCount();
                final int expectedBucketCount = expectedDocsCountForGeoShapes.get(geoTile);
                assertNotSame(bucketCount, 0);
                assertEquals("Geotile " + geoTile + " has wrong doc count ", expectedBucketCount, bucketCount);
                final GeoPoint geoPoint = (GeoPoint) propertiesKeys[i];
                MatcherAssert.assertThat(GeoTileUtils.stringEncode(geoPoint.lon(), geoPoint.lat(), precision), equalTo(geoTile));
                MatcherAssert.assertThat((long) propertiesDocCounts[i], equalTo(bucketCount));
            }
        }
    }

    public void testSimpleGeoPointsAggregation() {
        for (int precision = 1; precision <= GEOPOINT_MAX_PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("idx")
                .addAggregation(AggregationBuilders.geotileGrid(AGG_NAME).field(GEO_POINT_FIELD_NAME).precision(precision))
                .get();

            assertSearchResponse(response);

            GeoGrid geoGrid = response.getAggregations().get(AGG_NAME);
            List<? extends GeoGrid.Bucket> buckets = geoGrid.getBuckets();
            Object[] propertiesKeys = (Object[]) ((InternalAggregation) geoGrid).getProperty("_key");
            Object[] propertiesDocCounts = (Object[]) ((InternalAggregation) geoGrid).getProperty("_count");
            for (int i = 0; i < buckets.size(); i++) {
                GeoGrid.Bucket cell = buckets.get(i);
                String geoTile = cell.getKeyAsString();

                long bucketCount = cell.getDocCount();
                int expectedBucketCount = expectedDocCountsForSingleGeoPoint.get(geoTile);
                assertNotSame(bucketCount, 0);
                assertEquals("GeoTile " + geoTile + " has wrong doc count ", expectedBucketCount, bucketCount);
                GeoPoint geoPoint = (GeoPoint) propertiesKeys[i];
                assertThat(GeoTileUtils.stringEncode(geoPoint.lon(), geoPoint.lat(), precision), equalTo(geoTile));
                assertThat((long) propertiesDocCounts[i], equalTo(bucketCount));
            }
        }
    }

    public void testMultivaluedGeoPointsAggregation() throws Exception {
        for (int precision = 1; precision <= GEOPOINT_MAX_PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("multi_valued_idx")
                .addAggregation(AggregationBuilders.geotileGrid(AGG_NAME).field(GEO_POINT_FIELD_NAME).precision(precision))
                .get();

            assertSearchResponse(response);

            GeoGrid geoGrid = response.getAggregations().get(AGG_NAME);
            for (GeoGrid.Bucket cell : geoGrid.getBuckets()) {
                String geohash = cell.getKeyAsString();

                long bucketCount = cell.getDocCount();
                int expectedBucketCount = multiValuedExpectedDocCountsGeoPoint.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);
            }
        }
    }

    /**
     * Returns a set of buckets for the shape at different precision level. Override this method for different bucket
     * aggregations.
     *
     * @param geometry         {@link Geometry}
     * @param geoShapeDocValue {@link GeoShapeDocValue}
     * @return A {@link Set} of {@link String} which represents the buckets.
     */
    @Override
    protected Set<String> generateBucketsForGeometry(final Geometry geometry, final GeoShapeDocValue geoShapeDocValue) {
        final GeoPoint topLeft = new GeoPoint();
        final GeoPoint bottomRight = new GeoPoint();
        assert geometry != null;
        GeoBoundsHelper.updateBoundsForGeometry(geometry, topLeft, bottomRight);
        final Set<String> geoTiles = new HashSet<>();
        final boolean isIntersectingWithBoundingRectangle = geoShapeDocValue.isIntersectingRectangle(boundingRectangleForGeoShapesAgg);
        for (int precision = MAX_PRECISION_FOR_GEO_SHAPES_AGG_TESTING; precision > 0; precision--) {
            if (precision > MIN_PRECISION_WITHOUT_BB_AGGS && isIntersectingWithBoundingRectangle == false) {
                continue;
            }
            geoTiles.addAll(
                GeoTileUtils.encodeShape(geoShapeDocValue, precision).stream().map(GeoTileUtils::stringEncode).collect(Collectors.toSet())
            );
        }
        return geoTiles;
    }

    protected Set<String> generateBucketsForGeoPoint(final GeoPoint geoPoint) {
        Set<String> buckets = new HashSet<>();
        for (int precision = GEOPOINT_MAX_PRECISION; precision > 0; precision--) {
            final GeoPoint precisedGeoPoint = this.toStoragePrecision(geoPoint);
            final String tile = GeoTileUtils.stringEncode(precisedGeoPoint.getLon(), precisedGeoPoint.getLat(), precision);
            buckets.add(tile);
        }
        return buckets;
    }
}
