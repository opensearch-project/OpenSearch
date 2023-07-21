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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.geo.search.aggregations.bucket;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.geo.GeoBoundingBox;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoShapeDocValue;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoGrid;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.opensearch.geo.search.aggregations.common.GeoBoundsHelper;
import org.opensearch.geo.tests.common.AggregationBuilders;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geometry.utils.Geohash;
import org.opensearch.index.query.GeoBoundingBoxQueryBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.geometry.utils.Geohash.PRECISION;
import static org.opensearch.geometry.utils.Geohash.stringEncode;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class GeoHashGridIT extends AbstractGeoBucketAggregationIntegTest {

    private static final String AGG_NAME = "geohashgrid";

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        Random random = random();
        // Creating a BB for limiting the number buckets generated during aggregation
        boundingRectangleForGeoShapesAgg = getGridAggregationBoundingBox(random);
        expectedDocCountsForSingleGeoPoint = new HashMap<>();
        prepareSingleValueGeoPointIndex(random);
        prepareMultiValuedGeoPointIndex(random);
        prepareGeoShapeIndexForAggregations(random);
    }

    public void testSimple() {
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("idx")
                .addAggregation(AggregationBuilders.geohashGrid(AGG_NAME).field(GEO_POINT_FIELD_NAME).precision(precision))
                .get();

            assertSearchResponse(response);

            GeoGrid geoGrid = response.getAggregations().get(AGG_NAME);
            List<? extends GeoGrid.Bucket> buckets = geoGrid.getBuckets();
            Object[] propertiesKeys = (Object[]) ((InternalAggregation) geoGrid).getProperty("_key");
            Object[] propertiesDocCounts = (Object[]) ((InternalAggregation) geoGrid).getProperty("_count");
            for (int i = 0; i < buckets.size(); i++) {
                GeoGrid.Bucket cell = buckets.get(i);
                String geohash = cell.getKeyAsString();

                long bucketCount = cell.getDocCount();
                int expectedBucketCount = expectedDocCountsForSingleGeoPoint.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);
                GeoPoint geoPoint = (GeoPoint) propertiesKeys[i];
                assertThat(stringEncode(geoPoint.lon(), geoPoint.lat(), precision), equalTo(geohash));
                assertThat((long) propertiesDocCounts[i], equalTo(bucketCount));
            }
        }
    }

    public void testGeoShapes() {
        final GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(boundingRectangleForGeoShapesAgg.getMaxLat(), boundingRectangleForGeoShapesAgg.getMinLon()),
            new GeoPoint(boundingRectangleForGeoShapesAgg.getMinLat(), boundingRectangleForGeoShapesAgg.getMaxLon())
        );
        for (int precision = 1; precision <= MAX_PRECISION_FOR_GEO_SHAPES_AGG_TESTING; precision++) {
            GeoGridAggregationBuilder builder = AggregationBuilders.geohashGrid(AGG_NAME).field(GEO_SHAPE_FIELD_NAME).precision(precision);
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
                final String geohash = cell.getKeyAsString();

                final long bucketCount = cell.getDocCount();
                final int expectedBucketCount = expectedDocsCountForGeoShapes.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);
                final GeoPoint geoPoint = (GeoPoint) propertiesKeys[i];
                assertThat(stringEncode(geoPoint.lon(), geoPoint.lat(), precision), equalTo(geohash));
                assertThat((long) propertiesDocCounts[i], equalTo(bucketCount));
            }
        }
    }

    public void testMultivalued() {
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("multi_valued_idx")
                .addAggregation(AggregationBuilders.geohashGrid(AGG_NAME).field(GEO_POINT_FIELD_NAME).precision(precision))
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

    public void testFiltered() {
        GeoBoundingBoxQueryBuilder bbox = new GeoBoundingBoxQueryBuilder(GEO_POINT_FIELD_NAME);
        bbox.setCorners(smallestGeoHash).queryName("bbox");
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("idx")
                .addAggregation(
                    org.opensearch.search.aggregations.AggregationBuilders.filter("filtered", bbox)
                        .subAggregation(AggregationBuilders.geohashGrid(AGG_NAME).field(GEO_POINT_FIELD_NAME).precision(precision))
                )
                .get();

            assertSearchResponse(response);

            Filter filter = response.getAggregations().get("filtered");

            GeoGrid geoGrid = filter.getAggregations().get(AGG_NAME);
            for (GeoGrid.Bucket cell : geoGrid.getBuckets()) {
                String geohash = cell.getKeyAsString();
                long bucketCount = cell.getDocCount();
                int expectedBucketCount = expectedDocCountsForSingleGeoPoint.get(geohash);
                assertNotSame(bucketCount, 0);
                assertTrue("Buckets must be filtered", geohash.startsWith(smallestGeoHash));
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);

            }
        }
    }

    public void testUnmapped() {
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(AggregationBuilders.geohashGrid(AGG_NAME).field(GEO_POINT_FIELD_NAME).precision(precision))
                .get();

            assertSearchResponse(response);

            GeoGrid geoGrid = response.getAggregations().get(AGG_NAME);
            assertThat(geoGrid.getBuckets().size(), equalTo(0));
        }

    }

    public void testPartiallyUnmapped() {
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(AggregationBuilders.geohashGrid(AGG_NAME).field(GEO_POINT_FIELD_NAME).precision(precision))
                .get();

            assertSearchResponse(response);

            GeoGrid geoGrid = response.getAggregations().get(AGG_NAME);
            for (GeoGrid.Bucket cell : geoGrid.getBuckets()) {
                String geohash = cell.getKeyAsString();

                long bucketCount = cell.getDocCount();
                int expectedBucketCount = expectedDocCountsForSingleGeoPoint.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);
            }
        }
    }

    public void testTopMatch() {
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("idx")
                .addAggregation(
                    AggregationBuilders.geohashGrid(AGG_NAME).field(GEO_POINT_FIELD_NAME).size(1).shardSize(100).precision(precision)
                )
                .get();

            assertSearchResponse(response);

            GeoGrid geoGrid = response.getAggregations().get(AGG_NAME);
            // Check we only have one bucket with the best match for that resolution
            assertThat(geoGrid.getBuckets().size(), equalTo(1));
            for (GeoGrid.Bucket cell : geoGrid.getBuckets()) {
                String geohash = cell.getKeyAsString();
                long bucketCount = cell.getDocCount();
                int expectedBucketCount = 0;
                for (var cursor : expectedDocCountsForSingleGeoPoint.entrySet()) {
                    if (cursor.getKey().length() == precision) {
                        expectedBucketCount = Math.max(expectedBucketCount, cursor.getValue());
                    }
                }
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);
            }
        }
    }

    public void testSizeIsZero() {
        final int size = 0;
        final int shardSize = 10000;
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch("idx")
                .addAggregation(AggregationBuilders.geohashGrid(AGG_NAME).field(GEO_POINT_FIELD_NAME).size(size).shardSize(shardSize))
                .get()
        );
        assertThat(exception.getMessage(), containsString("[size] must be greater than 0. Found [0] in [" + AGG_NAME + "]"));
    }

    public void testShardSizeIsZero() {
        final int size = 100;
        final int shardSize = 0;
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch("idx")
                .addAggregation(AggregationBuilders.geohashGrid(AGG_NAME).field(GEO_POINT_FIELD_NAME).size(size).shardSize(shardSize))
                .get()
        );
        assertThat(exception.getMessage(), containsString("[shardSize] must be greater than 0. Found [0] in [" + AGG_NAME + "]"));
    }

    @Override
    protected Set<String> generateBucketsForGeometry(final Geometry geometry, final GeoShapeDocValue geometryDocValue) {
        final GeoPoint topLeft = new GeoPoint();
        final GeoPoint bottomRight = new GeoPoint();
        assert geometry != null;
        GeoBoundsHelper.updateBoundsForGeometry(geometry, topLeft, bottomRight);
        final Set<String> geoHashes = new HashSet<>();
        final boolean isIntersectingWithBoundingRectangle = geometryDocValue.isIntersectingRectangle(boundingRectangleForGeoShapesAgg);
        for (int precision = MAX_PRECISION_FOR_GEO_SHAPES_AGG_TESTING; precision > 0; precision--) {
            if (precision > MIN_PRECISION_WITHOUT_BB_AGGS && isIntersectingWithBoundingRectangle == false) {
                continue;
            }
            final GeoPoint topRight = new GeoPoint(topLeft.getLat(), bottomRight.getLon());
            String currentGeoHash = Geohash.stringEncode(topLeft.getLon(), topLeft.getLat(), precision);
            String startingRowGeoHash = currentGeoHash;
            String endGeoHashForCurrentRow = Geohash.stringEncode(topRight.getLon(), topRight.getLat(), precision);
            String terminatingGeoHash = Geohash.stringEncode(bottomRight.getLon(), bottomRight.getLat(), precision);
            while (true) {
                final Rectangle currentRectangle = Geohash.toBoundingBox(currentGeoHash);
                if (geometryDocValue.isIntersectingRectangle(currentRectangle)) {
                    geoHashes.add(currentGeoHash);
                }
                assert currentGeoHash != null;
                if (currentGeoHash.equals(terminatingGeoHash)) {
                    break;
                }
                if (currentGeoHash.equals(endGeoHashForCurrentRow)) {
                    // move in south direction
                    currentGeoHash = Geohash.getNeighbor(startingRowGeoHash, precision, 0, -1);
                    startingRowGeoHash = currentGeoHash;
                    endGeoHashForCurrentRow = Geohash.getNeighbor(endGeoHashForCurrentRow, precision, 0, -1);
                } else {
                    // move in East direction
                    currentGeoHash = Geohash.getNeighbor(currentGeoHash, precision, 1, 0);
                }
            }
        }
        return geoHashes;
    }

    @Override
    protected Set<String> generateBucketsForGeoPoint(final GeoPoint geoPoint) {
        Set<String> buckets = new HashSet<>();
        for (int precision = PRECISION; precision > 0; precision--) {
            final String hash = Geohash.stringEncode(geoPoint.getLon(), geoPoint.getLat(), precision);
            if ((smallestGeoHash == null) || (hash.length() < smallestGeoHash.length())) {
                smallestGeoHash = hash;
            }
            buckets.add(hash);
        }
        return buckets;
    }

}
