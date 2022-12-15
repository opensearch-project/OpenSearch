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

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.geo.GeoBoundingBox;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoShapeDocValue;
import org.opensearch.common.settings.Settings;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoGrid;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.opensearch.geo.search.aggregations.common.GeoBoundsHelper;
import org.opensearch.geo.tests.common.AggregationBuilders;
import org.opensearch.geo.tests.common.RandomGeoGeometryGenerator;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geometry.utils.Geohash;
import org.opensearch.index.query.GeoBoundingBoxQueryBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.geometry.utils.Geohash.PRECISION;
import static org.opensearch.geometry.utils.Geohash.stringEncode;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class GeoHashGridIT extends AbstractBucketAggregationIntegTest {

    private static ObjectIntMap<String> expectedDocCountsForGeohash;

    private static ObjectIntMap<String> multiValuedExpectedDocCountsForGeohash;

    private static ObjectIntMap<String> expectedDocCountsForGeoshapeGeohash;

    private static String smallestGeoHash = null;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        Random random = random();
        prepareSingleValueGeoPointIndex(random);
        prepareMultiValuedGeoPointIndex(random);
        prepareGeoShapeIndex(random);
    }

    public void testSimple() throws Exception {
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
                int expectedBucketCount = expectedDocCountsForGeohash.get(geohash);
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
            new GeoPoint(BOUNDING_RECTANGLE_FOR_GEO_SHAPES_AGG.getMaxLat(), BOUNDING_RECTANGLE_FOR_GEO_SHAPES_AGG.getMinLon()),
            new GeoPoint(BOUNDING_RECTANGLE_FOR_GEO_SHAPES_AGG.getMinLat(), BOUNDING_RECTANGLE_FOR_GEO_SHAPES_AGG.getMaxLon())
        );
        for (int precision = 1; precision <= MAX_PRECISION_FOR_GEO_SHAPES_AGG_TESTING; precision++) {
            GeoGridAggregationBuilder builder = AggregationBuilders.geohashGrid(AGG_NAME).field(GEO_SHAPE_FIELD_NAME).precision(precision);
            // This makes sure that for only higher precision we are providing the GeoBounding Box. This also ensures
            // that we are able to test both bounded and unbounded aggregations
            if (precision > 2) {
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
                final int expectedBucketCount = expectedDocCountsForGeoshapeGeohash.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);
                final GeoPoint geoPoint = (GeoPoint) propertiesKeys[i];
                assertThat(stringEncode(geoPoint.lon(), geoPoint.lat(), precision), equalTo(geohash));
                assertThat((long) propertiesDocCounts[i], equalTo(bucketCount));
            }
        }
    }

    public void testMultivalued() throws Exception {
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("multi_valued_idx")
                .addAggregation(AggregationBuilders.geohashGrid(AGG_NAME).field(GEO_POINT_FIELD_NAME).precision(precision))
                .get();
            assertSearchResponse(response);
            GeoGrid geoGrid = response.getAggregations().get(AGG_NAME);
            for (GeoGrid.Bucket cell : geoGrid.getBuckets()) {
                String geohash = cell.getKeyAsString();

                long bucketCount = cell.getDocCount();
                int expectedBucketCount = multiValuedExpectedDocCountsForGeohash.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);
            }
        }
    }

    public void testFiltered() throws Exception {
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
                int expectedBucketCount = expectedDocCountsForGeohash.get(geohash);
                assertNotSame(bucketCount, 0);
                assertTrue("Buckets must be filtered", geohash.startsWith(smallestGeoHash));
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);

            }
        }
    }

    public void testUnmapped() throws Exception {
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(AggregationBuilders.geohashGrid(AGG_NAME).field(GEO_POINT_FIELD_NAME).precision(precision))
                .get();

            assertSearchResponse(response);

            GeoGrid geoGrid = response.getAggregations().get(AGG_NAME);
            assertThat(geoGrid.getBuckets().size(), equalTo(0));
        }

    }

    public void testPartiallyUnmapped() throws Exception {
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(AggregationBuilders.geohashGrid(AGG_NAME).field(GEO_POINT_FIELD_NAME).precision(precision))
                .get();

            assertSearchResponse(response);

            GeoGrid geoGrid = response.getAggregations().get(AGG_NAME);
            for (GeoGrid.Bucket cell : geoGrid.getBuckets()) {
                String geohash = cell.getKeyAsString();

                long bucketCount = cell.getDocCount();
                int expectedBucketCount = expectedDocCountsForGeohash.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);
            }
        }
    }

    public void testTopMatch() throws Exception {
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
                for (ObjectIntCursor<String> cursor : expectedDocCountsForGeohash) {
                    if (cursor.key.length() == precision) {
                        expectedBucketCount = Math.max(expectedBucketCount, cursor.value);
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

    private void prepareSingleValueGeoPointIndex(final Random random) throws Exception {
        createIndex("idx_unmapped");
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put("index.number_of_shards", 4)
            .put("index.number_of_replicas", 0)
            .build();
        assertAcked(
            prepareCreate("idx").setSettings(settings)
                .setMapping(GEO_POINT_FIELD_NAME, "type=geo_point", KEYWORD_FIELD_NAME, "type=keyword")
        );
        final List<IndexRequestBuilder> cities = new ArrayList<>();
        expectedDocCountsForGeohash = new ObjectIntHashMap<>(NUM_DOCS * 2);
        for (int i = 0; i < NUM_DOCS; i++) {
            // generate random point
            double lat = (180d * random.nextDouble()) - 90d;
            double lng = (360d * random.nextDouble()) - 180d;
            String randomGeoHash = stringEncode(lng, lat, PRECISION);
            // Index at the highest resolution
            cities.add(indexCity("idx", randomGeoHash, lat + ", " + lng));
            expectedDocCountsForGeohash.put(randomGeoHash, expectedDocCountsForGeohash.getOrDefault(randomGeoHash, 0) + 1);
            // Update expected doc counts for all resolutions..
            for (int precision = PRECISION - 1; precision > 0; precision--) {
                String hash = stringEncode(lng, lat, precision);
                if ((smallestGeoHash == null) || (hash.length() < smallestGeoHash.length())) {
                    smallestGeoHash = hash;
                }
                expectedDocCountsForGeohash.put(hash, expectedDocCountsForGeohash.getOrDefault(hash, 0) + 1);
            }
        }
        indexRandom(true, cities);
        ensureGreen("idx_unmapped", "idx");
    }

    private void prepareMultiValuedGeoPointIndex(final Random random) throws Exception {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        final List<IndexRequestBuilder> cities = new ArrayList<>();
        assertAcked(
            prepareCreate("multi_valued_idx").setSettings(settings)
                .setMapping(GEO_POINT_FIELD_NAME, "type=geo_point", KEYWORD_FIELD_NAME, "type=keyword")
        );
        multiValuedExpectedDocCountsForGeohash = new ObjectIntHashMap<>(NUM_DOCS * 2);
        for (int i = 0; i < NUM_DOCS; i++) {
            final int numPoints = random.nextInt(4);
            final List<String> points = new ArrayList<>();
            final Set<String> geoHashes = new HashSet<>();
            for (int j = 0; j < numPoints; ++j) {
                final double lat = (180d * random.nextDouble()) - 90d;
                final double lng = (360d * random.nextDouble()) - 180d;
                points.add(lat + "," + lng);
                // Update expected doc counts for all resolutions..
                for (int precision = PRECISION; precision > 0; precision--) {
                    final String geoHash = stringEncode(lng, lat, precision);
                    geoHashes.add(geoHash);
                }
            }
            cities.add(indexCity("multi_valued_idx", Integer.toString(i), points));
            for (final String hash : geoHashes) {
                multiValuedExpectedDocCountsForGeohash.put(hash, multiValuedExpectedDocCountsForGeohash.getOrDefault(hash, 0) + 1);
            }
        }
        indexRandom(true, cities);
        ensureGreen("multi_valued_idx");
    }

    private void prepareGeoShapeIndex(final Random random) throws Exception {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();

        final List<IndexRequestBuilder> geoshapes = new ArrayList<>();
        assertAcked(prepareCreate(GEO_SHAPE_INDEX_NAME).setSettings(settings).setMapping(GEO_SHAPE_FIELD_NAME, "type" + "=geo_shape"));
        expectedDocCountsForGeoshapeGeohash = new ObjectIntHashMap<>();
        boolean isShapeIntersectingBB = false;
        for (int i = 0; i < NUM_DOCS;) {
            final Geometry geometry = RandomGeoGeometryGenerator.randomGeometry(random);
            final GeoShapeDocValue geometryDocValue = GeoShapeDocValue.createGeometryDocValue(geometry);
            // make sure that there is 1 shape is intersecting with the bounding box
            if (!isShapeIntersectingBB) {
                isShapeIntersectingBB = geometryDocValue.isIntersectingRectangle(BOUNDING_RECTANGLE_FOR_GEO_SHAPES_AGG);
                if (!isShapeIntersectingBB && i == NUM_DOCS - 1) {
                    continue;
                }
            }
            i++;
            final GeoPoint topLeft = new GeoPoint();
            final GeoPoint bottomRight = new GeoPoint();
            assert geometry != null;
            GeoBoundsHelper.updateBoundsForGeometry(geometry, topLeft, bottomRight);
            final Set<String> geoHashes = new HashSet<>();
            for (int precision = MAX_PRECISION_FOR_GEO_SHAPES_AGG_TESTING; precision > 0; precision--) {
                if (precision > 2 && !geometryDocValue.isIntersectingRectangle(BOUNDING_RECTANGLE_FOR_GEO_SHAPES_AGG)) {
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

            geoshapes.add(indexGeoShape(GEO_SHAPE_INDEX_NAME, geometry));
            for (final String hash : geoHashes) {
                expectedDocCountsForGeoshapeGeohash.put(hash, expectedDocCountsForGeoshapeGeohash.getOrDefault(hash, 0) + 1);
            }
        }
        indexRandom(true, geoshapes);
        ensureGreen(GEO_SHAPE_INDEX_NAME);
    }

}
