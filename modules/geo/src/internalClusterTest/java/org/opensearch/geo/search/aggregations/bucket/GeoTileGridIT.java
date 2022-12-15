/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.bucket;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import org.hamcrest.MatcherAssert;
import org.opensearch.Version;
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
import org.opensearch.geo.tests.common.RandomGeoGenerator;
import org.opensearch.geo.tests.common.RandomGeoGeometryGenerator;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.Rectangle;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.GeoTileUtils;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class GeoTileGridIT extends AbstractBucketAggregationIntegTest {

    private static final int GEOPOINT_MAX_PRECISION = 17;

    private static final String AGG_NAME = "geotilegrid";

    private static final String GEO_SHAPE_INDEX_NAME = "geoshape_index";

    private static final String GEO_SHAPE_FIELD_NAME = "location_geo_shape";

    private final Version version = VersionUtils.randomIndexCompatibleVersion(random());

    private static final Rectangle BOUNDING_RECTANGLE_FOR_GEO_SHAPES_AGG = new Rectangle(-4.1, 20.9, 21.9, -3.1);

    private static ObjectIntMap<String> expectedDocCountsForGeoshapeGeoTiles;

    private static ObjectIntMap<String> expectedDocCountsForGeoTiles;

    private static ObjectIntHashMap<String> multiValuedExpectedDocCountsForGeoTiles;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        final Random random = random();
        prepareSingleValueGeoPointIndex(random);
        prepareMultiValuedGeoPointIndex(random);
        prepareGeoShapeIndex(random);
        ensureSearchable();
    }

    public void testGeoShapes() {
        final GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(BOUNDING_RECTANGLE_FOR_GEO_SHAPES_AGG.getMaxLat(), BOUNDING_RECTANGLE_FOR_GEO_SHAPES_AGG.getMinLon()),
            new GeoPoint(BOUNDING_RECTANGLE_FOR_GEO_SHAPES_AGG.getMinLat(), BOUNDING_RECTANGLE_FOR_GEO_SHAPES_AGG.getMaxLon())
        );
        for (int precision = 1; precision <= MAX_PRECISION_FOR_GEO_SHAPES_AGG_TESTING; precision++) {
            GeoGridAggregationBuilder builder = AggregationBuilders.geotileGrid(AGG_NAME).field(GEO_SHAPE_FIELD_NAME).precision(precision);
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
                final String geoTile = cell.getKeyAsString();

                final long bucketCount = cell.getDocCount();
                final int expectedBucketCount = expectedDocCountsForGeoshapeGeoTiles.get(geoTile);
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
                int expectedBucketCount = expectedDocCountsForGeoTiles.get(geoTile);
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
                int expectedBucketCount = multiValuedExpectedDocCountsForGeoTiles.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);
            }
        }
    }

    private void prepareGeoShapeIndex(final Random random) throws Exception {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        final List<IndexRequestBuilder> geoshapes = new ArrayList<>();
        assertAcked(prepareCreate(GEO_SHAPE_INDEX_NAME).setSettings(settings).setMapping(GEO_SHAPE_FIELD_NAME, "type" + "=geo_shape"));
        expectedDocCountsForGeoshapeGeoTiles = new ObjectIntHashMap<>();
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
            final Set<String> geoTiles = new HashSet<>();
            for (int precision = MAX_PRECISION_FOR_GEO_SHAPES_AGG_TESTING; precision > 0; precision--) {
                geoTiles.addAll(
                    GeoTileUtils.encodeShape(geometryDocValue, precision)
                        .stream()
                        .map(GeoTileUtils::stringEncode)
                        .collect(Collectors.toSet())
                );
            }

            geoshapes.add(indexGeoShape(GEO_SHAPE_INDEX_NAME, geometry));
            for (final String hash : geoTiles) {
                expectedDocCountsForGeoshapeGeoTiles.put(hash, expectedDocCountsForGeoshapeGeoTiles.getOrDefault(hash, 0) + 1);
            }
        }
        indexRandom(true, geoshapes);
        ensureGreen(GEO_SHAPE_INDEX_NAME);
    }

    private void prepareSingleValueGeoPointIndex(final Random random) throws Exception {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        assertAcked(
            prepareCreate("idx").setSettings(settings)
                .setMapping(GEO_POINT_FIELD_NAME, "type=geo_point", KEYWORD_FIELD_NAME, "type=keyword")
        );
        final List<IndexRequestBuilder> cities = new ArrayList<>();
        expectedDocCountsForGeoTiles = new ObjectIntHashMap<>();
        for (int i = 0; i < NUM_DOCS; i++) {
            // generate random point
            final GeoPoint geoPoint = RandomGeoGenerator.randomPoint(random);
            final String cityName = GeoTileUtils.stringEncode(geoPoint.getLon(), geoPoint.getLat(), GEOPOINT_MAX_PRECISION);
            // Index at the highest resolution
            cities.add(indexCity("idx", cityName, geoPoint.getLat() + ", " + geoPoint.getLon()));
            // Update expected doc counts for all resolutions..
            for (int precision = GEOPOINT_MAX_PRECISION; precision > 0; precision--) {
                String tile = GeoTileUtils.stringEncode(geoPoint.getLon(), geoPoint.getLat(), precision);
                expectedDocCountsForGeoTiles.put(tile, expectedDocCountsForGeoTiles.getOrDefault(tile, 0) + 1);
            }
        }
        indexRandom(true, cities);
        ensureGreen("idx");
    }

    private void prepareMultiValuedGeoPointIndex(final Random random) throws Exception {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        final List<IndexRequestBuilder> cities = new ArrayList<>();
        assertAcked(
            prepareCreate("multi_valued_idx").setSettings(settings)
                .setMapping(GEO_POINT_FIELD_NAME, "type=geo_point", KEYWORD_FIELD_NAME, "type=keyword")
        );
        multiValuedExpectedDocCountsForGeoTiles = new ObjectIntHashMap<>(NUM_DOCS * 2);
        for (int i = 0; i < NUM_DOCS; i++) {
            final int numPoints = random.nextInt(4);
            final List<String> points = new ArrayList<>();
            final Set<String> geoTiles = new HashSet<>();
            for (int j = 0; j < numPoints; ++j) {
                // generate random point
                final GeoPoint geoPoint = RandomGeoGenerator.randomPoint(random);
                points.add(geoPoint.getLat() + "," + geoPoint.getLon());
                // Update expected doc counts for all resolutions..
                for (int precision = GEOPOINT_MAX_PRECISION; precision > 0; precision--) {
                    final String tile = GeoTileUtils.stringEncode(geoPoint.getLon(), geoPoint.getLat(), precision);
                    geoTiles.add(tile);
                }
            }
            cities.add(indexCity("multi_valued_idx", Integer.toString(i), points));
            for (final String hash : geoTiles) {
                multiValuedExpectedDocCountsForGeoTiles.put(hash, multiValuedExpectedDocCountsForGeoTiles.getOrDefault(hash, 0) + 1);
            }
        }
        indexRandom(true, cities);
        ensureGreen("multi_valued_idx");
    }
}
