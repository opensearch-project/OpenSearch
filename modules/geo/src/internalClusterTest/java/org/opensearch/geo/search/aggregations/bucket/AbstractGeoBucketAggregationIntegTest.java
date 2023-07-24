/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.bucket;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.opensearch.Version;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoShapeDocValue;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.geo.GeoModulePluginIntegTestCase;
import org.opensearch.geo.tests.common.RandomGeoGenerator;
import org.opensearch.geo.tests.common.RandomGeoGeometryGenerator;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.Rectangle;
import org.opensearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * This is the base class for all the Bucket Aggregation related integration tests. Use this class to add common
 * methods which can be used across different bucket aggregations. If there is any common code that can be used
 * across other integration test too then this is not the class. Use {@link GeoModulePluginIntegTestCase}
 */
public abstract class AbstractGeoBucketAggregationIntegTest extends GeoModulePluginIntegTestCase {

    protected static final int MAX_PRECISION_FOR_GEO_SHAPES_AGG_TESTING = 2;

    protected static final int MIN_PRECISION_WITHOUT_BB_AGGS = 2;

    protected static final int NUM_DOCS = 100;

    protected static final String GEO_SHAPE_INDEX_NAME = "geoshape_index";

    protected static Rectangle boundingRectangleForGeoShapesAgg;

    protected static Map<String, Integer> expectedDocsCountForGeoShapes;

    protected static Map<String, Integer> expectedDocCountsForSingleGeoPoint;

    protected static Map<String, Integer> multiValuedExpectedDocCountsGeoPoint;

    protected static final String GEO_SHAPE_FIELD_NAME = "location_geo_shape";

    protected static final String GEO_POINT_FIELD_NAME = "location";

    protected static final String KEYWORD_FIELD_NAME = "city";

    protected static String smallestGeoHash = null;

    protected final Version version = VersionUtils.randomIndexCompatibleVersion(random());

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    /**
     * Prepares a GeoShape index for testing the GeoShape bucket aggregations. Different bucket aggregations can use
     * different techniques for creating buckets. Override the method
     * {@link AbstractGeoBucketAggregationIntegTest#generateBucketsForGeometry} in the test class for creating the
     * buckets which will then be used for verifications.
     *
     * @param random {@link Random}
     * @throws Exception thrown during index creation.
     */
    protected void prepareGeoShapeIndexForAggregations(final Random random) throws Exception {
        expectedDocsCountForGeoShapes = new HashMap<>();
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        final List<IndexRequestBuilder> geoshapes = new ArrayList<>();
        assertAcked(prepareCreate(GEO_SHAPE_INDEX_NAME).setSettings(settings).setMapping(GEO_SHAPE_FIELD_NAME, "type" + "=geo_shape"));
        boolean isShapeIntersectingBB = false;
        for (int i = 0; i < NUM_DOCS;) {
            final Geometry geometry = RandomGeoGeometryGenerator.randomGeometry(random);
            final GeoShapeDocValue geometryDocValue = GeoShapeDocValue.createGeometryDocValue(geometry);
            // make sure that there is 1 shape is intersecting with the bounding box
            if (!isShapeIntersectingBB) {
                isShapeIntersectingBB = geometryDocValue.isIntersectingRectangle(boundingRectangleForGeoShapesAgg);
                if (!isShapeIntersectingBB && i == NUM_DOCS - 1) {
                    continue;
                }
            }

            i++;
            final Set<String> values = generateBucketsForGeometry(geometry, geometryDocValue);
            geoshapes.add(indexGeoShape(GEO_SHAPE_INDEX_NAME, geometry));
            for (final String hash : values) {
                expectedDocsCountForGeoShapes.put(hash, expectedDocsCountForGeoShapes.getOrDefault(hash, 0) + 1);
            }
        }
        indexRandom(true, geoshapes);
        ensureGreen(GEO_SHAPE_INDEX_NAME);
    }

    /**
     * Returns a set of buckets for the shape at different precision level. Override this method for different bucket
     * aggregations.
     *
     * @param geometry         {@link Geometry}
     * @param geoShapeDocValue {@link GeoShapeDocValue}
     * @return A {@link Set} of {@link String} which represents the buckets.
     */
    protected abstract Set<String> generateBucketsForGeometry(final Geometry geometry, final GeoShapeDocValue geoShapeDocValue);

    /**
     * Prepares a GeoPoint index for testing the GeoPoint bucket aggregations. Different bucket aggregations can use
     * different techniques for creating buckets. Override the method
     * {@link AbstractGeoBucketAggregationIntegTest#generateBucketsForGeoPoint} in the test class for creating the
     * buckets which will then be used for verifications.
     *
     * @param random {@link Random}
     * @throws Exception thrown during index creation.
     */
    protected void prepareSingleValueGeoPointIndex(final Random random) throws Exception {
        expectedDocCountsForSingleGeoPoint = new HashMap<>();
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
        for (int i = 0; i < NUM_DOCS; i++) {
            // generate random point
            final GeoPoint geoPoint = RandomGeoGenerator.randomPoint(random);
            cities.add(indexGeoPoint("idx", geoPoint.toString(), geoPoint.getLat() + ", " + geoPoint.getLon()));
            final Set<String> buckets = generateBucketsForGeoPoint(geoPoint);
            for (final String bucket : buckets) {
                expectedDocCountsForSingleGeoPoint.put(bucket, expectedDocCountsForSingleGeoPoint.getOrDefault(bucket, 0) + 1);
            }
        }
        indexRandom(true, cities);
        ensureGreen("idx_unmapped", "idx");
    }

    protected void prepareMultiValuedGeoPointIndex(final Random random) throws Exception {
        multiValuedExpectedDocCountsGeoPoint = new HashMap<>();
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        final List<IndexRequestBuilder> cities = new ArrayList<>();
        assertAcked(
            prepareCreate("multi_valued_idx").setSettings(settings)
                .setMapping(GEO_POINT_FIELD_NAME, "type=geo_point", KEYWORD_FIELD_NAME, "type=keyword")
        );
        for (int i = 0; i < NUM_DOCS; i++) {
            final int numPoints = random.nextInt(4);
            final List<String> points = new ArrayList<>();
            final Set<String> buckets = new HashSet<>();
            for (int j = 0; j < numPoints; ++j) {
                // generate random point
                final GeoPoint geoPoint = RandomGeoGenerator.randomPoint(random);
                points.add(geoPoint.getLat() + "," + geoPoint.getLon());
                buckets.addAll(generateBucketsForGeoPoint(geoPoint));
            }
            cities.add(indexGeoPoints("multi_valued_idx", Integer.toString(i), points));
            for (final String bucket : buckets) {
                multiValuedExpectedDocCountsGeoPoint.put(bucket, multiValuedExpectedDocCountsGeoPoint.getOrDefault(bucket, 0) + 1);
            }
        }
        indexRandom(true, cities);
        ensureGreen("multi_valued_idx");
    }

    /**
     * Returns a set of buckets for the GeoPoint at different precision level. Override this method for different bucket
     * aggregations.
     *
     * @param geoPoint {@link GeoPoint}
     * @return A {@link Set} of {@link String} which represents the buckets.
     */
    protected abstract Set<String> generateBucketsForGeoPoint(final GeoPoint geoPoint);

    /**
     * Indexes a GeoShape in the provided index.
     * @param index {@link String} index name
     * @param geometry {@link Geometry} the Geometry to be indexed
     * @return {@link IndexRequestBuilder}
     * @throws Exception thrown during creation of {@link IndexRequestBuilder}
     */
    protected IndexRequestBuilder indexGeoShape(final String index, final Geometry geometry) throws Exception {
        XContentBuilder source = jsonBuilder().startObject();
        source = source.field(GEO_SHAPE_FIELD_NAME, WKT.toWKT(geometry));
        source = source.endObject();
        return client().prepareIndex(index).setSource(source);
    }

    /**
     * Indexes a {@link List} of {@link GeoPoint}s in the provided Index name.
     * @param index {@link String} index name
     * @param name {@link String} value for the string field in index
     * @param latLon {@link List} of {@link String} representing the String representation of GeoPoint
     * @return {@link IndexRequestBuilder}
     * @throws Exception thrown during indexing.
     */
    protected IndexRequestBuilder indexGeoPoints(final String index, final String name, final List<String> latLon) throws Exception {
        XContentBuilder source = jsonBuilder().startObject().field(KEYWORD_FIELD_NAME, name);
        if (latLon != null) {
            source = source.field(GEO_POINT_FIELD_NAME, latLon);
        }
        source = source.endObject();
        return client().prepareIndex(index).setSource(source);
    }

    /**
     * Indexes a {@link GeoPoint} in the provided Index name.
     * @param index {@link String} index name
     * @param name {@link String} value for the string field in index
     * @param latLon {@link String} representing the String representation of GeoPoint
     * @return {@link IndexRequestBuilder}
     * @throws Exception thrown during indexing.
     */
    protected IndexRequestBuilder indexGeoPoint(final String index, final String name, final String latLon) throws Exception {
        return indexGeoPoints(index, name, List.of(latLon));
    }

    /**
     * Generates a Bounding Box of a fixed radius that can be used for shapes aggregations to reduce the size of
     * aggregation results.
     * @param random {@link Random}
     * @return {@link Rectangle}
     */
    protected Rectangle getGridAggregationBoundingBox(final Random random) {
        final double radius = getRadiusOfBoundingBox();
        assertTrue("The radius of Bounding Box is less than or equal to 0", radius > 0);
        return RandomGeoGeometryGenerator.randomRectangle(random, radius);
    }

    /**
     * Returns a radius for the Bounding box. Test classes can override this method to change the radius of BBox for
     * the test cases. If we increase this value, it will lead to creation of a lot of buckets that can lead of
     * IndexOutOfBoundsExceptions.
     * @return double
     */
    protected double getRadiusOfBoundingBox() {
        return 5.0;
    }

    /**
     * Encode and Decode the {@link GeoPoint} to get a {@link GeoPoint} which has the exact precision which is being
     * stored.
     * @param geoPoint {@link GeoPoint}
     * @return {@link GeoPoint}
     */
    protected GeoPoint toStoragePrecision(final GeoPoint geoPoint) {
        return new GeoPoint(
            GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(geoPoint.getLat())),
            GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(geoPoint.getLon()))
        );
    }

}
