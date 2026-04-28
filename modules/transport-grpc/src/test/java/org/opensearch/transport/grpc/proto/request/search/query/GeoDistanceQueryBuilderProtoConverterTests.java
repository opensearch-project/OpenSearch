/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.GeoDistanceQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.GeoDistanceQuery;
import org.opensearch.protobufs.GeoLocation;
import org.opensearch.protobufs.LatLonGeoLocation;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;

public class GeoDistanceQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private GeoDistanceQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new GeoDistanceQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        assertEquals(
            "Converter should handle GEO_DISTANCE case",
            QueryContainer.QueryContainerCase.GEO_DISTANCE,
            converter.getHandledQueryCase()
        );
    }

    public void testFromProto() {
        LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7).setLon(-74.0).build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("10km")
            .putLocation("location", geoLocation)
            .setBoost(2.0f)
            .setXName("test_geo_distance_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setGeoDistance(geoDistanceQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a GeoDistanceQueryBuilder", queryBuilder instanceof GeoDistanceQueryBuilder);
        GeoDistanceQueryBuilder geoDistanceQueryBuilder = (GeoDistanceQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "location", geoDistanceQueryBuilder.fieldName());
        assertEquals("Latitude should match", 40.7, geoDistanceQueryBuilder.point().getLat(), 0.001);
        assertEquals("Longitude should match", -74.0, geoDistanceQueryBuilder.point().getLon(), 0.001);
        // Distance is returned in meters, so 10km = 10000m
        assertEquals("Distance should match", 10000.0, geoDistanceQueryBuilder.distance(), 0.001);
        assertEquals("Boost should match", 2.0f, geoDistanceQueryBuilder.boost(), 0.001f);
        assertEquals("Query name should match", "test_geo_distance_query", geoDistanceQueryBuilder.queryName());
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        assertTrue(
            "Exception message should mention 'QueryContainer does not contain a GeoDistance query'",
            exception.getMessage().contains("QueryContainer does not contain a GeoDistance query")
        );
    }

    public void testFromProtoWithInvalidContainer() {
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        assertTrue(
            "Exception message should mention 'QueryContainer does not contain a GeoDistance query'",
            exception.getMessage().contains("QueryContainer does not contain a GeoDistance query")
        );
    }
}
