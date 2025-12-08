/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.GeoBoundingBoxQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.CoordsGeoBounds;
import org.opensearch.protobufs.GeoBoundingBoxQuery;
import org.opensearch.protobufs.GeoBounds;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;

public class GeoBoundingBoxQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private GeoBoundingBoxQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new GeoBoundingBoxQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        // Test that the converter returns the correct QueryContainerCase
        assertEquals(
            "Converter should handle GEO_BOUNDING_BOX case",
            QueryContainer.QueryContainerCase.GEO_BOUNDING_BOX,
            converter.getHandledQueryCase()
        );
    }

    public void testFromProto() {
        // Create a QueryContainer with GeoBoundingBoxQuery
        CoordsGeoBounds coords = CoordsGeoBounds.newBuilder().setTop(40.7).setLeft(-74.0).setBottom(40.6).setRight(-73.9).build();

        GeoBounds geoBounds = GeoBounds.newBuilder().setCoords(coords).build();

        GeoBoundingBoxQuery geoBoundingBoxQuery = GeoBoundingBoxQuery.newBuilder()
            .putBoundingBox("location", geoBounds)
            .setBoost(1.5f)
            .setXName("test_geo_bbox_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setGeoBoundingBox(geoBoundingBoxQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a GeoBoundingBoxQueryBuilder", queryBuilder instanceof GeoBoundingBoxQueryBuilder);
        GeoBoundingBoxQueryBuilder geoBoundingBoxQueryBuilder = (GeoBoundingBoxQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "location", geoBoundingBoxQueryBuilder.fieldName());
        assertEquals("Top should match", 40.7, geoBoundingBoxQueryBuilder.topLeft().getLat(), 0.001);
        assertEquals("Left should match", -74.0, geoBoundingBoxQueryBuilder.topLeft().getLon(), 0.001);
        assertEquals("Bottom should match", 40.6, geoBoundingBoxQueryBuilder.bottomRight().getLat(), 0.001);
        assertEquals("Right should match", -73.9, geoBoundingBoxQueryBuilder.bottomRight().getLon(), 0.001);
        assertEquals("Boost should match", 1.5f, geoBoundingBoxQueryBuilder.boost(), 0.001f);
        assertEquals("Query name should match", "test_geo_bbox_query", geoBoundingBoxQueryBuilder.queryName());
    }

    public void testFromProtoWithNullContainer() {
        // Test that the converter throws an exception for null input
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'QueryContainer must contain a GeoBoundingBoxQuery'",
            exception.getMessage().contains("QueryContainer must contain a GeoBoundingBoxQuery")
        );
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'QueryContainer must contain a GeoBoundingBoxQuery'",
            exception.getMessage().contains("QueryContainer must contain a GeoBoundingBoxQuery")
        );
    }
}
