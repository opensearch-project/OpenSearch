/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.CoordsGeoBounds;
import org.opensearch.protobufs.DoubleArray;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.GeoBoundingBoxQuery;
import org.opensearch.protobufs.GeoBounds;
import org.opensearch.protobufs.GeoDistanceQuery;
import org.opensearch.protobufs.GeoLocation;
import org.opensearch.protobufs.LatLonGeoLocation;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.MatchPhraseQuery;
import org.opensearch.protobufs.MinimumShouldMatch;
import org.opensearch.protobufs.MultiMatchQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.protobufs.TextQueryType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter;

/**
 * Test class for QueryBuilderProtoConverterRegistry to verify the map-based optimization.
 */
public class QueryBuilderProtoConverterRegistryTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistryImpl registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testMatchAllQueryConversion() {
        // Create a MatchAll query container
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a MatchAllQueryBuilder",
            "org.opensearch.index.query.MatchAllQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testTermQueryConversion() {
        // Create a Term query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setTerm(
                TermQuery.newBuilder().setField("test_field").setValue(FieldValue.newBuilder().setString("test_value").build()).build()
            )
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a TermQueryBuilder", "org.opensearch.index.query.TermQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testNullQueryContainer() {
        expectThrows(IllegalArgumentException.class, () -> registry.fromProto(null));
    }

    public void testUnsupportedQueryType() {
        // Create an empty query container (no query type set)
        QueryContainer queryContainer = QueryContainer.newBuilder().build();
        expectThrows(IllegalArgumentException.class, () -> registry.fromProto(queryContainer));
    }

    public void testConverterRegistration() {
        // Create a custom converter for testing
        QueryBuilderProtoConverter customConverter = new QueryBuilderProtoConverter() {
            @Override
            public QueryContainer.QueryContainerCase getHandledQueryCase() {
                return QueryContainer.QueryContainerCase.MATCH_ALL;
            }

            @Override
            public QueryBuilder fromProto(QueryContainer queryContainer) {
                // Return a mock QueryBuilder for testing
                return new org.opensearch.index.query.MatchAllQueryBuilder();
            }
        };

        // Register the custom converter
        registry.registerConverter(customConverter);

        // Test that it works
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build();

        QueryBuilder result = registry.fromProto(queryContainer);
        assertNotNull("Result should not be null", result);
    }

    public void testNullConverter() {
        expectThrows(IllegalArgumentException.class, () -> registry.registerConverter(null));
    }

    public void testNullHandledQueryCase() {
        // Create a custom converter that returns null for getHandledQueryCase
        QueryBuilderProtoConverter customConverter = new QueryBuilderProtoConverter() {
            @Override
            public QueryContainer.QueryContainerCase getHandledQueryCase() {
                return null;
            }

            @Override
            public QueryBuilder fromProto(QueryContainer queryContainer) {
                return new org.opensearch.index.query.MatchAllQueryBuilder();
            }
        };

        expectThrows(IllegalArgumentException.class, () -> registry.registerConverter(customConverter));
    }

    public void testNotSetHandledQueryCase() {
        // Create a custom converter that returns QUERYCONTAINER_NOT_SET for getHandledQueryCase
        QueryBuilderProtoConverter customConverter = new QueryBuilderProtoConverter() {
            @Override
            public QueryContainer.QueryContainerCase getHandledQueryCase() {
                return QueryContainer.QueryContainerCase.QUERYCONTAINER_NOT_SET;
            }

            @Override
            public QueryBuilder fromProto(QueryContainer queryContainer) {
                return new org.opensearch.index.query.MatchAllQueryBuilder();
            }
        };

        expectThrows(IllegalArgumentException.class, () -> registry.registerConverter(customConverter));
    }

    public void testGeoDistanceQueryConversion() {
        // Create a GeoDistance query container
        LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7589).setLon(-73.9851).build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("10km")
            .putLocation("location", geoLocation)
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setGeoDistance(geoDistanceQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a GeoDistanceQueryBuilder",
            "org.opensearch.index.query.GeoDistanceQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testGeoBoundingBoxQueryConversion() {
        // Create a GeoBoundingBox query container
        CoordsGeoBounds coords = CoordsGeoBounds.newBuilder().setTop(40.7).setLeft(-74.0).setBottom(40.6).setRight(-73.9).build();

        GeoBounds geoBounds = GeoBounds.newBuilder().setCoords(coords).build();

        GeoBoundingBoxQuery geoBoundingBoxQuery = GeoBoundingBoxQuery.newBuilder().putBoundingBox("location", geoBounds).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setGeoBoundingBox(geoBoundingBoxQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a GeoBoundingBoxQueryBuilder",
            "org.opensearch.index.query.GeoBoundingBoxQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testGeoDistanceQueryConversionWithDoubleArray() {
        // Create a GeoDistance query with DoubleArray format
        DoubleArray doubleArray = DoubleArray.newBuilder()
            .addDoubleArray(-73.9851) // lon
            .addDoubleArray(40.7589)  // lat
            .build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setDoubleArray(doubleArray).build();

        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("5mi")
            .putLocation("location", geoLocation)
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setGeoDistance(geoDistanceQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a GeoDistanceQueryBuilder",
            "org.opensearch.index.query.GeoDistanceQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testMultiMatchQueryConversion() {
        // Create a MultiMatch query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMultiMatch(
                MultiMatchQuery.newBuilder()
                    .setQuery("search term")
                    .addFields("title")
                    .addFields("content")
                    .setType(TextQueryType.TEXT_QUERY_TYPE_BEST_FIELDS)
                    .setMinimumShouldMatch(MinimumShouldMatch.newBuilder().setString("75%").build())
                    .setBoost(2.0f)
                    .setXName("test_multimatch_query")
                    .build()
            )
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a MultiMatchQueryBuilder",
            "org.opensearch.index.query.MultiMatchQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testMatchPhraseQueryConversion() {
        // Create a MatchPhrase query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMatchPhrase(
                MatchPhraseQuery.newBuilder()
                    .setField("title")
                    .setQuery("hello world")
                    .setAnalyzer("standard")
                    .setSlop(2)
                    .setBoost(1.5f)
                    .setXName("test_matchphrase_query")
                    .build()
            )
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a MatchPhraseQueryBuilder",
            "org.opensearch.index.query.MatchPhraseQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }
}
