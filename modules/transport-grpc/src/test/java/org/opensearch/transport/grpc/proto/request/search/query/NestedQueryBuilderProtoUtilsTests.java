/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.protobufs.ChildScoreMode;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.InnerHits;
import org.opensearch.protobufs.NestedQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchTestCase;

public class NestedQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistryImpl registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Set up the registry with all built-in converters
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testFromProtoWithBasicNestedQuery() {
        // Create a basic nested query with term query as inner query
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.name")
            .setValue(FieldValue.newBuilder().setString("john").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder().setPath("user").setQuery(innerQueryContainer).build();

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Path should match", "user", queryBuilder.path());
        assertNotNull("Inner query should not be null", queryBuilder.query());
        assertEquals("Default score mode should be Avg", ScoreMode.Avg, queryBuilder.scoreMode());
    }

    public void testFromProtoWithAllParameters() {
        // Create inner hits
        InnerHits innerHits = InnerHits.newBuilder().setName("user_hits").setSize(10).build();

        // Create a term query as inner query
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.age")
            .setValue(
                FieldValue.newBuilder()
                    .setGeneralNumber(org.opensearch.protobufs.GeneralNumber.newBuilder().setInt32Value(25).build())
                    .build()
            )
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_MAX)
            .setIgnoreUnmapped(true)
            .setBoost(2.0f)
            .setXName("nested_user_query")
            .setInnerHits(innerHits)
            .build();

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Path should match", "user", queryBuilder.path());
        assertEquals("Score mode should be Max", ScoreMode.Max, queryBuilder.scoreMode());
        assertTrue("Ignore unmapped should be true", queryBuilder.ignoreUnmapped());
        assertEquals("Boost should match", 2.0f, queryBuilder.boost(), 0.001f);
        assertEquals("Query name should match", "nested_user_query", queryBuilder.queryName());
        assertNotNull("Inner hits should not be null", queryBuilder.innerHit());
    }

    public void testFromProtoWithDifferentScoreModes() {
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.status")
            .setValue(FieldValue.newBuilder().setString("active").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        // Test AVG score mode
        NestedQuery avgQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_AVG)
            .build();

        NestedQueryBuilder avgBuilder = NestedQueryBuilderProtoUtils.fromProto(avgQuery, registry);
        assertEquals("Score mode should be Avg", ScoreMode.Avg, avgBuilder.scoreMode());

        // Test MIN score mode
        NestedQuery minQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_MIN)
            .build();

        NestedQueryBuilder minBuilder = NestedQueryBuilderProtoUtils.fromProto(minQuery, registry);
        assertEquals("Score mode should be Min", ScoreMode.Min, minBuilder.scoreMode());

        // Test NONE score mode
        NestedQuery noneQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_NONE)
            .build();

        NestedQueryBuilder noneBuilder = NestedQueryBuilderProtoUtils.fromProto(noneQuery, registry);
        assertEquals("Score mode should be None", ScoreMode.None, noneBuilder.scoreMode());

        // Test SUM score mode (maps to Total in Lucene)
        NestedQuery sumQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_SUM)
            .build();

        NestedQueryBuilder sumBuilder = NestedQueryBuilderProtoUtils.fromProto(sumQuery, registry);
        assertEquals("Score mode should be Total", ScoreMode.Total, sumBuilder.scoreMode());
    }

    public void testFromProtoWithUnspecifiedScoreMode() {
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.role")
            .setValue(FieldValue.newBuilder().setString("admin").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_UNSPECIFIED)
            .build();

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry);
        assertEquals("Default score mode should be Avg for unspecified", ScoreMode.Avg, queryBuilder.scoreMode());
    }

    public void testFromProtoWithNullNestedQuery() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> NestedQueryBuilderProtoUtils.fromProto(null, registry)
        );
        assertEquals("Exception message should match", "NestedQuery cannot be null", exception.getMessage());
    }

    public void testFromProtoWithEmptyPath() {
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.name")
            .setValue(FieldValue.newBuilder().setString("john").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder().setPath("").setQuery(innerQueryContainer).build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry)
        );
        assertEquals("Exception message should match", "Path is required for NestedQuery", exception.getMessage());
    }

    public void testFromProtoWithNullPath() {
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.name")
            .setValue(FieldValue.newBuilder().setString("john").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder().setQuery(innerQueryContainer).build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry)
        );
        assertEquals("Exception message should match", "Path is required for NestedQuery", exception.getMessage());
    }

    public void testFromProtoWithMissingQuery() {
        NestedQuery nestedQuery = NestedQuery.newBuilder().setPath("user").build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry)
        );
        assertEquals("Exception message should match", "Query is required for NestedQuery", exception.getMessage());
    }

    public void testFromProtoWithOptionalParametersOnly() {
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.email")
            .setValue(FieldValue.newBuilder().setString("test@example.com").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setIgnoreUnmapped(false)
            .setBoost(1.5f)
            .setXName("optional_params_query")
            .build();

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Path should match", "user", queryBuilder.path());
        assertFalse("Ignore unmapped should be false", queryBuilder.ignoreUnmapped());
        assertEquals("Boost should match", 1.5f, queryBuilder.boost(), 0.001f);
        assertEquals("Query name should match", "optional_params_query", queryBuilder.queryName());
        assertEquals("Default score mode should be Avg", ScoreMode.Avg, queryBuilder.scoreMode());
    }

    public void testFromProtoWithInnerHitsOnly() {
        InnerHits innerHits = InnerHits.newBuilder().setName("product_hits").setSize(5).setFrom(0).build();

        TermQuery termQuery = TermQuery.newBuilder()
            .setField("products.category")
            .setValue(FieldValue.newBuilder().setString("electronics").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder()
            .setPath("products")
            .setQuery(innerQueryContainer)
            .setInnerHits(innerHits)
            .build();

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Path should match", "products", queryBuilder.path());
        assertNotNull("Inner hits should not be null", queryBuilder.innerHit());
        assertEquals("Inner hits name should match", "product_hits", queryBuilder.innerHit().getName());
        assertEquals("Inner hits size should match", 5, queryBuilder.innerHit().getSize());
    }

    public void testFromProtoWithComplexNestedPath() {
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.profile.settings.theme")
            .setValue(FieldValue.newBuilder().setString("dark").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder()
            .setPath("user.profile")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_MAX)
            .build();

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Path should match", "user.profile", queryBuilder.path());
        assertEquals("Score mode should be Max", ScoreMode.Max, queryBuilder.scoreMode());
    }

    public void testFromProtoWithInvalidInnerQuery() {
        // Create an empty QueryContainer to simulate invalid query conversion
        QueryContainer emptyQueryContainer = QueryContainer.newBuilder().build();

        NestedQuery nestedQuery = NestedQuery.newBuilder().setPath("user").setQuery(emptyQueryContainer).build();

        // This should throw an exception when trying to convert the inner query
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry)
        );
        assertTrue(
            "Exception message should contain 'Failed to convert inner query'",
            exception.getMessage().contains("Failed to convert inner query for NestedQuery")
        );
    }

    public void testFromProtoWithInvalidInnerHits() {
        // Test with invalid inner hits to trigger exception handling
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.name")
            .setValue(FieldValue.newBuilder().setString("john").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        // Create inner hits with invalid configuration that will cause conversion to fail
        InnerHits invalidInnerHits = InnerHits.newBuilder()
            .setName("valid_name")
            .setSize(-1) // Invalid size - should trigger exception
            .build();

        NestedQuery nestedQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setInnerHits(invalidInnerHits)
            .build();

        // This should throw an exception due to invalid inner hits configuration
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry)
        );
        assertTrue(
            "Exception message should contain 'Failed to convert inner hits'",
            exception.getMessage().contains("Failed to convert inner hits for NestedQuery")
        );
    }

    public void testParseScoreModeDefaultCase() {
        // Test when no score mode is set - should default to ScoreMode.Avg
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.status")
            .setValue(FieldValue.newBuilder().setString("active").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        // Don't set score mode - this will test the default behavior
        NestedQuery nestedQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            // No setScoreMode() call - should use default ScoreMode.Avg
            .build();

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry);
        assertEquals("Default score mode should be Avg when not set", ScoreMode.Avg, queryBuilder.scoreMode());
    }

    public void testFromProtoWithIgnoreUnmappedTrue() {
        // Test setting ignoreUnmapped to true
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.status")
            .setValue(FieldValue.newBuilder().setString("active").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder().setPath("user").setQuery(innerQueryContainer).setIgnoreUnmapped(true).build();

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry);
        assertTrue("Ignore unmapped should be true", queryBuilder.ignoreUnmapped());
    }

    public void testFromProtoWithIgnoreUnmappedFalse() {
        // Test setting ignoreUnmapped to false
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.status")
            .setValue(FieldValue.newBuilder().setString("active").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder().setPath("user").setQuery(innerQueryContainer).setIgnoreUnmapped(false).build();

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry);
        assertFalse("Ignore unmapped should be false", queryBuilder.ignoreUnmapped());
    }

    public void testFromProtoWithAllScoreModes() {
        // Test all score modes to achieve full coverage of parseScoreMode method
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.status")
            .setValue(FieldValue.newBuilder().setString("active").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        // Test MAX score mode
        NestedQuery maxQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_MAX)
            .build();

        NestedQueryBuilder maxBuilder = NestedQueryBuilderProtoUtils.fromProto(maxQuery, registry);
        assertEquals("Score mode should be Max", ScoreMode.Max, maxBuilder.scoreMode());

        // Test MIN score mode
        NestedQuery minQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_MIN)
            .build();

        NestedQueryBuilder minBuilder = NestedQueryBuilderProtoUtils.fromProto(minQuery, registry);
        assertEquals("Score mode should be Min", ScoreMode.Min, minBuilder.scoreMode());

        // Test NONE score mode
        NestedQuery noneQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_NONE)
            .build();

        NestedQueryBuilder noneBuilder = NestedQueryBuilderProtoUtils.fromProto(noneQuery, registry);
        assertEquals("Score mode should be None", ScoreMode.None, noneBuilder.scoreMode());

        // Test SUM score mode (maps to Total in Lucene)
        NestedQuery sumQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_SUM)
            .build();

        NestedQueryBuilder sumBuilder = NestedQueryBuilderProtoUtils.fromProto(sumQuery, registry);
        assertEquals("Score mode should be Total", ScoreMode.Total, sumBuilder.scoreMode());
    }

    public void testFromProtoWithInvalidQueryConversion() {
        // Test exception handling when inner query conversion fails
        // Create a mock registry that throws an exception
        QueryBuilderProtoConverterRegistryImpl mockRegistry = new QueryBuilderProtoConverterRegistryImpl() {
            @Override
            public org.opensearch.index.query.QueryBuilder fromProto(QueryContainer queryContainer) {
                throw new RuntimeException("Simulated conversion failure");
            }
        };

        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.name")
            .setValue(FieldValue.newBuilder().setString("john").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder().setPath("user").setQuery(innerQueryContainer).build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> NestedQueryBuilderProtoUtils.fromProto(nestedQuery, mockRegistry)
        );
        assertTrue(
            "Exception message should contain 'Failed to convert inner query'",
            exception.getMessage().contains("Failed to convert inner query for NestedQuery")
        );
        assertTrue("Exception message should contain the cause", exception.getMessage().contains("Simulated conversion failure"));
    }

    public void testFromProtoWithInvalidInnerHitsConversion() {
        // Test exception handling when inner hits conversion fails
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.name")
            .setValue(FieldValue.newBuilder().setString("john").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        // Create inner hits that will cause conversion to fail
        // Using an invalid size that should trigger an exception in InnerHitsBuilderProtoUtils
        org.opensearch.protobufs.InnerHits invalidInnerHits = org.opensearch.protobufs.InnerHits.newBuilder()
            .setName("test_inner_hits")
            .setSize(-1) // Invalid size - should trigger exception
            .build();

        NestedQuery nestedQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setInnerHits(invalidInnerHits)
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> NestedQueryBuilderProtoUtils.fromProto(nestedQuery, registry)
        );
        assertTrue(
            "Exception message should contain 'Failed to convert inner hits'",
            exception.getMessage().contains("Failed to convert inner hits for NestedQuery")
        );
    }
}
