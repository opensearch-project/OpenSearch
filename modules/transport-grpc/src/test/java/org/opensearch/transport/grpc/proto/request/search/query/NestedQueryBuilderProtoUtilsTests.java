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

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Set up the registry with all built-in converters
        QueryBuilderProtoConverterRegistryImpl registry = new QueryBuilderProtoConverterRegistryImpl();
        NestedQueryBuilderProtoUtils.setRegistry(registry);
    }

    public void testFromProtoWithBasicNestedQuery() {
        // Create a basic nested query with term query as inner query
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.name")
            .setValue(FieldValue.newBuilder().setString("john").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder().setPath("user").setQuery(innerQueryContainer).build();

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery);

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

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery);

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

        NestedQueryBuilder avgBuilder = NestedQueryBuilderProtoUtils.fromProto(avgQuery);
        assertEquals("Score mode should be Avg", ScoreMode.Avg, avgBuilder.scoreMode());

        // Test MIN score mode
        NestedQuery minQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_MIN)
            .build();

        NestedQueryBuilder minBuilder = NestedQueryBuilderProtoUtils.fromProto(minQuery);
        assertEquals("Score mode should be Min", ScoreMode.Min, minBuilder.scoreMode());

        // Test NONE score mode
        NestedQuery noneQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_NONE)
            .build();

        NestedQueryBuilder noneBuilder = NestedQueryBuilderProtoUtils.fromProto(noneQuery);
        assertEquals("Score mode should be None", ScoreMode.None, noneBuilder.scoreMode());

        // Test SUM score mode (maps to Total in Lucene)
        NestedQuery sumQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_SUM)
            .build();

        NestedQueryBuilder sumBuilder = NestedQueryBuilderProtoUtils.fromProto(sumQuery);
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

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery);
        assertEquals("Default score mode should be Avg for unspecified", ScoreMode.Avg, queryBuilder.scoreMode());
    }

    public void testFromProtoWithNullNestedQuery() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> NestedQueryBuilderProtoUtils.fromProto(null)
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
            () -> NestedQueryBuilderProtoUtils.fromProto(nestedQuery)
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
            () -> NestedQueryBuilderProtoUtils.fromProto(nestedQuery)
        );
        assertEquals("Exception message should match", "Path is required for NestedQuery", exception.getMessage());
    }

    public void testFromProtoWithMissingQuery() {
        NestedQuery nestedQuery = NestedQuery.newBuilder().setPath("user").build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> NestedQueryBuilderProtoUtils.fromProto(nestedQuery)
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

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery);

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

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery);

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

        NestedQueryBuilder queryBuilder = NestedQueryBuilderProtoUtils.fromProto(nestedQuery);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Path should match", "user.profile", queryBuilder.path());
        assertEquals("Score mode should be Max", ScoreMode.Max, queryBuilder.scoreMode());
    }
}
