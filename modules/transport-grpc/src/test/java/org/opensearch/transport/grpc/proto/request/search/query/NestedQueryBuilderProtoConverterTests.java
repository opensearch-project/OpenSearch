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
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.ChildScoreMode;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.InnerHits;
import org.opensearch.protobufs.NestedQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchTestCase;

public class NestedQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private NestedQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new NestedQueryBuilderProtoConverter();
        // Set up the registry with all built-in converters
        QueryBuilderProtoConverterRegistryImpl registry = new QueryBuilderProtoConverterRegistryImpl();
        converter.setRegistry(registry);
    }

    public void testGetHandledQueryCase() {
        // Test that the converter returns the correct QueryContainerCase
        assertEquals("Converter should handle NESTED case", QueryContainer.QueryContainerCase.NESTED, converter.getHandledQueryCase());
    }

    public void testFromProtoWithValidNestedQuery() {
        // Create a nested query with term query as inner query
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.name")
            .setValue(FieldValue.newBuilder().setString("john").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_MAX)
            .setIgnoreUnmapped(true)
            .setBoost(1.5f)
            .setXName("test_nested_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setNested(nestedQuery).build();

        QueryBuilder result = converter.fromProto(queryContainer);

        assertNotNull("Result should not be null", result);
        assertTrue("Result should be NestedQueryBuilder", result instanceof NestedQueryBuilder);

        NestedQueryBuilder nestedQueryBuilder = (NestedQueryBuilder) result;
        assertEquals("Path should match", "user", nestedQueryBuilder.path());
        assertEquals("Score mode should be Max", ScoreMode.Max, nestedQueryBuilder.scoreMode());
        assertTrue("Ignore unmapped should be true", nestedQueryBuilder.ignoreUnmapped());
        assertEquals("Boost should match", 1.5f, nestedQueryBuilder.boost(), 0.001f);
        assertEquals("Query name should match", "test_nested_query", nestedQueryBuilder.queryName());
    }

    public void testFromProtoWithInnerHits() {
        // Create inner hits
        InnerHits innerHits = InnerHits.newBuilder().setName("user_inner_hits").setSize(10).setFrom(0).build();

        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.age")
            .setValue(
                FieldValue.newBuilder()
                    .setGeneralNumber(org.opensearch.protobufs.GeneralNumber.newBuilder().setInt32Value(25).build())
                    .build()
            )
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder().setPath("user").setQuery(innerQueryContainer).setInnerHits(innerHits).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setNested(nestedQuery).build();

        QueryBuilder result = converter.fromProto(queryContainer);

        assertNotNull("Result should not be null", result);
        assertTrue("Result should be NestedQueryBuilder", result instanceof NestedQueryBuilder);

        NestedQueryBuilder nestedQueryBuilder = (NestedQueryBuilder) result;
        assertEquals("Path should match", "user", nestedQueryBuilder.path());
        assertNotNull("Inner hits should not be null", nestedQueryBuilder.innerHit());
        assertEquals("Inner hits name should match", "user_inner_hits", nestedQueryBuilder.innerHit().getName());
        assertEquals("Inner hits size should match", 10, nestedQueryBuilder.innerHit().getSize());
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

        QueryContainer avgContainer = QueryContainer.newBuilder().setNested(avgQuery).build();
        NestedQueryBuilder avgResult = (NestedQueryBuilder) converter.fromProto(avgContainer);
        assertEquals("Score mode should be Avg", ScoreMode.Avg, avgResult.scoreMode());

        // Test MIN score mode
        NestedQuery minQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_MIN)
            .build();

        QueryContainer minContainer = QueryContainer.newBuilder().setNested(minQuery).build();
        NestedQueryBuilder minResult = (NestedQueryBuilder) converter.fromProto(minContainer);
        assertEquals("Score mode should be Min", ScoreMode.Min, minResult.scoreMode());

        // Test NONE score mode
        NestedQuery noneQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_NONE)
            .build();

        QueryContainer noneContainer = QueryContainer.newBuilder().setNested(noneQuery).build();
        NestedQueryBuilder noneResult = (NestedQueryBuilder) converter.fromProto(noneContainer);
        assertEquals("Score mode should be None", ScoreMode.None, noneResult.scoreMode());

        // Test SUM score mode (maps to Total in Lucene)
        NestedQuery sumQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_SUM)
            .build();

        QueryContainer sumContainer = QueryContainer.newBuilder().setNested(sumQuery).build();
        NestedQueryBuilder sumResult = (NestedQueryBuilder) converter.fromProto(sumContainer);
        assertEquals("Score mode should be Total", ScoreMode.Total, sumResult.scoreMode());
    }

    public void testFromProtoWithNullQueryContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));
        assertEquals("Exception message should match", "QueryContainer must contain a NestedQuery", exception.getMessage());
    }

    public void testFromProtoWithWrongQueryType() {
        // Create a term query instead of nested query
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.name")
            .setValue(FieldValue.newBuilder().setString("john").build())
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));
        assertEquals("Exception message should match", "QueryContainer must contain a NestedQuery", exception.getMessage());
    }

    public void testFromProtoWithEmptyQueryContainer() {
        QueryContainer queryContainer = QueryContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));
        assertEquals("Exception message should match", "QueryContainer must contain a NestedQuery", exception.getMessage());
    }

    public void testFromProtoWithMinimalNestedQuery() {
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.email")
            .setValue(FieldValue.newBuilder().setString("test@example.com").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder().setPath("user").setQuery(innerQueryContainer).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setNested(nestedQuery).build();

        QueryBuilder result = converter.fromProto(queryContainer);

        assertNotNull("Result should not be null", result);
        assertTrue("Result should be NestedQueryBuilder", result instanceof NestedQueryBuilder);

        NestedQueryBuilder nestedQueryBuilder = (NestedQueryBuilder) result;
        assertEquals("Path should match", "user", nestedQueryBuilder.path());
        assertEquals("Default score mode should be Avg", ScoreMode.Avg, nestedQueryBuilder.scoreMode());
        assertFalse("Ignore unmapped should be false by default", nestedQueryBuilder.ignoreUnmapped());
        assertEquals("Default boost should be 1.0", 1.0f, nestedQueryBuilder.boost(), 0.001f);
        assertNull("Query name should be null by default", nestedQueryBuilder.queryName());
    }

    public void testFromProtoWithComplexNestedPath() {
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.profile.settings.theme")
            .setValue(FieldValue.newBuilder().setString("dark").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedQuery nestedQuery = NestedQuery.newBuilder()
            .setPath("user.profile.settings")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_MAX)
            .setIgnoreUnmapped(false)
            .setBoost(2.5f)
            .setXName("complex_nested_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setNested(nestedQuery).build();

        QueryBuilder result = converter.fromProto(queryContainer);

        assertNotNull("Result should not be null", result);
        assertTrue("Result should be NestedQueryBuilder", result instanceof NestedQueryBuilder);

        NestedQueryBuilder nestedQueryBuilder = (NestedQueryBuilder) result;
        assertEquals("Path should match", "user.profile.settings", nestedQueryBuilder.path());
        assertEquals("Score mode should be Max", ScoreMode.Max, nestedQueryBuilder.scoreMode());
        assertFalse("Ignore unmapped should be false", nestedQueryBuilder.ignoreUnmapped());
        assertEquals("Boost should match", 2.5f, nestedQueryBuilder.boost(), 0.001f);
        assertEquals("Query name should match", "complex_nested_query", nestedQueryBuilder.queryName());
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

        QueryContainer queryContainer = QueryContainer.newBuilder().setNested(nestedQuery).build();

        QueryBuilder result = converter.fromProto(queryContainer);

        assertNotNull("Result should not be null", result);
        assertTrue("Result should be NestedQueryBuilder", result instanceof NestedQueryBuilder);

        NestedQueryBuilder nestedQueryBuilder = (NestedQueryBuilder) result;
        assertEquals("Default score mode should be Avg for unspecified", ScoreMode.Avg, nestedQueryBuilder.scoreMode());
    }
}
