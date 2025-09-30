/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.BoolQuery;
import org.opensearch.protobufs.ChildScoreMode;
import org.opensearch.protobufs.ExistsQuery;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.MatchPhraseQuery;
import org.opensearch.protobufs.MinimumShouldMatch;
import org.opensearch.protobufs.MultiMatchQuery;
import org.opensearch.protobufs.NestedQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.RegexpQuery;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.ScriptQuery;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.protobufs.TextQueryType;
import org.opensearch.protobufs.WildcardQuery;
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

    public void testScriptQueryConversion() {
        // Create a Script query container with inline script
        Script script = Script.newBuilder().setInline(InlineScript.newBuilder().setSource("doc['field'].value > 100").build()).build();

        ScriptQuery scriptQuery = ScriptQuery.newBuilder().setScript(script).setBoost(1.5f).setXName("test_script_query").build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setScript(scriptQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a ScriptQueryBuilder", "org.opensearch.index.query.ScriptQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testNestedQueryConversion() {
        // Create a Term query as inner query for the nested query
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.name")
            .setValue(FieldValue.newBuilder().setString("john").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        // Create a Nested query container
        NestedQuery nestedQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_AVG)
            .setBoost(1.5f)
            .setXName("test_nested_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setNested(nestedQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a NestedQueryBuilder", "org.opensearch.index.query.NestedQueryBuilder", queryBuilder.getClass().getName());
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

    public void testExistsQueryConversion() {
        // Create an Exists query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setExists(ExistsQuery.newBuilder().setField("test_field").setBoost(2.0f).setXName("test_exists").build())
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be an ExistsQueryBuilder", "org.opensearch.index.query.ExistsQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testRegexpQueryConversion() {
        // Create a Regexp query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setRegexp(RegexpQuery.newBuilder().setField("test_field").setValue("test.*pattern").setBoost(1.2f).build())
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a RegexpQueryBuilder", "org.opensearch.index.query.RegexpQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testWildcardQueryConversion() {
        // Create a Wildcard query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setWildcard(WildcardQuery.newBuilder().setField("test_field").setValue("test*pattern").setBoost(0.8f).build())
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a WildcardQueryBuilder",
            "org.opensearch.index.query.WildcardQueryBuilder",
            queryBuilder.getClass().getName()
        );
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

    public void testBoolQueryConversion() {
        // Create a Bool query container with nested queries
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setBool(
                BoolQuery.newBuilder()
                    .addMust(QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build())
                    .addShould(
                        QueryContainer.newBuilder()
                            .setTerm(
                                TermQuery.newBuilder()
                                    .setField("status")
                                    .setValue(FieldValue.newBuilder().setString("active").build())
                                    .build()
                            )
                            .build()
                    )
                    .setBoost(1.0f)
                    .build()
            )
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a BoolQueryBuilder", "org.opensearch.index.query.BoolQueryBuilder", queryBuilder.getClass().getName());
    }
}
