/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.IdsQuery;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.MatchPhraseQuery;
import org.opensearch.protobufs.MinimumShouldMatch;
import org.opensearch.protobufs.MultiMatchQuery;
import org.opensearch.protobufs.NumberRangeQuery;
import org.opensearch.protobufs.NumberRangeQueryAllOfFrom;
import org.opensearch.protobufs.NumberRangeQueryAllOfTo;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.RangeQuery;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.protobufs.TermsSetQuery;
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

    public void testIdsQueryConversion() {
        // Create an Ids query container
        IdsQuery idsQuery = IdsQuery.newBuilder().addValues("doc1").addValues("doc2").setBoost(1.5f).setXName("test_ids_query").build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setIds(idsQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be an IdsQueryBuilder", "org.opensearch.index.query.IdsQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testRangeQueryConversion() {
        // Create a Range query container with NumberRangeQuery
        NumberRangeQueryAllOfFrom fromValue = NumberRangeQueryAllOfFrom.newBuilder().setDouble(10.0).build();
        NumberRangeQueryAllOfTo toValue = NumberRangeQueryAllOfTo.newBuilder().setDouble(100.0).build();

        NumberRangeQuery numberRangeQuery = NumberRangeQuery.newBuilder().setField("age").setFrom(fromValue).setTo(toValue).build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setNumberRangeQuery(numberRangeQuery).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setRange(rangeQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a RangeQueryBuilder", "org.opensearch.index.query.RangeQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testTermsSetQueryConversion() {
        // Create a TermsSet query container
        TermsSetQuery termsSetQuery = TermsSetQuery.newBuilder()
            .setField("tags")
            .addTerms("urgent")
            .addTerms("important")
            .setMinimumShouldMatchField("tag_count")
            .setBoost(2.0f)
            .setXName("test_terms_set_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTermsSet(termsSetQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a TermsSetQueryBuilder",
            "org.opensearch.index.query.TermsSetQueryBuilder",
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
