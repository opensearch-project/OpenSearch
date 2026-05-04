/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Integration tests for bool query with minimum_should_match parameter.
 */
public class DslBoolQueryIT extends DslIntegTestBase {

    @Override
    protected void createTestIndex() {
        createIndex(INDEX);
        ensureGreen();
        
        // Index multiple documents with different field values
        client().prepareIndex(INDEX)
            .setId("1")
            .setSource("{\"name\":\"laptop\",\"price\":1200,\"brand\":\"brandX\",\"rating\":4.5,\"tag\":\"electronics\"}", XContentType.JSON)
            .get();
        client().prepareIndex(INDEX)
            .setId("2")
            .setSource("{\"name\":\"phone\",\"price\":800,\"brand\":\"brandY\",\"rating\":4.0,\"tag\":\"electronics\"}", XContentType.JSON)
            .get();
        client().prepareIndex(INDEX)
            .setId("3")
            .setSource("{\"name\":\"tablet\",\"price\":600,\"brand\":\"brandX\",\"rating\":3.5,\"tag\":\"electronics\"}", XContentType.JSON)
            .get();
        client().prepareIndex(INDEX)
            .setId("4")
            .setSource("{\"name\":\"monitor\",\"price\":400,\"brand\":\"brandZ\",\"rating\":4.2,\"tag\":\"accessories\"}", XContentType.JSON)
            .get();
        refresh(INDEX);
    }

    // Basic bool query tests

    public void testBoolQueryWithMust() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("tag", "electronics"))
        )));
    }

    public void testBoolQueryWithShould() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("brand", "brandX"))
                .should(QueryBuilders.termQuery("brand", "brandY"))
        )));
    }

    public void testBoolQueryWithMustNot() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("tag", "electronics"))
                .mustNot(QueryBuilders.termQuery("brand", "brandZ"))
        )));
    }

    public void testBoolQueryWithFilter() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("tag", "electronics"))
        )));
    }

    public void testBoolQueryWithAllClauses() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("tag", "electronics"))
                .should(QueryBuilders.termQuery("brand", "brandX"))
                .mustNot(QueryBuilders.termQuery("name", "phone"))
                .filter(QueryBuilders.termQuery("rating", 4.5))
        )));
    }

    // minimum_should_match: Integer tests

    public void testMinimumShouldMatchPositiveInteger() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("brand", "brandX"))
                .should(QueryBuilders.termQuery("brand", "brandY"))
                .should(QueryBuilders.termQuery("brand", "brandZ"))
                .minimumShouldMatch("2")
        )));
    }

    public void testMinimumShouldMatchNegativeInteger() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("brand", "brandX"))
                .should(QueryBuilders.termQuery("brand", "brandY"))
                .should(QueryBuilders.termQuery("brand", "brandZ"))
                .minimumShouldMatch("-1")
        )));
    }

    // minimum_should_match: Percentage tests

    public void testMinimumShouldMatchPositivePercentage() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("tag", "electronics"))
                .should(QueryBuilders.termQuery("brand", "brandX"))
                .should(QueryBuilders.termQuery("name", "laptop"))
                .should(QueryBuilders.termQuery("rating", 4.5))
                .minimumShouldMatch("75%")
        )));
    }

    public void testMinimumShouldMatchNegativePercentage() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("tag", "electronics"))
                .should(QueryBuilders.termQuery("brand", "brandX"))
                .should(QueryBuilders.termQuery("name", "laptop"))
                .should(QueryBuilders.termQuery("rating", 4.5))
                .minimumShouldMatch("-25%")
        )));
    }

    // minimum_should_match: Combination tests

    public void testMinimumShouldMatchSingleCombination() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("brand", "brandX"))
                .should(QueryBuilders.termQuery("brand", "brandY"))
                .minimumShouldMatch("2<75%")
        )));
    }

    public void testMinimumShouldMatchMultipleCombinations() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("tag", "electronics"))
                .should(QueryBuilders.termQuery("brand", "brandX"))
                .should(QueryBuilders.termQuery("name", "laptop"))
                .should(QueryBuilders.termQuery("rating", 4.5))
                .minimumShouldMatch("3<-1 5<50%")
        )));
    }

    // minimum_should_match with must/filter

    public void testMinimumShouldMatchWithMust() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("tag", "electronics"))
                .should(QueryBuilders.termQuery("brand", "brandX"))
                .should(QueryBuilders.termQuery("brand", "brandY"))
                .minimumShouldMatch("1")
        )));
    }

    public void testMinimumShouldMatchWithFilter() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("tag", "electronics"))
                .should(QueryBuilders.termQuery("brand", "brandX"))
                .should(QueryBuilders.termQuery("brand", "brandY"))
                .minimumShouldMatch("1")
        )));
    }

    // Nested bool queries

    public void testNestedBoolQuery() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("tag", "electronics"))
                .must(QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("brand", "brandX"))
                    .should(QueryBuilders.termQuery("brand", "brandY"))
                )
        )));
    }

    public void testNestedBoolQueryWithMinimumShouldMatch() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("tag", "electronics"))
                .must(QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("brand", "brandX"))
                    .should(QueryBuilders.termQuery("brand", "brandY"))
                    .should(QueryBuilders.termQuery("brand", "brandZ"))
                    .minimumShouldMatch("2")
                )
        )));
    }

    // Edge cases

    public void testBoolQueryWithOnlyShouldClause() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("brand", "brandX"))
        )));
    }

    public void testBoolQueryWithMinimumShouldMatchOne() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("brand", "brandX"))
                .should(QueryBuilders.termQuery("brand", "brandY"))
                .minimumShouldMatch("1")
        )));
    }

    public void testBoolQueryWithMinimumShouldMatchAll() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("tag", "electronics"))
                .should(QueryBuilders.termQuery("brand", "brandX"))
                .minimumShouldMatch("2")
        )));
    }
}
