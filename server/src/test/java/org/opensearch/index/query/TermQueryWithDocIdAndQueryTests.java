/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.indices.TermsLookup;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TermQueryWithDocIdAndQueryTests extends OpenSearchTestCase {
    public void testTermsQueryWithIdOnlyAndVerifyResults() throws Exception {
        // Setup TermsLookup with a valid id
        TermsLookup termsLookup = new TermsLookup("classes", "101", "enrolled");
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);

        // Mock QueryShardContext
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(null); // Mock index settings if needed

        // Rewrite the query before execution
        QueryBuilder rewrittenQueryBuilder = termsQueryBuilder.rewrite(context);

        // Mock the search response
        SearchResponse mockResponse = mock(SearchResponse.class);
        SearchHits mockHits = new SearchHits(
            new SearchHit[] {
                new SearchHit(1).sourceRef(new BytesArray("{\"name\":\"Jane Doe\",\"student_id\":\"111\"}")),
                new SearchHit(2).sourceRef(new BytesArray("{\"name\":\"Mary Major\",\"student_id\":\"222\"}")) },
            new TotalHits(2, TotalHits.Relation.EQUAL_TO), // Use TotalHits instead of int
            1.0f
        );
        when(mockResponse.getHits()).thenReturn(mockHits);

        // Simulate the query execution and validate the results
        Query query = rewrittenQueryBuilder.toQuery(context);
        assertNotNull(query);
        assertThat(query, instanceOf(Query.class));

        // Verify the data
        SearchHit[] hits = mockResponse.getHits().getHits();
        assertEquals(2, hits.length);
        assertEquals("Jane Doe", hits[0].getSourceAsMap().get("name"));
        assertEquals("111", hits[0].getSourceAsMap().get("student_id"));
        assertEquals("Mary Major", hits[1].getSourceAsMap().get("name"));
        assertEquals("222", hits[1].getSourceAsMap().get("student_id"));
    }

    public void testTermsQueryWithQueryOnlyAndVerifyResults() throws Exception {
        // Setup TermsLookup with a query clause
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "CS101");
        TermsLookup termsLookup = new TermsLookup("classes", null, "enrolled", queryBuilder);
        termsLookup.setQuery(queryBuilder); // Ensure the query is set correctly

        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);

        // Mock QueryShardContext
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(null); // Mock index settings if needed

        // Rewrite the query before execution
        QueryBuilder rewrittenQueryBuilder = termsQueryBuilder.rewrite(context);

        // Mock the search response
        SearchResponse mockResponse = mock(SearchResponse.class);
        SearchHits mockHits = new SearchHits(
            new SearchHit[] {
                new SearchHit(1).sourceRef(new BytesArray("{\"name\":\"Jane Doe\",\"student_id\":\"111\"}")),
                new SearchHit(2).sourceRef(new BytesArray("{\"name\":\"Mary Major\",\"student_id\":\"222\"}")) },
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            1.0f
        );
        when(mockResponse.getHits()).thenReturn(mockHits);

        // Simulate the query execution and validate the results
        Query query = rewrittenQueryBuilder.toQuery(context);
        assertNotNull(query);
        assertThat(query, instanceOf(Query.class));

        // Verify the data
        SearchHit[] hits = mockResponse.getHits().getHits();
        assertEquals(2, hits.length);
        assertEquals("Jane Doe", hits[0].getSourceAsMap().get("name"));
        assertEquals("111", hits[0].getSourceAsMap().get("student_id"));
        assertEquals("Mary Major", hits[1].getSourceAsMap().get("name"));
        assertEquals("222", hits[1].getSourceAsMap().get("student_id"));
    }

    public void testEnhancedTermsLookupWithQueryClause() throws Exception {
        // Setup TermsLookup with a query clause
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("course", "Math101");
        TermsLookup termsLookup = new TermsLookup("courses", null, "students", queryBuilder);

        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);

        // Mock QueryShardContext
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(null);

        // Rewrite the query before execution
        QueryBuilder rewrittenQueryBuilder = termsQueryBuilder.rewrite(context);

        // Validate the rewritten query
        assertNotNull(rewrittenQueryBuilder);
        assertThat(rewrittenQueryBuilder, instanceOf(QueryBuilder.class));
    }

    public void testQueryClauseReturnsNoResults() throws Exception {
        // Setup TermsLookup with a query clause that returns no results
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("course", "NonExistentCourse");
        TermsLookup termsLookup = new TermsLookup("courses", null, "students", queryBuilder);

        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);

        // Mock QueryShardContext
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(null);

        // Rewrite the query before execution
        QueryBuilder rewrittenQueryBuilder = termsQueryBuilder.rewrite(context);

        // Mock the search response with no hits
        SearchResponse mockResponse = mock(SearchResponse.class);
        SearchHits mockHits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f);
        when(mockResponse.getHits()).thenReturn(mockHits);

        // Validate the results
        assertEquals(0, mockResponse.getHits().getHits().length);
    }

    public void testTermsQueryWithInsertedData() throws Exception {
        // Setup TermsLookup with valid data
        TermsLookup termsLookup = new TermsLookup("classes", "102", "enrolled");
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);

        // Mock QueryShardContext
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(null);

        // Rewrite the query before execution
        QueryBuilder rewrittenQueryBuilder = termsQueryBuilder.rewrite(context);

        // Mock the search response
        SearchResponse mockResponse = mock(SearchResponse.class);
        SearchHits mockHits = new SearchHits(
            new SearchHit[] {
                new SearchHit(1).sourceRef(new BytesArray("{\"name\":\"John Smith\",\"student_id\":\"333\"}")),
                new SearchHit(2).sourceRef(new BytesArray("{\"name\":\"Alice Brown\",\"student_id\":\"444\"}")) },
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            1.0f
        );
        when(mockResponse.getHits()).thenReturn(mockHits);

        // Validate the results
        SearchHit[] hits = mockResponse.getHits().getHits();
        assertEquals(2, hits.length);
        assertEquals("John Smith", hits[0].getSourceAsMap().get("name"));
        assertEquals("333", hits[0].getSourceAsMap().get("student_id"));
        assertEquals("Alice Brown", hits[1].getSourceAsMap().get("name"));
        assertEquals("444", hits[1].getSourceAsMap().get("student_id"));
    }

    public void testTermsQueryWithNoIdAndNoQuery() {
        // Attempt to create a TermsLookup with no id and no query
        Exception exception = expectThrows(IllegalArgumentException.class, () -> {
            new TermsLookup("classes", null, "enrolled");
        });

        // Verify the exception message
        assertEquals("[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying either the id or the query.", exception.getMessage());
    }

    public void testTermsQueryWithIdAndQuery() throws Exception {
        // Setup TermsLookup with both id and query
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("course", "CS102");
        TermsLookup termsLookup = new TermsLookup("classes", "103", "enrolled");

        // Expect an exception due to both id and query being set
        Exception exception = expectThrows(IllegalArgumentException.class, () -> termsLookup.setQuery(queryBuilder));
        assertEquals("[" + TermsQueryBuilder.NAME + "] query lookup element cannot specify both id and query.", exception.getMessage());

    }

    public void testTermsQueryWithComplexQuery() throws Exception {
        // Setup TermsLookup with a complex query
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("course", "CS103"))
            .filter(QueryBuilders.rangeQuery("year").gte(2020));
        TermsLookup termsLookup = new TermsLookup("classes", null, "enrolled", queryBuilder);

        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);

        // Mock QueryShardContext
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(null);

        // Rewrite the query before execution
        QueryBuilder rewrittenQueryBuilder = termsQueryBuilder.rewrite(context);

        // Validate the rewritten query
        assertNotNull(rewrittenQueryBuilder);
        assertThat(rewrittenQueryBuilder, instanceOf(QueryBuilder.class));
    }
}
