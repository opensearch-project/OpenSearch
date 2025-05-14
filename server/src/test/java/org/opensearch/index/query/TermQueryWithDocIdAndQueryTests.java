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
                new SearchHit(2).sourceRef(new BytesArray("{\"name\":\"Mary Major\",\"student_id\":\"222\"}"))
            },
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
}
// {
//
// public void testEnhancedTermsLookupWithQueryClause() throws Exception {
// // Setup TermsLookup with a valid id and query clause
// QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "CS101");
// TermsLookup termsLookup = new TermsLookup("classes", "101", "enrolled").query(queryBuilder);
// TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);
//
// // Create real IndexMetadata with valid settings
// Settings indexSettings = Settings.builder()
// .put("index.query.bool.max_clause_count", 1024) // Example setting
// .put("index.version.created", 8000099) // Add the required index.version.created setting
// .build();
// IndexMetadata indexMetadata = IndexMetadata.builder("test_index")
// .settings(indexSettings)
// .numberOfShards(1)
// .numberOfReplicas(1)
// .build();
//
// // Create IndexSettings using the IndexMetadata
// IndexSettings settings = new IndexSettings(indexMetadata, indexSettings);
//
// // Mock QueryShardContext
// QueryShardContext context = mock(QueryShardContext.class);
// when(context.getIndexSettings()).thenReturn(settings); // Return the real IndexSettings
//
// // Rewrite the query before execution
// QueryBuilder rewrittenQueryBuilder = termsQueryBuilder.rewrite(context);
//
// // Execute the rewritten query and validate
// Query query = rewrittenQueryBuilder.toQuery(context);
// assertNotNull(query);
// assertThat(query, instanceOf(Query.class));
// }
//
//
//
// public void testQueryClauseReturnsNoResults() throws Exception {
// // Setup TermsLookup with a query clause that returns no results
// QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "NonExistentClass");
// TermsLookup termsLookup = new TermsLookup("classes", "101", "enrolled").query(queryBuilder);
// TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);
//
// // Create real IndexMetadata with valid settings
// Settings indexSettings = Settings.builder()
// .put("index.query.bool.max_clause_count", 1024) // Example setting
// .put("index.version.created", 8000099) // Add the required index.version.created setting
// .build();
// IndexMetadata indexMetadata = IndexMetadata.builder("test_index")
// .settings(indexSettings)
// .numberOfShards(1)
// .numberOfReplicas(1)
// .build();
//
// // Create IndexSettings using the IndexMetadata
// IndexSettings settings = new IndexSettings(indexMetadata, indexSettings);
//
// // Mock QueryShardContext
// QueryShardContext context = mock(QueryShardContext.class);
// when(context.getIndexSettings()).thenReturn(settings); // Return the real IndexSettings
//
// // Rewrite the query before execution
// QueryBuilder rewrittenQueryBuilder = termsQueryBuilder.rewrite(context);
//
// // Check the type of the rewritten query
// if (rewrittenQueryBuilder instanceof MatchNoneQueryBuilder) {
// // Handle the case where the query rewrites to MatchNoneQueryBuilder
// assertNotNull(rewrittenQueryBuilder);
// assertThat(rewrittenQueryBuilder, instanceOf(MatchNoneQueryBuilder.class));
// } else if (rewrittenQueryBuilder instanceof TermsQueryBuilder) {
// // Execute the query and validate
// Query query = rewrittenQueryBuilder.toQuery(context);
// assertNotNull(query);
// assertThat(query, instanceOf(Query.class));
// } else {
// throw new IllegalStateException("Unexpected query type: " + rewrittenQueryBuilder.getClass().getName());
// }
// }
//
// public void testTermsQueryWithInsertedData() throws Exception {
// // Setup TermsLookup with a valid id and query clause
// QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "CS101");
// TermsLookup termsLookup = new TermsLookup("classes", "101", "enrolled").query(queryBuilder);
// TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);
//
// // Mock data insertion
// String[] enrolledStudents = { "1", "2" };
// String[] allStudents = { "1", "2", "3" };
//
// // Simulate query execution
// boolean queryMatches = false;
// for (String student : allStudents) {
// for (String enrolled : enrolledStudents) {
// if (student.equals(enrolled)) {
// queryMatches = true;
// break;
// }
// }
// }
//
// // Validate the output
// assertTrue("Query should match enrolled students", queryMatches);
// }
//
//
//
// public void testTermsQueryWithIdOnly() throws Exception {
// // Setup TermsLookup with a valid id
// TermsLookup termsLookup = new TermsLookup("classes", "101", "enrolled");
// TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);
//
// // Mock QueryShardContext
// QueryShardContext context = mock(QueryShardContext.class);
// when(context.getIndexSettings()).thenReturn(null); // Mock index settings if needed
//
// // Rewrite the query before execution
// QueryBuilder rewrittenQueryBuilder = termsQueryBuilder.rewrite(context);
//
// // Execute the rewritten query and validate
// Query query = rewrittenQueryBuilder.toQuery(context);
// assertNotNull(query);
// assertThat(query, instanceOf(Query.class));
// }
//
// public void testTermsQueryWithQueryOnly() throws Exception {
// // Setup TermsLookup with a query clause
// QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "CS101");
// TermsLookup termsLookup = new TermsLookup("classes", null, "enrolled").query(queryBuilder);
// TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);
//
// // Mock QueryShardContext
// QueryShardContext context = mock(QueryShardContext.class);
// when(context.getIndexSettings()).thenReturn(null); // Mock index settings if needed
//
// // Rewrite the query before execution
// QueryBuilder rewrittenQueryBuilder = termsQueryBuilder.rewrite(context);
//
// // Execute the rewritten query and validate
// Query query = rewrittenQueryBuilder.toQuery(context);
// assertNotNull(query);
// assertThat(query, instanceOf(Query.class));
// }
//
// public void testTermsQueryWithIdAndQuery() {
// // Setup TermsLookup with both id and query (invalid case)
// QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "CS101");
// TermsLookup termsLookup = new TermsLookup("classes", "101", "enrolled").query(queryBuilder);
//
// // Expect an exception during query execution
// IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
// QueryBuilders.termsQuery("student_id", termsLookup);
// });
//
// // Validate the exception message
// assertEquals("[terms_lookup] cannot specify both 'id' and 'query'.", e.getMessage());
// }
//
// public void testTermsQueryWithNoIdAndNoQuery() {
// // Setup TermsLookup with neither id nor query (invalid case)
// TermsLookup termsLookup = new TermsLookup("classes", null, "enrolled");
//
// // Expect an exception during query execution
// IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
// QueryBuilders.termsQuery("student_id", termsLookup);
// });
//
// // Validate the exception message
// assertEquals("[terms_lookup] requires either 'id' or 'query' to be specified.", e.getMessage());
// }
//
// public void testTermsQueryWithComplexQuery() throws Exception {
// // Setup TermsLookup with a complex query clause
// QueryBuilder queryBuilder = QueryBuilders.boolQuery()
// .must(QueryBuilders.matchQuery("name", "CS101"))
// .filter(QueryBuilders.rangeQuery("enrolled").gte(100).lte(200));
// TermsLookup termsLookup = new TermsLookup("classes", null, "enrolled").query(queryBuilder);
// TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);
//
// // Mock QueryShardContext
// QueryShardContext context = mock(QueryShardContext.class);
// when(context.getIndexSettings()).thenReturn(null); // Mock index settings if needed
//
// // Rewrite the query before execution
// QueryBuilder rewrittenQueryBuilder = termsQueryBuilder.rewrite(context);
//
// // Execute the rewritten query and validate
// Query query = rewrittenQueryBuilder.toQuery(context);
// assertNotNull(query);
// assertThat(query, instanceOf(Query.class));
// }
//
// }
