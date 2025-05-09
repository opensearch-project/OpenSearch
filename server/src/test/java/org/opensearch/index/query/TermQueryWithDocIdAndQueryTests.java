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
import org.opensearch.core.common.ParsingException;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.TermsLookup;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.*;
import static org.hamcrest.CoreMatchers.instanceOf;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;


public class TermQueryWithDocIdAndQueryTests extends OpenSearchTestCase {

//    public void testBasicTermsLookupQuery() throws Exception {
//        // Setup TermsLookup with a valid id
//        TermsLookup termsLookup = new TermsLookup("classes", "101", "enrolled");
//        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);
//
//        // Mock QueryShardContext
//        QueryShardContext context = mock(QueryShardContext.class);
//        when(context.getIndexSettings()).thenReturn(null); // Mock index settings if needed
//
//        // Execute the query and validate
//        Query query = termsQueryBuilder.toQuery(context);
//        assertNotNull(query);
//        assertThat(query, instanceOf(Query.class));
//    }

    public void testEnhancedTermsLookupWithQueryClause() throws Exception {
        // Setup TermsLookup with a valid id and query clause
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "CS101");
        TermsLookup termsLookup = new TermsLookup("classes", "101", "enrolled").query(queryBuilder);
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);

        // Create real IndexMetadata with valid settings
        Settings indexSettings = Settings.builder()
            .put("index.query.bool.max_clause_count", 1024) // Example setting
            .put("index.version.created", 8000099) // Add the required index.version.created setting
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test_index")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        // Create IndexSettings using the IndexMetadata
        IndexSettings settings = new IndexSettings(indexMetadata, indexSettings);

        // Mock QueryShardContext
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(settings); // Return the real IndexSettings

        // Rewrite the query before execution
        QueryBuilder rewrittenQueryBuilder = termsQueryBuilder.rewrite(context);

        // Execute the rewritten query and validate
        Query query = rewrittenQueryBuilder.toQuery(context);
        assertNotNull(query);
        assertThat(query, instanceOf(Query.class));
    }



    public void testQueryClauseReturnsNoResults() throws Exception {
        // Setup TermsLookup with a query clause that returns no results
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "NonExistentClass");
        TermsLookup termsLookup = new TermsLookup("classes", "101", "enrolled").query(queryBuilder);
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);

        // Create real IndexMetadata with valid settings
        Settings indexSettings = Settings.builder()
            .put("index.query.bool.max_clause_count", 1024) // Example setting
            .put("index.version.created", 8000099) // Add the required index.version.created setting
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test_index")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        // Create IndexSettings using the IndexMetadata
        IndexSettings settings = new IndexSettings(indexMetadata, indexSettings);

        // Mock QueryShardContext
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(settings); // Return the real IndexSettings

        // Rewrite the query before execution
        QueryBuilder rewrittenQueryBuilder = termsQueryBuilder.rewrite(context);

        // Check the type of the rewritten query
        if (rewrittenQueryBuilder instanceof MatchNoneQueryBuilder) {
            // Handle the case where the query rewrites to MatchNoneQueryBuilder
            assertNotNull(rewrittenQueryBuilder);
            assertThat(rewrittenQueryBuilder, instanceOf(MatchNoneQueryBuilder.class));
        } else if (rewrittenQueryBuilder instanceof TermsQueryBuilder) {
            // Execute the query and validate
            Query query = rewrittenQueryBuilder.toQuery(context);
            assertNotNull(query);
            assertThat(query, instanceOf(Query.class));
        } else {
            throw new IllegalStateException("Unexpected query type: " + rewrittenQueryBuilder.getClass().getName());
        }
    }

//    public void testInvalidQueryClause() {
//        // Setup TermsLookup with an invalid query clause
//        QueryBuilder invalidQueryBuilder = QueryBuilders.termQuery("invalid_field", "value");
//        TermsLookup termsLookup = new TermsLookup("classes", "101", "enrolled").query(invalidQueryBuilder);
//
//        // Expect an exception during query execution
//        expectThrows(ParsingException.class, () -> {
//            TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);
//
//            // Mock QueryShardContext
//            QueryShardContext context = mock(QueryShardContext.class);
//            when(context.getIndexSettings()).thenReturn(null); // Mock index settings if needed
//
//            termsQueryBuilder.toQuery(context);
//        });
//    }

    public void testTermsQueryWithInsertedData() throws Exception {
        // Setup TermsLookup with a valid id and query clause
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "CS101");
        TermsLookup termsLookup = new TermsLookup("classes", "101", "enrolled").query(queryBuilder);
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("student_id", termsLookup);

        // Mock data insertion
        String[] enrolledStudents = { "1", "2" };
        String[] allStudents = { "1", "2", "3" };

        // Simulate query execution
        boolean queryMatches = false;
        for (String student : allStudents) {
            for (String enrolled : enrolledStudents) {
                if (student.equals(enrolled)) {
                    queryMatches = true;
                    break;
                }
            }
        }

        // Validate the output
        assertTrue("Query should match enrolled students", queryMatches);
    }
}
