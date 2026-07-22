/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertBooleanSubQuery;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class BoolPrefixFuzzinessTests extends AbstractQueryTestCase<MultiMatchQueryBuilder> {

    @Override
    protected MultiMatchQueryBuilder doCreateTestQueryBuilder() {
        return new MultiMatchQueryBuilder("test", TEXT_FIELD_NAME);
    }

    @Override
    protected void doAssertLuceneQuery(MultiMatchQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        // Not used in this test
    }

    public void testBoolPrefixWithFuzziness() throws Exception {
        QueryShardContext context = createShardContext();
        
        // Test with a single term
        MultiMatchQueryBuilder builder = new MultiMatchQueryBuilder("foo", TEXT_FIELD_NAME);
        builder.type(MultiMatchQueryBuilder.Type.BOOL_PREFIX);
        builder.fuzziness(Fuzziness.ONE);
        Query query = builder.toQuery(context);
        
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses().size(), equalTo(1));
        
        // Check the query structure directly
        BooleanClause clause = booleanQuery.clauses().get(0);
        Query innerQuery = clause.query();
        
        // The query should contain a FuzzyQuery
        assertThat(innerQuery, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) innerQuery;
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "foo")));
        
        // Test with multiple terms
        builder = new MultiMatchQueryBuilder("foo bar", TEXT_FIELD_NAME);
        builder.type(MultiMatchQueryBuilder.Type.BOOL_PREFIX);
        builder.fuzziness(Fuzziness.ONE);
        query = builder.toQuery(context);
        
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery multiTermQuery = (BooleanQuery) query;
        assertThat(multiTermQuery.clauses().size(), equalTo(2));
        
        // Check first term
        BooleanClause clause1 = multiTermQuery.clauses().get(0);
        Query innerQuery1 = clause1.query();
        assertThat(innerQuery1, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery1 = (FuzzyQuery) innerQuery1;
        assertThat(fuzzyQuery1.getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "foo")));
        
        // Check second term
        BooleanClause clause2 = multiTermQuery.clauses().get(1);
        Query innerQuery2 = clause2.query();
        assertThat(innerQuery2, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery2 = (FuzzyQuery) innerQuery2;
        assertThat(fuzzyQuery2.getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "bar")));
    }
    
    public void testBoolPrefixWithFuzzinessForMisspelledTerm() throws Exception {
        QueryShardContext context = createShardContext();
        
        // Test with a misspelled term (similar to the user's scenario)
        MultiMatchQueryBuilder builder = new MultiMatchQueryBuilder("lisence", TEXT_FIELD_NAME);
        builder.type(MultiMatchQueryBuilder.Type.BOOL_PREFIX);
        builder.fuzziness(Fuzziness.ONE);
        Query query = builder.toQuery(context);
        
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery boolQuery = (BooleanQuery) query;
        assertThat(boolQuery.clauses().size(), equalTo(1));
        
        // Check the query structure directly
        BooleanClause clause = boolQuery.clauses().get(0);
        Query innerQuery = clause.query();
        
        // The query should contain a FuzzyQuery
        assertThat(innerQuery, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) innerQuery;
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "lisence")));
        
        // Test with multiple fields
        builder = new MultiMatchQueryBuilder("lisence", TEXT_FIELD_NAME, KEYWORD_FIELD_NAME);
        builder.type(MultiMatchQueryBuilder.Type.BOOL_PREFIX);
        builder.fuzziness(Fuzziness.ONE);
        query = builder.toQuery(context);
        
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery multiFieldQuery = (BooleanQuery) query;
        assertThat(multiFieldQuery.clauses().size(), equalTo(2));
        
        // Test with multiple terms including a misspelled one
        builder = new MultiMatchQueryBuilder("opensarch lisence", TEXT_FIELD_NAME);
        builder.type(MultiMatchQueryBuilder.Type.BOOL_PREFIX);
        builder.fuzziness(Fuzziness.ONE);
        query = builder.toQuery(context);
        
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery multiTermMisspelledQuery = (BooleanQuery) query;
        assertThat(multiTermMisspelledQuery.clauses().size(), equalTo(2));
        
        // Check first term
        BooleanClause clause1 = multiTermMisspelledQuery.clauses().get(0);
        Query innerQuery1 = clause1.query();
        assertThat(innerQuery1, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery1 = (FuzzyQuery) innerQuery1;
        assertThat(fuzzyQuery1.getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "opensarch")));
        
        // Check second term
        BooleanClause clause2 = multiTermMisspelledQuery.clauses().get(1);
        Query innerQuery2 = clause2.query();
        assertThat(innerQuery2, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery2 = (FuzzyQuery) innerQuery2;
        assertThat(fuzzyQuery2.getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "lisence")));
    }
}
