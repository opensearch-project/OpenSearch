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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.core.common.ParsingException;
import org.opensearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.containsString;

public class ConstantScoreQueryBuilderTests extends AbstractQueryTestCase<ConstantScoreQueryBuilder> {
    /**
     * @return a {@link ConstantScoreQueryBuilder} with random boost between 0.1f and 2.0f
     */
    @Override
    protected ConstantScoreQueryBuilder doCreateTestQueryBuilder() {
        return new ConstantScoreQueryBuilder(RandomQueryBuilder.createQuery(random()));
    }

    @Override
    protected void doAssertLuceneQuery(ConstantScoreQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        Query innerQuery = queryBuilder.innerQuery().rewrite(context).toQuery(context);
        if (innerQuery == null) {
            assertThat(query, nullValue());
        } else if (innerQuery instanceof MatchNoDocsQuery) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else {
            assertThat(query, instanceOf(ConstantScoreQuery.class));
            ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) query;
            assertThat(constantScoreQuery.getQuery(), instanceOf(innerQuery.getClass()));
        }
    }

    /**
     * test that missing "filter" element causes {@link ParsingException}
     */
    public void testFilterElement() throws IOException {
        String queryString = "{ \"" + ConstantScoreQueryBuilder.NAME + "\" : {} }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(queryString));
        assertThat(e.getMessage(), containsString("requires a 'filter' element"));
    }

    /**
     * test that "filter" does not accept an array of queries, throws {@link ParsingException}
     */
    public void testNoArrayAsFilterElements() throws IOException {
        String queryString = "{ \""
            + ConstantScoreQueryBuilder.NAME
            + "\" : {\n"
            + "\"filter\" : [ { \"term\": { \"foo\": \"a\" } },\n"
            + "{ \"term\": { \"foo\": \"x\" } } ]\n"
            + "} }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(queryString));
        assertThat(e.getMessage(), containsString("unexpected token [START_ARRAY]"));
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new ConstantScoreQueryBuilder((QueryBuilder) null));
    }

    @Override
    public void testUnknownField() {
        assumeTrue("test doesn't apply for query filter queries", false);
    }

    public void testFromJson() throws IOException {
        String json = "{\n"
            + "  \"constant_score\" : {\n"
            + "    \"filter\" : {\n"
            + "      \"terms\" : {\n"
            + "        \"user\" : [ \"foobar\", \"opensearch\" ],\n"
            + "        \"boost\" : 42.0\n"
            + "      }\n"
            + "    },\n"
            + "    \"boost\" : 23.0\n"
            + "  }\n"
            + "}";

        ConstantScoreQueryBuilder parsed = (ConstantScoreQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 23.0, parsed.boost(), 0.0001);
        assertEquals(json, 42.0, parsed.innerQuery().boost(), 0.0001);
    }

    public void testRewriteToMatchNone() throws IOException {
        ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(new MatchNoneQueryBuilder());
        QueryBuilder rewrite = constantScoreQueryBuilder.rewrite(createShardContext());
        assertEquals(rewrite, new MatchNoneQueryBuilder());
    }

    @Override
    public void testMustRewrite() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);
        ConstantScoreQueryBuilder queryBuilder = new ConstantScoreQueryBuilder(new TermQueryBuilder("unmapped_field", "foo"));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }

    public void testVisit() {
        ConstantScoreQueryBuilder queryBuilder = new ConstantScoreQueryBuilder(new TermQueryBuilder("unmapped_field", "foo"));
        List<QueryBuilder> visitorQueries = new ArrayList<>();
        queryBuilder.visit(createTestVisitor(visitorQueries));

        assertEquals(2, visitorQueries.size());
    }

    public void testFilter() {
        // Test for non null filter
        BoolQueryBuilder filterBuilder = new BoolQueryBuilder();
        ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(filterBuilder);
        QueryBuilder filter = QueryBuilders.matchAllQuery();
        constantScoreQueryBuilder.filter(filter);
        assertEquals(1, filterBuilder.filter().size());
        assertEquals(filter, filterBuilder.filter().get(0));

        // Test for null filter
        filterBuilder = new BoolQueryBuilder();
        constantScoreQueryBuilder = new ConstantScoreQueryBuilder(filterBuilder);
        constantScoreQueryBuilder.filter(null);
        assertEquals(0, filterBuilder.filter().size());

    }
}
