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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.opensearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DisMaxQueryBuilderTests extends AbstractQueryTestCase<DisMaxQueryBuilder> {
    /**
     * @return a {@link DisMaxQueryBuilder} with random inner queries
     */
    @Override
    protected DisMaxQueryBuilder doCreateTestQueryBuilder() {
        DisMaxQueryBuilder dismax = new DisMaxQueryBuilder();
        int clauses = randomIntBetween(1, 5);
        for (int i = 0; i < clauses; i++) {
            dismax.add(RandomQueryBuilder.createQuery(random()));
        }
        if (randomBoolean()) {
            dismax.tieBreaker((float) randomDoubleBetween(0d, 1d, true));
        }
        return dismax;
    }

    @Override
    protected void doAssertLuceneQuery(DisMaxQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        Collection<Query> queries = AbstractQueryBuilder.toQueries(queryBuilder.innerQueries(), context);
        Query expected = new DisjunctionMaxQuery(queries, queryBuilder.tieBreaker());
        assertEquals(expected, query);
    }

    @Override
    protected Map<String, DisMaxQueryBuilder> getAlternateVersions() {
        Map<String, DisMaxQueryBuilder> alternateVersions = new HashMap<>();
        QueryBuilder innerQuery = createTestQueryBuilder().innerQueries().get(0);
        DisMaxQueryBuilder expectedQuery = new DisMaxQueryBuilder();
        expectedQuery.add(innerQuery);
        String contentString = "{\n" + "    \"dis_max\" : {\n" + "        \"queries\" : " + innerQuery.toString() + "    }\n" + "}";
        alternateVersions.put(contentString, expectedQuery);
        return alternateVersions;
    }

    public void testIllegalArguments() {
        DisMaxQueryBuilder disMaxQuery = new DisMaxQueryBuilder();
        expectThrows(IllegalArgumentException.class, () -> disMaxQuery.add(null));
    }

    public void testToQueryInnerPrefixQuery() throws Exception {
        String queryAsString = "{\n"
            + "    \"dis_max\":{\n"
            + "        \"queries\":[\n"
            + "            {\n"
            + "                \"prefix\":{\n"
            + "                    \""
            + TEXT_FIELD_NAME
            + "\":{\n"
            + "                        \"value\":\"sh\",\n"
            + "                        \"boost\":1.2\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        ]\n"
            + "    }\n"
            + "}";
        Query query = parseQuery(queryAsString).toQuery(createShardContext());
        Query expected = new DisjunctionMaxQuery(
            List.of(new BoostQuery(new PrefixQuery(new Term(TEXT_FIELD_NAME, "sh"), MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE), 1.2f)),
            0
        );
        assertEquals(expected, query);
    }

    public void testFromJson() throws IOException {
        String json = "{\n"
            + "  \"dis_max\" : {\n"
            + "    \"tie_breaker\" : 0.7,\n"
            + "    \"queries\" : [ {\n"
            + "      \"term\" : {\n"
            + "        \"age\" : {\n"
            + "          \"value\" : 34,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    }, {\n"
            + "      \"term\" : {\n"
            + "        \"age\" : {\n"
            + "          \"value\" : 35,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    } ],\n"
            + "    \"boost\" : 1.2\n"
            + "  }\n"
            + "}";

        DisMaxQueryBuilder parsed = (DisMaxQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 1.2, parsed.boost(), 0.0001);
        assertEquals(json, 0.7, parsed.tieBreaker(), 0.0001);
        assertEquals(json, 2, parsed.innerQueries().size());
    }

    public void testRewriteMultipleTimes() throws IOException {
        DisMaxQueryBuilder dismax = new DisMaxQueryBuilder();
        dismax.add(new WrapperQueryBuilder(new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()).toString()));
        QueryBuilder rewritten = dismax.rewrite(createShardContext());
        DisMaxQueryBuilder expected = new DisMaxQueryBuilder();
        expected.add(new MatchAllQueryBuilder());
        assertEquals(expected, rewritten);

        expected = new DisMaxQueryBuilder();
        expected.add(new MatchAllQueryBuilder());
        QueryBuilder rewrittenAgain = rewritten.rewrite(createShardContext());
        assertEquals(rewrittenAgain, expected);
        assertEquals(Rewriteable.rewrite(dismax, createShardContext()), expected);
    }

    public void testVisit() {
        DisMaxQueryBuilder dismax = new DisMaxQueryBuilder();
        dismax.add(new WrapperQueryBuilder(new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()).toString()));

        List<QueryBuilder> visitedQueries = new ArrayList<>();
        dismax.visit(createTestVisitor(visitedQueries));

        assertEquals(2, visitedQueries.size());
    }
}
