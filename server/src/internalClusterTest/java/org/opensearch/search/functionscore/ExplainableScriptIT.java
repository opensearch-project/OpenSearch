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

package org.opensearch.search.functionscore;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.common.lucene.search.function.Functions;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.script.ExplainableScoreScript;
import org.opensearch.script.ScoreScript;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.opensearch.client.Requests.searchRequest;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.functionScoreQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.opensearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class ExplainableScriptIT extends OpenSearchIntegTestCase {

    public static class ExplainableScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return "test";
                }

                @Override
                public <T> T compile(String scriptName, String scriptSource, ScriptContext<T> context, Map<String, String> params) {
                    assert scriptSource.equals("explainable_script");
                    assert context == ScoreScript.CONTEXT;
                    ScoreScript.Factory factory = (params1, lookup) -> new ScoreScript.LeafFactory() {
                        @Override
                        public boolean needs_score() {
                            return false;
                        }

                        @Override
                        public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {
                            return new MyScript(params1, lookup, ctx);
                        }
                    };
                    return context.factoryClazz.cast(factory);
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Collections.singleton(ScoreScript.CONTEXT);
                }
            };
        }
    }

    static class MyScript extends ScoreScript implements ExplainableScoreScript {

        MyScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
            super(params, lookup, leafContext);
        }

        @Override
        public Explanation explain(Explanation subQueryScore) throws IOException {
            return explain(subQueryScore, null);
        }

        @Override
        public Explanation explain(Explanation subQueryScore, String functionName) throws IOException {
            Explanation scoreExp = Explanation.match(subQueryScore.getValue(), "_score: ", subQueryScore);
            return Explanation.match(
                (float) (execute(null)),
                "This script" + Functions.nameOrEmptyFunc(functionName) + " returned " + execute(null),
                scoreExp
            );
        }

        @Override
        public double execute(ExplanationHolder explanation) {
            return ((Number) ((ScriptDocValues) getDoc().get("number_field")).get(0)).doubleValue();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ExplainableScriptPlugin.class);
    }

    public void testExplainScript() throws InterruptedException, IOException, ExecutionException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            indexRequests.add(
                client().prepareIndex("test")
                    .setId(Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("number_field", i).field("text", "text").endObject())
            );
        }
        indexRandom(true, true, indexRequests);
        client().admin().indices().prepareRefresh().get();
        ensureYellow();
        SearchResponse response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().explain(true)
                        .query(
                            functionScoreQuery(
                                termQuery("text", "text"),
                                scriptFunction(new Script(ScriptType.INLINE, "test", "explainable_script", Collections.emptyMap()))
                            ).boostMode(CombineFunction.REPLACE)
                        )
                )
        ).actionGet();

        OpenSearchAssertions.assertNoFailures(response);
        SearchHits hits = response.getHits();
        assertThat(hits.getTotalHits().value, equalTo(20L));
        int idCounter = 19;
        for (SearchHit hit : hits.getHits()) {
            assertThat(hit.getId(), equalTo(Integer.toString(idCounter)));
            assertThat(hit.getExplanation().toString(), containsString(Double.toString(idCounter)));

            // Since Apache Lucene 9.8, the scores are not computed because script (see please ExplainableScriptPlugin)
            // says "needs_score() == false"
            // 19.0 = min of:
            // 19.0 = This script returned 19.0
            // 0.0 = _score:
            // 0.0 = weight(text:text in 0) [PerFieldSimilarity], result of:
            // 0.0 = score(freq=1.0), with freq of:
            // 1.0 = freq, occurrences of term within document
            // 3.4028235E38 = maxBoost

            assertThat(hit.getExplanation().toString(), containsString("1.0 = freq, occurrences of term within document"));
            assertThat(hit.getExplanation().getDetails().length, equalTo(2));
            idCounter--;
        }
    }

    public void testExplainScriptWithName() throws InterruptedException, IOException, ExecutionException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(
            client().prepareIndex("test")
                .setId(Integer.toString(1))
                .setSource(jsonBuilder().startObject().field("number_field", 1).field("text", "text").endObject())
        );
        indexRandom(true, true, indexRequests);
        client().admin().indices().prepareRefresh().get();
        ensureYellow();
        SearchResponse response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().explain(true)
                        .query(
                            functionScoreQuery(
                                termQuery("text", "text"),
                                scriptFunction(new Script(ScriptType.INLINE, "test", "explainable_script", Collections.emptyMap()), "func1")
                            ).boostMode(CombineFunction.REPLACE)
                        )
                )
        ).actionGet();

        OpenSearchAssertions.assertNoFailures(response);
        SearchHits hits = response.getHits();
        assertThat(hits.getTotalHits().value, equalTo(1L));
        assertThat(hits.getHits()[0].getId(), equalTo("1"));
        assertThat(hits.getHits()[0].getExplanation().getDetails(), arrayWithSize(2));
        assertThat(hits.getHits()[0].getExplanation().getDetails()[0].getDescription(), containsString("_name: func1"));
    }

}
