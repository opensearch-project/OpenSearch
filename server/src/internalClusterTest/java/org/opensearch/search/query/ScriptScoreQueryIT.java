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

package org.opensearch.search.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.test.ParameterizedOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.index.query.QueryBuilders.scriptScoreQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertFirstHit;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertOrderedSearchHits;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSecondHit;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertThirdHit;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.hasScore;

public class ScriptScoreQueryIT extends ParameterizedOpenSearchIntegTestCase {

    public ScriptScoreQueryIT(Settings dynamicSettings) {
        super(dynamicSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.CONCURRENT_SEGMENT_SEARCH, "true").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("doc['field2'].value * param1", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                ScriptDocValues.Doubles field2Values = (ScriptDocValues.Doubles) doc.get("field2");
                Double param1 = (Double) vars.get("param1");
                return field2Values.getValue() * param1;
            });
            return scripts;
        }
    }

    // test that script_score works as expected:
    // 1) only matched docs retrieved
    // 2) score is calculated based on a script with params
    // 3) min score applied
    public void testScriptScore() throws Exception {
        assertAcked(prepareCreate("test-index").setMapping("field1", "type=text", "field2", "type=double"));
        int docCount = 10;
        for (int i = 1; i <= docCount; i++) {
            client().prepareIndex("test-index").setId("" + i).setSource("field1", "text" + (i % 2), "field2", i).get();
        }
        refresh();
        indexRandomForConcurrentSearch("test-index");

        Map<String, Object> params = new HashMap<>();
        params.put("param1", 0.1);
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['field2'].value * param1", params);
        SearchResponse resp = client().prepareSearch("test-index").setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script)).get();
        assertNoFailures(resp);
        assertOrderedSearchHits(resp, "10", "8", "6", "4", "2");
        assertFirstHit(resp, hasScore(1.0f));
        assertSecondHit(resp, hasScore(0.8f));
        assertThirdHit(resp, hasScore(0.6f));

        // applying min score
        resp = client().prepareSearch("test-index")
            .setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script).setMinScore(0.6f))
            .get();
        assertNoFailures(resp);
        assertOrderedSearchHits(resp, "10", "8", "6");
    }

    public void testScriptScoreBoolQuery() throws Exception {
        assertAcked(prepareCreate("test-index").setMapping("field1", "type=text", "field2", "type=double"));
        int docCount = 10;
        for (int i = 1; i <= docCount; i++) {
            client().prepareIndex("test-index").setId("" + i).setSource("field1", "text" + i, "field2", i).get();
        }
        refresh();
        indexRandomForConcurrentSearch("test-index");

        Map<String, Object> params = new HashMap<>();
        params.put("param1", 0.1);
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['field2'].value * param1", params);
        QueryBuilder boolQuery = boolQuery().should(matchQuery("field1", "text1")).should(matchQuery("field1", "text10"));
        SearchResponse resp = client().prepareSearch("test-index").setQuery(scriptScoreQuery(boolQuery, script)).get();
        assertNoFailures(resp);
        assertOrderedSearchHits(resp, "10", "1");
        assertFirstHit(resp, hasScore(1.0f));
        assertSecondHit(resp, hasScore(0.1f));
    }

    // test that when the internal query is rewritten script_score works well
    public void testRewrittenQuery() throws Exception {
        assertAcked(
            prepareCreate("test-index2").setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("field1", "type=date", "field2", "type=double")
        );
        client().prepareIndex("test-index2").setId("1").setSource("field1", "2019-09-01", "field2", 1).get();
        client().prepareIndex("test-index2").setId("2").setSource("field1", "2019-10-01", "field2", 2).get();
        client().prepareIndex("test-index2").setId("3").setSource("field1", "2019-11-01", "field2", 3).get();
        refresh();
        indexRandomForConcurrentSearch("test-index2");

        RangeQueryBuilder rangeQB = new RangeQueryBuilder("field1").from("2019-01-01"); // the query should be rewritten to from:null
        Map<String, Object> params = new HashMap<>();
        params.put("param1", 0.1);
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['field2'].value * param1", params);
        SearchResponse resp = client().prepareSearch("test-index2").setQuery(scriptScoreQuery(rangeQB, script)).get();
        assertNoFailures(resp);
        assertOrderedSearchHits(resp, "3", "2", "1");
    }

    public void testDisallowExpensiveQueries() throws Exception {
        try {
            assertAcked(prepareCreate("test-index").setMapping("field1", "type=text", "field2", "type=double"));
            int docCount = 10;
            for (int i = 1; i <= docCount; i++) {
                client().prepareIndex("test-index").setId("" + i).setSource("field1", "text" + (i % 2), "field2", i).get();
            }
            refresh();
            indexRandomForConcurrentSearch("test-index");

            Map<String, Object> params = new HashMap<>();
            params.put("param1", 0.1);

            // Execute with search.allow_expensive_queries = null => default value = true => success
            Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['field2'].value * param1", params);
            SearchResponse resp = client().prepareSearch("test-index")
                .setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script))
                .get();
            assertNoFailures(resp);

            // Set search.allow_expensive_queries to "false" => assert failure
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", false));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

            OpenSearchException e = expectThrows(
                OpenSearchException.class,
                () -> client().prepareSearch("test-index").setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script)).get()
            );
            assertEquals(
                "[script score] queries cannot be executed when 'search.allow_expensive_queries' is set to false.",
                e.getCause().getMessage()
            );

            // Set search.allow_expensive_queries to "true" => success
            updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", true));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
            resp = client().prepareSearch("test-index").setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script)).get();
            assertNoFailures(resp);
        } finally {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", (String) null));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        }
    }
}
