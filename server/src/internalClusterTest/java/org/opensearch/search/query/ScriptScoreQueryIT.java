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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Bits;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.ScoreScript;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptType;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.idsQuery;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.index.query.QueryBuilders.scriptScoreQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertFirstHit;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertOrderedSearchHits;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSecondHit;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertThirdHit;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.hasScore;

public class ScriptScoreQueryIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public ScriptScoreQueryIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomScriptPlugin.class, BulkScoringScriptPlugin.class);
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

    // test case added for issue https://github.com/opensearch-project/OpenSearch/issues/18446
    public void testScriptScoreNotExistingQuery() throws Exception {
        assertAcked(prepareCreate("test-index").setMapping("field2", "type=double"));
        client().prepareIndex("test-index").setId("1").setSource("field2", 1).get();
        refresh();
        indexRandomForConcurrentSearch("test-index");

        Map<String, Object> params = new HashMap<>();
        params.put("param1", 0.1);
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['field2'].value * param1", params);
        QueryBuilder idsQuery = idsQuery().addIds("2");
        QueryBuilder scriptScoreQuery = scriptScoreQuery(idsQuery, script);
        // Issue 18446 arises because of null Scorer returned from ScorerSupplier.
        // However, Lucene prefers bulkScorer() instead of scorer() which doesn't trigger NPE as stated in issue 18446.
        // As a result, we have to set profile to true to force the usage of scorer() instead of bulkScorer(),
        // to make sure we test the intended code path
        SearchResponse resp = client().prepareSearch("test-index").setQuery(scriptScoreQuery).setProfile(true).get();
        assertNoFailures(resp);
    }

    // ===== BulkScoringLeafFactory integration tests =====

    public void testBulkScoringProducesCorrectScores() throws Exception {
        assertAcked(
            prepareCreate("bulk-test-index").setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("field1", "type=text")
        );
        client().prepareIndex("bulk-test-index").setId("1").setSource("field1", "foo").get();
        client().prepareIndex("bulk-test-index").setId("2").setSource("field1", "foo").get();
        client().prepareIndex("bulk-test-index").setId("3").setSource("field1", "foo").get();
        refresh();
        indexRandomForConcurrentSearch("bulk-test-index");

        Script script = new Script(ScriptType.INLINE, BulkScoringScriptEngine.NAME, "bulk_score", Collections.emptyMap());
        SearchResponse resp = client().prepareSearch("bulk-test-index")
            .setQuery(scriptScoreQuery(termQuery("field1", "foo"), script))
            .get();
        assertNoFailures(resp);
        assertEquals(3, resp.getHits().getTotalHits().value());
        for (var hit : resp.getHits().getHits()) {
            assertTrue("Score should be positive from bulk scorer, got: " + hit.getScore(), hit.getScore() > 0f);
        }
    }

    public void testBulkScoringBypassedWithMinScore() throws Exception {
        assertAcked(
            prepareCreate("bulk-test-minscore").setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("field1", "type=text")
        );
        client().prepareIndex("bulk-test-minscore").setId("1").setSource("field1", "foo").get();
        client().prepareIndex("bulk-test-minscore").setId("2").setSource("field1", "foo").get();
        refresh();
        indexRandomForConcurrentSearch("bulk-test-minscore");

        // When minScore is set, ScriptScoreQuery bypasses bulk scoring and uses per-doc ScriptScorer.
        // The per-doc path (newInstance) returns a fixed score of 2.0.
        Script script = new Script(ScriptType.INLINE, BulkScoringScriptEngine.NAME, "bulk_score", Collections.emptyMap());
        SearchResponse resp = client().prepareSearch("bulk-test-minscore")
            .setQuery(scriptScoreQuery(termQuery("field1", "foo"), script).setMinScore(1.5f))
            .get();
        assertNoFailures(resp);
        assertEquals(2, resp.getHits().getTotalHits().value());
        for (var hit : resp.getHits().getHits()) {
            assertEquals(2.0f, hit.getScore(), 0.001f);
        }
    }

    public void testBulkScoringWithBoost() throws Exception {
        assertAcked(
            prepareCreate("bulk-test-boost").setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("field1", "type=text")
        );
        client().prepareIndex("bulk-test-boost").setId("1").setSource("field1", "foo").get();
        refresh();
        indexRandomForConcurrentSearch("bulk-test-boost");

        float boost = 3.0f;
        Script script = new Script(ScriptType.INLINE, BulkScoringScriptEngine.NAME, "bulk_score", Collections.emptyMap());
        // Use a term filter to match only our specific doc, avoiding random docs from indexRandomForConcurrentSearch
        SearchResponse baseResp = client().prepareSearch("bulk-test-boost")
            .setQuery(scriptScoreQuery(termQuery("field1", "foo"), script))
            .get();
        assertNoFailures(baseResp);
        assertEquals(1, baseResp.getHits().getTotalHits().value());
        float baseScore = baseResp.getHits().getHits()[0].getScore();

        // Query with boost — score should be base * boost
        SearchResponse resp = client().prepareSearch("bulk-test-boost")
            .setQuery(scriptScoreQuery(termQuery("field1", "foo"), script).boost(boost))
            .get();
        assertNoFailures(resp);
        assertEquals(1, resp.getHits().getTotalHits().value());
        assertEquals(baseScore * boost, resp.getHits().getHits()[0].getScore(), 0.001f);
    }

    public void testBulkScoringFallbackWhenReturnsNull() throws Exception {
        assertAcked(
            prepareCreate("bulk-test-fallback").setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("field1", "type=text")
        );
        client().prepareIndex("bulk-test-fallback").setId("1").setSource("field1", "foo").get();
        refresh();
        indexRandomForConcurrentSearch("bulk-test-fallback");

        // "fallback_score" source triggers null from bulkScorer() → falls back to per-doc scoring (returns 2.0)
        Script script = new Script(ScriptType.INLINE, BulkScoringScriptEngine.NAME, "fallback_score", Collections.emptyMap());
        SearchResponse resp = client().prepareSearch("bulk-test-fallback")
            .setQuery(scriptScoreQuery(termQuery("field1", "foo"), script))
            .get();
        assertNoFailures(resp);
        assertEquals(1, resp.getHits().getTotalHits().value());
        assertEquals(2.0f, resp.getHits().getHits()[0].getScore(), 0.001f);
    }

    public void testBulkScoringExplainUsesPerDocPath() throws Exception {
        assertAcked(
            prepareCreate("bulk-test-explain").setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("field1", "type=text")
        );
        client().prepareIndex("bulk-test-explain").setId("1").setSource("field1", "foo").get();
        refresh();
        indexRandomForConcurrentSearch("bulk-test-explain");

        // The bulk scorer produces score = (docId + 1) * boost, but explain always uses the per-doc
        // path (newInstance → execute() returns 2.0). Verify explain returns the per-doc score.
        Script script = new Script(ScriptType.INLINE, BulkScoringScriptEngine.NAME, "bulk_score", Collections.emptyMap());
        SearchResponse resp = client().prepareSearch("bulk-test-explain")
            .setQuery(scriptScoreQuery(termQuery("field1", "foo"), script))
            .setExplain(true)
            .get();
        assertNoFailures(resp);
        assertEquals(1, resp.getHits().getTotalHits().value());
        assertNotNull(resp.getHits().getHits()[0].getExplanation());
        assertTrue(resp.getHits().getHits()[0].getExplanation().getDescription().contains("script score function, computed with script:"));
    }

    // ===== Bulk Scoring Script Plugin =====

    public static class BulkScoringScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new BulkScoringScriptEngine();
        }
    }

    /**
     * A minimal script engine that produces a {@link ScoreScript.BulkScoringLeafFactory}.
     * <p>
     * Source "bulk_score": bulk scorer assigns score = (segment-local docId + 1) * boost.
     * Source "fallback_score": bulkScorer() returns null, falling back to per-doc scoring with score = 2.0.
     */
    public static class BulkScoringScriptEngine implements ScriptEngine {

        public static final String NAME = "bulk_test";

        @Override
        public String getType() {
            return NAME;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T compile(String name, String source, ScriptContext<T> context, Map<String, String> params) {
            if (context.equals(ScoreScript.CONTEXT) == false) {
                throw new IllegalArgumentException("Unsupported context: " + context.name);
            }

            ScoreScript.Factory factory = (Map<String, Object> p, SearchLookup lookup, IndexSearcher indexSearcher) -> {
                boolean useBulk = "bulk_score".equals(source);
                return new BulkScoringLeafFactoryImpl(p, lookup, indexSearcher, useBulk);
            };
            return (T) factory;
        }

        @Override
        public Set<ScriptContext<?>> getSupportedContexts() {
            return Set.of(ScoreScript.CONTEXT);
        }
    }

    private static class BulkScoringLeafFactoryImpl implements ScoreScript.BulkScoringLeafFactory {

        private final Map<String, Object> params;
        private final SearchLookup lookup;
        private final IndexSearcher indexSearcher;
        private final boolean useBulk;

        BulkScoringLeafFactoryImpl(Map<String, Object> params, SearchLookup lookup, IndexSearcher indexSearcher, boolean useBulk) {
            this.params = params;
            this.lookup = lookup;
            this.indexSearcher = indexSearcher;
            this.useBulk = useBulk;
        }

        @Override
        public boolean needs_score() {
            return false;
        }

        @Override
        public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {
            return new ScoreScript(params, lookup, indexSearcher, ctx) {
                @Override
                public double execute(ExplanationHolder explanation) {
                    return 2.0;
                }
            };
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context, BulkScorer subQueryBulkScorer, ScoreMode subQueryScoreMode, float boost)
            throws IOException {
            if (useBulk == false) {
                return null;
            }
            return new SimpleBulkScorer(subQueryBulkScorer, boost);
        }
    }

    /**
     * A simple bulk scorer that drives the sub-query's bulk scorer to discover matched docs,
     * then assigns each document a score of (docId + 1) * boost.
     */
    private static class SimpleBulkScorer extends BulkScorer {

        private final BulkScorer subQueryBulkScorer;
        private final float boost;

        SimpleBulkScorer(BulkScorer subQueryBulkScorer, float boost) {
            this.subQueryBulkScorer = subQueryBulkScorer;
            this.boost = boost;
        }

        @Override
        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            final float[] currentScore = new float[1];
            Scorable scorable = new Scorable() {
                @Override
                public float score() {
                    return currentScore[0];
                }
            };
            collector.setScorer(scorable);

            return subQueryBulkScorer.score(new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                    currentScore[0] = (doc + 1) * boost;
                    collector.collect(doc);
                }
            }, acceptDocs, min, max);
        }

        @Override
        public long cost() {
            return subQueryBulkScorer.cost();
        }
    }
}
