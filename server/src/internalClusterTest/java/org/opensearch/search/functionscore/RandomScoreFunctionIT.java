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

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.ArrayUtil;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.RandomScoreFunctionBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.ScoreAccessor;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchHit;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.hamcrest.CoreMatchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.functionScoreQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.opensearch.index.query.functionscore.ScoreFunctionBuilders.randomFunction;
import static org.opensearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.opensearch.script.MockScriptPlugin.NAME;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

public class RandomScoreFunctionIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public RandomScoreFunctionIT(Settings staticSettings) {
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
        return Arrays.asList(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("log(doc['index'].value + (factor * _score))", vars -> scoringScript(vars, ScoreAccessor::doubleValue));
            scripts.put("log(doc['index'].value + (factor * _score.intValue()))", vars -> scoringScript(vars, ScoreAccessor::intValue));
            scripts.put("log(doc['index'].value + (factor * _score.longValue()))", vars -> scoringScript(vars, ScoreAccessor::longValue));
            scripts.put("log(doc['index'].value + (factor * _score.floatValue()))", vars -> scoringScript(vars, ScoreAccessor::floatValue));
            scripts.put(
                "log(doc['index'].value + (factor * _score.doubleValue()))",
                vars -> scoringScript(vars, ScoreAccessor::doubleValue)
            );
            return scripts;
        }

        static Double scoringScript(Map<String, Object> vars, Function<ScoreAccessor, Number> scoring) {
            Map<?, ?> doc = (Map) vars.get("doc");
            Double index = ((Number) ((ScriptDocValues<?>) doc.get("index")).get(0)).doubleValue();
            Double score = scoring.apply((ScoreAccessor) vars.get("_score")).doubleValue();
            Integer factor = (Integer) vars.get("factor");
            return Math.log(index + (factor * score));
        }
    }

    public void testConsistentHitsWithSameSeed() throws Exception {
        createIndex("test");
        ensureGreen(); // make sure we are done otherwise preference could change?
        int docCount = randomIntBetween(100, 200);
        for (int i = 0; i < docCount; i++) {
            index("test", "type", "" + i, jsonBuilder().startObject().field("foo", i).endObject());
        }
        flush();
        refresh();
        indexRandomForConcurrentSearch("test");
        int outerIters = scaledRandomIntBetween(10, 20);
        for (int o = 0; o < outerIters; o++) {
            final int seed = randomInt();
            String preference = randomRealisticUnicodeOfLengthBetween(1, 10); // at least one char!!
            // randomPreference should not start with '_' (reserved for known preference types (e.g. _shards, _primary)
            while (preference.startsWith("_")) {
                preference = randomRealisticUnicodeOfLengthBetween(1, 10);
            }
            int innerIters = scaledRandomIntBetween(2, 5);
            SearchHit[] hits = null;
            for (int i = 0; i < innerIters; i++) {
                SearchResponse searchResponse = client().prepareSearch()
                    .setSize(docCount) // get all docs otherwise we are prone to tie-breaking
                    .setPreference(preference)
                    .setQuery(functionScoreQuery(matchAllQuery(), randomFunction().seed(seed).setField("foo")))
                    .get();
                assertThat(
                    "Failures " + Arrays.toString(searchResponse.getShardFailures()),
                    searchResponse.getShardFailures().length,
                    CoreMatchers.equalTo(0)
                );
                final int hitCount = searchResponse.getHits().getHits().length;
                final SearchHit[] currentHits = searchResponse.getHits().getHits();
                ArrayUtil.timSort(currentHits, (o1, o2) -> {
                    // for tie-breaking we have to resort here since if the score is
                    // identical we rely on collection order which might change.
                    int cmp = Float.compare(o1.getScore(), o2.getScore());
                    return cmp == 0 ? o1.getId().compareTo(o2.getId()) : cmp;
                });
                if (i == 0) {
                    assertThat(hits, nullValue());
                    hits = currentHits;
                } else {
                    assertThat(hits.length, equalTo(searchResponse.getHits().getHits().length));
                    for (int j = 0; j < hitCount; j++) {
                        assertThat("" + j, currentHits[j].getScore(), equalTo(hits[j].getScore()));
                        assertThat("" + j, currentHits[j].getId(), equalTo(hits[j].getId()));
                    }
                }

                // randomly change some docs to get them in different segments
                int numDocsToChange = randomIntBetween(20, 50);
                while (numDocsToChange > 0) {
                    int doc = randomInt(docCount - 1);// watch out this is inclusive the max values!
                    index("test", "type", "" + doc, jsonBuilder().startObject().field("foo", doc).endObject());
                    --numDocsToChange;
                }
                flush();
                refresh();
            }
        }
    }

    public void testScoreAccessWithinScript() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                "body",
                "type=text",
                "index",
                "type=" + randomFrom("short", "float", "long", "integer", "double")
            )
        );

        int docCount = randomIntBetween(100, 200);
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex("test")
                .setId("" + i)
                // we add 1 to the index field to make sure that the scripts below never compute log(0)
                .setSource("body", randomFrom(Arrays.asList("foo", "bar", "baz")), "index", i + 1)
                .get();
        }
        refresh();
        indexRandomForConcurrentSearch("test");

        Map<String, Object> params = new HashMap<>();
        params.put("factor", randomIntBetween(2, 4));

        // Test for accessing _score
        Script script = new Script(ScriptType.INLINE, NAME, "log(doc['index'].value + (factor * _score))", params);
        SearchResponse resp = client().prepareSearch("test")
            .setQuery(
                functionScoreQuery(
                    matchQuery("body", "foo"),
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(fieldValueFactorFunction("index").factor(2)),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(script)) }
                )
            )
            .get();
        assertNoFailures(resp);
        SearchHit firstHit = resp.getHits().getAt(0);
        assertThat(firstHit.getScore(), greaterThan(1f));

        // Test for accessing _score.intValue()
        script = new Script(ScriptType.INLINE, NAME, "log(doc['index'].value + (factor * _score.intValue()))", params);
        resp = client().prepareSearch("test")
            .setQuery(
                functionScoreQuery(
                    matchQuery("body", "foo"),
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(fieldValueFactorFunction("index").factor(2)),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(script)) }
                )
            )
            .get();
        assertNoFailures(resp);
        firstHit = resp.getHits().getAt(0);
        assertThat(firstHit.getScore(), greaterThan(1f));

        // Test for accessing _score.longValue()
        script = new Script(ScriptType.INLINE, NAME, "log(doc['index'].value + (factor * _score.longValue()))", params);
        resp = client().prepareSearch("test")
            .setQuery(
                functionScoreQuery(
                    matchQuery("body", "foo"),
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(fieldValueFactorFunction("index").factor(2)),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(script)) }
                )
            )
            .get();
        assertNoFailures(resp);
        firstHit = resp.getHits().getAt(0);
        assertThat(firstHit.getScore(), greaterThan(1f));

        // Test for accessing _score.floatValue()
        script = new Script(ScriptType.INLINE, NAME, "log(doc['index'].value + (factor * _score.floatValue()))", params);
        resp = client().prepareSearch("test")
            .setQuery(
                functionScoreQuery(
                    matchQuery("body", "foo"),
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(fieldValueFactorFunction("index").factor(2)),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(script)) }
                )
            )
            .get();
        assertNoFailures(resp);
        firstHit = resp.getHits().getAt(0);
        assertThat(firstHit.getScore(), greaterThan(1f));

        // Test for accessing _score.doubleValue()
        script = new Script(ScriptType.INLINE, NAME, "log(doc['index'].value + (factor * _score.doubleValue()))", params);
        resp = client().prepareSearch("test")
            .setQuery(
                functionScoreQuery(
                    matchQuery("body", "foo"),
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(fieldValueFactorFunction("index").factor(2)),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(script)) }
                )
            )
            .get();
        assertNoFailures(resp);
        firstHit = resp.getHits().getAt(0);
        assertThat(firstHit.getScore(), greaterThan(1f));
    }

    public void testSeedReportedInExplain() throws Exception {
        createIndex("test");
        ensureGreen();
        index("test", "type", "1", jsonBuilder().startObject().endObject());
        flush();
        refresh();
        indexRandomForConcurrentSearch("test");

        int seed = 12345678;

        SearchResponse resp = client().prepareSearch("test")
            .setQuery(functionScoreQuery(matchAllQuery(), randomFunction().seed(seed).setField(SeqNoFieldMapper.NAME)))
            .setExplain(true)
            .get();
        assertNoFailures(resp);
        assertEquals(1, resp.getHits().getTotalHits().value());
        SearchHit firstHit = resp.getHits().getAt(0);
        assertThat(firstHit.getExplanation().toString(), containsString("" + seed));
    }

    public void testSeedAndNameReportedInExplain() throws Exception {
        createIndex("test");
        ensureGreen();
        index("test", "type", "1", jsonBuilder().startObject().endObject());
        flush();
        refresh();
        indexRandomForConcurrentSearch("test");

        int seed = 12345678;

        final String queryName = "query1";
        final String functionName = "func1";
        SearchResponse resp = client().prepareSearch("test")
            .setQuery(
                functionScoreQuery(
                    matchAllQuery().queryName(queryName),
                    randomFunction(functionName).seed(seed).setField(SeqNoFieldMapper.NAME)
                )
            )
            .setExplain(true)
            .get();
        assertNoFailures(resp);
        assertEquals(1, resp.getHits().getTotalHits().value());
        SearchHit firstHit = resp.getHits().getAt(0);
        assertThat(firstHit.getExplanation().getDetails(), arrayWithSize(2));
        // "description": "*:* (_name: query1)"
        assertThat(firstHit.getExplanation().getDetails()[0].getDescription().toString(), containsString("_name: " + queryName));
        assertThat(firstHit.getExplanation().getDetails()[1].getDetails(), arrayWithSize(2));
        // "description": "random score function (seed: 12345678, field: _seq_no, _name: func1)"
        assertThat(firstHit.getExplanation().getDetails()[1].getDetails()[0].getDescription().toString(), containsString("seed: " + seed));
    }

    public void testNoDocs() throws Exception {
        createIndex("test");
        ensureGreen();

        SearchResponse resp = client().prepareSearch("test")
            .setQuery(functionScoreQuery(matchAllQuery(), randomFunction().seed(1234).setField(SeqNoFieldMapper.NAME)))
            .get();
        assertNoFailures(resp);
        assertEquals(0, resp.getHits().getTotalHits().value());

        resp = client().prepareSearch("test").setQuery(functionScoreQuery(matchAllQuery(), randomFunction())).get();
        assertNoFailures(resp);
        assertEquals(0, resp.getHits().getTotalHits().value());
    }

    public void testScoreRange() throws Exception {
        // all random scores should be in range [0.0, 1.0]
        createIndex("test");
        ensureGreen();
        int docCount = randomIntBetween(100, 200);
        for (int i = 0; i < docCount; i++) {
            String id = randomRealisticUnicodeOfCodepointLengthBetween(1, 50);
            index("test", "type", id, jsonBuilder().startObject().endObject());
        }
        flush();
        refresh();
        indexRandomForConcurrentSearch("test");
        int iters = scaledRandomIntBetween(10, 20);
        for (int i = 0; i < iters; ++i) {
            SearchResponse searchResponse = client().prepareSearch()
                .setQuery(functionScoreQuery(matchAllQuery(), randomFunction()))
                .setSize(docCount)
                .get();

            assertNoFailures(searchResponse);
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                assertThat(hit.getScore(), allOf(greaterThanOrEqualTo(0.0f), lessThanOrEqualTo(1.0f)));
            }
        }
    }

    public void testSeeds() throws Exception {
        createIndex("test");
        ensureGreen();
        final int docCount = randomIntBetween(100, 200);
        for (int i = 0; i < docCount; i++) {
            index("test", "type", "" + i, jsonBuilder().startObject().endObject());
        }
        flushAndRefresh();
        indexRandomForConcurrentSearch("test");

        assertNoFailures(
            client().prepareSearch()
                .setSize(docCount) // get all docs otherwise we are prone to tie-breaking
                .setQuery(functionScoreQuery(matchAllQuery(), randomFunction().seed(randomInt()).setField(SeqNoFieldMapper.NAME)))
                .get()
        );

        assertNoFailures(
            client().prepareSearch()
                .setSize(docCount) // get all docs otherwise we are prone to tie-breaking
                .setQuery(functionScoreQuery(matchAllQuery(), randomFunction().seed(randomLong()).setField(SeqNoFieldMapper.NAME)))
                .get()
        );

        assertNoFailures(
            client().prepareSearch()
                .setSize(docCount) // get all docs otherwise we are prone to tie-breaking
                .setQuery(
                    functionScoreQuery(
                        matchAllQuery(),
                        randomFunction().seed(randomRealisticUnicodeOfLengthBetween(10, 20)).setField(SeqNoFieldMapper.NAME)
                    )
                )
                .get()
        );
    }

    public void checkDistribution() throws Exception {
        int count = 10000;

        assertAcked(prepareCreate("test"));
        ensureGreen();

        for (int i = 0; i < count; i++) {
            index("test", "type", "" + i, jsonBuilder().startObject().endObject());
        }

        flush();
        refresh();

        int[] matrix = new int[count];

        for (int i = 0; i < count; i++) {

            SearchResponse searchResponse = client().prepareSearch()
                .setQuery(functionScoreQuery(matchAllQuery(), new RandomScoreFunctionBuilder()))
                .get();

            matrix[Integer.valueOf(searchResponse.getHits().getAt(0).getId())]++;
        }

        int filled = 0;
        int maxRepeat = 0;
        int sumRepeat = 0;
        for (int i = 0; i < matrix.length; i++) {
            int value = matrix[i];
            sumRepeat += value;
            maxRepeat = Math.max(maxRepeat, value);
            if (value > 0) {
                filled++;
            }
        }

        logger.info("max repeat: {}", maxRepeat);
        logger.info("avg repeat: {}", sumRepeat / (double) filled);
        logger.info("distribution: {}", filled / (double) count);

        int percentile50 = filled / 2;
        int percentile25 = (filled / 4);
        int percentile75 = percentile50 + percentile25;

        int sum = 0;

        for (int i = 0; i < matrix.length; i++) {
            if (matrix[i] == 0) {
                continue;
            }
            sum += i * matrix[i];
            if (percentile50 == 0) {
                logger.info("median: {}", i);
            } else if (percentile25 == 0) {
                logger.info("percentile_25: {}", i);
            } else if (percentile75 == 0) {
                logger.info("percentile_75: {}", i);
            }
            percentile50--;
            percentile25--;
            percentile75--;
        }

        logger.info("mean: {}", sum / (double) count);
    }
}
