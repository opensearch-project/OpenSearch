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

package org.opensearch.search.aggregations.metrics;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.global.Global;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.global;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.search.aggregations.AggregationBuilders.scriptedMetric;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

@ClusterScope(scope = Scope.SUITE)
@OpenSearchIntegTestCase.SuiteScopeTestCase
public class ScriptedMetricIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static long numDocs;

    public ScriptedMetricIT(Settings staticSettings) {
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
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        @SuppressWarnings("unchecked")
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("state['count'] = 1", vars -> aggScript(vars, state -> state.put("count", 1)));

            scripts.put("state.list.add(1)", vars -> aggScript(vars, state -> {
                // Lazily populate state.list for tests without an init script
                if (state.containsKey("list") == false) {
                    state.put("list", new ArrayList());
                }

                ((List) state.get("list")).add(1);
            }));

            scripts.put(
                "state[param1] = param2",
                vars -> aggScript(
                    vars,
                    state -> state.put(
                        (String) XContentMapValues.extractValue("params.param1", vars),
                        XContentMapValues.extractValue("params.param2", vars)
                    )
                )
            );

            scripts.put("vars.multiplier = 3", vars -> {
                ((Map<String, Object>) vars.get("vars")).put("multiplier", 3);

                Map<String, Object> state = (Map<String, Object>) vars.get("state");
                state.put("list", new ArrayList());

                return state;
            });

            scripts.put("state.list.add(vars.multiplier)", vars -> aggScript(vars, state -> {
                // Lazily populate state.list for tests without an init script
                if (state.containsKey("list") == false) {
                    state.put("list", new ArrayList());
                }

                ((List) state.get("list")).add(XContentMapValues.extractValue("vars.multiplier", vars));
            }));

            // Equivalent to:
            //
            // newaggregation = [];
            // sum = 0;
            //
            // for (s in state.list) {
            // sum += s
            // };
            //
            // newaggregation.add(sum);
            // return newaggregation"
            //
            scripts.put("sum state values as a new aggregation", vars -> {
                List newAggregation = new ArrayList();
                Map<String, Object> state = (Map<String, Object>) vars.get("state");
                List<?> list = (List<?>) state.get("list");

                if (list != null) {
                    Integer sum = 0;
                    for (Object s : list) {
                        sum += ((Number) s).intValue();
                    }
                    newAggregation.add(sum);
                }
                return newAggregation;
            });

            scripts.put("no-op aggregation", vars -> { return (Map<String, Object>) vars.get("state"); });

            scripts.put("no-op list aggregation", vars -> { return (List<List<?>>) vars.get("states"); });

            // Equivalent to:
            //
            // newaggregation = [];
            // sum = 0;
            //
            // for (state in states) {
            // for (s in state) {
            // sum += s
            // }
            // };
            //
            // newaggregation.add(sum);
            // return newaggregation"
            //
            scripts.put("sum all states (lists) values as a new aggregation", vars -> {
                List newAggregation = new ArrayList();
                Integer sum = 0;

                List<List<?>> states = (List<List<?>>) vars.get("states");
                for (List<?> list : states) {
                    if (list != null) {
                        for (Object s : list) {
                            sum += ((Number) s).intValue();
                        }
                    }
                }
                newAggregation.add(sum);
                return newAggregation;
            });

            scripts.put("sum all states' state.list values as a new aggregation", vars -> {
                List newAggregation = new ArrayList();
                Integer sum = 0;

                List<Map<String, Object>> states = (List<Map<String, Object>>) vars.get("states");

                if (states == null) {
                    return newAggregation;
                }

                for (Map<String, Object> state : states) {
                    List<?> list = (List<?>) state.get("list");
                    if (list != null) {
                        for (Object s : list) {
                            sum += ((Number) s).intValue();
                        }
                    }
                }
                newAggregation.add(sum);
                return newAggregation;
            });

            // Equivalent to:
            //
            // newaggregation = [];
            // sum = 0;
            //
            // for (state in states) {
            // for (s in state) {
            // sum += s
            // }
            // };
            //
            // newaggregation.add(sum * multiplier);
            // return newaggregation"
            //
            scripts.put("multiplied sum all states (lists) values as a new aggregation", vars -> {
                Integer multiplier = (Integer) vars.get("multiplier");
                List newAggregation = new ArrayList();
                Integer sum = 0;

                List<List<?>> states = (List<List<?>>) vars.get("states");
                for (List<?> list : states) {
                    if (list != null) {
                        for (Object s : list) {
                            sum += ((Number) s).intValue();
                        }
                    }
                }
                newAggregation.add(sum * multiplier);
                return newAggregation;
            });

            return scripts;
        }

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> nonDeterministicPluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("state.data = Math.random()", vars -> aggScript(vars, state -> state.put("data", ScriptedMetricIT.randomDouble())));

            scripts.put(
                "state['count'] = Math.random() >= 0.5 ? 1 : 0",
                vars -> aggScript(vars, state -> state.put("count", ScriptedMetricIT.randomDouble() >= 0.5 ? 1 : 0))
            );

            scripts.put("return Math.random()", vars -> ScriptedMetricIT.randomDouble());

            return scripts;
        }

        @SuppressWarnings("unchecked")
        static Map<String, Object> aggScript(Map<String, Object> vars, Consumer<Map<String, Object>> fn) {
            Map<String, Object> aggState = (Map<String, Object>) vars.get("state");
            fn.accept(aggState);
            return aggState;
        }
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");

        List<IndexRequestBuilder> builders = new ArrayList<>();

        numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            builders.add(
                client().prepareIndex("idx")
                    .setId("" + i)
                    .setSource(
                        jsonBuilder().startObject().field("value", randomAlphaOfLengthBetween(5, 15)).field("l_value", i).endObject()
                    )
            );
        }
        indexRandom(true, builders);

        // creating an index to test the empty buckets functionality. The way it
        // works is by indexing
        // two docs {value: 0} and {value : 2}, then building a histogram agg
        // with interval 1 and with empty
        // buckets computed.. the empty bucket is the one associated with key
        // "1". then each test will have
        // to check that this bucket exists with the appropriate sub
        // aggregations.
        prepareCreate("empty_bucket_idx").setMapping("value", "type=integer").get();
        builders = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            builders.add(
                client().prepareIndex("empty_bucket_idx")
                    .setId("" + i)
                    .setSource(jsonBuilder().startObject().field("value", i * 2).endObject())
            );
        }

        // When using the MockScriptPlugin we can map Stored scripts to inline scripts:
        // the id of the stored script is used in test method while the source of the stored script
        // must match a predefined script from CustomScriptPlugin.pluginScripts() method
        assertAcked(
            client().admin()
                .cluster()
                .preparePutStoredScript()
                .setId("initScript_stored")
                .setContent(
                    new BytesArray(
                        "{\"script\": {\"lang\": \"" + MockScriptPlugin.NAME + "\"," + " \"source\": \"vars.multiplier = 3\"} }"
                    ),
                    MediaTypeRegistry.JSON
                )
        );

        assertAcked(
            client().admin()
                .cluster()
                .preparePutStoredScript()
                .setId("mapScript_stored")
                .setContent(
                    new BytesArray(
                        "{\"script\": {\"lang\": \"" + MockScriptPlugin.NAME + "\"," + " \"source\": \"state.list.add(vars.multiplier)\"} }"
                    ),
                    MediaTypeRegistry.JSON
                )
        );

        assertAcked(
            client().admin()
                .cluster()
                .preparePutStoredScript()
                .setId("combineScript_stored")
                .setContent(
                    new BytesArray(
                        "{\"script\": {\"lang\": \""
                            + MockScriptPlugin.NAME
                            + "\","
                            + " \"source\": \"sum state values as a new aggregation\"} }"
                    ),
                    MediaTypeRegistry.JSON
                )
        );

        assertAcked(
            client().admin()
                .cluster()
                .preparePutStoredScript()
                .setId("reduceScript_stored")
                .setContent(
                    new BytesArray(
                        "{\"script\": {\"lang\": \""
                            + MockScriptPlugin.NAME
                            + "\","
                            + " \"source\": \"sum all states (lists) values as a new aggregation\"} }"
                    ),
                    MediaTypeRegistry.JSON
                )
        );

        indexRandom(true, builders);
        ensureSearchable();
    }

    private Path config;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        config = createTempDir().resolve("config");
        final Path scripts = config.resolve("scripts");

        try {
            Files.createDirectories(scripts);
            // When using the MockScriptPlugin we can map File scripts to inline scripts:
            // the name of the file script is used in test method while the source of the file script
            // must match a predefined script from CustomScriptPlugin.pluginScripts() method
            Files.write(scripts.resolve("init_script.mockscript"), "vars.multiplier = 3".getBytes("UTF-8"));
            Files.write(scripts.resolve("map_script.mockscript"), "state.list.add(vars.multiplier)".getBytes("UTF-8"));
            Files.write(scripts.resolve("combine_script.mockscript"), "sum state values as a new aggregation".getBytes("UTF-8"));
            Files.write(
                scripts.resolve("reduce_script.mockscript"),
                "sum all states (lists) values as a new aggregation".getBytes("UTF-8")
            );
        } catch (IOException e) {
            throw new RuntimeException("failed to create scripts");
        }
    }

    @Override
    protected Path nodeConfigPath(int nodeOrdinal) {
        return config;
    }

    public void testMap() {
        Script mapScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "state['count'] = 1", Collections.emptyMap());
        Script combineScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "no-op aggregation", Collections.emptyMap());
        Script reduceScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "no-op list aggregation", Collections.emptyMap());

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(scriptedMetric("scripted").mapScript(mapScript).combineScript(combineScript).reduceScript(reduceScript))
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        // with script based aggregation, if it does not support reduce then aggregationList size
        // will be numShards * slicesCount
        assertThat(aggregationList.size(), greaterThanOrEqualTo(getNumShards("idx").numPrimaries));
        int numShardsRun = 0;
        for (Object object : aggregationList) {
            assertThat(object, notNullValue());
            assertThat(object, instanceOf(Map.class));
            Map<?, ?> map = (Map<?, ?>) object;
            assertThat(map.size(), lessThanOrEqualTo(1));
            if (map.size() == 1) {
                assertThat(map.get("count"), notNullValue());
                assertThat(map.get("count"), instanceOf(Number.class));
                assertThat(map.get("count"), equalTo((Number) 1));
                numShardsRun++;
            }
        }
        // We don't know how many shards will have documents but we need to make
        // sure that at least one shard ran the map script
        assertThat(numShardsRun, greaterThan(0));
    }

    public void testMapWithParams() {
        // Split the params up between the script and the aggregation.
        Map<String, Object> scriptParams = Collections.singletonMap("param1", "12");
        Map<String, Object> aggregationParams = Collections.singletonMap("param2", 1);

        Script mapScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "state[param1] = param2", scriptParams);
        Script combineScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "no-op aggregation", Collections.emptyMap());
        Script reduceScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "no-op list aggregation", Collections.emptyMap());

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                scriptedMetric("scripted").params(aggregationParams)
                    .mapScript(mapScript)
                    .combineScript(combineScript)
                    .reduceScript(reduceScript)
            )
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        // with script based aggregation, if it does not support reduce then aggregationList size
        // will be numShards * slicesCount
        assertThat(aggregationList.size(), greaterThanOrEqualTo(getNumShards("idx").numPrimaries));
        int numShardsRun = 0;
        for (Object object : aggregationList) {
            assertThat(object, notNullValue());
            assertThat(object, instanceOf(Map.class));
            Map<?, ?> map = (Map<?, ?>) object;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                assertThat(entry, notNullValue());
                assertThat(entry.getKey(), notNullValue());
                assertThat(entry.getKey(), instanceOf(String.class));
                assertThat(entry.getValue(), notNullValue());
                assertThat(entry.getValue(), instanceOf(Number.class));
                String stringValue = (String) entry.getKey();
                assertThat(stringValue, equalTo("12"));
                Number numberValue = (Number) entry.getValue();
                assertThat(numberValue, equalTo((Number) 1));
                numShardsRun++;
            }
        }
        assertThat(numShardsRun, greaterThan(0));
    }

    public void testInitMutatesParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);

        Map<String, Object> params = new HashMap<>();
        params.put("vars", varsMap);

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                scriptedMetric("scripted").params(params)
                    .initScript(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "vars.multiplier = 3", Collections.emptyMap()))
                    .mapScript(
                        new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "state.list.add(vars.multiplier)", Collections.emptyMap())
                    )
                    .combineScript(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "no-op aggregation", Collections.emptyMap()))
                    .reduceScript(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "no-op list aggregation", Collections.emptyMap()))
            )
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        // with script based aggregation, if it does not support reduce then aggregationList size
        // will be numShards * slicesCount
        assertThat(aggregationList.size(), greaterThanOrEqualTo(getNumShards("idx").numPrimaries));
        long totalCount = 0;
        for (Object object : aggregationList) {
            assertThat(object, notNullValue());
            assertThat(object, instanceOf(HashMap.class));
            Map<String, Object> map = (Map<String, Object>) object;
            assertThat(map, hasKey("list"));
            assertThat(map.get("list"), instanceOf(List.class));
            List<?> list = (List<?>) map.get("list");
            for (Object o : list) {
                assertThat(o, notNullValue());
                assertThat(o, instanceOf(Number.class));
                Number numberValue = (Number) o;
                assertThat(numberValue, equalTo((Number) 3));
                totalCount += numberValue.longValue();
            }
        }
        assertThat(totalCount, equalTo(numDocs * 3));
    }

    public void testMapCombineWithParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);

        Map<String, Object> params = new HashMap<>();
        params.put("vars", varsMap);

        Script mapScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "state.list.add(1)", Collections.emptyMap());
        Script combineScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum state values as a new aggregation",
            Collections.emptyMap()
        );
        Script reduceScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "no-op list aggregation", Collections.emptyMap());

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                scriptedMetric("scripted").params(params).mapScript(mapScript).combineScript(combineScript).reduceScript(reduceScript)
            )
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        // with script based aggregation, if it does not support reduce then aggregationList size
        // will be numShards * slicesCount
        assertThat(aggregationList.size(), greaterThanOrEqualTo(getNumShards("idx").numPrimaries));
        long totalCount = 0;
        for (Object object : aggregationList) {
            assertThat(object, notNullValue());
            assertThat(object, instanceOf(List.class));
            List<?> list = (List<?>) object;
            for (Object o : list) {
                assertThat(o, notNullValue());
                assertThat(o, instanceOf(Number.class));
                Number numberValue = (Number) o;
                // A particular shard may not have any documents stored on it so
                // we have to assume the lower bound may be 0. The check at the
                // bottom of the test method will make sure the count is correct
                assertThat(numberValue.longValue(), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(numDocs)));
                totalCount += numberValue.longValue();
            }
        }
        assertThat(totalCount, equalTo(numDocs));
    }

    public void testInitMapCombineWithParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);

        Map<String, Object> params = new HashMap<>();
        params.put("vars", varsMap);

        Script initScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "vars.multiplier = 3", Collections.emptyMap());
        Script mapScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "state.list.add(vars.multiplier)",
            Collections.emptyMap()
        );
        Script combineScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum state values as a new aggregation",
            Collections.emptyMap()
        );
        Script reduceScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "no-op list aggregation", Collections.emptyMap());

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                scriptedMetric("scripted").params(params)
                    .initScript(initScript)
                    .mapScript(mapScript)
                    .combineScript(combineScript)
                    .reduceScript(reduceScript)
            )
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        // with script based aggregation, if it does not support reduce then aggregationList size
        // will be numShards * slicesCount
        assertThat(aggregationList.size(), greaterThanOrEqualTo(getNumShards("idx").numPrimaries));
        long totalCount = 0;
        for (Object object : aggregationList) {
            assertThat(object, notNullValue());
            assertThat(object, instanceOf(List.class));
            List<?> list = (List<?>) object;
            for (Object o : list) {
                assertThat(o, notNullValue());
                assertThat(o, instanceOf(Number.class));
                Number numberValue = (Number) o;
                // A particular shard may not have any documents stored on it so
                // we have to assume the lower bound may be 0. The check at the
                // bottom of the test method will make sure the count is correct
                assertThat(numberValue.longValue(), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(numDocs * 3)));
                totalCount += numberValue.longValue();
            }
        }
        assertThat(totalCount, equalTo(numDocs * 3));
    }

    public void testInitMapCombineReduceWithParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);

        Map<String, Object> params = new HashMap<>();
        params.put("vars", varsMap);

        Script initScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "vars.multiplier = 3", Collections.emptyMap());
        Script mapScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "state.list.add(vars.multiplier)",
            Collections.emptyMap()
        );
        Script combineScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum state values as a new aggregation",
            Collections.emptyMap()
        );
        Script reduceScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum all states (lists) values as a new aggregation",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                scriptedMetric("scripted").params(params)
                    .initScript(initScript)
                    .mapScript(mapScript)
                    .combineScript(combineScript)
                    .reduceScript(reduceScript)
            )
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(1));
        Object object = aggregationList.get(0);
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Number.class));
        assertThat(((Number) object).longValue(), equalTo(numDocs * 3));
    }

    @SuppressWarnings("rawtypes")
    public void testInitMapCombineReduceGetProperty() throws Exception {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);

        Map<String, Object> params = new HashMap<>();
        params.put("vars", varsMap);

        Script initScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "vars.multiplier = 3", Collections.emptyMap());
        Script mapScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "state.list.add(vars.multiplier)",
            Collections.emptyMap()
        );
        Script combineScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum state values as a new aggregation",
            Collections.emptyMap()
        );
        Script reduceScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum all states (lists) values as a new aggregation",
            Collections.emptyMap()
        );

        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                global("global").subAggregation(
                    scriptedMetric("scripted").params(params)
                        .initScript(initScript)
                        .mapScript(mapScript)
                        .combineScript(combineScript)
                        .reduceScript(reduceScript)
                )
            )
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(numDocs));

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(numDocs));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        ScriptedMetric scriptedMetricAggregation = global.getAggregations().get("scripted");
        assertThat(scriptedMetricAggregation, notNullValue());
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(1));
        Object object = aggregationList.get(0);
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Number.class));
        assertThat(((Number) object).longValue(), equalTo(numDocs * 3));
        assertThat(((InternalAggregation) global).getProperty("scripted"), sameInstance(scriptedMetricAggregation));
        assertThat((List) ((InternalAggregation) global).getProperty("scripted.value"), sameInstance((List) aggregationList));
        assertThat((List) ((InternalAggregation) scriptedMetricAggregation).getProperty("value"), sameInstance((List) aggregationList));
    }

    public void testMapCombineReduceWithParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);

        Map<String, Object> params = new HashMap<>();
        params.put("vars", varsMap);

        Script mapScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "state.list.add(vars.multiplier)",
            Collections.emptyMap()
        );
        Script combineScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum state values as a new aggregation",
            Collections.emptyMap()
        );
        Script reduceScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum all states (lists) values as a new aggregation",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                scriptedMetric("scripted").params(params).mapScript(mapScript).combineScript(combineScript).reduceScript(reduceScript)
            )
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(1));
        Object object = aggregationList.get(0);
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Number.class));
        assertThat(((Number) object).longValue(), equalTo(numDocs));
    }

    public void testInitMapReduceWithParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);

        Map<String, Object> params = new HashMap<>();
        params.put("vars", varsMap);

        Script initScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "vars.multiplier = 3", Collections.emptyMap());
        Script mapScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "state.list.add(vars.multiplier)",
            Collections.emptyMap()
        );
        Script combineScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "no-op aggregation", Collections.emptyMap());
        Script reduceScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum all states' state.list values as a new aggregation",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                scriptedMetric("scripted").params(params)
                    .initScript(initScript)
                    .mapScript(mapScript)
                    .combineScript(combineScript)
                    .reduceScript(reduceScript)
            )
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(1));
        Object object = aggregationList.get(0);
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Number.class));
        assertThat(((Number) object).longValue(), equalTo(numDocs * 3));
    }

    public void testMapReduceWithParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);
        Map<String, Object> params = new HashMap<>();
        params.put("vars", varsMap);

        Script mapScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "state.list.add(vars.multiplier)",
            Collections.emptyMap()
        );
        Script combineScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "no-op aggregation", Collections.emptyMap());
        Script reduceScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum all states' state.list values as a new aggregation",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                scriptedMetric("scripted").params(params).mapScript(mapScript).combineScript(combineScript).reduceScript(reduceScript)
            )
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(1));
        Object object = aggregationList.get(0);
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Number.class));
        assertThat(((Number) object).longValue(), equalTo(numDocs));
    }

    public void testInitMapCombineReduceWithParamsAndReduceParams() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);

        Map<String, Object> params = new HashMap<>();
        params.put("vars", varsMap);

        Map<String, Object> reduceParams = new HashMap<>();
        reduceParams.put("multiplier", 4);

        Script initScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "vars.multiplier = 3", Collections.emptyMap());
        Script mapScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "state.list.add(vars.multiplier)",
            Collections.emptyMap()
        );
        Script combineScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum state values as a new aggregation",
            Collections.emptyMap()
        );
        Script reduceScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "multiplied sum all states (lists) values as a new aggregation",
            reduceParams
        );

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                scriptedMetric("scripted").params(params)
                    .initScript(initScript)
                    .mapScript(mapScript)
                    .combineScript(combineScript)
                    .reduceScript(reduceScript)
            )
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(1));
        Object object = aggregationList.get(0);
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Number.class));
        assertThat(((Number) object).longValue(), equalTo(numDocs * 12));
    }

    public void testInitMapCombineReduceWithParamsStored() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);

        Map<String, Object> params = new HashMap<>();
        params.put("vars", varsMap);

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                scriptedMetric("scripted").params(params)
                    .initScript(new Script(ScriptType.STORED, null, "initScript_stored", Collections.emptyMap()))
                    .mapScript(new Script(ScriptType.STORED, null, "mapScript_stored", Collections.emptyMap()))
                    .combineScript(new Script(ScriptType.STORED, null, "combineScript_stored", Collections.emptyMap()))
                    .reduceScript(new Script(ScriptType.STORED, null, "reduceScript_stored", Collections.emptyMap()))
            )
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value(), equalTo(numDocs));

        Aggregation aggregation = response.getAggregations().get("scripted");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(ScriptedMetric.class));
        ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) aggregation;
        assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
        assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
        assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
        List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
        assertThat(aggregationList.size(), equalTo(1));
        Object object = aggregationList.get(0);
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Number.class));
        assertThat(((Number) object).longValue(), equalTo(numDocs * 3));
    }

    public void testInitMapCombineReduceWithParamsAsSubAgg() {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);

        Map<String, Object> params = new HashMap<>();
        params.put("vars", varsMap);

        Script initScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "vars.multiplier = 3", Collections.emptyMap());
        Script mapScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "state.list.add(vars.multiplier)",
            Collections.emptyMap()
        );
        Script combineScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum state values as a new aggregation",
            Collections.emptyMap()
        );
        Script reduceScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum all states (lists) values as a new aggregation",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .setSize(1000)
            .addAggregation(
                histogram("histo").field("l_value")
                    .interval(1)
                    .subAggregation(
                        scriptedMetric("scripted").params(params)
                            .initScript(initScript)
                            .mapScript(mapScript)
                            .combineScript(combineScript)
                            .reduceScript(reduceScript)
                    )
            )
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value(), equalTo(numDocs));
        Aggregation aggregation = response.getAggregations().get("histo");
        assertThat(aggregation, notNullValue());
        assertThat(aggregation, instanceOf(Histogram.class));
        Histogram histoAgg = (Histogram) aggregation;
        assertThat(histoAgg.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histoAgg.getBuckets();
        assertThat(buckets, notNullValue());
        for (Bucket b : buckets) {
            assertThat(b, notNullValue());
            assertThat(b.getDocCount(), equalTo(1L));
            Aggregations subAggs = b.getAggregations();
            assertThat(subAggs, notNullValue());
            assertThat(subAggs.asList().size(), equalTo(1));
            Aggregation subAgg = subAggs.get("scripted");
            assertThat(subAgg, notNullValue());
            assertThat(subAgg, instanceOf(ScriptedMetric.class));
            ScriptedMetric scriptedMetricAggregation = (ScriptedMetric) subAgg;
            assertThat(scriptedMetricAggregation.getName(), equalTo("scripted"));
            assertThat(scriptedMetricAggregation.aggregation(), notNullValue());
            assertThat(scriptedMetricAggregation.aggregation(), instanceOf(ArrayList.class));
            List<?> aggregationList = (List<?>) scriptedMetricAggregation.aggregation();
            assertThat(aggregationList.size(), equalTo(1));
            Object object = aggregationList.get(0);
            assertThat(object, notNullValue());
            assertThat(object, instanceOf(Number.class));
            assertThat(((Number) object).longValue(), equalTo(3L));
        }
    }

    public void testEmptyAggregation() throws Exception {
        Map<String, Object> varsMap = new HashMap<>();
        varsMap.put("multiplier", 1);

        Map<String, Object> params = new HashMap<>();
        params.put("vars", varsMap);

        Script initScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "vars.multiplier = 3", Collections.emptyMap());
        Script mapScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "state.list.add(vars.multiplier)",
            Collections.emptyMap()
        );
        Script combineScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum state values as a new aggregation",
            Collections.emptyMap()
        );
        Script reduceScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "sum all states (lists) values as a new aggregation",
            Collections.emptyMap()
        );

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                histogram("histo").field("value")
                    .interval(1L)
                    .minDocCount(0)
                    .subAggregation(
                        scriptedMetric("scripted").params(params)
                            .initScript(initScript)
                            .mapScript(mapScript)
                            .combineScript(combineScript)
                            .reduceScript(reduceScript)
                    )
            )
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, notNullValue());

        ScriptedMetric scriptedMetric = bucket.getAggregations().get("scripted");
        assertThat(scriptedMetric, notNullValue());
        assertThat(scriptedMetric.getName(), equalTo("scripted"));
        assertThat(scriptedMetric.aggregation(), notNullValue());
        assertThat(scriptedMetric.aggregation(), instanceOf(List.class));
        @SuppressWarnings("unchecked") // We'll just get a ClassCastException a couple lines down if we're wrong, its ok.
        List<Integer> aggregationResult = (List<Integer>) scriptedMetric.aggregation();
        assertThat(aggregationResult.size(), equalTo(1));
        assertThat(aggregationResult.get(0), equalTo(0));
    }

    /**
     * Make sure that a request using a deterministic script gets cached and nondeterministic scripts do not get cached.
     */
    public void testScriptCaching() throws Exception {
        Script mapScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "state['count'] = 1", Collections.emptyMap());
        Script combineScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "no-op aggregation", Collections.emptyMap());
        Script reduceScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "no-op list aggregation", Collections.emptyMap());

        Script ndInitScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "state.data = Math.random()", Collections.emptyMap());

        Script ndMapScript = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "state['count'] = Math.random() >= 0.5 ? 1 : 0",
            Collections.emptyMap()
        );

        Script ndRandom = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "return Math.random()", Collections.emptyMap());

        assertAcked(
            prepareCreate("cache_test_idx").setMapping("d", "type=long")
                .setSettings(Settings.builder().put("requests.cache.enable", true).put("number_of_shards", 1).put("number_of_replicas", 1))
                .get()
        );
        indexRandom(
            true,
            client().prepareIndex("cache_test_idx").setId("1").setSource("s", 1),
            client().prepareIndex("cache_test_idx").setId("2").setSource("s", 2)
        );

        // Make sure we are starting with a clear cache
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(0L)
        );

        // Test that a non-deterministic init script causes the result to not be cached
        SearchResponse r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(
                scriptedMetric("foo").initScript(ndInitScript).mapScript(mapScript).combineScript(combineScript).reduceScript(reduceScript)
            )
            .get();
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(0L)
        );

        // Test that a non-deterministic map script causes the result to not be cached
        r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(scriptedMetric("foo").mapScript(ndMapScript).combineScript(combineScript).reduceScript(reduceScript))
            .get();
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(0L)
        );

        // Test that a non-deterministic combine script causes the result to not be cached
        r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(scriptedMetric("foo").mapScript(mapScript).combineScript(ndRandom).reduceScript(reduceScript))
            .get();
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(0L)
        );

        // NOTE: random reduce scripts don't hit the query shard context (they are done on the coordinator) and so can be cached.
        r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(scriptedMetric("foo").mapScript(mapScript).combineScript(combineScript).reduceScript(ndRandom))
            .get();
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(1L)
        );

        // Test that all deterministic scripts cause the request to be cached
        r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(scriptedMetric("foo").mapScript(mapScript).combineScript(combineScript).reduceScript(reduceScript))
            .get();
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(2L)
        );
        internalCluster().wipeIndices("cache_test_idx");
    }

    public void testConflictingAggAndScriptParams() {
        Map<String, Object> params = Collections.singletonMap("param1", "12");
        Script mapScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "state.list.add(1)", params);
        Script combineScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "no-op aggregation", Collections.emptyMap());
        Script reduceScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "no-op list aggregation", Collections.emptyMap());

        SearchRequestBuilder builder = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                scriptedMetric("scripted").params(params).mapScript(mapScript).combineScript(combineScript).reduceScript(reduceScript)
            );

        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class, builder::get);
        assertThat(ex.getCause().getMessage(), containsString("Parameter name \"param1\" used in both aggregation and script parameters"));
    }
}
