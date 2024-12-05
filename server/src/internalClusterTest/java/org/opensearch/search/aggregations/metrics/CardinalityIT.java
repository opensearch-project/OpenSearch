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

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.global.Global;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.SearchService.CARDINALITY_AGGREGATION_PRUNING_THRESHOLD;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.cardinality;
import static org.opensearch.search.aggregations.AggregationBuilders.global;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class CardinalityIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public CardinalityIT(Settings staticSettings) {
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
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("_value", vars -> vars.get("_value"));

            scripts.put("doc['str_value'].value", vars -> {
                Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
                return doc.get("str_value");
            });

            scripts.put("doc['str_values']", vars -> {
                Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
                ScriptDocValues.Strings strValue = (ScriptDocValues.Strings) doc.get("str_values");
                return strValue;
            });

            scripts.put("doc[' + singleNumericField() + '].value", vars -> {
                Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
                return doc.get(singleNumericField());
            });

            scripts.put("doc[' + multiNumericField(false) + ']", vars -> {
                Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
                return (ScriptDocValues<?>) doc.get(multiNumericField(false));
            });

            return scripts;
        }

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> nonDeterministicPluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("Math.random()", vars -> CardinalityIT.randomDouble());

            return scripts;
        }
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put("index.number_of_shards", numberOfShards())
            .put("index.number_of_replicas", numberOfReplicas())
            .build();
    }

    static long numDocs;
    static long precisionThreshold;

    @Override
    public void setupSuiteScopeCluster() throws Exception {

        prepareCreate("idx").setMapping(
            jsonBuilder().startObject()
                .startObject("properties")
                .startObject("str_value")
                .field("type", "keyword")
                .endObject()
                .startObject("str_values")
                .field("type", "keyword")
                .endObject()
                .startObject("l_value")
                .field("type", "long")
                .endObject()
                .startObject("l_values")
                .field("type", "long")
                .endObject()
                .startObject("d_value")
                .field("type", "double")
                .endObject()
                .startObject("d_values")
                .field("type", "double")
                .endObject()
                .endObject()
                .endObject()
        ).get();

        numDocs = randomIntBetween(2, 100);
        precisionThreshold = randomIntBetween(0, 1 << randomInt(20));
        IndexRequestBuilder[] builders = new IndexRequestBuilder[(int) numDocs];
        for (int i = 0; i < numDocs; ++i) {
            builders[i] = client().prepareIndex("idx")
                .setSource(
                    jsonBuilder().startObject()
                        .field("str_value", "s" + i)
                        .array("str_values", new String[] { "s" + (i * 2), "s" + (i * 2 + 1) })
                        .field("l_value", i)
                        .array("l_values", new int[] { i * 2, i * 2 + 1 })
                        .field("d_value", i)
                        .array("d_values", new double[] { i * 2, i * 2 + 1 })
                        .endObject()
                );
        }
        indexRandom(true, builders);
        createIndex("idx_unmapped");

        IndexRequestBuilder[] dummyDocsBuilder = new IndexRequestBuilder[10];
        for (int i = 0; i < dummyDocsBuilder.length; i++) {
            dummyDocsBuilder[i] = client().prepareIndex("idx").setSource("a_field", "1");
        }
        indexRandom(true, dummyDocsBuilder);

        ensureSearchable();
    }

    private void assertCount(Cardinality count, long value) {
        if (value <= precisionThreshold) {
            // linear counting should be picked, and should be accurate
            assertEquals(value, count.getValue());
        } else {
            // error is not bound, so let's just make sure it is > 0
            assertThat(count.getValue(), greaterThan(0L));
        }
    }

    private static String singleNumericField() {
        return randomBoolean() ? "l_value" : "d_value";
    }

    private static String multiNumericField(boolean hash) {
        return randomBoolean() ? "l_values" : "d_values";
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
            .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_value"))
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, 0);
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
            .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_value"))
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testSingleValuedString() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_value"))
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testDisableDynamicPruning() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_value"))
            .get();
        assertSearchResponse(response);

        Cardinality count1 = response.getAggregations().get("cardinality");

        final ClusterUpdateSettingsResponse updateSettingResponse = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CARDINALITY_AGGREGATION_PRUNING_THRESHOLD.getKey(), 0))
            .get();
        assertEquals(updateSettingResponse.getTransientSettings().get(CARDINALITY_AGGREGATION_PRUNING_THRESHOLD.getKey()), "0");

        response = client().prepareSearch("idx")
            .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_value"))
            .get();
        assertSearchResponse(response);
        Cardinality count2 = response.getAggregations().get("cardinality");

        assertEquals(count1, count2);

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(CARDINALITY_AGGREGATION_PRUNING_THRESHOLD.getKey()))
            .get();
    }

    public void testSingleValuedNumeric() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field(singleNumericField()))
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testSingleValuedNumericGetProperty() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                global("global").subAggregation(
                    cardinality("cardinality").precisionThreshold(precisionThreshold).field(singleNumericField())
                )
            )
            .get();

        assertSearchResponse(searchResponse);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        // assertThat(global.getDocCount(), equalTo(numDocs));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        Cardinality cardinality = global.getAggregations().get("cardinality");
        assertThat(cardinality, notNullValue());
        assertThat(cardinality.getName(), equalTo("cardinality"));
        long expectedValue = numDocs;
        assertCount(cardinality, expectedValue);
        assertThat(((InternalAggregation) global).getProperty("cardinality"), equalTo(cardinality));
        assertThat(((InternalAggregation) global).getProperty("cardinality.value"), equalTo((double) cardinality.getValue()));
        assertThat((double) ((InternalAggregation) cardinality).getProperty("value"), equalTo((double) cardinality.getValue()));
    }

    public void testSingleValuedNumericHashed() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field(singleNumericField()))
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedString() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_values"))
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testMultiValuedNumeric() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field(multiNumericField(false)))
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testMultiValuedNumericHashed() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field(multiNumericField(true)))
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testSingleValuedStringScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                cardinality("cardinality").precisionThreshold(precisionThreshold)
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['str_value'].value", emptyMap()))
            )
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedStringScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                cardinality("cardinality").precisionThreshold(precisionThreshold)
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['str_values']", emptyMap()))
            )
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testSingleValuedNumericScript() throws Exception {
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc[' + singleNumericField() + '].value", emptyMap());
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).script(script))
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedNumericScript() throws Exception {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "doc[' + multiNumericField(false) + ']",
            Collections.emptyMap()
        );
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).script(script))
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testSingleValuedStringValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                cardinality("cardinality").precisionThreshold(precisionThreshold)
                    .field("str_value")
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value", emptyMap()))
            )
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedStringValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                cardinality("cardinality").precisionThreshold(precisionThreshold)
                    .field("str_values")
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value", emptyMap()))
            )
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testSingleValuedNumericValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                cardinality("cardinality").precisionThreshold(precisionThreshold)
                    .field(singleNumericField())
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value", emptyMap()))
            )
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedNumericValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                cardinality("cardinality").precisionThreshold(precisionThreshold)
                    .field(multiNumericField(false))
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value", emptyMap()))
            )
            .get();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testAsSubAgg() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("str_value")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .subAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_values"))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        for (Terms.Bucket bucket : terms.getBuckets()) {
            Cardinality count = bucket.getAggregations().get("cardinality");
            assertThat(count, notNullValue());
            assertThat(count.getName(), equalTo("cardinality"));
            assertCount(count, 2);
        }
    }

    /**
     * Make sure that a request using a deterministic script or not using a script get cached.
     * Ensure requests using nondeterministic scripts do not get cached.
     */
    public void testScriptCaching() throws Exception {
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

        // Test that a request using a nondeterministic script does not get cached
        SearchResponse r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(
                cardinality("foo").field("d").script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "Math.random()", emptyMap()))
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

        // Test that a request using a deterministic script gets cached
        r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(
                cardinality("foo").field("d").script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value", emptyMap()))
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
            equalTo(1L)
        );

        // Ensure that non-scripted requests are cached as normal
        r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(cardinality("foo").field("d")).get();
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
}
