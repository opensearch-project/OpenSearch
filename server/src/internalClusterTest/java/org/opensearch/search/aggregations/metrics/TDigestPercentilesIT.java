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

import org.apache.logging.log4j.LogManager;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationTestScriptsPlugin;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.global.Global;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.terms.Terms;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.search.aggregations.AggregationBuilders.filter;
import static org.opensearch.search.aggregations.AggregationBuilders.global;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.search.aggregations.AggregationBuilders.percentiles;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class TDigestPercentilesIT extends AbstractNumericTestCase {

    public TDigestPercentilesIT(Settings staticSettings) {
        super(staticSettings);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(AggregationTestScriptsPlugin.class);
    }

    private static double[] randomPercentiles() {
        final int length = randomIntBetween(1, 20);
        final Set<Double> uniquedPercentiles = new HashSet<>();
        for (int i = 0; i < length; ++i) {
            switch (randomInt(20)) {
                case 0:
                    uniquedPercentiles.add(0.0);
                    break;
                case 1:
                    uniquedPercentiles.add(100.0);
                    break;
                default:
                    uniquedPercentiles.add(randomDouble() * 100);
                    break;
            }
        }
        double[] percentiles = uniquedPercentiles.stream().mapToDouble(Double::doubleValue).sorted().toArray();
        LogManager.getLogger(TDigestPercentilesIT.class).info("Using percentiles={}", Arrays.toString(percentiles));
        return percentiles;
    }

    private static PercentilesAggregationBuilder randomCompression(PercentilesAggregationBuilder builder) {
        if (randomBoolean()) {
            builder.compression(randomIntBetween(20, 120) + randomDouble());
        }
        return builder;
    }

    private void assertConsistent(double[] pcts, Percentiles percentiles, long minValue, long maxValue) {
        final List<Percentile> percentileList = CollectionUtils.iterableAsArrayList(percentiles);
        assertEquals(pcts.length, percentileList.size());
        for (int i = 0; i < pcts.length; ++i) {
            final Percentile percentile = percentileList.get(i);
            assertThat(percentile.getPercent(), equalTo(pcts[i]));
            double value = percentile.getValue();
            assertThat(value, greaterThanOrEqualTo((double) minValue));
            assertThat(value, lessThanOrEqualTo((double) maxValue));

            if (percentile.getPercent() == 0) {
                assertThat(value, equalTo((double) minValue));
            }
            if (percentile.getPercent() == 100) {
                assertThat(value, equalTo((double) maxValue));
            }
        }

        for (int i = 1; i < percentileList.size(); ++i) {
            assertThat(percentileList.get(i).getValue(), greaterThanOrEqualTo(percentileList.get(i - 1).getValue()));
        }
    }

    @Override
    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                histogram("histo").field("value")
                    .interval(1L)
                    .minDocCount(0)
                    .subAggregation(randomCompression(percentiles("percentiles").field("value")).percentiles(10, 15))
            )
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, notNullValue());

        Percentiles percentiles = bucket.getAggregations().get("percentiles");
        assertThat(percentiles, notNullValue());
        assertThat(percentiles.getName(), equalTo("percentiles"));
        assertThat(percentiles.percentile(10), equalTo(Double.NaN));
        assertThat(percentiles.percentile(15), equalTo(Double.NaN));
    }

    @Override
    public void testUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx_unmapped")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentiles("percentiles")).field("value").percentiles(0, 10, 15, 100))
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(0L));

        Percentiles percentiles = searchResponse.getAggregations().get("percentiles");
        assertThat(percentiles, notNullValue());
        assertThat(percentiles.getName(), equalTo("percentiles"));
        assertThat(percentiles.percentile(0), equalTo(Double.NaN));
        assertThat(percentiles.percentile(10), equalTo(Double.NaN));
        assertThat(percentiles.percentile(15), equalTo(Double.NaN));
        assertThat(percentiles.percentile(100), equalTo(Double.NaN));
    }

    @Override
    public void testSingleValuedField() throws Exception {
        final double[] pcts = randomPercentiles();
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentiles("percentiles")).field("value").percentiles(pcts))
            .get();

        assertHitCount(searchResponse, 10);

        final Percentiles percentiles = searchResponse.getAggregations().get("percentiles");
        assertConsistent(pcts, percentiles, minValue, maxValue);
    }

    @Override
    public void testSingleValuedFieldGetProperty() throws Exception {
        final double[] pcts = randomPercentiles();
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(global("global").subAggregation(randomCompression(percentiles("percentiles")).field("value").percentiles(pcts)))
            .get();

        assertHitCount(searchResponse, 10);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(10L));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        Percentiles percentiles = global.getAggregations().get("percentiles");
        assertThat(percentiles, notNullValue());
        assertThat(percentiles.getName(), equalTo("percentiles"));
        assertThat(((InternalAggregation) global).getProperty("percentiles"), sameInstance(percentiles));
    }

    @Override
    public void testSingleValuedFieldPartiallyUnmapped() throws Exception {
        final double[] pcts = randomPercentiles();
        SearchResponse searchResponse = client().prepareSearch("idx", "idx_unmapped")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentiles("percentiles")).field("value").percentiles(pcts))
            .get();

        assertHitCount(searchResponse, 10);

        final Percentiles percentiles = searchResponse.getAggregations().get("percentiles");
        assertConsistent(pcts, percentiles, minValue, maxValue);
    }

    @Override
    public void testSingleValuedFieldWithValueScript() throws Exception {
        final double[] pcts = randomPercentiles();
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomCompression(percentiles("percentiles")).field("value")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - 1", emptyMap()))
                    .percentiles(pcts)
            )
            .get();

        assertHitCount(searchResponse, 10);

        final Percentiles percentiles = searchResponse.getAggregations().get("percentiles");
        assertConsistent(pcts, percentiles, minValue - 1, maxValue - 1);
    }

    @Override
    public void testSingleValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);
        final double[] pcts = randomPercentiles();
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomCompression(percentiles("percentiles")).field("value")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - dec", params))
                    .percentiles(pcts)
            )
            .get();

        assertHitCount(searchResponse, 10);

        final Percentiles percentiles = searchResponse.getAggregations().get("percentiles");
        assertConsistent(pcts, percentiles, minValue - 1, maxValue - 1);
    }

    @Override
    public void testMultiValuedField() throws Exception {
        final double[] pcts = randomPercentiles();
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentiles("percentiles")).field("values").percentiles(pcts))
            .get();

        assertHitCount(searchResponse, 10);

        final Percentiles percentiles = searchResponse.getAggregations().get("percentiles");
        assertConsistent(pcts, percentiles, minValues, maxValues);
    }

    @Override
    public void testMultiValuedFieldWithValueScript() throws Exception {
        final double[] pcts = randomPercentiles();
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomCompression(percentiles("percentiles")).field("values")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - 1", emptyMap()))
                    .percentiles(pcts)
            )
            .get();

        assertHitCount(searchResponse, 10);

        final Percentiles percentiles = searchResponse.getAggregations().get("percentiles");
        assertConsistent(pcts, percentiles, minValues - 1, maxValues - 1);
    }

    public void testMultiValuedFieldWithValueScriptReverse() throws Exception {
        final double[] pcts = randomPercentiles();
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomCompression(percentiles("percentiles")).field("values")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value * -1", emptyMap()))
                    .percentiles(pcts)
            )
            .get();

        assertHitCount(searchResponse, 10);

        final Percentiles percentiles = searchResponse.getAggregations().get("percentiles");
        assertConsistent(pcts, percentiles, -maxValues, -minValues);
    }

    @Override
    public void testMultiValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);
        final double[] pcts = randomPercentiles();
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomCompression(percentiles("percentiles")).field("values")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - dec", params))
                    .percentiles(pcts)
            )
            .get();

        assertHitCount(searchResponse, 10);

        final Percentiles percentiles = searchResponse.getAggregations().get("percentiles");
        assertConsistent(pcts, percentiles, minValues - 1, maxValues - 1);
    }

    @Override
    public void testScriptSingleValued() throws Exception {
        Script script = new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['value'].value", emptyMap());
        final double[] pcts = randomPercentiles();
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentiles("percentiles")).script(script).percentiles(pcts))
            .get();

        assertHitCount(searchResponse, 10);

        final Percentiles percentiles = searchResponse.getAggregations().get("percentiles");
        assertConsistent(pcts, percentiles, minValue, maxValue);
    }

    @Override
    public void testScriptSingleValuedWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);

        Script script = new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['value'].value - dec", params);

        final double[] pcts = randomPercentiles();
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentiles("percentiles")).script(script).percentiles(pcts))
            .get();

        assertHitCount(searchResponse, 10);

        final Percentiles percentiles = searchResponse.getAggregations().get("percentiles");
        assertConsistent(pcts, percentiles, minValue - 1, maxValue - 1);
    }

    @Override
    public void testScriptMultiValued() throws Exception {
        final double[] pcts = randomPercentiles();
        Script script = new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['values']", emptyMap());

        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentiles("percentiles")).script(script).percentiles(pcts))
            .get();

        assertHitCount(searchResponse, 10);

        final Percentiles percentiles = searchResponse.getAggregations().get("percentiles");
        assertConsistent(pcts, percentiles, minValues, maxValues);
    }

    @Override
    public void testScriptMultiValuedWithParams() throws Exception {
        Script script = AggregationTestScriptsPlugin.DECREMENT_ALL_VALUES;

        final double[] pcts = randomPercentiles();
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentiles("percentiles")).script(script).percentiles(pcts))
            .get();

        assertHitCount(searchResponse, 10);

        final Percentiles percentiles = searchResponse.getAggregations().get("percentiles");
        assertConsistent(pcts, percentiles, minValues - 1, maxValues - 1);
    }

    public void testOrderBySubAggregation() {
        boolean asc = randomBoolean();
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                histogram("histo").field("value")
                    .interval(2L)
                    .subAggregation(randomCompression(percentiles("percentiles").field("value").percentiles(99)))
                    .order(BucketOrder.aggregation("percentiles", "99", asc))
            )
            .get();

        assertHitCount(searchResponse, 10);

        Histogram histo = searchResponse.getAggregations().get("histo");
        double previous = asc ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            Percentiles percentiles = bucket.getAggregations().get("percentiles");
            double p99 = percentiles.percentile(99);
            if (asc) {
                assertThat(p99, greaterThanOrEqualTo(previous));
            } else {
                assertThat(p99, lessThanOrEqualTo(previous));
            }
            previous = p99;
        }
    }

    @Override
    public void testOrderByEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("terms").field("value")
                    .order(BucketOrder.compound(BucketOrder.aggregation("filter>percentiles.99", true)))
                    .subAggregation(
                        filter("filter", termQuery("value", 100)).subAggregation(
                            percentiles("percentiles").method(PercentilesMethod.TDIGEST).field("value")
                        )
                    )
            )
            .get();

        assertHitCount(searchResponse, 10);

        Terms terms = searchResponse.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets, notNullValue());
        assertThat(buckets.size(), equalTo(10));

        for (int i = 0; i < 10; i++) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsNumber(), equalTo((long) i + 1));
            assertThat(bucket.getDocCount(), equalTo(1L));
            Filter filter = bucket.getAggregations().get("filter");
            assertThat(filter, notNullValue());
            assertThat(filter.getDocCount(), equalTo(0L));
            Percentiles percentiles = filter.getAggregations().get("percentiles");
            assertThat(percentiles, notNullValue());
            assertThat(percentiles.percentile(99), equalTo(Double.NaN));

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
                percentiles("foo").field("d")
                    .percentiles(50.0)
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "Math.random()", emptyMap()))
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
                percentiles("foo").field("d")
                    .percentiles(50.0)
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - 1", emptyMap()))
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
        r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(percentiles("foo").field("d").percentiles(50.0)).get();
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
