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
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.search.aggregations.AggregationBuilders.filter;
import static org.opensearch.search.aggregations.AggregationBuilders.global;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.search.aggregations.AggregationBuilders.percentileRanks;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class TDigestPercentileRanksIT extends AbstractNumericTestCase {

    public TDigestPercentileRanksIT(Settings staticSettings) {
        super(staticSettings);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(AggregationTestScriptsPlugin.class);
    }

    private static double[] randomPercents(long minValue, long maxValue) {
        final int length = randomIntBetween(1, 20);
        final double[] percents = new double[length];
        for (int i = 0; i < percents.length; ++i) {
            switch (randomInt(20)) {
                case 0:
                    percents[i] = minValue;
                    break;
                case 1:
                    percents[i] = maxValue;
                    break;
                default:
                    percents[i] = (randomDouble() * (maxValue - minValue)) + minValue;
                    break;
            }
        }
        Arrays.sort(percents);
        LogManager.getLogger(TDigestPercentileRanksIT.class).info("Using values={}", Arrays.toString(percents));
        return percents;
    }

    private static PercentileRanksAggregationBuilder randomCompression(PercentileRanksAggregationBuilder builder) {
        if (randomBoolean()) {
            builder.compression(randomIntBetween(20, 120) + randomDouble());
        }
        return builder;
    }

    private void assertConsistent(double[] pcts, PercentileRanks values, long minValue) {
        final List<Percentile> percentileList = CollectionUtils.iterableAsArrayList(values);
        assertEquals(pcts.length, percentileList.size());
        for (int i = 0; i < pcts.length; ++i) {
            final Percentile percentile = percentileList.get(i);
            assertThat(percentile.getValue(), equalTo(pcts[i]));
            assertThat(percentile.getPercent(), greaterThanOrEqualTo(0.0));
            assertThat(percentile.getPercent(), lessThanOrEqualTo(100.0));

            if (percentile.getPercent() == 0) {
                assertThat(percentile.getValue(), lessThanOrEqualTo((double) minValue));
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
                    .subAggregation(randomCompression(percentileRanks("percentile_ranks", new double[] { 10, 15 }).field("value")))
            )
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, notNullValue());

        PercentileRanks reversePercentiles = bucket.getAggregations().get("percentile_ranks");
        assertThat(reversePercentiles, notNullValue());
        assertThat(reversePercentiles.getName(), equalTo("percentile_ranks"));
        assertThat(reversePercentiles.percent(10), equalTo(Double.NaN));
        assertThat(reversePercentiles.percent(15), equalTo(Double.NaN));
    }

    public void testNullValuesField() throws Exception {
        final double[] pcts = null;
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(percentileRanks("percentile_ranks", pcts).method(PercentilesMethod.TDIGEST).field("value"))
                .get()
        );
        assertThat(e.getMessage(), equalTo("[values] must not be null: [percentile_ranks]"));
    }

    public void testEmptyValuesField() throws Exception {
        final double[] pcts = new double[0];
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(percentileRanks("percentile_ranks", pcts).method(PercentilesMethod.TDIGEST).field("value"))
                .get()
        );
        assertThat(e.getMessage(), equalTo("[values] must not be an empty array: [percentile_ranks]"));
    }

    @Override
    public void testUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx_unmapped")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentileRanks("percentile_ranks", new double[] { 0, 10, 15, 100 })).field("value"))
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(0L));

        PercentileRanks reversePercentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertThat(reversePercentiles, notNullValue());
        assertThat(reversePercentiles.getName(), equalTo("percentile_ranks"));
        assertThat(reversePercentiles.percent(0), equalTo(Double.NaN));
        assertThat(reversePercentiles.percent(10), equalTo(Double.NaN));
        assertThat(reversePercentiles.percent(15), equalTo(Double.NaN));
        assertThat(reversePercentiles.percent(100), equalTo(Double.NaN));
    }

    @Override
    public void testSingleValuedField() throws Exception {
        final double[] pcts = randomPercents(minValue, maxValue);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).field("value"))
            .get();

        assertHitCount(searchResponse, 10);

        final PercentileRanks values = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, values, minValue);
    }

    @Override
    public void testSingleValuedFieldGetProperty() throws Exception {
        final double[] pcts = randomPercents(minValue, maxValue);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(global("global").subAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).field("value")))
            .get();

        assertHitCount(searchResponse, 10);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(10L));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        PercentileRanks values = global.getAggregations().get("percentile_ranks");
        assertThat(values, notNullValue());
        assertThat(values.getName(), equalTo("percentile_ranks"));
        assertThat(((InternalAggregation) global).getProperty("percentile_ranks"), sameInstance(values));
    }

    public void testSingleValuedFieldOutsideRange() throws Exception {
        final double[] pcts = new double[] { minValue - 1, maxValue + 1 };
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).field("value"))
            .get();

        assertHitCount(searchResponse, 10);

        final PercentileRanks values = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, values, minValue);
    }

    @Override
    public void testSingleValuedFieldPartiallyUnmapped() throws Exception {
        final double[] pcts = randomPercents(minValue, maxValue);
        SearchResponse searchResponse = client().prepareSearch("idx", "idx_unmapped")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).field("value"))
            .get();

        assertHitCount(searchResponse, 10);

        final PercentileRanks values = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, values, minValue);
    }

    @Override
    public void testSingleValuedFieldWithValueScript() throws Exception {
        final double[] pcts = randomPercents(minValue - 1, maxValue - 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomCompression(percentileRanks("percentile_ranks", pcts)).field("value")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - 1", emptyMap()))
            )
            .get();

        assertHitCount(searchResponse, 10);

        final PercentileRanks values = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, values, minValue - 1);
    }

    @Override
    public void testSingleValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);
        final double[] pcts = randomPercents(minValue - 1, maxValue - 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomCompression(percentileRanks("percentile_ranks", pcts)).field("value")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - dec", params))
            )
            .get();

        assertHitCount(searchResponse, 10);

        final PercentileRanks values = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, values, minValue - 1);
    }

    @Override
    public void testMultiValuedField() throws Exception {
        final double[] pcts = randomPercents(minValues, maxValues);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).field("values"))
            .get();

        assertHitCount(searchResponse, 10);

        final PercentileRanks values = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, values, minValues);
    }

    @Override
    public void testMultiValuedFieldWithValueScript() throws Exception {
        final double[] pcts = randomPercents(minValues - 1, maxValues - 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomCompression(percentileRanks("percentile_ranks", pcts)).field("values")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - 1", emptyMap()))
            )
            .get();

        assertHitCount(searchResponse, 10);

        final PercentileRanks values = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, values, minValues - 1);
    }

    public void testMultiValuedFieldWithValueScriptReverse() throws Exception {
        final double[] pcts = randomPercents(-maxValues, -minValues);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomCompression(percentileRanks("percentile_ranks", pcts)).field("values")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value * -1", emptyMap()))
            )
            .get();

        assertHitCount(searchResponse, 10);

        final PercentileRanks values = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, values, -maxValues);
    }

    @Override
    public void testMultiValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);
        final double[] pcts = randomPercents(minValues - 1, maxValues - 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomCompression(percentileRanks("percentile_ranks", pcts)).field("values")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - dec", params))
            )
            .get();

        assertHitCount(searchResponse, 10);

        final PercentileRanks values = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, values, minValues - 1);
    }

    @Override
    public void testScriptSingleValued() throws Exception {
        final double[] pcts = randomPercents(minValue, maxValue);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomCompression(percentileRanks("percentile_ranks", pcts)).script(
                    new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['value'].value", emptyMap())
                )
            )
            .get();

        assertHitCount(searchResponse, 10);

        final PercentileRanks values = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, values, minValue);
    }

    @Override
    public void testScriptSingleValuedWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);

        Script script = new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['value'].value - dec", params);

        final double[] pcts = randomPercents(minValue - 1, maxValue - 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).script(script))
            .get();

        assertHitCount(searchResponse, 10);

        final PercentileRanks values = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, values, minValue - 1);
    }

    @Override
    public void testScriptMultiValued() throws Exception {
        final double[] pcts = randomPercents(minValues, maxValues);
        Script script = new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['values']", emptyMap());
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).script(script))
            .get();

        assertHitCount(searchResponse, 10);

        final PercentileRanks values = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, values, minValues);
    }

    @Override
    public void testScriptMultiValuedWithParams() throws Exception {
        Script script = AggregationTestScriptsPlugin.DECREMENT_ALL_VALUES;

        final double[] pcts = randomPercents(minValues - 1, maxValues - 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).script(script))
            .get();

        assertHitCount(searchResponse, 10);

        final PercentileRanks values = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, values, minValues - 1);
    }

    public void testOrderBySubAggregation() {
        boolean asc = randomBoolean();
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                histogram("histo").field("value")
                    .interval(2L)
                    .subAggregation(randomCompression(percentileRanks("percentile_ranks", new double[] { 99 }).field("value")))
                    .order(BucketOrder.aggregation("percentile_ranks", "99", asc))
            )
            .get();

        assertHitCount(searchResponse, 10);

        Histogram histo = searchResponse.getAggregations().get("histo");
        double previous = asc ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            PercentileRanks values = bucket.getAggregations().get("percentile_ranks");
            double p99 = values.percent(99);
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
                    .order(BucketOrder.compound(BucketOrder.aggregation("filter>ranks.99", true)))
                    .subAggregation(
                        filter("filter", termQuery("value", 100)).subAggregation(
                            percentileRanks("ranks", new double[] { 99 }).method(PercentilesMethod.TDIGEST).field("value")
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
            PercentileRanks ranks = filter.getAggregations().get("ranks");
            assertThat(ranks, notNullValue());
            assertThat(ranks.percent(99), equalTo(Double.NaN));

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
                percentileRanks("foo", new double[] { 50.0 }).field("d")
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
                percentileRanks("foo", new double[] { 50.0 }).field("d")
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
        r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(percentileRanks("foo", new double[] { 50.0 }).field("d"))
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

}
