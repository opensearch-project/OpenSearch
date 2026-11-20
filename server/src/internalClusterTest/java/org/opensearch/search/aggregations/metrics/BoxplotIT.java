/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.LongStream;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE;
import static org.opensearch.search.aggregations.AggregationBuilders.boxplot;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.search.aggregations.PipelineAggregatorBuilders.bucketSelector;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Runs the boxplot aggregation on an index with several shards, with and without concurrent segment search, so that the digests of
 * every shard, and of every slice of a shard, have to be merged into one result.
 */
@OpenSearchIntegTestCase.SuiteScopeTestCase
public class BoxplotIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final String INDEX = "idx";
    private static final String UPPER_ABOVE_FIFTY = "params.u > 50";
    // group a holds 1..10, group b holds 100..110 and group c holds twenty 5s and a 500
    private static final int NUM_DOCS = 10 + 11 + 21;

    public BoxplotIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {
                Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), CONCURRENT_SEGMENT_SEARCH_MODE_ALL).build() },
            new Object[] {
                Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), CONCURRENT_SEGMENT_SEARCH_MODE_NONE).build() }
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(UpperScriptPlugin.class);
    }

    /**
     * Painless is not available to the tests of the server, so the script of the bucket selector is a mock script.
     */
    public static class UpperScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(UPPER_ABOVE_FIFTY, vars -> ((Number) vars.get("u")).doubleValue() > 50);
        }
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, randomIntBetween(2, 5)).put(SETTING_NUMBER_OF_REPLICAS, 0)
            ).setMapping("group", "type=keyword", "value", "type=long")
        );

        // Group c is mostly 5s so that its quartiles are all 5 and the single 500 is an outlier. With only a few points the t-digest
        // would interpolate the third quartile towards 500 and the 500 would no longer be an outlier.
        // Every group is indexed and refreshed on its own, so that the shards have several segments for concurrent search to slice.
        indexRandom(true, docs("a", LongStream.rangeClosed(1, 10).toArray()));
        indexRandom(true, docs("b", LongStream.rangeClosed(100, 110).toArray()));
        indexRandom(true, docs("c", LongStream.concat(LongStream.generate(() -> 5).limit(20), LongStream.of(500)).toArray()));
        ensureSearchable(INDEX);
    }

    private List<IndexRequestBuilder> docs(String group, long[] values) {
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (long value : values) {
            builders.add(client().prepareIndex(INDEX).setSource("group", group, "value", value));
        }
        return builders;
    }

    public void testAcrossShards() {
        SearchResponse response = client().prepareSearch(INDEX).addAggregation(boxplot("box").field("value")).get();

        assertSearchResponse(response);
        assertThat(response.getTotalShards(), greaterThan(1));
        Boxplot box = response.getAggregations().get("box");
        assertThat(box, notNullValue());
        assertThat(box.getName(), equalTo("box"));
        assertThat(box.getMin(), equalTo(1.0));
        assertThat(box.getMax(), equalTo(500.0));
    }

    public void testOrderByQ3() {
        SearchResponse response = client().prepareSearch(INDEX)
            .addAggregation(
                terms("groups").field("group").order(BucketOrder.aggregation("box.q3", false)).subAggregation(boxplot("box").field("value"))
            )
            .get();

        assertSearchResponse(response);
        Terms groups = response.getAggregations().get("groups");
        assertThat(groups, notNullValue());
        List<? extends Terms.Bucket> buckets = groups.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        // the third quartile of b, around 107, is far above those of a, around 8, and of c, which is 5
        assertThat(buckets.get(0).getKeyAsString(), equalTo("b"));
        double previousQ3 = Double.POSITIVE_INFINITY;
        for (Terms.Bucket bucket : buckets) {
            Boxplot box = bucket.getAggregations().get("box");
            assertThat(box, notNullValue());
            assertThat(box.getQ3(), lessThanOrEqualTo(previousQ3));
            previousQ3 = box.getQ3();
        }
    }

    public void testBucketSelectorOnUpper() {
        Script script = new Script(ScriptType.INLINE, UpperScriptPlugin.NAME, UPPER_ABOVE_FIFTY, Collections.emptyMap());

        SearchResponse response = client().prepareSearch(INDEX)
            .addAggregation(
                terms("groups").field("group")
                    .subAggregation(boxplot("box").field("value"))
                    .subAggregation(bucketSelector("wide", Collections.singletonMap("u", "box.upper"), script))
            )
            .get();

        assertSearchResponse(response);
        Terms groups = response.getAggregations().get("groups");
        assertThat(groups, notNullValue());
        // the upper whisker of c is 5, because the 500 is an outlier, and that of a is 10
        assertThat(groups.getBuckets().size(), equalTo(1));
        Terms.Bucket bucket = groups.getBuckets().get(0);
        assertThat(bucket.getKeyAsString(), equalTo("b"));
        Boxplot box = bucket.getAggregations().get("box");
        assertThat(box.getUpper(), greaterThan(50.0));
    }

    public void testEmptyHistogramBucket() {
        SearchResponse response = client().prepareSearch(INDEX)
            .addAggregation(
                histogram("histo").field("value")
                    .interval(1000)
                    .extendedBounds(0, 2000)
                    .minDocCount(0)
                    .subAggregation(boxplot("box").field("value"))
            )
            .get();

        assertSearchResponse(response);
        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        assertThat(buckets.get(0).getDocCount(), equalTo((long) NUM_DOCS));
        Boxplot filled = buckets.get(0).getAggregations().get("box");
        assertThat(filled, notNullValue());
        assertFalse(Double.isNaN(filled.getQ2()));

        assertThat(buckets.get(1).getDocCount(), equalTo(0L));
        Boxplot empty = buckets.get(1).getAggregations().get("box");
        assertThat(empty, notNullValue());
        assertTrue(Double.isNaN(empty.getQ2()));
    }
}
