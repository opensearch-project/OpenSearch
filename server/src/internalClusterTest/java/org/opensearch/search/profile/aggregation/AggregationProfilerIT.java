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

package org.opensearch.search.profile.aggregation;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.global.Global;
import org.opensearch.search.aggregations.bucket.sampler.DiversifiedOrdinalsSamplerAggregator;
import org.opensearch.search.aggregations.bucket.terms.GlobalOrdinalsStringTermsAggregator;
import org.opensearch.search.aggregations.metrics.Stats;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.fetch.FetchProfileShardResult;
import org.opensearch.search.profile.query.CollectorResult;
import org.opensearch.search.profile.query.QueryProfileShardResult;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.hamcrest.core.IsNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.avg;
import static org.opensearch.search.aggregations.AggregationBuilders.diversifiedSampler;
import static org.opensearch.search.aggregations.AggregationBuilders.global;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.stats;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.search.aggregations.AggregationBuilders.topHits;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class AggregationProfilerIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final String BUILD_LEAF_COLLECTOR = AggregationTimingType.BUILD_LEAF_COLLECTOR.toString();
    private static final String COLLECT = AggregationTimingType.COLLECT.toString();
    private static final String POST_COLLECTION = AggregationTimingType.POST_COLLECTION.toString();
    private static final String INITIALIZE = AggregationTimingType.INITIALIZE.toString();
    private static final String BUILD_AGGREGATION = AggregationTimingType.BUILD_AGGREGATION.toString();
    private static final String REDUCE = AggregationTimingType.REDUCE.toString();
    private static final Set<String> BREAKDOWN_KEYS = Set.of(
        INITIALIZE,
        BUILD_LEAF_COLLECTOR,
        COLLECT,
        POST_COLLECTION,
        BUILD_AGGREGATION,
        REDUCE,
        INITIALIZE + "_count",
        BUILD_LEAF_COLLECTOR + "_count",
        COLLECT + "_count",
        POST_COLLECTION + "_count",
        BUILD_AGGREGATION + "_count",
        REDUCE + "_count",
        INITIALIZE + "_start_time",
        BUILD_LEAF_COLLECTOR + "_start_time",
        COLLECT + "_start_time",
        POST_COLLECTION + "_start_time",
        BUILD_AGGREGATION + "_start_time",
        REDUCE + "_start_time"
    );

    private static final Set<String> CONCURRENT_SEARCH_BREAKDOWN_KEYS = Set.of(
        INITIALIZE,
        BUILD_LEAF_COLLECTOR,
        COLLECT,
        POST_COLLECTION,
        BUILD_AGGREGATION,
        REDUCE,
        INITIALIZE + "_count",
        BUILD_LEAF_COLLECTOR + "_count",
        COLLECT + "_count",
        POST_COLLECTION + "_count",
        BUILD_AGGREGATION + "_count",
        REDUCE + "_count",
        "max_" + INITIALIZE,
        "max_" + BUILD_LEAF_COLLECTOR,
        "max_" + COLLECT,
        "max_" + POST_COLLECTION,
        "max_" + BUILD_AGGREGATION,
        "max_" + REDUCE,
        "min_" + INITIALIZE,
        "min_" + BUILD_LEAF_COLLECTOR,
        "min_" + COLLECT,
        "min_" + POST_COLLECTION,
        "min_" + BUILD_AGGREGATION,
        "min_" + REDUCE,
        "avg_" + INITIALIZE,
        "avg_" + BUILD_LEAF_COLLECTOR,
        "avg_" + COLLECT,
        "avg_" + POST_COLLECTION,
        "avg_" + BUILD_AGGREGATION,
        "avg_" + REDUCE,
        "max_" + BUILD_LEAF_COLLECTOR + "_count",
        "max_" + COLLECT + "_count",
        "min_" + BUILD_LEAF_COLLECTOR + "_count",
        "min_" + COLLECT + "_count",
        "avg_" + BUILD_LEAF_COLLECTOR + "_count",
        "avg_" + COLLECT + "_count"
    );

    private static final String TOTAL_BUCKETS = "total_buckets";
    private static final String DEFERRED = "deferred_aggregators";
    private static final String COLLECTION_STRAT = "collection_strategy";
    private static final String RESULT_STRAT = "result_strategy";
    private static final String HAS_FILTER = "has_filter";
    private static final String SEGMENTS_WITH_SINGLE = "segments_with_single_valued_ords";
    private static final String SEGMENTS_WITH_MULTI = "segments_with_multi_valued_ords";

    private static final String NUMBER_FIELD = "number";
    private static final String TAG_FIELD = "tag";
    private static final String STRING_FIELD = "string_field";
    private final int numDocs = 5;
    private static final String REASON_SEARCH_TOP_HITS = "search_top_hits";
    private static final String REASON_AGGREGATION = "aggregation";

    public AggregationProfilerIT(Settings staticSettings) {
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
    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("idx")
                .setSettings(Map.of("number_of_shards", 1, "number_of_replicas", 0))
                .setMapping(STRING_FIELD, "type=keyword", NUMBER_FIELD, "type=integer", TAG_FIELD, "type=keyword")
                .get()
        );
        List<IndexRequestBuilder> builders = new ArrayList<>();

        String[] randomStrings = new String[randomIntBetween(2, 10)];
        for (int i = 0; i < randomStrings.length; i++) {
            randomStrings[i] = randomAlphaOfLength(10);
        }

        for (int i = 0; i < numDocs; i++) {
            builders.add(
                client().prepareIndex("idx")
                    .setSource(
                        jsonBuilder().startObject()
                            .field(STRING_FIELD, randomFrom(randomStrings))
                            .field(NUMBER_FIELD, randomIntBetween(0, 9))
                            .field(TAG_FIELD, randomBoolean() ? "more" : "less")
                            .endObject()
                    )
            );
        }

        indexRandom(true, false, builders);
        createIndex("idx_unmapped");
    }

    public void testSimpleProfile() {
        SearchResponse response = client().prepareSearch("idx")
            .setProfile(true)
            .addAggregation(histogram("histo").field(NUMBER_FIELD).interval(1L))
            .get();
        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(getNumShards("idx").numPrimaries));
        for (ProfileShardResult profileShardResult : profileResults.values()) {
            assertThat(profileShardResult, notNullValue());
            AggregationProfileShardResult aggProfileResults = profileShardResult.getAggregationProfileResults();
            assertThat(aggProfileResults, notNullValue());
            List<ProfileResult> aggProfileResultsList = aggProfileResults.getProfileResults();
            assertThat(aggProfileResultsList, notNullValue());
            assertThat(aggProfileResultsList.size(), equalTo(1));
            ProfileResult histoAggResult = aggProfileResultsList.get(0);
            assertThat(histoAggResult, notNullValue());
            assertThat(histoAggResult.getQueryName(), equalTo("NumericHistogramAggregator"));
            assertThat(histoAggResult.getLuceneDescription(), equalTo("histo"));
            assertThat(histoAggResult.getProfiledChildren().size(), equalTo(0));
            assertThat(histoAggResult.getTime(), greaterThan(0L));
            Map<String, Long> breakdown = histoAggResult.getTimeBreakdown();
            assertThat(breakdown, notNullValue());
            if (histoAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(breakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
                for (QueryProfileShardResult collectorResult : profileShardResult.getQueryProfileResults()) {
                    assertCollectorResultWithConcurrentSearchEnabled(collectorResult, 2);
                }
            } else {
                assertThat(breakdown.keySet(), equalTo(BREAKDOWN_KEYS));
                for (QueryProfileShardResult collectorResult : profileShardResult.getQueryProfileResults()) {
                    assertCollectorResult(collectorResult, 2);
                }
            }
            assertThat(breakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(breakdown.get(COLLECT), greaterThan(0L));
            assertThat(breakdown.get(BUILD_AGGREGATION).longValue(), greaterThan(0L));
            assertThat(breakdown.get(REDUCE), equalTo(0L));
            Map<String, Object> debug = histoAggResult.getDebugInfo();
            assertThat(debug, notNullValue());
            assertThat(debug.keySet(), equalTo(Set.of(TOTAL_BUCKETS)));
            assertThat(((Number) debug.get(TOTAL_BUCKETS)).longValue(), greaterThan(0L));
        }
    }

    public void testMultiLevelProfile() {
        SearchResponse response = client().prepareSearch("idx")
            .setProfile(true)
            .addAggregation(
                histogram("histo").field(NUMBER_FIELD)
                    .interval(1L)
                    .subAggregation(
                        terms("terms").field(TAG_FIELD)
                            .order(BucketOrder.aggregation("avg", false))
                            .subAggregation(avg("avg").field(NUMBER_FIELD))
                    )
            )
            .get();
        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(getNumShards("idx").numPrimaries));
        for (ProfileShardResult profileShardResult : profileResults.values()) {
            assertThat(profileShardResult, notNullValue());
            AggregationProfileShardResult aggProfileResults = profileShardResult.getAggregationProfileResults();
            assertThat(aggProfileResults, notNullValue());
            List<ProfileResult> aggProfileResultsList = aggProfileResults.getProfileResults();
            assertThat(aggProfileResultsList, notNullValue());
            assertThat(aggProfileResultsList.size(), equalTo(1));
            ProfileResult histoAggResult = aggProfileResultsList.get(0);
            assertThat(histoAggResult, notNullValue());
            assertThat(histoAggResult.getQueryName(), equalTo("NumericHistogramAggregator"));
            assertThat(histoAggResult.getLuceneDescription(), equalTo("histo"));
            assertThat(histoAggResult.getTime(), greaterThan(0L));
            Map<String, Long> histoBreakdown = histoAggResult.getTimeBreakdown();
            assertThat(histoBreakdown, notNullValue());
            if (histoAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(histoBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
                for (QueryProfileShardResult collectorResult : profileShardResult.getQueryProfileResults()) {
                    assertCollectorResultWithConcurrentSearchEnabled(collectorResult, 2);
                }
            } else {
                assertThat(histoBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
                for (QueryProfileShardResult collectorResult : profileShardResult.getQueryProfileResults()) {
                    assertCollectorResult(collectorResult, 2);
                }
            }
            assertThat(histoBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(histoBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(histoBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(histoBreakdown.get(REDUCE), equalTo(0L));
            Map<String, Object> histoDebugInfo = histoAggResult.getDebugInfo();
            assertThat(histoDebugInfo, notNullValue());
            assertThat(histoDebugInfo.keySet(), equalTo(Set.of(TOTAL_BUCKETS)));
            assertThat(((Number) histoDebugInfo.get(TOTAL_BUCKETS)).longValue(), greaterThan(0L));
            assertThat(histoAggResult.getProfiledChildren().size(), equalTo(1));

            ProfileResult termsAggResult = histoAggResult.getProfiledChildren().get(0);
            assertThat(termsAggResult, notNullValue());
            assertThat(termsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(termsAggResult.getLuceneDescription(), equalTo("terms"));
            assertThat(termsAggResult.getTime(), greaterThan(0L));
            Map<String, Long> termsBreakdown = termsAggResult.getTimeBreakdown();
            assertThat(termsBreakdown, notNullValue());
            if (termsAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(termsBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
            } else {
                assertThat(termsBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            }
            assertThat(termsBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(termsBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(termsBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(termsBreakdown.get(REDUCE), equalTo(0L));
            assertRemapTermsDebugInfo(termsAggResult);
            assertThat(termsAggResult.getProfiledChildren().size(), equalTo(1));

            ProfileResult avgAggResult = termsAggResult.getProfiledChildren().get(0);
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getLuceneDescription(), equalTo("avg"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            Map<String, Long> avgBreakdown = termsAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            if (avgAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(avgBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
            } else {
                assertThat(avgBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            }
            assertThat(avgBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(avgBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(avgBreakdown.get(REDUCE), equalTo(0L));
            assertThat(avgAggResult.getDebugInfo(), equalTo(Map.of()));
            assertThat(avgAggResult.getProfiledChildren().size(), equalTo(0));
        }
    }

    private void assertRemapTermsDebugInfo(ProfileResult termsAggResult) {
        assertThat(termsAggResult.getDebugInfo(), hasEntry(COLLECTION_STRAT, "remap"));
        assertThat(termsAggResult.getDebugInfo(), hasEntry(RESULT_STRAT, "terms"));
        assertThat(termsAggResult.getDebugInfo(), hasEntry(HAS_FILTER, false));
        // TODO we only index single valued docs but the ordinals ends up with multi valued sometimes
        assertThat(
            termsAggResult.getDebugInfo().toString(),
            (int) termsAggResult.getDebugInfo().get(SEGMENTS_WITH_SINGLE) + (int) termsAggResult.getDebugInfo().get(SEGMENTS_WITH_MULTI),
            greaterThan(0)
        );
    }

    public void testMultiLevelProfileBreadthFirst() {
        SearchResponse response = client().prepareSearch("idx")
            .setProfile(true)
            .addAggregation(
                histogram("histo").field(NUMBER_FIELD)
                    .interval(1L)
                    .subAggregation(
                        terms("terms").collectMode(SubAggCollectionMode.BREADTH_FIRST)
                            .field(TAG_FIELD)
                            .subAggregation(avg("avg").field(NUMBER_FIELD))
                    )
            )
            .get();
        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(getNumShards("idx").numPrimaries));
        for (ProfileShardResult profileShardResult : profileResults.values()) {
            assertThat(profileShardResult, notNullValue());
            AggregationProfileShardResult aggProfileResults = profileShardResult.getAggregationProfileResults();
            assertThat(aggProfileResults, notNullValue());
            List<ProfileResult> aggProfileResultsList = aggProfileResults.getProfileResults();
            assertThat(aggProfileResultsList, notNullValue());
            assertThat(aggProfileResultsList.size(), equalTo(1));
            ProfileResult histoAggResult = aggProfileResultsList.get(0);
            assertThat(histoAggResult, notNullValue());
            assertThat(histoAggResult.getQueryName(), equalTo("NumericHistogramAggregator"));
            assertThat(histoAggResult.getLuceneDescription(), equalTo("histo"));
            assertThat(histoAggResult.getTime(), greaterThan(0L));
            Map<String, Long> histoBreakdown = histoAggResult.getTimeBreakdown();
            assertThat(histoBreakdown, notNullValue());
            if (histoAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(histoBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
                for (QueryProfileShardResult collectorResult : profileShardResult.getQueryProfileResults()) {
                    assertCollectorResultWithConcurrentSearchEnabled(collectorResult, 2);
                }
            } else {
                assertThat(histoBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
                for (QueryProfileShardResult collectorResult : profileShardResult.getQueryProfileResults()) {
                    assertCollectorResult(collectorResult, 2);
                }
            }
            assertThat(histoBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(histoBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(histoBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(histoBreakdown.get(REDUCE), equalTo(0L));
            Map<String, Object> histoDebugInfo = histoAggResult.getDebugInfo();
            assertThat(histoDebugInfo, notNullValue());
            assertThat(histoDebugInfo.keySet(), equalTo(Set.of(TOTAL_BUCKETS)));
            assertThat(((Number) histoDebugInfo.get(TOTAL_BUCKETS)).longValue(), greaterThan(0L));
            assertThat(histoAggResult.getProfiledChildren().size(), equalTo(1));

            ProfileResult termsAggResult = histoAggResult.getProfiledChildren().get(0);
            assertThat(termsAggResult, notNullValue());
            assertThat(termsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(termsAggResult.getLuceneDescription(), equalTo("terms"));
            assertThat(termsAggResult.getTime(), greaterThan(0L));
            Map<String, Long> termsBreakdown = termsAggResult.getTimeBreakdown();
            assertThat(termsBreakdown, notNullValue());
            if (termsAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(termsBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
            } else {
                assertThat(termsBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            }
            assertThat(termsBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(termsBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(termsBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(termsBreakdown.get(REDUCE), equalTo(0L));
            assertRemapTermsDebugInfo(termsAggResult);
            assertThat(termsAggResult.getProfiledChildren().size(), equalTo(1));

            ProfileResult avgAggResult = termsAggResult.getProfiledChildren().get(0);
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getLuceneDescription(), equalTo("avg"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            Map<String, Long> avgBreakdown = avgAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            if (avgAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(avgBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
            } else {
                assertThat(avgBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            }
            assertThat(avgBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(avgBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(avgBreakdown.get(REDUCE), equalTo(0L));
            assertThat(avgAggResult.getDebugInfo(), equalTo(Map.of()));
            assertThat(avgAggResult.getProfiledChildren().size(), equalTo(0));
        }
    }

    public void testDiversifiedAggProfile() {
        SearchResponse response = client().prepareSearch("idx")
            .setProfile(true)
            .addAggregation(
                diversifiedSampler("diversify").shardSize(10)
                    .field(STRING_FIELD)
                    .maxDocsPerValue(2)
                    .subAggregation(max("max").field(NUMBER_FIELD))
            )
            .get();
        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(getNumShards("idx").numPrimaries));
        for (ProfileShardResult profileShardResult : profileResults.values()) {
            assertThat(profileShardResult, notNullValue());
            AggregationProfileShardResult aggProfileResults = profileShardResult.getAggregationProfileResults();
            assertThat(aggProfileResults, notNullValue());
            List<ProfileResult> aggProfileResultsList = aggProfileResults.getProfileResults();
            assertThat(aggProfileResultsList, notNullValue());
            assertThat(aggProfileResultsList.size(), equalTo(1));
            ProfileResult diversifyAggResult = aggProfileResultsList.get(0);
            assertThat(diversifyAggResult, notNullValue());
            assertThat(diversifyAggResult.getQueryName(), equalTo(DiversifiedOrdinalsSamplerAggregator.class.getSimpleName()));
            assertThat(diversifyAggResult.getLuceneDescription(), equalTo("diversify"));
            assertThat(diversifyAggResult.getTime(), greaterThan(0L));
            Map<String, Long> diversifyBreakdown = diversifyAggResult.getTimeBreakdown();
            assertThat(diversifyBreakdown, notNullValue());
            if (diversifyAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(diversifyBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
                for (QueryProfileShardResult collectorResult : profileShardResult.getQueryProfileResults()) {
                    assertCollectorResultWithConcurrentSearchEnabled(collectorResult, 2);
                }
            } else {
                assertThat(diversifyBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
                for (QueryProfileShardResult collectorResult : profileShardResult.getQueryProfileResults()) {
                    assertCollectorResult(collectorResult, 2);
                }
            }
            assertThat(diversifyBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(diversifyBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(diversifyBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(diversifyBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(diversifyBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(diversifyBreakdown.get(REDUCE), equalTo(0L));
            assertThat(diversifyAggResult.getDebugInfo(), equalTo(Map.of(DEFERRED, List.of("max"))));
            assertThat(diversifyAggResult.getProfiledChildren().size(), equalTo(1));

            ProfileResult maxAggResult = diversifyAggResult.getProfiledChildren().get(0);
            assertThat(maxAggResult, notNullValue());
            assertThat(maxAggResult.getQueryName(), equalTo("MaxAggregator"));
            assertThat(maxAggResult.getLuceneDescription(), equalTo("max"));
            assertThat(maxAggResult.getTime(), greaterThan(0L));
            Map<String, Long> maxBreakdown = maxAggResult.getTimeBreakdown();
            assertThat(maxBreakdown, notNullValue());
            if (maxAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(maxBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
            } else {
                assertThat(maxBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            }
            assertThat(diversifyBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(diversifyBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(diversifyBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(diversifyBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(maxBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(maxBreakdown.get(REDUCE), equalTo(0L));
            assertThat(maxAggResult.getDebugInfo(), equalTo(Map.of()));
            assertThat(maxAggResult.getProfiledChildren().size(), equalTo(0));
        }
    }

    public void testComplexProfile() {
        SearchResponse response = client().prepareSearch("idx")
            .setProfile(true)
            .addAggregation(
                histogram("histo").field(NUMBER_FIELD)
                    .interval(1L)
                    .subAggregation(
                        terms("tags").field(TAG_FIELD)
                            .subAggregation(avg("avg").field(NUMBER_FIELD))
                            .subAggregation(max("max").field(NUMBER_FIELD))
                    )
                    .subAggregation(
                        terms("strings").field(STRING_FIELD)
                            .subAggregation(avg("avg").field(NUMBER_FIELD))
                            .subAggregation(max("max").field(NUMBER_FIELD))
                            .subAggregation(
                                terms("tags").field(TAG_FIELD)
                                    .subAggregation(avg("avg").field(NUMBER_FIELD))
                                    .subAggregation(max("max").field(NUMBER_FIELD))
                            )
                    )
            )
            .get();
        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(getNumShards("idx").numPrimaries));
        for (ProfileShardResult profileShardResult : profileResults.values()) {
            assertThat(profileShardResult, notNullValue());
            AggregationProfileShardResult aggProfileResults = profileShardResult.getAggregationProfileResults();
            assertThat(aggProfileResults, notNullValue());
            List<ProfileResult> aggProfileResultsList = aggProfileResults.getProfileResults();
            assertThat(aggProfileResultsList, notNullValue());
            assertThat(aggProfileResultsList.size(), equalTo(1));
            ProfileResult histoAggResult = aggProfileResultsList.get(0);
            assertThat(histoAggResult, notNullValue());
            assertThat(histoAggResult.getQueryName(), equalTo("NumericHistogramAggregator"));
            assertThat(histoAggResult.getLuceneDescription(), equalTo("histo"));
            assertThat(histoAggResult.getTime(), greaterThan(0L));
            Map<String, Long> histoBreakdown = histoAggResult.getTimeBreakdown();
            assertThat(histoBreakdown, notNullValue());
            if (histoAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(histoBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
                for (QueryProfileShardResult collectorResult : profileShardResult.getQueryProfileResults()) {
                    assertCollectorResultWithConcurrentSearchEnabled(collectorResult, 2);
                }
            } else {
                assertThat(histoBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
                for (QueryProfileShardResult collectorResult : profileShardResult.getQueryProfileResults()) {
                    assertCollectorResult(collectorResult, 2);
                }
            }
            assertThat(histoBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(histoBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(histoBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(histoBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(histoBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(histoBreakdown.get(REDUCE), equalTo(0L));
            Map<String, Object> histoDebugInfo = histoAggResult.getDebugInfo();
            assertThat(histoDebugInfo, notNullValue());
            assertThat(histoDebugInfo.keySet(), equalTo(Set.of(TOTAL_BUCKETS)));
            assertThat(((Number) histoDebugInfo.get(TOTAL_BUCKETS)).longValue(), greaterThan(0L));
            assertThat(histoAggResult.getProfiledChildren().size(), equalTo(2));

            Map<String, ProfileResult> histoAggResultSubAggregations = histoAggResult.getProfiledChildren()
                .stream()
                .collect(Collectors.toMap(ProfileResult::getLuceneDescription, s -> s));

            ProfileResult tagsAggResult = histoAggResultSubAggregations.get("tags");
            assertThat(tagsAggResult, notNullValue());
            assertThat(tagsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(tagsAggResult.getTime(), greaterThan(0L));
            Map<String, Long> tagsBreakdown = tagsAggResult.getTimeBreakdown();
            assertThat(tagsBreakdown, notNullValue());
            if (tagsAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(tagsBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
            } else {
                assertThat(tagsBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            }
            assertThat(tagsBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(tagsBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(tagsBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(tagsBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(tagsBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(tagsBreakdown.get(REDUCE), equalTo(0L));
            assertRemapTermsDebugInfo(tagsAggResult);
            assertThat(tagsAggResult.getProfiledChildren().size(), equalTo(2));

            Map<String, ProfileResult> tagsAggResultSubAggregations = tagsAggResult.getProfiledChildren()
                .stream()
                .collect(Collectors.toMap(ProfileResult::getLuceneDescription, s -> s));

            ProfileResult avgAggResult = tagsAggResultSubAggregations.get("avg");
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            Map<String, Long> avgBreakdown = avgAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            if (avgAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(avgBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
            } else {
                assertThat(avgBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            }
            assertThat(avgBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(avgBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(avgBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(avgBreakdown.get(REDUCE), equalTo(0L));
            assertThat(avgAggResult.getDebugInfo(), equalTo(Map.of()));
            assertThat(avgAggResult.getProfiledChildren().size(), equalTo(0));

            ProfileResult maxAggResult = tagsAggResultSubAggregations.get("max");
            assertThat(maxAggResult, notNullValue());
            assertThat(maxAggResult.getQueryName(), equalTo("MaxAggregator"));
            assertThat(maxAggResult.getTime(), greaterThan(0L));
            Map<String, Long> maxBreakdown = maxAggResult.getTimeBreakdown();
            assertThat(maxBreakdown, notNullValue());
            if (maxAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(maxBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
            } else {
                assertThat(maxBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            }
            assertThat(maxBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(maxBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(maxBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(maxBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(maxBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(maxBreakdown.get(REDUCE), equalTo(0L));
            assertThat(maxAggResult.getDebugInfo(), equalTo(Map.of()));
            assertThat(maxAggResult.getProfiledChildren().size(), equalTo(0));

            ProfileResult stringsAggResult = histoAggResultSubAggregations.get("strings");
            assertThat(stringsAggResult, notNullValue());
            assertThat(stringsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(stringsAggResult.getTime(), greaterThan(0L));
            Map<String, Long> stringsBreakdown = stringsAggResult.getTimeBreakdown();
            assertThat(stringsBreakdown, notNullValue());
            if (stringsAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(stringsBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
            } else {
                assertThat(stringsBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            }
            assertThat(stringsBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(stringsBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(stringsBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(stringsBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(stringsBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(stringsBreakdown.get(REDUCE), equalTo(0L));
            assertRemapTermsDebugInfo(stringsAggResult);
            assertThat(stringsAggResult.getProfiledChildren().size(), equalTo(3));

            Map<String, ProfileResult> stringsAggResultSubAggregations = stringsAggResult.getProfiledChildren()
                .stream()
                .collect(Collectors.toMap(ProfileResult::getLuceneDescription, s -> s));

            avgAggResult = stringsAggResultSubAggregations.get("avg");
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            avgBreakdown = avgAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            if (avgAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(avgBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
            } else {
                assertThat(avgBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            }
            assertThat(avgBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(avgBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(avgBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(avgBreakdown.get(REDUCE), equalTo(0L));
            assertThat(avgAggResult.getDebugInfo(), equalTo(Map.of()));
            assertThat(avgAggResult.getProfiledChildren().size(), equalTo(0));

            maxAggResult = stringsAggResultSubAggregations.get("max");
            assertThat(maxAggResult, notNullValue());
            assertThat(maxAggResult.getQueryName(), equalTo("MaxAggregator"));
            assertThat(maxAggResult.getTime(), greaterThan(0L));
            maxBreakdown = maxAggResult.getTimeBreakdown();
            assertThat(maxBreakdown, notNullValue());
            if (maxAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(maxBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
            } else {
                assertThat(maxBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            }
            assertThat(maxBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(maxBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(maxBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(maxBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(maxBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(maxBreakdown.get(REDUCE), equalTo(0L));
            assertThat(maxAggResult.getDebugInfo(), equalTo(Map.of()));
            assertThat(maxAggResult.getProfiledChildren().size(), equalTo(0));

            tagsAggResult = stringsAggResultSubAggregations.get("tags");
            assertThat(tagsAggResult, notNullValue());
            assertThat(tagsAggResult.getQueryName(), equalTo(GlobalOrdinalsStringTermsAggregator.class.getSimpleName()));
            assertThat(tagsAggResult.getLuceneDescription(), equalTo("tags"));
            assertThat(tagsAggResult.getTime(), greaterThan(0L));
            tagsBreakdown = tagsAggResult.getTimeBreakdown();
            assertThat(tagsBreakdown, notNullValue());
            if (tagsAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(tagsBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
            } else {
                assertThat(tagsBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            }
            assertThat(tagsBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(tagsBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(tagsBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(tagsBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(tagsBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(tagsBreakdown.get(REDUCE), equalTo(0L));
            assertRemapTermsDebugInfo(tagsAggResult);
            assertThat(tagsAggResult.getProfiledChildren().size(), equalTo(2));

            tagsAggResultSubAggregations = tagsAggResult.getProfiledChildren()
                .stream()
                .collect(Collectors.toMap(ProfileResult::getLuceneDescription, s -> s));

            avgAggResult = tagsAggResultSubAggregations.get("avg");
            assertThat(avgAggResult, notNullValue());
            assertThat(avgAggResult.getQueryName(), equalTo("AvgAggregator"));
            assertThat(avgAggResult.getTime(), greaterThan(0L));
            avgBreakdown = avgAggResult.getTimeBreakdown();
            assertThat(avgBreakdown, notNullValue());
            if (avgAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(avgBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
            } else {
                assertThat(avgBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            }
            assertThat(avgBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(avgBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(avgBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(avgBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(avgBreakdown.get(REDUCE), equalTo(0L));
            assertThat(avgAggResult.getDebugInfo(), equalTo(Map.of()));
            assertThat(avgAggResult.getProfiledChildren().size(), equalTo(0));

            maxAggResult = tagsAggResultSubAggregations.get("max");
            assertThat(maxAggResult, notNullValue());
            assertThat(maxAggResult.getQueryName(), equalTo("MaxAggregator"));
            assertThat(maxAggResult.getTime(), greaterThan(0L));
            maxBreakdown = maxAggResult.getTimeBreakdown();
            assertThat(maxBreakdown, notNullValue());
            if (maxAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertThat(maxBreakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
            } else {
                assertThat(maxBreakdown.keySet(), equalTo(BREAKDOWN_KEYS));
            }
            assertThat(maxBreakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(maxBreakdown.get(BUILD_LEAF_COLLECTOR), greaterThan(0L));
            assertThat(maxBreakdown.get(COLLECT), greaterThan(0L));
            assertThat(maxBreakdown.get(POST_COLLECTION), greaterThan(0L));
            assertThat(maxBreakdown.get(BUILD_AGGREGATION), greaterThan(0L));
            assertThat(maxBreakdown.get(REDUCE), equalTo(0L));
            assertThat(maxAggResult.getDebugInfo(), equalTo(Map.of()));
            assertThat(maxAggResult.getProfiledChildren().size(), equalTo(0));
        }
    }

    public void testNoProfile() {
        SearchResponse response = client().prepareSearch("idx")
            .setProfile(false)
            .addAggregation(
                histogram("histo").field(NUMBER_FIELD)
                    .interval(1L)
                    .subAggregation(
                        terms("tags").field(TAG_FIELD)
                            .subAggregation(avg("avg").field(NUMBER_FIELD))
                            .subAggregation(max("max").field(NUMBER_FIELD))
                    )
                    .subAggregation(
                        terms("strings").field(STRING_FIELD)
                            .subAggregation(avg("avg").field(NUMBER_FIELD))
                            .subAggregation(max("max").field(NUMBER_FIELD))
                            .subAggregation(
                                terms("tags").field(TAG_FIELD)
                                    .subAggregation(avg("avg").field(NUMBER_FIELD))
                                    .subAggregation(max("max").field(NUMBER_FIELD))
                            )
                    )
            )
            .get();
        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(0));
    }

    public void testGlobalAggWithStatsSubAggregatorProfile() {
        boolean profileEnabled = true;
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(global("global").subAggregation(stats("value_stats").field(NUMBER_FIELD)))
            .setProfile(profileEnabled)
            .get();

        assertSearchResponse(response);
        Global global = response.getAggregations().get("global");
        assertThat(global, IsNull.notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo((long) numDocs));
        assertThat((long) ((InternalAggregation) global).getProperty("_count"), equalTo((long) numDocs));
        assertThat(global.getAggregations().asList().isEmpty(), is(false));

        Stats stats = global.getAggregations().get("value_stats");
        assertThat((Stats) ((InternalAggregation) global).getProperty("value_stats"), sameInstance(stats));
        assertThat(stats, IsNull.notNullValue());
        assertThat(stats.getName(), equalTo("value_stats"));

        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(getNumShards("idx").numPrimaries));
        for (ProfileShardResult profileShardResult : profileResults.values()) {
            assertThat(profileShardResult, notNullValue());
            List<QueryProfileShardResult> queryProfileShardResults = profileShardResult.getQueryProfileResults();
            assertEquals(queryProfileShardResults.size(), 2);
            // ensure there is no multi collector getting added with only global agg
            for (QueryProfileShardResult queryProfileShardResult : queryProfileShardResults) {
                assertEquals(queryProfileShardResult.getQueryResults().size(), 1);
                if (queryProfileShardResult.getQueryResults().get(0).getQueryName().equals("MatchAllDocsQuery")) {
                    assertEquals(0, queryProfileShardResult.getQueryResults().get(0).getProfiledChildren().size());
                    assertEquals("search_top_hits", queryProfileShardResult.getCollectorResult().getReason());
                    assertEquals(0, queryProfileShardResult.getCollectorResult().getProfiledChildren().size());
                } else if (queryProfileShardResult.getQueryResults().get(0).getQueryName().equals("ConstantScoreQuery")) {
                    assertEquals(1, queryProfileShardResult.getQueryResults().get(0).getProfiledChildren().size());
                    assertEquals("aggregation_global", queryProfileShardResult.getCollectorResult().getReason());
                    assertEquals(0, queryProfileShardResult.getCollectorResult().getProfiledChildren().size());
                } else {
                    fail("unexpected profile shard result in the response");
                }
            }
            AggregationProfileShardResult aggProfileResults = profileShardResult.getAggregationProfileResults();
            assertThat(aggProfileResults, notNullValue());
            List<ProfileResult> aggProfileResultsList = aggProfileResults.getProfileResults();
            assertThat(aggProfileResultsList, notNullValue());
            assertEquals(1, aggProfileResultsList.size());
            ProfileResult globalAggResult = aggProfileResultsList.get(0);
            assertThat(globalAggResult, notNullValue());
            assertEquals("GlobalAggregator", globalAggResult.getQueryName());
            assertEquals("global", globalAggResult.getLuceneDescription());
            assertEquals(1, globalAggResult.getProfiledChildren().size());
            assertThat(globalAggResult.getTime(), greaterThan(0L));
            Map<String, Long> breakdown = globalAggResult.getTimeBreakdown();
            assertThat(breakdown, notNullValue());
            if (globalAggResult.getMaxSliceTime() != null) {
                // concurrent segment search enabled
                assertEquals(CONCURRENT_SEARCH_BREAKDOWN_KEYS, breakdown.keySet());
                for (QueryProfileShardResult collectorResult : profileShardResult.getQueryProfileResults()) {
                    assertCollectorResultWithConcurrentSearchEnabled(collectorResult, 0);
                }
            } else {
                assertEquals(BREAKDOWN_KEYS, breakdown.keySet());
                for (QueryProfileShardResult collectorResult : profileShardResult.getQueryProfileResults()) {
                    assertCollectorResult(collectorResult, 0);
                }
            }
            assertThat(breakdown.get(INITIALIZE), greaterThan(0L));
            assertThat(breakdown.get(COLLECT), greaterThan(0L));
            assertThat(breakdown.get(BUILD_AGGREGATION).longValue(), greaterThan(0L));
            assertEquals(0, breakdown.get(REDUCE).intValue());
        }
    }

    public void testMultipleAggregationsProfile() {
        SearchResponse response = client().prepareSearch("idx")
            .setProfile(true)
            .addAggregation(histogram("histo_1").field(NUMBER_FIELD).interval(1L))
            .addAggregation(histogram("histo_2").field(NUMBER_FIELD).interval(1L))
            .get();
        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertThat(profileResults, notNullValue());
        assertThat(profileResults.size(), equalTo(getNumShards("idx").numPrimaries));
        for (ProfileShardResult profileShardResult : profileResults.values()) {
            assertThat(profileShardResult, notNullValue());
            List<QueryProfileShardResult> queryProfilerResults = profileShardResult.getQueryProfileResults();
            assertThat(queryProfilerResults, notNullValue());
            for (QueryProfileShardResult queryProfilerResult : queryProfilerResults) {
                CollectorResult collectorResult = queryProfilerResult.getCollectorResult();
                String reason = collectorResult.getReason();
                assertThat(reason, equalTo("search_multi"));
                List<CollectorResult> children = collectorResult.getProfiledChildren();
                assertThat(children.size(), equalTo(2));
                assertThat(children.get(1).getName(), containsString("[histo_1, histo_2]"));
            }
            AggregationProfileShardResult aggProfileResults = profileShardResult.getAggregationProfileResults();
            assertThat(aggProfileResults, notNullValue());
            List<ProfileResult> aggProfileResultsList = aggProfileResults.getProfileResults();
            assertThat(aggProfileResultsList, notNullValue());
            assertThat(aggProfileResultsList.size(), equalTo(2));
            for (ProfileResult histoAggResult : aggProfileResultsList) {
                assertThat(histoAggResult, notNullValue());
                assertThat(histoAggResult.getQueryName(), equalTo("NumericHistogramAggregator"));
                assertThat(histoAggResult.getLuceneDescription(), containsString("histo_"));
                assertThat(histoAggResult.getProfiledChildren().size(), equalTo(0));
                assertThat(histoAggResult.getTime(), greaterThan(0L));
                Map<String, Long> breakdown = histoAggResult.getTimeBreakdown();
                assertThat(breakdown, notNullValue());
                if (histoAggResult.getMaxSliceTime() != null) {
                    // concurrent segment search enabled
                    assertThat(breakdown.keySet(), equalTo(CONCURRENT_SEARCH_BREAKDOWN_KEYS));
                    for (QueryProfileShardResult collectorResult : profileShardResult.getQueryProfileResults()) {
                        assertCollectorResultWithConcurrentSearchEnabled(collectorResult, 2);
                    }
                } else {
                    assertThat(breakdown.keySet(), equalTo(BREAKDOWN_KEYS));
                    for (QueryProfileShardResult collectorResult : profileShardResult.getQueryProfileResults()) {
                        assertCollectorResult(collectorResult, 2);
                    }
                }
                assertThat(breakdown.get(INITIALIZE), greaterThan(0L));
                assertThat(breakdown.get(COLLECT), greaterThan(0L));
                assertThat(breakdown.get(BUILD_AGGREGATION).longValue(), greaterThan(0L));
                assertThat(breakdown.get(REDUCE), equalTo(0L));
                Map<String, Object> debug = histoAggResult.getDebugInfo();
                assertThat(debug, notNullValue());
                assertThat(debug.keySet(), equalTo(Set.of(TOTAL_BUCKETS)));
                assertThat(((Number) debug.get(TOTAL_BUCKETS)).longValue(), greaterThan(0L));
            }
        }
    }

    private void assertCollectorResult(QueryProfileShardResult collectorResult, int expectedChildrenCount) {
        long nodeTime = collectorResult.getCollectorResult().getTime();
        assertThat(collectorResult.getCollectorResult().getMaxSliceTime(), equalTo(nodeTime));
        assertThat(collectorResult.getCollectorResult().getMinSliceTime(), equalTo(nodeTime));
        assertThat(collectorResult.getCollectorResult().getAvgSliceTime(), equalTo(nodeTime));
        assertThat(collectorResult.getCollectorResult().getReduceTime(), equalTo(0L));
        assertThat(collectorResult.getCollectorResult().getSliceCount(), equalTo(1));
        assertThat(collectorResult.getCollectorResult().getProfiledChildren().size(), equalTo(expectedChildrenCount));
        if (expectedChildrenCount == 2) {
            assertThat(collectorResult.getCollectorResult().getProfiledChildren().get(0).getReason(), equalTo(REASON_SEARCH_TOP_HITS));
            assertThat(collectorResult.getCollectorResult().getProfiledChildren().get(1).getReason(), equalTo(REASON_AGGREGATION));
        }
    }

    private void assertCollectorResultWithConcurrentSearchEnabled(QueryProfileShardResult collectorResult, int expectedChildrenCount) {
        long nodeTime = collectorResult.getCollectorResult().getTime();
        assertThat(collectorResult.getCollectorResult().getMaxSliceTime(), lessThanOrEqualTo(nodeTime));
        assertThat(collectorResult.getCollectorResult().getMinSliceTime(), lessThanOrEqualTo(nodeTime));
        assertThat(collectorResult.getCollectorResult().getAvgSliceTime(), lessThanOrEqualTo(nodeTime));
        assertThat(collectorResult.getCollectorResult().getReduceTime(), greaterThan(0L));
        assertThat(collectorResult.getCollectorResult().getSliceCount(), greaterThanOrEqualTo(1));
        assertThat(collectorResult.getCollectorResult().getProfiledChildren().size(), equalTo(expectedChildrenCount));
        if (expectedChildrenCount == 2) {
            assertThat(collectorResult.getCollectorResult().getProfiledChildren().get(0).getReason(), equalTo(REASON_SEARCH_TOP_HITS));
            assertThat(collectorResult.getCollectorResult().getProfiledChildren().get(1).getReason(), equalTo(REASON_AGGREGATION));
        }
    }

    public void testTopHitsAggregationFetchProfiling() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setProfile(true)
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(topHits("top_hits_agg1").size(1))
            .addAggregation(topHits("top_hits_agg2").size(1).sort(SortBuilders.fieldSort(NUMBER_FIELD).order(SortOrder.DESC)))
            .get();

        assertSearchResponse(response);
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        assertNotNull("Profile results should not be null", profileResults);
        assertFalse("Profile results should not be empty", profileResults.isEmpty());

        int shardsWithDocuments = 0;
        int shardsWithCorrectProfile = 0;

        for (ProfileShardResult shardResult : profileResults.values()) {
            FetchProfileShardResult fetchProfileResult = shardResult.getFetchProfileResult();
            if (fetchProfileResult != null && !fetchProfileResult.getFetchProfileResults().isEmpty()) {
                shardsWithDocuments++;
                List<ProfileResult> fetchProfileResults = fetchProfileResult.getFetchProfileResults();

                // Count different types of fetch operations dynamically
                int mainFetchCount = 0;
                int topHitsAgg1Count = 0;
                int topHitsAgg2Count = 0;
                ProfileResult topHitsFetch1 = null;
                ProfileResult topHitsFetch2 = null;

                for (ProfileResult result : fetchProfileResults) {
                    if ("fetch".equals(result.getQueryName())) {
                        mainFetchCount++;
                    } else if (result.getQueryName().contains("top_hits_agg1")) {
                        if (topHitsFetch1 == null) {
                            topHitsFetch1 = result; // Keep first instance for validation
                        }
                        topHitsAgg1Count++;
                    } else if (result.getQueryName().contains("top_hits_agg2")) {
                        if (topHitsFetch2 == null) {
                            topHitsFetch2 = result; // Keep first instance for validation
                        }
                        topHitsAgg2Count++;
                    }
                }

                // Verify we have the expected aggregations (concurrent search may create multiple instances)
                assertTrue("Should have at least 1 top_hits_agg1 fetch operation", topHitsAgg1Count >= 1);
                assertTrue("Should have at least 1 top_hits_agg2 fetch operation", topHitsAgg2Count >= 1);
                assertTrue("Should have at least one main fetch operation", mainFetchCount >= 1);
                assertTrue("Should have at least 3 total fetch operations", fetchProfileResults.size() >= 3);

                assertNotNull("Should have top_hits_agg1 fetch operation", topHitsFetch1);
                assertTrue("Should be top_hits aggregation fetch", topHitsFetch1.getQueryName().startsWith("fetch_top_hits_aggregation"));
                assertTrue("Should contain aggregation name", topHitsFetch1.getQueryName().contains("top_hits_agg1"));
                assertNotNull(topHitsFetch1.getTimeBreakdown());
                assertEquals("Top hits fetch should have 1 child (FetchSourcePhase)", 1, topHitsFetch1.getProfiledChildren().size());
                assertEquals("FetchSourcePhase", topHitsFetch1.getProfiledChildren().get(0).getQueryName());

                assertNotNull("Should have top_hits_agg2 fetch operation", topHitsFetch2);
                assertTrue("Should be top_hits aggregation fetch", topHitsFetch2.getQueryName().startsWith("fetch_top_hits_aggregation"));
                assertTrue("Should contain aggregation name", topHitsFetch2.getQueryName().contains("top_hits_agg2"));
                assertNotNull(topHitsFetch2.getTimeBreakdown());
                assertEquals("Top hits fetch should have 1 child (FetchSourcePhase)", 1, topHitsFetch2.getProfiledChildren().size());
                assertEquals("FetchSourcePhase", topHitsFetch2.getProfiledChildren().get(0).getQueryName());

                for (ProfileResult fetchResult : fetchProfileResults) {
                    Map<String, Long> breakdown = fetchResult.getTimeBreakdown();
                    assertTrue(
                        "CREATE_STORED_FIELDS_VISITOR timing should be present",
                        breakdown.containsKey("create_stored_fields_visitor")
                    );
                    assertTrue("BUILD_SUB_PHASE_PROCESSORS timing should be present", breakdown.containsKey("build_sub_phase_processors"));
                    assertTrue("GET_NEXT_READER timing should be present", breakdown.containsKey("get_next_reader"));
                    assertTrue("LOAD_STORED_FIELDS timing should be present", breakdown.containsKey("load_stored_fields"));
                    assertTrue("LOAD_SOURCE timing should be present", breakdown.containsKey("load_source"));
                }

                shardsWithCorrectProfile++;
            }
        }

        assertTrue("Should have at least one shard with documents", shardsWithDocuments > 0);
        assertEquals(
            "All shards with documents should have correct fetch profile structure",
            shardsWithDocuments,
            shardsWithCorrectProfile
        );
    }
}
