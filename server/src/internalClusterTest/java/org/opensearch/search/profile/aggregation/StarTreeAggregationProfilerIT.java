/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.aggregation;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class StarTreeAggregationProfilerIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final String PRE_COMPUTE = AggregationTimingType.PRE_COMPUTE.toString();
    private static final String BUILD_LEAF_COLLECTOR = AggregationTimingType.BUILD_LEAF_COLLECTOR.toString();
    private static final String COLLECT = AggregationTimingType.COLLECT.toString();
    private static final String POST_COLLECTION = AggregationTimingType.POST_COLLECTION.toString();
    private static final String INITIALIZE = AggregationTimingType.INITIALIZE.toString();
    private static final String BUILD_AGGREGATION = AggregationTimingType.BUILD_AGGREGATION.toString();
    private static final String REDUCE = AggregationTimingType.REDUCE.toString();
    private static final Set<String> BREAKDOWN_KEYS = Set.of(
        INITIALIZE,
        PRE_COMPUTE,
        BUILD_LEAF_COLLECTOR,
        COLLECT,
        POST_COLLECTION,
        BUILD_AGGREGATION,
        REDUCE,
        INITIALIZE + "_count",
        PRE_COMPUTE + "_count",
        BUILD_LEAF_COLLECTOR + "_count",
        COLLECT + "_count",
        POST_COLLECTION + "_count",
        BUILD_AGGREGATION + "_count",
        REDUCE + "_count",
        INITIALIZE + "_start_time",
        PRE_COMPUTE + "_start_time",
        BUILD_LEAF_COLLECTOR + "_start_time",
        COLLECT + "_start_time",
        POST_COLLECTION + "_start_time",
        BUILD_AGGREGATION + "_start_time",
        REDUCE + "_start_time"
    );

    private static final Set<String> CONCURRENT_SEARCH_BREAKDOWN_KEYS = Set.of(
        INITIALIZE,
        PRE_COMPUTE,
        BUILD_LEAF_COLLECTOR,
        COLLECT,
        POST_COLLECTION,
        BUILD_AGGREGATION,
        REDUCE,
        INITIALIZE + "_count",
        PRE_COMPUTE + "_count",
        BUILD_LEAF_COLLECTOR + "_count",
        COLLECT + "_count",
        POST_COLLECTION + "_count",
        BUILD_AGGREGATION + "_count",
        REDUCE + "_count",
        "max_" + INITIALIZE,
        "max_" + PRE_COMPUTE,
        "max_" + BUILD_LEAF_COLLECTOR,
        "max_" + COLLECT,
        "max_" + POST_COLLECTION,
        "max_" + BUILD_AGGREGATION,
        "max_" + REDUCE,
        "min_" + INITIALIZE,
        "min_" + PRE_COMPUTE,
        "min_" + BUILD_LEAF_COLLECTOR,
        "min_" + COLLECT,
        "min_" + POST_COLLECTION,
        "min_" + BUILD_AGGREGATION,
        "min_" + REDUCE,
        "avg_" + INITIALIZE,
        "avg_" + PRE_COMPUTE,
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

    public StarTreeAggregationProfilerIT(Settings staticSettings) {
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
}
