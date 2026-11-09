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
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_PARTITION_MIN_SEGMENT_SIZE;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY;
import static org.opensearch.search.aggregations.AggregationBuilders.avg;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for avg aggregation with concurrent segment search partition strategies.
 */
public class AvgIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public AvgIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY.getKey(), "segment").build() },
            new Object[] { Settings.builder().put(CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY.getKey(), "force").build() },
            new Object[] {
                Settings.builder()
                    .put(CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY.getKey(), "balanced")
                    .put(CONCURRENT_SEGMENT_SEARCH_PARTITION_MIN_SEGMENT_SIZE.getKey(), 1000)
                    .build() }
        );
    }

    public void testAvgAggregation() throws Exception {
        createIndex("test_avg_agg", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());
        try {
            List<IndexRequestBuilder> builders = new ArrayList<>(5000);
            for (int i = 0; i < 5000; i++) {
                builders.add(client().prepareIndex("test_avg_agg").setSource("value", i + 1));
            }
            indexBulkWithSegments(builders, 2);
            indexRandomForConcurrentSearch("test_avg_agg");
            SearchResponse response = client().prepareSearch("test_avg_agg").addAggregation(avg("avg_agg").field("value")).get();
            assertSearchResponse(response);
            Avg avgAgg = response.getAggregations().get("avg_agg");
            assertThat(avgAgg, notNullValue());
            assertThat(avgAgg.getValue(), closeTo(2500.5, 0.1));
        } finally {
            internalCluster().wipeIndices("test_avg_agg");
        }
    }
}
