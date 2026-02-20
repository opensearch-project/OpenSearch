/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_PARTITION_MIN_SEGMENT_SIZE;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY;
import static org.opensearch.search.aggregations.AggregationBuilders.min;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for min aggregation with concurrent segment search partition strategies.
 */
public class MinIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public MinIT(Settings staticSettings) {
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

    public void testMinAggregation() throws Exception {
        createIndex("test", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build());
        try {
            int totalDocs = 5000;
            for (int i = 0; i < totalDocs; i++) {
                client().prepareIndex("test").setId(String.valueOf(i)).setSource("value", i + 1).get();
                if (i % 2500 == 2499) {
                    refresh();
                }
            }
            refresh();
            indexRandomForConcurrentSearch("test");
            SearchResponse response = client().prepareSearch("test").addAggregation(min("min_agg").field("value")).get();
            assertSearchResponse(response);
            Min minAgg = response.getAggregations().get("min_agg");
            assertThat(minAgg, notNullValue());
            assertThat(minAgg.getValue(), equalTo(1.0));
        } finally {
            internalCluster().wipeIndices("test");
        }
    }

    public void testMinAggregationMultipleShards() throws Exception {
        createIndex("test", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        try {
            int totalDocs = 5000;
            for (int i = 0; i < totalDocs; i++) {
                client().prepareIndex("test").setId(String.valueOf(i)).setSource("value", i + 1).get();
                if (i % 2500 == 2499) {
                    refresh();
                }
            }
            refresh();
            indexRandomForConcurrentSearch("test");
            SearchResponse response = client().prepareSearch("test").addAggregation(min("min_agg").field("value")).get();
            assertSearchResponse(response);
            Min minAgg = response.getAggregations().get("min_agg");
            assertThat(minAgg, notNullValue());
            assertThat(minAgg.getValue(), equalTo(1.0));
        } finally {
            internalCluster().wipeIndices("test");
        }
    }
}
