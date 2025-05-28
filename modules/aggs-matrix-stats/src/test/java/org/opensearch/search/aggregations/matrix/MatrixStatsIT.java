/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.matrix;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class MatrixStatsIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    public MatrixStatsIT(Settings nodeSettings) {
        super(nodeSettings);
    }

    public void testMatrixStatsMultiValueModeEffect() throws Exception {
        String index = "test_matrix_stats_multimode";
        Client client = client();

        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                )
                .get()
        );

        client.prepareIndex(index).setId("1").setSource("num", List.of(10, 30), "num2", List.of(40, 60)).setWaitForActiveShards(1).get();
        client.admin().indices().prepareRefresh(index).get();

        MatrixStatsAggregationBuilder avgAgg = new MatrixStatsAggregationBuilder("agg_avg").fields(List.of("num", "num2"))
            .multiValueMode(MultiValueMode.AVG);

        client.prepareSearch(index).setSize(0).setRequestCache(true).addAggregation(avgAgg).get();

        RequestCacheStats stats1 = getRequestCacheStats(client, index);
        long hit1 = stats1.getHitCount();
        long miss1 = stats1.getMissCount();

        client.prepareSearch(index).setSize(0).setRequestCache(true).addAggregation(avgAgg).get();

        RequestCacheStats stats2 = getRequestCacheStats(client, index);
        long hit2 = stats2.getHitCount();
        long miss2 = stats2.getMissCount();

        MatrixStatsAggregationBuilder minAgg = new MatrixStatsAggregationBuilder("agg_min").fields(List.of("num", "num2"))
            .multiValueMode(MultiValueMode.MIN);

        client.prepareSearch(index).setSize(0).setRequestCache(true).addAggregation(minAgg).get();

        RequestCacheStats stats3 = getRequestCacheStats(client, index);
        long hit3 = stats3.getHitCount();
        long miss3 = stats3.getMissCount();

        client.prepareSearch(index).setSize(0).setRequestCache(true).addAggregation(minAgg).get();

        RequestCacheStats stats4 = getRequestCacheStats(client, index);
        long hit4 = stats4.getHitCount();
        long miss4 = stats4.getMissCount();

        assertEquals("Expected 1 cache miss for first AVG request", 1, miss1);
        assertEquals("Expected 1 cache hit for second AVG request", hit1 + 1, hit2);
        assertEquals("Expected 1 cache miss for first MIN request", miss1 + 1, miss3);
        assertEquals("Expected 1 cache hit for second MIN request", hit2 + 1, hit4);
        assertEquals("Expected no additional cache misses for second MIN request", miss3, miss4);
    }

    private static RequestCacheStats getRequestCacheStats(Client client, String index) {
        return client.admin().indices().prepareStats(index).setRequestCache(true).get().getTotal().getRequestCache();
    }
}
