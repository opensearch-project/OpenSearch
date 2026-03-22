/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.index.query.QueryBuilders.spanNearQuery;
import static org.opensearch.index.query.QueryBuilders.spanTermQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_PARTITION_MIN_SEGMENT_SIZE;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Integration tests for span_near queries with concurrent segment search and partition strategies.
 */
public class SpanNearQueryIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public SpanNearQueryIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY.getKey(), "segment").build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() },
            new Object[] {
                Settings.builder()
                    .put(CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY.getKey(), "force")
                    .put(CONCURRENT_SEGMENT_SEARCH_PARTITION_MIN_SEGMENT_SIZE.getKey(), 1000)
                    .build() },
            new Object[] {
                Settings.builder()
                    .put(CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY.getKey(), "balanced")
                    .put(CONCURRENT_SEGMENT_SEARCH_PARTITION_MIN_SEGMENT_SIZE.getKey(), 1000)
                    .build() }
        );
    }

    public void testSpanNearQuery() throws Exception {
        createIndex("test", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build());
        int totalDocs = 2500;
        for (int i = 0; i < totalDocs; i++) {
            String content = (i % 100 == 0) ? "alpha beta" : (i % 50 == 0) ? "alpha gamma beta" : "other words " + i;
            client().prepareIndex("test").setId(String.valueOf(i)).setSource("field", content).get();
        }
        refresh();
        forceMerge(1);
        indexRandomForConcurrentSearch("test");
        SearchResponse response = client().prepareSearch("test")
            .setQuery(spanNearQuery(spanTermQuery("field", "alpha"), 0).addClause(spanTermQuery("field", "beta")).inOrder(true))
            .get();
        assertHitCount(response, 25L);
        response = client().prepareSearch("test")
            .setQuery(spanNearQuery(spanTermQuery("field", "alpha"), 1).addClause(spanTermQuery("field", "beta")).inOrder(true))
            .get();
        assertHitCount(response, 50L);
    }
}
