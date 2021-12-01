/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.stats;

import org.junit.After;
import org.junit.Before;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.metrics.MetricsTracker;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchService;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class MetricsTrackerIT extends OpenSearchIntegTestCase {

    String index = "idx";

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .addMapping("type", "tag", "type=keyword", "value", "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), false)
                )
        );
        addDocuments(index, 100);
    }

    public void testToggleResourceTrackingSetting() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Collections.singletonMap(SearchService.RESOURCE_TRACKING_SETTING.getKey(), false))
        );

        SearchRequestBuilder queryBuilder = client().prepareSearch(index).setQuery(new MatchQueryBuilder("tag", "tag99")).setSize(5);
        assertSearchResponse(queryBuilder.get());
        assertNull(SearchMetricsTrackerPlugin.queryTracker);
        assertNull(SearchMetricsTrackerPlugin.fetchTracker);

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Collections.singletonMap(SearchService.RESOURCE_TRACKING_SETTING.getKey(), true))
        );
        assertSearchResponse(queryBuilder.get());
        assertNotNull(SearchMetricsTrackerPlugin.queryTracker);
        assertNotNull(SearchMetricsTrackerPlugin.fetchTracker);
    }

    public void testQueryPhaseMemoryTracking() {
        // While exact memory usage per query is hard to assert and will vary by platform,
        // this test runs 3 increasingly expensive queries and validates the ordering of memory usage
        SearchRequestBuilder simpleMatchQuery = client().prepareSearch(index).setQuery(new MatchQueryBuilder("tag", "tag1")).setSize(0);
        long simpleMatchMemory = getResourcesForQuery(simpleMatchQuery, false);

        SearchRequestBuilder multiLevelAggQuery = client().prepareSearch(index)
            .setSize(0)
            .addAggregation(terms("values").field("value").subAggregation(terms("tagSub").field("tag")));
        long multiLevelAggMemory = getResourcesForQuery(multiLevelAggQuery, false);
        assertTrue(
            "multiLevelAggQuery memory consumption of ["
                + multiLevelAggMemory
                + "] was found to be lower than simple match query ["
                + simpleMatchMemory
                + "]",
            multiLevelAggMemory > simpleMatchMemory
        );
    }

    public void testFetchPhaseMemoryTracking() {
        SearchRequestBuilder simpleMatchQuery5 = client().prepareSearch(index).setQuery(new MatchQueryBuilder("tag", "tag99")).setSize(5);
        long simpleMatchMemory5 = getResourcesForQuery(simpleMatchQuery5, true);

        SearchRequestBuilder simpleMatchQuery1000 = client().prepareSearch(index).setQuery(matchQuery("tag", "tag99")).setSize(1000);
        long simpleMatchMemory1000 = getResourcesForQuery(simpleMatchQuery1000, true);
        assert simpleMatchMemory1000 > simpleMatchMemory5;
    }

    // Helper that ingests duplicate docs
    private void addDocuments(String index, int count) throws Exception {
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            for (int j = 0; j < i; j++) {
                builders.add(
                    client().prepareIndex(index, "type")
                        .setSource(jsonBuilder().startObject().field("value", "val-" + i).field("tag", "tag" + i).endObject())
                );
            }
        }
        indexRandom(true, builders);
    }

    // helper that returns the memory allocated by a query
    private long getResourcesForQuery(SearchRequestBuilder request, boolean fetchPhase) {
        // We are doing a warm-up run as first run uses showing higher memory
        // Why does first run in test takes more memory - points towards caching but cache is being cleared after each run.
        // Is there some other initialization overhead? Will buy a beer to anyone who can help answer this
        for (int i = 0; i < 2; i++) {
            assertSearchResponse(request.get());
            client().admin().indices().prepareClearCache("*").setFieldDataCache(true).setQueryCache(true).setRequestCache(true).execute();
        }
        return fetchPhase ? SearchMetricsTrackerPlugin.fetchMemorySnapshot : SearchMetricsTrackerPlugin.queryMemorySnapshot;
    }

    @Before
    public void setup() throws Exception {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Collections.singletonMap(SearchService.RESOURCE_TRACKING_SETTING.getKey(), true))
        );
    }

    @After
    public void cleanup() throws Exception {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(SearchService.RESOURCE_TRACKING_SETTING.getKey()))
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(SearchMetricsTrackerPlugin.class);
    }

    // Helper plugin tracks the resource tracker against the last shard that was queried on the node
    public static class SearchMetricsTrackerPlugin extends Plugin {
        public static MetricsTracker fetchTracker;
        public static MetricsTracker queryTracker;

        // As query and fetch trackers are references which get reset, capture last snapshots here
        public static long fetchMemorySnapshot;
        public static long queryMemorySnapshot;

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
                    queryTracker = searchContext.resourceTracker();
                    if (queryTracker != null) {
                        queryMemorySnapshot = queryTracker.getMemoryAllocated();
                    }
                }

                @Override
                public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
                    fetchTracker = searchContext.resourceTracker();
                    if (fetchTracker != null) {
                        fetchMemorySnapshot = fetchTracker.getMemoryAllocated();
                    }
                }
            });
            super.onIndexModule(indexModule);
        }
    }
}
