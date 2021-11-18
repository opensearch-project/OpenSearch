/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.stats;

import org.junit.AfterClass;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.metrics.ResourceTracker;
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
public class ResourceTrackerIT extends OpenSearchIntegTestCase {

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

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Collections.singletonMap(SearchService.RESOURCE_TRACKING_SETTING.getKey(), true))
        );

        addDocuments(index, 100);
    }

    public void testToggleResourceTrackingSetting() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Collections.singletonMap(SearchService.RESOURCE_TRACKING_SETTING.getKey(), false))
        );

        SearchRequestBuilder queryBuilder = client().prepareSearch(index).setQuery(new MatchQueryBuilder("tag", "tag99")).setSize(5);
        assertSearchResponse(queryBuilder.get());
        assertNull(SearchResourceTrackerPlugin.queryTracker);
        assertNull(SearchResourceTrackerPlugin.fetchTracker);

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Collections.singletonMap(SearchService.RESOURCE_TRACKING_SETTING.getKey(), true))
        );
        assertSearchResponse(queryBuilder.get());
        assertNotNull(SearchResourceTrackerPlugin.queryTracker);
        assertNotNull(SearchResourceTrackerPlugin.fetchTracker);
    }

    public void testQueryPhaseMemoryTracking() {
        // While exact memory usage per query is hard to assert and will vary by platform,
        // this test runs 3 increasingly expensive queries and validates the ordering of memory usage
        SearchRequestBuilder simpleMatchQuery = client().prepareSearch(index).setQuery(new MatchQueryBuilder("tag", "tag1")).setSize(0);
        long simpleMatchMemory = getResourcesForQuery(simpleMatchQuery, false);

        SearchRequestBuilder termsAggQuery = client().prepareSearch(index).setSize(0).addAggregation(terms("values").field("value"));
        long termsAggMemory = getResourcesForQuery(termsAggQuery, false);
        assertTrue(
            "termsAggregation memory consumption of ["
                + termsAggMemory
                + "] was found to be lower than simple match query ["
                + simpleMatchMemory
                + "]",
            termsAggMemory > simpleMatchMemory
        );

        SearchRequestBuilder multiLevelAggQuery = client().prepareSearch(index)
            .setSize(0)
            .addAggregation(terms("values").field("value").subAggregation(terms("tagSub").field("tag")));
        long multiLevelAggMemory = getResourcesForQuery(multiLevelAggQuery, false);
        assert multiLevelAggMemory > termsAggMemory;
    }

    public void testFetchPhaseMemoryTracking() {
        SearchRequestBuilder simpleMatchQuery5 = client().prepareSearch(index).setQuery(new MatchQueryBuilder("tag", "tag99")).setSize(5);
        long simpleMatchMemory5 = getResourcesForQuery(simpleMatchQuery5, true);

        SearchRequestBuilder simpleMatchQuery100 = client().prepareSearch(index)
            .setQuery(new MatchQueryBuilder("tag", "tag99"))
            .setSize(50);
        long simpleMatchMemory100 = getResourcesForQuery(simpleMatchQuery100, true);
        assert simpleMatchMemory100 > simpleMatchMemory5;

        SearchRequestBuilder simpleMatchQuery300 = client().prepareSearch(index).setQuery(matchQuery("tag", "tag99")).setSize(100);
        long simpleMatchMemory300 = getResourcesForQuery(simpleMatchQuery300, true);
        assert simpleMatchMemory300 > simpleMatchMemory100;
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
        // we are doing some warm-up runs as first run uses showing higher memory
        // TODO: why first run in test takes more memory - points towards caching but cache is being cleared after each run
        // is there some other initialization overhead? buy a beer to anyone who can help answer this
        for (int i = 0; i < 2; i++) {
            assertSearchResponse(request.get());
            client().admin().indices().prepareClearCache("*").setFieldDataCache(true).setQueryCache(true).setRequestCache(true).execute();
        }
        return fetchPhase ? SearchResourceTrackerPlugin.fetchMemorySnapshot : SearchResourceTrackerPlugin.queryMemorySnapshot;
    }

    @AfterClass
    public static void cleanup() throws Exception {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull(SearchService.RESOURCE_TRACKING_SETTING.getKey()))
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(SearchResourceTrackerPlugin.class);
    }

    // Helper plugin tracks the resource tracker against the last shard that was queried on the node
    public static class SearchResourceTrackerPlugin extends Plugin {
        public static ResourceTracker fetchTracker;
        public static ResourceTracker queryTracker;

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
