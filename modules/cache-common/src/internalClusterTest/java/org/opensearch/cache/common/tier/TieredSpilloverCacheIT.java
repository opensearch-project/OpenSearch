/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.store.OpenSearchOnHeapCache;
import org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.plugins.CachePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;
import org.junit.Assert;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cache.common.tier.TieredSpilloverCacheSettings.INVALID_SEGMENT_NUMBER_EXCEPTION_MESSAGE;
import static org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings.MAXIMUM_SIZE_IN_BYTES_KEY;
import static org.opensearch.indices.IndicesService.INDICES_CACHE_CLEAN_INTERVAL_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.greaterThan;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 0, scope = OpenSearchIntegTestCase.Scope.TEST)
public class TieredSpilloverCacheIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(TieredSpilloverCachePlugin.class, MockDiskCachePlugin.class);
    }

    static Settings defaultSettings(String onHeapCacheSizeInBytesOrPercentage, int numberOfSegments) {
        return Settings.builder()
            .put(FeatureFlags.PLUGGABLE_CACHE, "true")
            .put(
                CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_REQUEST_CACHE).getKey(),
                TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME
            )
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_ONHEAP_STORE_NAME.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ).getKey(),
                OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME
            )
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_DISK_STORE_NAME.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ).getKey(),
                MockDiskCache.MockDiskCacheFactory.NAME
            )
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_SEGMENTS.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ).getKey(),
                numberOfSegments
            )
            .put(
                OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                    .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                    .getKey(),
                onHeapCacheSizeInBytesOrPercentage
            )
            .build();
    }

    public void testPluginsAreInstalled() {
        internalCluster().startNode(Settings.builder().put(defaultSettings("1%", getNumberOfSegments())).build());
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());
        NodesInfoResponse nodesInfoResponse = OpenSearchIntegTestCase.client().admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        List<PluginInfo> pluginInfos = nodesInfoResponse.getNodes()
            .stream()
            .flatMap(
                (Function<NodeInfo, Stream<PluginInfo>>) nodeInfo -> nodeInfo.getInfo(PluginsAndModules.class).getPluginInfos().stream()
            )
            .collect(Collectors.toList());
        Assert.assertTrue(
            pluginInfos.stream()
                .anyMatch(pluginInfo -> pluginInfo.getName().equals("org.opensearch.cache.common.tier.TieredSpilloverCachePlugin"))
        );
    }

    public void testSanityChecksWithIndicesRequestCache() throws InterruptedException {
        int numberOfSegments = getNumberOfSegments();
        internalCluster().startNodes(3, Settings.builder().put(defaultSettings("1%", numberOfSegments)).build());
        Client client = client();
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate("index")
                .setMapping("f", "type=date")
                .setSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true).build())
                .get()
        );
        indexRandom(
            true,
            client.prepareIndex("index").setSource("f", "2014-03-10T00:00:00.000Z"),
            client.prepareIndex("index").setSource("f", "2014-05-13T00:00:00.000Z")
        );
        ensureSearchable("index");

        // This is not a random example: serialization with time zones writes shared strings
        // which used to not work well with the query cache because of the handles stream output
        // see #9500
        final SearchResponse r1 = client.prepareSearch("index")
            .setSize(0)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .addAggregation(
                dateHistogram("histo").field("f").timeZone(ZoneId.of("+01:00")).minDocCount(0).calendarInterval(DateHistogramInterval.MONTH)
            )
            .get();
        assertSearchResponse(r1);

        // The cached is actually used
        assertThat(
            client.admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(),
            greaterThan(0L)
        );
    }

    public void testWithDynamicTookTimePolicy() throws Exception {
        int onHeapCacheSizeInBytes = 2000;
        internalCluster().startNode(Settings.builder().put(defaultSettings(onHeapCacheSizeInBytes + "b", 1)).build());
        Client client = client();
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate("index")
                .setMapping("k", "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.refresh_interval", -1)
                )
                .get()
        );
        // Step 1 : Set a very high value for took time policy so that no items evicted from onHeap cache are spilled
        // to disk. And then hit requests so that few items are cached into cache.
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest().transientSettings(
            Settings.builder()
                .put(
                    TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(),
                    new TimeValue(100, TimeUnit.SECONDS)
                )
                .build()
        );
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).get());
        int numberOfIndexedItems = randomIntBetween(6, 10);
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            indexRandom(true, client.prepareIndex("index").setSource("k" + iterator, "hello" + iterator));
        }
        ensureSearchable("index");
        refreshAndWaitForReplication();
        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        ForceMergeResponse forceMergeResponse = client.admin().indices().prepareForceMerge("index").setFlush(true).get();
        OpenSearchAssertions.assertAllSuccessful(forceMergeResponse);
        long perQuerySizeInCacheInBytes = -1;
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            SearchResponse resp = client.prepareSearch("index")
                .setRequestCache(true)
                .setQuery(QueryBuilders.termQuery("k" + iterator, "hello" + iterator))
                .get();
            if (perQuerySizeInCacheInBytes == -1) {
                RequestCacheStats requestCacheStats = getRequestCacheStats(client, "index");
                perQuerySizeInCacheInBytes = requestCacheStats.getMemorySizeInBytes();
            }
            assertSearchResponse(resp);
        }
        RequestCacheStats requestCacheStats = getRequestCacheStats(client, "index");
        // Considering disk cache won't be used due to took time policy having a high value, we expect overall cache
        // size to be less than or equal to onHeapCache size.
        assertTrue(requestCacheStats.getMemorySizeInBytes() <= onHeapCacheSizeInBytes);
        long entriesInCache = requestCacheStats.getMemorySizeInBytes() / perQuerySizeInCacheInBytes;
        // All should be misses in the first attempt
        assertEquals(numberOfIndexedItems, requestCacheStats.getMissCount());
        assertEquals(numberOfIndexedItems - entriesInCache, requestCacheStats.getEvictions());
        assertEquals(0, requestCacheStats.getHitCount());

        // Step 2: Again hit same set of queries as above, we still won't see any hits as items keeps getting evicted.
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            SearchResponse resp = client.prepareSearch("index")
                .setRequestCache(true)
                .setQuery(QueryBuilders.termQuery("k" + iterator, "hello" + iterator))
                .get();
            assertSearchResponse(resp);
        }
        requestCacheStats = getRequestCacheStats(client, "index");
        // We still won't get any hits as items keep getting evicted in LRU fashion due to limited cache size.
        assertTrue(requestCacheStats.getMemorySizeInBytes() <= onHeapCacheSizeInBytes);
        assertEquals(numberOfIndexedItems * 2, requestCacheStats.getMissCount());
        assertEquals(numberOfIndexedItems * 2 - entriesInCache, requestCacheStats.getEvictions());
        assertEquals(0, requestCacheStats.getHitCount());
        long lastEvictionSeen = requestCacheStats.getEvictions();

        // Step 3: Decrease took time policy to zero so that disk cache also comes into play. Now we should be able
        // to cache all entries.
        updateSettingsRequest = new ClusterUpdateSettingsRequest().transientSettings(
            Settings.builder()
                .put(
                    TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(),
                    new TimeValue(0, TimeUnit.MILLISECONDS)
                )
                .build()
        );
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).get());
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            SearchResponse resp = client.prepareSearch("index")
                .setRequestCache(true)
                .setQuery(QueryBuilders.termQuery("k" + iterator, "hello" + iterator))
                .get();
            assertSearchResponse(resp);
        }
        requestCacheStats = getRequestCacheStats(client, "index");
        // All entries should get cached.
        assertEquals(numberOfIndexedItems * perQuerySizeInCacheInBytes, requestCacheStats.getMemorySizeInBytes());
        // No more evictions seen when compared with last step.
        assertEquals(0, requestCacheStats.getEvictions() - lastEvictionSeen);
        // Hit count should be equal to number of cache entries present in previous step.
        assertEquals(entriesInCache, requestCacheStats.getHitCount());
        assertEquals(numberOfIndexedItems * 3 - entriesInCache, requestCacheStats.getMissCount());
        long lastHitCountSeen = requestCacheStats.getHitCount();
        long lastMissCountSeen = requestCacheStats.getMissCount();

        // Step 4: Again hit the same requests, we should get hits for all entries.
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            SearchResponse resp = client.prepareSearch("index")
                .setRequestCache(true)
                .setQuery(QueryBuilders.termQuery("k" + iterator, "hello" + iterator))
                .get();
            assertSearchResponse(resp);
        }
        requestCacheStats = getRequestCacheStats(client, "index");
        // All entries should get cached.
        assertEquals(numberOfIndexedItems * perQuerySizeInCacheInBytes, requestCacheStats.getMemorySizeInBytes());
        // No more evictions seen when compared with last step.
        assertEquals(0, requestCacheStats.getEvictions() - lastEvictionSeen);
        assertEquals(lastHitCountSeen + numberOfIndexedItems, requestCacheStats.getHitCount());
        assertEquals(0, lastMissCountSeen - requestCacheStats.getMissCount());
    }

    public void testInvalidationWithIndicesRequestCache() throws Exception {
        int onHeapCacheSizeInBytes = 2000;
        int numberOfSegments = getNumberOfSegments();
        internalCluster().startNode(
            Settings.builder()
                .put(defaultSettings(onHeapCacheSizeInBytes + "b", numberOfSegments))
                .put(INDICES_CACHE_CLEAN_INTERVAL_SETTING.getKey(), new TimeValue(1))
                .build()
        );
        Client client = client();
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate("index")
                .setMapping("k", "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.refresh_interval", -1)
                )
                .get()
        );
        // Update took time policy to zero so that all entries are eligible to be cached on disk.
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest().transientSettings(
            Settings.builder()
                .put(
                    TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(),
                    new TimeValue(0, TimeUnit.MILLISECONDS)
                )
                .build()
        );
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).get());
        int numberOfIndexedItems = randomIntBetween(5, 10);
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            indexRandom(true, client.prepareIndex("index").setSource("k" + iterator, "hello" + iterator));
        }
        ensureSearchable("index");
        refreshAndWaitForReplication();
        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        ForceMergeResponse forceMergeResponse = client.admin().indices().prepareForceMerge("index").setFlush(true).get();
        OpenSearchAssertions.assertAllSuccessful(forceMergeResponse);
        long perQuerySizeInCacheInBytes = -1;
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            SearchResponse resp = client.prepareSearch("index")
                .setRequestCache(true)
                .setQuery(QueryBuilders.termQuery("k" + iterator, "hello" + iterator))
                .get();
            if (perQuerySizeInCacheInBytes == -1) {
                RequestCacheStats requestCacheStats = getRequestCacheStats(client, "index");
                perQuerySizeInCacheInBytes = requestCacheStats.getMemorySizeInBytes();
            }
            assertSearchResponse(resp);
        }
        RequestCacheStats requestCacheStats = getRequestCacheStats(client, "index");
        assertEquals(numberOfIndexedItems, requestCacheStats.getMissCount());
        assertEquals(0, requestCacheStats.getHitCount());
        assertEquals(0, requestCacheStats.getEvictions());
        assertEquals(perQuerySizeInCacheInBytes * numberOfIndexedItems, requestCacheStats.getMemorySizeInBytes());
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            SearchResponse resp = client.prepareSearch("index")
                .setRequestCache(true)
                .setQuery(QueryBuilders.termQuery("k" + iterator, "hello" + iterator))
                .get();
            assertSearchResponse(resp);
        }
        requestCacheStats = client.admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache();
        assertEquals(numberOfIndexedItems, requestCacheStats.getHitCount());
        assertEquals(numberOfIndexedItems, requestCacheStats.getMissCount());
        assertEquals(perQuerySizeInCacheInBytes * numberOfIndexedItems, requestCacheStats.getMemorySizeInBytes());
        assertEquals(0, requestCacheStats.getEvictions());
        // Explicit refresh would invalidate cache entries.
        refreshAndWaitForReplication();
        assertBusy(() -> {
            // Explicit refresh should clear up cache entries
            assertTrue(getRequestCacheStats(client, "index").getMemorySizeInBytes() == 0);
        }, 1, TimeUnit.SECONDS);
        requestCacheStats = client.admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache();
        assertEquals(0, requestCacheStats.getMemorySizeInBytes());
        // Hits and misses stats shouldn't get cleared up.
        assertEquals(numberOfIndexedItems, requestCacheStats.getHitCount());
        assertEquals(numberOfIndexedItems, requestCacheStats.getMissCount());
    }

    public void testWithExplicitCacheClear() throws Exception {
        int numberOfSegments = getNumberOfSegments();
        int onHeapCacheSizeInBytes = 2000;
        internalCluster().startNode(
            Settings.builder()
                .put(defaultSettings(onHeapCacheSizeInBytes + "b", numberOfSegments))
                .put(INDICES_CACHE_CLEAN_INTERVAL_SETTING.getKey(), new TimeValue(1))
                .build()
        );
        Client client = client();
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate("index")
                .setMapping("k", "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.refresh_interval", -1)
                )
                .get()
        );
        // Update took time policy to zero so that all entries are eligible to be cached on disk.
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest().transientSettings(
            Settings.builder()
                .put(
                    TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(),
                    new TimeValue(0, TimeUnit.MILLISECONDS)
                )
                .build()
        );
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).get());
        int numberOfIndexedItems = randomIntBetween(5, 10);
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            indexRandom(true, client.prepareIndex("index").setSource("k" + iterator, "hello" + iterator));
        }
        ensureSearchable("index");
        refreshAndWaitForReplication();
        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        ForceMergeResponse forceMergeResponse = client.admin().indices().prepareForceMerge("index").setFlush(true).get();
        OpenSearchAssertions.assertAllSuccessful(forceMergeResponse);

        long perQuerySizeInCacheInBytes = -1;
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            SearchResponse resp = client.prepareSearch("index")
                .setRequestCache(true)
                .setQuery(QueryBuilders.termQuery("k" + iterator, "hello" + iterator))
                .get();
            if (perQuerySizeInCacheInBytes == -1) {
                RequestCacheStats requestCacheStats = getRequestCacheStats(client, "index");
                perQuerySizeInCacheInBytes = requestCacheStats.getMemorySizeInBytes();
            }
            assertSearchResponse(resp);
        }
        RequestCacheStats requestCacheStats = getRequestCacheStats(client, "index");
        assertEquals(numberOfIndexedItems, requestCacheStats.getMissCount());
        assertEquals(0, requestCacheStats.getHitCount());
        assertEquals(0, requestCacheStats.getEvictions());
        assertEquals(perQuerySizeInCacheInBytes * numberOfIndexedItems, requestCacheStats.getMemorySizeInBytes());

        // Explicit clear the cache.
        ClearIndicesCacheRequest request = new ClearIndicesCacheRequest("index");
        ClearIndicesCacheResponse response = client.admin().indices().clearCache(request).get();
        assertNoFailures(response);

        assertBusy(() -> {
            // All entries should get cleared up.
            assertTrue(getRequestCacheStats(client, "index").getMemorySizeInBytes() == 0);
        }, 1, TimeUnit.SECONDS);
    }

    public void testWithDynamicDiskCacheSetting() throws Exception {
        int numberOfSegments = getNumberOfSegments();
        int onHeapCacheSizeInBytes = randomIntBetween(numberOfSegments + 1, numberOfSegments * 2); // Keep it low so
        // that all items are
        // cached onto disk.
        internalCluster().startNode(
            Settings.builder()
                .put(defaultSettings(onHeapCacheSizeInBytes + "b", numberOfSegments))
                .put(INDICES_CACHE_CLEAN_INTERVAL_SETTING.getKey(), new TimeValue(1))
                .build()
        );
        Client client = client();
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate("index")
                .setMapping("k", "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.refresh_interval", -1)
                )
                .get()
        );
        // Update took time policy to zero so that all entries are eligible to be cached on disk.
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest().transientSettings(
            Settings.builder()
                .put(
                    TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(),
                    new TimeValue(0, TimeUnit.MILLISECONDS)
                )
                .build()
        );
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).get());
        int numberOfIndexedItems = randomIntBetween(5, 10);
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            indexRandom(true, client.prepareIndex("index").setSource("k" + iterator, "hello" + iterator));
        }
        ensureSearchable("index");
        refreshAndWaitForReplication();
        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        ForceMergeResponse forceMergeResponse = client.admin().indices().prepareForceMerge("index").setFlush(true).get();
        OpenSearchAssertions.assertAllSuccessful(forceMergeResponse);
        long perQuerySizeInCacheInBytes = -1;
        // Step 1: Hit some queries. We will see misses and queries will be cached(onto disk cache) for subsequent
        // requests.
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            SearchResponse resp = client.prepareSearch("index")
                .setRequestCache(true)
                .setQuery(QueryBuilders.termQuery("k" + iterator, "hello" + iterator))
                .get();
            if (perQuerySizeInCacheInBytes == -1) {
                RequestCacheStats requestCacheStats = getRequestCacheStats(client, "index");
                perQuerySizeInCacheInBytes = requestCacheStats.getMemorySizeInBytes();
            }
            assertSearchResponse(resp);
        }

        RequestCacheStats requestCacheStats = getRequestCacheStats(client, "index");
        assertEquals(numberOfIndexedItems * perQuerySizeInCacheInBytes, requestCacheStats.getMemorySizeInBytes());
        assertEquals(numberOfIndexedItems, requestCacheStats.getMissCount());
        assertEquals(0, requestCacheStats.getHitCount());
        assertEquals(0, requestCacheStats.getEvictions());

        // Step 2: Hit same queries again. We will see hits now.
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            SearchResponse resp = client.prepareSearch("index")
                .setRequestCache(true)
                .setQuery(QueryBuilders.termQuery("k" + iterator, "hello" + iterator))
                .get();
            assertSearchResponse(resp);
        }
        requestCacheStats = getRequestCacheStats(client, "index");
        assertEquals(numberOfIndexedItems * perQuerySizeInCacheInBytes, requestCacheStats.getMemorySizeInBytes());
        assertEquals(numberOfIndexedItems, requestCacheStats.getMissCount());
        assertEquals(numberOfIndexedItems, requestCacheStats.getHitCount());
        assertEquals(0, requestCacheStats.getEvictions());
        long lastKnownHitCount = requestCacheStats.getHitCount();
        long lastKnownMissCount = requestCacheStats.getMissCount();

        // Step 3: Turn off disk cache now. And hit same queries again. We should not see hits now as all queries
        // were cached onto disk cache.
        updateSettingsRequest = new ClusterUpdateSettingsRequest().transientSettings(
            Settings.builder()
                .put(TieredSpilloverCacheSettings.DISK_CACHE_ENABLED_SETTING_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(), false)
                .build()
        );
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).get());

        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            SearchResponse resp = client.prepareSearch("index")
                .setRequestCache(true)
                .setQuery(QueryBuilders.termQuery("k" + iterator, "hello" + iterator))
                .get();
            assertSearchResponse(resp);
        }
        requestCacheStats = getRequestCacheStats(client, "index");
        assertEquals(numberOfIndexedItems * perQuerySizeInCacheInBytes, requestCacheStats.getMemorySizeInBytes()); //
        // Still shows disk cache entries as explicit clear or invalidation is required to clean up disk cache.
        assertEquals(lastKnownMissCount + numberOfIndexedItems, requestCacheStats.getMissCount());
        assertEquals(0, lastKnownHitCount - requestCacheStats.getHitCount()); // No new hits being seen.
        lastKnownMissCount = requestCacheStats.getMissCount();
        lastKnownHitCount = requestCacheStats.getHitCount();

        // Step 4: Invalidate entries via refresh.
        // Explicit refresh would invalidate cache entries.
        refreshAndWaitForReplication();
        assertBusy(() -> {
            // Explicit refresh should clear up cache entries
            assertTrue(getRequestCacheStats(client, "index").getMemorySizeInBytes() == 0);
        }, 1, TimeUnit.SECONDS);
        requestCacheStats = getRequestCacheStats(client, "index");
        assertEquals(0, lastKnownMissCount - requestCacheStats.getMissCount());
        assertEquals(0, lastKnownHitCount - requestCacheStats.getHitCount());
    }

    public void testWithInvalidSegmentNumberSetting() throws Exception {
        int numberOfSegments = getNumberOfSegments();
        int onHeapCacheSizeInBytes = randomIntBetween(numberOfSegments + 1, numberOfSegments * 2); // Keep it low so
        // that all items are
        // cached onto disk.
        assertThrows(
            INVALID_SEGMENT_NUMBER_EXCEPTION_MESSAGE,
            IllegalArgumentException.class,
            () -> internalCluster().startNode(
                Settings.builder()
                    .put(defaultSettings(onHeapCacheSizeInBytes + "b", 300))
                    .put(INDICES_CACHE_CLEAN_INTERVAL_SETTING.getKey(), new TimeValue(1))
                    .build()
            )
        );
    }

    private RequestCacheStats getRequestCacheStats(Client client, String indexName) {
        return client.admin().indices().prepareStats(indexName).setRequestCache(true).get().getTotal().getRequestCache();
    }

    public static int getNumberOfSegments() {
        return randomFrom(1, 2, 4, 8, 16, 64, 128, 256);
    }

    public static class MockDiskCachePlugin extends Plugin implements CachePlugin {

        public MockDiskCachePlugin() {}

        @Override
        public Map<String, ICache.Factory> getCacheFactoryMap() {
            return Map.of(MockDiskCache.MockDiskCacheFactory.NAME, new MockDiskCache.MockDiskCacheFactory(0, 10000, false));
        }

        @Override
        public String getName() {
            return "mock_disk_plugin";
        }
    }
}
