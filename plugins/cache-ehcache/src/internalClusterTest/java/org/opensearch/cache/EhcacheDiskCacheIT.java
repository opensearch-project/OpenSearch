/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.cache.store.disk.EhcacheDiskCache;
import org.opensearch.cache.store.disk.EhcacheThreadLeakFilter;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;
import org.junit.Assert;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cache.EhcacheDiskCacheSettings.DEFAULT_CACHE_SIZE_IN_BYTES;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_CACHE_EXPIRE_AFTER_ACCESS_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_LISTENER_MODE_SYNC_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_MAX_SIZE_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_STORAGE_PATH_KEY;
import static org.opensearch.indices.IndicesService.INDICES_CACHE_CLEAN_INTERVAL_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.greaterThan;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 0, scope = OpenSearchIntegTestCase.Scope.TEST)
@ThreadLeakFilters(filters = { EhcacheThreadLeakFilter.class })
public class EhcacheDiskCacheIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(EhcacheCachePlugin.class);
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.PLUGGABLE_CACHE, "true").build();
    }

    private Settings defaultSettings(long sizeInBytes, TimeValue expirationTime) {
        if (expirationTime == null) {
            expirationTime = TimeValue.MAX_VALUE;
        }
        try (NodeEnvironment env = newNodeEnvironment(Settings.EMPTY)) {
            return Settings.builder()
                .put(
                    EhcacheDiskCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(DISK_STORAGE_PATH_KEY)
                        .getKey(),
                    env.nodePaths()[0].indicesPath.toString() + "/" + UUID.randomUUID() + "/request_cache/"
                )
                .put(
                    CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_REQUEST_CACHE).getKey(),
                    EhcacheDiskCache.EhcacheDiskCacheFactory.EHCACHE_DISK_CACHE_NAME
                )
                .put(
                    EhcacheDiskCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(DISK_LISTENER_MODE_SYNC_KEY)
                        .getKey(),
                    true
                )
                .put(
                    EhcacheDiskCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE).get(DISK_MAX_SIZE_KEY).getKey(),
                    new ByteSizeValue(sizeInBytes)
                )
                .put(
                    EhcacheDiskCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(DISK_CACHE_EXPIRE_AFTER_ACCESS_KEY)
                        .getKey(),
                    expirationTime
                )
                .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void testPluginsAreInstalled() {
        internalCluster().startNode(Settings.builder().put(defaultSettings(DEFAULT_CACHE_SIZE_IN_BYTES, null)).build());
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
            pluginInfos.stream().anyMatch(pluginInfo -> pluginInfo.getName().equals("org.opensearch.cache.EhcacheCachePlugin"))
        );
    }

    public void testSanityChecksWithIndicesRequestCache() throws InterruptedException {
        internalCluster().startNode(Settings.builder().put(defaultSettings(DEFAULT_CACHE_SIZE_IN_BYTES, null)).build());
        Client client = client();
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate("index")
                .setMapping("f", "type=date")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .build()
                )
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
                dateHistogram("histo").field("f")
                    .timeZone(ZoneId.of("+01:00"))
                    .minDocCount(0)
                    .dateHistogramInterval(DateHistogramInterval.MONTH)
            )
            .get();
        assertSearchResponse(r1);

        // The cached is actually used
        assertThat(
            client.admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(),
            greaterThan(0L)
        );
    }

    public void testInvalidationWithIndicesRequestCache() throws Exception {
        internalCluster().startNode(
            Settings.builder()
                .put(defaultSettings(DEFAULT_CACHE_SIZE_IN_BYTES, null))
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
        requestCacheStats = getRequestCacheStats(client, "index");
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
        requestCacheStats = getRequestCacheStats(client, "index");
        assertEquals(0, requestCacheStats.getMemorySizeInBytes());
        // Hits and misses stats shouldn't get cleared up.
        assertEquals(numberOfIndexedItems, requestCacheStats.getHitCount());
        assertEquals(numberOfIndexedItems, requestCacheStats.getMissCount());
    }

    public void testExplicitCacheClearWithIndicesRequestCache() throws Exception {
        internalCluster().startNode(
            Settings.builder()
                .put(defaultSettings(DEFAULT_CACHE_SIZE_IN_BYTES, null))
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

    public void testEvictionsFlowWithExpirationTime() throws Exception {
        internalCluster().startNode(
            Settings.builder()
                .put(defaultSettings(DEFAULT_CACHE_SIZE_IN_BYTES, new TimeValue(0))) // Immediately evict items after
                // access
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
        int numberOfIndexedItems = 2;// randomIntBetween(5, 10);
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
        assertEquals(0, requestCacheStats.getHitCount());
        assertEquals(numberOfIndexedItems, requestCacheStats.getMissCount());
        assertEquals(perQuerySizeInCacheInBytes * numberOfIndexedItems, requestCacheStats.getMemorySizeInBytes());
        assertEquals(0, requestCacheStats.getEvictions());

        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            SearchResponse resp = client.prepareSearch("index")
                .setRequestCache(true)
                .setQuery(QueryBuilders.termQuery("k" + iterator, "hello" + iterator))
                .get();
            assertSearchResponse(resp);
        }
        requestCacheStats = getRequestCacheStats(client, "index");
        // Now that we have access the entries, they should expire after 1ms. So lets wait and verify that cache gets
        // cleared up.
        assertBusy(() -> {
            // Explicit refresh should clear up cache entries
            assertTrue(getRequestCacheStats(client, "index").getMemorySizeInBytes() == 0);
        }, 10, TimeUnit.MILLISECONDS);
        // Validate hit and miss count.
        assertEquals(numberOfIndexedItems, requestCacheStats.getHitCount());
        assertEquals(numberOfIndexedItems, requestCacheStats.getMissCount());
    }

    private RequestCacheStats getRequestCacheStats(Client client, String indexName) {
        return client.admin().indices().prepareStats(indexName).setRequestCache(true).get().getTotal().getRequestCache();
    }
}
