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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.cache.store.disk.EhcacheDiskCache;
import org.opensearch.cache.store.disk.EhcacheThreadLeakFilter;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_LISTENER_MODE_SYNC_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_STORAGE_PATH_KEY;
import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.greaterThan;

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

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        try (NodeEnvironment env = newNodeEnvironment(Settings.EMPTY)) {
            return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(
                    EhcacheDiskCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(DISK_STORAGE_PATH_KEY)
                        .getKey(),
                    env.nodePaths()[0].indicesPath.toString() + "/request_cache"
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
                .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void testPluginsAreInstalled() {
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

    public void testInvalidationAndCleanupLogicWithIndicesRequestCache() throws Exception {
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
                )
                .get()
        );
        int numberOfIndexedItems = randomIntBetween(2, 10);
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            indexRandom(true, client.prepareIndex("index").setSource("k" + iterator, "hello" + iterator));
        }
        ensureSearchable("index");
        for (int iterator = 0; iterator < numberOfIndexedItems; iterator++) {
            SearchResponse resp = client.prepareSearch("index")
                .setRequestCache(true)
                .setQuery(QueryBuilders.termQuery("k" + iterator, "hello" + iterator))
                .get();
            assertSearchResponse(resp);
        }
        RequestCacheStats requestCacheStats = client.admin()
            .indices()
            .prepareStats("index")
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();
        assertEquals(numberOfIndexedItems, requestCacheStats.getMissCount());
        assertEquals(0, requestCacheStats.getHitCount());
        assertEquals(0, requestCacheStats.getEvictions());
        assertTrue(requestCacheStats.getMemorySizeInBytes() > 0);
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
        // Explicit refresh would invalidate cache entries.
        refreshAndWaitForReplication();
        ClearIndicesCacheRequest request = new ClearIndicesCacheRequest("index");
        ClearIndicesCacheResponse response = client.admin().indices().clearCache(request).get();
        requestCacheStats = client.admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache();
        assertEquals(0, requestCacheStats.getMemorySizeInBytes());
        assertEquals(numberOfIndexedItems, requestCacheStats.getHitCount());
        assertEquals(numberOfIndexedItems, requestCacheStats.getMissCount());
    }
}
