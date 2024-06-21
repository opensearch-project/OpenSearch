/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergePolicyProvider;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.opensearch.indices.IndicesRequestCache.INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING;
import static org.opensearch.indices.IndicesService.INDICES_CACHE_CLEANUP_INTERVAL_SETTING_KEY;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class IndicesRequestCacheCleanupIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class);
    }

    public void testCacheWithInvalidation() throws Exception {
        Client client = client();
        String index = "index";
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setMapping("k", "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.refresh_interval", -1)
                        // Disable index refreshing to avoid cache being invalidated mid-test
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
                )
                .get()
        );
        indexRandom(false, client.prepareIndex(index).setSource("k", "hello"));
        ensureSearchable(index);
        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        forceMerge(client, index);
        SearchResponse resp = client.prepareSearch(index).setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello")).get();
        assertSearchResponse(resp);
        OpenSearchAssertions.assertAllSuccessful(resp);
        assertThat(resp.getHits().getTotalHits().value, equalTo(1L));

        assertCacheState(client, index, 0, 1);
        // Index but don't refresh
        indexRandom(false, client.prepareIndex(index).setSource("k", "hello2"));
        resp = client.prepareSearch(index).setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello")).get();
        assertSearchResponse(resp);
        // Should expect hit as here as refresh didn't happen
        assertCacheState(client, index, 1, 1);

        // assert segment counts stay the same
        assertEquals(1, getSegmentCount(client, index));
        // Explicit refresh would invalidate cache
        refreshAndWaitForReplication();
        // Hit same query again
        resp = client.prepareSearch(index).setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello")).get();
        assertSearchResponse(resp);
        // Should expect miss as key has changed due to change in IndexReader.CacheKey (due to refresh)
        assertCacheState(client, index, 1, 2);
    }

    // calling cache clear api, when staleness threshold is lower than staleness, it should clean the stale keys from cache
    public void testCacheClearAPIRemovesStaleKeysWhenStalenessThresholdIsLow() throws Exception {
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_STALENESS_THRESHOLD_SETTING_KEY, 0.10)
                .put(
                    IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_INTERVAL_SETTING_KEY,
                    // setting intentionally high to avoid cache cleaner interfering
                    TimeValue.timeValueMillis(300)
                )
        );
        Client client = client(node);
        String index1 = "index1";
        String index2 = "index2";
        setupIndex(client, index1);
        setupIndex(client, index2);

        // create first cache entry in index1
        createCacheEntry(client, index1, "hello");
        assertCacheState(client, index1, 0, 1);
        long memorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(memorySizeForIndex1 > 0);

        // create second cache entry in index1
        createCacheEntry(client, index1, "there");
        assertCacheState(client, index1, 0, 2);
        long finalMemorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(finalMemorySizeForIndex1 > memorySizeForIndex1);

        // create first cache entry in index2
        createCacheEntry(client, index2, "hello");
        assertCacheState(client, index2, 0, 1);
        assertTrue(getRequestCacheStats(client, index2).getMemorySizeInBytes() > 0);

        ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest(index2);
        client.admin().indices().clearCache(clearIndicesCacheRequest).actionGet();

        // assert segment counts stay the same
        assertEquals(1, getSegmentCount(client, index1));
        assertEquals(1, getSegmentCount(client, index2));
        // cache cleaner should have cleaned up the stale key from index 2
        assertEquals(0, getRequestCacheStats(client, index2).getMemorySizeInBytes());
        // cache cleaner should NOT have cleaned from index 1
        assertEquals(finalMemorySizeForIndex1, getRequestCacheStats(client, index1).getMemorySizeInBytes());
    }

    // when staleness threshold is lower than staleness, it should clean the stale keys from cache
    public void testStaleKeysCleanupWithLowThreshold() throws Exception {
        int cacheCleanIntervalInMillis = 1;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_STALENESS_THRESHOLD_SETTING_KEY, 0.10)
                .put(
                    IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_INTERVAL_SETTING_KEY,
                    TimeValue.timeValueMillis(cacheCleanIntervalInMillis)
                )
        );
        Client client = client(node);
        String index1 = "index1";
        String index2 = "index2";
        setupIndex(client, index1);
        setupIndex(client, index2);

        // create first cache entry in index1
        createCacheEntry(client, index1, "hello");
        assertCacheState(client, index1, 0, 1);
        long memorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(memorySizeForIndex1 > 0);

        // create second cache entry in index1
        createCacheEntry(client, index1, "there");
        assertCacheState(client, index1, 0, 2);
        long finalMemorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(finalMemorySizeForIndex1 > memorySizeForIndex1);

        // create first cache entry in index2
        createCacheEntry(client, index2, "hello");
        assertCacheState(client, index2, 0, 1);
        assertTrue(getRequestCacheStats(client, index2).getMemorySizeInBytes() > 0);

        // create 1 stale key
        indexRandom(false, client.prepareIndex(index2).setId("1").setSource("d", "hello"));
        forceMerge(client, index2);
        // sleep until cache cleaner would have cleaned up the stale key from index 2
        assertBusy(() -> {
            // assert segment counts stay the same
            assertEquals(1, getSegmentCount(client, index1));
            assertEquals(2, getSegmentCount(client, index2));
            // cache cleaner should have cleaned up the stale key from index 2
            assertEquals(0, getRequestCacheStats(client, index2).getMemorySizeInBytes());
            // cache cleaner should NOT have cleaned from index 1
            assertEquals(finalMemorySizeForIndex1, getRequestCacheStats(client, index1).getMemorySizeInBytes());
        }, cacheCleanIntervalInMillis * 2, TimeUnit.MILLISECONDS);
        // sleep until cache cleaner would have cleaned up the stale key from index 2
    }

    // when staleness threshold is equal to staleness, it should clean the stale keys from cache
    public void testCacheCleanupOnEqualStalenessAndThreshold() throws Exception {
        int cacheCleanIntervalInMillis = 1;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_STALENESS_THRESHOLD_SETTING_KEY, 0.33)
                .put(
                    IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_INTERVAL_SETTING_KEY,
                    TimeValue.timeValueMillis(cacheCleanIntervalInMillis)
                )
        );
        Client client = client(node);
        String index1 = "index1";
        String index2 = "index2";
        setupIndex(client, index1);
        setupIndex(client, index2);

        // create first cache entry in index1
        createCacheEntry(client, index1, "hello");
        assertCacheState(client, index1, 0, 1);
        long memorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(memorySizeForIndex1 > 0);

        // create second cache entry in index1
        createCacheEntry(client, index1, "there");
        assertCacheState(client, index1, 0, 2);
        long finalMemorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(finalMemorySizeForIndex1 > memorySizeForIndex1);

        // create first cache entry in index2
        createCacheEntry(client, index2, "hello");
        assertCacheState(client, index2, 0, 1);
        assertTrue(getRequestCacheStats(client, index2).getMemorySizeInBytes() > 0);

        // create 1 stale key
        indexRandom(false, client.prepareIndex(index2).setId("1").setSource("d", "hello"));
        forceMerge(client, index2);
        // sleep until cache cleaner would have cleaned up the stale key from index 2
        assertBusy(() -> {
            // assert segment counts stay the same
            assertEquals(1, getSegmentCount(client, index1));
            assertEquals(2, getSegmentCount(client, index2));
            // cache cleaner should have cleaned up the stale key from index 2
            assertEquals(0, getRequestCacheStats(client, index2).getMemorySizeInBytes());
            // cache cleaner should NOT have cleaned from index 1
            assertEquals(finalMemorySizeForIndex1, getRequestCacheStats(client, index1).getMemorySizeInBytes());
        }, cacheCleanIntervalInMillis * 2, TimeUnit.MILLISECONDS);
    }

    // when staleness threshold is higher than staleness, it should NOT clean the cache
    public void testCacheCleanupSkipsWithHighStalenessThreshold() throws Exception {
        int cacheCleanIntervalInMillis = 1;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_STALENESS_THRESHOLD_SETTING_KEY, 0.90)
                .put(
                    IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_INTERVAL_SETTING_KEY,
                    TimeValue.timeValueMillis(cacheCleanIntervalInMillis)
                )
        );
        Client client = client(node);
        String index1 = "index1";
        String index2 = "index2";
        setupIndex(client, index1);
        setupIndex(client, index2);

        // create first cache entry in index1
        createCacheEntry(client, index1, "hello");
        assertCacheState(client, index1, 0, 1);
        long memorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(memorySizeForIndex1 > 0);

        // create second cache entry in index1
        createCacheEntry(client, index1, "there");
        assertCacheState(client, index1, 0, 2);
        long finalMemorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(finalMemorySizeForIndex1 > memorySizeForIndex1);

        // create first cache entry in index2
        createCacheEntry(client, index2, "hello");
        assertCacheState(client, index2, 0, 1);
        assertTrue(getRequestCacheStats(client, index2).getMemorySizeInBytes() > 0);

        // force refresh so that it creates 1 stale key
        flushAndRefresh(index2);
        // sleep until cache cleaner would have cleaned up the stale key from index 2
        assertBusy(() -> {
            // assert segment counts stay the same
            assertEquals(1, getSegmentCount(client, index1));
            assertEquals(1, getSegmentCount(client, index2));
            // cache cleaner should NOT have cleaned up the stale key from index 2
            assertTrue(getRequestCacheStats(client, index2).getMemorySizeInBytes() > 0);
            // cache cleaner should NOT have cleaned from index 1
            assertEquals(finalMemorySizeForIndex1, getRequestCacheStats(client, index1).getMemorySizeInBytes());
        }, cacheCleanIntervalInMillis * 2, TimeUnit.MILLISECONDS);
    }

    // when staleness threshold is explicitly set to 0, cache cleaner regularly cleans up stale keys.
    public void testCacheCleanupOnZeroStalenessThreshold() throws Exception {
        int cacheCleanIntervalInMillis = 50;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_STALENESS_THRESHOLD_SETTING_KEY, 0)
                .put(
                    IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_INTERVAL_SETTING_KEY,
                    TimeValue.timeValueMillis(cacheCleanIntervalInMillis)
                )
        );
        Client client = client(node);
        String index1 = "index1";
        String index2 = "index2";
        setupIndex(client, index1);
        setupIndex(client, index2);

        // create 10 index1 cache entries
        for (int i = 1; i <= 10; i++) {
            long cacheSizeBefore = getRequestCacheStats(client, index1).getMemorySizeInBytes();
            createCacheEntry(client, index1, "hello" + i);
            assertCacheState(client, index1, 0, i);
            long cacheSizeAfter = getRequestCacheStats(client, index1).getMemorySizeInBytes();
            assertTrue(cacheSizeAfter > cacheSizeBefore);
        }

        long finalMemorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();

        // create first cache entry in index2
        createCacheEntry(client, index2, "hello");
        assertCacheState(client, index2, 0, 1);
        assertTrue(getRequestCacheStats(client, index2).getMemorySizeInBytes() > 0);

        // create 1 stale key
        indexRandom(false, client.prepareIndex(index2).setId("1").setSource("d", "hello"));
        forceMerge(client, index2);
        // sleep until cache cleaner would have cleaned up the stale key from index 2
        assertBusy(() -> {
            // assert segment counts stay the same
            assertEquals(1, getSegmentCount(client, index1));
            assertEquals(2, getSegmentCount(client, index2));
            // cache cleaner should have cleaned up the stale key from index 2
            assertEquals(0, getRequestCacheStats(client, index2).getMemorySizeInBytes());
            // cache cleaner should NOT have cleaned from index 1
            assertEquals(finalMemorySizeForIndex1, getRequestCacheStats(client, index1).getMemorySizeInBytes());
        }, cacheCleanIntervalInMillis * 2, TimeUnit.MILLISECONDS);
    }

    // when staleness threshold is not explicitly set, cache cleaner regularly cleans up stale keys
    public void testStaleKeysRemovalWithoutExplicitThreshold() throws Exception {
        int cacheCleanIntervalInMillis = 1;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(
                    IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_INTERVAL_SETTING_KEY,
                    TimeValue.timeValueMillis(cacheCleanIntervalInMillis)
                )
        );
        String index1 = "index1";
        String index2 = "index2";
        Client client = client(node);
        setupIndex(client, index1);
        setupIndex(client, index2);

        // create first cache entry in index1
        createCacheEntry(client, index1, "hello");
        assertCacheState(client, index1, 0, 1);
        long memorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(memorySizeForIndex1 > 0);

        // create second cache entry in index1
        createCacheEntry(client, index1, "there");
        assertCacheState(client, index1, 0, 2);
        long finalMemorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(finalMemorySizeForIndex1 > memorySizeForIndex1);

        // create first cache entry in index2
        createCacheEntry(client, index2, "hello");
        assertCacheState(client, index2, 0, 1);
        assertTrue(getRequestCacheStats(client, index2).getMemorySizeInBytes() > 0);

        // force refresh so that it creates 1 stale key
        indexRandom(false, client.prepareIndex(index2).setId("1").setSource("d", "hello"));
        forceMerge(client, index2);
        // sleep until cache cleaner would have cleaned up the stale key from index 2
        assertBusy(() -> {
            assertEquals(1, getSegmentCount(client, index1));
            assertEquals(2, getSegmentCount(client, index2));
            // cache cleaner should have cleaned up the stale key from index 2
            assertEquals(0, getRequestCacheStats(client, index2).getMemorySizeInBytes());
            // cache cleaner should NOT have cleaned from index 1
            assertEquals(finalMemorySizeForIndex1, getRequestCacheStats(client, index1).getMemorySizeInBytes());
        }, cacheCleanIntervalInMillis * 2, TimeUnit.MILLISECONDS);
    }

    // when cache cleaner interval setting is not set, cache cleaner is configured appropriately with the fall-back setting
    public void testCacheCleanupWithDefaultSettings() throws Exception {
        int cacheCleanIntervalInMillis = 1;
        String node = internalCluster().startNode(
            Settings.builder().put(INDICES_CACHE_CLEANUP_INTERVAL_SETTING_KEY, TimeValue.timeValueMillis(cacheCleanIntervalInMillis))
        );
        Client client = client(node);
        String index1 = "index1";
        String index2 = "index2";
        setupIndex(client, index1);
        setupIndex(client, index2);

        // create first cache entry in index1
        createCacheEntry(client, index1, "hello");
        assertCacheState(client, index1, 0, 1);
        long memorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(memorySizeForIndex1 > 0);

        // create second cache entry in index1
        createCacheEntry(client, index1, "there");
        assertCacheState(client, index1, 0, 2);
        long finalMemorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(finalMemorySizeForIndex1 > memorySizeForIndex1);

        // create first cache entry in index2
        createCacheEntry(client, index2, "hello");
        assertCacheState(client, index2, 0, 1);
        assertTrue(getRequestCacheStats(client, index2).getMemorySizeInBytes() > 0);

        // create 1 stale key
        indexRandom(false, client.prepareIndex(index2).setId("1").setSource("d", "hello"));
        forceMerge(client, index2);
        // sleep until cache cleaner would have cleaned up the stale key from index 2
        assertBusy(() -> {
            // assert segment counts stay the same
            assertEquals(1, getSegmentCount(client, index1));
            assertEquals(2, getSegmentCount(client, index2));
            // cache cleaner should have cleaned up the stale key from index 2
            assertEquals(0, getRequestCacheStats(client, index2).getMemorySizeInBytes());
            // cache cleaner should NOT have cleaned from index 1
            assertEquals(finalMemorySizeForIndex1, getRequestCacheStats(client, index1).getMemorySizeInBytes());
        }, cacheCleanIntervalInMillis * 2, TimeUnit.MILLISECONDS);
    }

    // staleness threshold updates flows through to the cache cleaner
    public void testDynamicStalenessThresholdUpdate() throws Exception {
        int cacheCleanIntervalInMillis = 1;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_STALENESS_THRESHOLD_SETTING_KEY, 0.90)
                .put(
                    IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_INTERVAL_SETTING_KEY,
                    TimeValue.timeValueMillis(cacheCleanIntervalInMillis)
                )
        );
        Client client = client(node);
        String index1 = "index1";
        String index2 = "index2";
        setupIndex(client, index1);
        setupIndex(client, index2);

        // create first cache entry in index1
        createCacheEntry(client, index1, "hello");
        assertCacheState(client, index1, 0, 1);
        long memorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(memorySizeForIndex1 > 0);

        // create second cache entry in index1
        createCacheEntry(client, index1, "there");
        assertCacheState(client, index1, 0, 2);
        assertTrue(getRequestCacheStats(client, index1).getMemorySizeInBytes() > memorySizeForIndex1);

        // create first cache entry in index2
        createCacheEntry(client, index2, "hello");
        assertCacheState(client, index2, 0, 1);
        long finalMemorySizeForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(finalMemorySizeForIndex1 > 0);

        // create 1 stale key
        indexRandom(false, client.prepareIndex(index2).setId("1").setSource("d", "hello"));
        forceMerge(client, index2);
        assertBusy(() -> {
            // cache cleaner should NOT have cleaned up the stale key from index 2
            assertTrue(getRequestCacheStats(client, index2).getMemorySizeInBytes() > 0);
        }, cacheCleanIntervalInMillis * 2, TimeUnit.MILLISECONDS);

        // Update indices.requests.cache.cleanup.staleness_threshold to "10%"
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), 0.10));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        assertBusy(() -> {
            // assert segment counts stay the same
            assertEquals(1, getSegmentCount(client, index1));
            assertEquals(2, getSegmentCount(client, index2));
            // cache cleaner should have cleaned up the stale key from index 2
            assertEquals(0, getRequestCacheStats(client, index2).getMemorySizeInBytes());
            // cache cleaner should NOT have cleaned from index 1
            assertEquals(finalMemorySizeForIndex1, getRequestCacheStats(client, index1).getMemorySizeInBytes());
        }, cacheCleanIntervalInMillis * 2, TimeUnit.MILLISECONDS);
    }

    // staleness threshold dynamic updates should throw exceptions on invalid input
    public void testInvalidStalenessThresholdUpdateThrowsException() throws Exception {
        // Update indices.requests.cache.cleanup.staleness_threshold to "10%" with illegal argument
        assertThrows("Ratio should be in [0-1.0]", IllegalArgumentException.class, () -> {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(
                Settings.builder().put(IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_STALENESS_THRESHOLD_SETTING_KEY, 10)
            );
            client().admin().cluster().updateSettings(updateSettingsRequest).actionGet();
        });
    }

    // closing the Index after caching will clean up from Indices Request Cache
    public void testCacheClearanceAfterIndexClosure() throws Exception {
        int cacheCleanIntervalInMillis = 100;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_STALENESS_THRESHOLD_SETTING_KEY, 0.10)
                .put(
                    IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_INTERVAL_SETTING_KEY,
                    TimeValue.timeValueMillis(cacheCleanIntervalInMillis)
                )
        );
        Client client = client(node);
        String index = "index";
        setupIndex(client, index);

        // assert there are no entries in the cache for index
        assertEquals(0, getRequestCacheStats(client, index).getMemorySizeInBytes());
        // assert there are no entries in the cache from other indices in the node
        assertEquals(0, getNodeCacheStats(client).getMemorySizeInBytes());
        // create first cache entry in index
        createCacheEntry(client, index, "hello");
        assertCacheState(client, index, 0, 1);
        assertTrue(getRequestCacheStats(client, index).getMemorySizeInBytes() > 0);
        assertTrue(getNodeCacheStats(client).getMemorySizeInBytes() > 0);

        // close index
        assertAcked(client.admin().indices().prepareClose(index));
        // request cache stats cannot be access since Index should be closed
        try {
            getRequestCacheStats(client, index);
        } catch (Exception e) {
            assert (e instanceof IndexClosedException);
        }
        // sleep until cache cleaner would have cleaned up the stale key from index
        assertBusy(() -> {
            // cache cleaner should have cleaned up the stale keys from index
            assertEquals(0, getNodeCacheStats(client).getMemorySizeInBytes());
        }, cacheCleanIntervalInMillis * 2, TimeUnit.MILLISECONDS);
    }

    // deleting the Index after caching will clean up from Indices Request Cache
    public void testCacheCleanupAfterIndexDeletion() throws Exception {
        int cacheCleanIntervalInMillis = 100;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_STALENESS_THRESHOLD_SETTING_KEY, 0.10)
                .put(
                    IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_INTERVAL_SETTING_KEY,
                    TimeValue.timeValueMillis(cacheCleanIntervalInMillis)
                )
        );
        Client client = client(node);
        String index = "index";
        setupIndex(client, index);

        // assert there are no entries in the cache for index
        assertEquals(0, getRequestCacheStats(client, index).getMemorySizeInBytes());
        // assert there are no entries in the cache from other indices in the node
        assertEquals(0, getNodeCacheStats(client).getMemorySizeInBytes());
        // create first cache entry in index
        createCacheEntry(client, index, "hello");
        assertCacheState(client, index, 0, 1);
        assertTrue(getRequestCacheStats(client, index).getMemorySizeInBytes() > 0);
        assertTrue(getNodeCacheStats(client).getMemorySizeInBytes() > 0);

        // delete index
        assertAcked(client.admin().indices().prepareDelete(index));
        // request cache stats cannot be access since Index should be deleted
        try {
            getRequestCacheStats(client, index);
        } catch (Exception e) {
            assert (e instanceof IndexNotFoundException);
        }

        // sleep until cache cleaner would have cleaned up the stale key from index
        assertBusy(() -> {
            // cache cleaner should have cleaned up the stale keys from index
            assertEquals(0, getNodeCacheStats(client).getMemorySizeInBytes());
        }, cacheCleanIntervalInMillis * 2, TimeUnit.MILLISECONDS);
    }

    // when staleness threshold is lower than staleness, it should clean the cache from all indices having stale keys
    public void testStaleKeysCleanupWithMultipleIndices() throws Exception {
        int cacheCleanIntervalInMillis = 10;
        String node = internalCluster().startNode(
            Settings.builder()
                .put(IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_STALENESS_THRESHOLD_SETTING_KEY, 0.10)
                .put(
                    IndicesRequestCache.INDICES_REQUEST_CACHE_CLEANUP_INTERVAL_SETTING_KEY,
                    TimeValue.timeValueMillis(cacheCleanIntervalInMillis)
                )
        );
        Client client = client(node);
        String index1 = "index1";
        String index2 = "index2";
        setupIndex(client, index1);
        setupIndex(client, index2);

        // assert cache is empty for index1
        assertEquals(0, getRequestCacheStats(client, index1).getMemorySizeInBytes());
        // create first cache entry in index1
        createCacheEntry(client, index1, "hello");
        assertCacheState(client, index1, 0, 1);
        long memorySizeForIndex1With1Entries = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(memorySizeForIndex1With1Entries > 0);

        // create second cache entry in index1
        createCacheEntry(client, index1, "there");
        assertCacheState(client, index1, 0, 2);
        long memorySizeForIndex1With2Entries = getRequestCacheStats(client, index1).getMemorySizeInBytes();
        assertTrue(memorySizeForIndex1With2Entries > memorySizeForIndex1With1Entries);

        // assert cache is empty for index2
        assertEquals(0, getRequestCacheStats(client, index2).getMemorySizeInBytes());
        // create first cache entry in index2
        createCacheEntry(client, index2, "hello");
        assertCacheState(client, index2, 0, 1);
        assertTrue(getRequestCacheStats(client, index2).getMemorySizeInBytes() > 0);

        // invalidate the cache for index1
        indexRandom(false, client.prepareIndex(index1).setId("1").setSource("d", "hello"));
        forceMerge(client, index1);
        // Assert cache is cleared up
        assertBusy(
            () -> { assertEquals(0, getRequestCacheStats(client, index1).getMemorySizeInBytes()); },
            cacheCleanIntervalInMillis * 2,
            TimeUnit.MILLISECONDS
        );

        // invalidate the cache for index2
        indexRandom(false, client.prepareIndex(index2).setId("1").setSource("d", "hello"));
        forceMerge(client, index2);

        // create another cache entry in index 1 same as memorySizeForIndex1With1Entries, this should not be cleaned up.
        createCacheEntry(client, index1, "hello");

        // sleep until cache cleaner would have cleaned up the stale key from index2
        assertBusy(() -> {
            // assert segment counts stay the same
            assertEquals(2, getSegmentCount(client, index1));
            assertEquals(2, getSegmentCount(client, index2));
            // cache cleaner should have cleaned up the stale key from index2 and hence cache should be empty
            assertEquals(0, getRequestCacheStats(client, index2).getMemorySizeInBytes());
            // cache cleaner should have only cleaned up the stale entities for index1
            long currentMemorySizeInBytesForIndex1 = getRequestCacheStats(client, index1).getMemorySizeInBytes();
            // assert the memory size of index1 to only contain 1 entry added after flushAndRefresh
            assertEquals(memorySizeForIndex1With1Entries, currentMemorySizeInBytesForIndex1);
        }, cacheCleanIntervalInMillis * 2, TimeUnit.MILLISECONDS);
    }

    private void setupIndex(Client client, String index) throws Exception {
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setMapping("k", "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        // Disable index refreshing to avoid cache being invalidated mid-test
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
                        // Disable background segment merges invalidating the cache
                        .put(MergePolicyProvider.INDEX_MERGE_ENABLED, false)
                )
                .get()
        );
        indexRandom(false, client.prepareIndex(index).setSource("k", "hello"));
        indexRandom(false, client.prepareIndex(index).setSource("k", "there"));
        ensureSearchable(index);
        forceMerge(client, index);
    }

    private int getSegmentCount(Client client, String indexName) {
        return client.admin()
            .indices()
            .segments(new IndicesSegmentsRequest(indexName))
            .actionGet()
            .getIndices()
            .get(indexName)
            .getShards()
            .get(0)
            .getShards()[0].getSegments()
            .size();
    }

    private void forceMerge(Client client, String index) {
        ForceMergeResponse forceMergeResponse = client.admin().indices().prepareForceMerge(index).setFlush(true).get();
        OpenSearchAssertions.assertAllSuccessful(forceMergeResponse);
        refreshAndWaitForReplication();
    }

    private void createCacheEntry(Client client, String index, String value) {
        SearchResponse resp = client.prepareSearch(index).setRequestCache(true).setQuery(QueryBuilders.termQuery("k", value)).get();
        assertSearchResponse(resp);
        OpenSearchAssertions.assertAllSuccessful(resp);
    }

    private static void assertCacheState(Client client, String index, long expectedHits, long expectedMisses) {
        RequestCacheStats requestCacheStats = getRequestCacheStats(client, index);
        // Check the hit count and miss count together so if they are not
        // correct we can see both values
        assertEquals(
            Arrays.asList(expectedHits, expectedMisses, 0L),
            Arrays.asList(requestCacheStats.getHitCount(), requestCacheStats.getMissCount(), requestCacheStats.getEvictions())
        );

    }

    private static RequestCacheStats getRequestCacheStats(Client client, String index) {
        return client.admin().indices().prepareStats(index).setRequestCache(true).get().getTotal().getRequestCache();
    }

    private static RequestCacheStats getNodeCacheStats(Client client) {
        NodesStatsResponse stats = client.admin().cluster().prepareNodesStats().execute().actionGet();
        for (NodeStats stat : stats.getNodes()) {
            if (stat.getNode().isDataNode()) {
                return stat.getIndices().getRequestCache();
            }
        }
        return null;
    }
}
