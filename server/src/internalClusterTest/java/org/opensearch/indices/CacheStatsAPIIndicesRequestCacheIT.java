/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Randomness;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.service.NodeCacheStats;
import org.opensearch.common.cache.stats.ImmutableCacheStats;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

// Use a single data node to simplify logic about cache stats across different shards.
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CacheStatsAPIIndicesRequestCacheIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    public CacheStatsAPIIndicesRequestCacheIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.<Object[]>asList(new Object[] { Settings.builder().put(FeatureFlags.PLUGGABLE_CACHE, "true").build() });
    }

    /**
     * Test aggregating by indices, indices+shards, shards, or no levels, and check the resulting stats
     * are as we expect.
     */
    public void testCacheStatsAPIWIthOnHeapCache() throws Exception {
        String index1Name = "index1";
        String index2Name = "index2";
        Client client = client();

        startIndex(client, index1Name);
        startIndex(client, index2Name);

        // Search twice for the same doc in index 1
        for (int i = 0; i < 2; i++) {
            searchIndex(client, index1Name, "");
        }

        // Search once for a doc in index 2
        searchIndex(client, index2Name, "");

        // First, aggregate by indices only
        ImmutableCacheStatsHolder indicesStats = getNodeCacheStatsResult(client, List.of(IndicesRequestCache.INDEX_DIMENSION_NAME));

        List<String> index1Dimensions = List.of(index1Name);
        // Since we searched twice, we expect to see 1 hit, 1 miss and 1 entry for index 1
        ImmutableCacheStats expectedStats = new ImmutableCacheStats(1, 1, 0, 0, 1);
        checkCacheStatsAPIResponse(indicesStats, index1Dimensions, expectedStats, false, true);
        // Get the request size for one request, so we can reuse it for next index
        long requestSize = indicesStats.getStatsForDimensionValues(List.of(index1Name)).getSizeInBytes();
        assertTrue(requestSize > 0);

        List<String> index2Dimensions = List.of(index2Name);
        // We searched once in index 2, we expect 1 miss + 1 entry
        expectedStats = new ImmutableCacheStats(0, 1, 0, requestSize, 1);
        checkCacheStatsAPIResponse(indicesStats, index2Dimensions, expectedStats, true, true);

        // The total stats for the node should be 1 hit, 2 misses, and 2 entries
        expectedStats = new ImmutableCacheStats(1, 2, 0, 2 * requestSize, 2);
        List<String> totalStatsKeys = List.of();
        checkCacheStatsAPIResponse(indicesStats, totalStatsKeys, expectedStats, true, true);

        // Aggregate by shards only
        ImmutableCacheStatsHolder shardsStats = getNodeCacheStatsResult(client, List.of(IndicesRequestCache.SHARD_ID_DIMENSION_NAME));

        List<String> index1Shard0Dimensions = List.of("[" + index1Name + "][0]");

        expectedStats = new ImmutableCacheStats(1, 1, 0, requestSize, 1);
        checkCacheStatsAPIResponse(shardsStats, index1Shard0Dimensions, expectedStats, true, true);

        List<String> index2Shard0Dimensions = List.of("[" + index2Name + "][0]");
        expectedStats = new ImmutableCacheStats(0, 1, 0, requestSize, 1);
        checkCacheStatsAPIResponse(shardsStats, index2Shard0Dimensions, expectedStats, true, true);

        // Aggregate by indices and shards
        ImmutableCacheStatsHolder indicesAndShardsStats = getNodeCacheStatsResult(
            client,
            List.of(IndicesRequestCache.INDEX_DIMENSION_NAME, IndicesRequestCache.SHARD_ID_DIMENSION_NAME)
        );

        index1Dimensions = List.of(index1Name, "[" + index1Name + "][0]");

        expectedStats = new ImmutableCacheStats(1, 1, 0, requestSize, 1);
        checkCacheStatsAPIResponse(indicesAndShardsStats, index1Dimensions, expectedStats, true, true);

        index2Dimensions = List.of(index2Name, "[" + index2Name + "][0]");
        expectedStats = new ImmutableCacheStats(0, 1, 0, requestSize, 1);
        checkCacheStatsAPIResponse(indicesAndShardsStats, index2Dimensions, expectedStats, true, true);
    }

    /**
     * Check the new stats API returns the same values as the old stats API. In particular,
     * check that the new and old APIs are both correctly estimating memory size,
     * using the logic that includes the overhead memory in ICacheKey.
     */
    public void testStatsMatchOldApi() throws Exception {
        String index = "index";
        Client client = client();
        startIndex(client, index);

        int numKeys = Randomness.get().nextInt(100) + 1;
        for (int i = 0; i < numKeys; i++) {
            searchIndex(client, index, String.valueOf(i));
        }
        // Get some hits as well
        for (int i = 0; i < numKeys / 2; i++) {
            searchIndex(client, index, String.valueOf(i));
        }

        RequestCacheStats oldApiStats = client.admin()
            .indices()
            .prepareStats(index)
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();
        assertNotEquals(0, oldApiStats.getMemorySizeInBytes());

        ImmutableCacheStatsHolder statsHolder = getNodeCacheStatsResult(client, List.of());
        ImmutableCacheStats expected = new ImmutableCacheStats(
            oldApiStats.getHitCount(),
            oldApiStats.getMissCount(),
            oldApiStats.getEvictions(),
            oldApiStats.getMemorySizeInBytes(),
            0
        );
        // Don't check entries, as the old API doesn't track this
        checkCacheStatsAPIResponse(statsHolder, List.of(), expected, true, false);
    }

    /**
     * Test the XContent in the response behaves correctly when we pass null levels.
     * Only the total cache stats should be returned.
     */
    public void testNullLevels() throws Exception {
        String index = "index";
        Client client = client();
        startIndex(client, index);
        int numKeys = Randomness.get().nextInt(100) + 1;
        for (int i = 0; i < numKeys; i++) {
            searchIndex(client, index, String.valueOf(i));
        }
        Map<String, Object> xContentMap = getStatsXContent(getNodeCacheStatsResult(client, null));
        // Null levels should result in only the total cache stats being returned -> 6 fields inside the response.
        assertEquals(6, xContentMap.size());
    }

    /**
     * Test clearing the cache using API sets memory size and number of items to 0, but leaves other stats
     * unaffected.
     */
    public void testCacheClear() throws Exception {
        String index = "index";
        Client client = client();

        startIndex(client, index);

        int expectedHits = 2;
        int expectedMisses = 7;
        // Search for the same doc to give hits
        for (int i = 0; i < expectedHits + 1; i++) {
            searchIndex(client, index, "");
        }
        // Search for new docs
        for (int i = 0; i < expectedMisses - 1; i++) {
            searchIndex(client, index, String.valueOf(i));
        }

        ImmutableCacheStats expectedTotal = new ImmutableCacheStats(expectedHits, expectedMisses, 0, 0, expectedMisses);
        ImmutableCacheStatsHolder statsHolder = getNodeCacheStatsResult(client, List.of());
        // Don't check the memory size, just assert it's nonzero
        checkCacheStatsAPIResponse(statsHolder, List.of(), expectedTotal, false, true);
        long originalMemorySize = statsHolder.getTotalSizeInBytes();
        assertNotEquals(0, originalMemorySize);

        // Clear cache
        ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest(index);
        client.admin().indices().clearCache(clearIndicesCacheRequest).actionGet();

        // Now size and items should be 0
        expectedTotal = new ImmutableCacheStats(expectedHits, expectedMisses, 0, 0, 0);
        statsHolder = getNodeCacheStatsResult(client, List.of());
        checkCacheStatsAPIResponse(statsHolder, List.of(), expectedTotal, true, true);
    }

    /**
     * Test the cache stats responses are in the expected place in XContent when we call the overall API
     * GET /_nodes/stats. They should be at nodes.[node_id].caches.request_cache.
     */
    public void testNodesStatsResponse() throws Exception {
        String index = "index";
        Client client = client();

        startIndex(client, index);

        NodesStatsResponse nodeStatsResponse = client.admin()
            .cluster()
            .prepareNodesStats("data:true")
            .all() // This mimics /_nodes/stats
            .get();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        Map<String, String> paramMap = new HashMap<>();
        ToXContent.Params params = new ToXContent.MapParams(paramMap);

        builder.startObject();
        nodeStatsResponse.toXContent(builder, params);
        builder.endObject();
        Map<String, Object> xContentMap = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), builder.toString(), true);
        // Values should be at nodes.[node_id].caches.request_cache
        // Get the node id
        Map<String, Object> nodesResponse = (Map<String, Object>) xContentMap.get("nodes");
        assertEquals(1, nodesResponse.size());
        String nodeId = nodesResponse.keySet().toArray(String[]::new)[0];
        Map<String, Object> cachesResponse = (Map<String, Object>) ((Map<String, Object>) nodesResponse.get(nodeId)).get("caches");
        assertNotNull(cachesResponse);
        // Request cache should be present in the response
        assertTrue(cachesResponse.containsKey("request_cache"));
    }

    private void startIndex(Client client, String indexName) throws InterruptedException {
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(indexName)
                .setMapping("k", "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .get()
        );
        indexRandom(true, client.prepareIndex(indexName).setSource("k", "hello"));
        ensureSearchable(indexName);
    }

    private SearchResponse searchIndex(Client client, String index, String searchSuffix) {
        SearchResponse resp = client.prepareSearch(index)
            .setRequestCache(true)
            .setQuery(QueryBuilders.termQuery("k", "hello" + searchSuffix))
            .get();
        assertSearchResponse(resp);
        OpenSearchAssertions.assertAllSuccessful(resp);
        return resp;
    }

    private static ImmutableCacheStatsHolder getNodeCacheStatsResult(Client client, List<String> aggregationLevels) throws IOException {
        CommonStatsFlags statsFlags = new CommonStatsFlags();
        statsFlags.includeAllCacheTypes();
        String[] flagsLevels;
        if (aggregationLevels == null) {
            flagsLevels = null;
        } else {
            flagsLevels = aggregationLevels.toArray(new String[0]);
        }
        statsFlags.setLevels(flagsLevels);

        NodesStatsResponse nodeStatsResponse = client.admin()
            .cluster()
            .prepareNodesStats("data:true")
            .addMetric(NodesStatsRequest.Metric.CACHE_STATS.metricName())
            .setIndices(statsFlags)
            .get();
        // Can always get the first data node as there's only one in this test suite
        assertEquals(1, nodeStatsResponse.getNodes().size());
        NodeCacheStats ncs = nodeStatsResponse.getNodes().get(0).getNodeCacheStats();
        return ncs.getStatsByCache(CacheType.INDICES_REQUEST_CACHE);
    }

    private static Map<String, Object> getStatsXContent(ImmutableCacheStatsHolder statsHolder) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        Map<String, String> paramMap = new HashMap<>();
        ToXContent.Params params = new ToXContent.MapParams(paramMap);

        builder.startObject();
        statsHolder.toXContent(builder, params);
        builder.endObject();

        String resultString = builder.toString();
        return XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), resultString, true);
    }

    private static void checkCacheStatsAPIResponse(
        ImmutableCacheStatsHolder statsHolder,
        List<String> dimensionValues,
        ImmutableCacheStats expectedStats,
        boolean checkMemorySize,
        boolean checkEntries
    ) {
        ImmutableCacheStats aggregatedStatsResponse = statsHolder.getStatsForDimensionValues(dimensionValues);
        assertNotNull(aggregatedStatsResponse);
        assertEquals(expectedStats.getHits(), (int) aggregatedStatsResponse.getHits());
        assertEquals(expectedStats.getMisses(), (int) aggregatedStatsResponse.getMisses());
        assertEquals(expectedStats.getEvictions(), (int) aggregatedStatsResponse.getEvictions());
        if (checkMemorySize) {
            assertEquals(expectedStats.getSizeInBytes(), (int) aggregatedStatsResponse.getSizeInBytes());
        }
        if (checkEntries) {
            assertEquals(expectedStats.getItems(), (int) aggregatedStatsResponse.getItems());
        }
    }
}
