/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.service.NodeCacheStats;
import org.opensearch.common.cache.stats.ImmutableCacheStats;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder.TIER_DIMENSION_NAME;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder.TIER_DIMENSION_VALUE_DISK;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder.TIER_DIMENSION_VALUE_ON_HEAP;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

// Use a single data node to simplify accessing cache stats across different shards.
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TieredSpilloverCacheStatsIT extends TieredSpilloverCacheBaseIT {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(TieredSpilloverCachePlugin.class, TieredSpilloverCacheIT.MockDiskCachePlugin.class);
    }

    private static final String HEAP_CACHE_SIZE_STRING = "10000B";
    private static final int HEAP_CACHE_SIZE = 10_000;
    private static final String index1Name = "index1";
    private static final String index2Name = "index2";

    /**
     * Test aggregating by indices
     */
    public void testIndicesLevelAggregation() throws Exception {
        internalCluster().startNodes(
            1,
            Settings.builder()
                .put(defaultSettings(HEAP_CACHE_SIZE_STRING, 1))
                .put(
                    TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(),
                    new TimeValue(0, TimeUnit.SECONDS)
                )
                .build()
        );
        Client client = client();
        Map<String, Integer> values = setupCacheForAggregationTests(client);

        ImmutableCacheStatsHolder allLevelsStatsHolder = getNodeCacheStatsResult(
            client,
            List.of(IndicesRequestCache.INDEX_DIMENSION_NAME, TIER_DIMENSION_NAME)
        );
        ImmutableCacheStatsHolder indicesOnlyStatsHolder = getNodeCacheStatsResult(
            client,
            List.of(IndicesRequestCache.INDEX_DIMENSION_NAME)
        );

        // Get values for indices alone, assert these match for statsHolders that have additional dimensions vs. a statsHolder that only has
        // the indices dimension
        ImmutableCacheStats index1ExpectedStats = returnNullIfAllZero(
            new ImmutableCacheStats(
                values.get("hitsOnHeapIndex1") + values.get("hitsOnDiskIndex1"),
                values.get("itemsOnDiskIndex1AfterTest") + values.get("itemsOnHeapIndex1AfterTest"),
                0,
                (values.get("itemsOnDiskIndex1AfterTest") + values.get("itemsOnHeapIndex1AfterTest")) * values.get("singleSearchSize"),
                values.get("itemsOnDiskIndex1AfterTest") + values.get("itemsOnHeapIndex1AfterTest")
            )
        );
        ImmutableCacheStats index2ExpectedStats = returnNullIfAllZero(
            new ImmutableCacheStats(
                values.get("hitsOnHeapIndex2") + values.get("hitsOnDiskIndex2"),
                values.get("itemsOnDiskIndex2AfterTest") + values.get("itemsOnHeapIndex2AfterTest"),
                0,
                (values.get("itemsOnDiskIndex2AfterTest") + values.get("itemsOnHeapIndex2AfterTest")) * values.get("singleSearchSize"),
                values.get("itemsOnDiskIndex2AfterTest") + values.get("itemsOnHeapIndex2AfterTest")
            )
        );

        for (ImmutableCacheStatsHolder statsHolder : List.of(allLevelsStatsHolder, indicesOnlyStatsHolder)) {
            assertEquals(index1ExpectedStats, statsHolder.getStatsForDimensionValues(List.of(index1Name)));
            assertEquals(index2ExpectedStats, statsHolder.getStatsForDimensionValues(List.of(index2Name)));
        }
    }

    /**
     * Test aggregating by indices and tier
     */
    public void testIndicesAndTierLevelAggregation() throws Exception {
        internalCluster().startNodes(
            1,
            Settings.builder()
                .put(defaultSettings(HEAP_CACHE_SIZE_STRING, 1))
                .put(
                    TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(),
                    new TimeValue(0, TimeUnit.SECONDS)
                )
                .build()
        );
        Client client = client();
        Map<String, Integer> values = setupCacheForAggregationTests(client);

        ImmutableCacheStatsHolder allLevelsStatsHolder = getNodeCacheStatsResult(
            client,
            List.of(IndicesRequestCache.INDEX_DIMENSION_NAME, TIER_DIMENSION_NAME)
        );

        // Get values broken down by indices+tiers
        ImmutableCacheStats index1HeapExpectedStats = returnNullIfAllZero(
            new ImmutableCacheStats(
                values.get("hitsOnHeapIndex1"),
                values.get("itemsOnHeapIndex1AfterTest") + values.get("itemsOnDiskIndex1AfterTest") + values.get("hitsOnDiskIndex1"),
                values.get("itemsOnDiskIndex1AfterTest"),
                values.get("itemsOnHeapIndex1AfterTest") * values.get("singleSearchSize"),
                values.get("itemsOnHeapIndex1AfterTest")
            )
        );
        assertEquals(
            index1HeapExpectedStats,
            allLevelsStatsHolder.getStatsForDimensionValues(List.of(index1Name, TIER_DIMENSION_VALUE_ON_HEAP))
        );

        ImmutableCacheStats index2HeapExpectedStats = returnNullIfAllZero(
            new ImmutableCacheStats(
                values.get("hitsOnHeapIndex2"),
                values.get("itemsOnHeapIndex2AfterTest") + values.get("itemsOnDiskIndex2AfterTest") + values.get("hitsOnDiskIndex2"),
                values.get("itemsOnDiskIndex2AfterTest"),
                values.get("itemsOnHeapIndex2AfterTest") * values.get("singleSearchSize"),
                values.get("itemsOnHeapIndex2AfterTest")
            )
        );
        assertEquals(
            index2HeapExpectedStats,
            allLevelsStatsHolder.getStatsForDimensionValues(List.of(index2Name, TIER_DIMENSION_VALUE_ON_HEAP))
        );

        ImmutableCacheStats index1DiskExpectedStats = returnNullIfAllZero(
            new ImmutableCacheStats(
                values.get("hitsOnDiskIndex1"),
                values.get("itemsOnHeapIndex1AfterTest") + values.get("itemsOnDiskIndex1AfterTest"),
                0,
                values.get("itemsOnDiskIndex1AfterTest") * values.get("singleSearchSize"),
                values.get("itemsOnDiskIndex1AfterTest")
            )
        );
        assertEquals(
            index1DiskExpectedStats,
            allLevelsStatsHolder.getStatsForDimensionValues(List.of(index1Name, TIER_DIMENSION_VALUE_DISK))
        );

        ImmutableCacheStats index2DiskExpectedStats = returnNullIfAllZero(
            new ImmutableCacheStats(
                values.get("hitsOnDiskIndex2"),
                values.get("itemsOnHeapIndex2AfterTest") + values.get("itemsOnDiskIndex2AfterTest"),
                0,
                values.get("itemsOnDiskIndex2AfterTest") * values.get("singleSearchSize"),
                values.get("itemsOnDiskIndex2AfterTest")
            )
        );
        assertEquals(
            index2DiskExpectedStats,
            allLevelsStatsHolder.getStatsForDimensionValues(List.of(index2Name, TIER_DIMENSION_VALUE_DISK))
        );
    }

    /**
     * Test aggregating by tier only
     */
    public void testTierLevelAggregation() throws Exception {
        internalCluster().startNodes(
            1,
            Settings.builder()
                .put(defaultSettings(HEAP_CACHE_SIZE_STRING, 1))
                .put(
                    TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(),
                    new TimeValue(0, TimeUnit.SECONDS)
                )
                .build()
        );
        Client client = client();
        Map<String, Integer> values = setupCacheForAggregationTests(client);
        // Get values for tiers alone and check they add correctly across indices
        ImmutableCacheStatsHolder tiersOnlyStatsHolder = getNodeCacheStatsResult(client, List.of(TIER_DIMENSION_NAME));
        ImmutableCacheStats totalHeapExpectedStats = returnNullIfAllZero(
            new ImmutableCacheStats(
                values.get("hitsOnHeapIndex1") + values.get("hitsOnHeapIndex2"),
                values.get("itemsOnHeapAfterTest") + values.get("itemsOnDiskAfterTest") + values.get("hitsOnDiskIndex1") + values.get(
                    "hitsOnDiskIndex2"
                ),
                values.get("itemsOnDiskAfterTest"),
                values.get("itemsOnHeapAfterTest") * values.get("singleSearchSize"),
                values.get("itemsOnHeapAfterTest")
            )
        );
        ImmutableCacheStats heapStats = tiersOnlyStatsHolder.getStatsForDimensionValues(List.of(TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(totalHeapExpectedStats, heapStats);
        ImmutableCacheStats totalDiskExpectedStats = returnNullIfAllZero(
            new ImmutableCacheStats(
                values.get("hitsOnDiskIndex1") + values.get("hitsOnDiskIndex2"),
                values.get("itemsOnHeapAfterTest") + values.get("itemsOnDiskAfterTest"),
                0,
                values.get("itemsOnDiskAfterTest") * values.get("singleSearchSize"),
                values.get("itemsOnDiskAfterTest")
            )
        );
        ImmutableCacheStats diskStats = tiersOnlyStatsHolder.getStatsForDimensionValues(List.of(TIER_DIMENSION_VALUE_DISK));
        assertEquals(totalDiskExpectedStats, diskStats);
    }

    public void testInvalidLevelsAreIgnored() throws Exception {
        internalCluster().startNodes(
            1,
            Settings.builder()
                .put(defaultSettings(HEAP_CACHE_SIZE_STRING, getNumberOfSegments()))
                .put(
                    TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(),
                    new TimeValue(0, TimeUnit.SECONDS)
                )
                .build()
        );
        Client client = client();
        Map<String, Integer> values = setupCacheForAggregationTests(client);

        ImmutableCacheStatsHolder allLevelsStatsHolder = getNodeCacheStatsResult(
            client,
            List.of(IndicesRequestCache.INDEX_DIMENSION_NAME, TIER_DIMENSION_NAME)
        );
        ImmutableCacheStatsHolder indicesOnlyStatsHolder = getNodeCacheStatsResult(
            client,
            List.of(IndicesRequestCache.INDEX_DIMENSION_NAME)
        );

        // Test invalid levels are ignored and permuting the order of levels in the request doesn't matter

        // This should be equivalent to just "indices"
        ImmutableCacheStatsHolder indicesEquivalentStatsHolder = getNodeCacheStatsResult(
            client,
            List.of(IndicesRequestCache.INDEX_DIMENSION_NAME, "unrecognized_dimension")
        );
        assertEquals(indicesOnlyStatsHolder, indicesEquivalentStatsHolder);

        // This should be equivalent to "indices", "tier"
        ImmutableCacheStatsHolder indicesAndTierEquivalentStatsHolder = getNodeCacheStatsResult(
            client,
            List.of(TIER_DIMENSION_NAME, "unrecognized_dimension_1", IndicesRequestCache.INDEX_DIMENSION_NAME, "unrecognized_dimension_2")
        );
        assertEquals(allLevelsStatsHolder, indicesAndTierEquivalentStatsHolder);

        // This should be equivalent to no levels passed in
        ImmutableCacheStatsHolder noLevelsEquivalentStatsHolder = getNodeCacheStatsResult(
            client,
            List.of("unrecognized_dimension_1", "unrecognized_dimension_2")
        );
        ImmutableCacheStatsHolder noLevelsStatsHolder = getNodeCacheStatsResult(client, List.of());
        assertEquals(noLevelsStatsHolder, noLevelsEquivalentStatsHolder);
    }

    /**
     * Check the new stats API returns the same values as the old stats API.
     */
    public void testStatsMatchOldApi() throws Exception {
        internalCluster().startNodes(
            1,
            Settings.builder()
                .put(defaultSettings(HEAP_CACHE_SIZE_STRING, getNumberOfSegments()))
                .put(
                    TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(),
                    new TimeValue(0, TimeUnit.SECONDS)
                )
                .build()
        );
        String index = "index";
        Client client = client();
        startIndex(client, index);

        // First search one time to see how big a single value will be
        searchIndex(client, index, 0);
        // get total stats
        long singleSearchSize = getTotalStats(client).getSizeInBytes();
        // Select numbers so we get some values on both heap and disk
        int itemsOnHeap = HEAP_CACHE_SIZE / (int) singleSearchSize;
        int itemsOnDisk = 1 + randomInt(30); // The first one we search (to get the size) always goes to disk
        int expectedEntries = itemsOnHeap + itemsOnDisk;

        for (int i = 1; i < expectedEntries; i++) {
            // Cause misses
            searchIndex(client, index, i);
        }
        int expectedMisses = itemsOnHeap + itemsOnDisk;

        // Cause some hits
        int expectedHits = randomIntBetween(itemsOnHeap, expectedEntries); // Select it so some hits come from both tiers
        for (int i = 0; i < expectedHits; i++) {
            searchIndex(client, index, i);
        }

        ImmutableCacheStats totalStats = getNodeCacheStatsResult(client, List.of()).getTotalStats();

        // Check the new stats API values are as expected
        assertEquals(
            new ImmutableCacheStats(expectedHits, expectedMisses, 0, expectedEntries * singleSearchSize, expectedEntries),
            totalStats
        );
        // Now check the new stats API values for the cache as a whole match the old stats API values
        RequestCacheStats oldAPIStats = client.admin()
            .indices()
            .prepareStats(index)
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();
        assertEquals(oldAPIStats.getHitCount(), totalStats.getHits());
        assertEquals(oldAPIStats.getMissCount(), totalStats.getMisses());
        assertEquals(oldAPIStats.getEvictions(), totalStats.getEvictions());
        assertEquals(oldAPIStats.getMemorySizeInBytes(), totalStats.getSizeInBytes());
    }

    public void testStatsWithMultipleSegments() throws Exception {
        int numberOfSegments = randomFrom(2, 4, 8, 16, 64);
        int singleSearchSizeApproxUpperBound = 700; // We know this from other tests and manually verifying
        int heap_cache_size_per_segment = singleSearchSizeApproxUpperBound * numberOfSegments; // Worst case if all
        // keys land up in same segment, it would still be able to accommodate.
        internalCluster().startNodes(
            1,
            Settings.builder()
                .put(defaultSettings(heap_cache_size_per_segment * numberOfSegments + "B", numberOfSegments))
                .put(
                    TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(),
                    new TimeValue(0, TimeUnit.SECONDS)
                )
                .build()
        );
        Client client = client();
        startIndex(client, index1Name);
        // First search one time to calculate item size
        searchIndex(client, index1Name, 0);
        // get total stats
        long singleSearchSize = getTotalStats(client).getSizeInBytes();
        // Now try to hit queries same as number of segments. All these should be able to reside inside onHeap cache.
        for (int i = 1; i < numberOfSegments; i++) {
            searchIndex(client, index1Name, i);
        }
        ImmutableCacheStatsHolder allLevelsStatsHolder = getNodeCacheStatsResult(
            client,
            List.of(IndicesRequestCache.INDEX_DIMENSION_NAME, TIER_DIMENSION_NAME)
        );
        ImmutableCacheStats index1OnHeapExpectedStats = returnNullIfAllZero(
            new ImmutableCacheStats(0, numberOfSegments, 0, singleSearchSize * numberOfSegments, numberOfSegments)
        );
        assertEquals(
            index1OnHeapExpectedStats,
            allLevelsStatsHolder.getStatsForDimensionValues(List.of(index1Name, TIER_DIMENSION_VALUE_ON_HEAP))
        );
        ImmutableCacheStats index1DiskCacheExpectedStats = returnNullIfAllZero(new ImmutableCacheStats(0, numberOfSegments, 0, 0, 0));
        assertEquals(
            index1DiskCacheExpectedStats,
            allLevelsStatsHolder.getStatsForDimensionValues(List.of(index1Name, TIER_DIMENSION_VALUE_DISK))
        );

        // Now fire same queries to get some hits
        for (int i = 0; i < numberOfSegments; i++) {
            searchIndex(client, index1Name, i);
        }
        allLevelsStatsHolder = getNodeCacheStatsResult(client, List.of(IndicesRequestCache.INDEX_DIMENSION_NAME, TIER_DIMENSION_NAME));
        index1OnHeapExpectedStats = returnNullIfAllZero(
            new ImmutableCacheStats(numberOfSegments, numberOfSegments, 0, singleSearchSize * numberOfSegments, numberOfSegments)
        );
        assertEquals(
            index1OnHeapExpectedStats,
            allLevelsStatsHolder.getStatsForDimensionValues(List.of(index1Name, TIER_DIMENSION_VALUE_ON_HEAP))
        );

        // Now try to evict from onheap cache by adding numberOfSegments ^ 2 which will guarantee this.
        for (int i = numberOfSegments; i < numberOfSegments + numberOfSegments * numberOfSegments; i++) {
            searchIndex(client, index1Name, i);
        }
        allLevelsStatsHolder = getNodeCacheStatsResult(client, List.of(IndicesRequestCache.INDEX_DIMENSION_NAME, TIER_DIMENSION_NAME));
        ImmutableCacheStats onHeapCacheStat = allLevelsStatsHolder.getStatsForDimensionValues(
            List.of(index1Name, TIER_DIMENSION_VALUE_ON_HEAP)
        );
        // Jut verifying evictions happened as can't fetch the exact number considering we don't have a way to get
        // segment number for queries.
        assertTrue(onHeapCacheStat.getEvictions() > 0);
        ImmutableCacheStats diskCacheStat = allLevelsStatsHolder.getStatsForDimensionValues(List.of(index1Name, TIER_DIMENSION_VALUE_DISK));

        // Similarly verify items are present on disk cache now
        assertEquals(onHeapCacheStat.getEvictions(), diskCacheStat.getItems());
        assertTrue(diskCacheStat.getSizeInBytes() > 0);
        assertTrue(diskCacheStat.getMisses() > 0);
        assertTrue(diskCacheStat.getHits() == 0);
        assertTrue(diskCacheStat.getEvictions() == 0);
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
                        // Disable index refreshing to avoid cache being invalidated mid-test
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
                        .build()
                )
                .get()
        );
        indexRandom(true, client.prepareIndex(indexName).setSource("k", "hello"));
        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        ForceMergeResponse forceMergeResponse = client.admin().indices().prepareForceMerge(indexName).setFlush(true).get();
        ensureSearchable(indexName);
    }

    private Map<String, Integer> setupCacheForAggregationTests(Client client) throws Exception {
        startIndex(client, index1Name);
        startIndex(client, index2Name);

        // First search one time to see how big a single value will be
        searchIndex(client, index1Name, 0);
        // get total stats
        long singleSearchSize = getTotalStats(client).getSizeInBytes();

        int itemsOnHeapAfterTest = HEAP_CACHE_SIZE / (int) singleSearchSize; // As the heap tier evicts, the items on it after the test will
        // be the same as its max capacity
        int itemsOnDiskAfterTest = 1 + randomInt(30); // The first one we search (to get the size) always goes to disk

        // Put some values on heap and disk for each index
        int itemsOnHeapIndex1AfterTest = randomInt(itemsOnHeapAfterTest);
        int itemsOnHeapIndex2AfterTest = itemsOnHeapAfterTest - itemsOnHeapIndex1AfterTest;
        int itemsOnDiskIndex1AfterTest = 1 + randomInt(itemsOnDiskAfterTest - 1);
        // The first one we search (to get the size) always goes to disk
        int itemsOnDiskIndex2AfterTest = itemsOnDiskAfterTest - itemsOnDiskIndex1AfterTest;
        int hitsOnHeapIndex1 = randomInt(itemsOnHeapIndex1AfterTest);
        int hitsOnDiskIndex1 = randomInt(itemsOnDiskIndex1AfterTest);
        int hitsOnHeapIndex2 = randomInt(itemsOnHeapIndex2AfterTest);
        int hitsOnDiskIndex2 = randomInt(itemsOnDiskIndex2AfterTest);

        // Put these values into a map so tests can know what to expect in stats responses
        Map<String, Integer> expectedValues = new HashMap<>();
        expectedValues.put("itemsOnHeapIndex1AfterTest", itemsOnHeapIndex1AfterTest);
        expectedValues.put("itemsOnHeapIndex2AfterTest", itemsOnHeapIndex2AfterTest);
        expectedValues.put("itemsOnDiskIndex1AfterTest", itemsOnDiskIndex1AfterTest);
        expectedValues.put("itemsOnDiskIndex2AfterTest", itemsOnDiskIndex2AfterTest);
        expectedValues.put("hitsOnHeapIndex1", hitsOnHeapIndex1);
        expectedValues.put("hitsOnDiskIndex1", hitsOnDiskIndex1);
        expectedValues.put("hitsOnHeapIndex2", hitsOnHeapIndex2);
        expectedValues.put("hitsOnDiskIndex2", hitsOnDiskIndex2);
        expectedValues.put("singleSearchSize", (int) singleSearchSize);
        expectedValues.put("itemsOnDiskAfterTest", itemsOnDiskAfterTest);
        expectedValues.put("itemsOnHeapAfterTest", itemsOnHeapAfterTest); // Can only pass 10 keys in Map.of() constructor

        // The earliest items (0 - itemsOnDiskAfterTest) are the ones which get evicted to disk
        for (int i = 1; i < itemsOnDiskIndex1AfterTest; i++) { // Start at 1 as 0 has already been searched
            searchIndex(client, index1Name, i);
        }
        for (int i = itemsOnDiskIndex1AfterTest; i < itemsOnDiskIndex1AfterTest + itemsOnDiskIndex2AfterTest; i++) {
            searchIndex(client, index2Name, i);
        }
        // The remaining items stay on heap
        for (int i = itemsOnDiskAfterTest; i < itemsOnDiskAfterTest + itemsOnHeapIndex1AfterTest; i++) {
            searchIndex(client, index1Name, i);
        }
        for (int i = itemsOnDiskAfterTest + itemsOnHeapIndex1AfterTest; i < itemsOnDiskAfterTest + itemsOnHeapAfterTest; i++) {
            searchIndex(client, index2Name, i);
        }
        // Get some hits on all combinations of indices and tiers
        for (int i = itemsOnDiskAfterTest; i < itemsOnDiskAfterTest + hitsOnHeapIndex1; i++) {
            // heap hits for index 1
            searchIndex(client, index1Name, i);
        }
        for (int i = itemsOnDiskAfterTest + itemsOnHeapIndex1AfterTest; i < itemsOnDiskAfterTest + itemsOnHeapIndex1AfterTest
            + hitsOnHeapIndex2; i++) {
            // heap hits for index 2
            searchIndex(client, index2Name, i);
        }
        for (int i = 0; i < hitsOnDiskIndex1; i++) {
            // disk hits for index 1
            searchIndex(client, index1Name, i);
        }
        for (int i = itemsOnDiskIndex1AfterTest; i < itemsOnDiskIndex1AfterTest + hitsOnDiskIndex2; i++) {
            // disk hits for index 2
            searchIndex(client, index2Name, i);
        }
        return expectedValues;
    }

    private ImmutableCacheStats returnNullIfAllZero(ImmutableCacheStats expectedStats) {
        // If the randomly chosen numbers are such that the expected stats would be 0, we actually have not interacted with the cache for
        // this index.
        // In this case, we expect the stats holder to have no stats for this node, and therefore we should get null from
        // statsHolder.getStatsForDimensionValues().
        // We will not see it in the XContent response.
        if (expectedStats.equals(new ImmutableCacheStats(0, 0, 0, 0, 0))) {
            return null;
        }
        return expectedStats;
    }

    // Duplicated from CacheStatsAPIIndicesRequestCacheIT.java, as we can't add a dependency on server.internalClusterTest

    private SearchResponse searchIndex(Client client, String index, int searchSuffix) {
        SearchResponse resp = client.prepareSearch(index)
            .setRequestCache(true)
            .setQuery(QueryBuilders.termQuery("k", "hello" + padWithZeros(4, searchSuffix)))
            // pad with zeros so request 0 and request 10 have the same size ("0000" and "0010" instead of "0" and "10")
            .get();
        assertSearchResponse(resp);
        OpenSearchAssertions.assertAllSuccessful(resp);
        return resp;
    }

    private String padWithZeros(int finalLength, int inputValue) {
        // Avoid forbidden API String.format()
        String input = String.valueOf(inputValue);
        if (input.length() >= finalLength) {
            return input;
        }
        StringBuilder sb = new StringBuilder();
        while (sb.length() < finalLength - input.length()) {
            sb.append('0');
        }
        sb.append(input);
        return sb.toString();
    }

    private ImmutableCacheStats getTotalStats(Client client) throws IOException {
        ImmutableCacheStatsHolder statsHolder = getNodeCacheStatsResult(client, List.of());
        return statsHolder.getStatsForDimensionValues(List.of());
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
}
