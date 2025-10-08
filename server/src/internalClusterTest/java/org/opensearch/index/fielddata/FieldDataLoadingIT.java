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

package org.opensearch.index.fielddata;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.indices.fielddata.cache.IndicesFieldDataCache.INDICES_FIELDDATA_CACHE_SIZE_KEY;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class FieldDataLoadingIT extends OpenSearchIntegTestCase {
    // To shorten runtimes, set cluster setting INDICES_CACHE_CLEAN_INTERVAL_SETTING to a lower value.
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(IndicesService.INDICES_CACHE_CLEAN_INTERVAL_SETTING.getKey(), "1ms")
            .build();
    }

    public void testEagerGlobalOrdinalsFieldDataLoading() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("name")
                    .field("type", "text")
                    .field("fielddata", true)
                    .field("eager_global_ordinals", true)
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("name", "name").get();
        client().admin().indices().prepareRefresh("test").get();

        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getIndicesStats().getFieldData().getMemorySizeInBytes(), greaterThan(0L));

        // Ensure cache cleared before other tests in the suite begin
        client().admin().indices().clearCache(new ClearIndicesCacheRequest().fieldDataCache(true)).actionGet();
        assertBusy(() -> {
            ClusterStatsResponse clearedResponse = client().admin().cluster().prepareClusterStats().get();
            assertEquals(0, clearedResponse.getIndicesStats().getFieldData().getMemorySizeInBytes());
        });
    }

    public void testIndicesFieldDataCacheSizeSetting() throws Exception {
        // Put one value into the cache
        int maxEntries = 10;
        int numFields = 2 * maxEntries;
        String fieldPrefix = "field";
        createIndex("index", numFields, fieldPrefix);
        client().prepareSearch("index").setQuery(new MatchAllQueryBuilder()).addSort(fieldPrefix + "0", SortOrder.ASC).get();
        long sizePerEntry = client().admin().cluster().prepareClusterStats().get().getIndicesStats().getFieldData().getMemorySizeInBytes();
        assertTrue(sizePerEntry > 0);

        // Set the max size setting so that it can fit maxEntries such entries
        long maxSize = maxEntries * sizePerEntry + 1;
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(INDICES_FIELDDATA_CACHE_SIZE_KEY.getKey(), maxSize + "b"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Add >N values to the cache and assert the size is less than the limit
        for (int i = 1; i < numFields; i++) {
            client().prepareSearch("index").setQuery(new MatchAllQueryBuilder()).addSort(fieldPrefix + i, SortOrder.ASC).get();
        }
        long cacheSize = client().admin().cluster().prepareClusterStats().get().getIndicesStats().getFieldData().getMemorySizeInBytes();
        assertTrue(cacheSize <= maxSize && cacheSize > sizePerEntry);

        // Set the max size setting to a smaller value and assert the new size is less than that (waiting for refresh)
        long newMaxSize = 2 * sizePerEntry + 1;
        updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(INDICES_FIELDDATA_CACHE_SIZE_KEY.getKey(), newMaxSize + "b"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        assertBusy(() -> {
            long newCacheSize = client().admin()
                .cluster()
                .prepareClusterStats()
                .get()
                .getIndicesStats()
                .getFieldData()
                .getMemorySizeInBytes();
            assertTrue(newCacheSize <= newMaxSize);
        });
    }

    private void createIndex(String index, int numFieldsPerIndex, String fieldPrefix) throws Exception {
        assert numFieldsPerIndex >= 1;
        XContentBuilder req = jsonBuilder().startObject().startObject("properties");
        for (int j = 0; j < numFieldsPerIndex; j++) {
            req.startObject(fieldPrefix + j).field("type", "text").field("fielddata", true).endObject();
        }
        req.endObject().endObject();
        assertAcked(prepareCreate(index).setMapping(req));
        Map<String, String> source = new HashMap<>();
        for (int j = 0; j < numFieldsPerIndex; j++) {
            source.put(fieldPrefix + j, "value");
        }
        client().prepareIndex(index).setId("1").setSource(source).get();
        client().admin().indices().prepareRefresh(index).get();

        // Put something into the cache and clear it, waiting for stats to return to 0.
        // Index creation temporarily opens + closes a test index using IndicesService.withTempIndexService()
        // that has the same name as the real index,
        // and this ensures the clear resulting from that close has completed before we go to the actual test.
        client().prepareSearch(index).setQuery(new MatchAllQueryBuilder()).addSort(fieldPrefix + "0", SortOrder.ASC).get();
        client().admin().indices().clearCache(new ClearIndicesCacheRequest().fieldDataCache(true)).actionGet();
        assertBusy(() -> {
            ClusterStatsResponse clearedResponse = client().admin().cluster().prepareClusterStats().get();
            assertEquals(0, clearedResponse.getIndicesStats().getFieldData().getMemorySizeInBytes());
        });
    }

    public void testFieldDataCacheClearConcurrentIndices() throws Exception {
        // Check concurrently clearing multiple indices from the FD cache correctly removes all expected keys.
        int numIndices = 10;
        String indexPrefix = "test";
        createAndSearchIndices(numIndices, 1, indexPrefix, "field");
        // TODO: Should be 1 entry per field per index in cache, but cannot check this directly until we add the items count stat in a
        // future PR

        // Concurrently clear multiple indices from FD cache
        Thread[] threads = new Thread[numIndices];
        Phaser phaser = new Phaser(numIndices + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numIndices);

        for (int i = 0; i < numIndices; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                try {
                    ClearIndicesCacheRequest clearCacheRequest = new ClearIndicesCacheRequest().fieldDataCache(true)
                        .indices(indexPrefix + finalI);
                    client().admin().indices().clearCache(clearCacheRequest).actionGet();
                    phaser.arriveAndAwaitAdvance();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();

        // Cache size should be 0
        assertBusy(() -> {
            ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
            assertEquals(0, response.getIndicesStats().getFieldData().getMemorySizeInBytes());
        });
    }

    public void testFieldDataCacheClearConcurrentFields() throws Exception {
        // Check concurrently clearing multiple indices + fields from the FD cache correctly removes all expected keys.
        int numIndices = 10;
        int numFieldsPerIndex = 5;
        String indexPrefix = "test";
        String fieldPrefix = "field";
        createAndSearchIndices(numIndices, numFieldsPerIndex, indexPrefix, fieldPrefix);

        // Concurrently clear multiple indices+fields from FD cache
        Thread[] threads = new Thread[numIndices * numFieldsPerIndex];
        Phaser phaser = new Phaser(numIndices * numFieldsPerIndex + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numIndices * numFieldsPerIndex);

        for (int i = 0; i < numIndices; i++) {
            int finalI = i;
            for (int j = 0; j < numFieldsPerIndex; j++) {
                int finalJ = j;
                threads[i * numFieldsPerIndex + j] = new Thread(() -> {
                    try {
                        ClearIndicesCacheRequest clearCacheRequest = new ClearIndicesCacheRequest().fieldDataCache(true)
                            .indices(indexPrefix + finalI)
                            .fields(fieldPrefix + finalJ);
                        client().admin().indices().clearCache(clearCacheRequest).actionGet();
                        phaser.arriveAndAwaitAdvance();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    countDownLatch.countDown();
                });
                threads[i * numFieldsPerIndex + j].start();
            }
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();

        // Cache size should be 0
        assertBusy(() -> {
            ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
            assertEquals(0, response.getIndicesStats().getFieldData().getMemorySizeInBytes());
        });
    }

    private void createAndSearchIndices(int numIndices, int numFieldsPerIndex, String indexPrefix, String fieldPrefix) throws Exception {
        for (int i = 0; i < numIndices; i++) {
            String index = indexPrefix + i;
            createIndex(index, numFieldsPerIndex, fieldPrefix);
        }
        // Separate loop to ensure createIndex() handles any cache wipe from opening+closing the temporary test index
        for (int i = 0; i < numIndices; i++) {
            // Search on each index to fill the cache
            String index = indexPrefix + i;
            for (int j = 0; j < numFieldsPerIndex; j++) {
                client().prepareSearch(index).setQuery(new MatchAllQueryBuilder()).addSort(fieldPrefix + j, SortOrder.ASC).get();
            }
        }
        ensureGreen();
        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertTrue(response.getIndicesStats().getFieldData().getMemorySizeInBytes() > 0L);
    }
}
