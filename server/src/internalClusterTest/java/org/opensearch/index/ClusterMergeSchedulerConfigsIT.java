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

package org.opensearch.index;

import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.indices.ClusterMergeSchedulerConfig;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.opensearch.common.util.concurrent.OpenSearchExecutors.NODE_PROCESSORS_SETTING;
import static org.opensearch.indices.ClusterMergeSchedulerConfig.CLUSTER_AUTO_THROTTLE_SETTING;
import static org.opensearch.indices.ClusterMergeSchedulerConfig.CLUSTER_MAX_MERGE_COUNT_SETTING;
import static org.opensearch.indices.ClusterMergeSchedulerConfig.CLUSTER_MAX_THREAD_COUNT_SETTING;

/**
 * Integration tests for {@link ClusterMergeSchedulerConfig} and merge scheduler settings hierarchy.
 * How this is supposed to work -
 *
 * <h2>Complete Decision Matrix</h2>
 * <table border="1">
 *   <caption>Settings hierarchy and fallback behavior for merge scheduler configuration</caption>
 *   <tr>
 *     <th>Index Settings State</th>
 *     <th>Cluster Defaults Exist?</th>
 *     <th>Resulting Configuration</th>
 *     <th>Test Method</th>
 *   </tr>
 *   <tr>
 *     <td>Nothing set</td>
 *     <td>No</td>
 *     <td>Absolute defaults (CPU-based)</td>
 *     <td>{@code testAbsoluteDefaults()}</td>
 *   </tr>
 *   <tr>
 *     <td>Nothing set</td>
 *     <td>Yes</td>
 *     <td>Cluster defaults</td>
 *     <td>{@code testClusterDefaults()}</td>
 *   </tr>
 *   <tr>
 *     <td>Only thread count set</td>
 *     <td>Doesn't matter</td>
 *     <td>thread_count = your value<br/>merge_count = your value + 5</td>
 *     <td>{@code testIndexLevelThreadCountOnly()}</td>
 *   </tr>
 *   <tr>
 *     <td>Only merge count set</td>
 *     <td>Doesn't matter</td>
 *     <td>merge_count = your value<br/>thread_count = absolute default</td>
 *     <td>{@code testIndexLevelMergeCountOnly()}</td>
 *   </tr>
 *   <tr>
 *     <td>Both set</td>
 *     <td>Doesn't matter</td>
 *     <td>Both use your values (if valid)</td>
 *     <td>{@code testIndexLevelBothSet()}</td>
 *   </tr>
 *   <tr>
 *     <td>Had 2, removed 1</td>
 *     <td>Doesn't matter</td>
 *     <td>Removed → absolute default<br/>Other → keeps index value</td>
 *     <td>{@code testRemoveOneOfTwoSettings()}</td>
 *   </tr>
 *   <tr>
 *     <td>Had 2, removed both</td>
 *     <td>Yes</td>
 *     <td>Both → cluster defaults</td>
 *     <td>{@code testRemoveAllSettings()}</td>
 *   </tr>
 *   <tr>
 *     <td>Had 1, removed it</td>
 *     <td>Yes</td>
 *     <td>Both → cluster defaults</td>
 *     <td>{@code testRemoveOnlyExplicitSetting()}</td>
 *   </tr>
 *   <tr>
 *     <td>Invalid combination</td>
 *     <td>N/A</td>
 *     <td>Rejected (thread_count > merge_count)</td>
 *     <td>{@code testInvalidConfiguration()}</td>
 *   </tr>
 * </table>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class ClusterMergeSchedulerConfigsIT extends OpenSearchIntegTestCase {

    @Override
    public Settings indexSettings() {
        Settings.Builder s = Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2);
        return s.build();
    }

    @Override
    protected Settings.Builder setRandomIndexMergeSettings(Random random, Settings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(
                TieredMergePolicyProvider.INDEX_COMPOUND_FORMAT_SETTING.getKey(),
                (random.nextBoolean() ? random.nextDouble() : random.nextBoolean()).toString()
            );
        }

        return builder;
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(NODE_PROCESSORS_SETTING.getKey(), 6).build();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().startClusterManagerOnlyNode();
    }

    public void testMergeSchedulerSettingsWithMultipleIndices() throws Exception {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());
        String indexName = "log-myindex-1";
        createIndex(indexName, indexSettings());
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService indexService = getIndexService(indicesService, new Index(indexName, uuid));
        assertEquals(8, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(3, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(true, indexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());

        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_MAX_MERGE_COUNT_SETTING.getKey(), 40).put(CLUSTER_MAX_THREAD_COUNT_SETTING.getKey(), 20)
            )
            .get();

        indexName = "log-myindex-2";
        createIndex(indexName, indexSettings());
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
        getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
        indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService secondIndexService = getIndexService(indicesService, new Index(indexName, uuid));
        assertEquals(40, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(40, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(20, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(true, indexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());
        assertEquals(true, secondIndexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());

        int replicas = randomIntBetween(1, Math.max(1, internalCluster().numDataNodes() - 1));
        // Create index with index level override in settings
        indexName = "log-myindex-3";
        createIndex(
            indexName,
            Settings.builder()
                .put(indexSettings())
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 150)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas)
                .build()
        );
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
        getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
        indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService thirdIndexService = getIndexService(indicesService, new Index(indexName, uuid));

        assertEquals(150, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(40, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(40, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(20, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(3, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(true, indexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());
        assertEquals(true, secondIndexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());
        assertEquals(true, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());

        // changing cluster level default should only affect indices without index level override
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_MAX_MERGE_COUNT_SETTING.getKey(), 35))
            .get();

        assertEquals(35, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(35, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(150, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(20, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(3, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());

        // updating cluster level auto_throttle to false
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_AUTO_THROTTLE_SETTING.getKey(), false))
            .get();

        // removing index level override should pick up the cluster level default
        UpdateSettingsRequestBuilder builder = client().admin().indices().prepareUpdateSettings(indexName);
        builder.setSettings(Settings.builder().putNull(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey()).build());
        builder.execute().actionGet();

        assertEquals(35, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(35, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(35, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(20, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(false, indexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());
        assertEquals(false, secondIndexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());
        assertEquals(false, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());

        // try to update with an invalid setting
        builder = client().admin().indices().prepareUpdateSettings(indexName);
        builder.setSettings(Settings.builder().put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 1).build());
        UpdateSettingsRequestBuilder finalBuilder = builder;
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> finalBuilder.execute().actionGet());
        assertTrue(exception.getMessage().contains("maxThreadCount (= 3) should be <= maxMergeCount (= 1)"));

        // verify no change in settings post failure
        assertEquals(35, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(35, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(35, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(20, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());

        // update index level setting to override cluster level default
        builder = client().admin().indices().prepareUpdateSettings(indexName);
        builder.setSettings(Settings.builder().put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 27).build());
        builder.execute().actionGet();

        assertEquals(35, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(35, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(27, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(20, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(3, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());

        // setting auto throttle back to true for one index
        builder = client().admin().indices().prepareUpdateSettings("log-myindex-2");
        builder.setSettings(Settings.builder().put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), "true").build());
        builder.execute().actionGet();

        assertEquals(false, indexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());
        assertEquals(true, secondIndexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());
        assertEquals(false, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());
    }

    public void testClusterMergeConfigs() throws Exception {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());

        String indexName = "log-myindex-1";
        createIndex(indexName, indexSettings());
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        ClusterSettings clusterSettings = indicesService.clusterService().getClusterSettings();

        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        Index index = new Index(indexName, uuid);
        IndexService indexService = getIndexService(indicesService, index);
        assertEquals(8, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(3, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(true, indexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());

        // Updating cluster defaults: CLUSTER_MAX_THREAD_COUNT_SETTING=60 and CLUSTER_MAX_MERGE_COUNT_SETTING=80
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_MAX_MERGE_COUNT_SETTING.getKey(), 80).put(CLUSTER_MAX_THREAD_COUNT_SETTING.getKey(), 60)
            )
            .get();

        assertEquals(80, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(60, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals("80", clusterSettings.get(CLUSTER_MAX_MERGE_COUNT_SETTING).toString());
        assertEquals("60", clusterSettings.get(CLUSTER_MAX_THREAD_COUNT_SETTING).toString());

        // Updating cluster defaults: CLUSTER_MAX_THREAD_COUNT_SETTING=100, expecting failure for failed validation
        // and no change in settings' values
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> client(clusterManagerName).admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(CLUSTER_MAX_THREAD_COUNT_SETTING.getKey(), 100))
                .get()
        );
        assertTrue(exception.getMessage().contains("maxThreadCount (= 100) should be <= maxMergeCount (= 80)"));

        assertEquals(80, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(60, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals("80", clusterSettings.get(CLUSTER_MAX_MERGE_COUNT_SETTING).toString());
        assertEquals("60", clusterSettings.get(CLUSTER_MAX_THREAD_COUNT_SETTING).toString());

        // Removing CLUSTER_MAX_MERGE_COUNT_SETTING and asserting it gets adjusted according to CLUSTER_MAX_THREAD_COUNT_SETTING
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(CLUSTER_MAX_MERGE_COUNT_SETTING.getKey()))
            .get();

        assertEquals((60 + 5), indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(60, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals("65", clusterSettings.get(CLUSTER_MAX_MERGE_COUNT_SETTING).toString());
        assertEquals("60", clusterSettings.get(CLUSTER_MAX_THREAD_COUNT_SETTING).toString());

        // updating CLUSTER_MAX_THREAD_COUNT_SETTING to 100 and expecting CLUSTER_MAX_MERGE_COUNT_SETTING to get
        // adjusted accordingly (since its not already set)
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_MAX_THREAD_COUNT_SETTING.getKey(), 100))
            .get();

        assertEquals(105, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(100, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals("105", clusterSettings.get(CLUSTER_MAX_MERGE_COUNT_SETTING).toString());
        assertEquals("100", clusterSettings.get(CLUSTER_MAX_THREAD_COUNT_SETTING).toString());

        // updating CLUSTER_MAX_MERGE_COUNT_SETTING to 200
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_MAX_MERGE_COUNT_SETTING.getKey(), 200))
            .get();

        assertEquals(200, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(100, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals("200", clusterSettings.get(CLUSTER_MAX_MERGE_COUNT_SETTING).toString());
        assertEquals("100", clusterSettings.get(CLUSTER_MAX_THREAD_COUNT_SETTING).toString());

        // Deleting CLUSTER_MAX_THREAD_COUNT_SETTING, and asserting this does not affect CLUSTER_MAX_MERGE_COUNT_SETTING
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(CLUSTER_MAX_THREAD_COUNT_SETTING.getKey()))
            .get();

        assertEquals(200, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(3, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals("200", clusterSettings.get(CLUSTER_MAX_MERGE_COUNT_SETTING).toString());
        assertEquals("3", clusterSettings.get(CLUSTER_MAX_THREAD_COUNT_SETTING).toString());
    }

    /**
     * Tests that absolute defaults are calculated correctly when no cluster or index settings exist.
     * <p>
     * Expected behavior:
     * <pre>
     * MAX_THREAD_COUNT = max(1, min(4, cpuCores/2))
     * MAX_MERGE_COUNT = MAX_THREAD_COUNT + 5
     * </pre>
     */
    public void testAbsoluteDefaults() throws Exception {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());

        String indexName = "test-absolute-defaults";
        createIndex(indexName, indexSettings());
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);

        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        Index index = new Index(indexName, uuid);
        IndexService indexService = getIndexService(indicesService, index);

        int expectedThreadCount = 3;
        int expectedMergeCount = expectedThreadCount + 5;

        assertEquals(expectedMergeCount, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(expectedThreadCount, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
    }

    /**
     * Tests that cluster defaults override absolute defaults when set.
     * <p>
     * Scenario:
     * <pre>
     * 1. Set cluster defaults: thread_count=10, merge_count=20
     * 2. Create index with no explicit settings
     * 3. Index should use cluster defaults
     * </pre>
     */
    public void testClusterDefaults() throws Exception {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());

        // Set cluster defaults
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_MAX_THREAD_COUNT_SETTING.getKey(), 10).put(CLUSTER_MAX_MERGE_COUNT_SETTING.getKey(), 20)
            )
            .get();

        String indexName = "test-cluster-defaults";
        createIndex(indexName, indexSettings());
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);

        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        ClusterSettings clusterSettings = indicesService.clusterService().getClusterSettings();

        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService indexService = getIndexService(indicesService, new Index(indexName, uuid));

        assertEquals(20, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(10, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals("20", clusterSettings.get(CLUSTER_MAX_MERGE_COUNT_SETTING).toString());
        assertEquals("10", clusterSettings.get(CLUSTER_MAX_THREAD_COUNT_SETTING).toString());
    }

    /**
     * Tests setting only MAX_THREAD_COUNT at index level.
     * <p>
     * Expected behavior:
     * <pre>
     * Set: thread_count = 6
     * Result:
     *   thread_count = 6
     *   merge_count = 11 (auto-calculated: 6 + 5)
     * </pre>
     */
    public void testIndexLevelThreadCountOnly() throws Exception {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());

        String indexName = "test-thread-count-only";
        createIndex(
            indexName,
            Settings.builder().put(indexSettings()).put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), 6).build()
        );
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);

        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        Index index = new Index(indexName, uuid);
        IndexService indexService = getIndexService(indicesService, index);

        assertEquals(6, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(11, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount()); // 6 + 5
    }

    /**
     * Tests setting only MAX_MERGE_COUNT at index level.
     * <p>
     * Expected behavior:
     * <pre>
     * Set: merge_count = 15
     * Result:
     *   merge_count = 15
     *   thread_count = absolute default (NOT cluster default even if it exists)
     * </pre>
     */
    public void testIndexLevelMergeCountOnly() throws Exception {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());

        // Set cluster defaults to verify they're NOT used for thread_count
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_MAX_THREAD_COUNT_SETTING.getKey(), 99).put(CLUSTER_MAX_MERGE_COUNT_SETTING.getKey(), 100)
            )
            .get();

        String indexName = "test-merge-count-only";
        createIndex(
            indexName,
            Settings.builder().put(indexSettings()).put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 15).build()
        );
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);

        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        Index index = new Index(indexName, uuid);
        IndexService indexService = getIndexService(indicesService, index);

        int expectedThreadCount = 3;

        assertEquals(15, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(expectedThreadCount, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
    }

    /**
     * Tests setting both MAX_THREAD_COUNT and MAX_MERGE_COUNT at index level.
     * <p>
     * Expected behavior: Both values are used as-is (assuming they're valid).
     */
    public void testIndexLevelBothSet() throws Exception {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());

        String indexName = "test-both-set";
        createIndex(
            indexName,
            Settings.builder()
                .put(indexSettings())
                .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), 8)
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 20)
                .build()
        );
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);

        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        Index index = new Index(indexName, uuid);
        IndexService indexService = getIndexService(indicesService, index);

        assertEquals(8, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
    }

    /**
     * Tests removing ONE of TWO explicit index settings.
     * <p>
     * Scenario:
     * <pre>
     * 1. Set cluster defaults: thread_count=13, merge_count=18
     * 2. Create index with both settings: thread_count=6, merge_count=15
     * 3. Remove thread_count
     * 4. Result: thread_count falls back to ABSOLUTE default (not cluster default of 3)
     *           merge_count stays at 15
     * </pre>
     */
    public void testRemoveOneOfTwoSettings() throws Exception {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());

        // Set cluster defaults
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_MAX_THREAD_COUNT_SETTING.getKey(), 13).put(CLUSTER_MAX_MERGE_COUNT_SETTING.getKey(), 18)
            )
            .get();

        String indexName = "test-remove-one-of-two-1";
        createIndex(
            indexName,
            Settings.builder()
                .put(indexSettings())
                .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), 6)
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 15)
                .build()
        );
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);

        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        Index index = new Index(indexName, uuid);
        IndexService indexService = getIndexService(indicesService, index);

        // Verify initial state
        assertEquals(6, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(15, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());

        // Remove thread_count setting
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().putNull(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey()))
            .execute()
            .actionGet();

        int expectedThreadCount = 3;

        // thread_count should fall back to absolute default, NOT cluster default of 3
        assertEquals(expectedThreadCount, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        // merge_count should stay at index-level setting
        assertEquals(15, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());

        // now remove merge_count when both index level settings are present

        indexName = "test-remove-one-of-two-2";
        createIndex(
            indexName,
            Settings.builder()
                .put(indexSettings())
                .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), 6)
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 15)
                .build()
        );
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);

        getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();

        uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        index = new Index(indexName, uuid);
        indexService = getIndexService(indicesService, index);

        // Verify initial state
        assertEquals(6, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(15, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());

        // Remove thread_count setting
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().putNull(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey()))
            .execute()
            .actionGet();

        int expectedMergeCount = 11; // MAX_THREAD_COUNT(=6) + 5

        // merge_count should fall back to absolute default, NOT cluster default
        assertEquals(expectedMergeCount, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        // thread_count should stay at index-level setting
        assertEquals(6, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
    }

    /**
     * Tests removing BOTH explicit index settings (when both were set).
     * <p>
     * Scenario:
     * <pre>
     * 1. Set cluster defaults: thread_count=60, merge_count=80
     * 2. Create index with both settings: thread_count=6, merge_count=15
     * 3. Remove BOTH settings
     * 4. Result: Both fall back to cluster defaults (thread_count=60, merge_count=80)
     * </pre>
     */
    public void testRemoveAllSettings() throws Exception {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());

        // Set cluster defaults
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_MAX_THREAD_COUNT_SETTING.getKey(), 60).put(CLUSTER_MAX_MERGE_COUNT_SETTING.getKey(), 80)
            )
            .get();

        String indexName = "test-remove-all";
        createIndex(
            indexName,
            Settings.builder()
                .put(indexSettings())
                .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), 6)
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 15)
                .build()
        );
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);

        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        ClusterSettings clusterSettings = indicesService.clusterService().getClusterSettings();
        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        Index index = new Index(indexName, uuid);
        IndexService indexService = getIndexService(indicesService, index);

        // Verify initial state
        assertEquals(6, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(15, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());

        // Remove both settings
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(
                Settings.builder()
                    .putNull(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey())
                    .putNull(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey())
            )
            .execute()
            .actionGet();

        // Both should fall back to cluster defaults
        assertEquals(60, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(80, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals("60", clusterSettings.get(CLUSTER_MAX_THREAD_COUNT_SETTING).toString());
        assertEquals("80", clusterSettings.get(CLUSTER_MAX_MERGE_COUNT_SETTING).toString());
    }

    /**
     * Tests removing the ONLY explicit index setting (the other was auto-calculated).
     * <p>
     * Scenario:
     * <pre>
     * 1. Set cluster defaults: thread_count=60, merge_count=80
     * 2. Create index with only thread_count=6 (merge_count auto-calculates to 11)
     * 3. Remove thread_count
     * 4. Result: Both fall back to cluster defaults because merge_count wasn't explicitly set
     * </pre>
     **/
    public void testRemoveOnlyExplicitSetting() throws Exception {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());

        String indexName = "test-remove-only-explicit";
        createIndex(indexName, indexSettings());
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);

        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        ClusterSettings clusterSettings = indicesService.clusterService().getClusterSettings();

        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        Index index = new Index(indexName, uuid);
        IndexService indexService = getIndexService(indicesService, index);

        int expectedThreadCount = 3;
        int expectedMergeCount = expectedThreadCount + 5;

        assertEquals(expectedMergeCount, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(expectedThreadCount, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());

        // Set cluster defaults
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_MAX_MERGE_COUNT_SETTING.getKey(), 80).put(CLUSTER_MAX_THREAD_COUNT_SETTING.getKey(), 60)
            )
            .get();

        // Index should immediately pick up cluster defaults
        assertEquals(80, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(60, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals("80", clusterSettings.get(CLUSTER_MAX_MERGE_COUNT_SETTING).toString());
        assertEquals("60", clusterSettings.get(CLUSTER_MAX_THREAD_COUNT_SETTING).toString());

        // Set only merge_count at index level
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 100))
            .execute()
            .actionGet();

        // merge_count should be 100, thread_count falls back to absolute default
        assertEquals(100, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(expectedThreadCount, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());

        // Remove the only explicit setting (merge_count)
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().putNull(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey()))
            .execute()
            .actionGet();

        // Both should fall back to cluster defaults
        assertEquals(80, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(60, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals("80", clusterSettings.get(CLUSTER_MAX_MERGE_COUNT_SETTING).toString());
        assertEquals("60", clusterSettings.get(CLUSTER_MAX_THREAD_COUNT_SETTING).toString());
    }

    /**
     * Tests that invalid configurations (thread_count > merge_count) are rejected.
     * <p>
     * The golden rule: {@code MAX_THREAD_COUNT must always be <= MAX_MERGE_COUNT}
     * </p>
     */
    public void testInvalidConfiguration() {
        String indexName = "test-invalid-config";

        // Try to create index with thread_count > merge_count (should fail)
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex(
                indexName,
                Settings.builder()
                    .put(indexSettings())
                    .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), 10)
                    .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 5)
                    .build()
            )
        );

        assertTrue(exception.getMessage().contains("maxThreadCount (= 10) should be <= maxMergeCount (= 5)"));
    }

    /**
     * Tests that cluster defaults apply to indices created BEFORE the cluster setting was set.
     * <p>
     * Scenario:
     * <pre>
     * 1. Create index (uses absolute defaults)
     * 2. Set cluster defaults
     * 3. Existing index should immediately pick up cluster defaults
     * </pre>
     */
    public void testClusterDefaultsApplyToExistingIndices() throws Exception {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());

        String indexName = "test-existing-index";
        createIndex(indexName, indexSettings());
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);

        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        Index index = new Index(indexName, uuid);
        IndexService indexService = getIndexService(indicesService, index);

        // Verify using absolute defaults initially
        int expectedThreadCount = 3;
        int expectedMergeCount = expectedThreadCount + 5;
        assertEquals(expectedMergeCount, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(expectedThreadCount, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());

        // Now set cluster defaults
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_MAX_THREAD_COUNT_SETTING.getKey(), 12).put(CLUSTER_MAX_MERGE_COUNT_SETTING.getKey(), 25)
            )
            .get();

        // Existing index should immediately pick up cluster defaults
        assertEquals(25, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(12, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
    }

    /**
     * Tests that updating cluster defaults affects all indices without explicit settings.
     * <p>
     * Scenario:
     * <pre>
     * 1. Set cluster defaults: thread_count=10, merge_count=20
     * 2. Create two indices: one with explicit settings, one without
     * 3. Update cluster defaults: thread_count=15, merge_count=30
     * 4. Index without explicit settings should use new cluster defaults
     * 5. Index with explicit settings should remain unchanged
     * </pre>
     */
    public void testClusterDefaultUpdate() throws ExecutionException, InterruptedException {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());

        // Set initial cluster defaults
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_MAX_THREAD_COUNT_SETTING.getKey(), 10).put(CLUSTER_MAX_MERGE_COUNT_SETTING.getKey(), 20)
            )
            .get();

        // Create index without explicit settings
        String indexWithoutSettings = "test-without-settings";
        createIndex(indexWithoutSettings, indexSettings());
        ensureYellowAndNoInitializingShards(indexWithoutSettings);
        ensureGreen(indexWithoutSettings);

        // Create index with explicit settings
        String indexWithSettings = "test-with-settings";
        createIndex(
            indexWithSettings,
            Settings.builder()
                .put(indexSettings())
                .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), 5)
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 12)
                .build()
        );
        ensureYellowAndNoInitializingShards(indexWithSettings);
        ensureGreen(indexWithSettings);

        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));

        String uuidWithout = getIndexResponse.getSettings().get(indexWithoutSettings).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService indexServiceWithout = indicesService.indexService(new Index(indexWithoutSettings, uuidWithout));

        String uuidWith = getIndexResponse.getSettings().get(indexWithSettings).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService indexServiceWith = indicesService.indexService(new Index(indexWithSettings, uuidWith));

        // Verify initial state
        assertEquals(20, indexServiceWithout.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(10, indexServiceWithout.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(12, indexServiceWith.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(5, indexServiceWith.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());

        // Update cluster defaults
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_MAX_THREAD_COUNT_SETTING.getKey(), 15).put(CLUSTER_MAX_MERGE_COUNT_SETTING.getKey(), 30)
            )
            .get();

        // Index without explicit settings should update
        assertEquals(30, indexServiceWithout.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(15, indexServiceWithout.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());

        // Index with explicit settings should remain unchanged
        assertEquals(12, indexServiceWith.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(5, indexServiceWith.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
    }

    /**
     * This demonstrates the full lifecycle: cluster defaults → index override → removal → fallback.
     */
    public void testFullLifecycle() throws Exception {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());

        String indexName = "log-myindex-5";
        createIndex(indexName, indexSettings());
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        ClusterSettings clusterSettings = indicesService.clusterService().getClusterSettings();

        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        Index index = new Index(indexName, uuid);
        IndexService indexService = getIndexService(indicesService, index);

        int expectedThreadCount = 3;
        int expectedMergeCount = expectedThreadCount + 5;

        assertEquals(expectedMergeCount, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(expectedThreadCount, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(true, indexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());

        // Updating cluster defaults: CLUSTER_MAX_THREAD_COUNT_SETTING=60 and CLUSTER_MAX_MERGE_COUNT_SETTING=80
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_MAX_MERGE_COUNT_SETTING.getKey(), 80).put(CLUSTER_MAX_THREAD_COUNT_SETTING.getKey(), 60)
            )
            .get();

        assertEquals(80, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(60, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals("80", clusterSettings.get(CLUSTER_MAX_MERGE_COUNT_SETTING).toString());
        assertEquals("60", clusterSettings.get(CLUSTER_MAX_THREAD_COUNT_SETTING).toString());

        UpdateSettingsRequestBuilder builder = client().admin().indices().prepareUpdateSettings(indexName);
        builder.setSettings(Settings.builder().put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 100).build());
        builder.execute().actionGet();

        assertEquals(100, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(expectedThreadCount, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals("80", clusterSettings.get(CLUSTER_MAX_MERGE_COUNT_SETTING).toString());
        assertEquals("60", clusterSettings.get(CLUSTER_MAX_THREAD_COUNT_SETTING).toString());

        builder = client().admin().indices().prepareUpdateSettings(indexName);
        builder.setSettings(Settings.builder().putNull(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey()).build());
        builder.execute().actionGet();

        assertEquals(80, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(60, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals("80", clusterSettings.get(CLUSTER_MAX_MERGE_COUNT_SETTING).toString());
        assertEquals("60", clusterSettings.get(CLUSTER_MAX_THREAD_COUNT_SETTING).toString());
    }

    private IndexService getIndexService(IndicesService indicesService, Index index) throws Exception {
        // We wait for the index to become available on the node.
        assertBusy(() -> assertNotNull("IndexService should not be null", indicesService.indexService(index)), 120, TimeUnit.SECONDS);
        return indicesService.indexService(index);
    }
}
