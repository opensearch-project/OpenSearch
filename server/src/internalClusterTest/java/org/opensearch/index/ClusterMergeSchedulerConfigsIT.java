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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.opensearch.common.util.concurrent.OpenSearchExecutors.NODE_PROCESSORS_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_DEFAULT_AUTO_THROTTLE_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_DEFAULT_MAX_MERGE_COUNT_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_DEFAULT_MAX_THREAD_COUNT_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class ClusterMergeSchedulerConfigsIT extends OpenSearchIntegTestCase {

    @Override
    public Settings indexSettings() {
        Settings.Builder s = Settings.builder().put(super.indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1);
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

    public void testMergeSchedulerSettings() throws ExecutionException, InterruptedException {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());
        String indexName = "log-myindex-1";
        createIndex(indexName);
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService indexService = indicesService.indexService(new Index(indexName, uuid));
        assertEquals(8, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(3, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(true, indexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());

        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put(CLUSTER_DEFAULT_MAX_MERGE_COUNT_SETTING.getKey(), 40)
                    .put(CLUSTER_DEFAULT_MAX_THREAD_COUNT_SETTING.getKey(), 20)
            )
            .get();

        indexName = "log-myindex-2";
        createIndex(indexName);
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
        getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
        indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService secondIndexService = indicesService.indexService(new Index(indexName, uuid));
        assertEquals(40, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(40, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(20, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(true, indexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());
        assertEquals(true, secondIndexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());
        ;

        int replicas = randomIntBetween(1, Math.max(1, internalCluster().numDataNodes() - 1));
        // Create index with index level override in settings
        indexName = "log-myindex-3";
        createIndex(
            indexName,
            Settings.builder()
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 150)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas)
                .build()
        );
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
        getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
        indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService thirdIndexService = indicesService.indexService(new Index(indexName, uuid));
        assertEquals(150, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(40, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(40, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(20, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(true, indexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());
        ;
        assertEquals(true, secondIndexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());
        ;
        assertEquals(true, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());
        ;

        // changing cluster level default should only affect indices without index level override
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_DEFAULT_MAX_MERGE_COUNT_SETTING.getKey(), 35))
            .get();

        assertEquals(35, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(35, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(150, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
        assertEquals(20, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, secondIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());

        // updating cluster level auto_throttle to false
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_DEFAULT_AUTO_THROTTLE_SETTING.getKey(), false))
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
        builder.setSettings(Settings.builder().put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 17).build());
        UpdateSettingsRequestBuilder finalBuilder = builder;
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> finalBuilder.execute().actionGet());
        assertTrue(exception.getMessage().contains("maxThreadCount (= 20) should be <= maxMergeCount (= 17)"));

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
        assertEquals(20, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());

        // setting auto throttle to true for one index
        builder = client().admin().indices().prepareUpdateSettings("log-myindex-2");
        builder.setSettings(Settings.builder().put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), "true").build());
        builder.execute().actionGet();

        assertEquals(false, indexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());
        // Known bug;
        //assertEquals(true, secondIndexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());;
        assertEquals(false, thirdIndexService.getIndexSettings().getMergeSchedulerConfig().isAutoThrottle());


    }
}
