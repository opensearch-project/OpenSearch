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
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.opensearch.indices.IndicesService.CLUSTER_DEFAULT_INDEX_MAX_MERGE_AT_ONCE_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class ClusterMaxMergesAtOnceIT extends AbstractSnapshotIntegTestCase {

    @Override
    public Settings indexSettings() {
        return Settings.builder().put(super.indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().startClusterManagerOnlyNode();
    }

    public void testClusterLevelDefaultUpdatesMergePolicy() throws ExecutionException, InterruptedException {
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
        assertEquals(30, ((OpenSearchTieredMergePolicy) indexService.getIndexSettings().getMergePolicy(true)).getMaxMergeAtOnce());

        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_DEFAULT_INDEX_MAX_MERGE_AT_ONCE_SETTING.getKey(), 20))
            .get();

        indexName = "log-myindex-2";
        createIndex(indexName);
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
        getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
        indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService secondIndexService = indicesService.indexService(new Index(indexName, uuid));
        assertEquals(20, ((OpenSearchTieredMergePolicy) indexService.getIndexSettings().getMergePolicy(true)).getMaxMergeAtOnce());
        assertEquals(20, ((OpenSearchTieredMergePolicy) secondIndexService.getIndexSettings().getMergePolicy(true)).getMaxMergeAtOnce());

        // Create index with index level override in settings
        indexName = "log-myindex-3";
        createIndex(
            indexName,
            Settings.builder().put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), 15).build()
        );
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
        getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
        indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService thirdIndexService = indicesService.indexService(new Index(indexName, uuid));
        assertEquals(15, ((OpenSearchTieredMergePolicy) thirdIndexService.getIndexSettings().getMergePolicy(true)).getMaxMergeAtOnce());

        // changing cluster level default should only affect indices without index level override
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_DEFAULT_INDEX_MAX_MERGE_AT_ONCE_SETTING.getKey(), 35))
            .get();
        assertEquals(35, ((OpenSearchTieredMergePolicy) indexService.getIndexSettings().getMergePolicy(true)).getMaxMergeAtOnce());
        assertEquals(35, ((OpenSearchTieredMergePolicy) secondIndexService.getIndexSettings().getMergePolicy(true)).getMaxMergeAtOnce());
        assertEquals(15, ((OpenSearchTieredMergePolicy) thirdIndexService.getIndexSettings().getMergePolicy(true)).getMaxMergeAtOnce());

        // removing index level override should pick up the cluster level default
        UpdateSettingsRequestBuilder builder = client().admin().indices().prepareUpdateSettings(indexName);
        builder.setSettings(
            Settings.builder().putNull(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey()).build()
        );
        builder.execute().actionGet();

        assertEquals(35, ((OpenSearchTieredMergePolicy) indexService.getIndexSettings().getMergePolicy(true)).getMaxMergeAtOnce());
        assertEquals(35, ((OpenSearchTieredMergePolicy) secondIndexService.getIndexSettings().getMergePolicy(true)).getMaxMergeAtOnce());
        assertEquals(35, ((OpenSearchTieredMergePolicy) thirdIndexService.getIndexSettings().getMergePolicy(true)).getMaxMergeAtOnce());

        // update index level setting to override cluster level default
        builder = client().admin().indices().prepareUpdateSettings(indexName);
        builder.setSettings(
            Settings.builder().put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), 17).build()
        );
        builder.execute().actionGet();

        assertEquals(35, ((OpenSearchTieredMergePolicy) indexService.getIndexSettings().getMergePolicy(true)).getMaxMergeAtOnce());
        assertEquals(35, ((OpenSearchTieredMergePolicy) secondIndexService.getIndexSettings().getMergePolicy(true)).getMaxMergeAtOnce());
        assertEquals(17, ((OpenSearchTieredMergePolicy) thirdIndexService.getIndexSettings().getMergePolicy(true)).getMaxMergeAtOnce());
    }
}
