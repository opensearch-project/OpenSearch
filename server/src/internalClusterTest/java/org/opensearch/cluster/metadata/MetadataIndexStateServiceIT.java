/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_READ_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MetadataIndexStateServiceIT extends RemoteStoreBaseIntegTestCase {

    private static final String TEST_INDEX = "test_open_close_index";

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL, Boolean.TRUE).build();
    }

    public void testIndexCloseAndOpen() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        Settings specificSettings = Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 1).build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        for (int i = 0; i < 10; i++) {
            client().prepareIndex(TEST_INDEX)
                .setId(Integer.toString(i))
                .setSource("field1", "value" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        assertAcked(client().admin().indices().prepareClose(TEST_INDEX).get());

        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
        IndexMetadata indexMetadata = clusterStateResponse.getState().metadata().index(TEST_INDEX);
        assertEquals(IndexMetadata.State.CLOSE, indexMetadata.getState());

        OpenIndexResponse openIndexResponse = client().admin().indices().prepareOpen(TEST_INDEX).get();

        assertTrue("Open operation should be acknowledged", openIndexResponse.isAcknowledged());
        assertTrue("Open operation shards should be acknowledged", openIndexResponse.isShardsAcknowledged());

        clusterStateResponse = client().admin().cluster().prepareState().get();
        indexMetadata = clusterStateResponse.getState().metadata().index(TEST_INDEX);
        assertEquals(IndexMetadata.State.OPEN, indexMetadata.getState());

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(searchResponse, 10);
        }, 30, TimeUnit.SECONDS);
    }

    public void testIndexCloseAndOpenWithSearchOnlyMode() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        internalCluster().startSearchOnlyNodes(1);

        Settings specificSettings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_READ_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        for (int i = 0; i < 10; i++) {
            client().prepareIndex(TEST_INDEX)
                .setId(Integer.toString(i))
                .setSource("field1", "value" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX, true).get());
        ensureGreen(TEST_INDEX);

        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(TEST_INDEX).get();
        assertTrue(settingsResponse.getSetting(TEST_INDEX, IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey()).equals("true"));

        assertAcked(client().admin().indices().prepareClose(TEST_INDEX).get());

        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
        IndexMetadata indexMetadata = clusterStateResponse.getState().metadata().index(TEST_INDEX);
        assertEquals(IndexMetadata.State.CLOSE, indexMetadata.getState());

        OpenIndexResponse openIndexResponse = client().admin().indices().prepareOpen(TEST_INDEX).get();

        assertTrue("Open operation should be acknowledged", openIndexResponse.isAcknowledged());
        assertTrue("Open operation shards should be acknowledged", openIndexResponse.isShardsAcknowledged());

        clusterStateResponse = client().admin().cluster().prepareState().get();
        indexMetadata = clusterStateResponse.getState().metadata().index(TEST_INDEX);
        assertEquals(IndexMetadata.State.OPEN, indexMetadata.getState());

        settingsResponse = client().admin().indices().prepareGetSettings(TEST_INDEX).get();
        assertTrue(settingsResponse.getSetting(TEST_INDEX, IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey()).equals("true"));

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(searchResponse, 10);
        }, 30, TimeUnit.SECONDS);
    }
}
