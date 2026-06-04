/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MetadataIndexStateServiceIT extends RemoteStoreBaseIntegTestCase {

    private static final String TEST_INDEX = "test_open_close_index";

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
        assertEquals(
            IndexMetadata.State.CLOSE,
            client().admin().cluster().prepareState().get().getState().metadata().index(TEST_INDEX).getState()
        );

        assertAcked(client().admin().indices().prepareOpen(TEST_INDEX).get());
        ensureGreen(TEST_INDEX);

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
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
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

        assertTrue(
            client().admin()
                .indices()
                .prepareGetSettings(TEST_INDEX)
                .get()
                .getSetting(TEST_INDEX, IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey())
                .equals("true")
        );

        assertAcked(client().admin().indices().prepareClose(TEST_INDEX).get());
        assertEquals(
            IndexMetadata.State.CLOSE,
            client().admin().cluster().prepareState().get().getState().metadata().index(TEST_INDEX).getState()
        );

        assertAcked(client().admin().indices().prepareOpen(TEST_INDEX).get());
        ensureGreen(TEST_INDEX);

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(searchResponse, 10);
        }, 30, TimeUnit.SECONDS);
    }
}
