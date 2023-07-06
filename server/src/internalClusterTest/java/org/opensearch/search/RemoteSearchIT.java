/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.junit.After;
import org.junit.Before;
import org.opensearch.action.admin.indices.close.CloseIndexRequest;
import org.opensearch.action.admin.indices.open.OpenIndexRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;

import static org.opensearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class RemoteSearchIT extends AbstractSnapshotIntegTestCase {

    private static final String REPOSITORY_NAME = "test-remote-store-repo";

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.REMOTE_STORE, "true").build();
    }

    @Before
    public void setup() {
        internalCluster().startClusterManagerOnlyNode();
        Path absolutePath = randomRepoPath().toAbsolutePath();
        assertAcked(
            clusterAdmin().preparePutRepository(REPOSITORY_NAME).setType("fs").setSettings(Settings.builder().put("location", absolutePath))
        );
    }

    @After
    public void teardown() {
        assertAcked(clusterAdmin().prepareDeleteRepository(REPOSITORY_NAME));
    }

    private Settings remoteStoreIndexSettings(int numberOfReplicas) {
        return Settings.builder()
            .put(super.indexSettings())
            .put("index.refresh_interval", "300s")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_STORE_REPOSITORY, REPOSITORY_NAME)
            .build();
    }

    private Settings remoteTranslogIndexSettings(int numberOfReplicas) {
        return Settings.builder()
            .put(remoteStoreIndexSettings(numberOfReplicas))
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, REPOSITORY_NAME)
            .build();
    }

    public void testRemoteSearchIndex() throws Exception {
        final String indexName = "test-idx-1";
        final int numReplicasIndex = randomIntBetween(0, 3);
        final int numOfDocs = 100;

        // Spin up node having search/data roles
        internalCluster().ensureAtLeastNumSearchAndDataNodes(numReplicasIndex + 1);

        // Create index with remote translog index settings
        createIndex(indexName, Settings.builder()
            .put(remoteTranslogIndexSettings(numReplicasIndex))
            .build());
        ensureGreen();

        // Index some documents
        indexRandomDocs(indexName, numOfDocs);
        ensureGreen();
        // Search the documents on the index
        assertDocCount(indexName, 100L);

        // Close the index
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(indexName);
        client().admin().indices().close(closeIndexRequest).actionGet();

        // Apply the remote search setting to the index
        client().admin().indices().updateSettings(new UpdateSettingsRequest(Settings.builder()
            .put(INDEX_STORE_TYPE_SETTING.getKey(), "remote_search")
            .build()
        )).actionGet();

        // Open the index back
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(indexName);
        client().admin().indices().open(openIndexRequest).actionGet();

        // Perform search on the index again
        assertDocCount(indexName, 100L);
    }
}
