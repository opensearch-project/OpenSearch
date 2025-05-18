/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.close.CloseIndexRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.ExecutionException;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CloseIndexMigrationTestCase extends MigrationBaseTestCase {
    private static final String TEST_INDEX = "ind";
    private final static String REMOTE_STORE_DIRECTION = "remote_store";
    private final static String MIXED_MODE = "mixed";

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL, "true").build();
    }

    /*
     * This test will verify the close request failure, when cluster mode is mixed
     * and migration to remote store is in progress.
     * */
    public void testFailCloseIndexWhileDocRepToRemoteStoreMigration() throws IllegalStateException {
        setAddRemote(false);
        // create a docrep cluster
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().validateClusterFormed();

        // add a non-remote node
        String nonRemoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();

        // create index in cluster
        Settings.Builder builder = Settings.builder().put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);
        internalCluster().client()
            .admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                builder.put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.include._name", nonRemoteNodeName)
            )
            .setWaitForActiveShards(ActiveShardCount.ALL)
            .execute()
            .actionGet();

        // set mixed mode
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), MIXED_MODE));
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // add a remote node
        addRemote = true;
        internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();

        // set remote store migration direction
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), REMOTE_STORE_DIRECTION));
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        ensureGreen(TEST_INDEX);

        // Try closing the index, expecting failure.
        ExecutionException ex = expectThrows(
            ExecutionException.class,
            () -> internalCluster().client().admin().indices().close(new CloseIndexRequest(TEST_INDEX)).get()

        );
        assertEquals("Cannot close index while remote migration is ongoing", ex.getCause().getMessage());
    }
}
