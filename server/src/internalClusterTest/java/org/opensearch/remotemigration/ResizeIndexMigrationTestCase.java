/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.shrink.ResizeType;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ResizeIndexMigrationTestCase extends MigrationBaseTestCase {
    private static final String TEST_INDEX = "test_index";
    private final static String REMOTE_STORE_DIRECTION = "remote_store";
    private final static String DOC_REP_DIRECTION = "docrep";
    private final static String MIXED_MODE = "mixed";

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL, "true").build();
    }

    /*
    * This test will verify the resize request failure, when cluster mode is mixed
    * and index is on DocRep node, and migration to remote store is in progress.
    * */
    public void testFailResizeIndexWhileDocRepToRemoteStoreMigration() throws Exception {
        setAddRemote(false);
        // create a docrep cluster
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().validateClusterFormed();

        // add a non-remote node
        String nonRemoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();

        logger.info("-->Create index on non-remote node and SETTING_REMOTE_STORE_ENABLED is false. Resize should not happen");
        Settings.Builder builder = Settings.builder().put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);
        internalCluster().client()
            .admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                builder.put("index.number_of_shards", 10)
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
        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();

        // set remote store migration direction
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), REMOTE_STORE_DIRECTION));
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        ResizeType resizeType;
        int resizeShardsNum;
        String cause;
        switch (randomIntBetween(0, 2)) {
            case 0:
                resizeType = ResizeType.SHRINK;
                resizeShardsNum = 5;
                cause = "shrink_index";
                break;
            case 1:
                resizeType = ResizeType.SPLIT;
                resizeShardsNum = 20;
                cause = "split_index";
                break;
            default:
                resizeType = ResizeType.CLONE;
                resizeShardsNum = 10;
                cause = "clone_index";
        }

        internalCluster().client()
            .admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(Settings.builder().put("index.blocks.write", true))
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        Settings.Builder resizeSettingsBuilder = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", resizeShardsNum)
            .putNull("index.blocks.write");

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> internalCluster().client()
                .admin()
                .indices()
                .prepareResizeIndex(TEST_INDEX, "first_split")
                .setResizeType(resizeType)
                .setSettings(resizeSettingsBuilder.build())
                .get()
        );
        assertEquals(
            ex.getMessage(),
            "Index " + resizeType + " is not allowed as remote migration mode is mixed" + " and index is remote store disabled"
        );
    }

    /*
     * This test will verify the resize request failure, when cluster mode is mixed
     * and index is on Remote Store node, and migration to DocRep node is in progress.
     * */
    public void testFailResizeIndexWhileRemoteStoreToDocRepMigration() throws Exception {
        // creates a remote cluster
        setAddRemote(true);
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().validateClusterFormed();

        // add a remote node
        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();

        logger.info("--> Create index on remote node and SETTING_REMOTE_STORE_ENABLED is true. Resize should not happen");
        Settings.Builder builder = Settings.builder().put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);
        internalCluster().client()
            .admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                builder.put("index.number_of_shards", 10)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.include._name", remoteNodeName)
            )
            .setWaitForActiveShards(ActiveShardCount.ALL)
            .execute()
            .actionGet();

        // set mixed mode
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), MIXED_MODE));
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // add a non-remote node
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();

        // set docrep migration direction
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), DOC_REP_DIRECTION));
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        ResizeType resizeType;
        int resizeShardsNum;
        String cause;
        switch (randomIntBetween(0, 2)) {
            case 0:
                resizeType = ResizeType.SHRINK;
                resizeShardsNum = 5;
                cause = "shrink_index";
                break;
            case 1:
                resizeType = ResizeType.SPLIT;
                resizeShardsNum = 20;
                cause = "split_index";
                break;
            default:
                resizeType = ResizeType.CLONE;
                resizeShardsNum = 10;
                cause = "clone_index";
        }

        internalCluster().client()
            .admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(Settings.builder().put("index.blocks.write", true))
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        Settings.Builder resizeSettingsBuilder = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", resizeShardsNum)
            .putNull("index.blocks.write");

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> internalCluster().client()
                .admin()
                .indices()
                .prepareResizeIndex(TEST_INDEX, "first_split")
                .setResizeType(resizeType)
                .setSettings(resizeSettingsBuilder.build())
                .get()
        );
        assertEquals(
            ex.getMessage(),
            "Index " + resizeType + " is not allowed as remote migration mode is mixed" + " and index is remote store enabled"
        );
    }
}
