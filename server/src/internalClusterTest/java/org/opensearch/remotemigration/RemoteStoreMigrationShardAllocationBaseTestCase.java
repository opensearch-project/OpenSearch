/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotState;

import java.util.Map;
import java.util.Optional;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.index.IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class RemoteStoreMigrationShardAllocationBaseTestCase extends MigrationBaseTestCase {
    protected static final String TEST_INDEX = "test_index";
    protected static final String NAME = "remote_store_migration";

    protected final ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();

    // set the compatibility mode of cluster [strict, mixed]
    protected void setClusterMode(String mode) {
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), mode));
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    // set the migration direction for cluster [remote_store, docrep, none]
    public void setDirection(String direction) {
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), direction));
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    // verify that the given nodeName exists in cluster
    protected DiscoveryNode assertNodeInCluster(String nodeName) {
        Map<String, DiscoveryNode> nodes = internalCluster().client().admin().cluster().prepareState().get().getState().nodes().getNodes();
        DiscoveryNode discoveryNode = null;
        for (Map.Entry<String, DiscoveryNode> entry : nodes.entrySet()) {
            DiscoveryNode node = entry.getValue();
            if (node.getName().equals(nodeName)) {
                discoveryNode = node;
                break;
            }
        }
        assertNotNull(discoveryNode);
        return discoveryNode;
    }

    // returns a comma-separated list of node names excluding `except`
    protected String allNodesExcept(String except) {
        StringBuilder exclude = new StringBuilder();
        DiscoveryNodes allNodes = internalCluster().client().admin().cluster().prepareState().get().getState().nodes();
        for (DiscoveryNode node : allNodes) {
            if (node.getName().equals(except) == false) {
                exclude.append(node.getName()).append(",");
            }
        }
        return exclude.toString();
    }

    // create a new test index
    protected void prepareIndexWithoutReplica(Optional<String> name) {
        String indexName = name.orElse(TEST_INDEX);
        internalCluster().client()
            .admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(null))
            )
            .execute()
            .actionGet();
    }

    protected ShardRouting getShardRouting(boolean isPrimary) {
        IndexShardRoutingTable table = internalCluster().client()
            .admin()
            .cluster()
            .prepareState()
            .execute()
            .actionGet()
            .getState()
            .getRoutingTable()
            .index(TEST_INDEX)
            .shard(0);
        return (isPrimary ? table.primaryShard() : table.replicaShards().get(0));
    }

    // create a snapshot
    public static SnapshotInfo createSnapshot(String snapshotRepoName, String snapshotName, String... indices) {
        SnapshotInfo snapshotInfo = internalCluster().client()
            .admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName)
            .setIndices(indices)
            .setWaitForCompletion(true)
            .get()
            .getSnapshotInfo();

        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertTrue(snapshotInfo.successfulShards() > 0);
        assertEquals(0, snapshotInfo.failedShards());
        return snapshotInfo;
    }

    // create new index
    public static void createIndex(String indexName, int replicaCount) {
        assertAcked(
            internalCluster().client()
                .admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicaCount)
                        .build()
                )
                .get()
        );
    }

    // restore indices from a snapshot
    public static RestoreSnapshotResponse restoreSnapshot(String snapshotRepoName, String snapshotName, String restoredIndexName) {
        RestoreSnapshotResponse restoreSnapshotResponse = internalCluster().client()
            .admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName)
            .setWaitForCompletion(false)
            .setIndices(TEST_INDEX)
            .setRenamePattern(TEST_INDEX)
            .setRenameReplacement(restoredIndexName)
            .get();
        assertEquals(restoreSnapshotResponse.status(), RestStatus.ACCEPTED);
        return restoreSnapshotResponse;
    }

    // verify that the created index is not remote store backed
    public static void assertNonRemoteStoreBackedIndex(String indexName) {
        Settings indexSettings = internalCluster().client()
            .admin()
            .indices()
            .prepareGetIndex()
            .execute()
            .actionGet()
            .getSettings()
            .get(indexName);
        assertEquals(ReplicationType.DOCUMENT.toString(), indexSettings.get(SETTING_REPLICATION_TYPE));
        assertNull(indexSettings.get(SETTING_REMOTE_STORE_ENABLED));
        assertNull(indexSettings.get(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertNull(indexSettings.get(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY));
    }

    // verify that the created index is remote store backed
    public static void assertRemoteStoreBackedIndex(String indexName) {
        Settings indexSettings = internalCluster().client()
            .admin()
            .indices()
            .prepareGetIndex()
            .execute()
            .actionGet()
            .getSettings()
            .get(indexName);
        assertEquals(ReplicationType.SEGMENT.toString(), indexSettings.get(SETTING_REPLICATION_TYPE));
        assertEquals("true", indexSettings.get(SETTING_REMOTE_STORE_ENABLED));
        assertEquals(REPOSITORY_NAME, indexSettings.get(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertEquals(REPOSITORY_2_NAME, indexSettings.get(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY));
        assertEquals(
            IndexSettings.DEFAULT_REMOTE_TRANSLOG_BUFFER_INTERVAL,
            INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(indexSettings)
        );
    }
}
