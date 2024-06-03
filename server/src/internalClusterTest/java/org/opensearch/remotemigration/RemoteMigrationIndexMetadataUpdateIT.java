/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.index.remote.RemoteIndexPath;
import org.opensearch.index.remote.RemoteIndexPathUploader;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteMigrationIndexMetadataUpdateIT extends MigrationBaseTestCase {
    /**
     * Scenario:
     * Performs a blue/green type migration from docrep to remote enabled cluster.
     * Asserts that remote based index settings are applied after all shards move over
     */
    public void testIndexSettingsUpdateAfterIndexMovedToRemoteThroughAllocationExclude() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 2 docrep nodes");
        addRemote = false;
        internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "docrep").build());
        internalCluster().validateClusterFormed();

        logger.info("---> Creates an index with 1 primary and 1 replica");
        String indexName = "migration-index-allocation-exclude";
        Settings oneReplica = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        logger.info("---> Asserts index still has docrep index settings");
        createIndexAndAssertDocrepProperties(indexName, oneReplica);

        logger.info("---> Start indexing in parallel thread");
        AsyncIndexingService asyncIndexingService = new AsyncIndexingService(indexName);
        asyncIndexingService.startIndexing();
        initDocRepToRemoteMigration();

        logger.info("---> Adding 2 remote enabled nodes to the cluster");
        addRemote = true;
        internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "remote").build());
        internalCluster().validateClusterFormed();

        logger.info("---> Excluding docrep nodes from allocation");
        excludeNodeSet("type", "docrep");
        waitForRelocation();
        waitNoPendingTasksOnAll();

        logger.info("---> Stop indexing and assert remote enabled index settings have been applied");
        asyncIndexingService.stopIndexing();
        assertRemoteProperties(indexName);
    }

    /**
     * Scenario:
     * Performs a manual _cluster/reroute to move shards from docrep to remote enabled nodes.
     * Asserts that remote based index settings are only applied for indices whose shards
     * have completely moved over to remote enabled nodes
     */
    public void testIndexSettingsUpdateAfterIndexMovedToRemoteThroughManualReroute() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 2 docrep nodes");
        List<String> docrepNodeNames = internalCluster().startDataOnlyNodes(2);
        internalCluster().validateClusterFormed();

        logger.info("---> Creating 2 indices with 1 primary and 1 replica");
        String indexName1 = "migration-index-manual-reroute-1";
        String indexName2 = "migration-index-manual-reroute-2";
        Settings oneReplica = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        createIndexAndAssertDocrepProperties(indexName1, oneReplica);
        createIndexAndAssertDocrepProperties(indexName2, oneReplica);

        logger.info("---> Starting parallel indexing on both indices");
        AsyncIndexingService indexOne = new AsyncIndexingService(indexName1);
        indexOne.startIndexing();

        AsyncIndexingService indexTwo = new AsyncIndexingService(indexName2);
        indexTwo.startIndexing();

        logger.info(
            "---> Stopping shard rebalancing to ensure shards do not automatically move over to newer nodes after they are launched"
        );
        stopShardRebalancing();

        logger.info("---> Starting 2 remote store enabled nodes");
        initDocRepToRemoteMigration();
        addRemote = true;
        List<String> remoteNodeNames = internalCluster().startDataOnlyNodes(2);
        internalCluster().validateClusterFormed();

        String primaryNode = primaryNodeName(indexName1);
        String replicaNode = docrepNodeNames.stream()
            .filter(nodeName -> nodeName.equals(primaryNodeName(indexName1)) == false)
            .collect(Collectors.toList())
            .get(0);

        logger.info("---> Moving over both shard copies for the first index to remote enabled nodes");
        assertAcked(
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(indexName1, 0, primaryNode, remoteNodeNames.get(0)))
                .execute()
                .actionGet()
        );
        waitForRelocation();

        assertAcked(
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(indexName1, 0, replicaNode, remoteNodeNames.get(1)))
                .execute()
                .actionGet()
        );
        waitForRelocation();

        logger.info("---> Moving only primary for the second index to remote enabled nodes");
        assertAcked(
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(indexName2, 0, primaryNodeName(indexName2), remoteNodeNames.get(0)))
                .execute()
                .actionGet()
        );
        waitForRelocation();
        waitNoPendingTasksOnAll();

        logger.info("---> Stopping indexing");
        indexOne.stopIndexing();
        indexTwo.stopIndexing();

        logger.info("---> Assert remote settings are applied for index one but not for index two");
        assertRemoteProperties(indexName1);
        assertDocrepProperties(indexName2);
    }

    /**
     * Scenario:
     * Creates a mixed mode cluster. One index gets created before remote nodes are introduced,
     * while the other one is created after remote nodes are added.
     * <p>
     * For the first index, asserts docrep settings at first, excludes docrep nodes from
     * allocation and asserts that remote index settings are applied after all shards
     * have been relocated.
     * <p>
     * For the second index, asserts that it already has remote enabled settings.
     * Indexes some more docs and asserts that the index metadata version does not increment
     */
    public void testIndexSettingsUpdatedOnlyForMigratingIndex() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 2 docrep nodes");
        addRemote = false;
        internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "docrep").build());
        internalCluster().validateClusterFormed();

        logger.info("---> Creating the first index with 1 primary and 1 replica");
        String indexName = "migration-index";
        Settings oneReplica = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        createIndexAndAssertDocrepProperties(indexName, oneReplica);

        logger.info("---> Starting indexing in parallel");
        AsyncIndexingService indexingService = new AsyncIndexingService(indexName);
        indexingService.startIndexing();

        logger.info("---> Storing current index metadata version");
        long initalMetadataVersion = internalCluster().client()
            .admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(indexName)
            .getVersion();

        logger.info("---> Adding 2 remote enabled nodes to the cluster");
        initDocRepToRemoteMigration();
        addRemote = true;
        internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "remote").build());
        internalCluster().validateClusterFormed();

        logger.info("---> Excluding docrep nodes from allocation");
        excludeNodeSet("type", "docrep");

        waitForRelocation();
        waitNoPendingTasksOnAll();
        indexingService.stopIndexing();

        logger.info("---> Assert remote settings are applied");
        assertRemoteProperties(indexName);
        assertTrue(
            initalMetadataVersion < internalCluster().client()
                .admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .index(indexName)
                .getVersion()
        );

        logger.info("---> Creating a new index on remote enabled nodes");
        String secondIndex = "remote-index";
        createIndex(
            secondIndex,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build()
        );
        indexBulk(secondIndex, 100);
        initalMetadataVersion = internalCluster().client()
            .admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(secondIndex)
            .getVersion();
        refresh(secondIndex);
        ensureGreen(secondIndex);

        waitNoPendingTasksOnAll();

        assertRemoteProperties(secondIndex);

        logger.info("---> Assert metadata version is not changed");
        assertEquals(
            initalMetadataVersion,
            internalCluster().client().admin().cluster().prepareState().get().getState().metadata().index(secondIndex).getVersion()
        );
    }

    /**
     * Scenario:
     * Creates an index with 1 primary, 2 replicas on 2 docrep nodes. Since the replica
     * configuration is incorrect, the index stays YELLOW.
     * Starts 2 more remote nodes and initiates shard relocation through allocation exclusion.
     * After shard relocation completes, shuts down the docrep nodes and asserts remote
     * index settings are applied even when the index is in YELLOW state
     */
    public void testIndexSettingsUpdatedEvenForMisconfiguredReplicas() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 2 docrep nodes");
        addRemote = false;
        List<String> docrepNodes = internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "docrep").build());
        internalCluster().validateClusterFormed();

        logger.info("---> Creating index with 1 primary and 2 replicas");
        String indexName = "migration-index-allocation-exclude";
        Settings oneReplica = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        createIndexAssertHealthAndDocrepProperties(indexName, oneReplica, this::ensureYellowAndNoInitializingShards);

        logger.info("---> Starting indexing in parallel");
        AsyncIndexingService asyncIndexingService = new AsyncIndexingService(indexName);
        asyncIndexingService.startIndexing();

        logger.info("---> Starts 2 remote enabled nodes");
        initDocRepToRemoteMigration();
        addRemote = true;
        internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "remote").build());
        internalCluster().validateClusterFormed();

        logger.info("---> Excluding docrep nodes from allocation");
        excludeNodeSet("type", "docrep");
        waitForRelocation();
        waitNoPendingTasksOnAll();
        asyncIndexingService.stopIndexing();

        logger.info("---> Assert cluster has turned green since more nodes are added to the cluster");
        ensureGreen(indexName);

        logger.info("---> Assert index still has dcorep settings since replica copies are still on docrep nodes");
        assertDocrepProperties(indexName);

        logger.info("---> Stopping docrep nodes");
        for (String node : docrepNodes) {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node));
        }
        waitNoPendingTasksOnAll();
        ensureYellowAndNoInitializingShards(indexName);

        logger.info("---> Assert remote settings are applied");
        assertRemoteProperties(indexName);
    }

    /**
     * Scenario:
     * Creates an index with 1 primary, 2 replicas on 2 docrep nodes.
     * Starts 2 more remote nodes and initiates shard relocation through allocation exclusion.
     * After shard relocation completes, restarts the docrep node holding extra replica shard copy
     * and asserts remote index settings are applied as soon as the docrep replica copy is unassigned
     */
    public void testIndexSettingsUpdatedWhenDocrepNodeIsRestarted() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 2 docrep nodes");
        addRemote = false;
        List<String> docrepNodes = internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "docrep").build());
        internalCluster().validateClusterFormed();

        logger.info("---> Creating index with 1 primary and 2 replicas");
        String indexName = "migration-index-allocation-exclude";
        Settings oneReplica = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        createIndexAssertHealthAndDocrepProperties(indexName, oneReplica, this::ensureYellowAndNoInitializingShards);

        logger.info("---> Starting indexing in parallel");
        AsyncIndexingService asyncIndexingService = new AsyncIndexingService(indexName);
        asyncIndexingService.startIndexing();

        logger.info("---> Starts 2 remote enabled nodes");
        initDocRepToRemoteMigration();
        addRemote = true;
        internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "remote").build());
        internalCluster().validateClusterFormed();

        logger.info("---> Excluding docrep nodes from allocation");
        excludeNodeSet("type", "docrep");
        waitForRelocation();
        waitNoPendingTasksOnAll();
        asyncIndexingService.stopIndexing();

        logger.info("---> Assert cluster has turned green since more nodes are added to the cluster");
        ensureGreen(indexName);

        logger.info("---> Assert index still has dcorep settings since replica copies are still on docrep nodes");
        assertDocrepProperties(indexName);

        ClusterState clusterState = internalCluster().client().admin().cluster().prepareState().get().getState();
        DiscoveryNodes nodes = clusterState.nodes();

        String docrepReplicaNodeName = "";
        for (ShardRouting shardRouting : clusterState.routingTable().index(indexName).shard(0).getShards()) {
            if (nodes.get(shardRouting.currentNodeId()).isRemoteStoreNode() == false) {
                docrepReplicaNodeName = nodes.get(shardRouting.currentNodeId()).getName();
                break;
            }
        }
        excludeNodeSet("type", null);

        logger.info("---> Stopping docrep node holding the replica copy");
        internalCluster().restartNode(docrepReplicaNodeName);
        ensureStableCluster(5);
        waitNoPendingTasksOnAll();

        logger.info("---> Assert remote index settings have been applied");
        assertRemoteProperties(indexName);
        logger.info("---> Assert cluster is yellow since remote index settings have been applied");
        ensureYellowAndNoInitializingShards(indexName);
    }

    /**
     * Scenario:
     * Creates a docrep cluster with 3 nodes and an index with 1 primary and 2 replicas.
     * Adds 3 more remote nodes to the cluster and moves over the primary copy from docrep
     * to remote through _cluster/reroute. Asserts that the remote store path based metadata
     * have been applied to the index.
     * Moves over the first replica copy and asserts that the remote store based settings has not been applied
     * Excludes docrep nodes from allocation to force migration of the 3rd replica copy and asserts remote
     * store settings has been applied as all shards have moved over
     */
    public void testRemotePathMetadataAddedWithFirstPrimaryMovingToRemote() throws Exception {
        String indexName = "index-1";
        internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 3 docrep nodes");
        internalCluster().startDataOnlyNodes(3, Settings.builder().put("node.attr._type", "docrep").build());
        internalCluster().validateClusterFormed();

        logger.info("---> Creating index with 1 primary and 2 replicas");
        Settings oneReplica = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2).build();
        createIndexAndAssertDocrepProperties(indexName, oneReplica);

        logger.info("---> Adding 3 remote enabled nodes");
        initDocRepToRemoteMigration();
        addRemote = true;
        List<String> remoteEnabledNodes = internalCluster().startDataOnlyNodes(
            3,
            Settings.builder().put("node.attr._type", "remote").build()
        );

        logger.info("---> Moving primary copy to remote enabled node");
        String primaryNodeName = primaryNodeName(indexName);
        assertAcked(
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(indexName, 0, primaryNodeName, remoteEnabledNodes.get(0)))
                .execute()
                .actionGet()
        );
        waitForRelocation();
        waitNoPendingTasksOnAll();

        logger.info("---> Assert custom remote path based metadata is applied");
        assertCustomIndexMetadata(indexName);

        logger.info("---> Moving over one replica copy to remote enabled node");
        String replicaNodeName = replicaNodeName(indexName);
        assertAcked(
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(indexName, 0, replicaNodeName, remoteEnabledNodes.get(1)))
                .execute()
                .actionGet()
        );
        waitForRelocation();
        waitNoPendingTasksOnAll();

        logger.info("---> Assert index still has docrep settings");
        assertDocrepProperties(indexName);

        logger.info("---> Excluding docrep nodes from allocation");
        excludeNodeSet("type", "docrep");
        waitForRelocation();
        waitNoPendingTasksOnAll();

        logger.info("---> Assert index has remote store settings");
        assertRemoteProperties(indexName);
    }

    /**
     * Scenario:
     * creates an index on docrep node with non-remote cluster-manager.
     * make the cluster mixed, add remote cluster-manager and data nodes.
     * <p>
     * exclude docrep nodes, assert that remote index path file exists
     * when shards start relocating to the remote nodes.
     */
    public void testRemoteIndexPathFileExistsAfterMigration() throws Exception {
        String docrepClusterManager = internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 2 docrep nodes");
        addRemote = false;
        internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "docrep").build());
        internalCluster().validateClusterFormed();

        logger.info("---> Creating index with 1 primary and 1 replica");
        String indexName = "migration-index";
        Settings oneReplica = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        createIndexAndAssertDocrepProperties(indexName, oneReplica);

        String indexUUID = internalCluster().client()
            .admin()
            .indices()
            .prepareGetSettings(indexName)
            .get()
            .getSetting(indexName, IndexMetadata.SETTING_INDEX_UUID);

        logger.info("---> Starting indexing in parallel");
        AsyncIndexingService indexingService = new AsyncIndexingService(indexName);
        indexingService.startIndexing();

        logger.info("---> Adding 2 remote enabled nodes to the cluster & cluster manager");
        initDocRepToRemoteMigration();
        addRemote = true;
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "remote").build());
        internalCluster().validateClusterFormed();

        assertTrue(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), RemoteStoreEnums.PathType.HASHED_PREFIX)
                )
                .get()
                .isAcknowledged()
        );

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(docrepClusterManager));
        internalCluster().validateClusterFormed();

        logger.info("---> Excluding docrep nodes from allocation");
        excludeNodeSet("type", "docrep");

        waitForRelocation();
        waitNoPendingTasksOnAll();
        indexingService.stopIndexing();

        // validate remote index path file exists
        logger.info("---> Asserting remote index path file exists");
        String fileNamePrefix = String.join(RemoteIndexPathUploader.DELIMITER, indexUUID, "7", RemoteIndexPath.DEFAULT_VERSION);

        assertTrue(FileSystemUtils.exists(translogRepoPath.resolve(RemoteIndexPath.DIR)));
        Path[] files = FileSystemUtils.files(translogRepoPath.resolve(RemoteIndexPath.DIR));
        assertEquals(1, files.length);
        assertTrue(Arrays.stream(files).anyMatch(file -> file.toString().contains(fileNamePrefix)));

        assertTrue(FileSystemUtils.exists(segmentRepoPath.resolve(RemoteIndexPath.DIR)));
        files = FileSystemUtils.files(segmentRepoPath.resolve(RemoteIndexPath.DIR));
        assertEquals(1, files.length);
        assertTrue(Arrays.stream(files).anyMatch(file -> file.toString().contains(fileNamePrefix)));
    }

    private void createIndexAndAssertDocrepProperties(String index, Settings settings) {
        createIndexAssertHealthAndDocrepProperties(index, settings, this::ensureGreen);
    }

    private void createIndexAssertHealthAndDocrepProperties(
        String index,
        Settings settings,
        Function<String, ClusterHealthStatus> ensureState
    ) {
        createIndex(index, settings);
        refresh(index);
        ensureState.apply(index);
        assertDocrepProperties(index);
    }

    /**
     * Assert current index settings have:
     * - index.remote_store.enabled == false
     * - index.remote_store.segment.repository == null
     * - index.remote_store.translog.repository == null
     * - index.replication.type == DOCUMENT
     */
    private void assertDocrepProperties(String index) {
        logger.info("---> Asserting docrep index settings");
        IndexMetadata iMd = internalCluster().client().admin().cluster().prepareState().get().getState().metadata().index(index);
        Settings settings = iMd.getSettings();
        assertFalse(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(settings));
        assertFalse(IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.exists(settings));
        assertFalse(IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.exists(settings));
        assertEquals(ReplicationType.DOCUMENT, IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(settings));
    }

    /**
     * Assert current index settings have:
     * - index.remote_store.enabled == true
     * - index.remote_store.segment.repository != null
     * - index.remote_store.translog.repository != null
     * - index.replication.type == SEGMENT
     * Asserts index metadata customs has the <code>remote_store</code> key
     */
    private void assertRemoteProperties(String index) {
        logger.info("---> Asserting remote index settings");
        IndexMetadata iMd = internalCluster().client().admin().cluster().prepareState().get().getState().metadata().index(index);
        Settings settings = iMd.getSettings();
        assertTrue(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(settings));
        assertTrue(IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.exists(settings));
        assertTrue(IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.exists(settings));
        assertEquals(ReplicationType.SEGMENT, IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(settings));
        assertNotNull(iMd.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY));
    }

    /**
     * Asserts index metadata customs has the <code>remote_store</code> key
     */
    private void assertCustomIndexMetadata(String index) {
        logger.info("---> Asserting custom index metadata");
        IndexMetadata iMd = internalCluster().client().admin().cluster().prepareState().get().getState().metadata().index(index);
        assertNotNull(iMd.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY));
        assertNotNull(iMd.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY).get(IndexMetadata.TRANSLOG_METADATA_KEY));
    }
}
