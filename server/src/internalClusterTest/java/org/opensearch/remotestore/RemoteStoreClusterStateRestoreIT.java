/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.opensearch.indices.ShardLimitValidator.SETTING_MAX_SHARDS_PER_CLUSTER_KEY;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreClusterStateRestoreIT extends BaseRemoteStoreRestoreIT {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true).build();
    }

    private void addNewNodes(int dataNodeCount, int clusterManagerNodeCount) {
        internalCluster().startNodes(dataNodeCount + clusterManagerNodeCount);
    }

    private Map<String, Long> initialTestSetup(int shardCount, int replicaCount, int dataNodeCount, int clusterManagerNodeCount) {
        prepareCluster(clusterManagerNodeCount, dataNodeCount, INDEX_NAME, replicaCount, shardCount);
        Map<String, Long> indexStats = indexData(1, false, INDEX_NAME);
        assertEquals(shardCount * (replicaCount + 1), getNumShards(INDEX_NAME).totalNumShards);
        ensureGreen(INDEX_NAME);
        return indexStats;
    }

    private void resetCluster(int dataNodeCount, int clusterManagerNodeCount) {
        internalCluster().stopAllNodes();
        addNewNodes(dataNodeCount, clusterManagerNodeCount);
    }

    private void restoreAndValidate(String clusterUUID, Map<String, Long> indexStats) throws Exception {
        restoreAndValidate(clusterUUID, indexStats, true);
    }

    private void restoreAndValidate(String clusterUUID, Map<String, Long> indexStats, boolean validate) throws Exception {
        // TODO once auto restore is merged, the remote cluster state will be restored

        if (validate) {
            // Step - 4 validation restore is successful.
            ensureGreen(INDEX_NAME);
            verifyRestoredData(indexStats, INDEX_NAME);
        }
    }

    private void restoreAndValidateFails(
        String clusterUUID,
        PlainActionFuture<RestoreRemoteStoreResponse> actionListener,
        Class<? extends Throwable> clazz,
        String errorSubString
    ) {

        try {
            restoreAndValidate(clusterUUID, null, false);
        } catch (Exception e) {
            assertTrue(
                String.format(Locale.ROOT, "%s %s", clazz, e),
                clazz.isAssignableFrom(e.getClass())
                    || clazz.isAssignableFrom(e.getCause().getClass())
                    || (e.getCause().getCause() != null && clazz.isAssignableFrom(e.getCause().getCause().getClass()))
            );
            assertTrue(
                String.format(Locale.ROOT, "Error message mismatch. Expected: [%s]. Actual: [%s]", errorSubString, e.getMessage()),
                e.getMessage().contains(errorSubString)
            );
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/9834")
    public void testFullClusterRestore() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        Map<String, Long> indexStats = initialTestSetup(shardCount, replicaCount, dataNodeCount, 1);
        String prevClusterUUID = clusterService().state().metadata().clusterUUID();

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        resetCluster(dataNodeCount, clusterManagerNodeCount);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step - 3 Trigger full cluster restore and validate
        restoreAndValidate(prevClusterUUID, indexStats);
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/9834")
    public void testFullClusterRestoreMultipleIndices() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        Map<String, Long> indexStats = initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);

        String secondIndexName = INDEX_NAME + "-2";
        createIndex(secondIndexName, remoteStoreIndexSettings(replicaCount, shardCount + 1));
        Map<String, Long> indexStats2 = indexData(1, false, secondIndexName);
        assertEquals((shardCount + 1) * (replicaCount + 1), getNumShards(secondIndexName).totalNumShards);
        ensureGreen(secondIndexName);

        String prevClusterUUID = clusterService().state().metadata().clusterUUID();

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        resetCluster(dataNodeCount, clusterManagerNodeCount);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step - 3 Trigger full cluster restore
        restoreAndValidate(prevClusterUUID, indexStats);
        ensureGreen(secondIndexName);
        verifyRestoredData(indexStats2, secondIndexName);
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/9834")
    public void testFullClusterRestoreFailureValidationFailures() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        // index some data to generate files in remote directory
        Map<String, Long> indexStats = initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);
        String prevClusterUUID = clusterService().state().metadata().clusterUUID();

        // Start of Test - 1
        // Test - 1 Trigger full cluster restore and validate it fails due to incorrect cluster UUID
        PlainActionFuture<RestoreRemoteStoreResponse> future = PlainActionFuture.newFuture();
        restoreAndValidateFails("randomUUID", future, IllegalStateException.class, "Remote Cluster State not found - randomUUID");
        // End of Test - 1

        // Start of Test - 3
        // Test - 2 Trigger full cluster restore and validate it fails due to cluster UUID same as current cluster UUID
        future = PlainActionFuture.newFuture();
        restoreAndValidateFails(
            clusterService().state().metadata().clusterUUID(),
            future,
            IllegalArgumentException.class,
            "clusterUUID to restore from should be different from current cluster UUID"
        );
        // End of Test - 2

        // Start of Test - 3
        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        // Restarting cluster with just 1 data node helps with applying cluster settings
        resetCluster(1, clusterManagerNodeCount);
        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        reduceShardLimits(1, 1);

        // Step - 4 Trigger full cluster restore and validate it fails
        future = PlainActionFuture.newFuture();
        restoreAndValidateFails(
            prevClusterUUID,
            future,
            IllegalArgumentException.class,
            "this action would add [2] total shards, but this cluster currently has [0]/[1] maximum shards open"
        );
        resetShardLimits();
        // End of Test - 3

        // Start of Test - 4
        // Test -4 Reset cluster and trigger full restore with same name index in the cluster
        // Test -4 Add required nodes for this test after last reset.
        addNewNodes(dataNodeCount - 1, 0);

        newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Test -4 Step - 2 Create a new index with same name
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 1));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        future = PlainActionFuture.newFuture();

        // Test -4 Step - 3 Trigger full cluster restore and validate fails
        restoreAndValidateFails(
            prevClusterUUID,
            future,
            IllegalStateException.class,
            "cannot restore index [remote-store-test-idx-1] because an open index with same name/uuid already exists in the cluster"
        );

        // Test -4 Step - 4 validation restore is successful.
        ensureGreen(INDEX_NAME);
        // End of Test - 4
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/9834")
    public void testFullClusterRestoreManifestFilePointsToInvalidIndexMetadataPathThrowsException() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);

        String prevClusterUUID = clusterService().state().metadata().clusterUUID();

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        resetCluster(dataNodeCount, clusterManagerNodeCount);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step - 4 Delete index metadata file in remote
        try {
            Files.move(
                segmentRepoPath.resolve(
                    RemoteClusterStateService.encodeString(clusterService().state().getClusterName().value())
                        + "/cluster-state/"
                        + prevClusterUUID
                        + "/index"
                ),
                segmentRepoPath.resolve("cluster-state/")
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Step - 5 Trigger full cluster restore and validate fails
        PlainActionFuture<RestoreRemoteStoreResponse> future = PlainActionFuture.newFuture();
        restoreAndValidateFails(prevClusterUUID, future, IllegalStateException.class, "asdsa");
    }

    private void reduceShardLimits(int maxShardsPerNode, int maxShardsPerCluster) {
        // Step 3 - Reduce shard limits to hit shard limit with less no of shards
        try {
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest().transientSettings(
                        Settings.builder()
                            .put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), maxShardsPerNode)
                            .put(SETTING_MAX_SHARDS_PER_CLUSTER_KEY, maxShardsPerCluster)
                    )
                )
                .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void resetShardLimits() {
        // Step - 5 Reset the cluster settings
        ClusterUpdateSettingsRequest resetRequest = new ClusterUpdateSettingsRequest();
        resetRequest.transientSettings(
            Settings.builder().putNull(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey()).putNull(SETTING_MAX_SHARDS_PER_CLUSTER_KEY)
        );

        try {
            client().admin().cluster().updateSettings(resetRequest).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
