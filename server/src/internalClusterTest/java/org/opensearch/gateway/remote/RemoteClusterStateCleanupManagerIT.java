/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.gateway.remote.RemoteClusterStateCleanupManager.CLUSTER_STATE_CLEANUP_INTERVAL_DEFAULT;
import static org.opensearch.gateway.remote.RemoteClusterStateCleanupManager.REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateCleanupManager.RETAINED_MANIFESTS;
import static org.opensearch.gateway.remote.RemoteClusterStateCleanupManager.SKIP_CLEANUP_STATE_CHANGES;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteClusterStateCleanupManagerIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "test-index";

    @Before
    public void setup() {
        asyncUploadMockFsRepo = false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true).build();
    }

    private Map<String, Long> initialTestSetup(int shardCount, int replicaCount, int dataNodeCount, int clusterManagerNodeCount) {
        prepareCluster(clusterManagerNodeCount, dataNodeCount, INDEX_NAME, replicaCount, shardCount);
        Map<String, Long> indexStats = indexData(1, false, INDEX_NAME);
        assertEquals(shardCount * (replicaCount + 1), getNumShards(INDEX_NAME).totalNumShards);
        ensureGreen(INDEX_NAME);
        return indexStats;
    }

    public void testRemoteCleanupTaskUpdated() {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);
        RemoteClusterStateCleanupManager remoteClusterStateCleanupManager = internalCluster().getClusterManagerNodeInstance(
            RemoteClusterStateCleanupManager.class
        );

        assertEquals(CLUSTER_STATE_CLEANUP_INTERVAL_DEFAULT, remoteClusterStateCleanupManager.getStaleFileDeletionTask().getInterval());
        assertTrue(remoteClusterStateCleanupManager.getStaleFileDeletionTask().isScheduled());

        // now disable
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING.getKey(), -1))
            .get();

        assertEquals(-1, remoteClusterStateCleanupManager.getStaleFileDeletionTask().getInterval().getMillis());
        assertFalse(remoteClusterStateCleanupManager.getStaleFileDeletionTask().isScheduled());

        // now set Clean up interval to 1 min
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING.getKey(), "1m"))
            .get();
        assertEquals(1, remoteClusterStateCleanupManager.getStaleFileDeletionTask().getInterval().getMinutes());
    }

    public void testRemoteCleanupDeleteStale() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);

        // update cluster state 21 times to ensure that clean up has run after this will upload 42 manifest files
        // to repository, if manifest files are less than that it means clean up has run
        updateClusterStateNTimes(RETAINED_MANIFESTS + SKIP_CLEANUP_STATE_CHANGES + 1);

        RepositoriesService repositoriesService = internalCluster().getClusterManagerNodeInstance(RepositoriesService.class);
        BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(REPOSITORY_NAME);
        BlobPath baseMetadataPath = repository.basePath()
            .add(
                Base64.getUrlEncoder()
                    .withoutPadding()
                    .encodeToString(getClusterState().getClusterName().value().getBytes(StandardCharsets.UTF_8))
            )
            .add("cluster-state")
            .add(getClusterState().metadata().clusterUUID());
        BlobPath manifestContainerPath = baseMetadataPath.add("manifest");
        RemoteClusterStateCleanupManager remoteClusterStateCleanupManager = internalCluster().getClusterManagerNodeInstance(
            RemoteClusterStateCleanupManager.class
        );

        // set cleanup interval to 100 ms to make the test faster
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING.getKey(), "100ms"))
            .get();

        assertTrue(response.isAcknowledged());
        assertBusy(() -> assertEquals(100, remoteClusterStateCleanupManager.getStaleFileDeletionTask().getInterval().getMillis()));

        assertBusy(() -> {
            int manifestFiles = repository.blobStore().blobContainer(manifestContainerPath).listBlobsByPrefix("manifest").size();
            logger.info("number of current manifest file: {}", manifestFiles);
            // we can't guarantee that we have same number of manifest as Retained manifest in our repo as there can be other queued task
            // other than replica count change which can upload new manifest files, that's why we check that number of manifests is between
            // Retained manifests and Retained manifests + 2 * Skip cleanup state changes (each cluster state update uploads 2 manifests)
            assertTrue(
                "Current number of manifest files: " + manifestFiles,
                manifestFiles >= RETAINED_MANIFESTS && manifestFiles < RETAINED_MANIFESTS + 2 * SKIP_CLEANUP_STATE_CHANGES
            );
        });

        // disable the clean up to avoid race condition during shutdown
        response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING.getKey(), "-1"))
            .get();

        assertTrue(response.isAcknowledged());
    }

    private void updateClusterStateNTimes(int n) {
        int newReplicaCount = randomIntBetween(0, 3);
        for (int i = n; i > 0; i--) {
            ClusterUpdateSettingsResponse response = client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING.getKey(), i, TimeUnit.SECONDS))
                .get();
            assertTrue(response.isAcknowledged());
        }
    }
}
