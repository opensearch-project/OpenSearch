/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteClusterStateServiceIT extends RemoteStoreBaseIntegTestCase {

    private static String INDEX_NAME = "test-index";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true).build();
    }

    private void prepareCluster(int numClusterManagerNodes, int numDataOnlyNodes, String indices, int replicaCount, int shardCount) {
        internalCluster().startClusterManagerOnlyNodes(numClusterManagerNodes);
        internalCluster().startDataOnlyNodes(numDataOnlyNodes);
        for (String index : indices.split(",")) {
            createIndex(index, remoteStoreIndexSettings(replicaCount, shardCount));
            ensureYellowAndNoInitializingShards(index);
            ensureGreen(index);
        }
    }

    private Map<String, Long> initialTestSetup(int shardCount, int replicaCount, int dataNodeCount, int clusterManagerNodeCount) {
        prepareCluster(clusterManagerNodeCount, dataNodeCount, INDEX_NAME, replicaCount, shardCount);
        Map<String, Long> indexStats = indexData(1, false, INDEX_NAME);
        assertEquals(shardCount * (replicaCount + 1), getNumShards(INDEX_NAME).totalNumShards);
        ensureGreen(INDEX_NAME);
        return indexStats;
    }

    public void testFullClusterRestoreStaleDelete() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);
        setReplicaCount(0);
        setReplicaCount(2);
        setReplicaCount(0);
        setReplicaCount(1);
        setReplicaCount(0);
        setReplicaCount(1);
        setReplicaCount(0);
        setReplicaCount(2);
        setReplicaCount(0);

        RemoteClusterStateService remoteClusterStateService = internalCluster().getClusterManagerNodeInstance(
            RemoteClusterStateService.class
        );

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

        assertEquals(10, repository.blobStore().blobContainer(baseMetadataPath.add("manifest")).listBlobsByPrefix("manifest").size());

        Map<String, IndexMetadata> indexMetadataMap = remoteClusterStateService.getLatestIndexMetadata(
            cluster().getClusterName(),
            getClusterState().metadata().clusterUUID()
        );
        assertEquals(0, indexMetadataMap.values().stream().findFirst().get().getNumberOfReplicas());
        assertEquals(shardCount, indexMetadataMap.values().stream().findFirst().get().getNumberOfShards());
    }

    private void setReplicaCount(int replicaCount) {
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, replicaCount))
            .get();
    }
}
