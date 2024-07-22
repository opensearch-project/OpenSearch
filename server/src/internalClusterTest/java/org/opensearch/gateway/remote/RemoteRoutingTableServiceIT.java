/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.junit.Before;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.model.RemoteRoutingTableBlobStore;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;

import static org.opensearch.cluster.routing.remote.InternalRemoteRoutingTableService.*;
import static org.opensearch.common.util.FeatureFlags.REMOTE_PUBLICATION_EXPERIMENTAL;
import static org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_TABLE;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;

import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteRoutingTableServiceIT extends RemoteStoreBaseIntegTestCase {
    private static String INDEX_NAME = "test-index";

    @Before
    public void setup() {
        asyncUploadMockFsRepo = false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(RemoteRoutingTableBlobStore.REMOTE_ROUTING_TABLE_PATH_TYPE_SETTING.getKey(), RemoteStoreEnums.PathType.HASHED_PREFIX.toString())
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, REPOSITORY_NAME)
            .put(REMOTE_PUBLICATION_EXPERIMENTAL, true)
            .build();
    }

    private RemoteStoreEnums.PathType pathType = RemoteStoreEnums.PathType.HASHED_PREFIX;

    public void testRemoteRoutingTableCreateIndex() throws Exception {
        prepareCluster(1, 2, INDEX_NAME, 1, 5);

        ensureGreen(INDEX_NAME);
        //createIndex("index-2", remoteStoreIndexSettings(2, 5));

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

        BlobPath indexRoutingPath = baseMetadataPath.add(INDEX_ROUTING_TABLE);
        List<IndexRoutingTable> indexRoutingTable = new ArrayList<>(getClusterState().routingTable().indicesRouting().values());
        RemoteStoreEnums.PathHashAlgorithm pathHashAlgo = RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64;
        BlobPath path = pathType.path(
            RemoteStorePathStrategy.PathInput.builder().basePath(indexRoutingPath).indexUUID(indexRoutingTable.get(0).getIndex().getUUID()).build(),
            pathHashAlgo
        );
        assertBusy(() -> {
            int indexRoutingFiles = repository.blobStore().blobContainer(path).listBlobs().size();
            //assertTrue(indexRoutingFiles>0);
            logger.info("number of index routing files {}", indexRoutingFiles);
        });

        String[] allNodes = internalCluster().getNodeNames();
        List<Long> routingTableVersions = new ArrayList<>();

        // get cluster state from all the nodes
        for (String node : allNodes) {
            RoutingTable routingTable = internalCluster().client(node)
                .admin()
                .cluster()
                .state(new ClusterStateRequest().local(true))
                .get().getState().routingTable();
            routingTableVersions.add(routingTable.version());
        }
        assertTrue(areRoutingTableVersionsSame(routingTableVersions));

        //updateIndexSettings(INDEX_NAME, IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2);

        assertBusy(() -> {
            int indexRoutingFiles = repository.blobStore().blobContainer(path).listBlobs().size();
            //assertTrue(indexRoutingFiles>0);
            logger.info("number of index routing files {}", indexRoutingFiles);
        });
    }

    private void updateIndexSettings(String indexName, String settingKey, int settingValue) {
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(settingKey, settingValue))
            .execute()
            .actionGet();
    }

    private boolean areRoutingTableVersionsSame(List<Long> routingTableVersions) {
        if (routingTableVersions == null || routingTableVersions.isEmpty()) {
            return false;
        }

        Long firstVersion = routingTableVersions.get(0);
        for (Long routingTableVersion : routingTableVersions) {
            if (!firstVersion.equals(routingTableVersion)) {
                logger.info("Responses are not same {} {}", firstVersion, routingTableVersion);

                return false;
            }
        }
        return true;
    }
}
