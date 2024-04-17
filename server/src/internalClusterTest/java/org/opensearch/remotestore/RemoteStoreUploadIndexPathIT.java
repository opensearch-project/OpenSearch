/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.index.remote.RemoteIndexPath;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreUploadIndexPathIT extends RemoteStoreBaseIntegTestCase {

    private final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true).build();
    }

    /**
     * Checks that the remote index path file gets created for the intended remote store path type and does not get created
     * wherever not required.
     */
    public void testRemoteIndexPathFileCreation() throws ExecutionException, InterruptedException, IOException {
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        // Case 1 - Hashed_prefix, we would need the remote index path file to be created.
        client(clusterManagerNode).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), RemoteStoreEnums.PathType.HASHED_PREFIX)
            )
            .get();

        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 1));
        validateRemoteIndexPathFile(true);
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).get());
        FileSystemUtils.deleteSubDirectories(translogRepoPath);
        FileSystemUtils.deleteSubDirectories(segmentRepoPath);

        // Case 2 - Hashed_infix, we would not have the remote index path file created here.
        client(clusterManagerNode).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), RemoteStoreEnums.PathType.HASHED_INFIX)
            )
            .get();
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 1));
        validateRemoteIndexPathFile(false);
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).get());

        // Case 3 - fixed, we would not have the remote index path file created here either.
        client(clusterManagerNode).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), RemoteStoreEnums.PathType.FIXED))
            .get();
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 1));
        validateRemoteIndexPathFile(false);
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).get());

    }

    private void validateRemoteIndexPathFile(boolean exists) {
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);

        assertEquals(exists, FileSystemUtils.exists(translogRepoPath.resolve(RemoteIndexPath.DIR)));
        assertEquals(
            exists,
            FileSystemUtils.exists(
                translogRepoPath.resolve(RemoteIndexPath.DIR)
                    .resolve(String.format(Locale.ROOT, RemoteIndexPath.FILE_NAME_FORMAT, indexUUID))
            )
        );
        assertEquals(exists, FileSystemUtils.exists(segmentRepoPath.resolve(RemoteIndexPath.DIR)));
        assertEquals(
            exists,
            FileSystemUtils.exists(
                segmentRepoPath.resolve(RemoteIndexPath.DIR)
                    .resolve(String.format(Locale.ROOT, RemoteIndexPath.FILE_NAME_FORMAT, indexUUID))
            )
        );
    }
}
