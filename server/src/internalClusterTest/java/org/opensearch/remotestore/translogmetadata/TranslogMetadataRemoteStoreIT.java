/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.translogmetadata;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreIT;
import org.opensearch.remotestore.translogmetadata.mocks.MockFsMetadataSupportedRepositoryPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TranslogMetadataRemoteStoreIT extends RemoteStoreIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockFsMetadataSupportedRepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(
                remoteStoreClusterSettings(
                    REPOSITORY_NAME,
                    segmentRepoPath,
                    MockFsMetadataSupportedRepositoryPlugin.TYPE_MD,
                    REPOSITORY_2_NAME,
                    translogRepoPath,
                    MockFsMetadataSupportedRepositoryPlugin.TYPE_MD
                )
            )
            .build();
    }

    // Test local only translog files which are not uploaded to remote store (no metadata present in remote)
    // Without the cleanup change in RemoteFsTranslog.createEmptyTranslog, this test fails with NPE.
    public void testLocalOnlyTranslogCleanupOnNodeRestart() throws Exception {
        clusterSettingsSuppliedByTest = true;

        // Overriding settings to use AsyncMultiStreamBlobContainer
        Settings settings = Settings.builder()
            .put(super.nodeSettings(1))
            .put(
                remoteStoreClusterSettings(
                    REPOSITORY_NAME,
                    segmentRepoPath,
                    MockFsMetadataSupportedRepositoryPlugin.TYPE_MD,
                    REPOSITORY_2_NAME,
                    translogRepoPath,
                    MockFsMetadataSupportedRepositoryPlugin.TYPE_MD
                )
            )
            .build();

        internalCluster().startClusterManagerOnlyNode(settings);
        String dataNode = internalCluster().startDataOnlyNode(settings);

        // 1. Create index with 0 replica
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 10000L, -1));
        ensureGreen(INDEX_NAME);

        // 2. Index docs
        int searchableDocs = 0;
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            indexBulk(INDEX_NAME, 15);
            refresh(INDEX_NAME);
            searchableDocs += 15;
        }
        indexBulk(INDEX_NAME, 15);

        assertHitCount(client(dataNode).prepareSearch(INDEX_NAME).setSize(0).get(), searchableDocs);

        // 3. Delete metadata from remote translog
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);

        String shardPath = getShardLevelBlobPath(client(), INDEX_NAME, BlobPath.cleanPath(), "0", TRANSLOG, METADATA).buildAsString();
        Path translogMetaDataPath = Path.of(translogRepoPath + "/" + shardPath);

        try (Stream<Path> files = Files.list(translogMetaDataPath)) {
            files.forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    // Ignore
                }
            });
        }

        internalCluster().restartNode(dataNode);

        ensureGreen(INDEX_NAME);

        assertHitCount(client(dataNode).prepareSearch(INDEX_NAME).setSize(0).get(), searchableDocs);
        indexBulk(INDEX_NAME, 15);
        refresh(INDEX_NAME);
        assertHitCount(client(dataNode).prepareSearch(INDEX_NAME).setSize(0).get(), searchableDocs + 15);
    }

}
