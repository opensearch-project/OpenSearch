/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteSnapshotFailureScenariosIT extends AbstractSnapshotIntegTestCase {
    private static final String BASE_REMOTE_REPO = "test-rs-repo" + TEST_REMOTE_STORE_REPO_SUFFIX;
    private static final String SNAP_REPO = "test-snap-repo";
    private static final String INDEX_NAME_1 = "testindex1";
    private static final String RESTORED_INDEX_NAME_1 = INDEX_NAME_1 + "-restored";
    private static final String INDEX_NAME_2 = "testindex2";
    private static final String RESTORED_INDEX_NAME_2 = INDEX_NAME_2 + "-restored";

    private Path remoteRepoPath;
    private Path snapRepoPath;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockFsRepositoryPlugin.class)).collect(Collectors.toList());
    }

    @Before
    public void setup() {
        remoteRepoPath = randomRepoPath().toAbsolutePath();
        snapRepoPath = randomRepoPath().toAbsolutePath();
    }

    @After
    public void teardown() {
        clusterAdmin().prepareCleanupRepository(BASE_REMOTE_REPO).get();
        clusterAdmin().prepareCleanupRepository(SNAP_REPO).get();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(BASE_REMOTE_REPO, remoteRepoPath, "mock", BASE_REMOTE_REPO, remoteRepoPath, "mock"))
            .build();
    }

    private Settings.Builder getIndexSettings(int numOfShards, int numOfReplicas) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numOfShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numOfReplicas)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s");
        return settingsBuilder;
    }

    private void indexDocuments(Client client, String indexName, int numOfDocs) {
        indexDocuments(client, indexName, 0, numOfDocs);
    }

    private void indexDocuments(Client client, String indexName, int fromId, int toId) {
        for (int i = fromId; i < toId; i++) {
            String id = Integer.toString(i);
            client.prepareIndex(indexName).setId(id).setSource("text", "sometext").get();
        }
        client.admin().indices().prepareFlush(indexName).get();
    }

    public void testRestoreDownloadFromRemoteStore() {
        String snapshotName1 = "test-restore-snapshot1";

        createRepository(SNAP_REPO, "fs", getRepositorySettings(snapRepoPath, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(INDEX_NAME_1, indexSettings);

        Settings indexSettings2 = getIndexSettings(1, 0).build();
        createIndex(INDEX_NAME_2, indexSettings2);

        final int numDocsInIndex1 = 5;
        final int numDocsInIndex2 = 6;
        indexDocuments(client, INDEX_NAME_1, numDocsInIndex1);
        indexDocuments(client, INDEX_NAME_2, numDocsInIndex2);
        ensureGreen(INDEX_NAME_1, INDEX_NAME_2);

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(SNAP_REPO, snapshotName1)
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME_1, INDEX_NAME_2)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
        assertTrue(createSnapshotResponse.getSnapshotInfo().isRemoteStoreIndexShallowCopyEnabled());

        logger.info("--> updating repository to induce remote store upload failure");
        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(BASE_REMOTE_REPO)
                .setType("mock")
                .setSettings(
                    Settings.builder().put("location", remoteRepoPath).put("regexes_to_fail_io", ".si").put("max_failure_number", 6L)
                )
        ); // we retry IO 5 times, keeping it 6 so that first read for single segment file will fail

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(SNAP_REPO, snapshotName1)
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME_1)
            .setRenamePattern(INDEX_NAME_1)
            .setRenameReplacement(RESTORED_INDEX_NAME_1)
            .get();

        ensureRed(RESTORED_INDEX_NAME_1);
        assertEquals(1, restoreSnapshotResponse.getRestoreInfo().failedShards());

        assertAcked(client().admin().indices().prepareClose(RESTORED_INDEX_NAME_1).get());
        restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(SNAP_REPO, snapshotName1)
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME_1)
            .setRenamePattern(INDEX_NAME_1)
            .setRenameReplacement(RESTORED_INDEX_NAME_1)
            .get();

        ensureGreen(RESTORED_INDEX_NAME_1);
        assertEquals(0, restoreSnapshotResponse.getRestoreInfo().failedShards());

        // resetting repository to original settings.
        logger.info("--> removing repository settings overrides");
        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(BASE_REMOTE_REPO)
                .setType("mock")
                .setSettings(Settings.builder().put("location", remoteRepoPath))
        );
    }

    public void testAcquireLockFails() throws IOException {
        final Client client = client();

        logger.info("-->  creating snapshot repository");
        createRepository(SNAP_REPO, "mock", getRepositorySettings(snapRepoPath, true));

        logger.info("-->  updating remote store repository to induce lock file upload failure");
        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(BASE_REMOTE_REPO)
                .setType("mock")
                .setSettings(Settings.builder().put("location", remoteRepoPath).put("regexes_to_fail_io", "lock$"))
        );

        logger.info("--> creating indices and index documents");
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(INDEX_NAME_1, indexSettings);
        createIndex(INDEX_NAME_2, indexSettings);

        final int numDocsInIndex1 = 5;
        final int numDocsInIndex2 = 6;
        indexDocuments(client, INDEX_NAME_1, numDocsInIndex1);
        indexDocuments(client, INDEX_NAME_2, numDocsInIndex2);
        ensureGreen(INDEX_NAME_1, INDEX_NAME_2);

        logger.info("--> create first shallow snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(SNAP_REPO, "test-snap")
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME_1, INDEX_NAME_2)
            .get();
        assertTrue(createSnapshotResponse.getSnapshotInfo().isRemoteStoreIndexShallowCopyEnabled());
        assertTrue(createSnapshotResponse.getSnapshotInfo().failedShards() > 0);
        assertSame(createSnapshotResponse.getSnapshotInfo().state(), SnapshotState.PARTIAL);
        logger.info("--> delete partial snapshot");
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(SNAP_REPO, "test-snap").get());
        // resetting repository to original settings.
        logger.info("--> removing repository settings overrides");
        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(BASE_REMOTE_REPO)
                .setType("mock")
                .setSettings(Settings.builder().put("location", remoteRepoPath))
        );
    }

    public void testWriteShallowSnapFileFails() throws IOException, InterruptedException {
        final Client client = client();

        Settings.Builder snapshotRepoSettingsBuilder = randomRepositorySettings().put(
            BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(),
            Boolean.TRUE
        ).putList("regexes_to_fail_io", "^" + BlobStoreRepository.SHALLOW_SNAPSHOT_PREFIX);

        logger.info("-->  creating snapshot repository");
        createRepository(SNAP_REPO, "mock", snapshotRepoSettingsBuilder);

        logger.info("--> creating indices and index documents");
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(INDEX_NAME_1, indexSettings);
        createIndex(INDEX_NAME_2, indexSettings);

        final int numDocsInIndex1 = 5;
        final int numDocsInIndex2 = 6;
        indexDocuments(client, INDEX_NAME_1, numDocsInIndex1);
        indexDocuments(client, INDEX_NAME_2, numDocsInIndex2);
        ensureGreen(INDEX_NAME_1, INDEX_NAME_2);

        logger.info("--> create first shallow snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(SNAP_REPO, "test-snap")
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME_1, INDEX_NAME_2)
            .get();
        assertTrue(createSnapshotResponse.getSnapshotInfo().isRemoteStoreIndexShallowCopyEnabled());

        assertTrue(createSnapshotResponse.getSnapshotInfo().failedShards() > 0);
        assertSame(createSnapshotResponse.getSnapshotInfo().state(), SnapshotState.PARTIAL);
        String[] lockFiles = getLockFilesInRemoteStore(INDEX_NAME_1, BASE_REMOTE_REPO);
        assertEquals("there should be no lock files, but found " + Arrays.toString(lockFiles), 0, lockFiles.length);
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(SNAP_REPO, "test-snap").get());
    }
}
