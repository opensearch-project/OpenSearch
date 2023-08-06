/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.junit.After;
import org.junit.Before;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.client.Requests;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.InternalTestCluster;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class RemoteRestoreSnapshotIT extends AbstractSnapshotIntegTestCase {
    private static final String BASE_REMOTE_REPO = "test-rs-repo" + TEST_REMOTE_STORE_REPO_SUFFIX;
    private Path remoteRepoPath;

    @Before
    public void setup() {
        remoteRepoPath = randomRepoPath().toAbsolutePath();
        createRepository(BASE_REMOTE_REPO, "fs", remoteRepoPath);
    }

    @After
    public void teardown() {
        assertAcked(clusterAdmin().prepareDeleteRepository(BASE_REMOTE_REPO));
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.REMOTE_STORE, "true")
            .put(remoteStoreClusterSettings(BASE_REMOTE_REPO))
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

    private void assertDocsPresentInIndex(Client client, String indexName, int numOfDocs) {
        for (int i = 0; i < numOfDocs; i++) {
            String id = Integer.toString(i);
            logger.info("checking for index " + indexName + " with docId" + id);
            assertTrue("doc with id" + id + " is not present for index " + indexName, client.prepareGet(indexName, id).get().isExists());
        }
    }

    public void testRestoreOperationsShallowCopyEnabled() throws IOException, ExecutionException, InterruptedException {
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        String primary = internalCluster().startDataOnlyNode();
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String snapshotRepoName = "test-restore-snapshot-repo";
        String snapshotName1 = "test-restore-snapshot1";
        String snapshotName2 = "test-restore-snapshot2";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);
        String restoredIndexName1 = indexName1 + "-restored";
        String restoredIndexName1Seg = indexName1 + "-restored-seg";
        String restoredIndexName1Doc = indexName1 + "-restored-doc";
        String restoredIndexName2 = indexName2 + "-restored";

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName1, indexSettings);

        Settings indexSettings2 = getIndexSettings(1, 0).build();
        createIndex(indexName2, indexSettings2);

        final int numDocsInIndex1 = 5;
        final int numDocsInIndex2 = 6;
        indexDocuments(client, indexName1, numDocsInIndex1);
        indexDocuments(client, indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        internalCluster().startDataOnlyNode();
        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(true)
            .setIndices(indexName1, indexName2)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        updateRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, false));
        CreateSnapshotResponse createSnapshotResponse2 = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName2)
            .setWaitForCompletion(true)
            .setIndices(indexName1, indexName2)
            .get();
        assertThat(createSnapshotResponse2.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse2.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse2.getSnapshotInfo().totalShards())
        );
        assertThat(createSnapshotResponse2.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        DeleteResponse deleteResponse = client().prepareDelete(indexName1, "0").execute().actionGet();
        assertEquals(deleteResponse.getResult(), DocWriteResponse.Result.DELETED);
        indexDocuments(client, indexName1, numDocsInIndex1, numDocsInIndex1 + randomIntBetween(2, 5));
        ensureGreen(indexName1);

        RestoreSnapshotResponse restoreSnapshotResponse1 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(false)
            .setIndices(indexName1)
            .setRenamePattern(indexName1)
            .setRenameReplacement(restoredIndexName1)
            .get();
        RestoreSnapshotResponse restoreSnapshotResponse2 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName2)
            .setWaitForCompletion(false)
            .setIndices(indexName2)
            .setRenamePattern(indexName2)
            .setRenameReplacement(restoredIndexName2)
            .get();
        assertEquals(restoreSnapshotResponse1.status(), RestStatus.ACCEPTED);
        assertEquals(restoreSnapshotResponse2.status(), RestStatus.ACCEPTED);
        ensureGreen(restoredIndexName1, restoredIndexName2);
        assertDocsPresentInIndex(client, restoredIndexName1, numDocsInIndex1);
        assertDocsPresentInIndex(client, restoredIndexName2, numDocsInIndex2);

        // deleting data for restoredIndexName1 and restoring from remote store.
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));
        ensureRed(restoredIndexName1);
        // Re-initialize client to make sure we are not using client from stopped node.
        client = client(clusterManagerNode);
        assertAcked(client.admin().indices().prepareClose(restoredIndexName1));
        client.admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices(restoredIndexName1).restoreAllShards(true),
                PlainActionFuture.newFuture()
            );
        ensureYellowAndNoInitializingShards(restoredIndexName1);
        ensureGreen(restoredIndexName1);
        assertDocsPresentInIndex(client(), restoredIndexName1, numDocsInIndex1);
        // indexing some new docs and validating
        indexDocuments(client, restoredIndexName1, numDocsInIndex1, numDocsInIndex1 + 2);
        ensureGreen(restoredIndexName1);
        assertDocsPresentInIndex(client, restoredIndexName1, numDocsInIndex1 + 2);

        // restore index as seg rep enabled with remote store and remote translog disabled
        RestoreSnapshotResponse restoreSnapshotResponse3 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(false)
            .setIgnoreIndexSettings(IndexMetadata.SETTING_REMOTE_STORE_ENABLED)
            .setIndices(indexName1)
            .setRenamePattern(indexName1)
            .setRenameReplacement(restoredIndexName1Seg)
            .get();
        assertEquals(restoreSnapshotResponse3.status(), RestStatus.ACCEPTED);
        ensureGreen(restoredIndexName1Seg);

        GetIndexResponse getIndexResponse = client.admin()
            .indices()
            .getIndex(new GetIndexRequest().indices(restoredIndexName1Seg).includeDefaults(true))
            .get();
        indexSettings = getIndexResponse.settings().get(restoredIndexName1Seg);
        assertNull(indexSettings.get(SETTING_REMOTE_STORE_ENABLED));
        assertNull(indexSettings.get(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, null));
        assertEquals(ReplicationType.SEGMENT.toString(), indexSettings.get(IndexMetadata.SETTING_REPLICATION_TYPE));
        assertDocsPresentInIndex(client, restoredIndexName1Seg, numDocsInIndex1);
        // indexing some new docs and validating
        indexDocuments(client, restoredIndexName1Seg, numDocsInIndex1, numDocsInIndex1 + 2);
        ensureGreen(restoredIndexName1Seg);
        assertDocsPresentInIndex(client, restoredIndexName1Seg, numDocsInIndex1 + 2);

        // restore index as doc rep based from shallow copy snapshot
        RestoreSnapshotResponse restoreSnapshotResponse4 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(false)
            .setIgnoreIndexSettings(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, IndexMetadata.SETTING_REPLICATION_TYPE)
            .setIndices(indexName1)
            .setRenamePattern(indexName1)
            .setRenameReplacement(restoredIndexName1Doc)
            .get();
        assertEquals(restoreSnapshotResponse4.status(), RestStatus.ACCEPTED);
        ensureGreen(restoredIndexName1Doc);

        getIndexResponse = client.admin()
            .indices()
            .getIndex(new GetIndexRequest().indices(restoredIndexName1Doc).includeDefaults(true))
            .get();
        indexSettings = getIndexResponse.settings().get(restoredIndexName1Doc);
        assertNull(indexSettings.get(SETTING_REMOTE_STORE_ENABLED));
        assertNull(indexSettings.get(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, null));
        assertNull(indexSettings.get(IndexMetadata.SETTING_REPLICATION_TYPE));
        assertDocsPresentInIndex(client, restoredIndexName1Doc, numDocsInIndex1);
        // indexing some new docs and validating
        indexDocuments(client, restoredIndexName1Doc, numDocsInIndex1, numDocsInIndex1 + 2);
        ensureGreen(restoredIndexName1Doc);
        assertDocsPresentInIndex(client, restoredIndexName1Doc, numDocsInIndex1 + 2);
    }

    public void testRestoreInSameRemoteStoreEnabledIndex() throws IOException {
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        String primary = internalCluster().startDataOnlyNode();
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String snapshotRepoName = "test-restore-snapshot-repo";
        String snapshotName1 = "test-restore-snapshot1";
        String snapshotName2 = "test-restore-snapshot2";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);
        String restoredIndexName2 = indexName2 + "-restored";

        boolean enableShallowCopy = randomBoolean();
        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, enableShallowCopy));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName1, indexSettings);

        Settings indexSettings2 = getIndexSettings(1, 0).build();
        createIndex(indexName2, indexSettings2);

        final int numDocsInIndex1 = 5;
        final int numDocsInIndex2 = 6;
        indexDocuments(client, indexName1, numDocsInIndex1);
        indexDocuments(client, indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        internalCluster().startDataOnlyNode();
        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(true)
            .setIndices(indexName1, indexName2)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        updateRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, false));
        CreateSnapshotResponse createSnapshotResponse2 = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName2)
            .setWaitForCompletion(true)
            .setIndices(indexName1, indexName2)
            .get();
        assertThat(createSnapshotResponse2.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse2.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse2.getSnapshotInfo().totalShards())
        );
        assertThat(createSnapshotResponse2.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        DeleteResponse deleteResponse = client().prepareDelete(indexName1, "0").execute().actionGet();
        assertEquals(deleteResponse.getResult(), DocWriteResponse.Result.DELETED);
        indexDocuments(client, indexName1, numDocsInIndex1, numDocsInIndex1 + randomIntBetween(2, 5));
        ensureGreen(indexName1);

        assertAcked(client().admin().indices().prepareClose(indexName1));

        RestoreSnapshotResponse restoreSnapshotResponse1 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(false)
            .setIndices(indexName1)
            .get();
        RestoreSnapshotResponse restoreSnapshotResponse2 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName2)
            .setWaitForCompletion(false)
            .setIndices(indexName2)
            .setRenamePattern(indexName2)
            .setRenameReplacement(restoredIndexName2)
            .get();
        assertEquals(restoreSnapshotResponse1.status(), RestStatus.ACCEPTED);
        assertEquals(restoreSnapshotResponse2.status(), RestStatus.ACCEPTED);
        ensureGreen(indexName1, restoredIndexName2);
        assertDocsPresentInIndex(client, indexName1, numDocsInIndex1);
        assertDocsPresentInIndex(client, restoredIndexName2, numDocsInIndex2);

        // deleting data for restoredIndexName1 and restoring from remote store.
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));
        ensureRed(indexName1);
        // Re-initialize client to make sure we are not using client from stopped node.
        client = client(clusterManagerNode);
        assertAcked(client.admin().indices().prepareClose(indexName1));
        client.admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(indexName1).restoreAllShards(true), PlainActionFuture.newFuture());
        ensureYellowAndNoInitializingShards(indexName1);
        ensureGreen(indexName1);
        assertDocsPresentInIndex(client(), indexName1, numDocsInIndex1);
        // indexing some new docs and validating
        indexDocuments(client, indexName1, numDocsInIndex1, numDocsInIndex1 + 2);
        ensureGreen(indexName1);
        assertDocsPresentInIndex(client, indexName1, numDocsInIndex1 + 2);
    }

    public void testRestoreShallowCopySnapshotWithDifferentRepo() throws IOException {
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        String primary = internalCluster().startDataOnlyNode();
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String snapshotRepoName = "test-restore-snapshot-repo";
        String remoteStoreRepo2Name = "test-rs-repo-2" + TEST_REMOTE_STORE_REPO_SUFFIX;
        String snapshotName1 = "test-restore-snapshot1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        Path absolutePath3 = randomRepoPath().toAbsolutePath();
        String restoredIndexName1 = indexName1 + "-restored";

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, false));
        createRepository(remoteStoreRepo2Name, "fs", absolutePath3);

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName1, indexSettings);

        Settings indexSettings2 = getIndexSettings(1, 0).build();
        createIndex(indexName2, indexSettings2);

        final int numDocsInIndex1 = 5;
        final int numDocsInIndex2 = 6;
        indexDocuments(client, indexName1, numDocsInIndex1);
        indexDocuments(client, indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        internalCluster().startDataOnlyNode();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(true)
            .setIndices(indexName1, indexName2)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        Settings remoteStoreIndexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, remoteStoreRepo2Name)
            .build();
        // restore index as a remote store index with different remote store repo
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(false)
            .setIndexSettings(remoteStoreIndexSettings)
            .setIndices(indexName1)
            .setRenamePattern(indexName1)
            .setRenameReplacement(restoredIndexName1)
            .get();
        assertEquals(restoreSnapshotResponse.status(), RestStatus.ACCEPTED);
        ensureGreen(restoredIndexName1);
        assertDocsPresentInIndex(client(), restoredIndexName1, numDocsInIndex1);

        // deleting data for restoredIndexName1 and restoring from remote store.
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));
        // Re-initialize client to make sure we are not using client from stopped node.
        client = client(clusterManagerNode);
        assertAcked(client.admin().indices().prepareClose(restoredIndexName1));
        client.admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices(restoredIndexName1).restoreAllShards(true),
                PlainActionFuture.newFuture()
            );
        ensureYellowAndNoInitializingShards(restoredIndexName1);
        ensureGreen(restoredIndexName1);
        // indexing some new docs and validating
        assertDocsPresentInIndex(client, restoredIndexName1, numDocsInIndex1);
        indexDocuments(client, restoredIndexName1, numDocsInIndex1, numDocsInIndex1 + 2);
        ensureGreen(restoredIndexName1);
        assertDocsPresentInIndex(client, restoredIndexName1, numDocsInIndex1 + 2);
    }

    public void testRestoreShallowSnapshotRepositoryOverriden() throws ExecutionException, InterruptedException {
        String indexName1 = "testindex1";
        String snapshotRepoName = "test-restore-snapshot-repo";
        String remoteStoreRepoNameUpdated = "test-rs-repo-updated" + TEST_REMOTE_STORE_REPO_SUFFIX;
        String snapshotName1 = "test-restore-snapshot1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        Path absolutePath2 = randomRepoPath().toAbsolutePath();
        String[] pathTokens = absolutePath1.toString().split("/");
        String basePath = pathTokens[pathTokens.length - 1];
        Arrays.copyOf(pathTokens, pathTokens.length - 1);
        Path location = PathUtils.get(String.join("/", pathTokens));
        pathTokens = absolutePath2.toString().split("/");
        String basePath2 = pathTokens[pathTokens.length - 1];
        Arrays.copyOf(pathTokens, pathTokens.length - 1);
        Path location2 = PathUtils.get(String.join("/", pathTokens));
        logger.info("Path 1 [{}]", absolutePath1);
        logger.info("Path 2 [{}]", absolutePath2);
        String restoredIndexName1 = indexName1 + "-restored";

        createRepository(snapshotRepoName, "fs", getRepositorySettings(location, basePath, true));

        Client client = client();
        Settings indexSettings = Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s")
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        createIndex(indexName1, indexSettings);

        int numDocsInIndex1 = randomIntBetween(2, 5);
        indexDocuments(client, indexName1, numDocsInIndex1);

        ensureGreen(indexName1);

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(true)
            .setIndices(indexName1)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        createRepository(BASE_REMOTE_REPO, "fs", absolutePath2);

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(true)
            .setIndices(indexName1)
            .setRenamePattern(indexName1)
            .setRenameReplacement(restoredIndexName1)
            .get();

        assertTrue(restoreSnapshotResponse.getRestoreInfo().failedShards() > 0);

        ensureRed(restoredIndexName1);

        client().admin().indices().close(Requests.closeIndexRequest(restoredIndexName1)).get();
        createRepository(remoteStoreRepoNameUpdated, "fs", remoteRepoPath);
        RestoreSnapshotResponse restoreSnapshotResponse2 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(true)
            .setIndices(indexName1)
            .setRenamePattern(indexName1)
            .setRenameReplacement(restoredIndexName1)
            .setSourceRemoteStoreRepository(remoteStoreRepoNameUpdated)
            .get();

        assertTrue(restoreSnapshotResponse2.getRestoreInfo().failedShards() == 0);
        ensureGreen(restoredIndexName1);
        assertDocsPresentInIndex(client, restoredIndexName1, numDocsInIndex1);

        // indexing some new docs and validating
        indexDocuments(client, restoredIndexName1, numDocsInIndex1, numDocsInIndex1 + 2);
        ensureGreen(restoredIndexName1);
        assertDocsPresentInIndex(client, restoredIndexName1, numDocsInIndex1 + 2);
    }

}
