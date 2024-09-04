/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.snapshots;

import org.opensearch.action.ActionRunnable;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;

import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CloneSnapshotV2IT extends AbstractSnapshotIntegTestCase {

    public void testCloneShallowCopyV2() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        final Path remoteStoreRepoPath = randomRepoPath();
        internalCluster().startClusterManagerOnlyNode(snapshotV2Settings(remoteStoreRepoPath));
        internalCluster().startDataOnlyNode(snapshotV2Settings(remoteStoreRepoPath));
        internalCluster().startDataOnlyNode(snapshotV2Settings(remoteStoreRepoPath));

        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String indexName3 = "testindex3";
        String snapshotRepoName = "test-clone-snapshot-repo";
        String snapshotName1 = "test-create-snapshot1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Client client = client();

        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(snapshotRepoName)
                .setType(FsRepository.TYPE)
                .setSettings(
                    Settings.builder()
                        .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
                        .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
                        .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                        .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
                        .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true)
                )
        );

        createIndex(indexName1, getRemoteStoreBackedIndexSettings());
        createIndex(indexName2, getRemoteStoreBackedIndexSettings());

        final int numDocsInIndex1 = 10;
        final int numDocsInIndex2 = 20;
        indexRandomDocs(indexName1, numDocsInIndex1);
        indexRandomDocs(indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(true)
            .get();
        SnapshotInfo sourceSnapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(sourceSnapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(sourceSnapshotInfo.successfulShards(), greaterThan(0));
        assertThat(sourceSnapshotInfo.successfulShards(), equalTo(sourceSnapshotInfo.totalShards()));
        assertThat(sourceSnapshotInfo.snapshotId().getName(), equalTo(snapshotName1));

        // Validate that the snapshot was created
        final BlobStoreRepository repository = (BlobStoreRepository) internalCluster().getCurrentClusterManagerNodeInstance(
            RepositoriesService.class
        ).repository(snapshotRepoName);
        PlainActionFuture<RepositoryData> repositoryDataPlainActionFuture = new PlainActionFuture<>();
        repository.getRepositoryData(repositoryDataPlainActionFuture);

        RepositoryData repositoryData = repositoryDataPlainActionFuture.get();

        assertTrue(repositoryData.getSnapshotIds().contains(sourceSnapshotInfo.snapshotId()));

        createIndex(indexName3, getRemoteStoreBackedIndexSettings());
        indexRandomDocs(indexName3, 10);
        ensureGreen(indexName3);

        AcknowledgedResponse response = client().admin()
            .cluster()
            .prepareCloneSnapshot(snapshotRepoName, snapshotName1, "test_clone_snapshot1")
            .setIndices("*")
            .get();
        assertTrue(response.isAcknowledged());
        awaitClusterManagerFinishRepoOperations();

        // Validate that snapshot is present in repository data
        PlainActionFuture<RepositoryData> repositoryDataPlainActionFutureClone = new PlainActionFuture<>();
        repository.getRepositoryData(repositoryDataPlainActionFutureClone);

        repositoryData = repositoryDataPlainActionFutureClone.get();
        assertEquals(repositoryData.getSnapshotIds().size(), 2);
        boolean foundCloneInRepoData = false;
        SnapshotId cloneSnapshotId = null;
        for (SnapshotId snapshotId : repositoryData.getSnapshotIds()) {
            if (snapshotId.getName().equals("test_clone_snapshot1")) {
                foundCloneInRepoData = true;
                cloneSnapshotId = snapshotId;
            }
        }
        final SnapshotId cloneSnapshotIdFinal = cloneSnapshotId;
        SnapshotInfo cloneSnapshotInfo = PlainActionFuture.get(
            f -> repository.threadPool().generic().execute(ActionRunnable.supply(f, () -> repository.getSnapshotInfo(cloneSnapshotIdFinal)))
        );

        assertTrue(foundCloneInRepoData);

        assertThat(cloneSnapshotInfo.getPinnedTimestamp(), equalTo(sourceSnapshotInfo.getPinnedTimestamp()));
        assertThat(cloneSnapshotInfo.indices(), equalTo(sourceSnapshotInfo.indices()));
        assertThat(cloneSnapshotInfo.totalShards(), equalTo(sourceSnapshotInfo.totalShards()));
    }

    public void testCloneShallowCopyAfterDisablingV2() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        final Path remoteStoreRepoPath = randomRepoPath();
        internalCluster().startClusterManagerOnlyNode(snapshotV2Settings(remoteStoreRepoPath));
        internalCluster().startDataOnlyNode(snapshotV2Settings(remoteStoreRepoPath));
        internalCluster().startDataOnlyNode(snapshotV2Settings(remoteStoreRepoPath));

        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String indexName3 = "testindex3";
        String snapshotRepoName = "test-clone-snapshot-repo";
        String sourceSnapshotV2 = "test-source-snapshot-v2";
        String sourceSnapshotV1 = "test-source-snapshot-v1";
        String cloneSnapshotV2 = "test-clone-snapshot-v2";
        String cloneSnapshotV1 = "test-clone-snapshot-v1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Client client = client();

        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(snapshotRepoName)
                .setType(FsRepository.TYPE)
                .setSettings(
                    Settings.builder()
                        .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
                        .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
                        .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                        .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
                        .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true)
                )
        );

        createIndex(indexName1, getRemoteStoreBackedIndexSettings());
        createIndex(indexName2, getRemoteStoreBackedIndexSettings());

        final int numDocsInIndex1 = 10;
        final int numDocsInIndex2 = 20;
        indexRandomDocs(indexName1, numDocsInIndex1);
        indexRandomDocs(indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        // create source snapshot which is v2
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, sourceSnapshotV2)
            .setWaitForCompletion(true)
            .get();
        SnapshotInfo sourceSnapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(sourceSnapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(sourceSnapshotInfo.successfulShards(), greaterThan(0));
        assertThat(sourceSnapshotInfo.successfulShards(), equalTo(sourceSnapshotInfo.totalShards()));
        assertThat(sourceSnapshotInfo.snapshotId().getName(), equalTo(sourceSnapshotV2));

        // Validate that the snapshot was created
        final BlobStoreRepository repository = (BlobStoreRepository) internalCluster().getCurrentClusterManagerNodeInstance(
            RepositoriesService.class
        ).repository(snapshotRepoName);
        PlainActionFuture<RepositoryData> repositoryDataPlainActionFuture = new PlainActionFuture<>();
        repository.getRepositoryData(repositoryDataPlainActionFuture);

        RepositoryData repositoryData = repositoryDataPlainActionFuture.get();

        assertTrue(repositoryData.getSnapshotIds().contains(sourceSnapshotInfo.snapshotId()));

        createIndex(indexName3, getRemoteStoreBackedIndexSettings());
        indexRandomDocs(indexName3, 10);
        ensureGreen(indexName3);

        // disable snapshot v2 in repo
        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(snapshotRepoName)
                .setType(FsRepository.TYPE)
                .setSettings(
                    Settings.builder()
                        .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
                        .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
                        .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                        .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
                        .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), false)
                )
        );

        // validate that the created snapshot is v1
        CreateSnapshotResponse createSnapshotResponseV1 = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, sourceSnapshotV1)
            .setWaitForCompletion(true)
            .get();
        SnapshotInfo sourceSnapshotInfoV1 = createSnapshotResponseV1.getSnapshotInfo();
        assertThat(sourceSnapshotInfoV1.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(sourceSnapshotInfoV1.successfulShards(), greaterThan(0));
        assertThat(sourceSnapshotInfoV1.successfulShards(), equalTo(sourceSnapshotInfoV1.totalShards()));
        assertThat(sourceSnapshotInfoV1.getPinnedTimestamp(), equalTo(0L));

        // Validate that snapshot is present in repository data
        PlainActionFuture<RepositoryData> repositoryDataV1PlainActionFuture = new PlainActionFuture<>();
        BlobStoreRepository repositoryV1 = (BlobStoreRepository) internalCluster().getCurrentClusterManagerNodeInstance(
            RepositoriesService.class
        ).repository(snapshotRepoName);
        repositoryV1.getRepositoryData(repositoryDataV1PlainActionFuture);

        repositoryData = repositoryDataV1PlainActionFuture.get();

        assertTrue(repositoryData.getSnapshotIds().contains(sourceSnapshotInfoV1.snapshotId()));
        assertEquals(repositoryData.getSnapshotIds().size(), 2);

        // clone should get created for v2 snapshot
        AcknowledgedResponse response = client().admin()
            .cluster()
            .prepareCloneSnapshot(snapshotRepoName, sourceSnapshotV2, cloneSnapshotV2)
            .setIndices("*")
            .get();
        assertTrue(response.isAcknowledged());
        awaitClusterManagerFinishRepoOperations();

        // Validate that snapshot is present in repository data
        PlainActionFuture<RepositoryData> repositoryDataCloneV2PlainActionFuture = new PlainActionFuture<>();
        BlobStoreRepository repositoryCloneV2 = (BlobStoreRepository) internalCluster().getCurrentClusterManagerNodeInstance(
            RepositoriesService.class
        ).repository(snapshotRepoName);
        repositoryCloneV2.getRepositoryData(repositoryDataCloneV2PlainActionFuture);

        repositoryData = repositoryDataCloneV2PlainActionFuture.get();

        assertEquals(repositoryData.getSnapshotIds().size(), 3);
        boolean foundCloneInRepoData = false;
        SnapshotId cloneSnapshotId = null;
        for (SnapshotId snapshotId : repositoryData.getSnapshotIds()) {
            if (snapshotId.getName().equals(cloneSnapshotV2)) {
                foundCloneInRepoData = true;
                cloneSnapshotId = snapshotId;
            }
        }
        final SnapshotId cloneSnapshotIdFinal = cloneSnapshotId;
        SnapshotInfo cloneSnapshotInfo = PlainActionFuture.get(
            f -> repository.threadPool().generic().execute(ActionRunnable.supply(f, () -> repository.getSnapshotInfo(cloneSnapshotIdFinal)))
        );

        assertTrue(foundCloneInRepoData);
        // pinned timestamp value in clone snapshot v2 matches source snapshot v2
        assertThat(cloneSnapshotInfo.getPinnedTimestamp(), equalTo(sourceSnapshotInfo.getPinnedTimestamp()));
        for (String index : sourceSnapshotInfo.indices()) {
            assertTrue(cloneSnapshotInfo.indices().contains(index));

        }

    }

    private Settings snapshotV2Settings(Path remoteStoreRepoPath) {
        String REMOTE_REPO_NAME = "remote-store-repo-name";
        Settings settings = Settings.builder()
            .put(remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath))
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), true)
            .build();
        return settings;
    }
}
