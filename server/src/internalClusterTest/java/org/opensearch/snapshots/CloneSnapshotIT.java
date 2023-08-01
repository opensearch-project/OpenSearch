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

import org.opensearch.action.ActionFuture;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotIndexStatus;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.snapshots.blobstore.IndexShardSnapshot;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

import org.opensearch.action.ActionRunnable;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.UUIDs;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.opensearch.index.snapshots.blobstore.SnapshotFiles;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoryShardId;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CloneSnapshotIT extends AbstractSnapshotIntegTestCase {

    public void testShardClone() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "fs", repoPath);

        final String indexName = "test-index";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));
        final String sourceSnapshot = "source-snapshot";
        final SnapshotInfo sourceSnapshotInfo = createFullSnapshot(repoName, sourceSnapshot);

        final BlobStoreRepository repository = (BlobStoreRepository) internalCluster().getCurrentClusterManagerNodeInstance(
            RepositoriesService.class
        ).repository(repoName);
        final RepositoryData repositoryData = getRepositoryData(repoName);
        final IndexId indexId = repositoryData.resolveIndexId(indexName);
        final int shardId = 0;
        final RepositoryShardId repositoryShardId = new RepositoryShardId(indexId, shardId);

        final SnapshotId targetSnapshotId = new SnapshotId("target-snapshot", UUIDs.randomBase64UUID(random()));

        final String currentShardGen = repositoryData.shardGenerations().getShardGen(indexId, shardId);
        final String newShardGeneration = PlainActionFuture.get(
            f -> repository.cloneShardSnapshot(sourceSnapshotInfo.snapshotId(), targetSnapshotId, repositoryShardId, currentShardGen, f)
        );

        final BlobStoreIndexShardSnapshot targetShardSnapshot = readShardSnapshot(repository, repositoryShardId, targetSnapshotId);
        final BlobStoreIndexShardSnapshot sourceShardSnapshot = readShardSnapshot(
            repository,
            repositoryShardId,
            sourceSnapshotInfo.snapshotId()
        );
        assertThat(targetShardSnapshot.incrementalFileCount(), is(0));
        final List<BlobStoreIndexShardSnapshot.FileInfo> sourceFiles = sourceShardSnapshot.indexFiles();
        final List<BlobStoreIndexShardSnapshot.FileInfo> targetFiles = targetShardSnapshot.indexFiles();
        final int fileCount = sourceFiles.size();
        assertEquals(fileCount, targetFiles.size());
        for (int i = 0; i < fileCount; i++) {
            assertTrue(sourceFiles.get(i).isSame(targetFiles.get(i)));
        }
        final BlobStoreIndexShardSnapshots shardMetadata = readShardGeneration(repository, repositoryShardId, newShardGeneration);
        final List<SnapshotFiles> snapshotFiles = shardMetadata.snapshots();
        assertThat(snapshotFiles, hasSize(2));
        assertTrue(snapshotFiles.get(0).isSame(snapshotFiles.get(1)));

        // verify that repeated cloning is idempotent
        final String newShardGeneration2 = PlainActionFuture.get(
            f -> repository.cloneShardSnapshot(sourceSnapshotInfo.snapshotId(), targetSnapshotId, repositoryShardId, newShardGeneration, f)
        );
        assertEquals(newShardGeneration, newShardGeneration2);
    }

    public void testCloneSnapshotIndex() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        createRepository(repoName, "fs");

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));
        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        indexRandomDocs(indexName, randomIntBetween(20, 100));
        if (randomBoolean()) {
            assertAcked(admin().indices().prepareDelete(indexName));
        }
        final String targetSnapshot = "target-snapshot";
        assertAcked(startClone(repoName, sourceSnapshot, targetSnapshot, indexName).get());

        final List<SnapshotStatus> status = clusterAdmin().prepareSnapshotStatus(repoName)
            .setSnapshots(sourceSnapshot, targetSnapshot)
            .get()
            .getSnapshots();
        assertThat(status, hasSize(2));
        final SnapshotIndexStatus status1 = status.get(0).getIndices().get(indexName);
        final SnapshotIndexStatus status2 = status.get(1).getIndices().get(indexName);
        assertEquals(status1.getStats().getTotalFileCount(), status2.getStats().getTotalFileCount());
        assertEquals(status1.getStats().getTotalSize(), status2.getStats().getTotalSize());
    }

    public void testCloneShallowSnapshotIndex() throws Exception {
        disableRepoConsistencyCheck("This test uses remote store repository");
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        final String remoteStoreRepoName = "remote-store-repo-name";
        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(remoteStoreRepoName));
        internalCluster().startDataOnlyNode();

        final String snapshotRepoName = "snapshot-repo-name";
        final Path snapshotRepoPath = randomRepoPath();
        createRepository(snapshotRepoName, "fs", snapshotRepoPath);

        final String shallowSnapshotRepoName = "shallow-snapshot-repo-name";
        final Path shallowSnapshotRepoPath = randomRepoPath();
        createRepository(shallowSnapshotRepoName, "fs", snapshotRepoSettingsForShallowCopy(shallowSnapshotRepoPath));

        final Path remoteStoreRepoPath = randomRepoPath();
        createRepository(remoteStoreRepoName, "fs", remoteStoreRepoPath);

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        final String snapshot = "snapshot";
        createFullSnapshot(snapshotRepoName, snapshot);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length == 0);

        indexRandomDocs(indexName, randomIntBetween(20, 100));

        final String shallowSnapshot = "shallow-snapshot";
        createFullSnapshot(shallowSnapshotRepoName, shallowSnapshot);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length == 1);

        if (randomBoolean()) {
            assertAcked(admin().indices().prepareDelete(indexName));
        }

        final String sourceSnapshot = shallowSnapshot;
        final String targetSnapshot = "target-snapshot";
        assertAcked(startClone(shallowSnapshotRepoName, sourceSnapshot, targetSnapshot, indexName, remoteStoreEnabledIndexName).get());
        logger.info("Lock files count: {}", getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length == 2);
    }

    public void testShallowCloneNameAvailability() throws Exception {
        disableRepoConsistencyCheck("This test uses remote store repository");
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        final String remoteStoreRepoName = "remote-store-repo-name";
        internalCluster().startClusterManagerOnlyNode(
            Settings.builder().put(LARGE_SNAPSHOT_POOL_SETTINGS).put(remoteStoreClusterSettings(remoteStoreRepoName)).build()
        );
        internalCluster().startDataOnlyNode();

        final String shallowSnapshotRepoName = "shallow-snapshot-repo-name";
        final Path shallowSnapshotRepoPath = randomRepoPath();
        createRepository(shallowSnapshotRepoName, "fs", snapshotRepoSettingsForShallowCopy(shallowSnapshotRepoPath));

        final Path remoteStoreRepoPath = randomRepoPath();
        createRepository(remoteStoreRepoName, "fs", remoteStoreRepoPath);

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        final String shallowSnapshot1 = "snapshot1";
        createFullSnapshot(shallowSnapshotRepoName, shallowSnapshot1);

        final String shallowSnapshot2 = "snapshot2";
        createFullSnapshot(shallowSnapshotRepoName, shallowSnapshot2);

        ExecutionException ex = expectThrows(
            ExecutionException.class,
            () -> startClone(shallowSnapshotRepoName, shallowSnapshot1, shallowSnapshot2, indexName, remoteStoreEnabledIndexName).get()
        );
        assertThat(ex.getMessage(), containsString("snapshot with the same name already exists"));
    }

    public void testCloneAfterRepoShallowSettingEnabled() throws Exception {
        disableRepoConsistencyCheck("This test uses remote store repository");
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        final String remoteStoreRepoName = "remote-store-repo-name";
        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(remoteStoreRepoName));
        internalCluster().startDataOnlyNode();

        final String snapshotRepoName = "snapshot-repo-name";
        final Path snapshotRepoPath = randomRepoPath();
        createRepository(snapshotRepoName, "fs", snapshotRepoPath);

        final Path remoteStoreRepoPath = randomRepoPath();
        createRepository(remoteStoreRepoName, "fs", remoteStoreRepoPath);

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        final String snapshot = "snapshot";
        createFullSnapshot(snapshotRepoName, snapshot);
        assertEquals(getSnapshot(snapshotRepoName, snapshot).state(), SnapshotState.SUCCESS);

        // Updating the snapshot repository flag to enable shallow snapshots
        createRepository(snapshotRepoName, "fs", snapshotRepoSettingsForShallowCopy(snapshotRepoPath));
        RepositoryMetadata updatedRepositoryMetadata = clusterAdmin().prepareGetRepositories(snapshotRepoName).get().repositories().get(0);
        assertTrue(updatedRepositoryMetadata.settings().getAsBoolean(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), false));

        final String targetSnapshot = "target-snapshot";
        assertAcked(startClone(snapshotRepoName, snapshot, targetSnapshot, indexName, remoteStoreEnabledIndexName).get());
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length == 0);
        assertEquals(getSnapshot(snapshotRepoName, targetSnapshot).isRemoteStoreIndexShallowCopyEnabled(), false);
    }

    public void testCloneAfterRepoShallowSettingDisabled() throws Exception {
        disableRepoConsistencyCheck("This test uses remote store repository");
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        final String remoteStoreRepoName = "remote-store-repo-name";
        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(remoteStoreRepoName));
        internalCluster().startDataOnlyNode();

        final String snapshotRepoName = "snapshot-repo-name";
        final Path snapshotRepoPath = randomRepoPath();
        createRepository(snapshotRepoName, "fs", snapshotRepoSettingsForShallowCopy(snapshotRepoPath));

        final Path remoteStoreRepoPath = randomRepoPath();
        createRepository(remoteStoreRepoName, "fs", remoteStoreRepoPath);

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        final String snapshot = "snapshot";
        createFullSnapshot(snapshotRepoName, snapshot);
        assertEquals(getSnapshot(snapshotRepoName, snapshot).state(), SnapshotState.SUCCESS);

        // Updating the snapshot repository flag to enable shallow snapshots
        createRepository(snapshotRepoName, "fs", snapshotRepoPath);
        RepositoryMetadata updatedRepositoryMetadata = clusterAdmin().prepareGetRepositories(snapshotRepoName).get().repositories().get(0);
        assertFalse(updatedRepositoryMetadata.settings().getAsBoolean(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), false));

        final String targetSnapshot = "target-snapshot";
        assertAcked(startClone(snapshotRepoName, snapshot, targetSnapshot, indexName, remoteStoreEnabledIndexName).get());
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length == 2);
        assertEquals(getSnapshot(snapshotRepoName, targetSnapshot).isRemoteStoreIndexShallowCopyEnabled(), true);
    }

    public void testClonePreventsSnapshotDelete() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        createRepository(repoName, "mock");

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));
        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        indexRandomDocs(indexName, randomIntBetween(20, 100));

        final String targetSnapshot = "target-snapshot";
        blockNodeOnAnyFiles(repoName, clusterManagerName);
        final ActionFuture<AcknowledgedResponse> cloneFuture = startClone(repoName, sourceSnapshot, targetSnapshot, indexName);
        waitForBlock(clusterManagerName, repoName, TimeValue.timeValueSeconds(30L));
        assertFalse(cloneFuture.isDone());

        ConcurrentSnapshotExecutionException ex = expectThrows(
            ConcurrentSnapshotExecutionException.class,
            () -> startDeleteSnapshot(repoName, sourceSnapshot).actionGet()
        );
        assertThat(ex.getMessage(), containsString("cannot delete snapshot while it is being cloned"));

        unblockNode(repoName, clusterManagerName);
        assertAcked(cloneFuture.get());
        final List<SnapshotStatus> status = clusterAdmin().prepareSnapshotStatus(repoName)
            .setSnapshots(sourceSnapshot, targetSnapshot)
            .get()
            .getSnapshots();
        assertThat(status, hasSize(2));
        final SnapshotIndexStatus status1 = status.get(0).getIndices().get(indexName);
        final SnapshotIndexStatus status2 = status.get(1).getIndices().get(indexName);
        assertEquals(status1.getStats().getTotalFileCount(), status2.getStats().getTotalFileCount());
        assertEquals(status1.getStats().getTotalSize(), status2.getStats().getTotalSize());
    }

    public void testConcurrentCloneAndSnapshot() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        createRepository(repoName, "mock");

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));
        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        indexRandomDocs(indexName, randomIntBetween(20, 100));

        final String targetSnapshot = "target-snapshot";
        final ActionFuture<CreateSnapshotResponse> snapshot2Future = startFullSnapshotBlockedOnDataNode("snapshot-2", repoName, dataNode);
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));
        final ActionFuture<AcknowledgedResponse> cloneFuture = startClone(repoName, sourceSnapshot, targetSnapshot, indexName);
        awaitNumberOfSnapshotsInProgress(2);
        unblockNode(repoName, dataNode);
        assertAcked(cloneFuture.get());
        assertSuccessful(snapshot2Future);
    }

    public void testLongRunningCloneAllowsConcurrentSnapshot() throws Exception {
        // large snapshot pool so blocked snapshot threads from cloning don't prevent concurrent snapshot finalizations
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String indexSlow = "index-slow";
        createIndexWithContent(indexSlow);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        final String targetSnapshot = "target-snapshot";
        blockClusterManagerOnShardClone(repoName);
        final ActionFuture<AcknowledgedResponse> cloneFuture = startClone(repoName, sourceSnapshot, targetSnapshot, indexSlow);
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));

        final String indexFast = "index-fast";
        createIndexWithRandomDocs(indexFast, randomIntBetween(20, 100));

        assertSuccessful(
            clusterAdmin().prepareCreateSnapshot(repoName, "fast-snapshot").setIndices(indexFast).setWaitForCompletion(true).execute()
        );

        assertThat(cloneFuture.isDone(), is(false));
        unblockNode(repoName, clusterManagerNode);

        assertAcked(cloneFuture.get());
    }

    public void testLongRunningSnapshotAllowsConcurrentClone() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String indexSlow = "index-slow";
        createIndexWithContent(indexSlow);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        final String indexFast = "index-fast";
        createIndexWithRandomDocs(indexFast, randomIntBetween(20, 100));

        blockDataNode(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = clusterAdmin().prepareCreateSnapshot(repoName, "fast-snapshot")
            .setIndices(indexFast)
            .setWaitForCompletion(true)
            .execute();
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        final String targetSnapshot = "target-snapshot";
        assertAcked(startClone(repoName, sourceSnapshot, targetSnapshot, indexSlow).get());

        assertThat(snapshotFuture.isDone(), is(false));
        unblockNode(repoName, dataNode);

        assertSuccessful(snapshotFuture);
    }

    public void testDeletePreventsClone() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        createRepository(repoName, "mock");

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));
        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        indexRandomDocs(indexName, randomIntBetween(20, 100));

        final String targetSnapshot = "target-snapshot";
        blockNodeOnAnyFiles(repoName, clusterManagerName);
        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteSnapshot(repoName, sourceSnapshot);
        waitForBlock(clusterManagerName, repoName, TimeValue.timeValueSeconds(30L));
        assertFalse(deleteFuture.isDone());

        ConcurrentSnapshotExecutionException ex = expectThrows(
            ConcurrentSnapshotExecutionException.class,
            () -> startClone(repoName, sourceSnapshot, targetSnapshot, indexName).actionGet()
        );
        assertThat(ex.getMessage(), containsString("cannot clone from snapshot that is being deleted"));

        unblockNode(repoName, clusterManagerName);
        assertAcked(deleteFuture.get());
    }

    public void testBackToBackClonesForIndexNotInCluster() throws Exception {
        // large snapshot pool so blocked snapshot threads from cloning don't prevent concurrent snapshot finalizations
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String indexBlocked = "index-blocked";
        createIndexWithContent(indexBlocked);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        assertAcked(admin().indices().prepareDelete(indexBlocked).get());

        final String targetSnapshot1 = "target-snapshot";
        blockClusterManagerOnShardClone(repoName);
        final ActionFuture<AcknowledgedResponse> cloneFuture1 = startClone(repoName, sourceSnapshot, targetSnapshot1, indexBlocked);
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));
        assertThat(cloneFuture1.isDone(), is(false));

        final int extraClones = randomIntBetween(1, 5);
        final List<ActionFuture<AcknowledgedResponse>> extraCloneFutures = new ArrayList<>(extraClones);
        for (int i = 0; i < extraClones; i++) {
            extraCloneFutures.add(startClone(repoName, sourceSnapshot, "target-snapshot-" + i, indexBlocked));
        }
        awaitNumberOfSnapshotsInProgress(1 + extraClones);
        for (ActionFuture<AcknowledgedResponse> extraCloneFuture : extraCloneFutures) {
            assertFalse(extraCloneFuture.isDone());
        }

        final int extraSnapshots = randomIntBetween(0, 5);
        if (extraSnapshots > 0) {
            createIndexWithContent(indexBlocked);
        }

        final List<ActionFuture<CreateSnapshotResponse>> extraSnapshotFutures = new ArrayList<>(extraSnapshots);
        for (int i = 0; i < extraSnapshots; i++) {
            extraSnapshotFutures.add(startFullSnapshot(repoName, "extra-snap-" + i));
        }

        awaitNumberOfSnapshotsInProgress(1 + extraClones + extraSnapshots);
        for (ActionFuture<CreateSnapshotResponse> extraSnapshotFuture : extraSnapshotFutures) {
            assertFalse(extraSnapshotFuture.isDone());
        }

        unblockNode(repoName, clusterManagerNode);
        assertAcked(cloneFuture1.get());

        for (ActionFuture<AcknowledgedResponse> extraCloneFuture : extraCloneFutures) {
            assertAcked(extraCloneFuture.get());
        }
        for (ActionFuture<CreateSnapshotResponse> extraSnapshotFuture : extraSnapshotFutures) {
            assertSuccessful(extraSnapshotFuture);
        }
    }

    public void testClusterManagerFailoverDuringCloneStep1() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        blockClusterManagerOnReadIndexMeta(repoName);
        final String cloneName = "target-snapshot";
        final ActionFuture<AcknowledgedResponse> cloneFuture = startCloneFromDataNode(repoName, sourceSnapshot, cloneName, testIndex);
        awaitNumberOfSnapshotsInProgress(1);
        final String clusterManagerNode = internalCluster().getClusterManagerName();
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));
        internalCluster().restartNode(clusterManagerNode);
        boolean cloneSucceeded = false;
        try {
            cloneFuture.actionGet(TimeValue.timeValueSeconds(30L));
            cloneSucceeded = true;
        } catch (SnapshotException sne) {
            // ignored, most of the time we will throw here but we could randomly run into a situation where the data node retries the
            // snapshot on disconnect slowly enough for it to work out
        }

        awaitNoMoreRunningOperations(internalCluster().getClusterManagerName());

        // Check if the clone operation worked out by chance as a result of the clone request being retried
        // because of the cluster-manager failover
        cloneSucceeded = cloneSucceeded
            || getRepositoryData(repoName).getSnapshotIds().stream().anyMatch(snapshotId -> snapshotId.getName().equals(cloneName));
        assertAllSnapshotsSuccessful(getRepositoryData(repoName), cloneSucceeded ? 2 : 1);
    }

    public void testFailsOnCloneMissingIndices() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        final Path repoPath = randomRepoPath();
        if (randomBoolean()) {
            createIndexWithContent("test-idx");
        }
        createRepository(repoName, "fs", repoPath);

        final String snapshotName = "snapshot";
        createFullSnapshot(repoName, snapshotName);
        expectThrows(
            IndexNotFoundException.class,
            () -> startClone(repoName, snapshotName, "target-snapshot", "does-not-exist").actionGet()
        );
    }

    public void testClusterManagerFailoverDuringCloneStep2() throws Exception {
        // large snapshot pool so blocked snapshot threads from cloning don't prevent concurrent snapshot finalizations
        internalCluster().startClusterManagerOnlyNodes(3, LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        final String targetSnapshot = "target-snapshot";
        blockClusterManagerOnShardClone(repoName);
        final ActionFuture<AcknowledgedResponse> cloneFuture = startCloneFromDataNode(repoName, sourceSnapshot, targetSnapshot, testIndex);
        awaitNumberOfSnapshotsInProgress(1);
        final String clusterManagerNode = internalCluster().getClusterManagerName();
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));
        internalCluster().restartNode(clusterManagerNode);
        expectThrows(SnapshotException.class, cloneFuture::actionGet);
        awaitNoMoreRunningOperations(internalCluster().getClusterManagerName());

        assertAllSnapshotsSuccessful(getRepositoryData(repoName), 2);
    }

    public void testExceptionDuringShardClone() throws Exception {
        // large snapshot pool so blocked snapshot threads from cloning don't prevent concurrent snapshot finalizations
        internalCluster().startClusterManagerOnlyNodes(3, LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        final String targetSnapshot = "target-snapshot";
        blockClusterManagerFromFinalizingSnapshotOnSnapFile(repoName);
        final ActionFuture<AcknowledgedResponse> cloneFuture = startCloneFromDataNode(repoName, sourceSnapshot, targetSnapshot, testIndex);
        awaitNumberOfSnapshotsInProgress(1);
        final String clusterManagerNode = internalCluster().getClusterManagerName();
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));
        unblockNode(repoName, clusterManagerNode);
        expectThrows(SnapshotException.class, cloneFuture::actionGet);
        awaitNoMoreRunningOperations(internalCluster().getClusterManagerName());
        assertAllSnapshotsSuccessful(getRepositoryData(repoName), 1);
        assertAcked(startDeleteSnapshot(repoName, sourceSnapshot).get());
    }

    public void testDoesNotStartOnBrokenSourceSnapshot() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final String sourceSnapshot = "source-snapshot";
        blockDataNode(repoName, dataNode);
        final Client clusterManagerClient = internalCluster().clusterManagerClient();
        final ActionFuture<CreateSnapshotResponse> sourceSnapshotFuture = clusterManagerClient.admin()
            .cluster()
            .prepareCreateSnapshot(repoName, sourceSnapshot)
            .setWaitForCompletion(true)
            .execute();
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));
        internalCluster().restartNode(dataNode);
        assertThat(sourceSnapshotFuture.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));

        final SnapshotException sne = expectThrows(
            SnapshotException.class,
            () -> startClone(clusterManagerClient, repoName, sourceSnapshot, "target-snapshot", testIndex).actionGet(
                TimeValue.timeValueSeconds(30L)
            )
        );
        assertThat(
            sne.getMessage(),
            containsString(
                "Can't clone index [" + getRepositoryData(repoName).resolveIndexId(testIndex) + "] because its snapshot was not successful."
            )
        );
    }

    public void testStartSnapshotWithSuccessfulShardClonePendingFinalization() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String indexName = "test-idx";
        createIndexWithContent(indexName);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        blockClusterManagerOnWriteIndexFile(repoName);
        final String cloneName = "clone-blocked";
        final ActionFuture<AcknowledgedResponse> blockedClone = startClone(repoName, sourceSnapshot, cloneName, indexName);
        waitForBlock(clusterManagerName, repoName, TimeValue.timeValueSeconds(30L));
        awaitNumberOfSnapshotsInProgress(1);
        blockNodeOnAnyFiles(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> otherSnapshot = startFullSnapshot(repoName, "other-snapshot");
        awaitNumberOfSnapshotsInProgress(2);
        assertFalse(blockedClone.isDone());
        unblockNode(repoName, clusterManagerName);
        awaitNumberOfSnapshotsInProgress(1);
        awaitClusterManagerFinishRepoOperations();
        unblockNode(repoName, dataNode);
        assertAcked(blockedClone.get());
        assertEquals(getSnapshot(repoName, cloneName).state(), SnapshotState.SUCCESS);
        assertSuccessful(otherSnapshot);
    }

    public void testStartCloneWithSuccessfulShardClonePendingFinalization() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String indexName = "test-idx";
        createIndexWithContent(indexName);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        blockClusterManagerOnWriteIndexFile(repoName);
        final String cloneName = "clone-blocked";
        final ActionFuture<AcknowledgedResponse> blockedClone = startClone(repoName, sourceSnapshot, cloneName, indexName);
        waitForBlock(clusterManagerName, repoName, TimeValue.timeValueSeconds(30L));
        awaitNumberOfSnapshotsInProgress(1);
        final String otherCloneName = "other-clone";
        final ActionFuture<AcknowledgedResponse> otherClone = startClone(repoName, sourceSnapshot, otherCloneName, indexName);
        awaitNumberOfSnapshotsInProgress(2);
        assertFalse(blockedClone.isDone());
        unblockNode(repoName, clusterManagerName);
        awaitNoMoreRunningOperations(clusterManagerName);
        awaitClusterManagerFinishRepoOperations();
        assertAcked(blockedClone.get());
        assertAcked(otherClone.get());
        assertEquals(getSnapshot(repoName, cloneName).state(), SnapshotState.SUCCESS);
        assertEquals(getSnapshot(repoName, otherCloneName).state(), SnapshotState.SUCCESS);
    }

    public void testStartCloneWithSuccessfulShardSnapshotPendingFinalization() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String indexName = "test-idx";
        createIndexWithContent(indexName);

        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        blockClusterManagerOnWriteIndexFile(repoName);
        final ActionFuture<CreateSnapshotResponse> blockedSnapshot = startFullSnapshot(repoName, "snap-blocked");
        waitForBlock(clusterManagerName, repoName, TimeValue.timeValueSeconds(30L));
        awaitNumberOfSnapshotsInProgress(1);
        final String cloneName = "clone";
        final ActionFuture<AcknowledgedResponse> clone = startClone(repoName, sourceSnapshot, cloneName, indexName);
        logger.info("--> wait for clone to start fully with shards assigned in the cluster state");
        try {
            awaitClusterState(clusterState -> {
                final List<SnapshotsInProgress.Entry> entries = clusterState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                    .entries();
                return entries.size() == 2 && entries.get(1).clones().isEmpty() == false;
            });
            assertFalse(blockedSnapshot.isDone());
        } finally {
            unblockNode(repoName, clusterManagerName);
        }
        awaitNoMoreRunningOperations();

        awaitClusterManagerFinishRepoOperations();

        assertSuccessful(blockedSnapshot);
        assertAcked(clone.get());
        assertEquals(getSnapshot(repoName, cloneName).state(), SnapshotState.SUCCESS);
    }

    private ActionFuture<AcknowledgedResponse> startCloneFromDataNode(
        String repoName,
        String sourceSnapshot,
        String targetSnapshot,
        String... indices
    ) {
        return startClone(dataNodeClient(), repoName, sourceSnapshot, targetSnapshot, indices);
    }

    private ActionFuture<AcknowledgedResponse> startClone(
        String repoName,
        String sourceSnapshot,
        String targetSnapshot,
        String... indices
    ) {
        return startClone(client(), repoName, sourceSnapshot, targetSnapshot, indices);
    }

    private static ActionFuture<AcknowledgedResponse> startClone(
        Client client,
        String repoName,
        String sourceSnapshot,
        String targetSnapshot,
        String... indices
    ) {
        return client.admin().cluster().prepareCloneSnapshot(repoName, sourceSnapshot, targetSnapshot).setIndices(indices).execute();
    }

    private void blockClusterManagerOnReadIndexMeta(String repoName) {
        ((MockRepository) internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class).repository(repoName))
            .setBlockOnReadIndexMeta();
    }

    private void blockClusterManagerOnShardClone(String repoName) {
        ((MockRepository) internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class).repository(repoName))
            .setBlockOnWriteShardLevelMeta();
    }

    /**
     * Assert that given {@link RepositoryData} contains exactly the given number of snapshots and all of them are successful.
     */
    private static void assertAllSnapshotsSuccessful(RepositoryData repositoryData, int successfulSnapshotCount) {
        final Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
        assertThat(snapshotIds, hasSize(successfulSnapshotCount));
        for (SnapshotId snapshotId : snapshotIds) {
            assertThat(repositoryData.getSnapshotState(snapshotId), is(SnapshotState.SUCCESS));
        }
    }

    private static BlobStoreIndexShardSnapshots readShardGeneration(
        BlobStoreRepository repository,
        RepositoryShardId repositoryShardId,
        String generation
    ) {
        return PlainActionFuture.get(
            f -> repository.threadPool()
                .generic()
                .execute(
                    ActionRunnable.supply(
                        f,
                        () -> BlobStoreRepository.INDEX_SHARD_SNAPSHOTS_FORMAT.read(
                            repository.shardContainer(repositoryShardId.index(), repositoryShardId.shardId()),
                            generation,
                            NamedXContentRegistry.EMPTY
                        )
                    )
                )
        );
    }

    private static BlobStoreIndexShardSnapshot readShardSnapshot(
        BlobStoreRepository repository,
        RepositoryShardId repositoryShardId,
        SnapshotId snapshotId
    ) {
        return PlainActionFuture.get(f -> repository.threadPool().generic().execute(ActionRunnable.supply(f, () -> {
            IndexShardSnapshot indexShardSnapshot = repository.loadShardSnapshot(
                repository.shardContainer(repositoryShardId.index(), repositoryShardId.shardId()),
                snapshotId
            );
            assert indexShardSnapshot instanceof BlobStoreIndexShardSnapshot
                : "indexShardSnapshot should be an instance of BlobStoreIndexShardSnapshot";
            return (BlobStoreIndexShardSnapshot) indexShardSnapshot;
        })));
    }
}
