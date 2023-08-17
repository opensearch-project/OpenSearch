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

package org.opensearch.repositories.blobstore;

import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.snapshots.blobstore.RemoteStoreShardShallowCopySnapshot;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManagerUtils;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.indices.IndicesService.CLUSTER_REPLICATION_TYPE_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_STORE_ENABLED_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_TRANSLOG_REPOSITORY_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Tests for the {@link BlobStoreRepository} and its subclasses.
 */
public class BlobStoreRepositoryRemoteIndexTests extends BlobStoreRepositoryHelperTests {
    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.REMOTE_STORE, "true").build();
    }

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(BlobStoreRepositoryTests.FsLikeRepoPlugin.class, MockRepository.Plugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .put(CLUSTER_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
            .put(CLUSTER_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.getKey(), "test-rs-repo")
            .put(CLUSTER_REMOTE_TRANSLOG_REPOSITORY_SETTING.getKey(), "test-rs-repo")
            .build();
    }

    // Validate Scenario Normal Snapshot -> remoteStoreShallowCopy Snapshot -> normal Snapshot
    public void testRetrieveShallowCopySnapshotCase1() throws IOException {
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        final Client client = client();
        final String snapshotRepositoryName = "test-repo";
        final String remoteStoreRepositoryName = "test-rs-repo";

        logger.info("-->  creating snapshot repository");

        Settings snapshotRepoSettings = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .build();
        createRepository(client, snapshotRepositoryName, snapshotRepoSettings);

        logger.info("-->  creating remote store repository");
        Settings remoteStoreRepoSettings = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .build();
        createRepository(client, remoteStoreRepositoryName, remoteStoreRepoSettings);

        logger.info("--> creating an index and indexing documents");
        final String indexName = "test-idx";
        createIndex(indexName);
        ensureGreen();
        indexDocuments(client, indexName);

        logger.info("--> creating a remote store enabled index and indexing documents");
        final String remoteStoreIndexName = "test-rs-idx";
        Settings indexSettings = getRemoteStoreBackedIndexSettings(remoteStoreRepositoryName);
        createIndex(remoteStoreIndexName, indexSettings);
        indexDocuments(client, remoteStoreIndexName);

        logger.info("--> create first snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices(indexName, remoteStoreIndexName)
            .get();
        final SnapshotId snapshotId1 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        String[] lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("there should be no lock files present in directory, but found " + Arrays.toString(lockFiles), 0, lockFiles.length);
        logger.info("--> create remote index shallow snapshot");
        Settings snapshotRepoSettingsForShallowCopy = Settings.builder()
            .put(snapshotRepoSettings)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), Boolean.TRUE)
            .build();
        updateRepository(client, snapshotRepositoryName, snapshotRepoSettingsForShallowCopy);

        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, "test-snap-2")
            .setWaitForCompletion(true)
            .setIndices(indexName, remoteStoreIndexName)
            .get();
        final SnapshotId snapshotId2 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("there should be only one lock file, but found " + Arrays.toString(lockFiles), 1, lockFiles.length);
        assertTrue(lockFiles[0].endsWith(snapshotId2.getUUID() + ".lock"));

        logger.info("--> create another normal snapshot");
        updateRepository(client, snapshotRepositoryName, snapshotRepoSettings);
        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, "test-snap-3")
            .setWaitForCompletion(true)
            .setIndices(indexName, remoteStoreIndexName)
            .get();
        final SnapshotId snapshotId3 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("there should be only one lock file, but found " + Arrays.toString(lockFiles), 1, lockFiles.length);
        assertTrue(lockFiles[0].endsWith(snapshotId2.getUUID() + ".lock"));

        logger.info("--> make sure the node's repository can resolve the snapshots");
        final List<SnapshotId> originalSnapshots = Arrays.asList(snapshotId1, snapshotId2, snapshotId3);

        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(snapshotRepositoryName);
        RepositoryData repositoryData = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        IndexId indexId = repositoryData.resolveIndexId(remoteStoreIndexName);

        List<SnapshotId> snapshotIds = repositoryData.getSnapshotIds()
            .stream()
            .sorted((s1, s2) -> s1.getName().compareTo(s2.getName()))
            .collect(Collectors.toList());
        assertThat(snapshotIds, equalTo(originalSnapshots));

        // shallow copy shard metadata - getRemoteStoreShallowCopyShardMetadata
        RemoteStoreShardShallowCopySnapshot shardShallowCopySnapshot = repository.getRemoteStoreShallowCopyShardMetadata(
            snapshotId2,
            indexId,
            new ShardId(remoteStoreIndexName, indexId.getId(), 0)
        );
        assertEquals(shardShallowCopySnapshot.getRemoteStoreRepository(), remoteStoreRepositoryName);
    }

    public void testGetRemoteStoreShallowCopyShardMetadata() throws IOException {
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        final Client client = client();
        final String snapshotRepositoryName = "test-repo";
        final String remoteStoreRepositoryName = "test-rs-repo";

        logger.info("-->  creating snapshot repository");

        Settings snapshotRepoSettings = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .build();
        createRepository(client, snapshotRepositoryName, snapshotRepoSettings);

        logger.info("-->  creating remote store repository");
        Settings remoteStoreRepoSettings = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .build();
        createRepository(client, remoteStoreRepositoryName, remoteStoreRepoSettings);

        logger.info("--> creating a remote store enabled index and indexing documents");
        final String remoteStoreIndexName = "test-rs-idx";
        Settings indexSettings = getRemoteStoreBackedIndexSettings(remoteStoreRepositoryName);
        createIndex(remoteStoreIndexName, indexSettings);
        indexDocuments(client, remoteStoreIndexName);

        logger.info("--> create remote index shallow snapshot");
        Settings snapshotRepoSettingsForShallowCopy = Settings.builder()
            .put(snapshotRepoSettings)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), Boolean.TRUE)
            .build();
        updateRepository(client, snapshotRepositoryName, snapshotRepoSettingsForShallowCopy);

        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, "test-snap-2")
            .setWaitForCompletion(true)
            .setIndices(remoteStoreIndexName)
            .get();
        final SnapshotId snapshotId = createSnapshotResponse.getSnapshotInfo().snapshotId();

        String[] lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("there should be only one lock file, but found " + Arrays.toString(lockFiles), 1, lockFiles.length);
        assertTrue(lockFiles[0].endsWith(snapshotId.getUUID() + ".lock"));

        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(snapshotRepositoryName);
        RepositoryData repositoryData = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        IndexSettings indexSetting = getIndexSettings(remoteStoreIndexName);
        IndexId indexId = repositoryData.resolveIndexId(remoteStoreIndexName);
        RemoteStoreShardShallowCopySnapshot shardShallowCopySnapshot = repository.getRemoteStoreShallowCopyShardMetadata(
            snapshotId,
            indexId,
            new ShardId(remoteStoreIndexName, indexSetting.getUUID(), 0)
        );
        assertEquals(shardShallowCopySnapshot.getRemoteStoreRepository(), remoteStoreRepositoryName);
        assertEquals(shardShallowCopySnapshot.getIndexUUID(), indexSetting.getUUID());
        assertEquals(shardShallowCopySnapshot.getRepositoryBasePath(), "");
    }

    // Validate Scenario remoteStoreShallowCopy Snapshot -> remoteStoreShallowCopy Snapshot
    // -> remoteStoreShallowCopy Snapshot -> normal snapshot
    public void testRetrieveShallowCopySnapshotCase2() throws IOException {
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        final Client client = client();
        final String snapshotRepositoryName = "test-repo";
        final String remoteStoreRepositoryName = "test-rs-repo";

        logger.info("-->  creating snapshot repository");
        Settings snapshotRepoSettings = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .build();
        createRepository(client, snapshotRepositoryName, snapshotRepoSettings);

        GetRepositoriesResponse updatedGetRepositoriesResponse = client.admin()
            .cluster()
            .prepareGetRepositories(snapshotRepositoryName)
            .get();

        RepositoryMetadata updatedRepositoryMetadata = updatedGetRepositoriesResponse.repositories().get(0);

        assertFalse(updatedRepositoryMetadata.settings().getAsBoolean(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), false));

        logger.info("-->  creating remote store repository");
        createRepository(client, remoteStoreRepositoryName);

        logger.info("--> creating an index and indexing documents");
        final String indexName = "test-idx";
        createIndex(indexName);
        ensureGreen();
        indexDocuments(client, indexName);

        logger.info("--> creating a remote store enabled index and indexing documents");
        final String remoteStoreIndexName = "test-rs-idx";
        Settings indexSettings = getRemoteStoreBackedIndexSettings(remoteStoreRepositoryName);
        createIndex(remoteStoreIndexName, indexSettings);
        indexDocuments(client, remoteStoreIndexName);

        logger.info("--> create first remote index shallow snapshot");

        Settings snapshotRepoSettingsForShallowCopy = Settings.builder()
            .put(snapshotRepoSettings)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .build();
        updateRepository(client, snapshotRepositoryName, snapshotRepoSettingsForShallowCopy);

        updatedGetRepositoriesResponse = client.admin().cluster().prepareGetRepositories(snapshotRepositoryName).get();

        updatedRepositoryMetadata = updatedGetRepositoriesResponse.repositories().get(0);

        assertTrue(updatedRepositoryMetadata.settings().getAsBoolean(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), false));

        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices(indexName, remoteStoreIndexName)
            .get();
        final SnapshotId snapshotId1 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        String[] lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("lock files are " + Arrays.toString(lockFiles), 1, lockFiles.length);
        assertTrue(lockFiles[0].endsWith(snapshotId1.getUUID() + ".lock"));

        logger.info("--> create second remote index shallow snapshot");
        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, "test-snap-2")
            .setWaitForCompletion(true)
            .setIndices(indexName, remoteStoreIndexName)
            .get();
        final SnapshotId snapshotId2 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("lock files are " + Arrays.toString(lockFiles), 2, lockFiles.length);
        List<SnapshotId> shallowCopySnapshotIDs = Arrays.asList(snapshotId1, snapshotId2);
        for (SnapshotId snapshotId : shallowCopySnapshotIDs) {
            assertTrue(lockFiles[0].contains(snapshotId.getUUID()) || lockFiles[1].contains(snapshotId.getUUID()));
        }
        logger.info("--> create third remote index shallow snapshot");
        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, "test-snap-3")
            .setWaitForCompletion(true)
            .setIndices(indexName, remoteStoreIndexName)
            .get();
        final SnapshotId snapshotId3 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals(3, lockFiles.length);
        shallowCopySnapshotIDs = Arrays.asList(snapshotId1, snapshotId2, snapshotId3);
        for (SnapshotId snapshotId : shallowCopySnapshotIDs) {
            assertTrue(
                lockFiles[0].contains(snapshotId.getUUID())
                    || lockFiles[1].contains(snapshotId.getUUID())
                    || lockFiles[2].contains(snapshotId.getUUID())
            );
        }
        logger.info("--> create normal snapshot");
        createRepository(client, snapshotRepositoryName, snapshotRepoSettings);
        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, "test-snap-4")
            .setWaitForCompletion(true)
            .setIndices(indexName, remoteStoreIndexName)
            .get();
        final SnapshotId snapshotId4 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("lock files are " + Arrays.toString(lockFiles), 3, lockFiles.length);
        shallowCopySnapshotIDs = Arrays.asList(snapshotId1, snapshotId2, snapshotId3);
        for (SnapshotId snapshotId : shallowCopySnapshotIDs) {
            assertTrue(
                lockFiles[0].contains(snapshotId.getUUID())
                    || lockFiles[1].contains(snapshotId.getUUID())
                    || lockFiles[2].contains(snapshotId.getUUID())
            );
        }

        logger.info("--> make sure the node's repository can resolve the snapshots");
        final List<SnapshotId> originalSnapshots = Arrays.asList(snapshotId1, snapshotId2, snapshotId3, snapshotId4);

        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(snapshotRepositoryName);
        List<SnapshotId> snapshotIds = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository)
            .getSnapshotIds()
            .stream()
            .sorted((s1, s2) -> s1.getName().compareTo(s2.getName()))
            .collect(Collectors.toList());
        assertThat(snapshotIds, equalTo(originalSnapshots));
    }

    public void testShallowAndFullCopyCreateAndDelete() throws IOException, InterruptedException {
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        final Client client = client();
        final String snapshotRepositoryName = "test-repo";
        final String remoteStoreRepositoryName = "test-rs-repo";

        logger.info("-->  creating snapshot repository");

        Settings snapshotRepoSettings = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), Boolean.TRUE)
            .build();
        createRepository(client, snapshotRepositoryName, snapshotRepoSettings);

        logger.info("-->  creating remote store repository");
        Settings remoteStoreRepoSettings = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .build();
        createRepository(client, remoteStoreRepositoryName, remoteStoreRepoSettings);

        logger.info("--> creating an index and indexing documents");
        final String indexName = "test-idx";
        createIndex(indexName);
        ensureGreen();
        indexDocuments(client, indexName);

        logger.info("--> creating a remote store enabled index and indexing documents");
        final String remoteStoreIndexName = "test-rs-idx";
        Settings indexSettings = getRemoteStoreBackedIndexSettings(remoteStoreRepositoryName);
        createIndex(remoteStoreIndexName, indexSettings);
        indexDocuments(client, remoteStoreIndexName);

        logger.info("--> create first shallow snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices(indexName, remoteStoreIndexName)
            .get();
        final SnapshotId snapshotId1 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        String[] lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("there should be only one lock file, but found " + Arrays.toString(lockFiles), 1, lockFiles.length);
        assertTrue(lockFiles[0].endsWith(snapshotId1.getUUID() + ".lock"));

        Settings snapshotRepoSettingsForShallowCopy = Settings.builder()
            .put(snapshotRepoSettings)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), Boolean.FALSE)
            .build();
        updateRepository(client, snapshotRepositoryName, snapshotRepoSettingsForShallowCopy);

        logger.info("--> create full copy snapshot");
        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, "test-snap-2")
            .setWaitForCompletion(true)
            .setIndices(indexName, remoteStoreIndexName)
            .get();
        final SnapshotId snapshotId2 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("there should be only one lock file, but found " + Arrays.toString(lockFiles), 1, lockFiles.length);

        logger.info("--> delete full copy snapshot");
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(snapshotRepositoryName, "test-snap-2").get());

        logger.info("--> create another full copy snapshot");
        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, "test-snap-3")
            .setWaitForCompletion(true)
            .setIndices(indexName, remoteStoreIndexName)
            .get();
        final SnapshotId snapshotId3 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("there should be only one lock file, but found " + Arrays.toString(lockFiles), 1, lockFiles.length);
        assertTrue(lockFiles[0].endsWith(snapshotId1.getUUID() + ".lock"));

        logger.info("--> make sure the node's repository can resolve the snapshots");
        final List<SnapshotId> originalSnapshots = Arrays.asList(snapshotId1, snapshotId3);

        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(snapshotRepositoryName);
        RepositoryData repositoryData = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        IndexId indexId = repositoryData.resolveIndexId(remoteStoreIndexName);

        List<SnapshotId> snapshotIds = repositoryData.getSnapshotIds()
            .stream()
            .sorted((s1, s2) -> s1.getName().compareTo(s2.getName()))
            .collect(Collectors.toList());
        assertThat(snapshotIds, equalTo(originalSnapshots));
    }

    public void testLocksForShallowClonePostDelete() throws IOException {
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        final Client client = client();
        final String snapshotRepositoryName = "test-repo";
        final String remoteStoreRepositoryName = "test-rs-repo";

        logger.info("-->  creating snapshot repository");

        Settings snapshotRepoSettings = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), Boolean.TRUE)
            .build();
        createRepository(client, snapshotRepositoryName, snapshotRepoSettings);

        logger.info("-->  creating remote store repository");
        Settings remoteStoreRepoSettings = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .build();
        createRepository(client, remoteStoreRepositoryName, remoteStoreRepoSettings);

        logger.info("--> creating an index and indexing documents");
        final String indexName = "test-idx";
        createIndex(indexName);
        ensureGreen();
        indexDocuments(client, indexName);

        logger.info("--> creating a remote store enabled index and indexing documents");
        final String remoteStoreIndexName = "test-rs-idx";
        Settings indexSettings = getRemoteStoreBackedIndexSettings(remoteStoreRepositoryName);
        createIndex(remoteStoreIndexName, indexSettings);
        indexDocuments(client, remoteStoreIndexName);

        deleteExtraFSFilesIfAny(remoteStoreIndexName, remoteStoreRepositoryName);

        logger.info("--> create first shallow snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, "test-snap")
            .setWaitForCompletion(true)
            .setIndices(indexName, remoteStoreIndexName)
            .get();
        final SnapshotId snapshotId1 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        String[] lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("there should be only one lock file, but found " + Arrays.toString(lockFiles), 1, lockFiles.length);
        String lockedMdFile = lockFiles[0].replace(RemoteStoreLockManagerUtils.SEPARATOR + snapshotId1.getUUID() + ".lock", "");

        logger.info("--> clone shallow snapshot");
        assertAcked(
            client.admin()
                .cluster()
                .prepareCloneSnapshot(snapshotRepositoryName, "test-snap", "test-snap-clone")
                .setIndices(indexName, remoteStoreIndexName)
                .get()
        );

        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("there should be two lock files, but found " + Arrays.toString(lockFiles), 2, lockFiles.length);

        logger.info("--> delete shallow snapshot");
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(snapshotRepositoryName, "test-snap").get());

        logger.info("-->  validate one lock file still present");
        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("there should be only one lock file, but found " + Arrays.toString(lockFiles), 1, lockFiles.length);
        assertTrue(lockFiles[0].contains(lockedMdFile));
        assertFalse(lockFiles[0].contains(snapshotId1.getUUID()));
    }

    public void testAcquireLockFails() throws IOException {
        testFailureScenario(true, false);
    }

    public void testWriteShallowSnapFileFails() throws IOException {
        testFailureScenario(false, true);
    }

    private void testFailureScenario(boolean acquireLockFails, boolean shallowSnapFileIOFails) throws IOException {
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        final Client client = client();
        final String snapshotRepositoryName = "test-repo";
        final String remoteStoreRepositoryName = "test-rs-repo";

        Settings.Builder snapshotRepoSettingsBuilder = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), Boolean.TRUE);

        if (shallowSnapFileIOFails) {
            snapshotRepoSettingsBuilder = snapshotRepoSettingsBuilder.putList(
                "regexes_to_fail_io",
                "^" + BlobStoreRepository.SHALLOW_SNAPSHOT_PREFIX
            );
        }

        Settings.Builder remoteStoreRepoSettingsBuilder = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()));

        if (acquireLockFails) {
            remoteStoreRepoSettingsBuilder = remoteStoreRepoSettingsBuilder.putList("regexes_to_fail_io", "lock$");
        }

        logger.info("-->  creating snapshot repository");
        createRepository(client(), snapshotRepositoryName, snapshotRepoSettingsBuilder.build(), "mock");

        logger.info("-->  creating remote store repository");
        createRepository(client(), remoteStoreRepositoryName, remoteStoreRepoSettingsBuilder.build(), "mock");
        logger.info("--> creating an index and indexing documents");
        final String indexName = "test-idx";
        createIndex(indexName);
        ensureGreen();
        indexDocuments(client, indexName);

        logger.info("--> creating a remote store enabled index and indexing documents");
        final String remoteStoreIndexName = "test-rs-idx";
        Settings indexSettings = getRemoteStoreBackedIndexSettings(remoteStoreRepositoryName);
        createIndex(remoteStoreIndexName, indexSettings);
        indexDocuments(client, remoteStoreIndexName);

        logger.info("--> create first shallow snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, "test-snap")
            .setWaitForCompletion(true)
            .setIndices(indexName, remoteStoreIndexName)
            .get();
        assertTrue(createSnapshotResponse.getSnapshotInfo().isRemoteStoreIndexShallowCopyEnabled());
        if (acquireLockFails || shallowSnapFileIOFails) {
            assertTrue(createSnapshotResponse.getSnapshotInfo().failedShards() > 0);
            assertSame(createSnapshotResponse.getSnapshotInfo().state(), SnapshotState.PARTIAL);
            if (shallowSnapFileIOFails) {
                String[] lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
                assertEquals("there should be one lock file, but found " + Arrays.toString(lockFiles), 1, lockFiles.length);
            }
            logger.info("--> delete partial snapshot");
            assertAcked(client().admin().cluster().prepareDeleteSnapshot(snapshotRepositoryName, "test-snap").get());
        } else {
            assertEquals(0, createSnapshotResponse.getSnapshotInfo().failedShards());
            assertSame(createSnapshotResponse.getSnapshotInfo().state(), SnapshotState.SUCCESS);
        }
    }
}
