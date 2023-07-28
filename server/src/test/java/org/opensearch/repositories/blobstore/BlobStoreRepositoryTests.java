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

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.snapshots.blobstore.RemoteStoreShardShallowCopySnapshot;
import org.opensearch.index.store.RemoteBufferedOutputDirectory;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.opensearch.repositories.RepositoryDataTests.generateRandomRepoData;

/**
 * Tests for the {@link BlobStoreRepository} and its subclasses.
 */
public class BlobStoreRepositoryTests extends OpenSearchSingleNodeTestCase {

    static final String REPO_TYPE = "fsLike";

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(FsLikeRepoPlugin.class);
    }

    // the reason for this plug-in is to drop any assertSnapshotOrGenericThread as mostly all access in this test goes from test threads
    public static class FsLikeRepoPlugin extends Plugin implements RepositoryPlugin {

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                REPO_TYPE,
                (metadata) -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings) {
                    @Override
                    protected void assertSnapshotOrGenericThread() {
                        // eliminate thread name check as we access blobStore on test/main threads
                    }
                }
            );
        }
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(FeatureFlags.REMOTE_STORE, "true").build();
    }

    public void testRetrieveSnapshots() throws Exception {
        final Client client = client();
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        logger.info("-->  creating repository");
        AcknowledgedResponse putRepositoryResponse = client.admin()
            .cluster()
            .preparePutRepository(repositoryName)
            .setType(REPO_TYPE)
            .setSettings(Settings.builder().put(node().settings()).put("location", location))
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> creating an index and indexing documents");
        final String indexName = "test-idx";
        createIndex(indexName);
        ensureGreen();
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            client().prepareIndex(indexName).setId(id).setSource("text", "sometext").get();
        }
        client().admin().indices().prepareFlush(indexName).get();

        logger.info("--> create first snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        final SnapshotId snapshotId1 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        logger.info("--> create second snapshot");
        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, "test-snap-2")
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        final SnapshotId snapshotId2 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        logger.info("--> make sure the node's repository can resolve the snapshots");
        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(repositoryName);
        final List<SnapshotId> originalSnapshots = Arrays.asList(snapshotId1, snapshotId2);

        List<SnapshotId> snapshotIds = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository)
            .getSnapshotIds()
            .stream()
            .sorted((s1, s2) -> s1.getName().compareTo(s2.getName()))
            .collect(Collectors.toList());
        assertThat(snapshotIds, equalTo(originalSnapshots));
    }

    private void createRepository(Client client, String repoName) {
        AcknowledgedResponse putRepositoryResponse = client.admin()
            .cluster()
            .preparePutRepository(repoName)
            .setType(REPO_TYPE)
            .setSettings(
                Settings.builder().put(node().settings()).put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            )
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    private void createRepository(Client client, String repoName, Settings repoSettings) {
        AcknowledgedResponse putRepositoryResponse = client.admin()
            .cluster()
            .preparePutRepository(repoName)
            .setType(REPO_TYPE)
            .setSettings(repoSettings)
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    private void updateRepository(Client client, String repoName, Settings repoSettings) {
        createRepository(client, repoName, repoSettings);
    }

    private Settings getRemoteStoreBackedIndexSettings(String remoteStoreRepo) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "1")
            .put("index.refresh_interval", "300s")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1")
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey())
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, remoteStoreRepo)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, remoteStoreRepo)
            .build();
    }

    private void indexDocuments(Client client, String indexName) {
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            client.prepareIndex(indexName).setId(id).setSource("text", "sometext").get();
        }
        client.admin().indices().prepareFlush(indexName).get();
    }

    private String[] getLockFilesInRemoteStore(String remoteStoreIndex, String remoteStoreRepository) throws IOException {
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(remoteStoreIndex)
            .get()
            .getSetting(remoteStoreIndex, IndexMetadata.SETTING_INDEX_UUID);
        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository remoteStorerepository = (BlobStoreRepository) repositoriesService.repository(remoteStoreRepository);
        BlobPath shardLevelBlobPath = remoteStorerepository.basePath().add(indexUUID).add("0").add("segments").add("lock_files");
        BlobContainer blobContainer = remoteStorerepository.blobStore().blobContainer(shardLevelBlobPath);
        try (RemoteBufferedOutputDirectory lockDirectory = new RemoteBufferedOutputDirectory(blobContainer)) {
            return Arrays.stream(lockDirectory.listAll()).filter(lock -> lock.endsWith(".lock")).toArray(String[]::new);
        }
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
        assert (lockFiles.length == 0) : "there should be no lock files present in directory, but found " + Arrays.toString(lockFiles);
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
        assert (lockFiles.length == 1) : "there should be only one lock file, but found " + Arrays.toString(lockFiles);
        assert lockFiles[0].endsWith(snapshotId2.getUUID() + ".lock");

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
        assert (lockFiles.length == 1) : "there should be only one lock file, but found " + Arrays.toString(lockFiles);
        assert lockFiles[0].endsWith(snapshotId2.getUUID() + ".lock");

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
        assert (lockFiles.length == 1) : "there should be only one lock file, but found " + Arrays.toString(lockFiles);
        assert lockFiles[0].endsWith(snapshotId.getUUID() + ".lock");

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

    private IndexSettings getIndexSettings(String indexName) {
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexService(resolveIndex(indexName));
        return indexService.getIndexSettings();
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
        assert (lockFiles.length == 1) : "lock files are " + Arrays.toString(lockFiles);
        assert lockFiles[0].endsWith(snapshotId1.getUUID() + ".lock");

        logger.info("--> create second remote index shallow snapshot");
        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, "test-snap-2")
            .setWaitForCompletion(true)
            .setIndices(indexName, remoteStoreIndexName)
            .get();
        final SnapshotId snapshotId2 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assert (lockFiles.length == 2) : "lock files are " + Arrays.toString(lockFiles);
        List<SnapshotId> shallowCopySnapshotIDs = Arrays.asList(snapshotId1, snapshotId2);
        for (SnapshotId snapshotId : shallowCopySnapshotIDs) {
            assert lockFiles[0].contains(snapshotId.getUUID()) || lockFiles[1].contains(snapshotId.getUUID());
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
        assert (lockFiles.length == 3);
        shallowCopySnapshotIDs = Arrays.asList(snapshotId1, snapshotId2, snapshotId3);
        for (SnapshotId snapshotId : shallowCopySnapshotIDs) {
            assert lockFiles[0].contains(snapshotId.getUUID())
                || lockFiles[1].contains(snapshotId.getUUID())
                || lockFiles[2].contains(snapshotId.getUUID());
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
        assert (lockFiles.length == 3) : "lock files are " + Arrays.toString(lockFiles);
        shallowCopySnapshotIDs = Arrays.asList(snapshotId1, snapshotId2, snapshotId3);
        for (SnapshotId snapshotId : shallowCopySnapshotIDs) {
            assert lockFiles[0].contains(snapshotId.getUUID())
                || lockFiles[1].contains(snapshotId.getUUID())
                || lockFiles[2].contains(snapshotId.getUUID());
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

    public void testReadAndWriteSnapshotsThroughIndexFile() throws Exception {
        final BlobStoreRepository repository = setupRepo();
        final long pendingGeneration = repository.metadata.pendingGeneration();
        // write to and read from a index file with no entries
        assertThat(OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).getSnapshotIds().size(), equalTo(0));
        final RepositoryData emptyData = RepositoryData.EMPTY;
        writeIndexGen(repository, emptyData, emptyData.getGenId());
        RepositoryData repoData = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        assertEquals(repoData, emptyData);
        assertEquals(repoData.getIndices().size(), 0);
        assertEquals(repoData.getSnapshotIds().size(), 0);
        assertEquals(pendingGeneration + 1L, repoData.getGenId());

        // write to and read from an index file with snapshots but no indices
        repoData = addRandomSnapshotsToRepoData(repoData, false);
        writeIndexGen(repository, repoData, repoData.getGenId());
        assertEquals(repoData, OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository));

        // write to and read from a index file with random repository data
        repoData = addRandomSnapshotsToRepoData(OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), true);
        writeIndexGen(repository, repoData, repoData.getGenId());
        assertEquals(repoData, OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository));
    }

    public void testIndexGenerationalFiles() throws Exception {
        final BlobStoreRepository repository = setupRepo();
        assertEquals(OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), RepositoryData.EMPTY);

        final long pendingGeneration = repository.metadata.pendingGeneration();

        // write to index generational file
        RepositoryData repositoryData = generateRandomRepoData();
        writeIndexGen(repository, repositoryData, RepositoryData.EMPTY_REPO_GEN);
        assertThat(OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), equalTo(repositoryData));
        final long expectedGeneration = pendingGeneration + 1L;
        assertThat(repository.latestIndexBlobId(), equalTo(expectedGeneration));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(expectedGeneration));

        // adding more and writing to a new index generational file
        repositoryData = addRandomSnapshotsToRepoData(OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), true);
        writeIndexGen(repository, repositoryData, repositoryData.getGenId());
        assertEquals(OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), repositoryData);
        assertThat(repository.latestIndexBlobId(), equalTo(expectedGeneration + 1L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(expectedGeneration + 1L));

        // removing a snapshot and writing to a new index generational file
        repositoryData = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository)
            .removeSnapshots(Collections.singleton(repositoryData.getSnapshotIds().iterator().next()), ShardGenerations.EMPTY);
        writeIndexGen(repository, repositoryData, repositoryData.getGenId());
        assertEquals(OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), repositoryData);
        assertThat(repository.latestIndexBlobId(), equalTo(expectedGeneration + 2L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(expectedGeneration + 2L));
    }

    public void testRepositoryDataConcurrentModificationNotAllowed() {
        final BlobStoreRepository repository = setupRepo();

        // write to index generational file
        RepositoryData repositoryData = generateRandomRepoData();
        final long startingGeneration = repositoryData.getGenId();
        final PlainActionFuture<RepositoryData> future1 = PlainActionFuture.newFuture();
        repository.writeIndexGen(repositoryData, startingGeneration, Version.CURRENT, Function.identity(), future1);

        // write repo data again to index generational file, errors because we already wrote to the
        // N+1 generation from which this repository data instance was created
        expectThrows(
            RepositoryException.class,
            () -> writeIndexGen(repository, repositoryData.withGenId(startingGeneration + 1), repositoryData.getGenId())
        );
    }

    public void testBadChunksize() throws Exception {
        final Client client = client();
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        expectThrows(
            RepositoryException.class,
            () -> client.admin()
                .cluster()
                .preparePutRepository(repositoryName)
                .setType(REPO_TYPE)
                .setSettings(
                    Settings.builder()
                        .put(node().settings())
                        .put("location", location)
                        .put("chunk_size", randomLongBetween(-10, 0), ByteSizeUnit.BYTES)
                )
                .get()
        );
    }

    public void testFsRepositoryCompressDeprecated() {
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final Settings settings = Settings.builder().put(node().settings()).put("location", location).build();
        final RepositoryMetadata metadata = new RepositoryMetadata("test-repo", REPO_TYPE, settings);

        Settings useCompressSettings = Settings.builder()
            .put(node().getEnvironment().settings())
            .put(FsRepository.REPOSITORIES_COMPRESS_SETTING.getKey(), true)
            .build();
        Environment useCompressEnvironment = new Environment(useCompressSettings, node().getEnvironment().configDir());

        new FsRepository(metadata, useCompressEnvironment, null, BlobStoreTestUtil.mockClusterService(), null);

        assertWarnings(
            "[repositories.fs.compress] setting was deprecated in OpenSearch and will be removed in a future release!"
                + " See the breaking changes documentation for the next major version."
        );
    }

    private static void writeIndexGen(BlobStoreRepository repository, RepositoryData repositoryData, long generation) throws Exception {
        PlainActionFuture.<RepositoryData, Exception>get(
            f -> repository.writeIndexGen(repositoryData, generation, Version.CURRENT, Function.identity(), f)
        );
    }

    private BlobStoreRepository setupRepo() {
        final Client client = client();
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        AcknowledgedResponse putRepositoryResponse = client.admin()
            .cluster()
            .preparePutRepository(repositoryName)
            .setType(REPO_TYPE)
            .setSettings(Settings.builder().put(node().settings()).put("location", location))
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(repositoryName);
        assertThat("getBlobContainer has to be lazy initialized", repository.getBlobContainer(), nullValue());
        return repository;
    }

    private RepositoryData addRandomSnapshotsToRepoData(RepositoryData repoData, boolean inclIndices) {
        int numSnapshots = randomIntBetween(1, 20);
        for (int i = 0; i < numSnapshots; i++) {
            SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            int numIndices = inclIndices ? randomIntBetween(0, 20) : 0;
            final ShardGenerations.Builder builder = ShardGenerations.builder();
            for (int j = 0; j < numIndices; j++) {
                builder.put(new IndexId(randomAlphaOfLength(8), UUIDs.randomBase64UUID()), 0, "1");
            }
            final ShardGenerations shardGenerations = builder.build();
            final Map<IndexId, String> indexLookup = shardGenerations.indices()
                .stream()
                .collect(Collectors.toMap(Function.identity(), ind -> randomAlphaOfLength(256)));
            repoData = repoData.addSnapshot(
                snapshotId,
                randomFrom(SnapshotState.SUCCESS, SnapshotState.PARTIAL, SnapshotState.FAILED),
                Version.CURRENT,
                shardGenerations,
                indexLookup,
                indexLookup.values().stream().collect(Collectors.toMap(Function.identity(), ignored -> UUIDs.randomBase64UUID(random())))
            );
        }
        return repoData;
    }

}
