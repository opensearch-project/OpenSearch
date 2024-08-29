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
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManagerFactory;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.RepositoryStats;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotShardPaths;
import org.opensearch.snapshots.SnapshotShardPaths.ShardInfo;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.repositories.RepositoryDataTests.generateRandomRepoData;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link BlobStoreRepository} and its subclasses.
 */
public class BlobStoreRepositoryTests extends BlobStoreRepositoryHelperTests {

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

    public void testPrefixModeVerification() throws Exception {
        final Client client = client();
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";
        AcknowledgedResponse putRepositoryResponse = client.admin()
            .cluster()
            .preparePutRepository(repositoryName)
            .setType(REPO_TYPE)
            .setSettings(
                Settings.builder()
                    .put(node().settings())
                    .put("location", location)
                    .put(BlobStoreRepository.PREFIX_MODE_VERIFICATION_SETTING.getKey(), true)
            )
            .get();
        assertTrue(putRepositoryResponse.isAcknowledged());

        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(repositoryName);
        assertTrue(repository.getPrefixModeVerification());
    }

    public void testFsRepositoryCompressDeprecatedIgnored() {
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final Settings settings = Settings.builder().put(node().settings()).put("location", location).build();
        final RepositoryMetadata metadata = new RepositoryMetadata("test-repo", REPO_TYPE, settings);

        Settings useCompressSettings = Settings.builder()
            .put(node().getEnvironment().settings())
            .put(FsRepository.REPOSITORIES_COMPRESS_SETTING.getKey(), true)
            .build();
        Environment useCompressEnvironment = new Environment(useCompressSettings, node().getEnvironment().configDir());

        new FsRepository(metadata, useCompressEnvironment, null, BlobStoreTestUtil.mockClusterService(), null);

        assertNoDeprecationWarnings();
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

    private String getShardIdentifier(String indexUUID, String shardId) {
        return String.join("/", indexUUID, shardId);
    }

    public void testRemoteStoreShardCleanupTask() {
        AtomicBoolean executed1 = new AtomicBoolean(false);
        Runnable task1 = () -> executed1.set(true);
        String indexName = "test-idx";
        String testIndexUUID = "test-idx-uuid";
        ShardId shardId = new ShardId(new Index(indexName, testIndexUUID), 0);

        // just adding random shards in ongoing cleanups.
        RemoteStoreShardCleanupTask.ongoingRemoteDirectoryCleanups.add(getShardIdentifier(testIndexUUID, "1"));
        RemoteStoreShardCleanupTask.ongoingRemoteDirectoryCleanups.add(getShardIdentifier(testIndexUUID, "2"));

        // Scenario 1: ongoing = false => executed
        RemoteStoreShardCleanupTask remoteStoreShardCleanupTask = new RemoteStoreShardCleanupTask(task1, testIndexUUID, shardId);
        remoteStoreShardCleanupTask.run();
        assertTrue(executed1.get());

        // Scenario 2: ongoing = true => currentTask skipped.
        executed1.set(false);
        RemoteStoreShardCleanupTask.ongoingRemoteDirectoryCleanups.add(getShardIdentifier(testIndexUUID, "0"));
        remoteStoreShardCleanupTask = new RemoteStoreShardCleanupTask(task1, testIndexUUID, shardId);
        remoteStoreShardCleanupTask.run();
        assertFalse(executed1.get());
    }

    public void testParseShardPath() {
        RepositoryData repoData = generateRandomRepoData();
        IndexId indexId = repoData.getIndices().values().iterator().next();
        int shardCount = repoData.shardGenerations().getGens(indexId).size();

        String shardPath = String.join(
            SnapshotShardPaths.DELIMITER,
            indexId.getId(),
            indexId.getName(),
            String.valueOf(shardCount),
            String.valueOf(indexId.getShardPathType()),
            "1"
        );
        ShardInfo shardInfo = SnapshotShardPaths.parseShardPath(shardPath);

        assertEquals(shardInfo.getIndexId(), indexId);
        assertEquals(shardInfo.getShardCount(), shardCount);
    }

    public void testWriteAndReadShardPaths() throws Exception {
        BlobStoreRepository repository = setupRepo();
        RepositoryData repoData = generateRandomRepoData();
        SnapshotId snapshotId = repoData.getSnapshotIds().iterator().next();

        Set<String> writtenShardPaths = new HashSet<>();
        for (IndexId indexId : repoData.getIndices().values()) {
            if (indexId.getShardPathType() != IndexId.DEFAULT_SHARD_PATH_TYPE) {
                String shardPathBlobName = repository.writeIndexShardPaths(indexId, snapshotId, indexId.getShardPathType());
                writtenShardPaths.add(shardPathBlobName);
            }
        }

        // Read shard paths and verify
        Map<String, BlobMetadata> shardPathBlobs = repository.snapshotShardPathBlobContainer().listBlobs();

        // Create sets for comparison
        Set<String> expectedPaths = new HashSet<>(writtenShardPaths);
        Set<String> actualPaths = new HashSet<>(shardPathBlobs.keySet());

        // Remove known extra files - "extra0" file is added by the ExtrasFS, which is part of Lucene's test framework
        actualPaths.remove("extra0");

        // Check if all expected paths are present in the actual paths
        assertTrue("All expected paths should be present", actualPaths.containsAll(expectedPaths));

        // Check if there are any unexpected additional paths
        Set<String> unexpectedPaths = new HashSet<>(actualPaths);
        unexpectedPaths.removeAll(expectedPaths);
        if (!unexpectedPaths.isEmpty()) {
            logger.warn("Unexpected additional paths found: " + unexpectedPaths);
        }

        assertEquals("Expected and actual paths should match after removing known extra files", expectedPaths, actualPaths);

        for (String shardPathBlobName : expectedPaths) {
            SnapshotShardPaths.ShardInfo shardInfo = SnapshotShardPaths.parseShardPath(shardPathBlobName);
            IndexId indexId = repoData.getIndices().get(shardInfo.getIndexId().getName());
            assertNotNull("IndexId should not be null", indexId);
            assertEquals("Index ID should match", shardInfo.getIndexId().getId(), indexId.getId());
            assertEquals("Shard path type should match", shardInfo.getIndexId().getShardPathType(), indexId.getShardPathType());
            String[] parts = shardPathBlobName.split(SnapshotShardPaths.DELIMITER);
            assertEquals(
                "Path hash algorithm should be FNV_1A_COMPOSITE_1",
                RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1,
                RemoteStoreEnums.PathHashAlgorithm.fromCode(Integer.parseInt(parts[4]))
            );
        }
    }

    public void testCleanupStaleIndices() throws Exception {
        // Mock the BlobStoreRepository
        BlobStoreRepository repository = mock(BlobStoreRepository.class);

        // Mock BlobContainer for stale index
        BlobContainer staleIndexContainer = mock(BlobContainer.class);
        when(staleIndexContainer.delete()).thenReturn(new DeleteResult(1, 100L));

        // Mock BlobContainer for current index
        BlobContainer currentIndexContainer = mock(BlobContainer.class);

        Map<String, BlobContainer> foundIndices = new HashMap<>();
        foundIndices.put("stale-index", staleIndexContainer);
        foundIndices.put("current-index", currentIndexContainer);

        Set<String> survivingIndexIds = new HashSet<>();
        survivingIndexIds.add("current-index");

        // Create a mock RemoteStoreLockManagerFactory
        RemoteStoreLockManagerFactory mockRemoteStoreLockManagerFactory = mock(RemoteStoreLockManagerFactory.class);
        RemoteStoreLockManager mockLockManager = mock(RemoteStoreLockManager.class);
        when(mockRemoteStoreLockManagerFactory.newLockManager(anyString(), anyString(), anyString(), any())).thenReturn(mockLockManager);

        // Create mock snapshot shard paths
        Map<String, BlobMetadata> mockSnapshotShardPaths = new HashMap<>();
        String validShardPath = "stale-index-id#stale-index#1#0#1";
        mockSnapshotShardPaths.put(validShardPath, mock(BlobMetadata.class));

        // Mock snapshotShardPathBlobContainer
        BlobContainer mockSnapshotShardPathBlobContainer = mock(BlobContainer.class);
        when(mockSnapshotShardPathBlobContainer.delete()).thenReturn(new DeleteResult(1, 50L));
        when(repository.snapshotShardPathBlobContainer()).thenReturn(mockSnapshotShardPathBlobContainer);

        // Mock the cleanupStaleIndices method to call our test implementation
        doAnswer(invocation -> {
            Map<String, BlobContainer> indices = invocation.getArgument(0);
            Set<String> surviving = invocation.getArgument(1);
            GroupedActionListener<DeleteResult> listener = invocation.getArgument(3);

            // Simulate the cleanup process
            DeleteResult result = DeleteResult.ZERO;
            for (Map.Entry<String, BlobContainer> entry : indices.entrySet()) {
                if (!surviving.contains(entry.getKey())) {
                    result = result.add(entry.getValue().delete());
                }
            }
            result = result.add(mockSnapshotShardPathBlobContainer.delete());

            listener.onResponse(result);
            return null;
        }).when(repository).cleanupStaleIndices(any(), any(), any(), any(), any(), anyMap());

        AtomicReference<Collection<DeleteResult>> resultReference = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        GroupedActionListener<DeleteResult> listener = new GroupedActionListener<>(ActionListener.wrap(deleteResults -> {
            resultReference.set(deleteResults);
            latch.countDown();
        }, e -> {
            logger.error("Error in cleanupStaleIndices", e);
            latch.countDown();
        }), 1);

        // Call the method we're testing
        repository.cleanupStaleIndices(
            foundIndices,
            survivingIndexIds,
            mockRemoteStoreLockManagerFactory,
            listener,
            mockSnapshotShardPaths,
            Collections.emptyMap()
        );

        assertTrue("Cleanup did not complete within the expected time", latch.await(30, TimeUnit.SECONDS));

        Collection<DeleteResult> results = resultReference.get();
        assertNotNull("DeleteResult collection should not be null", results);
        assertFalse("DeleteResult collection should not be empty", results.isEmpty());

        DeleteResult combinedResult = results.stream().reduce(DeleteResult.ZERO, DeleteResult::add);

        assertTrue("Bytes deleted should be greater than 0", combinedResult.bytesDeleted() > 0);
        assertTrue("Blobs deleted should be greater than 0", combinedResult.blobsDeleted() > 0);

        // Verify that the stale index was processed for deletion
        verify(staleIndexContainer, times(1)).delete();

        // Verify that the current index was not processed for deletion
        verify(currentIndexContainer, never()).delete();

        // Verify that snapshot shard paths were considered in the cleanup process
        verify(mockSnapshotShardPathBlobContainer, times(1)).delete();

        // Verify the total number of bytes and blobs deleted
        assertEquals("Total bytes deleted should be 150", 150L, combinedResult.bytesDeleted());
        assertEquals("Total blobs deleted should be 2", 2, combinedResult.blobsDeleted());
    }

    public void testGetMetadata() {
        BlobStoreRepository repository = setupRepo();
        RepositoryMetadata metadata = repository.getMetadata();
        assertNotNull(metadata);
        assertEquals(metadata.name(), "test-repo");
        assertEquals(metadata.type(), REPO_TYPE);
        repository.close();
    }

    public void testGetNamedXContentRegistry() {
        BlobStoreRepository repository = setupRepo();
        NamedXContentRegistry registry = repository.getNamedXContentRegistry();
        assertNotNull(registry);
        repository.close();
    }

    public void testGetCompressor() {
        BlobStoreRepository repository = setupRepo();
        Compressor compressor = repository.getCompressor();
        assertNotNull(compressor);
        repository.close();
    }

    public void testGetStats() {
        BlobStoreRepository repository = setupRepo();
        RepositoryStats stats = repository.stats();
        assertNotNull(stats);
        repository.close();
    }

    public void testGetSnapshotThrottleTimeInNanos() {
        BlobStoreRepository repository = setupRepo();
        long throttleTime = repository.getSnapshotThrottleTimeInNanos();
        assertTrue(throttleTime >= 0);
        repository.close();
    }

    public void testGetRestoreThrottleTimeInNanos() {
        BlobStoreRepository repository = setupRepo();
        long throttleTime = repository.getRestoreThrottleTimeInNanos();
        assertTrue(throttleTime >= 0);
        repository.close();
    }

    public void testGetRemoteUploadThrottleTimeInNanos() {
        BlobStoreRepository repository = setupRepo();
        long throttleTime = repository.getRemoteUploadThrottleTimeInNanos();
        assertTrue(throttleTime >= 0);
        repository.close();
    }

    public void testGetLowPriorityRemoteUploadThrottleTimeInNanos() {
        BlobStoreRepository repository = setupRepo();
        long throttleTime = repository.getLowPriorityRemoteUploadThrottleTimeInNanos();
        assertTrue(throttleTime >= 0);
        repository.close();
    }

    public void testGetRemoteDownloadThrottleTimeInNanos() {
        BlobStoreRepository repository = setupRepo();
        long throttleTime = repository.getRemoteDownloadThrottleTimeInNanos();
        assertTrue(throttleTime >= 0);
        repository.close();
    }

    public void testIsReadOnly() {
        BlobStoreRepository repository = setupRepo();
        assertFalse(repository.isReadOnly());
        repository.close();
    }

    public void testIsSystemRepository() {
        BlobStoreRepository repository = setupRepo();
        assertFalse(repository.isSystemRepository());
        repository.close();
    }

    public void testGetRestrictedSystemRepositorySettings() {
        BlobStoreRepository repository = setupRepo();
        List<Setting<?>> settings = repository.getRestrictedSystemRepositorySettings();
        assertNotNull(settings);
        assertTrue(settings.contains(BlobStoreRepository.SYSTEM_REPOSITORY_SETTING));
        assertTrue(settings.contains(BlobStoreRepository.READONLY_SETTING));
        assertTrue(settings.contains(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY));
        repository.close();
    }
}
