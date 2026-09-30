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

import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Numbers;
import org.opensearch.common.Priority;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.BlobVersionConflictException;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.VersionedBlob;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManagerFactory;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryCleanupResult;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.RepositoryStats;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.repositories.SnapshotDeletionAttempt;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotShardPaths;
import org.opensearch.snapshots.SnapshotShardPaths.ShardInfo;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.opensearch.repositories.RepositoryDataTests.generateRandomRepoData;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.calculateMaxWithinIntLimit;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
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

    static final String DIVERTING_REPO_TYPE = "fsLikeDiverting";

    static final AtomicInteger divertedNarrowDeletes = new AtomicInteger();

    static final String COUNTING_REPO_TYPE = "fsLikeCounting";
    static final AtomicInteger countedDeleteInternalCalls = new AtomicInteger();
    static final AtomicInteger countedGenerationWrites = new AtomicInteger();

    static final String INJECTING_REPO_TYPE = "fsLikeInjecting";
    static final AtomicBoolean failShardBlobDeleteOnce = new AtomicBoolean();
    static final AtomicInteger injectedShardBlobDeleteFailures = new AtomicInteger();

    static volatile boolean conditionalWrites;
    static volatile boolean failIndexLatestWrites;
    static final AtomicInteger plainIndexLatestWrites = new AtomicInteger();
    static final AtomicInteger conditionalIndexLatestWrites = new AtomicInteger();
    static final AtomicReference<CountDownLatch[]> parkNextIndexLatestWrite = new AtomicReference<>();
    static final AtomicReference<CountDownLatch[]> parkNextIndexNWrite = new AtomicReference<>();

    enum StoreBehaviour {
        ENFORCING,
        IGNORES_PRECONDITIONS
    }

    static volatile StoreBehaviour storeBehaviour = StoreBehaviour.ENFORCING;
    // Guarded by this map.
    static final Map<Path, Long> blobVersions = new HashMap<>();
    static final Semaphore probesDone = new Semaphore(0);
    static final AtomicInteger probeClaimChecks = new AtomicInteger();
    static final AtomicInteger conditionalWriteCalls = new AtomicInteger();

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
            final Map<String, Repository.Factory> factories = new HashMap<>();
            factories.put(
                REPO_TYPE,
                (metadata) -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings) {
                    @Override
                    protected void assertSnapshotOrGenericThread() {
                        // eliminate thread name check as we access blobStore on test/main threads
                    }
                }
            );
            factories.put(
                DIVERTING_REPO_TYPE,
                (metadata) -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings) {
                    @Override
                    protected void assertSnapshotOrGenericThread() {}

                    @Override
                    protected BlobStore createBlobStore() throws Exception {
                        final FsBlobStore store = (FsBlobStore) super.createBlobStore();
                        return new InjectingFsBlobStore(store.bufferSizeInBytes(), store.path(), isReadOnly());
                    }

                    @Override
                    public void deleteSnapshots(
                        Collection<SnapshotId> snapshotIds,
                        long repositoryStateId,
                        Version repositoryMetaVersion,
                        ActionListener<RepositoryData> listener
                    ) {
                        divertedNarrowDeletes.incrementAndGet();
                        listener.onFailure(new RepositoryException(metadata.name(), "diverted"));
                    }
                }
            );
            factories.put(
                COUNTING_REPO_TYPE,
                (metadata) -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings) {
                    @Override
                    protected void assertSnapshotOrGenericThread() {}

                    @Override
                    protected BlobStore createBlobStore() throws Exception {
                        final FsBlobStore store = (FsBlobStore) super.createBlobStore();
                        return new InjectingFsBlobStore(store.bufferSizeInBytes(), store.path(), isReadOnly());
                    }

                    @Override
                    public void deleteSnapshotsInternal(
                        Collection<SnapshotId> snapshotIds,
                        long repositoryStateId,
                        Version repositoryMetaVersion,
                        RemoteStoreLockManagerFactory remoteStoreLockManagerFactory,
                        RemoteSegmentStoreDirectoryFactory remoteSegmentStoreDirectoryFactory,
                        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService,
                        Map<SnapshotId, Long> snapshotIdsPinnedTimestampMap,
                        boolean isShallowSnapshotV2,
                        ActionListener<RepositoryData> listener
                    ) {
                        countedDeleteInternalCalls.incrementAndGet();
                        super.deleteSnapshotsInternal(
                            snapshotIds,
                            repositoryStateId,
                            repositoryMetaVersion,
                            remoteStoreLockManagerFactory,
                            remoteSegmentStoreDirectoryFactory,
                            remoteStorePinnedTimestampService,
                            snapshotIdsPinnedTimestampMap,
                            isShallowSnapshotV2,
                            listener
                        );
                    }

                    @Override
                    protected void writeIndexGen(
                        RepositoryData repositoryData,
                        long expectedGen,
                        Version version,
                        Function<ClusterState, ClusterState> stateFilter,
                        Priority repositoryUpdatePriority,
                        ActionListener<RepositoryData> listener
                    ) {
                        countedGenerationWrites.incrementAndGet();
                        super.writeIndexGen(repositoryData, expectedGen, version, stateFilter, repositoryUpdatePriority, listener);
                    }
                }
            );
            factories.put(
                INJECTING_REPO_TYPE,
                (metadata) -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings) {
                    @Override
                    protected void assertSnapshotOrGenericThread() {}

                    @Override
                    protected BlobStore createBlobStore() throws Exception {
                        final FsBlobStore store = (FsBlobStore) super.createBlobStore();
                        return new InjectingFsBlobStore(store.bufferSizeInBytes(), store.path(), isReadOnly());
                    }

                    @Override
                    public Optional<AbandonableSnapshotDelete> abandonableSnapshotDelete() {
                        return blobStoreAbandonableSnapshotDelete();
                    }
                }
            );
            return factories;
        }
    }

    static final class InjectingFsBlobStore extends FsBlobStore {
        InjectingFsBlobStore(int bufferSizeInBytes, Path path, boolean readonly) throws IOException {
            super(bufferSizeInBytes, path, readonly);
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            try {
                return new InjectingFsBlobContainer(this, path, buildAndCreate(path));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    static final class InjectingFsBlobContainer extends FsBlobContainer {
        InjectingFsBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path) {
            super(blobStore, blobPath, path);
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            if (blobNames.stream().anyMatch(name -> name.contains("indices/")) && failShardBlobDeleteOnce.compareAndSet(true, false)) {
                injectedShardBlobDeleteFailures.incrementAndGet();
                throw new IOException("injected shard blob delete failure");
            }
            synchronized (blobVersions) {
                super.deleteBlobsIgnoringIfNotExists(blobNames);
                for (String blobName : blobNames) {
                    bumpOnPlainWrite(blobName);
                }
            }
        }

        @Override
        public boolean isConditionalWriteSupported() {
            if (isProbeContainer()) {
                probeClaimChecks.incrementAndGet();
            }
            return conditionalWrites;
        }

        @Override
        public VersionedBlob readBlobWithVersion(String blobName) throws IOException {
            if (conditionalWrites == false) {
                return super.readBlobWithVersion(blobName);
            }
            synchronized (blobVersions) {
                final byte[] content;
                try (InputStream stream = readBlob(blobName)) {
                    content = stream.readAllBytes();
                }
                return new VersionedBlob(content, versionToken(blobName));
            }
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            synchronized (blobVersions) {
                super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
                bumpOnPlainWrite(blobName);
            }
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            if (BlobStoreRepository.INDEX_LATEST_BLOB.equals(blobName)) {
                beforeIndexLatestWrite(plainIndexLatestWrites);
            }
            if (path().toArray().length == 0 && blobName.matches(BlobStoreRepository.INDEX_FILE_PREFIX + "[0-9]+")) {
                park(parkNextIndexNWrite.getAndSet(null));
            }
            synchronized (blobVersions) {
                super.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
                bumpOnPlainWrite(blobName);
            }
        }

        @Override
        public String writeBlobConditionally(String blobName, InputStream inputStream, long blobSize, String expectedVersionToken)
            throws IOException {
            if (BlobStoreRepository.INDEX_LATEST_BLOB.equals(blobName)) {
                beforeIndexLatestWrite(conditionalIndexLatestWrites);
            }
            conditionalWriteCalls.incrementAndGet();
            if (conditionalWrites == false) {
                return super.writeBlobConditionally(blobName, inputStream, blobSize, expectedVersionToken);
            }
            final byte[] content = inputStream.readAllBytes();
            synchronized (blobVersions) {
                if (storeBehaviour != StoreBehaviour.IGNORES_PRECONDITIONS) {
                    final String current = blobExists(blobName) ? versionToken(blobName) : null;
                    if (Objects.equals(expectedVersionToken, current) == false) {
                        throw new BlobVersionConflictException("[" + blobName + "] is not at version [" + expectedVersionToken + "]");
                    }
                }
                super.writeBlobAtomic(blobName, new ByteArrayInputStream(content), content.length, false);
                blobVersions.merge(path.resolve(blobName), 1L, Long::sum);
                return versionToken(blobName);
            }
        }

        @Override
        public DeleteResult delete() throws IOException {
            final boolean probe = isProbeContainer()
                && listBlobs().keySet().stream().noneMatch(name -> name.equals("master.dat") || name.startsWith("data-"));
            final DeleteResult result = super.delete();
            if (probe) {
                probesDone.release();
            }
            return result;
        }

        private boolean isProbeContainer() {
            final String[] parts = path().toArray();
            return parts.length == 1 && parts[0].startsWith("tests-");
        }

        private String versionToken(String blobName) {
            return "v" + blobVersions.getOrDefault(path.resolve(blobName), 0L);
        }

        private void bumpOnPlainWrite(String blobName) {
            if (conditionalWrites) {
                blobVersions.merge(path.resolve(blobName), 1L, Long::sum);
            }
        }

        private static void beforeIndexLatestWrite(AtomicInteger attempts) throws IOException {
            attempts.incrementAndGet();
            park(parkNextIndexLatestWrite.getAndSet(null));
            if (failIndexLatestWrites) {
                throw new IOException("injected index.latest write failure");
            }
        }

        private static void park(CountDownLatch[] park) throws IOException {
            if (park != null) {
                park[0].countDown();
                try {
                    if (park[1].await(30, TimeUnit.SECONDS) == false) {
                        throw new IOException("a parked write was never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
        }
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put("thread_pool.snapshot.max", 4).build();
    }

    public void testRetrieveSnapshots() throws Exception {
        final Client client = client();
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        logger.info("-->  creating repository");
        Settings.Builder settings = Settings.builder().put(node().settings()).put("location", location);
        OpenSearchIntegTestCase.putRepository(client.admin().cluster(), repositoryName, REPO_TYPE, settings);

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
        repository.writeIndexGen(repositoryData, startingGeneration, Version.CURRENT, Function.identity(), Priority.NORMAL, future1);

        // write repo data again to index generational file, errors because we already wrote to the
        // N+1 generation from which this repository data instance was created
        expectThrows(
            RepositoryException.class,
            () -> writeIndexGen(repository, repositoryData.withGenId(startingGeneration + 1), repositoryData.getGenId())
        );
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAbandonedDeletionCommitsNothing() throws Exception {
        final BlobStoreRepository repository = declaringRepository("abandoned-commits-nothing");
        final RepositoryData initial = addRandomSnapshotsToRepoData(RepositoryData.EMPTY, false);
        writeIndexGen(repository, initial, RepositoryData.EMPTY_REPO_GEN);
        final Repository.AbandonableSnapshotDelete entrypoint = proveDeclared(repository);

        final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final long generationBefore = committed.getGenId();
        final long pointerBefore = repository.readSnapshotIndexLatestBlob();
        final SnapshotId toDelete = committed.getSnapshotIds().iterator().next();

        final SnapshotDeletionAttempt deletion = new SnapshotDeletionAttempt();
        deletion.expire(ActionListener.wrap(() -> {}));

        final PlainActionFuture<RepositoryData> future = PlainActionFuture.newFuture();
        entrypoint.deleteSnapshots(Collections.singleton(toDelete), generationBefore, Version.CURRENT, deletion, future);
        final RepositoryException failure = expectThrows(RepositoryException.class, () -> future.actionGet(TimeValue.timeValueSeconds(30)));
        assertFalse(
            "no index-N blob may be written for an abandoned deletion",
            repository.blobContainer().blobExists(BlobStoreRepository.INDEX_FILE_PREFIX + (generationBefore + 1))
        );
        assertThat(failure.getMessage(), containsString("was abandoned before its generation commit"));

        final RepositoryData afterwards = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        assertThat("the generation must not have moved", afterwards.getGenId(), equalTo(generationBefore));
        assertTrue(
            "the snapshot must still be recorded, so a later deletion can still remove it",
            afterwards.getSnapshotIds().contains(toDelete)
        );
        assertThat(
            "the pointer must still name the generation that is committed",
            repository.readSnapshotIndexLatestBlob(),
            equalTo(pointerBefore)
        );
    }

    public void testBadChunksize() throws Exception {
        final Client client = client();
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";
        Settings.Builder settings = Settings.builder()
            .put(node().settings())
            .put("location", location)
            .put("chunk_size", randomLongBetween(-10, 0), ByteSizeUnit.BYTES);
        expectThrows(
            RepositoryException.class,
            () -> OpenSearchIntegTestCase.putRepository(client.admin().cluster(), repositoryName, REPO_TYPE, settings)
        );
    }

    public void testPrefixModeVerification() throws Exception {
        final Client client = client();
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";
        Settings.Builder settings = Settings.builder()
            .put(node().settings())
            .put("location", location)
            .put(BlobStoreRepository.PREFIX_MODE_VERIFICATION_SETTING.getKey(), true);
        OpenSearchIntegTestCase.putRepository(client.admin().cluster(), repositoryName, REPO_TYPE, settings);

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

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testIndexLatestIsWrittenAfterTheCommitOnlyOnAProvenStore() throws Exception {
        final RemoteStorePinnedTimestampService pinning = mock(RemoteStorePinnedTimestampService.class);
        doAnswer(invocation -> {
            invocation.<ActionListener<Void>>getArgument(2).onResponse(null);
            return null;
        }).when(pinning).unpinTimestamp(anyLong(), anyString(), any());
        for (String writer : List.of(
            "generation-write",
            "narrow",
            "lock-file",
            "pinned-timestamp",
            "ignores-preconditions",
            "claims-nothing",
            "budgeted"
        )) {
            final BlobStoreRepository repository = declaringRepository("pointer-order-" + writer);
            writeIndexGen(repository, withSnapshots("a"), RepositoryData.EMPTY_REPO_GEN);
            final boolean budgeted = writer.equals("budgeted");
            final Repository.AbandonableSnapshotDelete entrypoint = budgeted ? proveDeclared(repository) : null;
            final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
            final long generation = committed.getGenId();
            final SnapshotId toDelete = committed.getSnapshotIds().iterator().next();
            conditionalWrites = budgeted || writer.equals("ignores-preconditions");
            storeBehaviour = writer.equals("ignores-preconditions") ? StoreBehaviour.IGNORES_PRECONDITIONS : StoreBehaviour.ENFORCING;
            final long seen;
            try {
                if (writer.equals("ignores-preconditions") || writer.equals("claims-nothing")) {
                    assertNeverHandedOut(repository);
                    assertEquals("[" + writer + "] the probe asks the store once, and not again once refuted", 1, probeClaimChecks.get());
                    if (writer.equals("claims-nothing")) {
                        assertEquals("[" + writer + "] and makes no conditional write", 0, conditionalWriteCalls.get());
                    }
                }
                plainIndexLatestWrites.set(0);
                conditionalIndexLatestWrites.set(0);
                seen = indexLatestWhenTheGenerationCommits(repository, generation, f -> {
                    if (budgeted) {
                        entrypoint.deleteSnapshots(
                            Collections.singleton(toDelete),
                            generation,
                            Version.CURRENT,
                            new SnapshotDeletionAttempt(),
                            f
                        );
                    } else if (writer.equals("narrow")) {
                        repository.deleteSnapshots(Collections.singleton(toDelete), generation, Version.CURRENT, f);
                    } else if (writer.equals("lock-file")) {
                        repository.deleteSnapshotsAndReleaseLockFiles(
                            Collections.singleton(toDelete),
                            generation,
                            Version.CURRENT,
                            null,
                            f
                        );
                    } else if (writer.equals("pinned-timestamp")) {
                        repository.deleteSnapshotsWithPinnedTimestamp(Map.of(toDelete, 1L), generation, Version.CURRENT, null, pinning, f);
                    } else {
                        repository.writeIndexGen(committed, generation, Version.CURRENT, Function.identity(), Priority.NORMAL, f);
                    }
                });
            } finally {
                conditionalWrites = false;
                storeBehaviour = StoreBehaviour.ENFORCING;
            }
            assertEquals("[" + writer + "] index.latest when the generation commits", budgeted ? generation : generation + 1, seen);
            assertEquals(
                "[" + writer + "] index.latest once the writer answered",
                generation + 1,
                repository.readSnapshotIndexLatestBlob()
            );
            assertEquals("[" + writer + "] plain index.latest writes", budgeted ? 0 : 1, plainIndexLatestWrites.get());
            assertEquals("[" + writer + "] conditional index.latest writes", budgeted ? 1 : 0, conditionalIndexLatestWrites.get());
        }
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testIndexLatestNamesTheLastCommitWhenWritersRaceOnAProvenStore() throws Exception {
        for (String race : List.of(
            "delayed-delete/generation-write",
            "delayed-delete/narrow-delete",
            "delayed-delete/cleanup",
            "delayed-delete/budgeted-delete",
            "losing-write/budgeted-delete",
            "losing-write/cleanup"
        )) {
            final boolean delayedDelete = race.startsWith("delayed-delete/");
            final String writer = race.substring(race.indexOf('/') + 1);
            final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
            final BlobStoreRepository repository = putRepository("race-" + race.replace('/', '-'), INJECTING_REPO_TYPE, location);
            writeIndexGen(repository, withSnapshots("a", "b"), RepositoryData.EMPTY_REPO_GEN);
            final Repository.AbandonableSnapshotDelete entrypoint = proveDeclared(repository);
            final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
            final long generation = committed.getGenId();
            final Iterator<SnapshotId> ids = committed.getSnapshotIds().iterator();
            final SnapshotId first = ids.next();
            final SnapshotId second = ids.next();
            final CountDownLatch parked = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final PlainActionFuture<RepositoryData> parkedWriter = PlainActionFuture.newFuture();
            conditionalWrites = true;
            (delayedDelete ? parkNextIndexLatestWrite : parkNextIndexNWrite).set(new CountDownLatch[] { parked, release });
            try {
                try {
                    if (delayedDelete) {
                        entrypoint.deleteSnapshots(
                            Collections.singleton(first),
                            generation,
                            Version.CURRENT,
                            new SnapshotDeletionAttempt(),
                            parkedWriter
                        );
                    } else {
                        repository.writeIndexGen(
                            committed,
                            generation,
                            Version.CURRENT,
                            Function.identity(),
                            Priority.NORMAL,
                            parkedWriter
                        );
                    }
                    assertTrue("[" + race + "] the parked writer never reached its write", parked.await(30, TimeUnit.SECONDS));
                    final RepositoryData current = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
                    plainIndexLatestWrites.set(0);
                    conditionalIndexLatestWrites.set(0);
                    if (writer.equals("generation-write")) {
                        writeIndexGen(repository, current, current.getGenId());
                    } else if (writer.equals("narrow-delete")) {
                        PlainActionFuture.<RepositoryData, Exception>get(
                            f -> repository.deleteSnapshots(Collections.singleton(second), current.getGenId(), Version.CURRENT, f)
                        );
                    } else if (writer.equals("budgeted-delete")) {
                        PlainActionFuture.<RepositoryData, Exception>get(
                            f -> entrypoint.deleteSnapshots(
                                Collections.singleton(second),
                                current.getGenId(),
                                Version.CURRENT,
                                new SnapshotDeletionAttempt(),
                                f
                            )
                        );
                    } else {
                        Files.write(
                            location.resolve(BlobStoreRepository.SNAPSHOT_FORMAT.blobName(UUIDs.randomBase64UUID())),
                            new byte[] { 1 }
                        );
                        PlainActionFuture.<RepositoryCleanupResult, Exception>get(
                            f -> repository.cleanup(current.getGenId(), Version.CURRENT, null, null, f)
                        );
                    }
                    assertEquals(
                        "[" + race + "] the writer that ran must have committed the next generation",
                        generation + 2,
                        OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).getGenId()
                    );
                    assertEquals("[" + race + "] and must not write index.latest with a plain write", 0, plainIndexLatestWrites.get());
                    assertTrue("[" + race + "] it writes index.latest with a conditional write", conditionalIndexLatestWrites.get() > 0);
                    if (delayedDelete) {
                        assertFalse(
                            "[" + race + "] it must have removed index-(N+1), so a lowered index.latest would name no blob",
                            Files.exists(location.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + (generation + 1)))
                        );
                    }
                    plainIndexLatestWrites.set(0);
                    conditionalIndexLatestWrites.set(0);
                } finally {
                    release.countDown();
                    parkNextIndexLatestWrite.set(null);
                    parkNextIndexNWrite.set(null);
                }
                if (delayedDelete) {
                    parkedWriter.actionGet(TimeValue.timeValueSeconds(30));
                } else {
                    expectThrows(Exception.class, () -> parkedWriter.actionGet(TimeValue.timeValueSeconds(30)));
                }
            } finally {
                conditionalWrites = false;
                parkNextIndexLatestWrite.set(null);
                parkNextIndexNWrite.set(null);
            }
            assertEquals(
                "[" + race + "] the parked writer, once released, must write no index.latest",
                0,
                plainIndexLatestWrites.get() + conditionalIndexLatestWrites.get()
            );
            assertEquals(
                "[" + race + "] index.latest must name the generation that committed last",
                generation + 2,
                repository.readSnapshotIndexLatestBlob()
            );
            assertTrue(Files.exists(location.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + (generation + 2))));
        }
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testABudgetedDeleteConfirmsIndexLatestWhateverItHeld() throws Exception {
        for (String held : List.of("valid", "dangling", "malformed", "missing")) {
            final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
            final BlobStoreRepository repository = putRepository("pointer-" + held, INJECTING_REPO_TYPE, location);
            writeIndexGen(repository, withSnapshots("a"), RepositoryData.EMPTY_REPO_GEN);
            final Repository.AbandonableSnapshotDelete entrypoint = proveDeclared(repository);
            final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
            final long generation = committed.getGenId();
            final SnapshotId toDelete = committed.getSnapshotIds().iterator().next();
            final Path pointer = location.resolve(BlobStoreRepository.INDEX_LATEST_BLOB);
            if (held.equals("dangling")) {
                Files.write(pointer, Numbers.longToBytes(1000L));
            } else if (held.equals("malformed")) {
                Files.write(pointer, new byte[] { 1, 2, 3 });
            } else if (held.equals("missing")) {
                Files.delete(pointer);
            }
            conditionalWrites = true;
            try {
                PlainActionFuture.<RepositoryData, Exception>get(
                    f -> entrypoint.deleteSnapshots(
                        Collections.singleton(toDelete),
                        generation,
                        Version.CURRENT,
                        new SnapshotDeletionAttempt(),
                        f
                    )
                );
            } finally {
                conditionalWrites = false;
            }
            assertEquals(
                "[" + held + "] index.latest must now name the committed generation",
                Long.BYTES + ":" + (generation + 1),
                Files.size(pointer) + ":" + repository.readSnapshotIndexLatestBlob()
            );
            assertEquals(
                "[" + held + "] and the index-N blobs it supersedes are removed",
                Set.of(BlobStoreRepository.INDEX_FILE_PREFIX + (generation + 1)),
                rootIndexN(location)
            );
        }
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARepositoryThatDoesNotDeclareTheEntrypointKeepsItsOverrides() throws Exception {
        conditionalWrites = true;
        try {
            final BlobStoreRepository counting = putRepository(
                "counting-repo",
                COUNTING_REPO_TYPE,
                OpenSearchIntegTestCase.randomRepoPath(node().settings())
            );
            twoRealSnapshotsReturningTheFirst("counting-repo");
            assertTrue("a repository that does not declare hands out no entrypoint", counting.abandonableSnapshotDelete().isEmpty());
            countedDeleteInternalCalls.set(0);
            countedGenerationWrites.set(0);
            assertTrue(client().admin().cluster().prepareDeleteSnapshot("counting-repo", "first").get().isAcknowledged());
            assertEquals("the delete goes through the public deleteSnapshotsInternal", 1, countedDeleteInternalCalls.get());
            assertEquals("and its generation through the protected writeIndexGen", 1, countedGenerationWrites.get());

            final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
            final BlobStoreRepository diverting = putRepository("diverting-repo", DIVERTING_REPO_TYPE, location);
            final SnapshotId diverted = twoRealSnapshotsReturningTheFirst("diverting-repo");
            assertTrue("nor does one that overrides the narrow delete", diverting.abandonableSnapshotDelete().isEmpty());
            divertedNarrowDeletes.set(0);
            final Exception failure = expectThrows(
                Exception.class,
                () -> client().admin().cluster().prepareDeleteSnapshot("diverting-repo", "first").get()
            );
            assertEquals("its narrow override answers the delete", 1, divertedNarrowDeletes.get());
            assertTrue(
                "with the override's own failure",
                ExceptionsHelper.unwrapCausesAndSuppressed(failure, t -> String.valueOf(t.getMessage()).contains("diverted")).isPresent()
            );
            final RepositoryData after = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(diverting);
            assertTrue("the snapshot is still recorded", after.getSnapshotIds().contains(diverted));
            assertTrue(
                "its root blob is still there",
                Files.exists(location.resolve(BlobStoreRepository.SNAPSHOT_FORMAT.blobName(diverted.getUUID())))
            );
            final IndexId indexId = after.getIndices().values().iterator().next();
            try (Stream<Path> blobs = Files.walk(location)) {
                assertTrue(
                    "and so are its shard blobs",
                    blobs.anyMatch(blob -> blob.toString().contains(BlobStoreRepository.INDICES_DIR + "/" + indexId.getId() + "/"))
                );
            }
            final String restored = "restored-" + indexId.getName();
            assertEquals(
                0,
                client().admin()
                    .cluster()
                    .prepareRestoreSnapshot("diverting-repo", "first")
                    .setRenamePattern(indexId.getName())
                    .setRenameReplacement(restored)
                    .setWaitForCompletion(true)
                    .get()
                    .getRestoreInfo()
                    .failedShards()
            );
            ensureGreen(restored);
            client().admin().indices().prepareRefresh(restored).get();
            assertEquals(
                "the restored index holds every document the snapshot took",
                5L,
                client().prepareSearch(restored).setSize(0).get().getHits().getTotalHits().value()
            );
        } finally {
            conditionalWrites = false;
        }
    }

    private static void assertNeverHandedOut(BlobStoreRepository repository) throws InterruptedException {
        probesDone.drainPermits();
        probeClaimChecks.set(0);
        conditionalWriteCalls.set(0);
        assertTrue("no entrypoint is handed out before a probe of the store passes", repository.abandonableSnapshotDelete().isEmpty());
        assertTrue("the store probe did not complete", probesDone.tryAcquire(30, TimeUnit.SECONDS));
        assertTrue("nor once a probe of the store has failed", repository.abandonableSnapshotDelete().isEmpty());
        assertTrue("nor on a later read", repository.abandonableSnapshotDelete().isEmpty());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnUnconfirmedIndexLatestKeepsEveryIndexNItMayName() throws Exception {
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final BlobStoreRepository repository = putRepository("unconfirmed-pointer", INJECTING_REPO_TYPE, location);
        writeIndexGen(repository, withSnapshots("a"), RepositoryData.EMPTY_REPO_GEN);
        for (int i = 0; i < 3; i++) {
            final RepositoryData current = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
            writeIndexGen(repository, current, current.getGenId());
        }
        final Repository.AbandonableSnapshotDelete entrypoint = proveDeclared(repository);
        final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final long generation = committed.getGenId();
        final SnapshotId toDelete = committed.getSnapshotIds().iterator().next();
        final Path belowThePointer = location.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + (generation - 3));
        Files.write(belowThePointer, new byte[] { 1 });
        final Path staleRootBlob = location.resolve(BlobStoreRepository.SNAPSHOT_FORMAT.blobName(toDelete.getUUID()));
        Files.write(staleRootBlob, new byte[] { 1 });
        plainIndexLatestWrites.set(0);
        conditionalIndexLatestWrites.set(0);
        conditionalWrites = true;
        failIndexLatestWrites = true;
        try {
            PlainActionFuture.<RepositoryData, Exception>get(
                f -> entrypoint.deleteSnapshots(
                    Collections.singleton(toDelete),
                    generation,
                    Version.CURRENT,
                    new SnapshotDeletionAttempt(),
                    f
                )
            );
            Files.write(location.resolve(BlobStoreRepository.SNAPSHOT_FORMAT.blobName(UUIDs.randomBase64UUID())), new byte[] { 1 });
            PlainActionFuture.<RepositoryCleanupResult, Exception>get(
                f -> repository.cleanup(generation + 1, Version.CURRENT, null, null, f)
            );
        } finally {
            failIndexLatestWrites = false;
            conditionalWrites = false;
        }
        assertTrue("the entrypoint stays handed out", repository.abandonableSnapshotDelete().isPresent());
        assertEquals("a proven store's index.latest is written only by conditional writes", 0, plainIndexLatestWrites.get());
        assertTrue("and conditional writes were made", conditionalIndexLatestWrites.get() > 0);
        assertEquals(
            "both writers committed",
            generation + 2,
            OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).getGenId()
        );
        final long pointer = repository.readSnapshotIndexLatestBlob();
        assertEquals("the failed writes left index.latest where it was", generation, pointer);
        assertTrue(
            "the index-N blob index.latest still names must survive both cleanups",
            Files.exists(location.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + pointer))
        );
        assertFalse("an index-N below the confirmed pointer is still removed", Files.exists(belowThePointer));
        assertFalse("and the rest of the cleanup still runs", Files.exists(staleRootBlob));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnExpiryDuringTheCommitIsAnsweredWithTheCommittedGeneration() throws Exception {
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final BlobStoreRepository repository = putRepository("commit-in-flight", INJECTING_REPO_TYPE, location);
        writeIndexGen(repository, withSnapshots("a", "b"), RepositoryData.EMPTY_REPO_GEN);
        final Repository.AbandonableSnapshotDelete entrypoint = proveDeclared(repository);
        final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final long generation = committed.getGenId();
        final SnapshotId toDelete = committed.getSnapshotIds().iterator().next();
        final Path deletedSnapshotBlob = location.resolve(BlobStoreRepository.SNAPSHOT_FORMAT.blobName(toDelete.getUUID()));
        Files.write(deletedSnapshotBlob, new byte[] { 1 });
        final SnapshotDeletionAttempt attempt = new SnapshotDeletionAttempt();
        final PlainActionFuture<RepositoryData> answeredOnExpiry = PlainActionFuture.newFuture();
        final List<SnapshotDeletionAttempt.Expiry> expiries = new CopyOnWriteArrayList<>();
        final String name = repository.getMetadata().name();
        final ClusterStateListener expireAtCommit = event -> {
            if (committedGeneration(event.state(), name) == generation + 1 && expiries.isEmpty()) {
                expiries.add(attempt.expire(answeredOnExpiry));
            }
        };
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        clusterService.addListener(expireAtCommit);
        conditionalWrites = true;
        try {
            final PlainActionFuture<RepositoryData> deleted = PlainActionFuture.newFuture();
            entrypoint.deleteSnapshots(Collections.singleton(toDelete), generation, Version.CURRENT, attempt, deleted);
            deleted.actionGet(TimeValue.timeValueSeconds(30));
        } finally {
            conditionalWrites = false;
            clusterService.removeListener(expireAtCommit);
        }
        assertEquals(
            "the expiry must have arrived while the commit was in flight",
            List.of(SnapshotDeletionAttempt.Expiry.PENDING),
            expiries
        );
        final RepositoryData answered = answeredOnExpiry.actionGet(TimeValue.timeValueSeconds(30));
        assertEquals("the expiry is answered with the committed generation", generation + 1, answered.getGenId());
        assertFalse(answered.getSnapshotIds().contains(toDelete));
        assertEquals("index.latest must still name the generation before the commit", generation, repository.readSnapshotIndexLatestBlob());
        assertTrue(
            "the superseded index-N blob must be left",
            Files.exists(location.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + generation))
        );
        assertTrue("and so must the deleted snapshot's root blob", Files.exists(deletedSnapshotBlob));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnAttemptAbandonedAtItsCommitStartsNoShardBlobDelete() throws Exception {
        final BlobStoreRepository repository = declaringRepository("abandoned-at-commit");
        final SnapshotId toDelete = twoRealSnapshotsReturningTheFirst("abandoned-at-commit");
        final Repository.AbandonableSnapshotDelete entrypoint = proveDeclared(repository);
        final long generation = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).getGenId();
        final SnapshotDeletionAttempt attempt = new SnapshotDeletionAttempt();
        final List<SnapshotDeletionAttempt.Expiry> expiries = new CopyOnWriteArrayList<>();
        final ClusterStateListener expireAtCommit = event -> {
            if (committedGeneration(event.state(), "abandoned-at-commit") == generation + 1 && expiries.isEmpty()) {
                expiries.add(attempt.expire(ActionListener.wrap(() -> {})));
            }
        };
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        clusterService.addListener(expireAtCommit);
        injectedShardBlobDeleteFailures.set(0);
        failShardBlobDeleteOnce.set(true);
        conditionalWrites = true;
        final RepositoryData answered;
        try {
            final PlainActionFuture<RepositoryData> deleted = PlainActionFuture.newFuture();
            entrypoint.deleteSnapshots(Collections.singleton(toDelete), generation, Version.CURRENT, attempt, deleted);
            answered = deleted.actionGet(TimeValue.timeValueSeconds(30));
        } finally {
            conditionalWrites = false;
            failShardBlobDeleteOnce.set(false);
            clusterService.removeListener(expireAtCommit);
        }
        assertEquals("the attempt must expire while its commit is in flight", List.of(SnapshotDeletionAttempt.Expiry.PENDING), expiries);
        assertEquals("the delete answers with the generation it committed", generation + 1, answered.getGenId());
        assertEquals("an abandoned attempt must start no shard blob delete", 0, injectedShardBlobDeleteFailures.get());
    }

    private static Set<String> rootIndexN(Path location) throws IOException {
        try (Stream<Path> blobs = Files.list(location)) {
            return blobs.map(blob -> blob.getFileName().toString())
                .filter(name -> name.startsWith(BlobStoreRepository.INDEX_FILE_PREFIX))
                .collect(Collectors.toSet());
        }
    }

    private static void writeIndexGen(BlobStoreRepository repository, RepositoryData repositoryData, long generation) throws Exception {
        PlainActionFuture.<RepositoryData, Exception>get(
            f -> repository.writeIndexGen(repositoryData, generation, Version.CURRENT, Function.identity(), Priority.NORMAL, f)
        );
    }

    private BlobStoreRepository putRepository(String name, String type, Path location) {
        OpenSearchIntegTestCase.putRepository(
            client().admin().cluster(),
            name,
            type,
            Settings.builder().put(node().settings()).put("location", location)
        );
        return (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository(name);
    }

    private BlobStoreRepository declaringRepository(String name) {
        return putRepository(name, INJECTING_REPO_TYPE, OpenSearchIntegTestCase.randomRepoPath(node().settings()));
    }

    private static Repository.AbandonableSnapshotDelete proveDeclared(BlobStoreRepository repository) throws InterruptedException {
        probesDone.drainPermits();
        final boolean previous = conditionalWrites;
        conditionalWrites = true;
        try {
            assertTrue("no entrypoint is handed out before a probe of the store passes", repository.abandonableSnapshotDelete().isEmpty());
            assertTrue("the store probe did not complete", probesDone.tryAcquire(30, TimeUnit.SECONDS));
            final Optional<Repository.AbandonableSnapshotDelete> entrypoint = repository.abandonableSnapshotDelete();
            assertTrue("a declaring repository over a proven store hands out the entrypoint", entrypoint.isPresent());
            return entrypoint.get();
        } finally {
            conditionalWrites = previous;
        }
    }

    private SnapshotId twoRealSnapshotsReturningTheFirst(String repositoryName) {
        final String indexName = "idx-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createIndex(indexName);
        ensureGreen();
        for (int i = 0; i < 5; i++) {
            client().prepareIndex(indexName).setId(Integer.toString(i)).setSource("text", "sometext").get();
        }
        client().admin().indices().prepareFlush(indexName).get();
        final SnapshotId first = client().admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, "first")
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get()
            .getSnapshotInfo()
            .snapshotId();
        client().admin().cluster().prepareCreateSnapshot(repositoryName, "second").setWaitForCompletion(true).setIndices(indexName).get();
        return first;
    }

    private static RepositoryData withSnapshots(String... names) {
        RepositoryData data = RepositoryData.EMPTY;
        for (String name : names) {
            data = data.addSnapshot(
                new SnapshotId(name, UUIDs.randomBase64UUID()),
                SnapshotState.SUCCESS,
                Version.CURRENT,
                ShardGenerations.EMPTY,
                Collections.emptyMap(),
                Collections.emptyMap()
            );
        }
        return data;
    }

    private static long readIndexLatest(BlobStoreRepository repository) {
        try {
            return repository.readSnapshotIndexLatestBlob();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long committedGeneration(ClusterState state, String repository) {
        final RepositoriesMetadata repositories = state.metadata().custom(RepositoriesMetadata.TYPE);
        final RepositoryMetadata metadata = repositories == null ? null : repositories.repository(repository);
        return metadata == null ? RepositoryData.UNKNOWN_REPO_GEN : metadata.generation();
    }

    private long indexLatestWhenTheGenerationCommits(
        BlobStoreRepository repository,
        long from,
        Consumer<ActionListener<RepositoryData>> write
    ) throws Exception {
        final String name = repository.getMetadata().name();
        final List<Long> seen = new CopyOnWriteArrayList<>();
        final ClusterStateListener atCommit = event -> {
            if (committedGeneration(event.previousState(), name) == from && committedGeneration(event.state(), name) == from + 1) {
                seen.add(readIndexLatest(repository));
            }
        };
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        clusterService.addListener(atCommit);
        try {
            PlainActionFuture.<RepositoryData, Exception>get(write::accept);
        } finally {
            clusterService.removeListener(atCommit);
        }
        assertThat("the generation commit must have been observed exactly once", seen, hasSize(1));
        return seen.get(0);
    }

    private BlobStoreRepository setupRepo() {
        final Client client = client();
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        Settings.Builder settings = Settings.builder().put(node().settings()).put("location", location);
        OpenSearchIntegTestCase.putRepository(client.admin().cluster(), repositoryName, REPO_TYPE, settings);

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

        // Version 2.17 has file name starting with indexId
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

        // Version 2.17 has file name starting with snapshot_path_
        shardPath = String.join(
            SnapshotShardPaths.DELIMITER,
            SnapshotShardPaths.FILE_PREFIX + indexId.getId(),
            indexId.getName(),
            String.valueOf(shardCount),
            String.valueOf(indexId.getShardPathType()),
            "1"
        );
        shardInfo = SnapshotShardPaths.parseShardPath(shardPath);
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
            String[] parts = shardPathBlobName.split("\\" + SnapshotShardPaths.DELIMITER);
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

        List<SnapshotId> snapshotIds = new ArrayList<>();
        snapshotIds.add(new SnapshotId("snap1", UUIDs.randomBase64UUID()));
        snapshotIds.add(new SnapshotId("snap2", UUIDs.randomBase64UUID()));

        Set<String> survivingIndexIds = new HashSet<>();
        survivingIndexIds.add("current-index");

        RepositoryData repositoryData = generateRandomRepoData();

        // Create a mock RemoteStoreLockManagerFactory
        RemoteStoreLockManagerFactory mockRemoteStoreLockManagerFactory = mock(RemoteStoreLockManagerFactory.class);
        RemoteSegmentStoreDirectoryFactory mockRemoteSegmentStoreDirectoryFactory = mock(RemoteSegmentStoreDirectoryFactory.class);
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
            Map<String, BlobContainer> indices = invocation.getArgument(1);
            Set<String> surviving = invocation.getArgument(2);
            GroupedActionListener<DeleteResult> listener = invocation.getArgument(6);

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
        }).when(repository).cleanupStaleIndices(any(), any(), any(), any(), any(), any(), any(), any(), anyMap(), any());

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
            snapshotIds,
            foundIndices,
            survivingIndexIds,
            mockRemoteStoreLockManagerFactory,
            null,
            repositoryData,
            listener,
            mockSnapshotShardPaths,
            Collections.emptyMap(),
            null
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

    public void testAbandonmentStopsTheStaleIndexDrain() throws Exception {
        final TestThreadPool oneDeletionThread = new TestThreadPool(
            getTestName(),
            Settings.builder().put("thread_pool.snapshot_deletion.max", 1).build()
        );
        final Path location = createTempDir();
        final RepositoryMetadata metadata = new RepositoryMetadata(
            "stale-index-drain",
            FsRepository.TYPE,
            Settings.builder().put("location", location).build()
        );
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(metadata);
        when(clusterService.getClusterApplierService().threadPool()).thenReturn(oneDeletionThread);
        final BlobStoreRepository repository = new FsRepository(
            metadata,
            TestEnvironment.newEnvironment(
                Settings.builder()
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                    .put(Environment.PATH_REPO_SETTING.getKey(), location)
                    .build()
            ),
            xContentRegistry(),
            clusterService,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
        repository.start();
        try {
            final CountDownLatch enteredLatch = new CountDownLatch(1);
            final CountDownLatch releaseLatch = new CountDownLatch(1);
            final CountDownLatch doneLatch = new CountDownLatch(1);

            final BlobContainer inFlight = mock(BlobContainer.class);
            when(inFlight.delete()).thenAnswer(invocation -> {
                enteredLatch.countDown();
                releaseLatch.await();
                return new DeleteResult(1, 100L);
            });
            final BlobContainer queuedBehind1 = mock(BlobContainer.class);
            when(queuedBehind1.delete()).thenReturn(new DeleteResult(1, 100L));
            final BlobContainer queuedBehind2 = mock(BlobContainer.class);
            when(queuedBehind2.delete()).thenReturn(new DeleteResult(1, 100L));

            final Map<String, BlobContainer> staleIndices = new LinkedHashMap<>();
            staleIndices.put("in-flight-index", inFlight);
            staleIndices.put("queued-index-1", queuedBehind1);
            staleIndices.put("queued-index-2", queuedBehind2);

            final SnapshotDeletionAttempt deletion = new SnapshotDeletionAttempt();
            final GroupedActionListener<DeleteResult> listener = new GroupedActionListener<>(
                ActionListener.wrap(results -> { doneLatch.countDown(); }, e -> {
                    logger.error("Error draining the stale indices", e);
                    doneLatch.countDown();
                }),
                1
            );

            repository.cleanupStaleIndices(
                Collections.emptyList(),
                staleIndices,
                Collections.emptySet(),
                null,
                null,
                RepositoryData.EMPTY,
                listener,
                Collections.emptyMap(),
                Collections.emptyMap(),
                deletion
            );

            assertTrue("the first index's cleanup never started", enteredLatch.await(30, TimeUnit.SECONDS));
            deletion.expire(ActionListener.wrap(() -> {}));
            releaseLatch.countDown();
            assertTrue("the drain did not finish", doneLatch.await(30, TimeUnit.SECONDS));

            verify(inFlight, times(1)).delete();
            verify(queuedBehind1, never()).delete();
            verify(queuedBehind2, never()).delete();
        } finally {
            repository.close();
            ThreadPool.terminate(oneDeletionThread, 30, TimeUnit.SECONDS);
        }
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

    public void testGetStats_When_Sse_Enabled_WithExtended_Stats() {
        BlobStoreRepository repository = setupRepo();
        BlobStoreRepository repoSpy = Mockito.spy(repository);

        BlobStore blobStore = getMockedBlobStoreWithStats(10L, 20L, true);
        BlobStore sseBlobStore = getMockedBlobStoreWithStats(5L, 10L, true);

        Mockito.doReturn(blobStore).when(repoSpy).getBlobStore(false);
        Mockito.doReturn(sseBlobStore).when(repoSpy).getBlobStore(true);

        RepositoryStats stats = repoSpy.stats();
        assertNotNull(stats);
        assertTrue(stats.detailed);
        Map<String, Long> mergedStats = stats.extendedStats.get(BlobStore.Metric.REQUEST_SUCCESS);

        assertEquals(15, mergedStats.get("GET").longValue());
        assertEquals(30, mergedStats.get("PUT").longValue());

        repository.close();
    }

    public void testGetStats_When_Sse_Only_Enabled_WithExtended_Stats() {
        BlobStoreRepository repository = setupRepo();
        BlobStoreRepository repoSpy = Mockito.spy(repository);

        BlobStore sseBlobStore = getMockedBlobStoreWithStats(5L, 10L, true);
        Mockito.doReturn(sseBlobStore).when(repoSpy).getBlobStore(true);

        RepositoryStats stats = repoSpy.stats();
        assertNotNull(stats);
        assertTrue(stats.detailed);
        Map<String, Long> mergedStats = stats.extendedStats.get(BlobStore.Metric.REQUEST_SUCCESS);

        assertEquals(5, mergedStats.get("GET").longValue());
        assertEquals(10, mergedStats.get("PUT").longValue());

        repository.close();
    }

    public void testGetStats_When_Sse_not_Enabled_WithExtended_Stats() {
        BlobStoreRepository repository = setupRepo();
        BlobStoreRepository repoSpy = Mockito.spy(repository);

        BlobStore blobStore = getMockedBlobStoreWithStats(10L, 20L, true);
        Mockito.doReturn(blobStore).when(repoSpy).getBlobStore(false);

        RepositoryStats stats = repoSpy.stats();
        assertNotNull(stats);
        assertTrue(stats.detailed);
        Map<String, Long> mergedStats = stats.extendedStats.get(BlobStore.Metric.REQUEST_SUCCESS);

        assertEquals(10, mergedStats.get("GET").longValue());
        assertEquals(20, mergedStats.get("PUT").longValue());

        repository.close();
    }

    public void testGetStats_When_Sse_Enabled() {
        BlobStoreRepository repository = setupRepo();
        BlobStoreRepository repoSpy = Mockito.spy(repository);

        BlobStore blobStore = getMockedBlobStoreWithStats(10L, 20L, false);
        BlobStore sseBlobStore = getMockedBlobStoreWithStats(5L, 10L, false);

        Mockito.doReturn(blobStore).when(repoSpy).getBlobStore(false);
        Mockito.doReturn(sseBlobStore).when(repoSpy).getBlobStore(true);

        RepositoryStats stats = repoSpy.stats();
        assertNotNull(stats);
        assertFalse(stats.detailed);

        assertEquals(45, stats.requestCounts.get("requests_count").longValue());
        repository.close();
    }

    public void testGetStats_When_Sse_Disabled() {
        BlobStoreRepository repository = setupRepo();
        BlobStoreRepository repoSpy = Mockito.spy(repository);

        BlobStore blobStore = getMockedBlobStoreWithStats(10L, 20L, false);

        Mockito.doReturn(blobStore).when(repoSpy).getBlobStore(false);

        RepositoryStats stats = repoSpy.stats();
        assertNotNull(stats);
        assertFalse(stats.detailed);

        assertEquals(30, stats.requestCounts.get("requests_count").longValue());
        repository.close();
    }

    public void testGetStats_When_Sse_Only_Enabled() {
        BlobStoreRepository repository = setupRepo();
        BlobStoreRepository repoSpy = Mockito.spy(repository);

        BlobStore sseBlobStore = getMockedBlobStoreWithStats(5L, 10L, false);
        Mockito.doReturn(sseBlobStore).when(repoSpy).getBlobStore(true);

        RepositoryStats stats = repoSpy.stats();
        assertNotNull(stats);
        assertFalse(stats.detailed);

        assertEquals(15, stats.requestCounts.get("requests_count").longValue());
        repository.close();
    }

    private BlobStore getMockedBlobStoreWithStats(long getCount, long putCount, boolean extendedStats) {
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        HashMap<String, Long> blobStoreStatsMap = new HashMap<>();
        if (extendedStats) {
            blobStoreStatsMap.put("GET", getCount);
            blobStoreStatsMap.put("PUT", putCount);
            Map<BlobStore.Metric, Map<String, Long>> blobStoreMetricMap = Map.of(BlobStore.Metric.REQUEST_SUCCESS, blobStoreStatsMap);
            Mockito.when(blobStore.extendedStats()).thenReturn(blobStoreMetricMap);
        } else {
            blobStoreStatsMap.put("requests_count", getCount + putCount);
            Mockito.when(blobStore.stats()).thenReturn(blobStoreStatsMap);
        }
        return blobStore;
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

    public void testSnapshotRepositoryDataCacheDefaultSetting() {
        // given
        BlobStoreRepository repository = setupRepo();
        long maxThreshold = BlobStoreRepository.calculateMaxSnapshotRepositoryDataCacheThreshold();

        // when
        long expectedThreshold = Math.max(ByteSizeUnit.KB.toBytes(500), maxThreshold / 2);

        // then
        assertEquals(repository.repositoryDataCacheThreshold, expectedThreshold);
    }

    public void testHeapThresholdUsed() {
        // given
        long defaultThresholdOfHeap = ByteSizeUnit.GB.toBytes(1);
        long defaultAbsoluteThreshold = ByteSizeUnit.KB.toBytes(500);

        // when
        long expectedThreshold = calculateMaxWithinIntLimit(defaultThresholdOfHeap, defaultAbsoluteThreshold);

        // then
        assertEquals(defaultThresholdOfHeap, expectedThreshold);
    }

    public void testAbsoluteThresholdUsed() {
        // given
        long defaultThresholdOfHeap = ByteSizeUnit.KB.toBytes(499);
        long defaultAbsoluteThreshold = ByteSizeUnit.KB.toBytes(500);

        // when
        long result = calculateMaxWithinIntLimit(defaultThresholdOfHeap, defaultAbsoluteThreshold);

        // then
        assertEquals(defaultAbsoluteThreshold, result);
    }

    public void testThresholdCappedAtIntMax() {
        // given
        int maxSafeArraySize = Integer.MAX_VALUE - 8;
        long defaultThresholdOfHeap = (long) maxSafeArraySize + 1;
        long defaultAbsoluteThreshold = ByteSizeUnit.KB.toBytes(500);

        // when
        long expectedThreshold = calculateMaxWithinIntLimit(defaultThresholdOfHeap, defaultAbsoluteThreshold);

        // then
        assertEquals(maxSafeArraySize, expectedThreshold);
    }

    /**
     * Verifies that {@code BlobStoreRepository.remoteDirectoryCleanupAsync} propagates the
     * {@link IndexMetadata} argument through to {@code RemoteSegmentStoreDirectory.remoteDirectoryCleanup},
     * which in turn passes it as an {@link IndexSettings} to the factory's 8-arg {@code newDirectory}.
     * Guards against regressions where IndexMetadata is dropped or replaced with null during cleanup.
     */
    public void testRemoteDirectoryCleanupAsyncPropagatesIndexMetadata() throws Exception {
        RemoteSegmentStoreDirectoryFactory factory = mock(RemoteSegmentStoreDirectoryFactory.class);
        // factory.newDirectory throws IOException to short-circuit; we only care about the args passed
        when(factory.newDirectory(anyString(), anyString(), any(), any(), nullable(String.class), eq(false), eq(false), any())).thenThrow(
            new IOException("expected-short-circuit")
        );

        ThreadPool tp = mock(ThreadPool.class);
        ExecutorService directExecutor = OpenSearchExecutors.newDirectExecutorService();
        when(tp.executor(ThreadPool.Names.REMOTE_PURGE)).thenReturn(directExecutor);

        String indexUUID = "test-uuid";
        ShardId shardId = new ShardId(new Index("idx", indexUUID), 0);
        RemoteStorePathStrategy pathStrategy = new RemoteStorePathStrategy(RemoteStoreEnums.PathType.FIXED);

        IndexMetadata indexMetadata = IndexMetadata.builder("idx")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUUID)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .build();

        // Case (a): non-null IndexMetadata propagated
        BlobStoreRepository.remoteDirectoryCleanupAsync(
            factory,
            tp,
            "repo",
            indexUUID,
            shardId,
            ThreadPool.Names.REMOTE_PURGE,
            pathStrategy,
            false,
            indexMetadata
        );
        ArgumentCaptor<IndexSettings> captor = ArgumentCaptor.forClass(IndexSettings.class);
        verify(factory).newDirectory(
            eq("repo"),
            eq(indexUUID),
            eq(shardId),
            eq(pathStrategy),
            eq(null),
            eq(false),
            eq(false),
            captor.capture()
        );
        assertNotNull("IndexSettings should not be null when IndexMetadata is provided", captor.getValue());

        // Case (b): null IndexMetadata propagated as null
        Mockito.reset(factory);
        when(factory.newDirectory(anyString(), anyString(), any(), any(), nullable(String.class), eq(false), eq(false), any())).thenThrow(
            new IOException("expected-short-circuit")
        );
        BlobStoreRepository.remoteDirectoryCleanupAsync(
            factory,
            tp,
            "repo",
            indexUUID,
            shardId,
            ThreadPool.Names.REMOTE_PURGE,
            pathStrategy,
            false,
            null
        );
        verify(factory).newDirectory(
            eq("repo"),
            eq(indexUUID),
            eq(shardId),
            eq(pathStrategy),
            eq(null),
            eq(false),
            eq(false),
            (IndexSettings) eq(null)
        );
    }
}
