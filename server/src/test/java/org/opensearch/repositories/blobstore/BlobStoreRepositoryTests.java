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
import org.opensearch.search.SearchHit;
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

    /** A second type whose repository overrides the narrower delete, for the inheritance test below. */
    static final String DIVERTING_REPO_TYPE = "fsLikeDiverting";

    /** Narrow-overload calls made by the diverting type. Static because the factory that builds it is. */
    static final AtomicInteger divertedNarrowDeletes = new AtomicInteger();

    /** A type whose repository counts calls into the two methods a subclass can override on the delete path. */
    static final String COUNTING_REPO_TYPE = "fsLikeCounting";
    static final AtomicInteger countedDeleteInternalCalls = new AtomicInteger();
    static final AtomicInteger countedGenerationWrites = new AtomicInteger();

    static final String INJECTING_REPO_TYPE = "fsLikeInjecting";
    static final AtomicBoolean failShardBlobDeleteOnce = new AtomicBoolean();
    static final AtomicInteger injectedShardBlobDeleteFailures = new AtomicInteger();
    static final AtomicBoolean failIndexContainerDeleteOnce = new AtomicBoolean();
    /** The names, relative to the repository root, of the batch the one-shot shard blob delete failure refused. */
    static final List<String> failedShardBlobBatch = new CopyOnWriteArrayList<>();

    static volatile boolean conditionalWrites;
    static volatile boolean failIndexLatestWrites;
    static final AtomicInteger plainIndexLatestWrites = new AtomicInteger();
    static final AtomicInteger conditionalIndexLatestWrites = new AtomicInteger();
    /** {entered, release}: the next index.latest write of either kind parks on it, once. */
    static final AtomicReference<CountDownLatch[]> parkNextIndexLatestWrite = new AtomicReference<>();
    /** {entered, release}: the next write of a root index-N blob parks on it, once, before the blob is written. */
    static final AtomicReference<CountDownLatch[]> parkNextIndexNWrite = new AtomicReference<>();

    /**
     * How the injecting store answers the conditional-write API while {@link #conditionalWrites} is set. An ENFORCING store
     * evaluates both preconditions and versions every write and delete, so a probe of it passes; the other is the shape a
     * probe must refute.
     */
    enum StoreBehaviour {
        ENFORCING,
        /** Accepts every conditional write, whatever its precondition. */
        IGNORES_PRECONDITIONS
    }

    static volatile StoreBehaviour storeBehaviour = StoreBehaviour.ENFORCING;
    /** Per blob, by absolute path. Guarded by itself. */
    static final Map<Path, Long> blobVersions = new HashMap<>();
    /** Released when a store probe deletes its own container, which a probe does last. */
    static final Semaphore probesDone = new Semaphore(0);
    /** Claim checks made on a probe's container, and conditional writes made through the injecting store. */
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
            // A repository that tries to divert the delete by overriding the NARROWER overload -- the shape that drops an
            // attempt, because that signature has nowhere to put one. Registered as its own type so it is built by the node
            // exactly as the type above is: a hand-constructed repository cannot commit a generation, because the cluster
            // state has never heard of it and the commit is a cluster state update.
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

                    /** Declares the delete entrypoint for its own path, which it hands out once its store is proven. */
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
            // Shard-level cleanup deletes through the root container with paths under indices/, behind a hash prefix when
            // putRepository has randomly chosen a hashed shard path type; nothing else does that during a deletion, so the
            // one-shot failure lands on the shard-level unit.
            if (blobNames.stream().anyMatch(name -> name.contains("indices/")) && failShardBlobDeleteOnce.compareAndSet(true, false)) {
                injectedShardBlobDeleteFailures.incrementAndGet();
                failedShardBlobBatch.addAll(blobNames);
                throw new IOException("injected shard blob delete failure");
            }
            synchronized (blobVersions) {
                super.deleteBlobsIgnoringIfNotExists(blobNames);
                for (String blobName : blobNames) {
                    bumpOnPlainWrite(blobName);
                }
            }
        }

        /** Answers from a switch; while it is set the store versions blobs as {@link #storeBehaviour} says. */
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
            if (path().buildAsString().contains("indices/") && failIndexContainerDeleteOnce.compareAndSet(true, false)) {
                throw new IOException("injected index container delete failure");
            }
            // Repository verification also uses tests- containers, and writes master.dat or data-*.dat into them; a probe never does.
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

        /** Under {@link #blobVersions}. */
        private String versionToken(String blobName) {
            return "v" + blobVersions.getOrDefault(path.resolve(blobName), 0L);
        }

        /** Under {@link #blobVersions}. */
        private void bumpOnPlainWrite(String blobName) {
            if (conditionalWrites) {
                blobVersions.merge(path.resolve(blobName), 1L, Long::sum);
            }
        }

        // Parks before super, so no conditional-write lock is held while parked.
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
        // One test parks one snapshot-pool thread while a second deletion runs start to finish; the pool's default maximum is
        // half the processors capped at five, which can be one.
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

    /**
     * A deletion whose caller has already given up must not commit.
     * {@code testTheBudgetedDeleteUpdatesIndexLatestOnlyAfterItCommits} and
     * {@code testABudgetedDeleteAnswersSuccessAfterACleanupFailure} run the same entrypoint with a live attempt and see it
     * commit.
     */
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
        final RepositoryException failure = expectThrows(RepositoryException.class, future::actionGet);
        assertThat(failure.getMessage(), containsString("abandoned"));

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

    /**
     * On a store that is not proven, a generation write updates index.latest beside the new index-N blob, before the generation
     * commits.
     */
    public void testAGenerationWriteUpdatesIndexLatestBeforeItCommits() throws Exception {
        final BlobStoreRepository repository = setupRepo();
        writeIndexGen(repository, withSnapshots("a"), RepositoryData.EMPTY_REPO_GEN);
        final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final long generation = committed.getGenId();
        assertEquals(generation, repository.readSnapshotIndexLatestBlob());

        final List<Long> seenAtCommit = new CopyOnWriteArrayList<>();
        PlainActionFuture.<RepositoryData, Exception>get(f -> repository.writeIndexGen(committed, generation, Version.CURRENT, state -> {
            seenAtCommit.add(readIndexLatest(repository));
            return state;
        }, Priority.NORMAL, f));
        assertThat("the commit step must have run exactly once", seenAtCommit, hasSize(1));
        assertEquals("index.latest must already name the generation being committed", generation + 1, (long) seenAtCommit.get(0));
    }

    public void testEveryUnbudgetedDeleteUpdatesIndexLatestBeforeItCommits() throws Exception {
        final RemoteStorePinnedTimestampService pinning = mock(RemoteStorePinnedTimestampService.class);
        doAnswer(invocation -> {
            invocation.<ActionListener<Void>>getArgument(2).onResponse(null);
            return null;
        }).when(pinning).unpinTimestamp(anyLong(), anyString(), any());
        for (String entrypoint : List.of("narrow", "lock-file", "pinned-timestamp")) {
            final BlobStoreRepository repository = putRepository(
                "pointer-before-commit-" + entrypoint,
                REPO_TYPE,
                OpenSearchIntegTestCase.randomRepoPath(node().settings())
            );
            writeIndexGen(repository, withSnapshots("a"), RepositoryData.EMPTY_REPO_GEN);
            final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
            final SnapshotId toDelete = committed.getSnapshotIds().iterator().next();
            final long seen = indexLatestWhenTheGenerationCommits(repository, committed.getGenId(), f -> {
                if (entrypoint.equals("narrow")) {
                    repository.deleteSnapshots(Collections.singleton(toDelete), committed.getGenId(), Version.CURRENT, f);
                } else if (entrypoint.equals("lock-file")) {
                    repository.deleteSnapshotsAndReleaseLockFiles(
                        Collections.singleton(toDelete),
                        committed.getGenId(),
                        Version.CURRENT,
                        null,
                        f
                    );
                } else {
                    repository.deleteSnapshotsWithPinnedTimestamp(
                        Map.of(toDelete, 1L),
                        committed.getGenId(),
                        Version.CURRENT,
                        null,
                        pinning,
                        f
                    );
                }
            });
            assertEquals("[" + entrypoint + "] must update index.latest before it commits", committed.getGenId() + 1, seen);
        }
    }

    /** On a proven store, a budgeted deletion updates index.latest only after its generation commits. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testTheBudgetedDeleteUpdatesIndexLatestOnlyAfterItCommits() throws Exception {
        final BlobStoreRepository repository = declaringRepository("budgeted-pointer-after-commit");
        writeIndexGen(repository, withSnapshots("a"), RepositoryData.EMPTY_REPO_GEN);
        final Repository.AbandonableSnapshotDelete entrypoint = proveDeclared(repository);
        final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final SnapshotId toDelete = committed.getSnapshotIds().iterator().next();
        final long seen;
        conditionalWrites = true;
        try {
            seen = indexLatestWhenTheGenerationCommits(
                repository,
                committed.getGenId(),
                f -> entrypoint.deleteSnapshots(
                    Collections.singleton(toDelete),
                    committed.getGenId(),
                    Version.CURRENT,
                    new SnapshotDeletionAttempt(),
                    f
                )
            );
        } finally {
            conditionalWrites = false;
        }
        assertEquals("not yet moved when the commit is applied", committed.getGenId(), seen);
        assertEquals("moved once the deletion answered", committed.getGenId() + 1, repository.readSnapshotIndexLatestBlob());
    }

    /**
     * A repository that does not declare the delete entrypoint is deleted through its own overridable methods with the feature
     * on, and one that overrides the narrow delete keeps that override: it answers the delete. Both run on the injecting store,
     * which here claims and enforces conditional writes, so nothing but the declaration decides.
     */
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

    /** A declaring repository over a store that does not claim conditional writes: one probe, which refutes it, and no entrypoint. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testNoEntrypointOverAStoreThatDoesNotClaimConditionalWrites() throws Exception {
        final BlobStoreRepository repository = putRepository(
            "claims-nothing",
            INJECTING_REPO_TYPE,
            OpenSearchIntegTestCase.randomRepoPath(node().settings())
        );
        writeIndexGen(repository, withSnapshots("a"), RepositoryData.EMPTY_REPO_GEN);
        assertNeverHandedOut(repository);
        assertEquals("the probe asks the store once", 1, probeClaimChecks.get());
        assertEquals("and makes no conditional write", 0, conditionalWriteCalls.get());
    }

    /**
     * A declaring repository over a store that claims conditional writes but accepts every one of them: the probe refutes it,
     * no second probe starts, and a generation write still updates index.latest with a plain write before it commits.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testNoEntrypointOverAStoreThatIgnoresPreconditions() throws Exception {
        final BlobStoreRepository repository = putRepository(
            "ignores-preconditions",
            INJECTING_REPO_TYPE,
            OpenSearchIntegTestCase.randomRepoPath(node().settings())
        );
        writeIndexGen(repository, withSnapshots("a"), RepositoryData.EMPTY_REPO_GEN);
        conditionalWrites = true;
        storeBehaviour = StoreBehaviour.IGNORES_PRECONDITIONS;
        try {
            assertNeverHandedOut(repository);
            assertEquals("a refuted store is not probed again", 1, probeClaimChecks.get());
            plainIndexLatestWrites.set(0);
            conditionalIndexLatestWrites.set(0);
            final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
            writeIndexGen(repository, committed, committed.getGenId());
            assertEquals("the generation write updates index.latest with a plain write", 1, plainIndexLatestWrites.get());
            assertEquals("and with no conditional one", 0, conditionalIndexLatestWrites.get());
        } finally {
            conditionalWrites = false;
            storeBehaviour = StoreBehaviour.ENFORCING;
        }
    }

    /**
     * Reads the capability of a repository whose store probe is expected to fail: empty before the probe, which the first read
     * starts, and empty once it has completed.
     */
    private static void assertNeverHandedOut(BlobStoreRepository repository) throws InterruptedException {
        probesDone.drainPermits();
        probeClaimChecks.set(0);
        conditionalWriteCalls.set(0);
        assertTrue("no entrypoint is handed out before a probe of the store passes", repository.abandonableSnapshotDelete().isEmpty());
        assertTrue("the store probe did not complete", probesDone.tryAcquire(30, TimeUnit.SECONDS));
        assertTrue("nor once a probe of the store has failed", repository.abandonableSnapshotDelete().isEmpty());
        assertTrue("nor on a later read", repository.abandonableSnapshotDelete().isEmpty());
    }

    public void testAnUnbudgetedDeleteStillAnswersSuccessAfterACleanupFailure() throws Exception {
        for (String entrypoint : List.of("narrow", "lock-file")) {
            final BlobStoreRepository repository = putRepository(
                "cleanup-failure-" + entrypoint,
                INJECTING_REPO_TYPE,
                OpenSearchIntegTestCase.randomRepoPath(node().settings())
            );
            final SnapshotId toDelete = twoRealSnapshotsReturningTheFirst("cleanup-failure-" + entrypoint);
            final long generation = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).getGenId();
            injectedShardBlobDeleteFailures.set(0);
            failShardBlobDeleteOnce.set(true);
            try {
                PlainActionFuture.<RepositoryData, Exception>get(f -> {
                    if (entrypoint.equals("narrow")) {
                        repository.deleteSnapshots(Collections.singleton(toDelete), generation, Version.CURRENT, f);
                    } else {
                        repository.deleteSnapshotsAndReleaseLockFiles(
                            Collections.singleton(toDelete),
                            generation,
                            Version.CURRENT,
                            null,
                            f
                        );
                    }
                });
            } finally {
                failShardBlobDeleteOnce.set(false);
            }
            assertEquals("[" + entrypoint + "] the cleanup failure must have been injected", 1, injectedShardBlobDeleteFailures.get());
            final RepositoryData after = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
            assertEquals("[" + entrypoint + "] must have committed the next generation", generation + 1, after.getGenId());
            assertFalse("[" + entrypoint + "] must have removed the snapshot", after.getSnapshotIds().contains(toDelete));
        }
    }

    /**
     * A budgeted delete whose shard-level cleanup fails after its generation committed still answers success with that
     * generation: the snapshot is gone from the repository data, the blobs of the refused batch are left in the repository,
     * and the snapshot that shares them still restores.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testABudgetedDeleteAnswersSuccessAfterACleanupFailure() throws Exception {
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final BlobStoreRepository repository = putRepository("cleanup-failure", INJECTING_REPO_TYPE, location);
        final SnapshotId toDelete = twoRealSnapshotsReturningTheFirst("cleanup-failure");
        final Repository.AbandonableSnapshotDelete entrypoint = proveDeclared(repository);
        final long generation = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).getGenId();
        final SnapshotDeletionAttempt attempt = new SnapshotDeletionAttempt();
        final PlainActionFuture<RepositoryData> future = PlainActionFuture.newFuture();
        injectedShardBlobDeleteFailures.set(0);
        failedShardBlobBatch.clear();
        failShardBlobDeleteOnce.set(true);
        conditionalWrites = true;
        final RepositoryData answered;
        try {
            entrypoint.deleteSnapshots(Collections.singleton(toDelete), generation, Version.CURRENT, attempt, future);
            answered = future.actionGet(TimeValue.timeValueSeconds(30));
        } finally {
            conditionalWrites = false;
            failShardBlobDeleteOnce.set(false);
        }
        assertEquals("the cleanup failure must have been injected", 1, injectedShardBlobDeleteFailures.get());
        assertEquals("the delete answers with the generation it committed", generation + 1, answered.getGenId());
        assertFalse(answered.getSnapshotIds().contains(toDelete));
        assertEquals(
            "the cleanup failure must be recorded on the attempt",
            "injected shard blob delete failure",
            attempt.cleanupFailure().getMessage()
        );
        assertTrue(
            "a blob of the refused batch must be left in the repository",
            failedShardBlobBatch.stream().anyMatch(name -> Files.exists(location.resolve(name)))
        );

        client().admin()
            .cluster()
            .prepareRestoreSnapshot("cleanup-failure", "second")
            .setRenamePattern("(.+)")
            .setRenameReplacement("restored-$1")
            .setWaitForCompletion(true)
            .get();
        client().admin().indices().prepareRefresh("restored-*").get();
        final Map<String, Map<String, Object>> restored = new HashMap<>();
        for (SearchHit hit : client().prepareSearch("restored-*").setSize(10).get().getHits()) {
            restored.put(hit.getId(), hit.getSourceAsMap());
        }
        final Map<String, Map<String, Object>> arranged = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            arranged.put(Integer.toString(i), Map.of("text", "sometext"));
        }
        assertEquals("the surviving snapshot must restore every document", arranged, restored);
    }

    /** A delayed budgeted delete cannot lower index.latest on a store that enforces conditional writes. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAConditionalIndexLatestIsNotLoweredByADelayedBudgetedDelete() throws Exception {
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final BlobStoreRepository repository = putRepository("pointer-not-lowered", INJECTING_REPO_TYPE, location);
        writeIndexGen(repository, withSnapshots("a", "b"), RepositoryData.EMPTY_REPO_GEN);
        final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final long generation = committed.getGenId();
        final Iterator<SnapshotId> ids = committed.getSnapshotIds().iterator();
        final SnapshotId first = ids.next();
        final SnapshotId second = ids.next();
        final Repository.AbandonableSnapshotDelete entrypoint = proveDeclared(repository);
        final CountDownLatch parked = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final PlainActionFuture<RepositoryData> w1 = PlainActionFuture.newFuture();
        conditionalWrites = true;
        parkNextIndexLatestWrite.set(new CountDownLatch[] { parked, release });
        try {
            try {
                entrypoint.deleteSnapshots(Collections.singleton(first), generation, Version.CURRENT, new SnapshotDeletionAttempt(), w1);
                assertTrue("the first deletion never reached its index.latest write", parked.await(30, TimeUnit.SECONDS));
                assertEquals(
                    "the first deletion must have committed before its index.latest write",
                    generation + 1,
                    OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).getGenId()
                );
                // The second deletion runs start to finish while the first is still parked.
                PlainActionFuture.<RepositoryData, Exception>get(
                    w2 -> entrypoint.deleteSnapshots(
                        Collections.singleton(second),
                        generation + 1,
                        Version.CURRENT,
                        new SnapshotDeletionAttempt(),
                        w2
                    )
                );
                assertFalse(
                    "the second deletion must have removed index-(N+1), so a lowered index.latest would name no blob",
                    Files.exists(location.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + (generation + 1)))
                );
            } finally {
                release.countDown();
                parkNextIndexLatestWrite.set(null);
            }
            w1.actionGet(TimeValue.timeValueSeconds(30));
        } finally {
            conditionalWrites = false;
            parkNextIndexLatestWrite.set(null);
        }
        assertEquals("index.latest must still name the newest generation", generation + 2, repository.readSnapshotIndexLatestBlob());
        assertTrue(Files.exists(location.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + (generation + 2))));
    }

    /**
     * When a budgeted delete cannot confirm its index.latest write, and a repository cleanup after it cannot either, neither
     * removes a root index-N blob index.latest may still name: the pointer keeps naming a blob that exists. Everything else
     * their cleanups remove still goes, including an index-N below the generation index.latest confirms. On a proven store
     * index.latest is written only conditionally, even when every conditional write fails, and the store stays proven, so the
     * entrypoint stays handed out.
     */
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
        // A superseded index-N blob below the generation index.latest confirms, and a root blob of the deleted snapshot, which
        // the deletion's own cleanup removes.
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

    /** A budgeted delete whose index.latest write landed removes the index-N blobs it supersedes. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAConfirmedIndexLatestLetsABudgetedDeleteRemoveSupersededIndexN() throws Exception {
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final BlobStoreRepository repository = putRepository("confirmed-pointer", INJECTING_REPO_TYPE, location);
        writeIndexGen(repository, withSnapshots("a"), RepositoryData.EMPTY_REPO_GEN);
        final Repository.AbandonableSnapshotDelete entrypoint = proveDeclared(repository);
        final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final long generation = committed.getGenId();
        final SnapshotId toDelete = committed.getSnapshotIds().iterator().next();
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
        assertEquals(generation + 1, repository.readSnapshotIndexLatestBlob());
        assertEquals(Set.of(BlobStoreRepository.INDEX_FILE_PREFIX + (generation + 1)), rootIndexN(location));
    }

    /**
     * On a proven store every generation writer updates index.latest after its own commit, with a conditional write, and never
     * with a plain one: here each of them runs to completion while a budgeted delete that committed first is parked at its own
     * conditional write, and index.latest ends up naming the later writer's generation.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testEveryGenerationWriterOnAProvenStoreUpdatesIndexLatestAfterItsCommit() throws Exception {
        for (String writer : List.of("generation-write", "narrow-delete", "cleanup")) {
            final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
            final BlobStoreRepository repository = putRepository("linearized-" + writer, INJECTING_REPO_TYPE, location);
            writeIndexGen(repository, withSnapshots("a", "b"), RepositoryData.EMPTY_REPO_GEN);
            final Repository.AbandonableSnapshotDelete entrypoint = proveDeclared(repository);
            final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
            final long generation = committed.getGenId();
            final Iterator<SnapshotId> ids = committed.getSnapshotIds().iterator();
            final SnapshotId first = ids.next();
            final SnapshotId second = ids.next();
            final CountDownLatch parked = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final PlainActionFuture<RepositoryData> budgeted = PlainActionFuture.newFuture();
            conditionalWrites = true;
            parkNextIndexLatestWrite.set(new CountDownLatch[] { parked, release });
            try {
                try {
                    entrypoint.deleteSnapshots(
                        Collections.singleton(first),
                        generation,
                        Version.CURRENT,
                        new SnapshotDeletionAttempt(),
                        budgeted
                    );
                    assertTrue("the budgeted delete never reached its index.latest write", parked.await(30, TimeUnit.SECONDS));
                    plainIndexLatestWrites.set(0);
                    conditionalIndexLatestWrites.set(0);
                    if (writer.equals("generation-write")) {
                        final RepositoryData current = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
                        writeIndexGen(repository, current, current.getGenId());
                    } else if (writer.equals("narrow-delete")) {
                        PlainActionFuture.<RepositoryData, Exception>get(
                            f -> repository.deleteSnapshots(Collections.singleton(second), generation + 1, Version.CURRENT, f)
                        );
                    } else {
                        Files.write(
                            location.resolve(BlobStoreRepository.SNAPSHOT_FORMAT.blobName(UUIDs.randomBase64UUID())),
                            new byte[] { 1 }
                        );
                        PlainActionFuture.<RepositoryCleanupResult, Exception>get(
                            f -> repository.cleanup(generation + 1, Version.CURRENT, null, null, f)
                        );
                    }
                    assertEquals(
                        "the [" + writer + "] writer must have committed its own generation",
                        generation + 2,
                        OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).getGenId()
                    );
                    assertEquals(
                        "the [" + writer + "] writer must not write index.latest with a plain write",
                        0,
                        plainIndexLatestWrites.get()
                    );
                    assertTrue("it writes index.latest with a conditional write", conditionalIndexLatestWrites.get() > 0);
                } finally {
                    release.countDown();
                    parkNextIndexLatestWrite.set(null);
                }
                budgeted.actionGet(TimeValue.timeValueSeconds(30));
            } finally {
                conditionalWrites = false;
                parkNextIndexLatestWrite.set(null);
            }
            assertEquals("index.latest must name the later writer's generation", generation + 2, repository.readSnapshotIndexLatestBlob());
            assertTrue(Files.exists(location.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + (generation + 2))));
        }
    }

    /**
     * On a proven store a generation writer that loses its commit writes no index.latest. It reserves a generation and parks
     * before its index-N write; a budgeted delete or a repository cleanup then commits the next generation; released, the
     * loser writes its orphan index-N and fails its commit, and index.latest still names the generation that committed.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAGenerationWriterThatLosesItsCommitOnAProvenStoreWritesNoIndexLatest() throws Exception {
        for (String partner : List.of("budgeted-delete", "cleanup")) {
            final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
            final BlobStoreRepository repository = putRepository("loser-" + partner, INJECTING_REPO_TYPE, location);
            writeIndexGen(repository, withSnapshots("a", "b"), RepositoryData.EMPTY_REPO_GEN);
            final Repository.AbandonableSnapshotDelete entrypoint = proveDeclared(repository);
            final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
            final long generation = committed.getGenId();
            final SnapshotId toDelete = committed.getSnapshotIds().iterator().next();
            final CountDownLatch parked = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final PlainActionFuture<RepositoryData> loser = PlainActionFuture.newFuture();
            conditionalWrites = true;
            parkNextIndexNWrite.set(new CountDownLatch[] { parked, release });
            try {
                try {
                    repository.writeIndexGen(committed, generation, Version.CURRENT, Function.identity(), Priority.NORMAL, loser);
                    assertTrue("the losing writer never reached its index-N write", parked.await(30, TimeUnit.SECONDS));
                    if (partner.equals("budgeted-delete")) {
                        PlainActionFuture.<RepositoryData, Exception>get(
                            f -> entrypoint.deleteSnapshots(
                                Collections.singleton(toDelete),
                                generation,
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
                            f -> repository.cleanup(generation, Version.CURRENT, null, null, f)
                        );
                    }
                    plainIndexLatestWrites.set(0);
                    conditionalIndexLatestWrites.set(0);
                } finally {
                    release.countDown();
                    parkNextIndexNWrite.set(null);
                }
                expectThrows(Exception.class, () -> loser.actionGet(TimeValue.timeValueSeconds(30)));
            } finally {
                conditionalWrites = false;
                parkNextIndexNWrite.set(null);
            }
            final long committedGeneration = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).getGenId();
            assertEquals(
                "index.latest must name the generation that committed beside the [" + partner + "]",
                committedGeneration,
                repository.readSnapshotIndexLatestBlob()
            );
            assertTrue(Files.exists(location.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + committedGeneration)));
            assertEquals(
                "the losing writer must write no index.latest",
                0,
                plainIndexLatestWrites.get() + conditionalIndexLatestWrites.get()
            );
        }
    }

    /**
     * A budgeted delete repairs an index.latest that names no existing index-N blob, or that is not a generation at all, even
     * when the value it holds is not below the delete's own generation.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnIndexLatestThatNamesNoBlobIsRepairedByABudgetedDelete() throws Exception {
        for (byte[] planted : List.of(Numbers.longToBytes(1000L), new byte[] { 1, 2, 3 })) {
            final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
            final BlobStoreRepository repository = putRepository("repair-" + planted.length, INJECTING_REPO_TYPE, location);
            writeIndexGen(repository, withSnapshots("a"), RepositoryData.EMPTY_REPO_GEN);
            final Repository.AbandonableSnapshotDelete entrypoint = proveDeclared(repository);
            final RepositoryData committed = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
            final long generation = committed.getGenId();
            final SnapshotId toDelete = committed.getSnapshotIds().iterator().next();
            Files.write(location.resolve(BlobStoreRepository.INDEX_LATEST_BLOB), planted);
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
                "index.latest must now name the committed generation",
                Long.BYTES + ":" + (generation + 1),
                Files.size(location.resolve(BlobStoreRepository.INDEX_LATEST_BLOB)) + ":" + repository.readSnapshotIndexLatestBlob()
            );
            assertTrue(Files.exists(location.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + (generation + 1))));
        }
    }

    /**
     * An expiry that arrives while a budgeted delete's generation commit is being applied waits for the commit and is then
     * answered with the committed repository data. The delete then leaves index.latest, the superseded index-N blob and the
     * deleted snapshot's root blob where they were.
     */
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
            PlainActionFuture.<RepositoryData, Exception>get(
                f -> entrypoint.deleteSnapshots(Collections.singleton(toDelete), generation, Version.CURRENT, attempt, f)
            );
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

    /**
     * A budgeted delete that removes the only snapshot of an index records the failure to remove that index's blobs on its
     * attempt, answers success, and leaves the index's container in the repository.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testABudgetedDeleteRecordsAFailedStaleIndexCleanup() throws Exception {
        final Path location = OpenSearchIntegTestCase.randomRepoPath(node().settings());
        final BlobStoreRepository repository = putRepository("stale-index-failure", INJECTING_REPO_TYPE, location);
        final String onlyInFirst = "idx-x-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        final String inSecond = "idx-y-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createIndex(onlyInFirst);
        createIndex(inSecond);
        ensureGreen();
        client().prepareIndex(onlyInFirst).setId("1").setSource("text", "sometext").get();
        client().prepareIndex(inSecond).setId("1").setSource("text", "sometext").get();
        client().admin().indices().prepareFlush(onlyInFirst, inSecond).get();
        final SnapshotId first = client().admin()
            .cluster()
            .prepareCreateSnapshot("stale-index-failure", "first")
            .setWaitForCompletion(true)
            .setIndices(onlyInFirst)
            .get()
            .getSnapshotInfo()
            .snapshotId();
        client().admin()
            .cluster()
            .prepareCreateSnapshot("stale-index-failure", "second")
            .setWaitForCompletion(true)
            .setIndices(inSecond)
            .get();
        final Repository.AbandonableSnapshotDelete entrypoint = proveDeclared(repository);
        final RepositoryData before = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final IndexId staleIndex = before.resolveIndexId(onlyInFirst);
        final SnapshotDeletionAttempt attempt = new SnapshotDeletionAttempt();
        failIndexContainerDeleteOnce.set(true);
        conditionalWrites = true;
        final RepositoryData answered;
        try {
            answered = PlainActionFuture.<RepositoryData, Exception>get(
                f -> entrypoint.deleteSnapshots(Collections.singleton(first), before.getGenId(), Version.CURRENT, attempt, f)
            );
        } finally {
            conditionalWrites = false;
        }
        final boolean injected = failIndexContainerDeleteOnce.getAndSet(false) == false;
        assertTrue("the index container delete failure must have been injected", injected);
        assertFalse("the delete answers success without the snapshot", answered.getSnapshotIds().contains(first));
        assertNotNull("the failed stale index cleanup must be recorded on the attempt", attempt.cleanupFailure());
        assertTrue(
            "and the index's container is left in the repository",
            Files.exists(location.resolve(BlobStoreRepository.INDICES_DIR).resolve(staleIndex.getId()))
        );
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

    /** A repository of the injecting type, which declares the delete entrypoint for its own path. */
    private BlobStoreRepository declaringRepository(String name) {
        return putRepository(name, INJECTING_REPO_TYPE, OpenSearchIntegTestCase.randomRepoPath(node().settings()));
    }

    /**
     * Has a declaring repository's first capability read start its store probe, with the injecting store enforcing
     * conditional writes, waits for the probe, and returns the entrypoint the repository then hands out. The repository needs
     * a committed generation first: until it has one it is best-effort, and a best-effort repository is never probed.
     */
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

    /** Two real snapshots of one real index, so that deleting the first always leaves shard-level blobs to remove. */
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

    /**
     * Runs {@code write} and returns what index.latest held when the cluster state recording the generation after {@code from}
     * was applied on this node. Listeners run before the update's completion callback, which is where a write schedules
     * anything it does after the commit, so this reads the pointer strictly after step 2 and strictly before that.
     */
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
        // Fresh per invocation: the identifiers below key into a static set, so a fixed UUID makes repeated
        // runs in the same JVM (-Dtests.iters) collide and skip scenario 1's task.
        String testIndexUUID = UUIDs.randomBase64UUID();
        ShardId shardId = new ShardId(new Index(indexName, testIndexUUID), 0);

        try {
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
        } finally {
            // ongoingRemoteDirectoryCleanups is static, and production only removes the identifier it added
            // itself, so the adds above would otherwise stay in the set for the life of the JVM. Keyed on the
            // per-invocation UUID, this removes exactly the identifiers this test added.
            RemoteStoreShardCleanupTask.ongoingRemoteDirectoryCleanups.removeIf(id -> id.startsWith(testIndexUUID + "/"));
        }
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

    /**
     * A stale index whose cleanup had already started when the caller gave up is still finished, but no index left on the
     * queue behind it is touched. Every index must still be answered for, abandoned or not, or the deletion that is
     * waiting on the whole drain would never be told it is over.
     */
    public void testAbandonmentStopsTheStaleIndexDrain() throws Exception {
        // A repository of this test's own rather than the node's, so that its deletion pool can have a single thread. The entry
        // point starts a worker per stale index up to that pool's maximum, so with one thread the first index is in flight while
        // the other two are still queued; the node's pool is never smaller than three, and would claim all three at once.
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
        // Typed as the base class: the entry point is package-private there, and FsRepository is in another package.
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

            // Holds the first index's cleanup open until the attempt has been abandoned, so that the abandonment lands while
            // that cleanup is in flight rather than before it or after the drain is over.
            final BlobContainer inFlight = mock(BlobContainer.class);
            when(inFlight.delete()).thenAnswer(invocation -> {
                enteredLatch.countDown();
                releaseLatch.await();
                return new DeleteResult(1, 100L);
            });
            final BlobContainer queuedBehind1 = mock(BlobContainer.class);
            final BlobContainer queuedBehind2 = mock(BlobContainer.class);

            // Insertion-ordered, because the queue is filled in this map's order and the in-flight index has to be taken first.
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
            // The entry point answers only once every stale index has, so this also shows that the abandoned indices were
            // answered for rather than dropped, and that the drain kept going instead of stalling on the first abandoned index.
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
