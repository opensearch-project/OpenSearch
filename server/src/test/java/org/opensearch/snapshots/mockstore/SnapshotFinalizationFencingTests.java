/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots.mockstore;

import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.repositories.SnapshotFinalizationAttempt;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.snapshots.SnapshotException;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.when;

public class SnapshotFinalizationFencingTests extends OpenSearchTestCase {

    private final RecoverySettings recoverySettings = new RecoverySettings(
        Settings.EMPTY,
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAbandonedFinalizeIsRefusedAndRecordsNothingBeforeItStartsAndAtTheLastCheckBeforeTheGenerationWrite() throws Exception {
        try (BlobStoreRepository repository = createRepository()) {
            final SnapshotId snapshotId = new SnapshotId("foo", UUIDs.randomBase64UUID());
            final long generationBefore = PlainActionFuture.<RepositoryData, Exception>get(repository::getRepositoryData).getGenId();

            final Set<String> rootBlobsBefore = rootBlobs(repository);

            final SnapshotFinalizationAttempt attempt = new SnapshotFinalizationAttempt();
            assertTrue(attempt.abandon());
            final SnapshotException thrown = expectThrows(SnapshotException.class, () -> finalizeWith(repository, snapshotId, attempt));

            assertThat(thrown.getMessage(), containsString(snapshotId.getName()));
            assertThat(thrown.getCause(), instanceOf(SnapshotException.class));
            assertThat(thrown.getCause().getMessage(), containsString("abandoned before finalization completed"));

            final Set<String> rootBlobsAfter = rootBlobs(repository);
            assertThat(rootBlobsAfter, equalTo(rootBlobsBefore));
            assertThat(rootBlobsAfter, not(hasItem("snap-" + snapshotId.getUUID() + ".dat")));

            final RepositoryData afterwards = PlainActionFuture.<RepositoryData, Exception>get(repository::getRepositoryData);
            assertThat(afterwards.getSnapshotIds(), empty());
            assertThat(afterwards.getGenId(), equalTo(generationBefore));
            assertGenerations(generationBefore);
        }

        try (MockRepository repository = createParkingRepository()) {
            finalizeThroughEntrypoint(repository, new SnapshotId("baseline", UUIDs.randomBase64UUID()), new SnapshotFinalizationAttempt())
                .actionGet(TimeValue.timeValueSeconds(30L));
            final long generationBefore = PlainActionFuture.<RepositoryData, Exception>get(repository::getRepositoryData).getGenId();
            final SnapshotId snapshotId = new SnapshotId("foo", UUIDs.randomBase64UUID());
            final SnapshotFinalizationAttempt attempt = new SnapshotFinalizationAttempt();
            final IndexId indexId = new IndexId("hashed-index", UUIDs.randomBase64UUID(), PathType.HASHED_PREFIX.getCode());
            final IndexMetadata indexMetadata = IndexMetadata.builder(indexId.getName())
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, indexId.getId())
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            abandonAtShardPathWrite.set(attempt);

            final PlainActionFuture<RepositoryData> future = finalizeThroughEntrypoint(
                repository,
                snapshotId,
                attempt,
                ShardGenerations.builder().put(indexId, 0, UUIDs.randomBase64UUID()).build(),
                Metadata.builder().put(indexMetadata, false).build()
            );

            final SnapshotException thrown = expectThrows(SnapshotException.class, () -> future.actionGet(TimeValue.timeValueSeconds(30L)));
            assertTrue("the caller must still be able to give up while the shard paths are written", abandonedAtShardPathWrite.get());
            assertThat(thrown.getCause(), instanceOf(SnapshotException.class));
            assertThat(thrown.getCause().getMessage(), containsString("abandoned before finalization completed"));
            assertThat(rootFiles(), not(hasItem(BlobStoreRepository.INDEX_FILE_PREFIX + (generationBefore + 1))));
            assertThat(latestPointer(), equalTo(generationBefore));
            final RepositoryData afterwards = PlainActionFuture.<RepositoryData, Exception>get(repository::getRepositoryData);
            assertThat(afterwards.getGenId(), equalTo(generationBefore));
            assertFalse(afterwards.getSnapshotIds().contains(snapshotId));
            assertGenerations(generationBefore);
        }
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testFinalizationInsideTheGenerationWriteCannotBeAbandoned() throws Exception {
        try (MockRepository repository = createParkingRepository()) {
            finalizeThroughEntrypoint(repository, new SnapshotId("baseline", UUIDs.randomBase64UUID()), new SnapshotFinalizationAttempt())
                .actionGet(TimeValue.timeValueSeconds(30L));
            final long generationBefore = PlainActionFuture.<RepositoryData, Exception>get(repository::getRepositoryData).getGenId();
            final SnapshotId snapshotId = new SnapshotId("foo", UUIDs.randomBase64UUID());
            final SnapshotFinalizationAttempt attempt = new SnapshotFinalizationAttempt();
            repository.setBlockOnceOnRootWrite(BlobStoreRepository.INDEX_FILE_PREFIX + (generationBefore + 1));

            final PlainActionFuture<RepositoryData> future = finalizeThroughEntrypoint(repository, snapshotId, attempt);
            assertBusy(() -> assertTrue("the root generation write never parked", repository.blocked()));
            assertFalse("a call inside the generation write must not be given up on", attempt.abandon());
            repository.unblock();

            final RepositoryData committed = future.actionGet(TimeValue.timeValueSeconds(30L));
            assertThat(committed.getGenId(), equalTo(generationBefore + 1));
            assertTrue(committed.getSnapshotIds().contains(snapshotId));
            assertGenerations(generationBefore + 1);
        }
    }

    private MockEventuallyConsistentRepository.Context context;

    private ClusterService clusterService;

    private BlobStoreRepository createRepository() throws Exception {
        final RepositoryMetadata metadata = new RepositoryMetadata(
            "testRepo",
            "mockEventuallyConsistent",
            Settings.builder().put(BlobStoreRepository.SUPPORT_URL_REPO.getKey(), false).build(),
            RepositoryData.EMPTY_REPO_GEN,
            RepositoryData.EMPTY_REPO_GEN
        );
        clusterService = BlobStoreTestUtil.mockClusterService(metadata);
        context = new MockEventuallyConsistentRepository.Context();
        final BlobStoreRepository repository = new MockEventuallyConsistentRepository(
            metadata,
            xContentRegistry(),
            clusterService,
            recoverySettings,
            context,
            random()
        ) {
            @Override
            public Optional<AbandonableSnapshotFinalization> abandonableSnapshotFinalization() {
                return blobStoreAbandonableSnapshotFinalization();
            }
        };
        clusterService.addStateApplier(event -> repository.updateState(event.state()));
        repository.updateState(clusterService.state());
        repository.start();
        setConditionalWriteProof(repository, "PROVEN");
        return repository;
    }

    @SuppressForbidden(reason = "the proof state is private to BlobStoreRepository and must not gain a test seam")
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void setConditionalWriteProof(BlobStoreRepository repository, String state) throws Exception {
        final Field field = BlobStoreRepository.class.getDeclaredField("conditionalWriteProof");
        field.setAccessible(true);
        final AtomicReference reference = (AtomicReference) field.get(repository);
        reference.set(Enum.valueOf((Class<? extends Enum>) reference.get().getClass().asSubclass(Enum.class), state));
    }

    private Set<String> rootBlobs(BlobStoreRepository repository) throws IOException {
        context.forceConsistent();
        return new HashSet<>(repository.blobStore().blobContainer(repository.basePath()).listBlobs().keySet());
    }

    private TestThreadPool threadPool;

    private final AtomicReference<SnapshotFinalizationAttempt> abandonAtShardPathWrite = new AtomicReference<>();

    private final AtomicBoolean abandonedAtShardPathWrite = new AtomicBoolean();

    private Path repositoryLocation;

    @After
    public void stopThreadPool() {
        if (threadPool != null) {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private MockRepository createParkingRepository() throws Exception {
        threadPool = new TestThreadPool(getTestName());
        final Path home = createTempDir();
        repositoryLocation = home.resolve("repos").resolve("repo");
        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), home.toString())
                .put(Environment.PATH_REPO_SETTING.getKey(), home.resolve("repos").toString())
                .build()
        );
        final RepositoryMetadata metadata = new RepositoryMetadata(
            "testRepo",
            "mock",
            Settings.builder().put("location", repositoryLocation.toString()).put("conditional_writes", true).build(),
            RepositoryData.EMPTY_REPO_GEN,
            RepositoryData.EMPTY_REPO_GEN
        );
        clusterService = BlobStoreTestUtil.mockClusterService(metadata);
        when(clusterService.getClusterApplierService().threadPool()).thenReturn(threadPool);
        final MockRepository repository = new MockRepository(metadata, environment, xContentRegistry(), clusterService, recoverySettings) {
            @Override
            protected BlobContainer snapshotShardPathBlobContainer() {
                return new FilterBlobContainer(super.snapshotShardPathBlobContainer()) {
                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return child;
                    }

                    @Override
                    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                        throws IOException {
                        final SnapshotFinalizationAttempt toAbandon = abandonAtShardPathWrite.getAndSet(null);
                        if (toAbandon != null) {
                            abandonedAtShardPathWrite.set(toAbandon.abandon());
                        }
                        super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
                    }
                };
            }
        };
        clusterService.addStateApplier(event -> repository.updateState(event.state()));
        repository.updateState(clusterService.state());
        repository.start();
        setConditionalWriteProof(repository, "PROVEN");
        return repository;
    }

    private static PlainActionFuture<RepositoryData> finalizeThroughEntrypoint(
        BlobStoreRepository repository,
        SnapshotId snapshotId,
        SnapshotFinalizationAttempt attempt
    ) throws Exception {
        return finalizeThroughEntrypoint(repository, snapshotId, attempt, ShardGenerations.EMPTY, Metadata.EMPTY_METADATA);
    }

    private static PlainActionFuture<RepositoryData> finalizeThroughEntrypoint(
        BlobStoreRepository repository,
        SnapshotId snapshotId,
        SnapshotFinalizationAttempt attempt,
        ShardGenerations shardGenerations,
        Metadata clusterMetadata
    ) throws Exception {
        final Optional<Repository.AbandonableSnapshotFinalization> entrypoint = repository.abandonableSnapshotFinalization();
        assertTrue("the declaring repository must hand out the entrypoint", entrypoint.isPresent());
        final PlainActionFuture<RepositoryData> future = PlainActionFuture.newFuture();
        entrypoint.get()
            .finalizeSnapshot(
                shardGenerations,
                PlainActionFuture.<RepositoryData, Exception>get(repository::getRepositoryData).getGenId(),
                clusterMetadata,
                snapshotInfo(snapshotId),
                Version.CURRENT,
                Function.identity(),
                Priority.NORMAL,
                attempt,
                future
            );
        return future;
    }

    private Set<String> rootFiles() throws IOException {
        try (Stream<Path> files = Files.list(repositoryLocation)) {
            return files.map(file -> file.getFileName().toString()).collect(Collectors.toSet());
        }
    }

    private void assertGenerations(long expected) {
        final RepositoryMetadata metadata = clusterService.state()
            .metadata()
            .<RepositoriesMetadata>custom(RepositoriesMetadata.TYPE)
            .repository("testRepo");
        assertThat(metadata.generation(), equalTo(expected));
        assertThat(metadata.pendingGeneration(), equalTo(expected));
    }

    private long latestPointer() throws IOException {
        return ByteBuffer.wrap(Files.readAllBytes(repositoryLocation.resolve(BlobStoreRepository.INDEX_LATEST_BLOB))).getLong();
    }

    private RepositoryData finalizeWith(BlobStoreRepository repository, SnapshotId snapshotId, SnapshotFinalizationAttempt attempt)
        throws Exception {
        final Optional<Repository.AbandonableSnapshotFinalization> entrypoint = repository.abandonableSnapshotFinalization();
        assertTrue("the declaring repository must hand out the entrypoint", entrypoint.isPresent());
        return PlainActionFuture.<RepositoryData, Exception>get(
            f -> entrypoint.get()
                .finalizeSnapshot(
                    ShardGenerations.EMPTY,
                    RepositoryData.EMPTY_REPO_GEN,
                    Metadata.EMPTY_METADATA,
                    snapshotInfo(snapshotId),
                    Version.CURRENT,
                    Function.identity(),
                    Priority.NORMAL,
                    attempt,
                    f
                )
        );
    }

    private static SnapshotInfo snapshotInfo(SnapshotId snapshotId) {
        return new SnapshotInfo(
            snapshotId,
            Collections.emptyList(),
            Collections.emptyList(),
            0L,
            null,
            1L,
            5,
            Collections.emptyList(),
            true,
            Collections.emptyMap(),
            false,
            0
        );
    }
}
