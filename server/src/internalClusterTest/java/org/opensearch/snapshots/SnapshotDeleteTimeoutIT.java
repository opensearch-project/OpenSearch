/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.util.BytesRefUtils;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.search.SearchHit;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;
import org.junit.After;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotDeleteTimeoutIT extends AbstractSnapshotIntegTestCase {

    private static final String REPO = "test-repo";
    private static final String INDEX = "test-idx";
    private static final String IO_TIMEOUT_KEY = "snapshot.repository.io_timeout";

    private static final TimeValue BUDGET = TimeValue.timeValueSeconds(5L);

    private static final TimeValue POST_COMMIT_BUDGET = TimeValue.timeValueSeconds(20L);

    private static final TimeValue DEFAULT_BUDGET = TimeValue.timeValueMinutes(30L);

    private static final TimeValue PATIENCE = TimeValue.timeValueSeconds(60L);

    private static final Settings ONE_SNAPSHOT_THREAD = Settings.builder()
        .put("thread_pool.snapshot.core", 1)
        .put("thread_pool.snapshot.max", 1)
        .build();

    private static final String FIELD = "foo";

    private static final int ARRANGED_DOCS = 5;

    private static final int READ_BACK_SIZE = ARRANGED_DOCS + 10;

    private static final String SNAPSHOT_DELETED = "snap-deleted";
    private static final String SNAPSHOT_RELEASED = "snap-released";
    private static final String RESTORED_INDEX = "restored-idx";

    private Path repoPath;

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING.getKey(), true).build();
    }

    public void testBudgetDrainsTheQueueWhileTheWorkerIsStillParked() throws Exception {
        final String clusterManager = startClusterAndRepository();
        createFullSnapshot(REPO, "snap-parked");
        createFullSnapshot(REPO, "snap-queued");
        createFullSnapshot(REPO, "snap-retained");
        final long generationBefore = repositoryMetadata().generation();

        setIoTimeout(BUDGET);
        blockClusterManagerOnWriteIndexFile(REPO);
        final ActionFuture<AcknowledgedResponse> parked = startDeleteSnapshot(REPO, "snap-parked");
        waitForBlock(clusterManager, REPO, PATIENCE);
        setIoTimeout(DEFAULT_BUDGET);

        assertEquals(generationBefore + 1, repositoryMetadata().pendingGeneration());
        assertEquals("the parked writer must not have committed a generation", generationBefore, repositoryMetadata().generation());
        final String parkedUuid = startedDeletionUuid();

        final ActionFuture<AcknowledgedResponse> queued = startDeleteSnapshot(REPO, "snap-queued");
        final AtomicBoolean queuedBehindTheParkedDelete = new AtomicBoolean();
        awaitClusterState(state -> {
            final List<SnapshotDeletionsInProgress.Entry> entries = state.custom(
                SnapshotDeletionsInProgress.TYPE,
                SnapshotDeletionsInProgress.EMPTY
            ).getEntries();
            if (entries.size() == 2) {
                queuedBehindTheParkedDelete.set(true);
            }
            return queuedBehindTheParkedDelete.get() || entries.stream().noneMatch(entry -> entry.uuid().equals(parkedUuid));
        });
        assertTrue(
            "the queued delete must publish before the parked delete's " + BUDGET + " budget expires",
            queuedBehindTheParkedDelete.get()
        );

        awaitBudgetExpiry(parked, 1);

        assertBusy(() -> {
            final List<SnapshotDeletionsInProgress.Entry> entries = deletionEntries();
            assertThat("the abandoned entry must have left the cluster state", entries, hasSize(1));
            assertNotEquals("the entry that left must be the parked one", parkedUuid, entries.get(0).uuid());
            assertEquals("the queued entry must have been promoted", SnapshotDeletionsInProgress.State.STARTED, entries.get(0).state());
        });

        assertBusy(() -> assertEquals(generationBefore + 2, repositoryMetadata().pendingGeneration()));
        assertEquals("nothing may have committed yet", generationBefore, repositoryMetadata().generation());

        unblockNode(REPO, clusterManager);
        assertAcked(queued.actionGet(PATIENCE));
        awaitNoMoreRunningOperations();

        final RepositoryMetadata atRest = repositoryMetadata();
        assertEquals("the later of the two writers must be the one that committed", generationBefore + 2, atRest.generation());
        assertEquals("the loser must not have left an unfinished write behind", atRest.generation(), atRest.pendingGeneration());

        assertEquals("index.latest must name the generation that committed", generationBefore + 2, indexLatest());

        assertThat(getRepositoryData(REPO).getSnapshotIds(), hasSize(2));

        assertRepositoryStillUsable("snap-after-drain");
    }

    public void testLatePointerWriteCannotOutrunClusterManagerFailover() throws Exception {
        final String oldClusterManager = startClusterAndRepository();
        final String successor = internalCluster().startClusterManagerOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        final MockRepository successorRepository = (MockRepository) internalCluster().getInstance(RepositoriesService.class, successor)
            .repository(REPO);
        successorRepository.abandonableSnapshotDelete();
        assertTrue("the successor's store probe did not complete", successorRepository.awaitConditionalWriteProbe(PATIENCE));
        assertTrue("the successor must hand out the delete entrypoint", successorRepository.abandonableSnapshotDelete().isPresent());
        final String successorId = internalCluster().getInstance(ClusterService.class, successor).localNode().getId();
        final Map<String, Object> arranged = arrangeVerifiableContents();
        createFullSnapshot(REPO, SNAPSHOT_DELETED);
        createFullSnapshot(REPO, "snap-retained");
        final long generationBefore = repositoryMetadata().generation();
        final String dataNode = internalCluster().getDataNodeNames().iterator().next();
        final ThreadContext callerContext = internalCluster().getInstance(ThreadPool.class, dataNode).getThreadContext();
        final AtomicReference<List<String>> warnings = new AtomicReference<>();
        final PlainActionFuture<AcknowledgedResponse> parked = PlainActionFuture.newFuture();

        blockOnceOnTheNextIndexLatestWrite(oldClusterManager);
        try {
            setIoTimeout(POST_COMMIT_BUDGET);
            internalCluster().client(dataNode)
                .admin()
                .cluster()
                .prepareDeleteSnapshot(REPO, SNAPSHOT_DELETED)
                .execute(
                    ActionListener.runBefore(
                        parked,
                        () -> warnings.set(callerContext.getResponseHeaders().getOrDefault("Warning", List.of()))
                    )
                );
            waitForBlock(oldClusterManager, REPO, PATIENCE);
            setIoTimeout(DEFAULT_BUDGET);
            assertEquals(
                "the parked worker must already have committed its generation",
                generationBefore + 1,
                repositoryMetadata().generation()
            );
            assertEquals("and not yet have moved index.latest", generationBefore, indexLatest());
            awaitAckedWhileParked(parked, oldClusterManager);
            assertThat(
                "the caller must be told that files the deleted snapshot no longer uses may remain",
                warnings.get(),
                hasItem(
                    containsString(
                        "were deleted from repository [" + REPO + "], but removal of the files they no longer use did not finish"
                    )
                )
            );
            assertEquals(
                "the old cluster manager must record its parked call as past its budget",
                Set.of(REPO),
                internalCluster().getInstance(RepositoriesService.class, oldClusterManager).repositoriesWithCallsPastBudget()
            );

            client().execute(AddVotingConfigExclusionsAction.INSTANCE, new AddVotingConfigExclusionsRequest(oldClusterManager)).get();
            awaitClusterState(oldClusterManager, state -> successorId.equals(state.nodes().getClusterManagerNodeId()));
            assertEquals(successor, internalCluster().getClusterManagerName());
            assertTrue(
                "the old cluster manager must still be in the cluster",
                clusterAdmin().prepareState()
                    .get()
                    .getState()
                    .nodes()
                    .getNodes()
                    .values()
                    .stream()
                    .anyMatch(node -> node.getName().equals(oldClusterManager))
            );
            assertTrue(
                "its pointer write must still be parked",
                ((MockRepository) internalCluster().getInstance(RepositoriesService.class, oldClusterManager).repository(REPO)).blocked()
            );

            createFullSnapshot(REPO, "snap-successor");
            assertEquals("the successor must have committed the next generation", generationBefore + 2, repositoryMetadata().generation());
            assertEquals("and moved index.latest to it", generationBefore + 2, indexLatest());
            assertFalse(
                "and removed the blob the parked write names",
                Files.exists(repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + (generationBefore + 1)))
            );
        } finally {
            unblockNode(REPO, oldClusterManager);
        }
        assertBusy(
            () -> assertTrue(
                "the old cluster manager's parked call has not returned",
                internalCluster().getInstance(RepositoriesService.class, oldClusterManager).repositoriesWithCallsPastBudget().isEmpty()
            ),
            PATIENCE.seconds(),
            TimeUnit.SECONDS
        );

        assertEquals("the resumed write must not move index.latest back", generationBefore + 2, indexLatest());
        assertIndexLatestNamesTheCommittedGeneration();
        assertEquals(Set.of(BlobStoreRepository.INDEX_FILE_PREFIX + (generationBefore + 2)), rootIndexNBlobs());
        final RepositoryMetadata atRest = repositoryMetadata();
        assertEquals(generationBefore + 2, atRest.generation());
        assertEquals(atRest.generation(), atRest.pendingGeneration());
        assertEquals(Set.of("snap-retained", "snap-successor"), names(getRepositoryData(REPO).getSnapshotIds()));
        assertRestoresTo(REPO, "snap-retained", arranged);
        assertRestoresTo(REPO, "snap-successor", arranged);
    }

    private void blockOnceOnTheNextIndexLatestWrite(String clusterManager) {
        final MockRepository repository = (MockRepository) internalCluster().getInstance(RepositoriesService.class, clusterManager)
            .repository(REPO);
        repository.setBlockOnceOnConditionalRootWrite(BlobStoreRepository.INDEX_LATEST_BLOB);
        repository.setBlockOnceOnRootWrite(BlobStoreRepository.INDEX_LATEST_BLOB);
    }

    private void assertIndexLatestNamesTheCommittedGeneration() throws Exception {
        final long committed = repositoryMetadata().generation();
        final long pointer = indexLatest();
        final Set<String> rootGenerations = rootIndexNBlobs();
        assertTrue(
            "index.latest names generation ["
                + pointer
                + "] but the root holds only "
                + rootGenerations
                + ", committed ["
                + committed
                + "]",
            rootGenerations.contains(BlobStoreRepository.INDEX_FILE_PREFIX + pointer)
        );
        assertEquals("index.latest must name the committed generation", committed, pointer);
    }

    public void testASnapshotReleasedByAGivenUpDeleteCanBeRestoredWhenTheWorkerResumesAtOnce() throws Exception {
        final String clusterManager = startClusterAndRepository();
        final Map<String, Object> arranged = arrangeVerifiableContents();
        createFullSnapshot(REPO, SNAPSHOT_DELETED);
        final Path deletedSnapshotBlob = rootSnapshotBlob(SNAPSHOT_DELETED);
        final ActionFuture<AcknowledgedResponse> abandoned = parkTheDeleteOfTheOnlySnapshot(clusterManager);
        final ActionFuture<CreateSnapshotResponse> released = queueASnapshotOfTheUnchangedIndex();

        awaitAckedWhileParked(abandoned, clusterManager);
        assertTrue("the deleted snapshot's root blob must still be there while the worker is parked", Files.exists(deletedSnapshotBlob));
        unblockNode(REPO, clusterManager);
        awaitEveryWorkerFinished();

        assertReleasedSnapshotRestoresTo(released, arranged);
    }

    public void testASnapshotReleasedByAGivenUpDeleteStartsWhileTheOnlySnapshotThreadIsHeld() throws Exception {
        final String clusterManager = startClusterAndRepository(ONE_SNAPSHOT_THREAD);
        final Map<String, Object> arranged = arrangeVerifiableContents();
        createFullSnapshot(REPO, SNAPSHOT_DELETED);
        final Path deletedSnapshotBlob = rootSnapshotBlob(SNAPSHOT_DELETED);
        final ActionFuture<AcknowledgedResponse> abandoned = parkTheDeleteOfTheOnlySnapshot(clusterManager);
        boolean snapshotPoolSeen = false;
        for (ThreadPoolStats.Stats stat : internalCluster().getInstance(ThreadPool.class, clusterManager).stats()) {
            if (ThreadPool.Names.SNAPSHOT.equals(stat.getName())) {
                snapshotPoolSeen = true;
                assertEquals("the parked worker must be holding the only snapshot thread", 1, stat.getActive());
            }
        }
        assertTrue("the cluster manager must report a snapshot pool", snapshotPoolSeen);
        final ActionFuture<CreateSnapshotResponse> released = queueASnapshotOfTheUnchangedIndex();

        awaitAckedWhileParked(abandoned, clusterManager);
        try {
            awaitShardBlobsWritten(SNAPSHOT_RELEASED);
        } catch (TimeoutException e) {
            throw new AssertionError("the released snapshot must start while the only snapshot thread is held", e);
        }
        assertTrue("the deleted snapshot's root blob must still be there while the worker is parked", Files.exists(deletedSnapshotBlob));
        unblockNode(REPO, clusterManager);
        awaitEveryWorkerFinished();
        assertTrue("the given-up worker must leave the deleted snapshot's root blob for a cleanup", Files.exists(deletedSnapshotBlob));

        assertReleasedSnapshotRestoresTo(released, arranged);
    }

    private Map<String, Object> arrangeVerifiableContents() {
        flushAndRefresh(INDEX);
        final Map<String, Object> expected = new HashMap<>(readContents(INDEX));
        assertThat("the shared arrange must have left exactly its own document behind", expected.keySet(), hasSize(1));
        for (int i = 0; i < ARRANGED_DOCS; i++) {
            final String id = "doc-" + i;
            final String value = "value-" + i;
            index(INDEX, "_doc", id, FIELD, value);
            expected.put(id, value);
        }
        flushAndRefresh(INDEX);
        assertEquals("the index must hold exactly the arranged documents before it is snapshotted", expected, readContents(INDEX));
        return expected;
    }

    private Map<String, Object> readContents(String index) {
        final SearchResponse response = client().prepareSearch(index).setSize(READ_BACK_SIZE).get();
        final Map<String, Object> contents = new HashMap<>();
        for (SearchHit hit : response.getHits().getHits()) {
            contents.put(hit.getId(), hit.getSourceAsMap().get(FIELD));
        }
        assertEquals(
            "every counted hit must have been returned, or this map is short for a reason that is not data loss",
            response.getHits().getTotalHits().value(),
            (long) contents.size()
        );
        return contents;
    }

    private ActionFuture<AcknowledgedResponse> parkTheDeleteOfTheOnlySnapshot(String clusterManager) throws Exception {
        final long generationBefore = repositoryMetadata().generation();

        setIoTimeout(POST_COMMIT_BUDGET);
        blockClusterManagerFromDeletingIndexNFile(REPO);
        final ActionFuture<AcknowledgedResponse> parked = startDeleteSnapshot(REPO, SNAPSHOT_DELETED);
        waitForBlock(clusterManager, REPO, PATIENCE);
        setIoTimeout(DEFAULT_BUDGET);

        assertEquals(
            "the block point must be past the generation commit, or the index prefix is not yet a deletion candidate",
            generationBefore + 1,
            repositoryMetadata().generation()
        );
        return parked;
    }

    private ActionFuture<CreateSnapshotResponse> queueASnapshotOfTheUnchangedIndex() throws Exception {
        final ActionFuture<CreateSnapshotResponse> released = startFullSnapshot(REPO, SNAPSHOT_RELEASED);
        awaitQueuedShard(SNAPSHOT_RELEASED);
        return released;
    }

    private void awaitQueuedShard(String snapshotName) throws Exception {
        awaitClusterState(
            state -> shardsOf(state, snapshotName).stream()
                .anyMatch(status -> status.equals(SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED))
        );
    }

    private void awaitShardBlobsWritten(String snapshotName) throws Exception {
        awaitClusterState(state -> {
            final List<SnapshotsInProgress.ShardSnapshotStatus> shards = shardsOf(state, snapshotName);
            return shards.isEmpty() == false && shards.stream().allMatch(status -> status.state().completed());
        });
    }

    private List<SnapshotsInProgress.ShardSnapshotStatus> shardsOf(ClusterState state, String snapshotName) {
        for (SnapshotsInProgress.Entry entry : state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries()) {
            if (entry.snapshot().getSnapshotId().getName().equals(snapshotName)) {
                return List.copyOf(entry.shards().values());
            }
        }
        return List.of();
    }

    private void awaitEveryWorkerFinished() throws Exception {
        awaitNoMoreRunningOperations();
        awaitGivenUpCallsReturned();
    }

    @After
    public void awaitGivenUpCallsReturned() throws Exception {
        assertBusy(
            () -> assertTrue(
                "a repository call given up on by its budget has not returned",
                internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class)
                    .repositoriesWithCallsPastBudget()
                    .isEmpty()
            )
        );
    }

    private void assertReleasedSnapshotRestoresTo(ActionFuture<CreateSnapshotResponse> released, Map<String, Object> arranged)
        throws Exception {
        assertSuccessful(released);

        final RestoreInfo restored = clusterAdmin().prepareRestoreSnapshot(REPO, SNAPSHOT_RELEASED)
            .setIndices(INDEX)
            .setRenamePattern(INDEX)
            .setRenameReplacement(RESTORED_INDEX)
            .setWaitForCompletion(true)
            .get()
            .getRestoreInfo();
        assertNotNull("the restore must have produced a result", restored);
        assertEquals("no shard of the released snapshot may have failed to restore", 0, restored.failedShards());
        assertEquals("every shard of the released snapshot must have restored", restored.totalShards(), restored.successfulShards());
        ensureGreen(RESTORED_INDEX);

        assertEquals("the restored index must hold exactly the documents that were arranged", arranged, readContents(RESTORED_INDEX));

        final IndexId releasedIndexId = getRepositoryData(REPO).resolveIndexId(INDEX);
        assertTrue(
            "the blob prefix holding the released snapshot's shard data must still exist",
            Files.exists(repoPath.resolve(BlobStoreRepository.INDICES_DIR).resolve(releasedIndexId.getId()))
        );

        final List<SnapshotInfo> listed = clusterAdmin().prepareGetSnapshots(REPO).get().getSnapshots();
        assertThat(listed, hasSize(1));
        assertEquals(SNAPSHOT_RELEASED, listed.get(0).snapshotId().getName());
        assertThat(listed.get(0).state(), is(SnapshotState.SUCCESS));
        assertRepositoryStillUsable("snap-after-release");
    }

    private String startClusterAndRepository() throws Exception {
        return startClusterAndRepository(LARGE_SNAPSHOT_POOL_SETTINGS);
    }

    private String startClusterAndRepository(Settings clusterManagerSettings) throws Exception {
        return startClusterAndRepository(clusterManagerSettings, Settings.EMPTY);
    }

    private String startClusterAndRepository(Settings clusterManagerSettings, Settings repositorySettings) throws Exception {
        internalCluster().startClusterManagerOnlyNode(clusterManagerSettings);
        internalCluster().startDataOnlyNode();
        repoPath = randomRepoPath();
        createRepository(
            REPO,
            "mock",
            Settings.builder().put("location", repoPath).put("conditional_writes", true).put(repositorySettings)
        );
        createIndexWithContent(INDEX);
        proveRepository();
        return internalCluster().getClusterManagerName();
    }

    private void proveRepository() throws Exception {
        createFullSnapshot(REPO, "warm-up");
        assertAcked(clusterAdmin().prepareDeleteSnapshot(REPO, "warm-up").get());
        final MockRepository repository = (MockRepository) internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class)
            .repository(REPO);
        assertTrue("the store probe did not complete", repository.awaitConditionalWriteProbe(PATIENCE));
        assertTrue("the repository must hand out the delete entrypoint", repository.abandonableSnapshotDelete().isPresent());
    }

    private void setIoTimeout(TimeValue value) {
        assertAcked(
            clusterAdmin().prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(IO_TIMEOUT_KEY, value.getStringRep()).build())
                .get()
        );
    }

    private void awaitBudgetExpiry(ActionFuture<AcknowledgedResponse> future, int snapshotCount) {
        final Exception failure = expectThrows(Exception.class, () -> future.actionGet(PATIENCE));
        final Throwable timeout = ExceptionsHelper.unwrap(failure, OpenSearchTimeoutException.class);
        assertNotNull("expected an OpenSearchTimeoutException, got [" + failure + "]", timeout);
        assertThat(
            "expected the budget's own expiry for this delete, not a wait that gave up on the future",
            timeout.getMessage(),
            containsString("delete " + snapshotCount + " snapshot(s) from [" + REPO + "]")
        );
        assertThat(timeout.getMessage(), containsString("timed out after"));
        assertThat(timeout.getMessage(), containsString("the deletion may still take effect"));
    }

    private void awaitAckedWhileParked(ActionFuture<AcknowledgedResponse> future, String clusterManager) {
        try {
            assertAcked(future.actionGet(PATIENCE));
        } catch (AssertionError | RuntimeException e) {
            unblockNode(REPO, clusterManager);
            throw e;
        }
    }

    private void assertRestoresTo(String repository, String snapshotName, Map<String, Object> arranged) {
        final RestoreInfo restored = clusterAdmin().prepareRestoreSnapshot(repository, snapshotName)
            .setIndices(INDEX)
            .setRenamePattern(INDEX)
            .setRenameReplacement(RESTORED_INDEX)
            .setWaitForCompletion(true)
            .get()
            .getRestoreInfo();
        assertNotNull("the restore of [" + snapshotName + "] must have produced a result", restored);
        assertEquals("no shard of [" + snapshotName + "] may have failed to restore", 0, restored.failedShards());
        ensureGreen(RESTORED_INDEX);
        assertEquals(
            "[" + snapshotName + "] must restore exactly the documents that were arranged",
            arranged,
            readContents(RESTORED_INDEX)
        );
        assertAcked(client().admin().indices().prepareDelete(RESTORED_INDEX));
    }

    private long indexLatest() throws Exception {
        return BytesRefUtils.bytesToLong(new BytesRef(Files.readAllBytes(repoPath.resolve(BlobStoreRepository.INDEX_LATEST_BLOB))));
    }

    private Set<String> rootIndexNBlobs() throws Exception {
        try (Stream<Path> blobs = Files.list(repoPath)) {
            return blobs.map(blob -> blob.getFileName().toString())
                .filter(name -> name.startsWith(BlobStoreRepository.INDEX_FILE_PREFIX))
                .collect(Collectors.toSet());
        }
    }

    private Path rootSnapshotBlob(String snapshotName) {
        final SnapshotId snapshotId = getRepositoryData(REPO).getSnapshotIds()
            .stream()
            .filter(id -> id.getName().equals(snapshotName))
            .findFirst()
            .orElseThrow();
        return repoPath.resolve(BlobStoreRepository.SNAPSHOT_FORMAT.blobName(snapshotId.getUUID()));
    }

    private void assertRepositoryStillUsable(String snapshotName) {
        assertThat(createFullSnapshot(REPO, snapshotName).state(), is(SnapshotState.SUCCESS));
    }

    private List<SnapshotDeletionsInProgress.Entry> deletionEntries() {
        return clusterAdmin().prepareState()
            .get()
            .getState()
            .custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY)
            .getEntries();
    }

    private String startedDeletionUuid() {
        for (SnapshotDeletionsInProgress.Entry entry : deletionEntries()) {
            if (entry.state() == SnapshotDeletionsInProgress.State.STARTED) {
                return entry.uuid();
            }
        }
        throw new AssertionError("no started deletion found in " + deletionEntries());
    }

    private static Set<String> names(Collection<SnapshotId> snapshotIds) {
        return snapshotIds.stream().map(SnapshotId::getName).collect(Collectors.toSet());
    }

    private RepositoryMetadata repositoryMetadata() {
        final RepositoriesMetadata repositories = clusterAdmin().prepareState()
            .get()
            .getState()
            .metadata()
            .custom(RepositoriesMetadata.TYPE);
        assertNotNull("no repositories metadata in the cluster state", repositories);
        final RepositoryMetadata metadata = repositories.repository(REPO);
        assertNotNull("no metadata for repository [" + REPO + "]", metadata);
        return metadata;
    }
}
