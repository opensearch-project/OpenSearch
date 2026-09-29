/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.support.PlainActionFuture;
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
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.env.Environment;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.opensearch.index.snapshots.blobstore.SnapshotFiles;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.search.SearchHit;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

/**
 * End-to-end coverage for the time budget on snapshot finalization: when one finalization outlives its budget, only its
 * caller is answered, with a timeout. A finalization whose budget expires before it starts writing the repository
 * generation is stopped: the budget's removal takes its in-progress entry out and hands the repository on, and the
 * released call is refused and releases nothing. One whose budget expires while it writes the repository generation
 * keeps its in-progress entry, the snapshot's name and the per-repository operation token until it completes or fails.
 * Either way nothing queued behind it is failed.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotFinalizationTimeoutIT extends AbstractSnapshotIntegTestCase {

    private static final String REPO = "test-repo";

    private static final String INDEX = "test-idx";

    /**
     * An index held by the queued snapshot and by nothing else, so that a refusal to delete it is attributable to the
     * queued snapshot's own in-progress marker. Without a second index the queued snapshot would share
     * {@link #INDEX} with the parked one, and every refusal would be explained by the parked marker alone.
     */
    private static final String QUEUED_INDEX = "queued-idx";

    private static final String IO_TIMEOUT_KEY = "snapshot.repository.io_timeout";

    /** The snapshot whose finalization is parked inside the repository and therefore outlives its budget. */
    private static final String PARKED = "parked-snapshot";

    /** A snapshot that finishes its shards while the parked one holds the repository, so it is queued behind it. */
    private static final String QUEUED = "queued-snapshot";

    /** A snapshot whose finalization times out and whose name is then used again. */
    private static final String RETRIED = "retried-snapshot";

    /** An index first snapshotted by a finalization that times out. */
    private static final String NEW_INDEX = "new-idx";

    /**
     * Appended to the waits on a second snapshot: a finalization parked at its entrypoint leaves the rest of the repository
     * free, so a snapshot started meanwhile must still read repository data, take an in-progress marker and finish its
     * shards, which a blob-level block would prevent.
     */
    private static final String PARKING_LEAVES_THE_REPOSITORY_FREE =
        "; a snapshot started while a finalization is parked must still reach cluster state and finish its shards";

    /** Appended to the one setup wait whose failure really is a budget that expired earlier than the wait needed. */
    private static final String BUDGET_EXPIRED_TOO_EARLY = "; the budget expired before the sibling was queued behind it";

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING.getKey(), true).build();
    }

    /**
     * Runs before the inherited teardown: every budgeted finalization has returned, so nothing on the cluster manager is
     * still recorded as past its budget.
     */
    @After
    public void assertNoCallIsLeftPastItsBudget() throws Exception {
        assertBusy(
            () -> assertTrue(
                "a finalization is still recorded as past its budget",
                internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class)
                    .repositoriesWithCallsPastBudget()
                    .isEmpty()
            )
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(FinalizationParkingMockRepositoryPlugin.class);
    }

    /**
     * A finalization whose budget expires while it writes its metadata is refused once those writes complete, before the
     * repository generation or {@code index.latest} moves. One whose budget expires before it enters the repository is
     * stopped, and while the stopped call is still parked the snapshot queued behind it, a delete of its index (refused
     * until the budget expires), a retry under its name, a repository cleanup and unregistering the repository all run.
     * Released, the stopped call writes nothing at the root; registered again at the same location, the repository points
     * at the committed generation and records exactly one snapshot under the name, the retry.
     */
    public void testStoppedFinalizationReleasesItsQueueIndexAndName() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        final Path repoPath = randomRepoPath();
        createEnforcingRepository(repoPath);
        createIndexWithContent(INDEX);
        proveRepository(clusterManagerNode);
        createIndexWithContent(QUEUED_INDEX);
        final Set<String> documents = documentIds(QUEUED_INDEX);
        createSnapshot(REPO, "other", Collections.singletonList(QUEUED_INDEX));
        final FinalizationParkingMockRepository repository = parkingRepository(clusterManagerNode, REPO);

        final long committed = getRepositoryData(REPO).getGenId();
        final Set<String> rootBlobsBeforeMetadata = rootBlobNames(repoPath);
        repository.pauseOnce(FinalizationParkingMockRepository.Pause.METADATA_WRITE);
        setIoTimeout("5s");
        final ActionFuture<CreateSnapshotResponse> duringMetadata = startSnapshot(clusterManagerNode, "during-metadata");
        try {
            waitForBlock(clusterManagerNode, REPO, TimeValue.timeValueSeconds(60L));
            assertThat(awaitAbandonment(duringMetadata, "during-metadata").getMessage(), containsString("so it will not be recorded"));
        } finally {
            unblockNode(REPO, clusterManagerNode);
        }
        assertBusy(
            () -> assertTrue(
                internalCluster().getInstance(RepositoriesService.class, clusterManagerNode).repositoriesWithCallsPastBudget().isEmpty()
            )
        );
        final Set<String> writtenDuringMetadata = new HashSet<>(rootBlobNames(repoPath));
        writtenDuringMetadata.removeAll(rootBlobsBeforeMetadata);
        assertThat("the refusal must come after the metadata writes", writtenDuringMetadata, hasItem(startsWith("snap-")));
        assertThat(getRepositoryData(REPO).getGenId(), equalTo(committed));
        assertThat(indexLatest(repoPath), equalTo(committed));

        repository.pauseOnce(FinalizationParkingMockRepository.Pause.PRE_ENTRY);
        setIoTimeout("10s");
        final ActionFuture<CreateSnapshotResponse> timedOut = startSnapshot(clusterManagerNode, RETRIED, INDEX);
        final ActionFuture<CreateSnapshotResponse> queued;
        final Set<String> rootBlobsBeforeRelease;
        try {
            repository.awaitPaused();
            final SnapshotInProgressException gated = expectThrows(
                SnapshotInProgressException.class,
                () -> client(clusterManagerNode).admin().indices().prepareDelete(INDEX).get()
            );
            assertThat(gated.getMessage(), containsString("Cannot delete indices that are being snapshotted"));
            setIoTimeout("1h");
            queued = startSnapshot(clusterManagerNode, QUEUED, QUEUED_INDEX);
            awaitQueuedBehindTheParkedFinalization(clusterManagerNode, RETRIED);
            awaitAbandonment(timedOut, RETRIED);

            assertAcked(client(clusterManagerNode).admin().indices().prepareDelete(INDEX).get());
            assertThat(queued.actionGet(TimeValue.timeValueSeconds(60L)).getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
            final SnapshotInfo retried = client(clusterManagerNode).admin()
                .cluster()
                .prepareCreateSnapshot(REPO, RETRIED)
                .setIndices(QUEUED_INDEX)
                .setWaitForCompletion(true)
                .get()
                .getSnapshotInfo();
            assertThat("a same-name retry must run its normal flow", retried.state(), is(SnapshotState.SUCCESS));
            clusterAdmin().prepareCleanupRepository(REPO).get();
            rootBlobsBeforeRelease = rootBlobNames(repoPath);
            assertAcked(clusterAdmin().prepareDeleteRepository(REPO).get());
        } finally {
            repository.release();
        }
        assertBusy(
            () -> assertTrue(
                internalCluster().getInstance(RepositoriesService.class, clusterManagerNode).repositoriesWithCallsPastBudget().isEmpty()
            )
        );
        awaitNoMoreRunningOperations(clusterManagerNode);
        createEnforcingRepository(repoPath);

        assertThat("the stopped call must write nothing at the root", rootBlobNames(repoPath), equalTo(rootBlobsBeforeRelease));
        assertOneSnapshotNamed(repoPath, getRepositoryData(REPO).getGenId(), RETRIED);
        assertThat(snapshotNames(getRepositoryData(REPO).getSnapshotIds()), equalTo(Set.of("other", QUEUED, RETRIED)));
        assertRestoresDocuments(RETRIED, QUEUED_INDEX, documents);
        assertRestoresDocuments(QUEUED, QUEUED_INDEX, documents);
    }

    /**
     * A finalization whose budget expires while it writes the repository generation only has its caller answered: a retry
     * under its name and a delete of its index are refused while it runs, so the repository never records two snapshots
     * with one name. It lands after the retry was refused as the one commit, the record stays single across a
     * cluster-manager restart, and the snapshot restores.
     */
    public void testRetryUnderATimedOutNameIsRefusedWhileTheCallRuns() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final Path repoPath = randomRepoPath();
        createEnforcingRepository(repoPath);
        createIndexWithContent(INDEX);
        proveRepository(clusterManagerNode);
        final Set<String> documents = documentIds(INDEX);
        final long generation = getRepositoryData(REPO).getGenId();

        final FinalizationParkingMockRepository repository = parkingRepository(clusterManagerNode, REPO);
        repository.pauseOnce(FinalizationParkingMockRepository.Pause.POST_CLAIM);
        setIoTimeout("1s");
        final ActionFuture<CreateSnapshotResponse> timedOut = startSnapshot(clusterManagerNode, RETRIED);
        try {
            repository.awaitPaused();
            setIoTimeout("1h");
            assertThat(
                awaitAbandonment(timedOut, RETRIED).getMessage(),
                containsString("it was already writing the repository generation and may still complete")
            );
            final InvalidSnapshotNameException refused = expectThrows(
                InvalidSnapshotNameException.class,
                () -> client(clusterManagerNode).admin().cluster().prepareCreateSnapshot(REPO, RETRIED).setIndices(INDEX).get()
            );
            assertThat(refused.getMessage(), containsString("already in-progress"));
            expectThrows(SnapshotInProgressException.class, () -> client(clusterManagerNode).admin().indices().prepareDelete(INDEX).get());
        } finally {
            repository.release();
        }
        awaitClusterState(clusterManagerNode, state -> repositoryGeneration(state) == generation + 1 && nothingInProgress(state));
        assertBusy(() -> {
            final long latest;
            try {
                latest = indexLatest(repoPath);
            } catch (NoSuchFileException e) {
                throw new AssertionError("index.latest is not written yet", e);
            }
            assertThat(latest, equalTo(generation + 1));
        });

        assertOneSnapshotNamed(repoPath, generation + 1, RETRIED);
        assertThat(getSnapshot(REPO, RETRIED).state(), is(SnapshotState.SUCCESS));

        internalCluster().restartNode(clusterManagerNode);
        ensureGreen(INDEX);
        assertThat(getRepositoryData(REPO).getGenId(), equalTo(generation + 1));
        assertOneSnapshotNamed(repoPath, generation + 1, RETRIED);

        assertRestoresDocuments(RETRIED, INDEX, documents);
        final InvalidSnapshotNameException exists = expectThrows(
            InvalidSnapshotNameException.class,
            () -> clusterAdmin().prepareCreateSnapshot(REPO, RETRIED).setIndices(INDEX).get()
        );
        assertThat(exists.getMessage(), containsString("already exists"));
    }

    /**
     * A snapshot started while a timed-out finalization is still running takes its shard generations, and the index id of
     * an index that finalization snapshots first, from that finalization. When both land each shard's index lists both
     * and the new index is recorded against one index id, and deleting the later one leaves the earlier one restorable.
     */
    public void testSnapshotStartedAfterATimeoutKeepsTheShardLineage() throws Exception {
        final String clusterManagerNode = startClusterWithFencingRepository();
        createSnapshot(REPO, "s0", Collections.singletonList(INDEX));
        index(INDEX, "_doc", "second_id", "foo", "baz");
        flush(INDEX);
        createIndexWithContent(NEW_INDEX);
        final Set<String> documents = documentIds(INDEX);
        final Set<String> newDocuments = documentIds(NEW_INDEX);
        final FinalizationParkingMockRepository repository = parkingRepository(clusterManagerNode, REPO);
        repository.pauseOnce(FinalizationParkingMockRepository.Pause.POST_CLAIM);
        setIoTimeout("1s");

        final ActionFuture<CreateSnapshotResponse> first = startSnapshot(clusterManagerNode, "s1", INDEX, NEW_INDEX);
        final ActionFuture<CreateSnapshotResponse> second;
        try {
            repository.awaitPaused();
            setIoTimeout("1h");
            awaitAbandonment(first, "s1");
            second = startSnapshot(clusterManagerNode, "s2", INDEX, NEW_INDEX);
            awaitClusterState(clusterManagerNode, state -> shardsDone(state, "s2"));
        } finally {
            repository.release();
        }
        assertThat(second.actionGet(TimeValue.timeValueSeconds(60L)).getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        awaitNoMoreRunningOperations(clusterManagerNode);

        final RepositoryData repositoryData = getRepositoryData(REPO);
        assertThat(snapshotNames(repositoryData.getSnapshotIds()), equalTo(Set.of("s0", "s1", "s2")));
        assertThat(snapshotNames(repositoryData.getSnapshots(repositoryData.resolveIndexId(NEW_INDEX))), equalTo(Set.of("s1", "s2")));
        assertThat("the shard index lost a snapshot", shardIndexSnapshotNames(repositoryData, INDEX), hasItems("s1", "s2"));
        assertThat("the shard index lost a snapshot", shardIndexSnapshotNames(repositoryData, NEW_INDEX), hasItems("s1", "s2"));
        assertAcked(startDeleteSnapshot(REPO, "s2").get());
        assertRestoresDocuments("s1", INDEX, documents);
        assertRestoresDocuments("s1", NEW_INDEX, newDocuments);
    }

    /**
     * A finalization whose budget expires while it writes the repository generation keeps its entry, and is not ended a
     * second time by a later cluster state change that ends snapshots, here a data node leaving while another snapshot
     * runs on it: the repository is handed its call once.
     */
    public void testTimedOutFinalizationIsNotEndedTwice() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        final String firstDataNode = internalCluster().startDataOnlyNode();
        final String secondDataNode = internalCluster().startDataOnlyNode();
        createEnforcingRepository(randomRepoPath());
        createIndexWithContent(INDEX, indexSettingsNoReplicas(1).put("index.routing.allocation.require._name", firstDataNode).build());
        proveRepository(clusterManagerNode);
        createIndexWithContent(
            QUEUED_INDEX,
            indexSettingsNoReplicas(1).put("index.routing.allocation.require._name", secondDataNode).build()
        );
        final FinalizationParkingMockRepository repository = parkingRepository(clusterManagerNode, REPO);
        repository.pauseOnce(FinalizationParkingMockRepository.Pause.POST_CLAIM);
        setIoTimeout("1s");

        final ActionFuture<CreateSnapshotResponse> parked = startSnapshot(clusterManagerNode, PARKED, INDEX);
        final ActionFuture<CreateSnapshotResponse> onLeavingNode;
        try {
            repository.awaitPaused();
            setIoTimeout("1h");
            awaitAbandonment(parked, PARKED);
            assertNotNull("a call writing the generation must keep its entry", inProgressEntry(clusterManagerNode, PARKED));
            blockDataNode(REPO, secondDataNode);
            onLeavingNode = startSnapshot(clusterManagerNode, QUEUED, QUEUED_INDEX);
            waitForBlock(secondDataNode, REPO, TimeValue.timeValueSeconds(60L));
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(secondDataNode));
            awaitClusterState(clusterManagerNode, state -> shardsDone(state, QUEUED));
        } finally {
            repository.release();
        }
        onLeavingNode.actionGet(TimeValue.timeValueSeconds(60L));
        awaitNoMoreRunningOperations(clusterManagerNode);
        assertThat("the timed-out finalization was ended a second time", repository.entrypointFinalizationsOf(PARKED), equalTo(1));
        assertAcked(client().admin().indices().prepareDelete(QUEUED_INDEX).get());
    }

    private Set<String> documentIds(String index) {
        refresh(index);
        final SearchHit[] hits = client().prepareSearch(index).setSize(100).get().getHits().getHits();
        final Set<String> ids = new HashSet<>();
        for (SearchHit hit : hits) {
            ids.add(hit.getId());
        }
        return ids;
    }

    /** Restores {@code index} from {@code snapshotName} under a new name and requires exactly {@code expected}. */
    private void assertRestoresDocuments(String snapshotName, String index, Set<String> expected) {
        final String restored = "restored-" + snapshotName + "-" + index;
        final RestoreSnapshotResponse response = clusterAdmin().prepareRestoreSnapshot(REPO, snapshotName)
            .setIndices(index)
            .setRenamePattern("(.+)")
            .setRenameReplacement(restored)
            .setWaitForCompletion(true)
            .get();
        assertThat(response.getRestoreInfo().failedShards(), equalTo(0));
        assertThat(documentIds(restored), equalTo(expected));
    }

    private static boolean shardsDone(ClusterState state, String snapshotName) {
        for (SnapshotsInProgress.Entry entry : state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries()) {
            if (entry.snapshot().getSnapshotId().getName().equals(snapshotName)) {
                return entry.state().completed();
            }
        }
        return false;
    }

    private static long repositoryGeneration(ClusterState state) {
        return state.metadata().<RepositoriesMetadata>custom(RepositoriesMetadata.TYPE).repository(REPO).generation();
    }

    private static boolean nothingInProgress(ClusterState state) {
        return state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries().isEmpty()
            && state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY).hasDeletionsInProgress() == false;
    }

    /**
     * Reads the raw root blob of {@code generation}, before anything could repair it, and requires that it records
     * exactly one snapshot named {@code snapshotName} and that {@code index.latest} names that generation.
     */
    private static void assertOneSnapshotNamed(Path repoPath, long generation, String snapshotName) throws IOException {
        assertThat("index.latest does not name the committed generation", indexLatest(repoPath), equalTo(generation));
        final RepositoryData raw;
        try (
            InputStream blob = Files.newInputStream(repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + generation));
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                blob
            )
        ) {
            raw = RepositoryData.snapshotsFromXContent(parser, generation);
        }
        final long named = raw.getSnapshotIds().stream().filter(id -> id.getName().equals(snapshotName)).count();
        assertThat("the root blob records [" + snapshotName + "] other than once", named, equalTo(1L));
    }

    private static long indexLatest(Path repoPath) throws IOException {
        return ByteBuffer.wrap(Files.readAllBytes(repoPath.resolve(BlobStoreRepository.INDEX_LATEST_BLOB))).getLong();
    }

    private static Set<String> rootBlobNames(Path repoPath) throws IOException {
        try (Stream<Path> contents = Files.list(repoPath)) {
            return contents.filter(Files::isRegularFile).map(file -> file.getFileName().toString()).collect(Collectors.toSet());
        }
    }

    /** The snapshot names the current shard index of shard 0 of {@code indexName} lists, read on a repository thread. */
    private Set<String> shardIndexSnapshotNames(RepositoryData repositoryData, String indexName) {
        final IndexId indexId = repositoryData.resolveIndexId(indexName);
        final String generation = repositoryData.shardGenerations().getShardGen(indexId, 0);
        final BlobStoreRepository repository = (BlobStoreRepository) internalCluster().getCurrentClusterManagerNodeInstance(
            RepositoriesService.class
        ).repository(REPO);
        final BlobStoreIndexShardSnapshots shardSnapshots = PlainActionFuture.get(
            f -> repository.threadPool()
                .generic()
                .execute(
                    ActionRunnable.supply(
                        f,
                        () -> BlobStoreRepository.INDEX_SHARD_SNAPSHOTS_FORMAT.read(
                            repository.shardContainer(indexId, 0),
                            generation,
                            NamedXContentRegistry.EMPTY
                        )
                    )
                )
        );
        final Set<String> names = new HashSet<>();
        for (SnapshotFiles snapshotFiles : shardSnapshots.snapshots()) {
            names.add(snapshotFiles.snapshot());
        }
        return names;
    }

    private String startClusterWithFencingRepository() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        createEnforcingRepository(randomRepoPath());
        createIndexWithContent(INDEX);
        proveRepository(clusterManagerNode);
        return clusterManagerNode;
    }

    /** Registers {@link #REPO} as the enforcing mock store, which declares the finalization entrypoint once proven. */
    private void createEnforcingRepository(Path location) {
        createRepository(
            REPO,
            FinalizationParkingMockRepositoryPlugin.TYPE,
            Settings.builder().put("location", location).put("conditional_writes", true)
        );
    }

    /**
     * Makes {@link #REPO} hand out its finalization entrypoint, so that finalizations on it are budgeted: a snapshot
     * gives it a committed generation, so it is strictly consistent, and a read of the capability then probes its store
     * before the snapshot is deleted. Needs {@link #INDEX}.
     */
    private void proveRepository(String clusterManagerNode) throws Exception {
        createSnapshot(REPO, "warm-up", Collections.singletonList(INDEX));
        final FinalizationParkingMockRepository repository = parkingRepository(clusterManagerNode, REPO);
        assertTrue(repository.abandonableSnapshotFinalization().isEmpty());
        assertAcked(startDeleteSnapshot(REPO, "warm-up").get());
        assertTrue("the store probe did not complete", repository.awaitConditionalWriteProbe(TimeValue.timeValueSeconds(30L)));
        assertTrue("the enforcing mock store must hand out the entrypoint", repository.abandonableSnapshotFinalization().isPresent());
    }

    /**
     * The named repository on {@code node}, as the parking double.
     */
    private FinalizationParkingMockRepository parkingRepository(String node, String repositoryName) {
        final Repository repository = internalCluster().getInstance(RepositoriesService.class, node).repository(repositoryName);
        assertThat(repository, instanceOf(FinalizationParkingMockRepository.class));
        return (FinalizationParkingMockRepository) repository;
    }

    private void setIoTimeout(String value) {
        assertAcked(
            clusterAdmin().prepareUpdateSettings().setPersistentSettings(Settings.builder().put(IO_TIMEOUT_KEY, value).build()).get()
        );
    }

    private ActionFuture<CreateSnapshotResponse> startSnapshot(String viaNode, String snapshotName) {
        return startSnapshot(viaNode, snapshotName, INDEX);
    }

    /** Non-partial by default, which is what makes the entry gate an index delete at all. */
    private ActionFuture<CreateSnapshotResponse> startSnapshot(String viaNode, String snapshotName, String... indices) {
        return client(viaNode).admin()
            .cluster()
            .prepareCreateSnapshot(REPO, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indices)
            .execute();
    }

    /**
     * Waits, bounded, for the create call to fail with the finalization budget's own exception, and returns it. Bounded
     * because the defect this catches is a finalization that never returns: unbounded, the run would die on the suite
     * timeout with no attribution, which reads as infrastructure trouble rather than as this test failing.
     * <p>
     * The exception type is asserted by unwrapping rather than by the {@code expectThrows}, which tolerates a wrapping
     * transport exception instead of ruling one out: a repository error or a rejected schedule would satisfy the
     * {@code expectThrows} and then fail the assertions below, which is the point.
     */
    private Throwable awaitAbandonment(ActionFuture<CreateSnapshotResponse> future, String snapshotName) {
        final Exception failure = expectThrows(Exception.class, () -> future.actionGet(TimeValue.timeValueSeconds(60L)));
        final Throwable timeout = ExceptionsHelper.unwrap(failure, OpenSearchTimeoutException.class);
        assertNotNull("expected an OpenSearchTimeoutException, got [" + failure + "]", timeout);
        assertThat(timeout.getMessage(), containsString("finalize snapshot ["));
        assertThat(timeout.getMessage(), containsString(snapshotName));
        return timeout;
    }

    private SnapshotId awaitQueuedBehindTheParkedFinalization(String node, String parkedSnapshot) throws Exception {
        final AtomicReference<SnapshotId> queuedSnapshotId = new AtomicReference<>();
        assertBusy(() -> {
            assertNotNull(
                "the abandoned snapshot's marker is already gone" + BUDGET_EXPIRED_TOO_EARLY,
                inProgressEntry(node, parkedSnapshot)
            );
            final SnapshotsInProgress.Entry entry = inProgressEntry(node, QUEUED);
            assertNotNull("the sibling never got an in-progress marker" + PARKING_LEAVES_THE_REPOSITORY_FREE, entry);
            assertTrue(
                "the sibling's shards have not finished, so it is not queued yet" + PARKING_LEAVES_THE_REPOSITORY_FREE,
                entry.state().completed()
            );
            queuedSnapshotId.set(entry.snapshot().getSnapshotId());
        });
        return queuedSnapshotId.get();
    }

    private static Set<String> snapshotNames(Collection<SnapshotId> snapshotIds) {
        final Set<String> names = new HashSet<>();
        for (SnapshotId snapshotId : snapshotIds) {
            names.add(snapshotId.getName());
        }
        return names;
    }

    private SnapshotsInProgress.Entry inProgressEntry(String node, String snapshotName) {
        final ClusterState state = internalCluster().getInstance(ClusterService.class, node).state();
        final SnapshotsInProgress inProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        for (SnapshotsInProgress.Entry entry : inProgress.entries()) {
            if (entry.snapshot().getSnapshotId().getName().equals(snapshotName)) {
                return entry;
            }
        }
        return null;
    }

    /**
     * A {@link MockRepository} that pauses one finalization at the point a test arms, so that its budget expires there. A
     * pause is armed once and taken by the first call that reaches it.
     */
    public static class FinalizationParkingMockRepository extends MockRepository {

        /** Where the next finalization is paused. */
        enum Pause {
            /** Holds the finalization before it enters the repository, until {@link #release()}. */
            PRE_ENTRY,
            /** Blocks the finalization's first blob write, which writes its metadata, until {@link #unblock()}. */
            METADATA_WRITE,
            /** Starts the finalization's generation write, then holds it before it enters the repository, until {@link #release()}. */
            POST_CLAIM
        }

        private final AtomicReference<Pause> armed = new AtomicReference<>();

        private final AtomicReference<Runnable> paused = new AtomicReference<>();

        private final Map<String, AtomicInteger> entrypointFinalizations = new ConcurrentHashMap<>();

        /** Guarded by this. */
        private Optional<AbandonableSnapshotFinalization> entrypoint = Optional.empty();

        public FinalizationParkingMockRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings);
        }

        void pauseOnce(Pause pause) {
            armed.set(pause);
        }

        /** Waits, bounded, for a finalization to be held. */
        void awaitPaused() throws Exception {
            assertBusy(() -> assertNotNull("no finalization was paused", paused.get()), 60L, TimeUnit.SECONDS);
        }

        int entrypointFinalizationsOf(String snapshotName) {
            final AtomicInteger count = entrypointFinalizations.get(snapshotName);
            return count == null ? 0 : count.get();
        }

        /**
         * Resumes the held finalization, and the real finalization runs. If its budget expired before it started
         * writing the repository generation, it is refused at its first check and releases nothing, because the
         * budget's removal already took its entry out and handed the repository on; otherwise it completes or fails as
         * usual.
         * <p>
         * A no-op when nothing is held, so it is safe in the {@code finally} the inherited
         * {@code verifyNoLeakedListeners} requires it to be called from - a test that failed before the pause happened
         * must report its own failure rather than a null here.
         */
        void release() {
            final Runnable resume = paused.getAndSet(null);
            if (resume != null) {
                resume.run();
            }
        }

        /**
         * The entrypoint the service budgets finalizations through: the inherited one, mapped so that a finalization can
         * be paused and counted. Mapped once, so the answer is the same object on every call once present.
         */
        @Override
        public synchronized Optional<AbandonableSnapshotFinalization> abandonableSnapshotFinalization() {
            if (entrypoint.isEmpty()) {
                entrypoint = super.abandonableSnapshotFinalization().map(
                    inherited -> (
                        shardGenerations,
                        repositoryStateId,
                        clusterMetadata,
                        snapshotInfo,
                        repositoryMetaVersion,
                        stateTransformer,
                        repositoryUpdatePriority,
                        attempt,
                        listener) -> {
                        entrypointFinalizations.computeIfAbsent(snapshotInfo.snapshotId().getName(), k -> new AtomicInteger())
                            .incrementAndGet();
                        final Runnable call = () -> inherited.finalizeSnapshot(
                            shardGenerations,
                            repositoryStateId,
                            clusterMetadata,
                            snapshotInfo,
                            repositoryMetaVersion,
                            stateTransformer,
                            repositoryUpdatePriority,
                            attempt,
                            listener
                        );
                        if (armed.compareAndSet(Pause.METADATA_WRITE, null)) {
                            setBlockOnAnyFiles(true);
                        } else if (armed.compareAndSet(Pause.POST_CLAIM, null)) {
                            attempt.startGenerationWrite();
                            paused.set(call);
                            return;
                        } else if (armed.compareAndSet(Pause.PRE_ENTRY, null)) {
                            paused.set(call);
                            return;
                        }
                        call.run();
                    }
                );
            }
            return entrypoint;
        }
    }

    /** Registers {@link FinalizationParkingMockRepository} under a repository type of its own. */
    public static class FinalizationParkingMockRepositoryPlugin extends MockRepository.Plugin {

        public static final String TYPE = "finalizationparkingmock";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                TYPE,
                metadata -> new FinalizationParkingMockRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings)
            );
        }
    }
}
