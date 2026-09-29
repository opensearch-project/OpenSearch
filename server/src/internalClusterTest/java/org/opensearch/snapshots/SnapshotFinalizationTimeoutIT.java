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
import org.opensearch.Version;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
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
import org.opensearch.repositories.ShardGenerations;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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
     * A second index, in no parked snapshot and never deleted. It exists so that the assertion on a recorded snapshot's
     * index set has something to be true of: with {@link #INDEX} deleted and nothing else in the cluster, "the snapshot
     * does not name the deleted index" would hold of an empty index set and prove nothing.
     */
    private static final String SURVIVING_INDEX = "surviving-idx";

    /**
     * An index held by the queued snapshot and by nothing else, so that a refusal to delete it is attributable to the
     * queued snapshot's own in-progress marker. Without a second index the queued snapshot would share
     * {@link #INDEX} with the parked one, and every refusal would be explained by the parked marker alone.
     */
    private static final String QUEUED_INDEX = "queued-idx";

    private static final String IO_TIMEOUT_KEY = "snapshot.repository.io_timeout";

    /** The setting's own default, restored before any trailing snapshot so that snapshot never races a short budget. */
    private static final String DEFAULT_IO_TIMEOUT = "30m";

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
        // The base class returns only MockRepository.Plugin, which can neither observe the attempt a finalization is
        // given nor park a finalization.
        return Collections.singletonList(FinalizationParkingMockRepositoryPlugin.class);
    }

    /**
     * A non-partial create keeps the index-delete gate shut ({@code SnapshotsService.snapshottingIndices} filters on
     * {@code partial() == false} and nothing else) while its entry is present. A budget that expires before the
     * finalization started writing the repository generation removes the entry, so the index can be deleted while the
     * stopped call is still parked; released, the call is refused and records nothing. No recorded snapshot names the
     * deleted index, because the stopped call never writes the repository generation. The inherited repository
     * consistency check stays enabled.
     */
    public void testDeletingTheIndexOfAStoppedFinalizationLeavesItInNoRecordedSnapshot() throws Exception {
        final String clusterManagerNode = startClusterWithFencingRepository();
        createIndexWithContent(SURVIVING_INDEX);
        final FinalizationParkingMockRepository repository = fencingRepository(clusterManagerNode);
        // The finalization boundary, not the blob layer: the two index-delete attempts below are cluster-manager work
        // that reads repository data on the way in, and a blob-level block would park that too.
        repository.parkOnceFinalizationStarts();
        // 30s, so that the control showing the gate shut fits inside the budget; it is one delete round-trip, and if it is
        // missed the control fails saying the gate was already open.
        setIoTimeout("30s");

        final RepositoryData beforeFinalization = getRepositoryData(REPO);
        final ActionFuture<CreateSnapshotResponse> parked = startSnapshot(clusterManagerNode, PARKED);
        try {
            assertBusy(() -> assertNotNull("the finalization never parked inside the repository", repository.parkedFinalization()));

            // Control 1, and the reachability half of the question: the gate is shut while the marker is present.
            // Without it this test could not tell a gate that lifted from an index that was never gated at all.
            final SnapshotInProgressException gated = expectThrows(
                SnapshotInProgressException.class,
                () -> client(clusterManagerNode).admin().indices().prepareDelete(INDEX).get()
            );
            assertThat(gated.getMessage(), containsString("Cannot delete indices that are being snapshotted"));
            assertThat(gated.getMessage(), containsString(INDEX));

            awaitAbandonment(parked, PARKED);

            // Control 2: the expiry removed the entry, so the gate is open while the stopped call is still parked.
            assertAcked(client(clusterManagerNode).admin().indices().prepareDelete(INDEX).get());
        } finally {
            repository.releaseParkedFinalization();
        }

        awaitNoMoreRunningOperations(clusterManagerNode);
        assertBusy(
            () -> assertTrue(
                internalCluster().getInstance(RepositoriesService.class, clusterManagerNode).repositoriesWithCallsPastBudget().isEmpty()
            )
        );
        assertNothingRecorded(beforeFinalization, PARKED);

        // And the other half: whatever the repository does end up recording must not name the deleted index. The
        // trailing snapshot is also what gives the enabled consistency check a root blob to check.
        assertRepositoryStillUsable("snap-after-the-index-was-deleted");
        assertNoRecordedSnapshotReferences(INDEX, SURVIVING_INDEX);
    }

    /**
     * A finalization whose budget expires while it writes the repository generation only has its caller answered: a retry
     * under its name and a delete of its index are refused while it runs, so the repository never records two snapshots
     * with one name. It lands after the retry was refused; the record stays single across a cluster-manager restart,
     * deletes of other snapshots keep working, and the snapshot restores.
     */
    public void testRetryUnderATimedOutNameIsRefusedWhileTheCallRuns() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        final Path repoPath = randomRepoPath();
        createEnforcingRepository(repoPath);
        createIndexWithContent(INDEX);
        proveRepository(clusterManagerNode);
        index(INDEX, "_doc", "second_id", "foo", "baz");
        refresh(INDEX);
        final Set<String> documents = documentIds(INDEX);
        createSnapshot(REPO, "other-1", Collections.singletonList(INDEX));
        createSnapshot(REPO, "other-2", Collections.singletonList(INDEX));
        final long generation = getRepositoryData(REPO).getGenId();

        final FinalizationParkingMockRepository repository = fencingRepository(clusterManagerNode);
        // The call starts writing the repository generation before it enters the repository, so wherever the budget
        // expires it lands in the writing window, where the call is not given up on.
        repository.claimWritingOnce();
        repository.setBlockOnWriteIndexFile();
        setIoTimeout("1s");
        final ActionFuture<CreateSnapshotResponse> timedOut = startSnapshot(clusterManagerNode, RETRIED);
        try {
            waitForBlock(clusterManagerNode, REPO, TimeValue.timeValueSeconds(60L));
            setIoTimeout("1h");
            awaitAbandonment(timedOut, RETRIED);
            final Throwable writing = ExceptionsHelper.unwrap(
                expectThrows(Exception.class, () -> timedOut.actionGet(TimeValue.timeValueSeconds(60L))),
                OpenSearchTimeoutException.class
            );
            assertThat(writing.getMessage(), containsString("it was already writing the repository generation and may still complete"));
            final InvalidSnapshotNameException refused = expectThrows(
                InvalidSnapshotNameException.class,
                () -> client(clusterManagerNode).admin().cluster().prepareCreateSnapshot(REPO, RETRIED).setIndices(INDEX).get()
            );
            assertThat(refused.getMessage(), containsString("already in-progress"));
            expectThrows(SnapshotInProgressException.class, () -> client(clusterManagerNode).admin().indices().prepareDelete(INDEX).get());
        } finally {
            unblockNode(REPO, clusterManagerNode);
        }
        awaitClusterState(clusterManagerNode, state -> repositoryGeneration(state) == generation + 1 && nothingInProgress(state));
        // Waits for the pointer rather than assuming it is written before the generation is published. A replacement deletes the
        // old pointer before moving the new one in, so a missing file is retried like a stale one.
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
        assertAcked(startDeleteSnapshot(REPO, "other-1").get());

        internalCluster().restartNode(clusterManagerNode);
        ensureGreen(INDEX);
        final long afterRestart = getRepositoryData(REPO).getGenId();
        assertOneSnapshotNamed(repoPath, afterRestart, RETRIED);
        assertAcked(startDeleteSnapshot(REPO, "other-2").get());

        assertRestoresDocuments(RETRIED, documents);
        final InvalidSnapshotNameException exists = expectThrows(
            InvalidSnapshotNameException.class,
            () -> clusterAdmin().prepareCreateSnapshot(REPO, RETRIED).setIndices(INDEX).get()
        );
        assertThat(exists.getMessage(), containsString("already exists"));
    }

    /**
     * A snapshot started while a timed-out finalization is still running takes its shard generation from that
     * finalization, so when both land the shard's index lists both, and deleting the later one leaves the earlier one
     * restorable.
     */
    public void testSnapshotStartedAfterATimeoutKeepsTheShardLineage() throws Exception {
        final String clusterManagerNode = startClusterWithFencingRepository();
        createSnapshot(REPO, "s0", Collections.singletonList(INDEX));
        index(INDEX, "_doc", "second_id", "foo", "baz");
        flush(INDEX);
        final Set<String> documents = documentIds(INDEX);
        final FinalizationParkingMockRepository repository = fencingRepository(clusterManagerNode);
        repository.parkWritingFinalizationOf("s1");
        setIoTimeout("1s");

        final ActionFuture<CreateSnapshotResponse> first = startSnapshot(clusterManagerNode, "s1");
        final ActionFuture<CreateSnapshotResponse> second;
        try {
            repository.awaitParked();
            setIoTimeout("1h");
            awaitAbandonment(first, "s1");
            second = startSnapshot(clusterManagerNode, "s2");
            awaitClusterState(clusterManagerNode, state -> shardsDone(state, "s2"));
        } finally {
            repository.releaseParkedFinalization();
        }
        assertThat(second.actionGet(TimeValue.timeValueSeconds(60L)).getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        awaitNoMoreRunningOperations(clusterManagerNode);

        final RepositoryData repositoryData = getRepositoryData(REPO);
        assertThat(snapshotNames(repositoryData.getSnapshotIds()), equalTo(Set.of("s0", "s1", "s2")));
        assertThat("the shard index lost a snapshot", shardIndexSnapshotNames(repositoryData, INDEX), hasItems("s1", "s2"));
        assertAcked(startDeleteSnapshot(REPO, "s2").get());
        assertRestoresDocuments("s1", documents);
    }

    /**
     * A snapshot of a new index started while a timed-out finalization of that index is still running reuses its index
     * id, so both are recorded against one index id.
     */
    public void testSnapshotOfANewIndexAfterATimeoutReusesItsIndexId() throws Exception {
        final String clusterManagerNode = startClusterWithFencingRepository();
        createIndexWithContent(NEW_INDEX);
        final Set<String> documents = documentIds(NEW_INDEX);
        final FinalizationParkingMockRepository repository = fencingRepository(clusterManagerNode);
        repository.parkWritingFinalizationOf("x1");
        setIoTimeout("1s");

        final ActionFuture<CreateSnapshotResponse> first = startSnapshot(clusterManagerNode, "x1", NEW_INDEX);
        final ActionFuture<CreateSnapshotResponse> second;
        try {
            repository.awaitParked();
            setIoTimeout("1h");
            awaitAbandonment(first, "x1");
            second = startSnapshot(clusterManagerNode, "x2", NEW_INDEX);
            awaitClusterState(clusterManagerNode, state -> shardsDone(state, "x2"));
        } finally {
            repository.releaseParkedFinalization();
        }
        assertThat(second.actionGet(TimeValue.timeValueSeconds(60L)).getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        awaitNoMoreRunningOperations(clusterManagerNode);

        final RepositoryData repositoryData = getRepositoryData(REPO);
        final IndexId indexId = repositoryData.resolveIndexId(NEW_INDEX);
        assertThat(snapshotNames(repositoryData.getSnapshots(indexId)), equalTo(Set.of("x1", "x2")));
        assertRestoresDocuments("x1", documents);
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
        final FinalizationParkingMockRepository repository = fencingRepository(clusterManagerNode);
        repository.parkWritingFinalizationOf(PARKED);
        setIoTimeout("1s");

        final ActionFuture<CreateSnapshotResponse> parked = startSnapshot(clusterManagerNode, PARKED, INDEX);
        final ActionFuture<CreateSnapshotResponse> onLeavingNode;
        try {
            repository.awaitParked();
            setIoTimeout("1h");
            awaitAbandonment(parked, PARKED);
            assertNotNull("a call writing the generation must keep its entry", inProgressEntry(clusterManagerNode, PARKED));
            blockDataNode(REPO, secondDataNode);
            onLeavingNode = startSnapshot(clusterManagerNode, QUEUED, QUEUED_INDEX);
            waitForBlock(secondDataNode, REPO, TimeValue.timeValueSeconds(60L));
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(secondDataNode));
            awaitClusterState(clusterManagerNode, state -> shardsDone(state, QUEUED));
        } finally {
            repository.releaseParkedFinalization();
        }
        onLeavingNode.actionGet(TimeValue.timeValueSeconds(60L));
        awaitNoMoreRunningOperations(clusterManagerNode);
        assertThat("the timed-out finalization was ended a second time", repository.finalizationsOf(PARKED), equalTo(1));
        assertAcked(client().admin().indices().prepareDelete(QUEUED_INDEX).get());
    }

    /**
     * Once a finalization is stopped by its budget before it wrote the repository generation, repository cleanup and
     * unregistering the repository are admitted while the stopped call is still parked, because that call writes no root
     * generation. Released, it is refused and leaves nothing at the root; the repository registered again at the same
     * location points at the committed generation, and the snapshot taken before restores every document.
     */
    public void testStoppedFinalizationAdmitsCleanupAndUnregistration() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final Path repoPath = randomRepoPath();
        createEnforcingRepository(repoPath);
        createIndexWithContent(INDEX);
        proveRepository(clusterManagerNode);
        final Set<String> documents = documentIds(INDEX);
        createSnapshot(REPO, "retained", Collections.singletonList(INDEX));
        final FinalizationParkingMockRepository repository = fencingRepository(clusterManagerNode);
        repository.parkFinalizationOf(PARKED);
        setIoTimeout("1s");

        final ActionFuture<CreateSnapshotResponse> parked = startSnapshot(clusterManagerNode, PARKED);
        try {
            repository.awaitParked();
            setIoTimeout("1h");
            awaitAbandonment(parked, PARKED);
            clusterAdmin().prepareCleanupRepository(REPO).get();
            assertAcked(clusterAdmin().prepareDeleteRepository(REPO).get());
        } finally {
            repository.releaseParkedFinalization();
        }
        awaitNoMoreRunningOperations(clusterManagerNode);
        createEnforcingRepository(repoPath);

        final String parkedUuid = repository.finalizedSnapshotUuid(PARKED);
        assertNotNull(parkedUuid);
        assertThat(rootBlobNames(repoPath), not(hasItem("snap-" + parkedUuid + ".dat")));
        assertThat(rootBlobNames(repoPath), not(hasItem("meta-" + parkedUuid + ".dat")));
        final long committed = getRepositoryData(REPO).getGenId();
        assertThat(indexLatest(repoPath), equalTo(committed));
        assertThat(rootBlobNames(repoPath), hasItem(BlobStoreRepository.INDEX_FILE_PREFIX + committed));
        assertRestoresDocuments("retained", documents);
    }

    /**
     * Characterisation of a known limit, not a guarantee. A snapshot started while another finalization is
     * still running inherits that finalization's shard generation, which lists the other snapshot's name. If that
     * finalization then fails, as a finalization stopped by its budget does, a later snapshot under its name fails at the
     * shard level as a duplicate.
     */
    public void testFailedFinalizationNameStaysInTheInheritedShardIndex() throws Exception {
        final String clusterManagerNode = startClusterWithFencingRepository();
        final FinalizationParkingMockRepository repository = fencingRepository(clusterManagerNode);
        repository.parkFinalizationOf(RETRIED);

        final ActionFuture<CreateSnapshotResponse> failed = startSnapshot(clusterManagerNode, RETRIED);
        final ActionFuture<CreateSnapshotResponse> inheriting;
        try {
            repository.awaitParked();
            inheriting = startSnapshot(clusterManagerNode, "inheriting");
            awaitClusterState(clusterManagerNode, state -> shardsDone(state, "inheriting"));
        } finally {
            repository.failParkedFinalization();
        }
        expectThrows(Exception.class, () -> failed.actionGet(TimeValue.timeValueSeconds(60L)));
        assertThat(inheriting.actionGet(TimeValue.timeValueSeconds(60L)).getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        awaitNoMoreRunningOperations(clusterManagerNode);
        assertThat(snapshotNames(getRepositoryData(REPO).getSnapshotIds()), equalTo(Set.of("inheriting")));

        final SnapshotInfo retried = clusterAdmin().prepareCreateSnapshot(REPO, RETRIED)
            .setIndices(INDEX)
            .setWaitForCompletion(true)
            .get()
            .getSnapshotInfo();
        assertThat(retried.shardFailures(), hasSize(1));
        assertThat(retried.shardFailures().get(0).reason(), containsString("Duplicate snapshot name [" + RETRIED + "]"));
    }

    /**
     * A finalization whose budget expires before it started writing the repository generation is stopped, and everything
     * it was holding back runs while the stopped call is still parked: the snapshot queued behind it, a delete of its
     * index, a retry under its name and a repository cleanup. Released, the stopped call is refused and writes nothing,
     * and the repository records exactly one snapshot under the name, the retry.
     */
    public void testBudgetExpiryBeforeTheGenerationWriteReleasesEverything() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        final Path repoPath = randomRepoPath();
        createEnforcingRepository(repoPath);
        createIndexWithContent(INDEX);
        proveRepository(clusterManagerNode);
        createIndexWithContent(QUEUED_INDEX);
        final Set<String> documents = documentIds(QUEUED_INDEX);
        createSnapshot(REPO, "other", Collections.singletonList(QUEUED_INDEX));
        final FinalizationParkingMockRepository repository = fencingRepository(clusterManagerNode);
        repository.parkFinalizationOf(RETRIED);
        setIoTimeout("10s");

        final ActionFuture<CreateSnapshotResponse> timedOut = startSnapshot(clusterManagerNode, RETRIED, INDEX);
        final ActionFuture<CreateSnapshotResponse> queued;
        final Set<String> rootBlobsBeforeRelease;
        try {
            repository.awaitParked();
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
        } finally {
            repository.releaseParkedFinalization();
        }
        assertBusy(
            () -> assertTrue(
                internalCluster().getInstance(RepositoriesService.class, clusterManagerNode).repositoriesWithCallsPastBudget().isEmpty()
            )
        );
        awaitNoMoreRunningOperations(clusterManagerNode);

        assertThat("the stopped call must write nothing at the root", rootBlobNames(repoPath), equalTo(rootBlobsBeforeRelease));
        final long generation = repositoryGeneration(internalCluster().clusterService(clusterManagerNode).state());
        assertOneSnapshotNamed(repoPath, generation, RETRIED);
        assertThat(snapshotNames(getRepositoryData(REPO).getSnapshotIds()), equalTo(Set.of("other", QUEUED, RETRIED)));
        assertRestoresDocuments(RETRIED, documents);
        assertRestoresDocuments(QUEUED, documents);
    }

    /**
     * A shallow-copy snapshot is finalized without a budget, even on a repository that hands out an entrypoint: the
     * service finalizes it through the narrow overload, with normal priority. A full-copy snapshot on a repository of the
     * same type goes through the entrypoint.
     */
    public void testShallowCopySnapshotIsNotBudgeted() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        createRepository(
            "shallow-repo",
            FinalizationParkingMockRepositoryPlugin.TYPE,
            Settings.builder()
                .put("location", randomRepoPath())
                .put("test_entrypoint", true)
                .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
        );
        createRepository(
            "full-copy-repo",
            FinalizationParkingMockRepositoryPlugin.TYPE,
            Settings.builder().put("location", randomRepoPath()).put("test_entrypoint", true)
        );
        createIndexWithContent(INDEX);

        createSnapshot("shallow-repo", "shallow", Collections.singletonList(INDEX));
        final FinalizationParkingMockRepository shallow = parkingRepository(clusterManagerNode, "shallow-repo");
        assertThat("a shallow-copy finalization must not use the entrypoint", shallow.entrypointFinalizationsOf("shallow"), equalTo(0));
        assertThat(shallow.narrowFinalizationsOf("shallow"), equalTo(1));
        assertThat(shallow.narrowPriorityOf("shallow"), equalTo(Priority.NORMAL));

        createSnapshot("full-copy-repo", "full-copy", Collections.singletonList(INDEX));
        final FinalizationParkingMockRepository fullCopy = parkingRepository(clusterManagerNode, "full-copy-repo");
        assertThat("a full-copy finalization must use the entrypoint", fullCopy.entrypointFinalizationsOf("full-copy"), equalTo(1));
        assertThat(fullCopy.narrowFinalizationsOf("full-copy"), equalTo(0));
    }

    /**
     * Every snapshot the repository actually recorded must name the index that survived and must not name the one that
     * was deleted while a finalization was parked. Read back through {@code _snapshot} rather than off
     * {@link RepositoryData} alone so the root blob, the {@code snap-} blob and the index lookup all have to agree;
     * {@code RepositoryData}'s own index set is then asserted too, because a dangling {@code IndexId} there is what
     * {@code BlobStoreTestUtil.assertIndexUUIDs} trips on.
     */
    private void assertNoRecordedSnapshotReferences(String deletedIndex, String survivingIndex) {
        final List<SnapshotInfo> recorded = clusterAdmin().prepareGetSnapshots(REPO).get().getSnapshots();
        assertThat("nothing was recorded at all, so this assertion would be vacuous", recorded, not(empty()));
        for (SnapshotInfo snapshotInfo : recorded) {
            assertThat(
                "snapshot [" + snapshotInfo.snapshotId() + "] names an index that was deleted mid-finalization",
                snapshotInfo.indices(),
                not(hasItem(deletedIndex))
            );
            assertThat(
                "the surviving index is missing, so the assertion above holds of nothing",
                snapshotInfo.indices(),
                hasItem(survivingIndex)
            );
        }
        final Set<String> recordedIndices = getRepositoryData(REPO).getIndices().keySet();
        assertThat(recordedIndices, not(hasItem(deletedIndex)));
        assertThat(recordedIndices, hasItem(survivingIndex));
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

    /** Restores the one index {@code snapshotName} holds under a new name and requires exactly {@code expected}. */
    private void assertRestoresDocuments(String snapshotName, Set<String> expected) {
        final String restored = "restored-" + snapshotName;
        final RestoreSnapshotResponse response = clusterAdmin().prepareRestoreSnapshot(REPO, snapshotName)
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
        final FinalizationParkingMockRepository repository = fencingRepository(clusterManagerNode);
        // Read once the snapshot has made the repository strictly consistent and before its delete: this read starts the probe.
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

    private FinalizationParkingMockRepository fencingRepository(String node) {
        final Repository repository = internalCluster().getInstance(RepositoriesService.class, node).repository(REPO);
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
    private ActionFuture<CreateSnapshotResponse> startSnapshot(String viaNode, String snapshotName, String index) {
        return client(viaNode).admin()
            .cluster()
            .prepareCreateSnapshot(REPO, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(index)
            .execute();
    }

    /**
     * Waits, bounded, for the create call to fail with the finalization budget's own exception. Bounded because the
     * defect this catches is a finalization that never returns: unbounded, the run would die on the suite timeout with
     * no attribution, which reads as infrastructure trouble rather than as this test failing.
     * <p>
     * The exception type is asserted by unwrapping rather than by the {@code expectThrows}, which tolerates a wrapping
     * transport exception instead of ruling one out: a repository error or a rejected schedule would satisfy the
     * {@code expectThrows} and then fail the assertions below, which is the point.
     */
    private void awaitAbandonment(ActionFuture<CreateSnapshotResponse> future, String snapshotName) {
        final Exception failure = expectThrows(Exception.class, () -> future.actionGet(TimeValue.timeValueSeconds(60L)));
        final Throwable timeout = ExceptionsHelper.unwrap(failure, OpenSearchTimeoutException.class);
        assertNotNull("expected an OpenSearchTimeoutException, got [" + failure + "]", timeout);
        assertThat(timeout.getMessage(), containsString("finalize snapshot ["));
        assertThat(timeout.getMessage(), containsString(snapshotName));
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
     * The repository must hold no record of the snapshot and must still be on the committed generation it was on before,
     * so the refused finalization committed nothing.
     */
    private void assertNothingRecorded(RepositoryData beforeFinalization, String snapshotName) {
        final RepositoryData afterFinalization = getRepositoryData(REPO);
        assertThat(afterFinalization.getSnapshotIds(), empty());
        assertThat(afterFinalization.getGenId(), equalTo(beforeFinalization.getGenId()));
        expectThrows(SnapshotMissingException.class, () -> clusterAdmin().prepareGetSnapshots(REPO).setSnapshots(snapshotName).get());
    }

    /**
     * A stopped finalization's removal, or the call's own completion, has to hand the repository on, and a snapshot
     * taken afterwards is what proves it did - without this, nothing here distinguishes "the budget worked" from "the
     * budget wedged the repository".
     * <p>
     * Bounded deliberately: with no timeout, a token that was never released shows up as the whole suite hanging,
     * which reads as infrastructure trouble rather than as this test failing.
     */
    private void assertRepositoryStillUsable(String snapshotName) {
        // Restore the default budget first. This snapshot is meant to fail only if the repository is wedged, so it must
        // not also be racing the short budget the test set -- that would reintroduce a timing dependency.
        setIoTimeout(DEFAULT_IO_TIMEOUT);
        final CreateSnapshotResponse response = startFullSnapshot(REPO, snapshotName).actionGet(TimeValue.timeValueSeconds(60L));
        assertThat(response.getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        assertThat("the abandoned snapshot must not have been recorded", getRepositoryData(REPO).getSnapshotIds(), hasSize(1));
    }

    /**
     * A {@link MockRepository} that parks a chosen finalization at the finalization entrypoint, capturing the whole call
     * without entering the repository, so its budget expires while it holds the per-repository operation token and nothing
     * else on the repository is held. An override rather than {@code blockOnceFinalizationStarts}, whose blob-level block
     * also parks the repository-data read a second snapshot needs before it can take its in-progress marker.
     */
    public static class FinalizationParkingMockRepository extends SnapshotFinalizationFencingIT.FencingMockRepository {

        /** Resumes the parked call; given a failure, answers the call with it instead of running it. */
        private final AtomicReference<Consumer<Exception>> parked = new AtomicReference<>();

        private final CountDownLatch parkedSignal = new CountDownLatch(1);

        private volatile boolean parkOnceFinalizationStarts;

        private volatile String parkFinalizationOf;

        private volatile String parkWritingFinalizationOf;

        private volatile boolean claimWritingOnce;

        private final Map<String, AtomicInteger> finalizations = new ConcurrentHashMap<>();

        private final Map<String, String> finalizedUuids = new ConcurrentHashMap<>();

        /** Set by the repository setting {@code test_entrypoint}: hand out a counting entrypoint that runs the narrow body. */
        private final boolean testEntrypoint;

        private final Map<String, AtomicInteger> entrypointFinalizations = new ConcurrentHashMap<>();

        private final Map<String, AtomicInteger> narrowFinalizations = new ConcurrentHashMap<>();

        private final Map<String, Priority> narrowPriorities = new ConcurrentHashMap<>();

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
            testEntrypoint = metadata.settings().getAsBoolean("test_entrypoint", false);
        }

        int entrypointFinalizationsOf(String snapshotName) {
            final AtomicInteger count = entrypointFinalizations.get(snapshotName);
            return count == null ? 0 : count.get();
        }

        int narrowFinalizationsOf(String snapshotName) {
            final AtomicInteger count = narrowFinalizations.get(snapshotName);
            return count == null ? 0 : count.get();
        }

        Priority narrowPriorityOf(String snapshotName) {
            return narrowPriorities.get(snapshotName);
        }

        @Override
        public void finalizeSnapshot(
            ShardGenerations shardGenerations,
            long repositoryStateId,
            Metadata clusterMetadata,
            SnapshotInfo snapshotInfo,
            Version repositoryMetaVersion,
            Function<ClusterState, ClusterState> stateTransformer,
            Priority repositoryUpdatePriority,
            ActionListener<RepositoryData> listener
        ) {
            final String snapshotName = snapshotInfo.snapshotId().getName();
            narrowFinalizations.computeIfAbsent(snapshotName, k -> new AtomicInteger()).incrementAndGet();
            narrowPriorities.put(snapshotName, repositoryUpdatePriority);
            super.finalizeSnapshot(
                shardGenerations,
                repositoryStateId,
                clusterMetadata,
                snapshotInfo,
                repositoryMetaVersion,
                stateTransformer,
                repositoryUpdatePriority,
                listener
            );
        }

        /** Parks the first finalization made through this repository, so its budget expires while it is still held. */
        void parkOnceFinalizationStarts() {
            parkOnceFinalizationStarts = true;
        }

        /** Parks the first finalization of the named snapshot made through this repository. */
        void parkFinalizationOf(String snapshotName) {
            parkFinalizationOf = snapshotName;
        }

        /**
         * Parks the first finalization of the named snapshot after it has started writing the repository generation, as a
         * declarer does before it writes anything that makes the snapshot part of the repository: a budget that expires
         * while it is parked lands in the writing window.
         */
        void parkWritingFinalizationOf(String snapshotName) {
            parkWritingFinalizationOf = snapshotName;
        }

        /**
         * The next finalization starts writing the repository generation before it enters the repository, so a budget
         * that expires while it is blocked further in lands in the writing window.
         */
        void claimWritingOnce() {
            claimWritingOnce = true;
        }

        /** The parked finalization, or null if none has been parked yet or the parked one has already been released. */
        Object parkedFinalization() {
            return parked.get();
        }

        /** Waits, bounded, for a finalization to be parked. */
        void awaitParked() throws InterruptedException {
            assertTrue("no finalization was parked", parkedSignal.await(60L, TimeUnit.SECONDS));
        }

        /** How many finalizations of the named snapshot this repository was handed. */
        int finalizationsOf(String snapshotName) {
            final AtomicInteger count = finalizations.get(snapshotName);
            return count == null ? 0 : count.get();
        }

        /** The uuid of the last finalization of the named snapshot this repository was handed, or null. */
        String finalizedSnapshotUuid(String snapshotName) {
            return finalizedUuids.get(snapshotName);
        }

        /**
         * Resumes the parked finalization, and the real finalization runs. If its budget expired before it started
         * writing the repository generation, it is refused at its first check and releases nothing, because the
         * budget's removal already took its entry out and handed the repository on; otherwise it completes or fails as
         * usual.
         * <p>
         * A no-op when nothing is parked, so it is safe in the {@code finally} the inherited
         * {@code verifyNoLeakedListeners} requires it to be called from - a test that failed before the park happened
         * must report its own failure rather than a null here.
         */
        void releaseParkedFinalization() {
            final Consumer<Exception> resume = parked.getAndSet(null);
            if (resume != null) {
                resume.accept(null);
            }
        }

        /** Answers the parked finalization with a repository failure instead of running it. A no-op when nothing is parked. */
        void failParkedFinalization() {
            final Consumer<Exception> resume = parked.getAndSet(null);
            if (resume != null) {
                resume.accept(new IOException("simulated repository failure"));
            }
        }

        /**
         * The entrypoint the service budgets finalizations through: the inherited one, mapped so that a finalization can
         * be parked and counted, or with {@code test_entrypoint} a counting one that runs the narrow body. Mapped once,
         * so the answer is the same object on every call once present.
         */
        @Override
        public synchronized Optional<AbandonableSnapshotFinalization> abandonableSnapshotFinalization() {
            if (entrypoint.isEmpty()) {
                if (testEntrypoint) {
                    entrypoint = Optional.of(
                        (
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
                            super.finalizeSnapshot(
                                shardGenerations,
                                repositoryStateId,
                                clusterMetadata,
                                snapshotInfo,
                                repositoryMetaVersion,
                                stateTransformer,
                                repositoryUpdatePriority,
                                listener
                            );
                        }
                    );
                } else {
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
                            final String snapshotName = snapshotInfo.snapshotId().getName();
                            entrypointFinalizations.computeIfAbsent(snapshotName, k -> new AtomicInteger()).incrementAndGet();
                            finalizations.computeIfAbsent(snapshotName, k -> new AtomicInteger()).incrementAndGet();
                            finalizedUuids.put(snapshotName, snapshotInfo.snapshotId().getUUID());
                            final Consumer<Exception> call = failure -> {
                                if (failure != null) {
                                    listener.onFailure(failure);
                                    return;
                                }
                                inherited.finalizeSnapshot(
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
                            };
                            if (claimWritingOnce) {
                                claimWritingOnce = false;
                                attempt.startGenerationWrite();
                            }
                            if (snapshotName.equals(parkWritingFinalizationOf)) {
                                parkWritingFinalizationOf = null;
                                attempt.startGenerationWrite();
                                parked.set(call);
                                parkedSignal.countDown();
                                return;
                            }
                            if (parkOnceFinalizationStarts || snapshotName.equals(parkFinalizationOf)) {
                                // Disarmed on the way in and never rearmed: the snapshot queued behind the parked one,
                                // and the trailing snapshot each test ends with, must finalize for real.
                                parkOnceFinalizationStarts = false;
                                parkFinalizationOf = null;
                                parked.set(call);
                                parkedSignal.countDown();
                                return;
                            }
                            call.accept(null);
                        }
                    );
                }
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
