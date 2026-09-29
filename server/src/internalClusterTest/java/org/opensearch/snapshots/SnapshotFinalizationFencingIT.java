/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.SnapshotFinalizationAttempt;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end coverage for the abandonment check in snapshot finalization: a finalization abandoned before it starts
 * writing the repository generation is refused and records nothing.
 * <p>
 * The attempt the service shares with the repository reaches it through the finalization entrypoint a repository
 * hands out, which the service uses with the feature flag on when the repository declares one.
 * {@code BlobStoreRepository}'s narrow overload passes a fresh attempt that nobody gives up. To exercise the check
 * through the real snapshot API, these tests run on the enforcing mock store, which declares the entrypoint once its
 * store is proven, through a repository type of their own that maps the inherited entrypoint, passes the service's
 * attempt on unchanged, and holds or blocks the finalization so that its budget expires at a chosen point.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotFinalizationFencingIT extends AbstractSnapshotIntegTestCase {

    /** Substring of the message thrown by {@code BlobStoreRepository.failIfAbandoned}. */
    private static final String REFUSAL_MESSAGE = "snapshot was abandoned before finalization completed";

    private static final String REPO_NAME = "test-repo";

    private static final String INDEX_NAME = "test-index";

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
        return Collections.singletonList(FencingMockRepositoryPlugin.class);
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING.getKey(), true).build();
    }

    /** Creates the enforcing mock store at {@code location}, gives it a committed generation, and waits until it is proven. */
    private void createProvenRepository(String clusterManagerNode, Path location) throws Exception {
        createRepository(
            REPO_NAME,
            FencingMockRepositoryPlugin.TYPE,
            Settings.builder().put("location", location).put("conditional_writes", true)
        );
        createIndexWithContent(INDEX_NAME);
        createSnapshot(REPO_NAME, "warm-up", Collections.singletonList(INDEX_NAME));
        final FencingMockRepository repository = fencingRepository(clusterManagerNode);
        // Read once the snapshot has made the repository strictly consistent and before its delete: this read starts the probe.
        assertTrue(repository.abandonableSnapshotFinalization().isEmpty());
        assertAcked(startDeleteSnapshot(REPO_NAME, "warm-up").get());
        assertTrue("the store probe did not complete", repository.awaitConditionalWriteProbe(TimeValue.timeValueSeconds(30L)));
        assertTrue("the enforcing mock store must hand out the entrypoint", repository.abandonableSnapshotFinalization().isPresent());
    }

    // These tests depend on something in the inherited teardown that is easy to miss. A refusal after the metadata
    // writes leaves unreferenced metadata blobs, and every refusal leaves shard data the data node had already
    // written. assertRepoConsistency compares root snapshot blobs against the repository data for exact set equality,
    // which those leftovers would break - except that it runs a repository cleanup first, and that reclaims them. So
    // these tests are also standing evidence that cleanup reclaims what a refusal leaves behind. If a future change
    // narrows what cleanup considers stale, these tests fail with a message about set equality that will not obviously
    // point here.

    /**
     * A budget that expires before the finalization reaches the repository, and one that expires while its metadata is
     * being written, each stop it before the generation moves, and each leaves what its point of refusal implies.
     */
    public void testExpiryBeforeOrDuringTheMetadataWriteRefusesBeforeTheGenerationMoves() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final Path repoPath = randomRepoPath();
        createProvenRepository(clusterManagerNode, repoPath);

        final FencingMockRepository repository = fencingRepository(clusterManagerNode);
        for (int checkpoint : new int[] { 1, 2 }) {
            try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(SnapshotsService.class))) {
                final String snapshotName = "abandoned-at-checkpoint-" + checkpoint;
                repository.instrument(snapshotName);
                if (checkpoint == 1) {
                    repository.holdOnceFinalizationStarts();
                } else {
                    repository.blockOnceFinalizationStarts();
                }
                setIoTimeout("5s");

                if (checkpoint == 2) {
                    appender.addExpectation(new RefusalLoggedExpectation(snapshotName));
                }
                final RepositoryData beforeFinalization = getRepositoryData(REPO_NAME);
                final ActionFuture<CreateSnapshotResponse> create = client(clusterManagerNode).admin()
                    .cluster()
                    .prepareCreateSnapshot(REPO_NAME, snapshotName)
                    .setWaitForCompletion(true)
                    .execute();
                try {
                    if (checkpoint == 1) {
                        assertBusy(() -> assertTrue("the finalization was never held", repository.finalizationHeld()));
                    } else {
                        waitForBlock(clusterManagerNode, REPO_NAME, TimeValue.timeValueSeconds(60L));
                    }
                    setIoTimeout("1h");
                    assertStoppedByItsBudget(create, snapshotName);
                } finally {
                    repository.releaseHeldFinalization();
                    unblockNode(REPO_NAME, clusterManagerNode);
                }
                if (checkpoint == 2) {
                    assertBusy(appender::assertAllExpectationsMatched);
                    assertTrue("the attempt must have been given up on", repository.instrumentedAttempt().isAbandoned());
                }
                assertBusy(
                    () -> assertTrue(
                        internalCluster().getInstance(RepositoriesService.class, clusterManagerNode)
                            .repositoriesWithCallsPastBudget()
                            .isEmpty()
                    )
                );

                assertNothingRecorded(beforeFinalization, snapshotName);
                assertRootBlobsConsistentWith(checkpoint, repoPath, repository.fencedSnapshotUuid());
                assertMarkerRemoved();
            }
        }

        // Both stopped calls must have handed the repository on.
        assertRepositoryStillUsable("later-snapshot");
    }

    /** With nothing expiring, a finalization through the entrypoint succeeds, records the snapshot and restores. */
    public void testHealthySnapshotIsUnaffected() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        createProvenRepository(clusterManagerNode, randomRepoPath());

        final FencingMockRepository repository = fencingRepository(clusterManagerNode);
        repository.instrument("healthy-snapshot");

        final RepositoryData beforeSnapshot = getRepositoryData(REPO_NAME);
        final SnapshotInfo snapshotInfo = createFullSnapshot(REPO_NAME, "healthy-snapshot");
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        assertNotNull("the finalization must have gone through the entrypoint", repository.instrumentedAttempt());
        assertFalse("a healthy finalization must never be given up on", repository.instrumentedAttempt().isAbandoned());

        final RepositoryData afterSnapshot = getRepositoryData(REPO_NAME);
        assertThat(afterSnapshot.getSnapshotIds(), hasSize(1));
        assertThat(afterSnapshot.getGenId(), greaterThan(beforeSnapshot.getGenId()));

        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
        final RestoreSnapshotResponse restore = client().admin()
            .cluster()
            .prepareRestoreSnapshot(REPO_NAME, "healthy-snapshot")
            .setWaitForCompletion(true)
            .get();
        assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 1L);
    }

    private void setIoTimeout(String value) {
        assertAcked(
            clusterAdmin().prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("snapshot.repository.io_timeout", value).build())
                .get()
        );
    }

    /** The caller of a finalization stopped by its budget is told, with a timeout, that it will not be recorded. */
    private static void assertStoppedByItsBudget(ActionFuture<CreateSnapshotResponse> create, String snapshotName) {
        final Exception failure = expectThrows(Exception.class, () -> create.actionGet(TimeValue.timeValueSeconds(60L)));
        final Throwable timeout = ExceptionsHelper.unwrap(failure, OpenSearchTimeoutException.class);
        assertNotNull("expected an OpenSearchTimeoutException, got [" + failure + "]", timeout);
        assertThat(timeout.getMessage(), containsString(snapshotName));
        assertThat(timeout.getMessage(), containsString("so it will not be recorded"));
    }

    /** Matches the log line of a stopped finalization's refusal, carrying the repository's refusal as its cause. */
    private static final class RefusalLoggedExpectation implements MockLogAppender.LoggingExpectation {

        private final String snapshotName;

        private volatile boolean seen;

        RefusalLoggedExpectation(String snapshotName) {
            this.snapshotName = snapshotName;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLoggerName().equals(SnapshotsService.class.getCanonicalName()) == false
                || event.getMessage().getFormattedMessage().contains("refused after its caller was told it timed out") == false
                || event.getMessage().getFormattedMessage().contains(snapshotName) == false) {
                return;
            }
            for (Throwable candidate = event.getThrown(); candidate != null; candidate = candidate.getCause()) {
                if (candidate instanceof SnapshotException
                    && candidate.getMessage() != null
                    && candidate.getMessage().contains(REFUSAL_MESSAGE)) {
                    seen = true;
                    return;
                }
            }
        }

        @Override
        public void assertMatched() {
            assertTrue("expected the stopped finalization's refusal to be logged", seen);
        }
    }

    private FencingMockRepository fencingRepository(String node) {
        return (FencingMockRepository) internalCluster().getInstance(RepositoriesService.class, node).repository(REPO_NAME);
    }

    /**
     * The repository must hold no record of the snapshot and must still be on the committed generation it was on before,
     * so the refused finalization committed nothing.
     */
    private void assertNothingRecorded(RepositoryData beforeFinalization, String snapshotName) {
        final RepositoryData afterFinalization = getRepositoryData(REPO_NAME);
        assertThat(afterFinalization.getSnapshotIds(), empty());
        assertThat(afterFinalization.getGenId(), equalTo(beforeFinalization.getGenId()));
        expectThrows(SnapshotMissingException.class, () -> clusterAdmin().prepareGetSnapshots(REPO_NAME).setSnapshots(snapshotName).get());
    }

    /** The in-progress marker is what blocks index deletes and queues the repository, so its removal is the unblock. */
    private void assertMarkerRemoved() throws Exception {
        assertBusy(() -> {
            final ClusterState state = clusterAdmin().prepareState().get().getState();
            assertThat(state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries(), empty());
        });
    }

    /**
     * Corroborates where the stopped call was refused against what is actually in the repository. Refused before it
     * entered the repository ({@code checkpoint} 1), it wrote no root or metadata blob for the snapshot - the data nodes
     * have already written its shard data either way; refused once the metadata writes completed ({@code checkpoint} 2),
     * it leaves the snapshot's metadata blobs unreferenced. Neither refusal commits a generation, which
     * {@link #assertNothingRecorded} asserts; SnapshotFinalizationFencingTests asserts that no root generation blob is
     * written.
     * <p>
     * A refusal at the last check before the generation write leaves the same root blobs as one after the metadata
     * writes, so this does not tell those two apart; SnapshotFinalizationFencingTests pins that check on its own.
     */
    private void assertRootBlobsConsistentWith(int checkpoint, Path repoPath, String snapshotUuid) throws IOException {
        // Without this the blob names below become "meta-null.dat", which no repository ever contains, and the
        // first-checkpoint branch - the one the "nothing was written" claim rests on - would pass for a snapshot that
        // never reached finalization at all.
        assertNotNull("finalization was never entered for the instrumented snapshot", snapshotUuid);
        final Set<String> rootBlobs = rootBlobNames(repoPath);
        final String globalMetadataBlob = "meta-" + snapshotUuid + ".dat";
        final String snapshotInfoBlob = "snap-" + snapshotUuid + ".dat";
        if (checkpoint == 1) {
            assertThat(rootBlobs, not(hasItem(globalMetadataBlob)));
            assertThat(rootBlobs, not(hasItem(snapshotInfoBlob)));
        } else {
            assertThat(rootBlobs, hasItem(globalMetadataBlob));
            assertThat(rootBlobs, hasItem(snapshotInfoBlob));
        }
    }

    /**
     * A stopped finalization's removal has to hand the repository on, and the released call releases nothing a second
     * time; a snapshot taken afterwards is what proves the repository moved on. Bounded deliberately: with no timeout, a
     * token that was never released shows up as the whole suite hanging, which reads as infrastructure trouble rather
     * than as this test failing.
     */
    private void assertRepositoryStillUsable(String snapshotName) {
        final CreateSnapshotResponse response = startFullSnapshot(REPO_NAME, snapshotName).actionGet(TimeValue.timeValueSeconds(60L));
        assertThat(response.getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        assertThat(getRepositoryData(REPO_NAME).getSnapshotIds(), hasSize(1));
    }

    private static Set<String> rootBlobNames(Path repoPath) throws IOException {
        try (Stream<Path> contents = Files.list(repoPath)) {
            return contents.filter(Files::isRegularFile).map(path -> path.getFileName().toString()).collect(Collectors.toSet());
        }
    }

    /**
     * A repository that maps the inherited entrypoint to observe, hold or block the instrumented finalization, passing
     * the service's attempt on unchanged. Doing this at the repository boundary keeps the production classes free of a
     * hook that exists only for tests.
     */
    public static class FencingMockRepository extends MockRepository {

        private final AtomicReference<String> fencedSnapshotUuid = new AtomicReference<>();

        private final AtomicReference<SnapshotFinalizationAttempt> instrumentedAttempt = new AtomicReference<>();

        private final AtomicReference<Runnable> held = new AtomicReference<>();

        private volatile String instrumentedSnapshot;

        private volatile boolean blockOnceFinalizationStarts;

        private volatile boolean holdOnceFinalizationStarts;

        /** Guarded by this. */
        private Optional<AbandonableSnapshotFinalization> fencedEntrypoint = Optional.empty();

        public FencingMockRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings);
        }

        /** Instruments one snapshot's finalization, forgetting any previously instrumented one. */
        void instrument(String snapshotName) {
            instrumentedSnapshot = snapshotName;
            fencedSnapshotUuid.set(null);
            instrumentedAttempt.set(null);
        }

        /** Parks the first blob operation made from inside finalization, so its budget can expire while it is held. */
        void blockOnceFinalizationStarts() {
            blockOnceFinalizationStarts = true;
        }

        /** Holds the instrumented finalization before it enters the repository, so its budget can expire first. */
        void holdOnceFinalizationStarts() {
            holdOnceFinalizationStarts = true;
        }

        /** Resumes a held finalization; a no-op when none is held. */
        void releaseHeldFinalization() {
            final Runnable resume = held.getAndSet(null);
            if (resume != null) {
                resume.run();
            }
        }

        boolean finalizationHeld() {
            return held.get() != null;
        }

        String fencedSnapshotUuid() {
            return fencedSnapshotUuid.get();
        }

        /** The attempt the service handed the instrumented finalization, or null. */
        SnapshotFinalizationAttempt instrumentedAttempt() {
            return instrumentedAttempt.get();
        }

        /**
         * Maps the inherited entrypoint so that the instrumented finalization can be observed, held or blocked. The
         * service's attempt is passed on unchanged. It keeps the first mapped entrypoint instead of reading the inherited
         * answer on every call: a shortcut these tests can take because this repository stays writable, strictly
         * consistent and not shallow once proven, not what the accessor's contract allows.
         */
        @Override
        public synchronized Optional<AbandonableSnapshotFinalization> abandonableSnapshotFinalization() {
            if (fencedEntrypoint.isEmpty()) {
                fencedEntrypoint = super.abandonableSnapshotFinalization().map(
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
                        if (snapshotInfo.snapshotId().getName().equals(instrumentedSnapshot)) {
                            fencedSnapshotUuid.set(snapshotInfo.snapshotId().getUUID());
                            instrumentedAttempt.set(attempt);
                            if (holdOnceFinalizationStarts) {
                                holdOnceFinalizationStarts = false;
                                held.set(call);
                                return;
                            }
                            if (blockOnceFinalizationStarts) {
                                blockOnceFinalizationStarts = false;
                                setBlockOnAnyFiles(true);
                            }
                        }
                        call.run();
                    }
                );
            }
            return fencedEntrypoint;
        }
    }

    /** Registers {@link FencingMockRepository} under a repository type of its own. */
    public static class FencingMockRepositoryPlugin extends MockRepository.Plugin {

        public static final String TYPE = "fencingmock";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                TYPE,
                metadata -> new FencingMockRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings)
            );
        }
    }
}
