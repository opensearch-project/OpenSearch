/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.RepositoryCleanupInProgress;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.util.BytesRefUtils;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.search.SearchHit;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.BlockClusterManagerServiceOnClusterManager;
import org.opensearch.test.junit.annotations.TestLogging;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Integration tests for the time budget applied to the repository call that deletes snapshots.
 * <p>
 * A snapshot delete hands a listener to its repository and waits. If the store degrades and never answers, nothing
 * completes that listener; the removal of the in-progress marker runs only on completion, so the marker stays in
 * cluster state, the caller gets silence rather than an error, queued deletes and creates starve, and repository
 * cleanup is refused for every repository in the cluster. The budget bounds the answer, not the I/O: an expiry before the
 * delete's generation commit answers the caller with a timeout and removes the marker, one after it answers success, and in
 * both cases the queue drains while the repository call keeps running.
 * <p>
 * Until that call does return, the cluster manager still refuses repository cleanup, and modifying or unregistering the
 * repository, although the marker has left the cluster state: it records the call as past its budget, and each test ends by
 * waiting for that record to clear.
 * <p>
 * Every test here needs a repository writer to park at a chosen point, so they use only the stock block points in
 * {@link AbstractSnapshotIntegTestCase}: {@code blockClusterManagerOnWriteIndexFile} parks a writer inside the
 * index-N {@code writeAtomic}, i.e. after the first of the three steps of
 * {@link BlobStoreRepository#writeIndexGen} and therefore <em>before</em> the generation compare-and-set;
 * {@code blockClusterManagerFromDeletingIndexNFile} parks it in the third step, in the cleanup of superseded index-N
 * blobs that follows both the compare-and-set and the {@code index.latest} write. No custom repository subclass is
 * needed, so the base class's {@code nodePlugins()} -- which already registers {@code MockRepository.Plugin} -- is
 * inherited as is. One test also holds the cluster-manager's update thread with the framework's
 * {@code BlockClusterManagerServiceOnClusterManager}, because no stock block point exists between a completed root write
 * and its commit.
 * <p>
 * The repository is created under the mock repository's conditional-write opt-in, and each test waits for the probe of its
 * store before it starts: only a repository whose store has passed that probe hands out the delete entrypoint that is given a
 * time budget.
 * <p>
 * Three properties of this shape govern every test below and are worth stating once.
 * <p>
 * First, which of two overlapping generation writers commits is not a race. The third step refuses to commit unless
 * the pending generation in the cluster state is still its own, and the second writer's first step has already
 * raised that value, so the <em>earlier</em> writer is always the one that throws and the <em>later</em> writer
 * always commits. What the shape cannot control is the order in which released writers do their own blob writes:
 * the block flags are repository-wide and one {@code unblock} clears all of them, so two parked writers resume
 * together. Every assertion here is on state that is the same under either resume order.
 * <p>
 * Second, a writer that loses leaves its index-N blob behind, unreferenced -- not an unfinished write: the winner's
 * third step publishes the same value as both the safe and the pending generation, and the loser changes no cluster
 * state at all. The winner's own cleanup of superseded index-N blobs may already reclaim that orphan; the trailing
 * ordinary snapshot each of those tests ends with reclaims it otherwise, which is what the inherited
 * repository-consistency teardown needs. That snapshot doubles as the proof the repository was left usable.
 * <p>
 * Third, the budget is restored to the setting's default the moment the first writer is parked. The wrapper reads the
 * setting once, when the call it bounds is dispatched, so the parked delete keeps the short budget it was armed with
 * while nothing dispatched afterwards -- a promoted delete, a repository cleanup, the trailing snapshot, the
 * teardown's own delete and cleanup -- is racing a five-second clock it was never meant to be measured against.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotDeleteTimeoutIT extends AbstractSnapshotIntegTestCase {

    private static final String REPO = "test-repo";
    private static final String INDEX = "test-idx";
    private static final String IO_TIMEOUT_KEY = "snapshot.repository.io_timeout";

    /** The setting's floor is one second. Five leaves room for the parked writer to be observed before expiry. */
    private static final TimeValue BUDGET = TimeValue.timeValueSeconds(5L);

    /** The setting's own default, restored as soon as the writer under test is parked. */
    private static final TimeValue DEFAULT_BUDGET = TimeValue.timeValueMinutes(30L);

    /**
     * Bounded deliberately. The defect under test is a repository call that never returns, so an unbounded wait
     * turns a missing budget into the whole suite hanging, which reads as infrastructure trouble rather than as
     * this test failing.
     */
    private static final TimeValue PATIENCE = TimeValue.timeValueSeconds(60L);

    /** One snapshot thread on the cluster manager, which is what that pool is sized to on a node with one or two processors. */
    private static final Settings ONE_SNAPSHOT_THREAD = Settings.builder()
        .put("thread_pool.snapshot.core", 1)
        .put("thread_pool.snapshot.max", 1)
        .build();

    /** The single field {@code createIndexWithContent} writes, reused so the arranged documents need no mapping of their own. */
    private static final String FIELD = "foo";

    /** Documents added beyond the one the shared arrange already wrote. Small deliberately: the point is exactness, not volume. */
    private static final int ARRANGED_DOCS = 5;

    /** Comfortably past the arranged count, so an unexpected extra document arrives as a hit rather than being paged out. */
    private static final int READ_BACK_SIZE = ARRANGED_DOCS + 10;

    private static final String SNAPSHOT_DELETED = "snap-deleted";
    private static final String SNAPSHOT_RELEASED = "snap-released";
    private static final String RESTORED_INDEX = "restored-idx";
    private static final String OTHER_INDEX = "other-idx";

    private Path repoPath;

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING.getKey(), true).build();
    }

    /**
     * One timing dependency, stated rather than left implicit: the queued delete's entry has to reach the cluster
     * state before the parked delete is answered, or there is nothing queued to promote, so the work between arming
     * the budget and awaiting its expiry has to fit inside {@link #BUDGET}. That work is two cluster-state publishes
     * and a handful of cluster-state reads.
     * <p>
     * Once the queue has drained, the committed generation is the later writer's, {@code index.latest} names that same
     * generation, safe and pending agree, and the losing writer changed nothing: the snapshot it was deleting is still in
     * the repository.
     * <p>
     * Which writer loses is not a race. The abandoned writer's new generation is {@code generationBefore + 1}, and by
     * the time it reaches the third step the promoted writer's first step has published
     * {@code generationBefore + 2} as the pending generation; the third step commits only if the pending generation
     * it finds is its own, so the abandoned writer always throws. Resume order decides only which of that step's two
     * failure branches it hits.
     */
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

        // The parked writer is inside the second step, so the first step has already published its pending
        // generation. Asserting that here is what pins the block point: if this writer were parked anywhere before
        // the first step, the promotion assertion further down would prove much less.
        assertEquals(generationBefore + 1, repositoryMetadata().pendingGeneration());
        assertEquals("the parked writer must not have committed a generation", generationBefore, repositoryMetadata().generation());

        final ActionFuture<AcknowledgedResponse> queued = startDeleteSnapshot(REPO, "snap-queued");
        awaitNDeletionsInProgress(2);
        final String parkedUuid = startedDeletionUuid();

        awaitBudgetExpiry(parked, 1);

        // The entry left the cluster state while its own repository call is still parked.
        assertBusy(() -> {
            final List<SnapshotDeletionsInProgress.Entry> entries = deletionEntries();
            assertThat("the abandoned entry must have left the cluster state", entries, hasSize(1));
            assertNotEquals("the entry that left must be the parked one", parkedUuid, entries.get(0).uuid());
            assertEquals("the queued entry must have been promoted", SnapshotDeletionsInProgress.State.STARTED, entries.get(0).state());
        });

        // Promoted is weaker than run. This is the run: the promoted delete reached the repository and published its
        // own pending generation, one past the abandoned writer's.
        assertBusy(() -> assertEquals(generationBefore + 2, repositoryMetadata().pendingGeneration()));
        // And no generation has been committed yet. Do not read this as evidence that the abandoned writer is still
        // parked -- it is not, because the block flags the harness sets are sticky, so the promoted writer parks at
        // the same point and neither of them commits. The assertion is worth keeping for what it does say: the
        // pending generation has advanced twice while the committed one has not moved at all.
        assertEquals("nothing may have committed yet", generationBefore, repositoryMetadata().generation());

        unblockNode(REPO, clusterManager);
        // The promoted delete is the later writer, so it is the one that commits; the abandoned writer's third step
        // finds a pending generation that is not its own and throws, under either resume order.
        assertAcked(queued.actionGet(PATIENCE));
        awaitNoMoreRunningOperations();

        final RepositoryMetadata atRest = repositoryMetadata();
        assertEquals("the later of the two writers must be the one that committed", generationBefore + 2, atRest.generation());
        assertEquals("the loser must not have left an unfinished write behind", atRest.generation(), atRest.pendingGeneration());

        // Read straight from the blob, and before anything else touches the repository: the inherited consistency
        // teardown runs a repository cleanup first, and that cleanup writes a new generation and a new pointer.
        //
        // Safe to read now rather than racing the writers, for the same reason the generation above is: the promoted
        // delete's own listener is completed only after the pointer write, its entry leaves the cluster state after
        // that, and it is no longer on the short budget -- so awaitNoMoreRunningOperations cannot return on an
        // expiry that abandoned it mid-write.
        assertEquals("index.latest must name the generation that committed", generationBefore + 2, indexLatest());

        // The losing writer applied nothing: the snapshot it was deleting is still in the repository data, and only
        // the promoted delete's snapshot is gone.
        assertThat(getRepositoryData(REPO).getSnapshotIds(), hasSize(2));

        assertRepositoryStillUsable("snap-after-drain");
    }

    /**
     * A delete promoted behind a budgeted delete answered after its commit, while that delete's worker is still parked, runs
     * against the current generation.
     */
    public void testPromotedDeleteSurvivesAGenerationAdvancedByTheAbandonedWorker() throws Exception {
        final String clusterManager = startClusterAndRepository();
        createFullSnapshot(REPO, "snap-abandoned");
        createFullSnapshot(REPO, "snap-promoted");
        createFullSnapshot(REPO, "snap-retained");
        final long generationBefore = repositoryMetadata().generation();

        setIoTimeout(BUDGET);
        blockClusterManagerFromDeletingIndexNFile(REPO);
        final ActionFuture<AcknowledgedResponse> abandoned = startDeleteSnapshot(REPO, "snap-abandoned");
        waitForBlock(clusterManager, REPO, PATIENCE);
        setIoTimeout(DEFAULT_BUDGET);

        // This is the whole point of this block point rather than the index-N write one: the parked writer is past
        // its compare-and-set, so the generation the expiry arm captured before this pass ran is no longer current.
        assertEquals(
            "the block point must be past the generation commit for this test to mean anything",
            generationBefore + 1,
            repositoryMetadata().generation()
        );

        final ActionFuture<AcknowledgedResponse> promoted = startDeleteSnapshot(REPO, "snap-promoted");
        awaitNDeletionsInProgress(2);

        // Past its commit, so the delete is answered with success once the budget expires, while the worker is still parked.
        awaitAckedWhileParked(abandoned, clusterManager);

        // Released immediately after the expiry, and before waiting on the promoted delete: the promoted delete's own
        // third step would park at this same block point.
        unblockNode(REPO, clusterManager);

        try {
            assertAcked(promoted.actionGet(PATIENCE));
        } catch (Exception e) {
            // Quoted from BlobStoreRepository#safeRepositoryData, which every delete passes through before it
            // writes anything. It is the failure a promoted delete handed the pre-expiry repository data dies on.
            assertThat(
                "the promoted delete computed its work from the generation the abandoned worker superseded",
                ExceptionsHelper.stackTrace(e),
                not(containsString("concurrent modification of the index-N file, expected current generation ["))
            );
            throw e;
        }

        awaitNoMoreRunningOperations();
        assertThat("both deletes must have been applied", getRepositoryData(REPO).getSnapshotIds(), hasSize(1));
    }

    /**
     * A delete answered after its generation committed, while its worker is still parked in its own cleanup, keeps a
     * repository cleanup, and modifying or unregistering the repository, refused until that worker returns -- although the
     * delete's entry has left the cluster state, which is all the refusals without the feature read. The worker's own sweep then
     * finishes, the cleanup units it had not begun stay undone, and the cleanup admitted afterwards removes what they would
     * have removed.
     * <p>
     * At this park point the worker has no conflicting work left, so this test proves admission and the blob state around
     * it, not that the refusal prevents damage.
     */
    public void testCleanupAndRepositoryChangesWaitForTheGivenUpWorker() throws Exception {
        final String clusterManager = startClusterAndRepository();
        final Map<String, Object> arranged = arrangeVerifiableContents();
        createIndexWithContent(OTHER_INDEX);
        createFullSnapshot(REPO, "snap-abandoned");
        assertAcked(client().admin().indices().prepareDelete(OTHER_INDEX));
        // Holds INDEX only, so OTHER_INDEX is referenced by the abandoned snapshot alone.
        createFullSnapshot(REPO, "snap-retained");
        final IndexId otherIndexId = getRepositoryData(REPO).resolveIndexId(OTHER_INDEX);
        final Path otherIndexPath = repoPath.resolve(BlobStoreRepository.INDICES_DIR).resolve(otherIndexId.getId());
        final Path abandonedSnapshotBlob = rootSnapshotBlob("snap-abandoned");
        final long generationBefore = repositoryMetadata().generation();

        setIoTimeout(BUDGET);
        blockClusterManagerFromDeletingIndexNFile(REPO);
        final ActionFuture<AcknowledgedResponse> abandoned = startDeleteSnapshot(REPO, "snap-abandoned");
        waitForBlock(clusterManager, REPO, PATIENCE);
        setIoTimeout(DEFAULT_BUDGET);
        try {
            assertEquals("the worker must be parked past its commit", generationBefore + 1, repositoryMetadata().generation());
            assertEquals("index.latest must already name the committed generation", generationBefore + 1, indexLatest());
            // Past its commit, so the delete is answered with success once the budget expires, while the worker is still parked.
            assertAcked(abandoned.actionGet(PATIENCE));
            assertBusy(() -> assertThat(deletionEntries(), empty()));

            assertRefused(clusterAdmin().prepareCleanupRepository(REPO).execute(), "a cleanup", "outlived its time budget");
            final Settings current = repositoryMetadata().settings();
            assertRefused(
                clusterAdmin().preparePutRepository(REPO)
                    .setType("mock")
                    .setSettings(Settings.builder().put(current).put("compress", current.getAsBoolean("compress", false) == false))
                    .execute(),
                "a settings change",
                "trying to modify or unregister repository that is currently used"
            );
            assertRefused(
                clusterAdmin().prepareDeleteRepository(REPO).execute(),
                "unregistering the repository",
                "trying to modify or unregister repository that is currently used"
            );
            assertFalse("no cleanup may have started", cleanupInProgress());

            // Parked in its sweep of the index-N blobs its commit superseded, ahead of the units that remove unreferenced data.
            assertTrue(Files.exists(repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + generationBefore)));
            assertTrue(Files.exists(repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + (generationBefore + 1))));
            assertTrue(Files.exists(otherIndexPath));
            assertTrue(Files.exists(abandonedSnapshotBlob));
        } finally {
            unblockNode(REPO, clusterManager);
        }
        awaitGivenUpCallsReturned();

        // Its sweep ran; the units after it read the abandonment and did not begin.
        assertFalse(
            "the parked sweep must have run",
            Files.exists(repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + generationBefore))
        );
        assertTrue("the unreferenced index must be left for a cleanup", Files.exists(otherIndexPath));
        assertTrue("the deleted snapshot's root blob must be left for a cleanup", Files.exists(abandonedSnapshotBlob));
        assertEquals(generationBefore + 1, indexLatest());

        final CleanupRepositoryResponse cleaned = clusterAdmin().prepareCleanupRepository(REPO).get();
        assertThat("the admitted cleanup must remove what the given-up delete left", cleaned.result().blobs(), greaterThan(0L));
        assertFalse(Files.exists(otherIndexPath));
        assertFalse(Files.exists(abandonedSnapshotBlob));
        final long generationAfter = generationBefore + 2;
        assertEquals(Set.of(BlobStoreRepository.INDEX_FILE_PREFIX + generationAfter), rootIndexNBlobs());
        assertEquals(generationAfter, indexLatest());

        final IndexId retainedIndexId = getRepositoryData(REPO).resolveIndexId(INDEX);
        // Resolved through the index's shard path type, which the test framework randomizes per repository.
        try (Stream<Path> shardBlobs = Files.list(repoPath.resolve(resolvePath(retainedIndexId, "0")))) {
            assertTrue("the retained snapshot's shard blobs must be there", shardBlobs.findAny().isPresent());
        }
        assertRestoresTo(REPO, "snap-retained", arranged);
    }

    /**
     * Repository cleanup beside a given-up delete whose worker is parked at its {@code index.latest} write: past its
     * generation commit and past its own abandonment check, so the write goes ahead once the worker is let go. The cleanup is
     * refused while the worker is parked, so the worker writes the pointer alone, and it must name the committed generation
     * both before and after a cleanup admitted once the worker has returned.
     */
    public void testIndexLatestNamesAnExistingGenerationAfterACleanupBesideAParkedPointerWrite() throws Exception {
        final String clusterManager = startClusterAndRepository();
        final Map<String, Object> arranged = arrangeVerifiableContents();
        createFullSnapshot(REPO, SNAPSHOT_DELETED);
        createFullSnapshot(REPO, "snap-retained");
        final long generationBefore = repositoryMetadata().generation();

        blockOnceOnTheNextIndexLatestWrite(clusterManager);
        try {
            setIoTimeout(BUDGET);
            startDeleteSnapshot(REPO, SNAPSHOT_DELETED);
            waitForBlock(clusterManager, REPO, PATIENCE);
            setIoTimeout(DEFAULT_BUDGET);
            assertEquals(
                "the parked worker must already have committed its generation",
                generationBefore + 1,
                repositoryMetadata().generation()
            );

            // Only the budget can take the entry out of the cluster state while the worker is parked.
            assertBusy(() -> assertThat(deletionEntries(), empty()), PATIENCE.seconds(), TimeUnit.SECONDS);

            Exception cleanupRefusal = null;
            try {
                clusterAdmin().prepareCleanupRepository(REPO).get(PATIENCE);
            } catch (Exception e) {
                cleanupRefusal = e;
            }

            unblockNode(REPO, clusterManager);
            awaitGivenUpCallsReturned();

            // Read before anything else writes to the repository root.
            assertIndexLatestNamesTheCommittedGeneration();
            assertNotNull("repository cleanup must be refused while the given-up call has not returned", cleanupRefusal);

            clusterAdmin().prepareCleanupRepository(REPO).get(PATIENCE);
            assertIndexLatestNamesTheCommittedGeneration();
            assertRestoresTo(REPO, "snap-retained", arranged);
        } finally {
            unblockNode(REPO, clusterManager);
        }
    }

    /**
     * A snapshot released by a given-up delete finalizes while the delete's worker is parked at its {@code index.latest}
     * write, past its generation commit and past its own abandonment check. The finalization commits the next generation
     * and removes the superseded root blobs, including the one the parked worker's write names. Once the worker is let go,
     * the pointer must still name a generation whose root blob exists, and both snapshots must restore exactly.
     */
    public void testIndexLatestNamesAnExistingGenerationAfterAReleasedSnapshotFinalizesBesideAParkedPointerWrite() throws Exception {
        final String clusterManager = startClusterAndRepository();
        final Map<String, Object> arranged = arrangeVerifiableContents();
        createFullSnapshot(REPO, SNAPSHOT_DELETED);
        createFullSnapshot(REPO, "snap-retained");
        final long generationBefore = repositoryMetadata().generation();

        blockOnceOnTheNextIndexLatestWrite(clusterManager);
        try {
            setIoTimeout(BUDGET);
            startDeleteSnapshot(REPO, SNAPSHOT_DELETED);
            waitForBlock(clusterManager, REPO, PATIENCE);
            setIoTimeout(DEFAULT_BUDGET);
            assertEquals(
                "the parked worker must already have committed its generation",
                generationBefore + 1,
                repositoryMetadata().generation()
            );
            final ActionFuture<CreateSnapshotResponse> released = queueASnapshotOfTheUnchangedIndex();

            assertThat(
                "the released snapshot must succeed while the worker is parked",
                released.actionGet(PATIENCE).getSnapshotInfo().state(),
                is(SnapshotState.SUCCESS)
            );
            // Precondition, not a regression guard: the overlap this test is about has been built.
            assertEquals(
                "the released snapshot must have committed the next generation",
                generationBefore + 2,
                repositoryMetadata().generation()
            );
            assertFalse(
                "and removed the root blob of the generation the parked worker is about to name",
                Files.exists(repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + (generationBefore + 1)))
            );

            unblockNode(REPO, clusterManager);
            awaitGivenUpCallsReturned();

            assertRestoresTo(REPO, SNAPSHOT_RELEASED, arranged);
            assertRestoresTo(REPO, "snap-retained", arranged);
            assertIndexLatestNamesTheCommittedGeneration();
        } finally {
            unblockNode(REPO, clusterManager);
        }
    }

    /**
     * Parks the cluster manager's next write of the root {@code index.latest} blob, once, at its entry, whether that write is
     * conditional or plain, until the repository is unblocked.
     */
    private void blockOnceOnTheNextIndexLatestWrite(String clusterManager) {
        final MockRepository repository = (MockRepository) internalCluster().getInstance(RepositoriesService.class, clusterManager)
            .repository(REPO);
        repository.setBlockOnceOnConditionalRootWrite(BlobStoreRepository.INDEX_LATEST_BLOB);
        repository.setBlockOnceOnRootWrite(BlobStoreRepository.INDEX_LATEST_BLOB);
    }

    /** Reads the root of the repository directly: the pointer, the committed generation, and which root index-N blobs exist. */
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

    /**
     * Characterisation of a disclosed limit: a budgeted delete whose root write completed but whose commit was declined leaves
     * {@code index-(G+1)} behind, and a repository instance built while that gap is open, for example after a full restart,
     * adopts it, so a delete answered as timed out takes effect.
     * <p>
     * The commit is held, not the write. No stock block point exists after a completed root write -- the next repository
     * I/O is after the commit -- so the cluster-manager's update thread is held with the framework's
     * {@link BlockClusterManagerServiceOnClusterManager} while the writer finishes.
     * <p>
     * One timing dependency beyond the file's shared one: everything from dispatch until the hold engages -- the park, the
     * budget restore's publish, two cluster-state reads and the hold itself -- must fit inside {@link #BUDGET}, or the
     * removal runs unobserved and phase 2 fails.
     */
    public void testAnAbandonedDeleteWithNothingBehindItLeavesAGenerationGap() throws Exception {
        final String clusterManager = startClusterAndRepository();
        final Map<String, Object> arranged = arrangeVerifiableContents();
        final long generationBefore = declineTheCommitOfAGivenUpDelete(clusterManager);

        // ---- Phase 5: reconstruct. A full restart builds every repository instance afresh and keeps the gap as the witness. ----
        internalCluster().fullRestart();
        ensureGreen();
        final RepositoryMetadata afterRestart = repositoryMetadata();
        // Read before anything writes. This pins the clause: the unknown-generation clause would also adopt, but only after
        // erasing exactly this pair.
        assertEquals("the restart must carry the committed generation", generationBefore, afterRestart.generation());
        assertEquals("and the gap", generationBefore + 1, afterRestart.pendingGeneration());

        // ---- Phase 6: the fresh instance adopts the declined generation (through the public repository API, not a flag). ----
        assertEquals(
            "a reconstructed repository must adopt the declined generation",
            generationBefore + 1,
            getRepositoryData(REPO).getGenId()
        );

        // ---- Phase 7: and with it the delete, as an operator sees it. ----
        final List<SnapshotInfo> listed = clusterAdmin().prepareGetSnapshots(REPO).get().getSnapshots();
        assertEquals(
            "the snapshot whose delete was reported as timed out must be gone, and the retained one present",
            Set.of("snap-retained"),
            listed.stream().map(info -> info.snapshotId().getName()).collect(Collectors.toSet())
        );

        // ---- Phase 8: usable, and the first commit builds on the adopted generation. ----
        // This write also moves the pointer and closes the gap, which the inherited consistency teardown needs. The assertions
        // above are taken first, because afterwards nothing is left to see.
        assertRepositoryStillUsable("snap-after-adoption");
        final RepositoryMetadata closed = repositoryMetadata();
        assertEquals("the first write after adoption commits the next generation", generationBefore + 2, closed.generation());
        assertEquals("and closes the gap", closed.generation(), closed.pendingGeneration());
        assertEquals(
            "the adopted delete must persist through the next commit",
            Set.of("snap-retained", "snap-after-adoption"),
            names(getRepositoryData(REPO).getSnapshotIds())
        );
        assertSettledAfterADeclinedCommit(arranged);
    }

    /**
     * The declined commit of {@link #testAnAbandonedDeleteWithNothingBehindItLeavesAGenerationGap}, followed by an ordinary
     * snapshot on the cluster manager that declined it. That repository instance did not start unclean, so it builds on the
     * committed generation: the delete is rolled back, the next generation still holds the snapshot, and the declined blob
     * is removed with the other superseded index-N blobs. Characterises this route without any change to adoption.
     */
    public void testADeclinedDeleteIsRolledBackByTheNextSnapshot() throws Exception {
        final String clusterManager = startClusterAndRepository();
        final Map<String, Object> arranged = arrangeVerifiableContents();
        final long generationBefore = declineTheCommitOfAGivenUpDelete(clusterManager);

        assertRepositoryStillUsable("snap-closing");
        assertEquals(generationBefore + 2, repositoryMetadata().generation());
        assertEquals(
            "the next generation must still hold the snapshot whose delete was declined",
            Set.of("snap-abandoned", "snap-retained", "snap-closing"),
            names(getRepositoryData(REPO).getSnapshotIds())
        );
        assertFalse(
            "the declined blob must be removed with the superseded ones",
            Files.exists(repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + (generationBefore + 1)))
        );
        assertSettledAfterADeclinedCommit(arranged);
    }

    /**
     * The same declined commit, with a repository cleanup first. The declined blob is not stale to the committed
     * generation, so the cleanup removes nothing and writes nothing, and the gap stays open until the next snapshot closes
     * it by rolling the delete back. Characterises this route without any change to adoption.
     */
    public void testACleanupLeavesADeclinedDeleteForTheNextSnapshot() throws Exception {
        final String clusterManager = startClusterAndRepository();
        final Map<String, Object> arranged = arrangeVerifiableContents();
        final long generationBefore = declineTheCommitOfAGivenUpDelete(clusterManager);
        awaitGivenUpCallsReturned();

        assertEquals("the cleanup must find nothing to remove", 0L, clusterAdmin().prepareCleanupRepository(REPO).get().result().blobs());
        final RepositoryMetadata afterCleanup = repositoryMetadata();
        assertEquals("the cleanup must not commit a generation", generationBefore, afterCleanup.generation());
        assertEquals("and the gap must stay open", generationBefore + 1, afterCleanup.pendingGeneration());
        assertTrue(Files.exists(repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + (generationBefore + 1))));

        assertRepositoryStillUsable("snap-closing");
        assertEquals(
            "the next snapshot must roll the declined delete back",
            Set.of("snap-abandoned", "snap-retained", "snap-closing"),
            names(getRepositoryData(REPO).getSnapshotIds())
        );
        assertSettledAfterADeclinedCommit(arranged);
    }

    /**
     * The same declined commit, with the repository unregistered and registered again once the given-up call has returned.
     * The new registration carries no generation, so its instance lists the root blobs and adopts the declined generation,
     * and with it the delete. Characterises this route without any change to adoption.
     */
    public void testRegisteringTheRepositoryAgainAdoptsADeclinedDelete() throws Exception {
        final String clusterManager = startClusterAndRepository();
        final Map<String, Object> arranged = arrangeVerifiableContents();
        final long generationBefore = declineTheCommitOfAGivenUpDelete(clusterManager);
        awaitGivenUpCallsReturned();

        assertAcked(clusterAdmin().prepareDeleteRepository(REPO));
        createRepository(REPO, "mock", Settings.builder().put("location", repoPath).put("conditional_writes", true));
        assertEquals("the registration must adopt the declined generation", generationBefore + 1, getRepositoryData(REPO).getGenId());
        assertEquals(Set.of("snap-retained"), names(getRepositoryData(REPO).getSnapshotIds()));

        assertRepositoryStillUsable("snap-closing");
        assertEquals(Set.of("snap-retained", "snap-closing"), names(getRepositoryData(REPO).getSnapshotIds()));
        assertSettledAfterADeclinedCommit(arranged);
    }

    /**
     * The arrange and residue checks of {@link #testAnAbandonedDeleteWithNothingBehindItLeavesAGenerationGap}: deletes
     * "snap-abandoned" with a short budget, lets its root write complete, holds its commit until the budget has expired,
     * and asserts the residue the declined commit leaves. Returns the generation committed before the delete.
     */
    private long declineTheCommitOfAGivenUpDelete(String clusterManager) throws Exception {
        // ---- Phase 1: arrange. Admit the root write, let it complete, hold its commit. ----
        createFullSnapshot(REPO, "snap-abandoned");
        // Never deleted and never written to again, so it cannot close the gap the way a third snapshot would.
        createFullSnapshot(REPO, "snap-retained");
        final long generationBefore = repositoryMetadata().generation();
        final Path declinedBlob = repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + (generationBefore + 1));

        setIoTimeout(BUDGET);
        blockClusterManagerOnWriteIndexFile(REPO);
        final ActionFuture<AcknowledgedResponse> parked = startDeleteSnapshot(REPO, "snap-abandoned");
        waitForBlock(clusterManager, REPO, PATIENCE);
        setIoTimeout(DEFAULT_BUDGET);

        // Step 1 has published G+1, and it runs only after the pre-commit abandonment check read the attempt live, so the
        // root write is admitted. The writer is parked at the entry of that write, not after it.
        assertEquals(
            "the first step must have published its pending generation",
            generationBefore + 1,
            repositoryMetadata().pendingGeneration()
        );
        assertEquals("and the parked writer must not have committed one", generationBefore, repositoryMetadata().generation());

        // Hold the commit, then let the write finish. Once startDisrupting returns, the cluster-manager's update thread is
        // parked, so the commit the writer submits next can be queued but not executed.
        final BlockClusterManagerServiceOnClusterManager holdCommit = new BlockClusterManagerServiceOnClusterManager(random());
        setDisruptionScheme(holdCommit);
        holdCommit.startDisrupting();
        try {
            unblockNode(REPO, clusterManager);
            // The writer wrote the blob and then submitted the commit, in that order, inside one SNAPSHOT task. The pool going
            // idle therefore happens-after both. This waits for a condition that is already ordered; it does not create order.
            awaitClusterManagerFinishRepoOperations();

            // Precondition, not a regression guard: the complete root write is on disk and its commit is still queued,
            // unexecuted.
            assertTrue("the admitted root write must have completed", Files.exists(declinedBlob));
            assertThat(
                "the commit must still be queued behind the held update thread",
                pendingClusterManagerTasks(clusterManager),
                hasItem("set safe repository generation [" + REPO + "][" + (generationBefore + 1) + "]")
            );

            // ---- Phase 2: the budget's own expiry, while the commit is held. ----
            // The expiry hook marks the attempt abandoned and only then fails the delete. The delete's failure arm submits this
            // removal, so seeing it queued happens-after the abandonment. Nothing else can fail the delete while its commit is
            // held.
            assertBusy(
                () -> assertThat(pendingClusterManagerTasks(clusterManager), hasItem("remove snapshot deletion metadata")),
                PATIENCE.seconds(),
                TimeUnit.SECONDS
            );
        } finally {
            // On every path, so that a failure above does not leave the cluster-manager's update thread held.
            holdCommit.stopDisrupting();
        }
        internalCluster().clearDisruptionScheme();
        // The message is what tells the budget's own expiry apart from this wait giving up on the future.
        awaitBudgetExpiry(parked, 1);

        // ---- Phase 3: the held commit has been processed and declined. ----
        // It was queued, at NORMAL priority, before this LANGUID task, on a single-threaded prioritised executor, so this
        // task completing happens-after the commit's execute. A timed-out wait comes back as a response, not an error.
        assertFalse(
            "the wait for queued cluster-state tasks must not have timed out, or the reads below can precede the decline",
            clusterAdmin().prepareHealth().setWaitForEvents(Priority.LANGUID).get().isTimedOut()
        );

        // ---- Phase 4: the residue, before anything can repair it. ----
        final RepositoryMetadata atRest = repositoryMetadata();
        assertEquals(
            "the fence declined the commit, so the committed generation must be where it started",
            generationBefore,
            atRest.generation()
        );
        assertEquals("and nothing walks the pending generation back", generationBefore + 1, atRest.pendingGeneration());
        // The declined commit never reaches the post-commit tail, which is the only place a delete that can be given up on
        // writes the pointer.
        final long pointer = BytesRefUtils.bytesToLong(
            new BytesRef(Files.readAllBytes(repoPath.resolve(BlobStoreRepository.INDEX_LATEST_BLOB)))
        );
        assertEquals("index.latest must still name the last committed generation", generationBefore, pointer);
        // The orphan is the delete, not just a number: it was serialized from repository data with the snapshot removed.
        final RepositoryData declined;
        try (
            XContentParser parser = MediaTypeRegistry.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, Files.readAllBytes(declinedBlob))
        ) {
            declined = RepositoryData.snapshotsFromXContent(parser, generationBefore + 1);
        }
        assertEquals(
            "the declined root write must already omit the deleted snapshot",
            Set.of("snap-retained"),
            names(declined.getSnapshotIds())
        );
        return generationBefore;
    }

    /**
     * What every reconstruction of a declined commit ends with: index.latest names the committed generation and that
     * generation's blob exists, the retained snapshot restores exactly, and the snapshot whose delete was declined is
     * either gone or restores exactly.
     */
    private void assertSettledAfterADeclinedCommit(Map<String, Object> arranged) throws Exception {
        final long generation = repositoryMetadata().generation();
        assertEquals("index.latest must name the committed generation", generation, indexLatest());
        assertTrue(
            "and that generation's blob must exist",
            Files.exists(repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + generation))
        );
        assertRestoresTo(REPO, "snap-retained", arranged);
        if (names(getRepositoryData(REPO).getSnapshotIds()).contains("snap-abandoned")) {
            assertRestoresTo(REPO, "snap-abandoned", arranged);
        }
    }

    /**
     * The data-safety test. A snapshot reporting success is not
     * evidence that its data survived: the failure this guards against is a snapshot that records success after the blobs
     * it referenced were removed, so a test that reads only the result cannot see it. Restoring is what sees it.
     * <p>
     * The hazard, spelled out because the arrange only makes sense against it. The delete under test removes the only
     * snapshot of the index, and it is parked past its generation commit, so the repository already references no index at
     * all while the worker that removes the blobs of everything it no longer references is still parked. That worker
     * captured the set of index directories it may remove <em>before</em> it wrote anything. A snapshot released by the
     * expiry and started from the pre-expiry repository data resolves the same index identifier, so it writes under a
     * directory in that captured set; it also inherits the same shard generations, so it concludes its segment files
     * already exist and uploads nothing. The released worker then removes that directory. The snapshot finalises
     * successfully with no data underneath it.
     * <p>
     * Two things make the test green. The released snapshot is started from a fresh repository read, which rebinds its
     * index identifier to one the parked worker never captured and its shard generations to ones that make it upload its
     * own copy; and the released worker consults its own abandonment before beginning destructive cleanup. Either alone
     * closes the path, so this test does not attribute the save to one of them -- it asserts the property both exist for.
     * <p>
     * Ordering is chosen, not left to chance: the released snapshot's shard blobs are all written before the parked worker
     * is let go, so the cleanup it resumes into is strictly later than those writes. That is the ordering in which the
     * defect is lethal rather than merely possible.
     */
    public void testASnapshotReleasedByAGivenUpDeleteCanStillBeRestored() throws Exception {
        // Arrange
        final String clusterManager = startClusterAndRepository();
        final Map<String, Object> arranged = arrangeVerifiableContents();
        createFullSnapshot(REPO, SNAPSHOT_DELETED);
        final Path deletedSnapshotBlob = rootSnapshotBlob(SNAPSHOT_DELETED);
        final ActionFuture<AcknowledgedResponse> abandoned = parkTheDeleteOfTheOnlySnapshot(clusterManager);
        final ActionFuture<CreateSnapshotResponse> released = queueASnapshotOfTheUnchangedIndex();

        // Act
        // Past its commit, so the delete is answered with success once the budget expires, while the worker is still parked.
        awaitAckedWhileParked(abandoned, clusterManager);
        awaitShardBlobsWritten(SNAPSHOT_RELEASED);
        assertTrue("the deleted snapshot's root blob must still be there while the worker is parked", Files.exists(deletedSnapshotBlob));
        unblockNode(REPO, clusterManager);
        awaitEveryWorkerFinished();

        // Assert
        assertReleasedSnapshotRestoresTo(released, arranged);
    }

    /**
     * The same property under the other interleaving: the parked worker is let go at the expiry, before the released
     * snapshot has written anything, so its cleanup runs alongside those writes instead of after them.
     * <p>
     * Worth a test of its own because, without the fresh read and the abandonment check, the two orders fail differently. With
     * the cleanup after the writes the released snapshot finalises successfully and cannot be restored; with the cleanup first
     * it cannot read the shard generation it was handed and fails outright. The assertions below refuse both, and neither of
     * them depends on which order a run actually takes -- which it has to not, because one {@code unblock} clears every block
     * flag and the released writers resume together.
     */
    public void testASnapshotReleasedByAGivenUpDeleteCanBeRestoredWhenTheWorkerResumesAtOnce() throws Exception {
        // Arrange
        final String clusterManager = startClusterAndRepository();
        final Map<String, Object> arranged = arrangeVerifiableContents();
        createFullSnapshot(REPO, SNAPSHOT_DELETED);
        final Path deletedSnapshotBlob = rootSnapshotBlob(SNAPSHOT_DELETED);
        final ActionFuture<AcknowledgedResponse> abandoned = parkTheDeleteOfTheOnlySnapshot(clusterManager);
        final ActionFuture<CreateSnapshotResponse> released = queueASnapshotOfTheUnchangedIndex();

        // Act
        // Past its commit, so the delete is answered with success once the budget expires, while the worker is still parked.
        awaitAckedWhileParked(abandoned, clusterManager);
        assertTrue("the deleted snapshot's root blob must still be there while the worker is parked", Files.exists(deletedSnapshotBlob));
        unblockNode(REPO, clusterManager);
        awaitEveryWorkerFinished();

        // Assert
        assertReleasedSnapshotRestoresTo(released, arranged);
    }

    /**
     * The released snapshot starts while the given-up delete's worker is still parked, even when that worker holds the only
     * snapshot-pool thread the cluster manager has -- which is that pool's size on a node with one or two processors. The worker
     * is holding it: the delete's repository body, and the cleanup of superseded index-N blobs it is parked in, run on that pool.
     * Nothing that starts a released snapshot may wait for that thread. If it did, the snapshots the budget released would start
     * only once the stalled call returned, which is the wait the budget exists to end, and the wait for their shard blobs below
     * would time out.
     */
    public void testASnapshotReleasedByAGivenUpDeleteStartsWhileTheOnlySnapshotThreadIsHeld() throws Exception {
        // Arrange
        final String clusterManager = startClusterAndRepository(ONE_SNAPSHOT_THREAD);
        final Map<String, Object> arranged = arrangeVerifiableContents();
        createFullSnapshot(REPO, SNAPSHOT_DELETED);
        final Path deletedSnapshotBlob = rootSnapshotBlob(SNAPSHOT_DELETED);
        final ActionFuture<AcknowledgedResponse> abandoned = parkTheDeleteOfTheOnlySnapshot(clusterManager);
        // The premise, checked rather than assumed: the parked worker holds the cluster manager's only snapshot thread.
        boolean snapshotPoolSeen = false;
        for (ThreadPoolStats.Stats stat : internalCluster().getInstance(ThreadPool.class, clusterManager).stats()) {
            if (ThreadPool.Names.SNAPSHOT.equals(stat.getName())) {
                snapshotPoolSeen = true;
                assertEquals("the parked worker must be holding the only snapshot thread", 1, stat.getActive());
            }
        }
        assertTrue("the cluster manager must report a snapshot pool", snapshotPoolSeen);
        final ActionFuture<CreateSnapshotResponse> released = queueASnapshotOfTheUnchangedIndex();

        // Act
        // Past its commit, so the delete is answered with success once the budget expires, while the worker is still parked.
        awaitAckedWhileParked(abandoned, clusterManager);
        awaitShardBlobsWritten(SNAPSHOT_RELEASED);
        assertTrue("the deleted snapshot's root blob must still be there while the worker is parked", Files.exists(deletedSnapshotBlob));
        unblockNode(REPO, clusterManager);
        awaitEveryWorkerFinished();

        // Assert
        assertReleasedSnapshotRestoresTo(released, arranged);
    }

    /**
     * A delete answered with success after its generation committed, while its cleanup has not finished, warns its caller: the
     * response carries a {@code Warning} header saying that files the deleted snapshots no longer use may remain.
     */
    public void testADeleteAnsweredBeforeItsCleanupFinishedWarnsItsCaller() throws Exception {
        final String clusterManager = startClusterAndRepository();
        createFullSnapshot(REPO, "snap-deleted-early");
        createFullSnapshot(REPO, "snap-retained");

        setIoTimeout(BUDGET);
        blockClusterManagerFromDeletingIndexNFile(REPO);
        // Sent from the data node, so that the answer crosses the transport layer, which carries response headers.
        final String dataNode = internalCluster().getDataNodeNames().iterator().next();
        final ThreadContext callerContext = internalCluster().getInstance(ThreadPool.class, dataNode).getThreadContext();
        final PlainActionFuture<List<String>> warnings = PlainActionFuture.newFuture();
        internalCluster().client(dataNode)
            .admin()
            .cluster()
            .prepareDeleteSnapshot(REPO, "snap-deleted-early")
            .execute(
                ActionListener.wrap(
                    response -> warnings.onResponse(callerContext.getResponseHeaders().getOrDefault("Warning", List.of())),
                    warnings::onFailure
                )
            );
        waitForBlock(clusterManager, REPO, PATIENCE);
        setIoTimeout(DEFAULT_BUDGET);

        final List<String> headers;
        try {
            headers = warnings.actionGet(PATIENCE);
        } finally {
            unblockNode(REPO, clusterManager);
        }
        assertThat(
            "the caller must be told that files the deleted snapshot no longer uses may remain",
            headers,
            hasItem(
                containsString("were deleted from repository [" + REPO + "], but removal of the files they no longer use did not finish")
            )
        );
        awaitNoMoreRunningOperations();
        assertRepositoryStillUsable("snap-after-warning");
    }

    /**
     * A failed reconciliation read is retried while the given-up delete's worker still holds the cluster manager's only
     * snapshot thread. The first read of the reconciliation that is to start the released snapshot fails, and its retry has to run
     * while that worker is parked: queued behind it on the snapshot pool, the retry would start the released snapshot only once the
     * stalled call returned, and the wait for that snapshot's shard blobs below would time out. The repository does not cache its
     * data, so the read goes to the store, which fails it. The test waits on the deletion leaving the cluster state and on the log
     * line of the failed first attempt, not on the delete's answer.
     */
    @TestLogging(value = "org.opensearch.snapshots.SnapshotsService:DEBUG", reason = "the test waits for the failed attempt's log line")
    public void testAFailedReconciliationReadIsRetriedWhileTheOnlySnapshotThreadIsHeld() throws Exception {
        // Arrange
        final String clusterManager = startClusterAndRepository(
            ONE_SNAPSHOT_THREAD,
            Settings.builder().put(BlobStoreRepository.CACHE_REPOSITORY_DATA.getKey(), false).build()
        );
        final ClusterState topology = clusterAdmin().prepareState().get().getState();
        assertFalse("the cluster manager must hold no data", topology.nodes().getClusterManagerNode().isDataNode());
        assertNotEquals(
            "the primary must be on the data node",
            topology.nodes().getClusterManagerNodeId(),
            topology.routingTable().index(INDEX).shard(0).primaryShard().currentNodeId()
        );
        final Map<String, Object> arranged = arrangeVerifiableContents();
        createFullSnapshot(REPO, SNAPSHOT_DELETED);
        parkTheDeleteOfTheOnlySnapshot(clusterManager);
        // The premise, checked rather than assumed: the parked worker holds the cluster manager's only snapshot thread.
        boolean snapshotPoolSeen = false;
        for (ThreadPoolStats.Stats stat : internalCluster().getInstance(ThreadPool.class, clusterManager).stats()) {
            if (ThreadPool.Names.SNAPSHOT.equals(stat.getName())) {
                snapshotPoolSeen = true;
                assertEquals("the parked worker must be holding the only snapshot thread", 1, stat.getActive());
            }
        }
        assertTrue("the cluster manager must report a snapshot pool", snapshotPoolSeen);
        final MockRepository repository = (MockRepository) internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class)
            .repository(REPO);
        final AtomicInteger firstAttemptFailures = new AtomicInteger();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(SnapshotsService.class))) {
            appender.addExpectation(new MockLogAppender.LoggingExpectation() {
                @Override
                public void match(LogEvent event) {
                    if (event.getLevel() == Level.DEBUG
                        && event.getMessage().getFormattedMessage().contains("[" + REPO + "] reconciliation attempt 0 failed")) {
                        firstAttemptFailures.incrementAndGet();
                    }
                }

                @Override
                public void assertMatched() {}
            });
            final ActionFuture<CreateSnapshotResponse> released = queueASnapshotOfTheUnchangedIndex();
            repository.setRandomControlIOExceptionRate(1.0);
            try {
                // Act
                awaitNDeletionsInProgress(0);
                assertBusy(
                    () -> assertTrue("the first reconciliation read must have failed", firstAttemptFailures.get() > 0),
                    PATIENCE.seconds(),
                    TimeUnit.SECONDS
                );
                repository.setRandomControlIOExceptionRate(0.0);
                try {
                    awaitShardBlobsWritten(SNAPSHOT_RELEASED);
                } catch (Exception e) {
                    throw new AssertionError("the retried read must start the released snapshot while the only snapshot thread is held", e);
                }
            } finally {
                repository.setRandomControlIOExceptionRate(0.0);
                unblockNode(REPO, clusterManager);
            }
            awaitEveryWorkerFinished();

            // Assert
            assertEquals("the first reconciliation read must have failed exactly once", 1, firstAttemptFailures.get());
            assertReleasedSnapshotRestoresTo(released, arranged);
        }
    }

    /**
     * A clone still being prepared when a given-up delete is released is kept when the promoted delete's own repository read fails,
     * which fails that delete alone. Once the delete's worker and the clone's preparation are let go, the clone runs
     * to a success whose blobs are all in the repository, and it restores the source's documents. The clone is a snapshot its caller
     * asked for, and nothing it did failed. Its preparation is parked reading its source's index metadata, since its admission reads
     * the source on the snapshot pool and so cannot wait there behind the parked worker. The repository does not cache its data, so
     * the promoted delete's read goes to the store, which fails it.
     */
    public void testACloneBeingPreparedIsKeptWhenAPromotedDeletesOwnReadFails() throws Exception {
        // Arrange
        final String clusterManager = startClusterAndRepository(
            LARGE_SNAPSHOT_POOL_SETTINGS,
            Settings.builder().put(BlobStoreRepository.CACHE_REPOSITORY_DATA.getKey(), false).build()
        );
        final String cloneSource = "snap-clone-source";
        final String promotedTarget = "snap-promoted-delete";
        final String cloneName = "snap-clone";
        final Map<String, Object> arranged = arrangeVerifiableContents();
        createFullSnapshot(REPO, cloneSource);
        createFullSnapshot(REPO, promotedTarget);
        createFullSnapshot(REPO, SNAPSHOT_DELETED);
        final ActionFuture<AcknowledgedResponse> abandoned = parkTheDeleteOfTheOnlySnapshot(clusterManager);
        startDeleteSnapshot(REPO, promotedTarget);
        awaitNDeletionsInProgress(2);
        final MockRepository repository = (MockRepository) internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class)
            .repository(REPO);
        final AtomicBoolean preparationParked = new AtomicBoolean();
        final ActionFuture<AcknowledgedResponse> clone;
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(MockRepository.class))) {
            appender.addExpectation(new MockLogAppender.LoggingExpectation() {
                @Override
                public void match(LogEvent event) {
                    if (event.getMessage()
                        .getFormattedMessage()
                        .contains("blocking I/O operation for file [" + BlobStoreRepository.METADATA_PREFIX)) {
                        preparationParked.set(true);
                    }
                }

                @Override
                public void assertMatched() {}
            });
            repository.setBlockOnReadIndexMeta();
            clone = clusterAdmin().prepareCloneSnapshot(REPO, cloneSource, cloneName).setIndices(INDEX).execute();
            assertBusy(
                () -> assertTrue("the clone's preparation must be parked reading its source's index metadata", preparationParked.get()),
                PATIENCE.seconds(),
                TimeUnit.SECONDS
            );
        }
        assertTrue(
            "the clone is still being prepared",
            clusterAdmin().prepareState()
                .get()
                .getState()
                .custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .entries()
                .stream()
                .anyMatch(entry -> entry.snapshot().getSnapshotId().getName().equals(cloneName) && entry.clones().isEmpty())
        );

        // Act
        repository.setRandomControlIOExceptionRate(1.0);
        try {
            // Parked past its commit, so it is acknowledged when its budget expires.
            awaitAckedWhileParked(abandoned, clusterManager);
            awaitNDeletionsInProgress(0);
        } finally {
            repository.setRandomControlIOExceptionRate(0.0);
        }
        final String kept = "the clone must not be failed by the recovery read";
        try {
            assertFalse(kept, clone.isDone());
        } finally {
            unblockNode(REPO, clusterManager);
        }
        try {
            assertAcked(clone.actionGet(PATIENCE));
        } catch (Exception e) {
            throw new AssertionError(kept, e);
        }
        awaitEveryWorkerFinished();

        // Assert, before any cleanup
        final RepositoryData repositoryData = getRepositoryData(REPO);
        final SnapshotId cloneId = repositoryData.getSnapshotIds()
            .stream()
            .filter(id -> id.getName().equals(cloneName))
            .findFirst()
            .orElseThrow(() -> new AssertionError("the clone must be in the repository"));
        assertEquals("the clone must have succeeded", SnapshotState.SUCCESS, repositoryData.getSnapshotState(cloneId));
        final IndexId indexId = repositoryData.resolveIndexId(INDEX);
        final Path shardPath = repoPath.resolve(resolvePath(indexId, "0"));
        assertTrue(
            "the clone's snapshot blob must be in the repository",
            Files.exists(repoPath.resolve("snap-" + cloneId.getUUID() + ".dat"))
        );
        assertTrue(
            "the clone's shard index blob must be in the repository",
            Files.exists(shardPath.resolve("index-" + repositoryData.shardGenerations().getShardGen(indexId, 0)))
        );
        assertTrue("the root index blob must be in the repository", Files.exists(repoPath.resolve("index-" + repositoryData.getGenId())));
        final RestoreInfo restored = clusterAdmin().prepareRestoreSnapshot(REPO, cloneName)
            .setIndices(INDEX)
            .setRenamePattern(INDEX)
            .setRenameReplacement(RESTORED_INDEX)
            .setWaitForCompletion(true)
            .get()
            .getRestoreInfo();
        assertNotNull("the restore must have produced a result", restored);
        assertEquals("no shard of the clone may have failed to restore", 0, restored.failedShards());
        ensureGreen(RESTORED_INDEX);
        assertEquals("the restored clone must hold exactly the source's documents", arranged, readContents(RESTORED_INDEX));
        assertRepositoryStillUsable("snap-after-clone");
    }

    /**
     * Gives the index contents that can be checked one document at a time rather than counted in aggregate, and returns
     * them keyed by document id.
     * <p>
     * The document the shared arrange already wrote is read back rather than restated here, so this does not carry a
     * copy of a value that lives in the base class and would go stale if it changed there. That one existing document is
     * also why {@code indexRandomDocs} cannot be used: it asserts the index holds exactly the number of documents it
     * itself indexed, which the existing one makes false.
     */
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

    /**
     * Every document of an index as a map of id to field value, so a restored index can be compared against the arranged
     * contents value by value. The total-hit cross-check is what stops a document that was paged out of the response
     * from reading as a document that is missing from the index.
     */
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

    /**
     * Parks the delete of the only snapshot in the repository, with the short budget armed, and returns its future.
     * <p>
     * The block point has to be past the generation commit, which is asserted rather than assumed: only then has the
     * repository stopped referencing the index, and only then is the index's whole blob prefix something the parked
     * worker's own cleanup is entitled to remove. Parked before the commit the worker would instead fail its
     * compare-and-set on resume and never reach that cleanup, and the test would be green without exercising anything.
     */
    private ActionFuture<AcknowledgedResponse> parkTheDeleteOfTheOnlySnapshot(String clusterManager) throws Exception {
        final long generationBefore = repositoryMetadata().generation();

        setIoTimeout(BUDGET);
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

    /**
     * Queues a differently named snapshot of the index the parked delete's snapshot covered, without changing a document
     * of it first. Reuse is what makes the overlap lethal: every blob this snapshot needs already exists under the prefix
     * the parked worker captured, so started from stale state it writes nothing and depends entirely on blobs that
     * worker is about to remove.
     */
    private ActionFuture<CreateSnapshotResponse> queueASnapshotOfTheUnchangedIndex() throws Exception {
        final ActionFuture<CreateSnapshotResponse> released = startFullSnapshot(REPO, SNAPSHOT_RELEASED);
        awaitQueuedShard(SNAPSHOT_RELEASED);
        return released;
    }

    /**
     * Waits for a snapshot to have a shard held back by the delete that owns the repository. Queuing has to be observed
     * before the budget expires, or there is nothing for the expiry to release, so this is the one wait in the arrange
     * that has to fit inside {@link #BUDGET} -- it is a single cluster-state publish.
     */
    private void awaitQueuedShard(String snapshotName) throws Exception {
        awaitClusterState(
            state -> shardsOf(state, snapshotName).stream()
                .anyMatch(status -> status.equals(SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED))
        );
    }

    /**
     * Waits for every shard of a snapshot to have finished writing its blobs.
     * <p>
     * Read from the shard statuses rather than from the entry's own state, because the entry stays in the cluster state
     * until its finalisation completes and that finalisation parks at the same block point as the worker under test.
     * The condition is stable once true: it can only be undone by the entry leaving, which finalisation has to do first.
     */
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

    /**
     * Waits until nothing is still working on the repository, which takes two waits rather than one.
     * <p>
     * The released worker is no longer in the cluster state -- its entry left when the budget expired -- so the ordinary
     * wait for running operations says nothing at all about it. The cluster manager's record of calls past their budget
     * does: it holds the given-up call until that call's own answer arrives. Omitting the second wait would let the
     * restore beat a deletion that had not happened yet, and the test would pass for the wrong reason.
     */
    private void awaitEveryWorkerFinished() throws Exception {
        awaitNoMoreRunningOperations();
        awaitGivenUpCallsReturned();
    }

    /**
     * Waits until no repository call on the cluster manager is recorded as past its budget. Runs after every test as well,
     * ahead of the inherited teardown, whose repository cleanup is refused while such a call is recorded.
     */
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

    /**
     * The assertion the whole shape exists to make: the released snapshot is restorable and restores the documents that
     * were arranged, exactly.
     * <p>
     * Everything here runs before the inherited repository-consistency teardown, which performs a repository cleanup and
     * would reclaim the very blob prefix the existence check below reads.
     */
    private void assertReleasedSnapshotRestoresTo(ActionFuture<CreateSnapshotResponse> released, Map<String, Object> arranged)
        throws Exception {
        // Success first, because it is the assertion this test exists to strengthen rather than replace: a snapshot whose
        // data was removed after it wrote it reports exactly this.
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

        // The point of the test. A restore that succeeds and returns fewer documents, or different ones, fails here.
        assertEquals("the restored index must hold exactly the documents that were arranged", arranged, readContents(RESTORED_INDEX));

        // The same property one level down, and cheap now the rest is standing: the prefix the released snapshot's shard
        // data lives under is still there. Mechanism-agnostic on purpose -- it holds whether the prefix survived because
        // the snapshot was rebound away from the one the released worker captured or because that worker stopped short
        // of its cleanup.
        final IndexId releasedIndexId = getRepositoryData(REPO).resolveIndexId(INDEX);
        assertTrue(
            "the blob prefix holding the released snapshot's shard data must still exist",
            Files.exists(repoPath.resolve(BlobStoreRepository.INDICES_DIR).resolve(releasedIndexId.getId()))
        );

        // Nearly free once the harness is standing: the surviving snapshot is still listed, and the repository is still
        // usable afterwards rather than merely not corrupt.
        final List<SnapshotInfo> listed = clusterAdmin().prepareGetSnapshots(REPO).get().getSnapshots();
        assertThat(listed, hasSize(1));
        assertEquals(SNAPSHOT_RELEASED, listed.get(0).snapshotId().getName());
        assertThat(listed.get(0).state(), is(SnapshotState.SUCCESS));
        assertRepositoryStillUsable("snap-after-release");
    }

    /**
     * Five snapshot threads on the cluster manager, the {@code LARGE_SNAPSHOT_POOL_SETTINGS} the base class offers
     * for exactly this. Not cosmetic: {@code BlobStoreRepository#deleteSnapshotsInternal} dispatches the whole delete
     * onto the snapshot pool, and a writer parked inside the index-N write is holding a thread of that pool. Sized to
     * one -- which the framework does whenever it randomizes the node down to one or two processors -- a second delete
     * would never reach the repository at all while the first is parked, and every assertion here about the second
     * delete's own generation would time out.
     */
    private String startClusterAndRepository() throws Exception {
        return startClusterAndRepository(LARGE_SNAPSHOT_POOL_SETTINGS);
    }

    /** The same, with the cluster manager's node settings, and so its snapshot pool size, chosen by the caller. */
    private String startClusterAndRepository(Settings clusterManagerSettings) throws Exception {
        return startClusterAndRepository(clusterManagerSettings, Settings.EMPTY);
    }

    /** The same, with settings of the caller's added to the repository's. */
    private String startClusterAndRepository(Settings clusterManagerSettings, Settings repositorySettings) throws Exception {
        internalCluster().startClusterManagerOnlyNode(clusterManagerSettings);
        internalCluster().startDataOnlyNode();
        repoPath = randomRepoPath();
        createRepository(
            REPO,
            "mock",
            Settings.builder().put("location", repoPath).put("conditional_writes", true).put(repositorySettings)
        );
        // createIndexWithContent already indexes one document, which is all the tests that only need a snapshot to have
        // shard content to write and delete ask of the index. Do not add indexRandomDocs beside it. That helper asserts
        // the index holds exactly the number of documents it indexed, which the document this one already wrote makes
        // false -- and the assertion reads backwards, because assertDocCount passes the real count as JUnit's
        // "expected", so the failure looks like a lost document rather than an extra one. In-tree callers pair
        // indexRandomDocs with the content-free createIndex instead. The tests that do need contents they can check
        // document by document call arrangeVerifiableContents, which reads this document back rather than assuming it.
        createIndexWithContent(INDEX);
        proveRepository();
        return internalCluster().getClusterManagerName();
    }

    /**
     * Makes the cluster manager's instance of the repository hand out its delete entrypoint. A snapshot and its delete give the
     * repository a committed generation, so it is strictly consistent, and the delete's read of the capability starts the probe
     * of its store; this waits for that probe.
     */
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

    /**
     * Waits, bounded, for a delete to fail, and asserts it failed for the reason under test: that the cause chain
     * <em>contains</em> a timeout naming this repository and this operation, since {@code ExceptionsHelper.unwrap}
     * tolerates a wrapping transport exception rather than ruling one out. The message assertion is what carries the
     * test: a bare {@code expectThrows} would also be satisfied by a repository error, by a rejected schedule, and --
     * against a tree with no budget at all -- by this very wait giving up on a future that is never going to
     * complete, which arrives as an {@link OpenSearchTimeoutException} of its own. The budget value, which the
     * message also carries, is not asserted.
     */
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

    /**
     * Waits, bounded, for a delete whose worker is parked past its generation commit to be acknowledged. If it is not, the
     * worker is let go before the failure propagates, so that the teardown does not wait on a parked worker.
     */
    private void awaitAckedWhileParked(ActionFuture<AcknowledgedResponse> future, String clusterManager) {
        try {
            assertAcked(future.actionGet(PATIENCE));
        } catch (AssertionError | RuntimeException e) {
            unblockNode(REPO, clusterManager);
            throw e;
        }
    }

    /**
     * Waits, bounded, for a request to fail, and asserts that its cause chain carries {@code text}. A request that is
     * admitted instead, or that is still waiting when the bound runs out, fails this with {@code what} named.
     */
    private void assertRefused(ActionFuture<?> future, String what, String text) {
        final Exception failure;
        try {
            future.actionGet(PATIENCE);
            throw new AssertionError(what + " must be refused, but was admitted");
        } catch (Exception e) {
            failure = e;
        }
        assertTrue(
            what + " must be refused with [" + text + "], got [" + failure + "]",
            ExceptionsHelper.unwrapCausesAndSuppressed(failure, t -> String.valueOf(t.getMessage()).contains(text)).isPresent()
        );
    }

    /**
     * Restores the arranged index from the named snapshot under another name, asserts it holds exactly the arranged
     * documents, and deletes the restored copy again.
     */
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

    /** The generation {@code index.latest} names, read straight from the blob. */
    private long indexLatest() throws Exception {
        return BytesRefUtils.bytesToLong(new BytesRef(Files.readAllBytes(repoPath.resolve(BlobStoreRepository.INDEX_LATEST_BLOB))));
    }

    /** The names of the root index-N blobs. */
    private Set<String> rootIndexNBlobs() throws Exception {
        try (Stream<Path> blobs = Files.list(repoPath)) {
            return blobs.map(blob -> blob.getFileName().toString())
                .filter(name -> name.startsWith(BlobStoreRepository.INDEX_FILE_PREFIX))
                .collect(Collectors.toSet());
        }
    }

    /** The root blob of the named snapshot, resolved from the repository data while the snapshot still exists. */
    private Path rootSnapshotBlob(String snapshotName) {
        final SnapshotId snapshotId = getRepositoryData(REPO).getSnapshotIds()
            .stream()
            .filter(id -> id.getName().equals(snapshotName))
            .findFirst()
            .orElseThrow();
        return repoPath.resolve(BlobStoreRepository.SNAPSHOT_FORMAT.blobName(snapshotId.getUUID()));
    }

    /**
     * One ordinary successful snapshot. It proves the budget wound the operation down rather than wedging the
     * repository, and its generation write reclaims the index-N blob a losing writer left behind, which the
     * inherited repository-consistency teardown needs. The budget was restored to the default when the writer under
     * test parked, so this snapshot fails only if the repository is wedged, not because it is racing a short budget.
     */
    private void assertRepositoryStillUsable(String snapshotName) {
        assertThat(createFullSnapshot(REPO, snapshotName).state(), is(SnapshotState.SUCCESS));
    }

    private void awaitNDeletionsInProgress(int count) throws Exception {
        awaitClusterState(
            state -> state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY).getEntries().size() == count
        );
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

    private boolean cleanupInProgress() {
        return clusterAdmin().prepareState()
            .get()
            .getState()
            .custom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY)
            .hasCleanupInProgress();
    }

    /** Sources of the tasks queued or running on a node's cluster-manager service, read in-process: no task needed to read them. */
    private Set<String> pendingClusterManagerTasks(String node) {
        return internalCluster().getInstance(ClusterService.class, node)
            .getClusterManagerService()
            .pendingTasks()
            .stream()
            .map(task -> task.getSource().string())
            .collect(Collectors.toSet());
    }

    private static Set<String> names(Collection<SnapshotId> snapshotIds) {
        return snapshotIds.stream().map(SnapshotId::getName).collect(Collectors.toSet());
    }

    /**
     * Read from the cluster state rather than from the repository, so that counting generation commits costs no blob
     * I/O and cannot itself be affected by a parked writer.
     */
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
