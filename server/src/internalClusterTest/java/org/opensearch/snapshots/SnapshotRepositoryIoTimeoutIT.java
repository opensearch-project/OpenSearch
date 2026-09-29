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
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.Assertions;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for the per-repository I/O time budget applied to {@code endSnapshot}'s repository-data read.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotRepositoryIoTimeoutIT extends AbstractSnapshotIntegTestCase {

    private static final String REPO = "test-repo";
    private static final String INDEX = "test-idx";
    private static final String IO_TIMEOUT_KEY = "snapshot.repository.io_timeout";

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING.getKey(), true).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ParkingMockRepositoryPlugin.class);
    }

    /**
     * Catches a late completion of the abandoned read being delivered instead of dropped. Delivering it re-enters
     * finalizeSnapshotEntry, whose first line asserts the per-repository token is still held: with assertions on that
     * is an AssertionError, with assertions off it is a second cluster-manager-side writer on a repository whose token
     * was already released. The second snapshot succeeding is the positive proof the token was released rather than
     * left held. A double release would be caught by leaveRepoLoop's assert, under -ea only.
     */
    public void testLateRepositoryDataCompletionAfterTimeoutIsDropped() throws Exception {
        assumeTrue("test only works with assertions enabled", Assertions.ENABLED);
        final ParkingMockRepository repository = startClusterAndParkFinalizationReads();
        try {
            setIoTimeout("5s");
            final ActionFuture<CreateSnapshotResponse> future = startSnapshot("snap-dropped");
            assertBusy(() -> assertNotNull("finalization read never parked", repository.parkedListener()));
            awaitBudgetExpiry(future);
        } finally {
            repository.stopParkingFinalizationReads();
        }

        final ActionListener<RepositoryData> parked = repository.parkedListener();
        assertBusy(() -> assertTrue(clusterManagerSnapshotsService().assertAllListenersResolved()));
        parked.onResponse(getRepositoryData(REPO));

        final List<SnapshotInfo> listed = clusterAdmin().prepareGetSnapshots(REPO).setIgnoreUnavailable(true).get().getSnapshots();
        assertThat("the dropped read must not have recorded the snapshot", listed, empty());
        assertThat(snapshotsInProgressEntries(), empty());

        assertRepositoryStillUsable("snap-after-drop");
    }

    /**
     * Catches a call site that reads a budget fixed at construction rather than the live field the update consumer writes: the
     * node starts on the 30m default, so a construction-time capture would park the finalization for half an hour.
     */
    public void testDynamicIoTimeoutTakesEffectWithoutRestart() throws Exception {
        final ParkingMockRepository repository = startClusterAndParkFinalizationReads();
        assertEquals(
            "the node must start on the setting's default, so 1s is unambiguously the dynamically applied value",
            TimeValue.timeValueMinutes(30),
            clusterManagerSnapshotsService().repositoryIoTimeout()
        );

        setIoTimeout("1s");
        assertBusy(() -> assertEquals(TimeValue.timeValueSeconds(1), clusterManagerSnapshotsService().repositoryIoTimeout()));

        try {
            final ActionFuture<CreateSnapshotResponse> future = startSnapshot("snap-dynamic");
            assertBusy(() -> assertNotNull("finalization read never parked", repository.parkedListener()));

            awaitBudgetExpiry(future);
            assertThat("the expiry removed the in-progress entry", snapshotsInProgressEntries(), empty());
        } finally {
            repository.stopParkingFinalizationReads();
        }
        assertRepositoryStillUsable("snap-after-dynamic");
    }

    /**
     * Waits, bounded, for the create-snapshot call to fail, and asserts it failed for the reason under test: that the
     * cause chain <em>contains</em> an {@link OpenSearchTimeoutException}, since {@code ExceptionsHelper.unwrap}
     * tolerates a wrapping transport exception rather than ruling one out, and that its message carries the repository
     * name and the literal {@code timed out after}. The budget value, which the message also carries, is not asserted.
     * <p>
     * Bounded because the defect these tests catch is a repository read that never returns. Unbounded, removing the
     * wrap would block here forever and the run would die on the 20-minute suite timeout with no attribution, which
     * reads as infrastructure trouble rather than as this test failing.
     */
    private void awaitBudgetExpiry(ActionFuture<CreateSnapshotResponse> future) {
        final Exception failure = expectThrows(Exception.class, () -> future.actionGet(TimeValue.timeValueSeconds(60L)));
        final Throwable timeout = ExceptionsHelper.unwrap(failure, OpenSearchTimeoutException.class);
        assertNotNull("expected an OpenSearchTimeoutException, got [" + failure + "]", timeout);
        assertThat(timeout.getMessage(), containsString("get repository data for [" + REPO + "]"));
        assertThat(timeout.getMessage(), containsString("timed out after"));
    }

    /**
     * A timeout has to wind down through the service and release the per-repository operation token, and a snapshot
     * taken afterwards is what proves it did -- without this, nothing here distinguishes "the budget worked" from "the
     * budget wedged the repository".
     * <p>
     * It is also what gives the repository a root blob. These tests time out a snapshot and never complete one, so the
     * inherited {@code assertRepoConsistency} teardown would otherwise throw on a missing {@code index.latest}
     * before reaching any of its real checks -- and its {@code prepareCleanupRepository}
     * step cannot help, because there is no stale state to reclaim, there is no state at all.
     * <p>
     * Bounded deliberately: with no timeout, a token that was never released shows up as the whole suite hanging, which
     * reads as infrastructure trouble rather than as this test failing.
     */
    private void assertRepositoryStillUsable(String snapshotName) {
        setIoTimeout(TimeValue.timeValueMinutes(30).getStringRep());
        final CreateSnapshotResponse response = startFullSnapshot(REPO, snapshotName).actionGet(TimeValue.timeValueSeconds(60L));
        assertThat(response.getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        assertThat("the abandoned snapshot must not have been recorded", getRepositoryData(REPO).getSnapshotIds(), hasSize(1));
    }

    private ParkingMockRepository startClusterAndParkFinalizationReads() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        createRepository(REPO, ParkingMockRepositoryPlugin.TYPE);
        assertAcked(prepareCreate(INDEX, 0, indexSettingsNoReplicas(1)));
        ensureGreen();
        indexRandomDocs(INDEX, randomIntBetween(10, 50));
        final ParkingMockRepository repository = clusterManagerRepository();
        repository.startParkingFinalizationReads();
        return repository;
    }

    private void setIoTimeout(String value) {
        assertAcked(
            clusterAdmin().prepareUpdateSettings().setPersistentSettings(Settings.builder().put(IO_TIMEOUT_KEY, value).build()).get()
        );
    }

    private ActionFuture<CreateSnapshotResponse> startSnapshot(String snapshotName) {
        return clusterAdmin().prepareCreateSnapshot(REPO, snapshotName).setWaitForCompletion(true).setIndices(INDEX).execute();
    }

    private ParkingMockRepository clusterManagerRepository() {
        final String clusterManager = internalCluster().getClusterManagerName();
        final Repository repository = internalCluster().getInstance(RepositoriesService.class, clusterManager).repository(REPO);
        assertThat(repository, instanceOf(ParkingMockRepository.class));
        return (ParkingMockRepository) repository;
    }

    private SnapshotsService clusterManagerSnapshotsService() {
        return internalCluster().getInstance(SnapshotsService.class, internalCluster().getClusterManagerName());
    }

    private List<SnapshotsInProgress.Entry> snapshotsInProgressEntries() {
        final SnapshotsInProgress inProgress = clusterAdmin().prepareState()
            .get()
            .getState()
            .custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        return inProgress.entries();
    }

    /**
     * A repository that parks {@code endSnapshot}'s repository-data read: it captures the listener and returns without
     * completing it and without issuing any I/O.
     * <p>
     * An override rather than {@code blockNodeOnAnyFiles} because {@code BlobStoreRepository.getRepositoryData} has a
     * cache fast path that completes the listener synchronously with no blob I/O, so whether a blob-level block parks
     * <em>this</em> read depends on {@code bestEffortConsistency}, which for a freshly created test repository may
     * still be true. The override removes the question.
     */
    public static class ParkingMockRepository extends MockRepository {

        private final AtomicReference<ActionListener<RepositoryData>> parked = new AtomicReference<>();
        private volatile boolean parkFinalizationReads;

        public ParkingMockRepository(
            final RepositoryMetadata metadata,
            final Environment environment,
            final NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings);
        }

        void startParkingFinalizationReads() {
            parkFinalizationReads = true;
        }

        void stopParkingFinalizationReads() {
            parkFinalizationReads = false;
        }

        ActionListener<RepositoryData> parkedListener() {
            return parked.get();
        }

        @Override
        public void getRepositoryData(ActionListener<RepositoryData> listener) {
            if (parkFinalizationReads && hasSnapshotInProgress()) {
                parked.set(listener);
                return;
            }
            super.getRepositoryData(listener);
        }

        private boolean hasSnapshotInProgress() {
            final SnapshotsInProgress inProgress = clusterService.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            final String name = getMetadata().name();
            return inProgress.entries().stream().anyMatch(entry -> entry.repository().equals(name));
        }
    }

    /** A plugin that registers {@link ParkingMockRepository} under its own repository type. */
    public static class ParkingMockRepositoryPlugin extends MockRepository.Plugin {

        public static final String TYPE = "parkingmock";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                TYPE,
                metadata -> new ParkingMockRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings)
            );
        }
    }
}
