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

    public void testDynamicIoTimeoutTakesEffectWithoutRestartAndALateRepositoryDataCompletionAfterTimeoutIsDropped() throws Exception {
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

        assumeTrue("test only works with assertions enabled", Assertions.ENABLED);
        assertAcked(deleteSnapshot(REPO, "snap-after-dynamic").actionGet());
        repository.startParkingFinalizationReads();
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

    private void awaitBudgetExpiry(ActionFuture<CreateSnapshotResponse> future) {
        final Exception failure = expectThrows(Exception.class, () -> future.actionGet(TimeValue.timeValueSeconds(60L)));
        final Throwable timeout = ExceptionsHelper.unwrap(failure, OpenSearchTimeoutException.class);
        assertNotNull("expected an OpenSearchTimeoutException, got [" + failure + "]", timeout);
        assertThat(timeout.getMessage(), containsString("get repository data for [" + REPO + "]"));
        assertThat(timeout.getMessage(), containsString("timed out after"));
    }

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
            parked.set(null);
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
