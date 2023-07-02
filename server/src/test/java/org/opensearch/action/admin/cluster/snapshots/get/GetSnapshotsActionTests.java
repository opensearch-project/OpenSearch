/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.snapshots.get;

import org.opensearch.action.ActionListener;
import org.opensearch.action.IndicesRequest;

import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.Index;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoriesServiceTests;
import org.opensearch.repositories.Repository;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.action.ActionRequestValidationException;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;

public class GetSnapshotsActionTests extends OpenSearchTestCase {
    private TransportService transportService;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private RepositoriesService repositoriesService;
    private final String[] snapshotsName1 = { "test_snapshot" };
    private final String[] currSnapshotOnly = { GetSnapshotsRequest.CURRENT_SNAPSHOT };
    private final String[] snapshotsName2 = { "test_snapshot1", "test_snapshot2" };
    private final String repoName = "test_repo";
    private final String indexName = "test_index";
    private static final String repoType = "internal";

    private GetSnapshotsActionTests.TestTransportGetSnapshotsAction getSnapshotsAction;

    public class TestTransportGetSnapshotsAction extends TransportGetSnapshotsAction {
        TestTransportGetSnapshotsAction() {
            super(
                GetSnapshotsActionTests.this.transportService,
                GetSnapshotsActionTests.this.clusterService,
                GetSnapshotsActionTests.this.threadPool,
                repositoriesService,
                new ActionFilters(Collections.emptySet()),
                new GetSnapshotsActionTests.Resolver()
            );
        }

        @Override
        protected void clusterManagerOperation(
            GetSnapshotsRequest request,
            ClusterState state,
            ActionListener<GetSnapshotsResponse> listener
        ) {
            ClusterState stateWithIndex = ClusterStateCreationUtils.state(indexName, 1, 1);
            super.clusterManagerOperation(request, stateWithIndex, listener);
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        threadPool = new TestThreadPool("GetSnapshotsActionTests");
        clusterService = createClusterService(threadPool);
        CapturingTransport capturingTransport = new CapturingTransport();
        transportService = capturingTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );

        Map<String, Repository.Factory> typesRegistry = GetSnapshotsActionTests.getTypesRegistry(threadPool);
        repositoriesService = new RepositoriesService(
            Settings.EMPTY,
            clusterService,
            transportService,
            typesRegistry,
            typesRegistry,
            threadPool
        );
        repositoriesService.start();
        transportService.start();
        transportService.acceptIncomingRequests();
        getSnapshotsAction = new GetSnapshotsActionTests.TestTransportGetSnapshotsAction();
    }

    private static Map<String, Repository.Factory> getTypesRegistry(ThreadPool threadPool) {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        final ClusterService clusterServiceMock = mock(ClusterService.class);
        when(clusterServiceMock.getClusterApplierService()).thenReturn(clusterApplierService);
        Map<String, Repository.Factory> typesRegistry = Map.of(
            RepositoriesServiceTests.TestRepository.TYPE,
            RepositoriesServiceTests.TestRepository::new,
            RepositoriesServiceTests.MeteredRepositoryTypeA.TYPE,
            metadata -> new RepositoriesServiceTests.MeteredRepositoryTypeA(metadata, clusterServiceMock),
            RepositoriesServiceTests.MeteredRepositoryTypeB.TYPE,
            metadata -> new RepositoriesServiceTests.MeteredRepositoryTypeB(metadata, clusterServiceMock)
        );

        return typesRegistry;
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        clusterService.close();
        super.tearDown();
    }

    public void testRepoMissing() {
        GetSnapshotsRequest repoMissingRequest = new GetSnapshotsRequest();
        getSnapshotsAction.execute(null, repoMissingRequest, ActionListener.wrap(repoMissingResponse -> {
            assertThrows(ActionRequestValidationException.class, () -> { repoMissingResponse.getSnapshots(); });
        }, exception -> { assertTrue(exception.getMessage().contains("repository is missing")); }));

        repoMissingRequest = new GetSnapshotsRequest().snapshots(snapshotsName1);

        getSnapshotsAction.execute(null, repoMissingRequest, ActionListener.wrap(repoMissingResponse -> {
            assertThrows(ActionRequestValidationException.class, () -> { repoMissingResponse.getSnapshots(); });
        }, exception -> { assertTrue(exception.getMessage().contains("repository is missing")); }));

        repoMissingRequest = new GetSnapshotsRequest().ignoreUnavailable(true);

        getSnapshotsAction.execute(null, repoMissingRequest, ActionListener.wrap(repoMissingResponse -> {
            assertThrows(ActionRequestValidationException.class, () -> { repoMissingResponse.getSnapshots(); });
        }, exception -> { assertTrue(exception.getMessage().contains("repository is missing")); }));

        repoMissingRequest = new GetSnapshotsRequest().verbose(true);

        getSnapshotsAction.execute(null, repoMissingRequest, ActionListener.wrap(repoMissingResponse -> {
            assertThrows(ActionRequestValidationException.class, () -> { repoMissingResponse.getSnapshots(); });
        }, exception -> { assertTrue(exception.getMessage().contains("repository is missing")); }));

        repoMissingRequest = new GetSnapshotsRequest().verbose(false);

        getSnapshotsAction.execute(null, repoMissingRequest, ActionListener.wrap(repoMissingResponse -> {
            assertThrows(ActionRequestValidationException.class, () -> { repoMissingResponse.getSnapshots(); });
        }, exception -> { assertTrue(exception.getMessage().contains("repository is missing")); }));
    }

    public void testCurrentRepoSnapshot() {
        final Task task = createTask();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(repoName, repoType, Settings.EMPTY);
        Repository repository = repositoriesService.createRepository(repositoryMetadata, repositoriesService.typesRegistry());
        ;
        Map<String, Repository> repositoryMap = new HashMap<>();
        repositoryMap.put(repoName, repository);
        repositoriesService = repositoriesService.repositories(repositoryMap);
        assertEquals(repositoryMap, repositoriesService.repositories());

        GetSnapshotsRequest repoSnapshotRequest = new GetSnapshotsRequest().repository(repoName)
            .snapshots(currSnapshotOnly)
            .ignoreUnavailable(true);
        getSnapshotsAction.execute(null, repoSnapshotRequest, ActionListener.wrap(repoSnapshotResponse -> {
            assertNotNull("snapshots should be set as we are checking the current snapshot", repoSnapshotResponse.getSnapshots());
        }, exception -> { throw new AssertionError(exception); }));

        repoSnapshotRequest = new GetSnapshotsRequest().repository(repoName)
            .snapshots(currSnapshotOnly)
            .ignoreUnavailable(true)
            .verbose(false);
        getSnapshotsAction.execute(null, repoSnapshotRequest, ActionListener.wrap(repoSnapshotResponse -> {
            assertNotNull("snapshots should be set as we are checking the current snapshot", repoSnapshotResponse.getSnapshots());
        }, exception -> { throw new AssertionError(exception); }));

        repoSnapshotRequest = new GetSnapshotsRequest().repository(repoName)
            .snapshots(currSnapshotOnly)
            .ignoreUnavailable(false)
            .verbose(false);
        getSnapshotsAction.execute(null, repoSnapshotRequest, ActionListener.wrap(repoSnapshotResponse -> {
            assertNotNull("snapshots should be set as we are checking the current snapshot", repoSnapshotResponse.getSnapshots());
        }, exception -> { throw new AssertionError(exception); }));

        repoSnapshotRequest = new GetSnapshotsRequest().repository(repoName)
            .snapshots(currSnapshotOnly)
            .ignoreUnavailable(true)
            .verbose(false);
        getSnapshotsAction.execute(task, repoSnapshotRequest, ActionListener.wrap(repoSnapshotResponse -> {
            assertNotNull("snapshots should be set as we are checking the current snapshot", repoSnapshotResponse.getSnapshots());
        }, exception -> { throw new AssertionError(exception); }));
    }

    private static Task createTask() {
        return new Task(
            randomLong(),
            "transport",
            MultiGetAction.NAME,
            "description",
            new TaskId(randomLong() + ":" + randomLong()),
            Collections.emptyMap()
        );
    }

    static class Resolver extends IndexNameExpressionResolver {
        Resolver() {
            super(new ThreadContext(Settings.EMPTY));
        }

        @Override
        public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
            return request.indices();
        }

        @Override
        public Index[] concreteIndices(ClusterState state, IndicesRequest request) {
            Index[] out = new Index[request.indices().length];
            for (int x = 0; x < out.length; x++) {
                out[x] = new Index(request.indices()[x], "_na_");
            }
            return out;
        }
    }

}
