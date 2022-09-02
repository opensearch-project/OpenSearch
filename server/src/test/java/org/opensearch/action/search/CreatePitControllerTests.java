/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteClusterConnectionTests;
import org.opensearch.transport.Transport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.action.search.PitTestsUtil.getPitId;

/**
 * Functional tests for various methods in create pit controller. Covers update pit phase specifically since
 * integration tests don't cover it.
 */
public class CreatePitControllerTests extends OpenSearchTestCase {

    DiscoveryNode node1 = null;
    DiscoveryNode node2 = null;
    DiscoveryNode node3 = null;
    String pitId = null;
    TransportSearchAction transportSearchAction = null;
    Task task = null;
    DiscoveryNodes nodes = null;
    NamedWriteableRegistry namedWriteableRegistry = null;
    SearchResponse searchResponse = null;
    ActionListener<CreatePitResponse> createPitListener = null;
    ClusterService clusterServiceMock = null;

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());
    Settings settings = Settings.builder().put("node.name", CreatePitControllerTests.class.getSimpleName()).build();
    NodeClient client = new NodeClient(settings, threadPool);

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(String id, List<DiscoveryNode> knownNodes, Version version) {
        return startTransport(id, knownNodes, version, Settings.EMPTY);
    }

    private MockTransportService startTransport(
        final String id,
        final List<DiscoveryNode> knownNodes,
        final Version version,
        final Settings settings
    ) {
        return RemoteClusterConnectionTests.startTransport(id, knownNodes, version, threadPool, settings);
    }

    @Before
    public void setupData() {
        node1 = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        node2 = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        node3 = new DiscoveryNode("node_3", buildNewFakeTransportAddress(), Version.CURRENT);
        pitId = getPitId();
        namedWriteableRegistry = new NamedWriteableRegistry(
            Arrays.asList(
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, IdsQueryBuilder.NAME, IdsQueryBuilder::new)
            )
        );
        nodes = DiscoveryNodes.builder().add(node1).add(node2).add(node3).build();
        transportSearchAction = mock(TransportSearchAction.class);
        task = new Task(
            randomLong(),
            "transport",
            SearchAction.NAME,
            "description",
            new TaskId(randomLong() + ":" + randomLong()),
            Collections.emptyMap()
        );
        InternalSearchResponse response = new InternalSearchResponse(
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN),
            InternalAggregations.EMPTY,
            null,
            null,
            false,
            null,
            1
        );
        searchResponse = new SearchResponse(
            response,
            null,
            3,
            3,
            0,
            100,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY,
            pitId
        );
        createPitListener = new ActionListener<CreatePitResponse>() {
            @Override
            public void onResponse(CreatePitResponse createPITResponse) {
                assertEquals(3, createPITResponse.getTotalShards());
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        };

        clusterServiceMock = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);

        final Settings keepAliveSettings = Settings.builder().put(CreatePitController.PIT_INIT_KEEP_ALIVE.getKey(), 30000).build();
        when(clusterServiceMock.getSettings()).thenReturn(keepAliveSettings);

        when(state.getMetadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(state.metadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(clusterServiceMock.state()).thenReturn(state);
        when(state.getNodes()).thenReturn(nodes);
    }

    /**
     * Test if transport call for update pit is made to all nodes present as part of PIT ID returned from phase one of create pit
     */
    public void testUpdatePitAfterCreatePitSuccess() throws InterruptedException {
        List<DiscoveryNode> updateNodesInvoked = new CopyOnWriteArrayList<>();
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService cluster1Transport = startTransport("cluster_1_node", knownNodes, Version.CURRENT);
            MockTransportService cluster2Transport = startTransport("cluster_2_node", knownNodes, Version.CURRENT)
        ) {
            knownNodes.add(cluster1Transport.getLocalDiscoNode());
            knownNodes.add(cluster2Transport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    Version.CURRENT,
                    threadPool,
                    null
                )
            ) {
                transportService.start();
                transportService.acceptIncomingRequests();
                SearchTransportService searchTransportService = new SearchTransportService(transportService, null) {
                    @Override
                    public void updatePitContext(
                        Transport.Connection connection,
                        UpdatePitContextRequest request,
                        ActionListener<UpdatePitContextResponse> listener
                    ) {
                        updateNodesInvoked.add(connection.getNode());
                        Thread t = new Thread(() -> listener.onResponse(new UpdatePitContextResponse("pitid", 500000, 500000)));
                        t.start();
                    }

                    /**
                     * Test if cleanup request is called
                     */
                    @Override
                    public void sendFreePITContexts(
                        Transport.Connection connection,
                        List<PitSearchContextIdForNode> contextIds,
                        ActionListener<DeletePitResponse> listener
                    ) {
                        deleteNodesInvoked.add(connection.getNode());
                        Thread t = new Thread(() -> listener.onResponse(new DeletePitResponse(new ArrayList<>())));
                        t.start();
                    }

                    @Override
                    public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                        return new SearchAsyncActionTests.MockConnection(node);
                    }
                };

                CountDownLatch latch = new CountDownLatch(1);

                CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
                request.setIndices(new String[] { "index" });

                PitService pitService = new PitService(clusterServiceMock, searchTransportService, transportService, client);
                CreatePitController controller = new CreatePitController(
                    searchTransportService,
                    clusterServiceMock,
                    transportSearchAction,
                    namedWriteableRegistry,
                    pitService
                );

                ActionListener<CreatePitResponse> updatelistener = new LatchedActionListener<>(new ActionListener<CreatePitResponse>() {
                    @Override
                    public void onResponse(CreatePitResponse createPITResponse) {
                        assertEquals(3, createPITResponse.getTotalShards());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError(e);
                    }
                }, latch);

                StepListener<SearchResponse> createListener = new StepListener<>();
                controller.executeCreatePit(request, task, createListener, updatelistener);
                createListener.onResponse(searchResponse);
                latch.await();
                assertEquals(3, updateNodesInvoked.size());
                assertEquals(0, deleteNodesInvoked.size());
            }
        }
    }

    /**
     * If create phase results in failure, update pit phase should not proceed and propagate the exception
     */
    public void testUpdatePitAfterCreatePitFailure() throws InterruptedException {
        List<DiscoveryNode> updateNodesInvoked = new CopyOnWriteArrayList<>();
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService cluster1Transport = startTransport("cluster_1_node", knownNodes, Version.CURRENT);
            MockTransportService cluster2Transport = startTransport("cluster_2_node", knownNodes, Version.CURRENT)
        ) {
            knownNodes.add(cluster1Transport.getLocalDiscoNode());
            knownNodes.add(cluster2Transport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    Version.CURRENT,
                    threadPool,
                    null
                )
            ) {
                transportService.start();
                transportService.acceptIncomingRequests();
                SearchTransportService searchTransportService = new SearchTransportService(transportService, null) {
                    @Override
                    public void updatePitContext(
                        Transport.Connection connection,
                        UpdatePitContextRequest request,
                        ActionListener<UpdatePitContextResponse> listener
                    ) {
                        updateNodesInvoked.add(connection.getNode());
                        Thread t = new Thread(() -> listener.onResponse(new UpdatePitContextResponse("pitid", 500000, 500000)));
                        t.start();
                    }

                    @Override
                    public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                        return new SearchAsyncActionTests.MockConnection(node);
                    }

                    @Override
                    public void sendFreePITContexts(
                        Transport.Connection connection,
                        List<PitSearchContextIdForNode> contextIds,
                        ActionListener<DeletePitResponse> listener
                    ) {
                        deleteNodesInvoked.add(connection.getNode());
                        Thread t = new Thread(() -> listener.onResponse(new DeletePitResponse(new ArrayList<>())));
                        t.start();
                    }
                };

                CountDownLatch latch = new CountDownLatch(1);

                CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
                request.setIndices(new String[] { "index" });
                PitService pitService = new PitService(clusterServiceMock, searchTransportService, transportService, client);
                CreatePitController controller = new CreatePitController(
                    searchTransportService,
                    clusterServiceMock,
                    transportSearchAction,
                    namedWriteableRegistry,
                    pitService
                );

                ActionListener<CreatePitResponse> updatelistener = new LatchedActionListener<>(new ActionListener<CreatePitResponse>() {
                    @Override
                    public void onResponse(CreatePitResponse createPITResponse) {
                        throw new AssertionError("on response is called");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        assertTrue(e.getCause().getMessage().contains("Exception occurred in phase 1"));
                    }
                }, latch);

                StepListener<SearchResponse> createListener = new StepListener<>();

                controller.executeCreatePit(request, task, createListener, updatelistener);
                createListener.onFailure(new Exception("Exception occurred in phase 1"));
                latch.await();
                assertEquals(0, updateNodesInvoked.size());
                /**
                 * cleanup is not called on create pit phase one failure
                 */
                assertEquals(0, deleteNodesInvoked.size());
            }
        }
    }

    /**
     * Testing that any update pit failures fails the request
     */
    public void testUpdatePitFailureForNodeDrop() throws InterruptedException {
        List<DiscoveryNode> updateNodesInvoked = new CopyOnWriteArrayList<>();
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService cluster1Transport = startTransport("cluster_1_node", knownNodes, Version.CURRENT);
            MockTransportService cluster2Transport = startTransport("cluster_2_node", knownNodes, Version.CURRENT)
        ) {
            knownNodes.add(cluster1Transport.getLocalDiscoNode());
            knownNodes.add(cluster2Transport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    Version.CURRENT,
                    threadPool,
                    null
                )
            ) {
                transportService.start();
                transportService.acceptIncomingRequests();

                SearchTransportService searchTransportService = new SearchTransportService(transportService, null) {
                    @Override
                    public void updatePitContext(
                        Transport.Connection connection,
                        UpdatePitContextRequest request,
                        ActionListener<UpdatePitContextResponse> listener
                    ) {

                        updateNodesInvoked.add(connection.getNode());
                        if (connection.getNode().getId() == "node_3") {
                            Thread t = new Thread(() -> listener.onFailure(new Exception("node 3 down")));
                            t.start();
                        } else {
                            Thread t = new Thread(() -> listener.onResponse(new UpdatePitContextResponse("pitid", 500000, 500000)));
                            t.start();
                        }
                    }

                    @Override
                    public void sendFreePITContexts(
                        Transport.Connection connection,
                        List<PitSearchContextIdForNode> contextIds,
                        ActionListener<DeletePitResponse> listener
                    ) {
                        deleteNodesInvoked.add(connection.getNode());
                        Thread t = new Thread(() -> listener.onResponse(new DeletePitResponse(new ArrayList<>())));
                        t.start();
                    }

                    @Override
                    public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                        return new SearchAsyncActionTests.MockConnection(node);
                    }
                };

                CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
                request.setIndices(new String[] { "index" });
                PitService pitService = new PitService(clusterServiceMock, searchTransportService, transportService, client);
                CreatePitController controller = new CreatePitController(
                    searchTransportService,
                    clusterServiceMock,
                    transportSearchAction,
                    namedWriteableRegistry,
                    pitService
                );

                CountDownLatch latch = new CountDownLatch(1);

                ActionListener<CreatePitResponse> updatelistener = new LatchedActionListener<>(new ActionListener<CreatePitResponse>() {
                    @Override
                    public void onResponse(CreatePitResponse createPITResponse) {
                        throw new AssertionError("response is called");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        assertTrue(e.getMessage().contains("node 3 down"));
                    }
                }, latch);

                StepListener<SearchResponse> createListener = new StepListener<>();
                controller.executeCreatePit(request, task, createListener, updatelistener);
                createListener.onResponse(searchResponse);
                latch.await();
                assertEquals(3, updateNodesInvoked.size());
                /**
                 * check if cleanup is called for all nodes in case of update pit failure
                 */
                assertEquals(3, deleteNodesInvoked.size());
            }
        }
    }

    public void testUpdatePitFailureWhereAllNodesDown() throws InterruptedException {
        List<DiscoveryNode> updateNodesInvoked = new CopyOnWriteArrayList<>();
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService cluster1Transport = startTransport("cluster_1_node", knownNodes, Version.CURRENT);
            MockTransportService cluster2Transport = startTransport("cluster_2_node", knownNodes, Version.CURRENT)
        ) {
            knownNodes.add(cluster1Transport.getLocalDiscoNode());
            knownNodes.add(cluster2Transport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    Version.CURRENT,
                    threadPool,
                    null
                )
            ) {
                transportService.start();
                transportService.acceptIncomingRequests();
                SearchTransportService searchTransportService = new SearchTransportService(transportService, null) {
                    @Override
                    public void updatePitContext(
                        Transport.Connection connection,
                        UpdatePitContextRequest request,
                        ActionListener<UpdatePitContextResponse> listener
                    ) {
                        updateNodesInvoked.add(connection.getNode());
                        Thread t = new Thread(() -> listener.onFailure(new Exception("node down")));
                        t.start();
                    }

                    @Override
                    public void sendFreePITContexts(
                        Transport.Connection connection,
                        List<PitSearchContextIdForNode> contextIds,
                        ActionListener<DeletePitResponse> listener
                    ) {
                        deleteNodesInvoked.add(connection.getNode());
                        Thread t = new Thread(() -> listener.onResponse(new DeletePitResponse(new ArrayList<>())));
                        t.start();
                    }

                    @Override
                    public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                        return new SearchAsyncActionTests.MockConnection(node);
                    }
                };
                CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
                request.setIndices(new String[] { "index" });
                PitService pitService = new PitService(clusterServiceMock, searchTransportService, transportService, client);
                CreatePitController controller = new CreatePitController(
                    searchTransportService,
                    clusterServiceMock,
                    transportSearchAction,
                    namedWriteableRegistry,
                    pitService
                );

                CountDownLatch latch = new CountDownLatch(1);

                ActionListener<CreatePitResponse> updatelistener = new LatchedActionListener<>(new ActionListener<CreatePitResponse>() {
                    @Override
                    public void onResponse(CreatePitResponse createPITResponse) {
                        throw new AssertionError("response is called");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        assertTrue(e.getMessage().contains("node down"));
                    }
                }, latch);

                StepListener<SearchResponse> createListener = new StepListener<>();
                controller.executeCreatePit(request, task, createListener, updatelistener);
                createListener.onResponse(searchResponse);
                latch.await();
                assertEquals(3, updateNodesInvoked.size());
                /**
                 * check if cleanup is called for all nodes in case of update pit failure
                 */
                assertEquals(3, deleteNodesInvoked.size());
            }
        }
    }
}
