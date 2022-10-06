/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.action.search;

import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteClusterConnectionTests;
import org.opensearch.transport.Transport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.action.search.PitTestsUtil.getPitId;
import static org.opensearch.action.support.PlainActionFuture.newFuture;

/**
 * Functional tests for transport delete pit action
 */
public class TransportDeletePitActionTests extends OpenSearchTestCase {
    DiscoveryNode node1 = null;
    DiscoveryNode node2 = null;
    DiscoveryNode node3 = null;
    String pitId = null;
    TransportSearchAction transportSearchAction = null;
    Task task = null;
    DiscoveryNodes nodes = null;
    NamedWriteableRegistry namedWriteableRegistry = null;
    ClusterService clusterServiceMock = null;
    Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
    private ThreadPool threadPool = new ThreadPool(settings);
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
    public void testDeletePitSuccess() throws InterruptedException, ExecutionException {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
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
                    public void sendFreePITContexts(
                        Transport.Connection connection,
                        List<PitSearchContextIdForNode> contextIds,
                        ActionListener<DeletePitResponse> listener
                    ) {
                        deleteNodesInvoked.add(connection.getNode());
                        DeletePitInfo deletePitInfo = new DeletePitInfo(true, "pitId");
                        List<DeletePitInfo> deletePitInfos = new ArrayList<>();
                        deletePitInfos.add(deletePitInfo);
                        Thread t = new Thread(() -> listener.onResponse(new DeletePitResponse(deletePitInfos)));
                        t.start();
                    }

                    @Override
                    public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                        return new SearchAsyncActionTests.MockConnection(node);
                    }
                };
                PitService pitService = new PitService(clusterServiceMock, searchTransportService, transportService, client);
                TransportDeletePitAction action = new TransportDeletePitAction(
                    transportService,
                    actionFilters,
                    namedWriteableRegistry,
                    pitService
                );
                DeletePitRequest deletePITRequest = new DeletePitRequest(pitId);
                PlainActionFuture<DeletePitResponse> future = newFuture();
                action.execute(task, deletePITRequest, future);
                DeletePitResponse dr = future.get();
                assertTrue(dr.getDeletePitResults().get(0).getPitId().equals("pitId"));
                assertTrue(dr.getDeletePitResults().get(0).isSuccessful());
                assertEquals(3, deleteNodesInvoked.size());

            }
        }
    }

    public void testDeleteAllPITSuccess() throws InterruptedException, ExecutionException {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
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
                    public void sendFreePITContexts(
                        Transport.Connection connection,
                        List<PitSearchContextIdForNode> contextIds,
                        final ActionListener<DeletePitResponse> listener
                    ) {
                        deleteNodesInvoked.add(connection.getNode());
                        DeletePitInfo deletePitInfo = new DeletePitInfo(true, "pitId");
                        List<DeletePitInfo> deletePitInfos = new ArrayList<>();
                        deletePitInfos.add(deletePitInfo);
                        Thread t = new Thread(() -> listener.onResponse(new DeletePitResponse(deletePitInfos)));
                        t.start();
                    }

                    @Override
                    public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                        return new SearchAsyncActionTests.MockConnection(node);
                    }
                };
                PitService pitService = new PitService(clusterServiceMock, searchTransportService, transportService, client) {
                    @Override
                    public void getAllPits(ActionListener<GetAllPitNodesResponse> getAllPitsListener) {
                        ListPitInfo listPitInfo = new ListPitInfo(getPitId(), 0, 0);
                        List<ListPitInfo> list = new ArrayList<>();
                        list.add(listPitInfo);
                        GetAllPitNodeResponse getAllPitNodeResponse = new GetAllPitNodeResponse(
                            cluster1Transport.getLocalDiscoNode(),
                            list
                        );
                        List<GetAllPitNodeResponse> nodeList = new ArrayList();
                        nodeList.add(getAllPitNodeResponse);
                        getAllPitsListener.onResponse(new GetAllPitNodesResponse(new ClusterName("cn"), nodeList, new ArrayList()));
                    }
                };
                TransportDeletePitAction action = new TransportDeletePitAction(
                    transportService,
                    actionFilters,
                    namedWriteableRegistry,
                    pitService
                );
                DeletePitRequest deletePITRequest = new DeletePitRequest("_all");
                PlainActionFuture<DeletePitResponse> future = newFuture();
                action.execute(task, deletePITRequest, future);
                DeletePitResponse dr = future.get();
                assertTrue(dr.getDeletePitResults().get(0).getPitId().equals("pitId"));
                assertTrue(dr.getDeletePitResults().get(0).isSuccessful());
                assertEquals(3, deleteNodesInvoked.size());

            }
        }
    }

    public void testDeletePitWhenNodeIsDown() throws InterruptedException, ExecutionException {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
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
                    public void sendFreePITContexts(
                        Transport.Connection connection,
                        List<PitSearchContextIdForNode> contextIds,
                        ActionListener<DeletePitResponse> listener
                    ) {
                        deleteNodesInvoked.add(connection.getNode());

                        if (connection.getNode().getId() == "node_3") {
                            Thread t = new Thread(() -> listener.onFailure(new Exception("node 3 down")));
                            t.start();
                        } else {
                            Thread t = new Thread(() -> listener.onResponse(new DeletePitResponse(new ArrayList<>())));
                            t.start();
                        }
                    }

                    @Override
                    public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                        return new SearchAsyncActionTests.MockConnection(node);
                    }
                };
                PitService pitService = new PitService(clusterServiceMock, searchTransportService, transportService, client);
                TransportDeletePitAction action = new TransportDeletePitAction(
                    transportService,
                    actionFilters,
                    namedWriteableRegistry,
                    pitService
                );
                DeletePitRequest deletePITRequest = new DeletePitRequest(pitId);
                PlainActionFuture<DeletePitResponse> future = newFuture();
                action.execute(task, deletePITRequest, future);
                Exception e = assertThrows(ExecutionException.class, () -> future.get());
                assertThat(e.getMessage(), containsString("node 3 down"));
                assertEquals(3, deleteNodesInvoked.size());
            }
        }
    }

    public void testDeletePitWhenAllNodesAreDown() {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
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
                    public void sendFreePITContexts(
                        Transport.Connection connection,
                        List<PitSearchContextIdForNode> contextIds,
                        ActionListener<DeletePitResponse> listener
                    ) {
                        deleteNodesInvoked.add(connection.getNode());
                        Thread t = new Thread(() -> listener.onFailure(new Exception("node 3 down")));
                        t.start();
                    }

                    @Override
                    public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                        return new SearchAsyncActionTests.MockConnection(node);
                    }
                };
                PitService pitService = new PitService(clusterServiceMock, searchTransportService, transportService, client);
                TransportDeletePitAction action = new TransportDeletePitAction(
                    transportService,
                    actionFilters,
                    namedWriteableRegistry,
                    pitService
                );
                DeletePitRequest deletePITRequest = new DeletePitRequest(pitId);
                PlainActionFuture<DeletePitResponse> future = newFuture();
                action.execute(task, deletePITRequest, future);
                Exception e = assertThrows(ExecutionException.class, () -> future.get());
                assertThat(e.getMessage(), containsString("node 3 down"));
                assertEquals(3, deleteNodesInvoked.size());
            }
        }
    }

    public void testDeletePitFailure() {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);

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
                    public void sendFreePITContexts(
                        Transport.Connection connection,
                        List<PitSearchContextIdForNode> contextId,
                        ActionListener<DeletePitResponse> listener
                    ) {
                        deleteNodesInvoked.add(connection.getNode());

                        if (connection.getNode().getId() == "node_3") {
                            Thread t = new Thread(() -> listener.onFailure(new Exception("node down")));
                            t.start();
                        } else {
                            Thread t = new Thread(() -> listener.onResponse(new DeletePitResponse(new ArrayList<>())));
                            t.start();
                        }
                    }

                    @Override
                    public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                        return new SearchAsyncActionTests.MockConnection(node);
                    }
                };
                PitService pitService = new PitService(clusterServiceMock, searchTransportService, transportService, client);
                TransportDeletePitAction action = new TransportDeletePitAction(
                    transportService,
                    actionFilters,
                    namedWriteableRegistry,
                    pitService
                );
                DeletePitRequest deletePITRequest = new DeletePitRequest(pitId);
                PlainActionFuture<DeletePitResponse> future = newFuture();
                action.execute(task, deletePITRequest, future);
                Exception e = assertThrows(ExecutionException.class, () -> future.get());
                assertThat(e.getMessage(), containsString("node down"));
                assertEquals(3, deleteNodesInvoked.size());
            }
        }
    }

    public void testDeleteAllPitWhenNodeIsDown() {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);

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
                    public void sendFreePITContexts(
                        Transport.Connection connection,
                        List<PitSearchContextIdForNode> contextIds,
                        final ActionListener<DeletePitResponse> listener
                    ) {
                        deleteNodesInvoked.add(connection.getNode());
                        if (connection.getNode().getId() == "node_3") {
                            Thread t = new Thread(() -> listener.onFailure(new Exception("node 3 down")));
                            t.start();
                        } else {
                            Thread t = new Thread(() -> listener.onResponse(new DeletePitResponse(new ArrayList<>())));
                            t.start();
                        }
                    }

                    @Override
                    public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                        return new SearchAsyncActionTests.MockConnection(node);
                    }
                };
                PitService pitService = new PitService(clusterServiceMock, searchTransportService, transportService, client) {
                    @Override
                    public void getAllPits(ActionListener<GetAllPitNodesResponse> getAllPitsListener) {
                        ListPitInfo listPitInfo = new ListPitInfo(getPitId(), 0, 0);
                        List<ListPitInfo> list = new ArrayList<>();
                        list.add(listPitInfo);
                        GetAllPitNodeResponse getAllPitNodeResponse = new GetAllPitNodeResponse(
                            cluster1Transport.getLocalDiscoNode(),
                            list
                        );
                        List<GetAllPitNodeResponse> nodeList = new ArrayList();
                        nodeList.add(getAllPitNodeResponse);
                        getAllPitsListener.onResponse(new GetAllPitNodesResponse(new ClusterName("cn"), nodeList, new ArrayList()));
                    }
                };
                TransportDeletePitAction action = new TransportDeletePitAction(
                    transportService,
                    actionFilters,
                    namedWriteableRegistry,
                    pitService
                );
                DeletePitRequest deletePITRequest = new DeletePitRequest("_all");
                PlainActionFuture<DeletePitResponse> future = newFuture();
                action.execute(task, deletePITRequest, future);
                Exception e = assertThrows(ExecutionException.class, () -> future.get());
                assertThat(e.getMessage(), containsString("node 3 down"));
                assertEquals(3, deleteNodesInvoked.size());
            }
        }
    }

    public void testDeleteAllPitWhenAllNodesAreDown() {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);

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
                    public void sendFreePITContexts(
                        Transport.Connection connection,
                        List<PitSearchContextIdForNode> contextIds,
                        final ActionListener<DeletePitResponse> listener
                    ) {
                        deleteNodesInvoked.add(connection.getNode());
                        Thread t = new Thread(() -> listener.onFailure(new Exception("node down")));
                        t.start();
                    }

                    @Override
                    public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                        return new SearchAsyncActionTests.MockConnection(node);
                    }
                };
                PitService pitService = new PitService(clusterServiceMock, searchTransportService, transportService, client) {
                    @Override
                    public void getAllPits(ActionListener<GetAllPitNodesResponse> getAllPitsListener) {
                        ListPitInfo listPitInfo = new ListPitInfo(getPitId(), 0, 0);
                        List<ListPitInfo> list = new ArrayList<>();
                        list.add(listPitInfo);
                        GetAllPitNodeResponse getAllPitNodeResponse = new GetAllPitNodeResponse(
                            cluster1Transport.getLocalDiscoNode(),
                            list
                        );
                        List<GetAllPitNodeResponse> nodeList = new ArrayList();
                        nodeList.add(getAllPitNodeResponse);
                        getAllPitsListener.onResponse(new GetAllPitNodesResponse(new ClusterName("cn"), nodeList, new ArrayList()));
                    }
                };
                TransportDeletePitAction action = new TransportDeletePitAction(
                    transportService,
                    actionFilters,
                    namedWriteableRegistry,
                    pitService
                );
                DeletePitRequest deletePITRequest = new DeletePitRequest("_all");
                PlainActionFuture<DeletePitResponse> future = newFuture();
                action.execute(task, deletePITRequest, future);
                Exception e = assertThrows(ExecutionException.class, () -> future.get());
                assertThat(e.getMessage(), containsString("node down"));
                assertEquals(3, deleteNodesInvoked.size());
            }
        }
    }

    public void testDeleteAllPitFailure() {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);

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

                    public void sendFreePITContexts(
                        Transport.Connection connection,
                        List<PitSearchContextIdForNode> contextId,
                        final ActionListener<DeletePitResponse> listener
                    ) {
                        deleteNodesInvoked.add(connection.getNode());
                        if (connection.getNode().getId() == "node_3") {
                            Thread t = new Thread(() -> listener.onFailure(new Exception("node 3 is down")));
                            t.start();
                        } else {
                            Thread t = new Thread(() -> listener.onResponse(new DeletePitResponse(new ArrayList<>())));
                            t.start();
                        }
                    }

                    @Override
                    public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                        return new SearchAsyncActionTests.MockConnection(node);
                    }
                };
                PitService pitService = new PitService(clusterServiceMock, searchTransportService, transportService, client) {
                    @Override
                    public void getAllPits(ActionListener<GetAllPitNodesResponse> getAllPitsListener) {
                        ListPitInfo listPitInfo = new ListPitInfo(getPitId(), 0, 0);
                        List<ListPitInfo> list = new ArrayList<>();
                        list.add(listPitInfo);
                        GetAllPitNodeResponse getAllPitNodeResponse = new GetAllPitNodeResponse(
                            cluster1Transport.getLocalDiscoNode(),
                            list
                        );
                        List<GetAllPitNodeResponse> nodeList = new ArrayList();
                        nodeList.add(getAllPitNodeResponse);
                        getAllPitsListener.onResponse(new GetAllPitNodesResponse(new ClusterName("cn"), nodeList, new ArrayList()));
                    }
                };
                TransportDeletePitAction action = new TransportDeletePitAction(
                    transportService,
                    actionFilters,
                    namedWriteableRegistry,
                    pitService
                );
                DeletePitRequest deletePITRequest = new DeletePitRequest("_all");
                PlainActionFuture<DeletePitResponse> future = newFuture();
                action.execute(task, deletePITRequest, future);
                Exception e = assertThrows(ExecutionException.class, () -> future.get());
                assertThat(e.getMessage(), containsString("java.lang.Exception: node 3 is down"));
                assertEquals(3, deleteNodesInvoked.size());
            }
        }
    }
}
