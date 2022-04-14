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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.Transport;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    ActionListener<CreatePITResponse> createPitListener = null;
    ClusterService clusterServiceMock = null;

    @Before
    public void setupData() {
        node1 = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        node2 = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        node3 = new DiscoveryNode("node_3", buildNewFakeTransportAddress(), Version.CURRENT);
        setPitId();
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
        createPitListener = new ActionListener<CreatePITResponse>() {
            @Override
            public void onResponse(CreatePITResponse createPITResponse) {
                assertEquals(3, createPITResponse.getTotalShards());
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        };

        clusterServiceMock = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);

        final Settings keepAliveSettings = Settings.builder()
            .put(SearchService.CREATE_PIT_TEMPORARY_KEEPALIVE_SETTING.getKey(), 30000)
            .build();
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
        SearchTransportService searchTransportService = new SearchTransportService(null, null) {
            @Override
            public void updatePitContext(
                Transport.Connection connection,
                UpdatePITContextRequest request,
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
            public void sendFreeContext(
                Transport.Connection connection,
                ShardSearchContextId contextId,
                ActionListener<SearchFreeContextResponse> listener
            ) {
                deleteNodesInvoked.add(connection.getNode());
                Thread t = new Thread(() -> listener.onResponse(new SearchFreeContextResponse(true)));
                t.start();
            }

            @Override
            public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                return new SearchAsyncActionTests.MockConnection(node);
            }
        };

        CountDownLatch latch = new CountDownLatch(1);

        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });

        CreatePITController controller = new CreatePITController(
            request,
            searchTransportService,
            clusterServiceMock,
            transportSearchAction,
            namedWriteableRegistry,
            task,
            createPitListener
        );

        CreatePITResponse createPITResponse = new CreatePITResponse(searchResponse);

        ActionListener<CreatePITResponse> updatelistener = new LatchedActionListener<>(new ActionListener<CreatePITResponse>() {
            @Override
            public void onResponse(CreatePITResponse createPITResponse) {
                assertEquals(3, createPITResponse.getTotalShards());
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        }, latch);

        StepListener<CreatePITResponse> createListener = new StepListener<>();

        controller.executeUpdatePitId(request, createListener, updatelistener);
        createListener.onResponse(createPITResponse);
        latch.await();
        assertEquals(3, updateNodesInvoked.size());
        assertEquals(0, deleteNodesInvoked.size());
    }

    /**
     * If create phase results in failure, update pit phase should not proceed and propagate the exception
     */
    public void testUpdatePitAfterCreatePitFailure() throws InterruptedException {
        List<DiscoveryNode> updateNodesInvoked = new CopyOnWriteArrayList<>();
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        SearchTransportService searchTransportService = new SearchTransportService(null, null) {
            @Override
            public void updatePitContext(
                Transport.Connection connection,
                UpdatePITContextRequest request,
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
            public void sendFreeContext(
                Transport.Connection connection,
                ShardSearchContextId contextId,
                ActionListener<SearchFreeContextResponse> listener
            ) {
                deleteNodesInvoked.add(connection.getNode());
                Thread t = new Thread(() -> listener.onResponse(new SearchFreeContextResponse(true)));
                t.start();
            }
        };

        CountDownLatch latch = new CountDownLatch(1);

        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });

        CreatePITController controller = new CreatePITController(
            request,
            searchTransportService,
            clusterServiceMock,
            transportSearchAction,
            namedWriteableRegistry,
            task,
            createPitListener
        );

        ActionListener<CreatePITResponse> updatelistener = new LatchedActionListener<>(new ActionListener<CreatePITResponse>() {
            @Override
            public void onResponse(CreatePITResponse createPITResponse) {
                throw new AssertionError("on response is called");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e.getCause().getMessage().contains("Exception occurred in phase 1"));
            }
        }, latch);

        StepListener<CreatePITResponse> createListener = new StepListener<>();

        controller.executeUpdatePitId(request, createListener, updatelistener);
        createListener.onFailure(new Exception("Exception occurred in phase 1"));
        latch.await();
        assertEquals(0, updateNodesInvoked.size());
        /**
         * cleanup is not called on create pit phase one failure
         */
        assertEquals(0, deleteNodesInvoked.size());
    }

    /**
     * Testing that any update pit failures fails the request
     */
    public void testUpdatePitFailureForNodeDrop() throws InterruptedException {
        List<DiscoveryNode> updateNodesInvoked = new CopyOnWriteArrayList<>();
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        SearchTransportService searchTransportService = new SearchTransportService(null, null) {
            @Override
            public void updatePitContext(
                Transport.Connection connection,
                UpdatePITContextRequest request,
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
            public void sendFreeContext(
                Transport.Connection connection,
                ShardSearchContextId contextId,
                ActionListener<SearchFreeContextResponse> listener
            ) {
                deleteNodesInvoked.add(connection.getNode());
                Thread t = new Thread(() -> listener.onResponse(new SearchFreeContextResponse(true)));
                t.start();
            }

            @Override
            public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                return new SearchAsyncActionTests.MockConnection(node);
            }
        };
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        CreatePITController controller = new CreatePITController(
            request,
            searchTransportService,
            clusterServiceMock,
            transportSearchAction,
            namedWriteableRegistry,
            task,
            createPitListener
        );

        CreatePITResponse createPITResponse = new CreatePITResponse(searchResponse);
        CountDownLatch latch = new CountDownLatch(1);

        ActionListener<CreatePITResponse> updatelistener = new LatchedActionListener<>(new ActionListener<CreatePITResponse>() {
            @Override
            public void onResponse(CreatePITResponse createPITResponse) {
                throw new AssertionError("response is called");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e.getMessage().contains("node 3 down"));
            }
        }, latch);

        StepListener<CreatePITResponse> createListener = new StepListener<>();
        controller.executeUpdatePitId(request, createListener, updatelistener);
        createListener.onResponse(createPITResponse);
        latch.await();
        assertEquals(3, updateNodesInvoked.size());
        /**
         * check if cleanup is called for all nodes in case of update pit failure
         */
        assertEquals(3, deleteNodesInvoked.size());
    }

    public void testUpdatePitFailureWhereAllNodesDown() throws InterruptedException {
        List<DiscoveryNode> updateNodesInvoked = new CopyOnWriteArrayList<>();
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        SearchTransportService searchTransportService = new SearchTransportService(null, null) {
            @Override
            public void updatePitContext(
                Transport.Connection connection,
                UpdatePITContextRequest request,
                ActionListener<UpdatePitContextResponse> listener
            ) {
                updateNodesInvoked.add(connection.getNode());
                Thread t = new Thread(() -> listener.onFailure(new Exception("node down")));
                t.start();
            }

            @Override
            public void sendFreeContext(
                Transport.Connection connection,
                ShardSearchContextId contextId,
                ActionListener<SearchFreeContextResponse> listener
            ) {
                deleteNodesInvoked.add(connection.getNode());
                Thread t = new Thread(() -> listener.onResponse(new SearchFreeContextResponse(true)));
                t.start();
            }

            @Override
            public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                return new SearchAsyncActionTests.MockConnection(node);
            }
        };
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        CreatePITController controller = new CreatePITController(
            request,
            searchTransportService,
            clusterServiceMock,
            transportSearchAction,
            namedWriteableRegistry,
            task,
            createPitListener
        );

        CreatePITResponse createPITResponse = new CreatePITResponse(searchResponse);
        CountDownLatch latch = new CountDownLatch(1);

        ActionListener<CreatePITResponse> updatelistener = new LatchedActionListener<>(new ActionListener<CreatePITResponse>() {
            @Override
            public void onResponse(CreatePITResponse createPITResponse) {
                throw new AssertionError("response is called");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e.getMessage().contains("node down"));
            }
        }, latch);

        StepListener<CreatePITResponse> createListener = new StepListener<>();
        controller.executeUpdatePitId(request, createListener, updatelistener);
        createListener.onResponse(createPITResponse);
        latch.await();
        assertEquals(3, updateNodesInvoked.size());
        /**
         * check if cleanup is called for all nodes in case of update pit failure
         */
        assertEquals(3, deleteNodesInvoked.size());

    }

    QueryBuilder randomQueryBuilder() {
        if (randomBoolean()) {
            return new TermQueryBuilder(randomAlphaOfLength(10), randomAlphaOfLength(10));
        } else if (randomBoolean()) {
            return new MatchAllQueryBuilder();
        } else {
            return new IdsQueryBuilder().addIds(randomAlphaOfLength(10));
        }
    }

    private void setPitId() {
        AtomicArray<SearchPhaseResult> array = new AtomicArray<>(3);
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult1 = new SearchAsyncActionTests.TestSearchPhaseResult(
            new ShardSearchContextId("a", 1),
            node1
        );
        testSearchPhaseResult1.setSearchShardTarget(new SearchShardTarget("node_1", new ShardId("idx", "uuid1", 2), null, null));
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult2 = new SearchAsyncActionTests.TestSearchPhaseResult(
            new ShardSearchContextId("b", 12),
            node2
        );
        testSearchPhaseResult2.setSearchShardTarget(new SearchShardTarget("node_2", new ShardId("idy", "uuid2", 42), null, null));
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult3 = new SearchAsyncActionTests.TestSearchPhaseResult(
            new ShardSearchContextId("c", 42),
            node3
        );
        testSearchPhaseResult3.setSearchShardTarget(new SearchShardTarget("node_3", new ShardId("idy", "uuid2", 43), null, null));
        array.setOnce(0, testSearchPhaseResult1);
        array.setOnce(1, testSearchPhaseResult2);
        array.setOnce(2, testSearchPhaseResult3);

        final Version version = Version.CURRENT;
        final Map<String, AliasFilter> aliasFilters = new HashMap<>();
        for (SearchPhaseResult result : array.asList()) {
            final AliasFilter aliasFilter;
            if (randomBoolean()) {
                aliasFilter = new AliasFilter(randomQueryBuilder());
            } else if (randomBoolean()) {
                aliasFilter = new AliasFilter(randomQueryBuilder(), "alias-" + between(1, 10));
            } else {
                aliasFilter = AliasFilter.EMPTY;
            }
            if (randomBoolean()) {
                aliasFilters.put(result.getSearchShardTarget().getShardId().getIndex().getUUID(), aliasFilter);
            }
        }
        pitId = SearchContextId.encode(array.asList(), aliasFilters, version);
    }

}
