/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pit;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.GetAllPitNodesRequest;
import org.opensearch.action.search.GetAllPitNodesResponse;
import org.opensearch.action.search.GetAllPitsAction;
import org.opensearch.action.search.PitTestsUtil;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Requests;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.action.search.PitTestsUtil.assertSegments;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Multi node integration tests for PIT creation and search operation with PIT ID.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class PitMultiNodeIT extends OpenSearchIntegTestCase {

    @Before
    public void setupIndex() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).execute().get();
        ensureGreen();
    }

    @After
    public void clearIndex() {
        client().admin().indices().prepareDelete("index").get();
    }

    public void testPit() throws Exception {
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        SearchResponse searchResponse = client().prepareSearch("index")
            .setSize(2)
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
            .get();
        assertEquals(2, searchResponse.getSuccessfulShards());
        assertEquals(2, searchResponse.getTotalShards());
        validatePitStats("index", 2, 2);
        PitTestsUtil.assertUsingGetAllPits(client(), pitResponse.getId(), pitResponse.getCreationTime());
        assertSegments(false, client(), pitResponse.getId());
    }

    public void testCreatePitWhileNodeDropWithAllowPartialCreationFalse() throws Exception {
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), false);
        request.setIndices(new String[] { "index" });
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
                ExecutionException ex = expectThrows(ExecutionException.class, execute::get);
                assertTrue(ex.getMessage().contains("Failed to execute phase [create_pit]"));
                validatePitStats("index", 0, 0);
                return super.onNodeStopped(nodeName);
            }
        });
    }

    public void testCreatePitWhileNodeDropWithAllowPartialCreationTrue() throws Exception {
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
                CreatePitResponse pitResponse = execute.get();
                PitTestsUtil.assertUsingGetAllPits(client(), pitResponse.getId(), pitResponse.getCreationTime());
                assertSegments(false, "index", 1, client(), pitResponse.getId());
                assertEquals(1, pitResponse.getSuccessfulShards());
                assertEquals(2, pitResponse.getTotalShards());
                SearchResponse searchResponse = client().prepareSearch("index")
                    .setSize(2)
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                    .get();
                assertEquals(1, searchResponse.getSuccessfulShards());
                assertEquals(1, searchResponse.getTotalShards());
                validatePitStats("index", 1, 1);
                return super.onNodeStopped(nodeName);
            }
        });
    }

    public void testPitSearchWithNodeDrop() throws Exception {
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                SearchResponse searchResponse = client().prepareSearch()
                    .setSize(2)
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                    .get();
                assertEquals(1, searchResponse.getSuccessfulShards());
                assertEquals(1, searchResponse.getFailedShards());
                assertEquals(0, searchResponse.getSkippedShards());
                assertEquals(2, searchResponse.getTotalShards());
                validatePitStats("index", 1, 1);
                PitTestsUtil.assertUsingGetAllPits(client(), pitResponse.getId(), pitResponse.getCreationTime());
                return super.onNodeStopped(nodeName);
            }
        });
    }

    public void testPitSearchWithNodeDropWithPartialSearchResultsFalse() throws Exception {
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ActionFuture<SearchResponse> execute = client().prepareSearch()
                    .setSize(2)
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                    .setAllowPartialSearchResults(false)
                    .execute();
                ExecutionException ex = expectThrows(ExecutionException.class, execute::get);
                assertTrue(ex.getMessage().contains("Partial shards failure"));
                return super.onNodeStopped(nodeName);
            }
        });
    }

    public void testPitInvalidDefaultKeepAlive() {
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("point_in_time.max_keep_alive", "1m").put("search.default_keep_alive", "2m"))
                .get()
        );
        assertThat(exc.getMessage(), containsString("was (2m > 1m)"));
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "5m").put("point_in_time.max_keep_alive", "5m"))
                .get()
        );
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "2m"))
                .get()
        );
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("point_in_time.max_keep_alive", "2m"))
                .get()
        );
        exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "3m"))
                .get()
        );
        assertThat(exc.getMessage(), containsString("was (3m > 2m)"));
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "1m"))
                .get()
        );
        exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("point_in_time.max_keep_alive", "30s"))
                .get()
        );
        assertThat(exc.getMessage(), containsString("was (1m > 30s)"));
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );
    }

    public void testConcurrentCreates() throws InterruptedException {
        CreatePitRequest createPitRequest = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        createPitRequest.setIndices(new String[] { "index" });

        int concurrentRuns = randomIntBetween(20, 50);
        AtomicInteger numSuccess = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(PitMultiNodeIT.class.getName());
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(concurrentRuns);
            Set<String> createSet = new HashSet<>();
            for (int i = 0; i < concurrentRuns; i++) {
                Runnable thread = () -> {
                    logger.info("Triggering pit create --->");
                    LatchedActionListener listener = new LatchedActionListener<>(new ActionListener<CreatePitResponse>() {
                        @Override
                        public void onResponse(CreatePitResponse createPitResponse) {
                            if (createSet.add(createPitResponse.getId())) {
                                numSuccess.incrementAndGet();
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {}
                    }, countDownLatch);
                    client().execute(CreatePitAction.INSTANCE, createPitRequest, listener);
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            assertEquals(concurrentRuns, numSuccess.get());
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    public void testConcurrentCreatesWithDeletes() throws InterruptedException, ExecutionException {
        CreatePitRequest createPitRequest = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        createPitRequest.setIndices(new String[] { "index" });
        List<String> pitIds = new ArrayList<>();
        String id = client().execute(CreatePitAction.INSTANCE, createPitRequest).get().getId();
        pitIds.add(id);
        DeletePitRequest deletePITRequest = new DeletePitRequest(pitIds);
        Set<String> createSet = new HashSet<>();
        AtomicInteger numSuccess = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(PitMultiNodeIT.class.getName());
            int concurrentRuns = randomIntBetween(20, 50);

            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(concurrentRuns);
            long randomDeleteThread = randomLongBetween(0, concurrentRuns - 1);
            for (int i = 0; i < concurrentRuns; i++) {
                int currentThreadIteration = i;
                Runnable thread = () -> {
                    if (currentThreadIteration == randomDeleteThread) {
                        LatchedActionListener listener = new LatchedActionListener<>(new ActionListener<CreatePitResponse>() {
                            @Override
                            public void onResponse(CreatePitResponse createPitResponse) {
                                if (createSet.add(createPitResponse.getId())) {
                                    numSuccess.incrementAndGet();
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {}
                        }, countDownLatch);
                        client().execute(CreatePitAction.INSTANCE, createPitRequest, listener);
                    } else {
                        LatchedActionListener listener = new LatchedActionListener<>(new ActionListener<DeletePitResponse>() {
                            @Override
                            public void onResponse(DeletePitResponse deletePitResponse) {
                                if (deletePitResponse.getDeletePitResults().get(0).isSuccessful()) {
                                    numSuccess.incrementAndGet();
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {}
                        }, countDownLatch);
                        client().execute(DeletePitAction.INSTANCE, deletePITRequest, listener);
                    }
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            assertEquals(concurrentRuns, numSuccess.get());

        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    public void validatePitStats(String index, long expectedPitCurrent, long expectedOpenContexts) throws ExecutionException,
        InterruptedException {
        // Clear the index transaction log
        FlushRequest flushRequest = Requests.flushRequest(index);
        client().admin().indices().flush(flushRequest).get();
        // Test stats
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.indices(index);
        indicesStatsRequest.all();
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().stats(indicesStatsRequest).get();
        long pitCurrent = indicesStatsResponse.getIndex(index).getTotal().search.getTotal().getPitCurrent();
        long openContexts = indicesStatsResponse.getIndex(index).getTotal().search.getOpenContexts();
        assertEquals(expectedPitCurrent, pitCurrent);
        assertEquals(expectedOpenContexts, openContexts);
    }

    public void testGetAllPits() throws Exception {
        client().admin().indices().prepareCreate("index1").get();
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index", "index1" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        CreatePitResponse pitResponse1 = client().execute(CreatePitAction.INSTANCE, request).get();
        CreatePitResponse pitResponse2 = client().execute(CreatePitAction.INSTANCE, request).get();
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(false);
        clusterStateRequest.clear().nodes(true).routingTable(true).indices("*");
        ClusterStateResponse clusterStateResponse = client().admin().cluster().state(clusterStateRequest).get();
        final List<DiscoveryNode> nodes = new LinkedList<>();
        for (ObjectCursor<DiscoveryNode> cursor : clusterStateResponse.getState().nodes().getDataNodes().values()) {
            DiscoveryNode node = cursor.value;
            nodes.add(node);
        }
        DiscoveryNode[] disNodesArr = new DiscoveryNode[nodes.size()];
        nodes.toArray(disNodesArr);
        GetAllPitNodesRequest getAllPITNodesRequest = new GetAllPitNodesRequest(disNodesArr);
        ActionFuture<GetAllPitNodesResponse> execute1 = client().execute(GetAllPitsAction.INSTANCE, getAllPITNodesRequest);
        GetAllPitNodesResponse getPitResponse = execute1.get();
        assertEquals(3, getPitResponse.getPitInfos().size());
        List<String> resultPitIds = getPitResponse.getPitInfos().stream().map(p -> p.getPitId()).collect(Collectors.toList());
        // asserting that we get all unique PIT IDs
        Assert.assertTrue(resultPitIds.contains(pitResponse.getId()));
        Assert.assertTrue(resultPitIds.contains(pitResponse1.getId()));
        Assert.assertTrue(resultPitIds.contains(pitResponse2.getId()));
        client().admin().indices().prepareDelete("index1").get();
    }

    public void testGetAllPitsDuringNodeDrop() throws Exception {
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        GetAllPitNodesRequest getAllPITNodesRequest = new GetAllPitNodesRequest(getDiscoveryNodes());
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ActionFuture<GetAllPitNodesResponse> execute1 = client().execute(GetAllPitsAction.INSTANCE, getAllPITNodesRequest);
                GetAllPitNodesResponse getPitResponse = execute1.get();
                // we still get a pit id from the data node which is up
                assertEquals(1, getPitResponse.getPitInfos().size());
                // failure for node drop
                assertEquals(1, getPitResponse.failures().size());
                assertTrue(getPitResponse.failures().get(0).getMessage().contains("Failed node"));
                return super.onNodeStopped(nodeName);
            }
        });
    }

    private DiscoveryNode[] getDiscoveryNodes() throws ExecutionException, InterruptedException {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(false);
        clusterStateRequest.clear().nodes(true).routingTable(true).indices("*");
        ClusterStateResponse clusterStateResponse = client().admin().cluster().state(clusterStateRequest).get();
        final List<DiscoveryNode> nodes = new LinkedList<>();
        for (ObjectCursor<DiscoveryNode> cursor : clusterStateResponse.getState().nodes().getDataNodes().values()) {
            DiscoveryNode node = cursor.value;
            nodes.add(node);
        }
        DiscoveryNode[] disNodesArr = new DiscoveryNode[nodes.size()];
        nodes.toArray(disNodesArr);
        return disNodesArr;
    }

    public void testConcurrentGetWithDeletes() throws InterruptedException, ExecutionException {
        CreatePitRequest createPitRequest = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        createPitRequest.setIndices(new String[] { "index" });
        List<String> pitIds = new ArrayList<>();
        String id = client().execute(CreatePitAction.INSTANCE, createPitRequest).get().getId();
        pitIds.add(id);
        DeletePitRequest deletePITRequest = new DeletePitRequest(pitIds);
        GetAllPitNodesRequest getAllPITNodesRequest = new GetAllPitNodesRequest(getDiscoveryNodes());
        AtomicInteger numSuccess = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(PitMultiNodeIT.class.getName());
            int concurrentRuns = randomIntBetween(20, 50);

            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(concurrentRuns);
            long randomDeleteThread = randomLongBetween(0, concurrentRuns - 1);
            for (int i = 0; i < concurrentRuns; i++) {
                int currentThreadIteration = i;
                Runnable thread = () -> {
                    if (currentThreadIteration == randomDeleteThread) {
                        LatchedActionListener listener = new LatchedActionListener<>(new ActionListener<GetAllPitNodesResponse>() {
                            @Override
                            public void onResponse(GetAllPitNodesResponse getAllPitNodesResponse) {
                                if (getAllPitNodesResponse.failures().isEmpty()) {
                                    numSuccess.incrementAndGet();
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {}
                        }, countDownLatch);
                        client().execute(GetAllPitsAction.INSTANCE, getAllPITNodesRequest, listener);
                    } else {
                        LatchedActionListener listener = new LatchedActionListener<>(new ActionListener<DeletePitResponse>() {
                            @Override
                            public void onResponse(DeletePitResponse deletePitResponse) {
                                if (deletePitResponse.getDeletePitResults().get(0).isSuccessful()) {
                                    numSuccess.incrementAndGet();
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {}
                        }, countDownLatch);
                        client().execute(DeletePitAction.INSTANCE, deletePITRequest, listener);
                    }
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            assertEquals(concurrentRuns, numSuccess.get());

        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

}
