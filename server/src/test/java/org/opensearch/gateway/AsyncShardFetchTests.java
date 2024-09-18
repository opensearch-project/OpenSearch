/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.ClusterManagerMetrics;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AsyncShardFetchTests extends OpenSearchTestCase {
    private final DiscoveryNode node1 = new DiscoveryNode(
        "node1",
        buildNewFakeTransportAddress(),
        Collections.emptyMap(),
        Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
        Version.CURRENT
    );
    private final Response response1 = new Response(node1);
    private final Response response1_2 = new Response(node1);
    private final Throwable failure1 = new Throwable("simulated failure 1");
    private final DiscoveryNode node2 = new DiscoveryNode(
        "node2",
        buildNewFakeTransportAddress(),
        Collections.emptyMap(),
        Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
        Version.CURRENT
    );
    private final Response response2 = new Response(node2);
    private final Throwable failure2 = new Throwable("simulate failure 2");

    private ThreadPool threadPool;
    private TestFetch test;
    private Counter asyncFetchTotalCounter;
    private Counter asyncFetchFailureCounter;
    private Counter dummyCounter;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.threadPool = new TestThreadPool(getTestName());
        final MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
        this.asyncFetchFailureCounter = mock(Counter.class);
        this.asyncFetchTotalCounter = mock(Counter.class);
        this.dummyCounter = mock(Counter.class);
        when(metricsRegistry.createCounter(anyString(), anyString(), anyString())).thenAnswer(invocationOnMock -> {
            String counterName = (String) invocationOnMock.getArguments()[0];
            if (counterName.contains("allocation.reroute.async.fetch.total.count")) {
                return asyncFetchTotalCounter;
            } else if (counterName.contains("allocation.reroute.async.fetch.failure.count")) {
                return asyncFetchFailureCounter;
            }
            return dummyCounter;
        });
        this.test = new TestFetch(threadPool, metricsRegistry);
    }

    @After
    public void terminate() throws Exception {
        terminate(threadPool);
    }

    public void testClose() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).build();
        test.addSimulation(node1.getId(), response1);

        // first fetch, no data, still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));
        // counter remains 0 because fetch is ongoing
        verify(asyncFetchTotalCounter, times(0)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // fire a response, wait on reroute incrementing
        test.fireSimulationAndWait(node1.getId());
        // counter goes up because fetch completed
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // verify we get back the data node
        assertThat(test.reroute.get(), equalTo(1));
        test.close();
        try {
            test.fetchData(nodes, emptyMap());
            // counter should not go up when calling fetchData since fetch never completed
            verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
            verify(asyncFetchFailureCounter, times(0)).add(anyDouble());
            fail("fetch data should fail when closed");
        } catch (IllegalStateException e) {
            // all is well
        }
    }

    public void testFullCircleSingleNodeSuccess() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).build();
        test.addSimulation(node1.getId(), response1);

        // first fetch, no data, still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));

        // fire a response, wait on reroute incrementing
        test.fireSimulationAndWait(node1.getId());
        // total counter goes up by 1 after success
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // verify we get back the data node
        assertThat(test.reroute.get(), equalTo(1));
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(1));
        assertThat(fetchData.getData().get(node1), sameInstance(response1));
        // counter remains same because fetchData does not trigger new async fetch
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());
    }

    public void testFullCircleSingleNodeFailure() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).build();
        // add a failed response for node1
        test.addSimulation(node1.getId(), failure1);

        // first fetch, no data, still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));

        // fire a response, wait on reroute incrementing
        test.fireSimulationAndWait(node1.getId());
        // Failure results in increased counter for failure and total count
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(1)).add(anyDouble());

        // failure, fetched data exists, but has no data
        assertThat(test.reroute.get(), equalTo(1));
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(0));
        // counter remains same because fetchData does not trigger new async fetch
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(1)).add(anyDouble());

        // on failure, we reset the failure on a successive call to fetchData, and try again afterwards
        test.addSimulation(node1.getId(), response1);
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        // No additional failure, empty data so no change in counter
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(1)).add(anyDouble());

        test.fireSimulationAndWait(node1.getId());
        // Success will increase total counter but not failure counter
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(1)).add(anyDouble());

        // 2 reroutes, cause we have a failure that we clear
        assertThat(test.reroute.get(), equalTo(3));
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(1));
        assertThat(fetchData.getData().get(node1), sameInstance(response1));
        // counter remains same because fetchData does not trigger new async fetch
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(1)).add(anyDouble());
    }

    public void testIgnoreResponseFromDifferentRound() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).build();
        test.addSimulation(node1.getId(), response1);

        // first fetch, no data, still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));
        // counter 0 because fetchData is not completed
        verify(asyncFetchTotalCounter, times(0)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // handle a response with incorrect round id, wait on reroute incrementing
        test.processAsyncFetch(Collections.singletonList(response1), Collections.emptyList(), 0);
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(1));
        // counter increments to 1 because we called processAsyncFetch with a valid response, even though the round was incorrect
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // fire a response (with correct round id), wait on reroute incrementing
        test.fireSimulationAndWait(node1.getId());
        // total counter now goes up by 1 because fetchData completed
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // verify we get back the data node
        assertThat(test.reroute.get(), equalTo(2));
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(1));
        assertThat(fetchData.getData().get(node1), sameInstance(response1));
        // total counter remains same because fetchdata does not trigger new async fetch
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());
    }

    public void testIgnoreFailureFromDifferentRound() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).build();
        // add a failed response for node1
        test.addSimulation(node1.getId(), failure1);

        // first fetch, no data, still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));
        // counter 0 because fetchData still ongoing
        verify(asyncFetchTotalCounter, times(0)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // handle a failure with incorrect round id, wait on reroute incrementing
        test.processAsyncFetch(
            Collections.emptyList(),
            Collections.singletonList(new FailedNodeException(node1.getId(), "dummy failure", failure1)),
            0
        );
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(1));
        // total and failure counter go up by 1 because of the failure
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(1)).add(anyDouble());

        // fire a response, wait on reroute incrementing
        test.fireSimulationAndWait(node1.getId());
        // total and failure counter go up by 1 because of the failure
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(2)).add(anyDouble());
        // failure, fetched data exists, but has no data
        assertThat(test.reroute.get(), equalTo(2));
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(0));
        // total and failure counter remain same because fetchData does not trigger new async fetch
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(2)).add(anyDouble());
    }

    public void testTwoNodesOnSetup() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).add(node2).build();
        test.addSimulation(node1.getId(), response1);
        test.addSimulation(node2.getId(), response2);

        // no fetched data, 2 requests still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));
        // counter 0 because fetch ongoing
        verify(asyncFetchTotalCounter, times(0)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // fire the first response, it should trigger a reroute
        test.fireSimulationAndWait(node1.getId());
        // counter 1 because one fetch completed
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // there is still another on going request, so no data
        assertThat(test.getNumberOfInFlightFetches(), equalTo(1));
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        // counter still 1 because fetchData did not trigger new async fetch
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // fire the second simulation, this should allow us to get the data
        test.fireSimulationAndWait(node2.getId());
        // counter 2 because 2 fetches completed
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());
        // no more ongoing requests, we should fetch the data
        assertThat(test.reroute.get(), equalTo(2));
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(2));
        assertThat(fetchData.getData().get(node1), sameInstance(response1));
        assertThat(fetchData.getData().get(node2), sameInstance(response2));
        // counter still 2 because fetchData call did not trigger new async fetch
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());
    }

    public void testTwoNodesOnSetupAndFailure() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).add(node2).build();
        test.addSimulation(node1.getId(), response1);
        test.addSimulation(node2.getId(), failure2);

        // no fetched data, 2 requests still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));
        // counter 0 because both fetches ongoing
        verify(asyncFetchTotalCounter, times(0)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // fire the first response, it should trigger a reroute
        test.fireSimulationAndWait(node1.getId());
        assertThat(test.reroute.get(), equalTo(1));
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        // counter 1 because one fetch completed
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // fire the second simulation, this should allow us to get the data
        test.fireSimulationAndWait(node2.getId());
        // failure counter up by 1 because one fetch failed
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(1)).add(anyDouble());
        assertThat(test.reroute.get(), equalTo(2));

        // since one of those failed, we should only have one entry
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(1));
        assertThat(fetchData.getData().get(node1), sameInstance(response1));
        // total and failure counters same because fetchData did not trigger new async fetch
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(1)).add(anyDouble());
    }

    public void testTwoNodesAddedInBetween() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).build();
        test.addSimulation(node1.getId(), response1);

        // no fetched data, request still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));
        // counter 0 because both fetches ongoing
        verify(asyncFetchTotalCounter, times(0)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // fire the first response, it should trigger a reroute
        test.fireSimulationAndWait(node1.getId());
        // counter 1 because fetch completed
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // now, add a second node to the nodes, it should add it to the ongoing requests
        nodes = DiscoveryNodes.builder(nodes).add(node2).build();
        test.addSimulation(node2.getId(), response2);
        // no fetch data, has a new node introduced
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        // counter still 1 because second fetch ongoing
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // fire the second simulation, this should allow us to get the data
        test.fireSimulationAndWait(node2.getId());
        // counter now 2 because 2 fetches completed
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // since both succeeded, we should have 2 entries
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(2));
        assertThat(fetchData.getData().get(node1), sameInstance(response1));
        assertThat(fetchData.getData().get(node2), sameInstance(response2));
        // counter still 2 because fetchData did not trigger new async fetch
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());
    }

    public void testClearCache() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).build();
        test.addSimulation(node1.getId(), response1);

        // must work also with no data
        test.clearCacheForNode(node1.getId());

        // no fetched data, request still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));
        // counter 0 because fetch ongoing
        verify(asyncFetchTotalCounter, times(0)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        test.fireSimulationAndWait(node1.getId());
        assertThat(test.reroute.get(), equalTo(1));
        // counter 1 because 1 fetch completed
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // verify we get back right data from node
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(1));
        assertThat(fetchData.getData().get(node1), sameInstance(response1));
        // counter still 1 because a new fetch is not called
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // second fetch gets same data
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(1));
        assertThat(fetchData.getData().get(node1), sameInstance(response1));
        // counter still 1 because a new fetch is not called
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        test.clearCacheForNode(node1.getId());

        // prepare next request
        test.addSimulation(node1.getId(), response1_2);

        // no fetched data, new request on going
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        // counter still 1 because new fetch is still ongoing
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        test.fireSimulationAndWait(node1.getId());
        assertThat(test.reroute.get(), equalTo(2));
        // counter now 2 because second fetch completed
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // verify we get new data back
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(1));
        assertThat(fetchData.getData().get(node1), sameInstance(response1_2));
        // counter still 2 because fetchData did not trigger new async fetch
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());
    }

    public void testConcurrentRequestAndClearCache() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).build();
        test.addSimulation(node1.getId(), response1);

        // no fetched data, request still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));
        // counter 0 because fetch ongoing
        verify(asyncFetchTotalCounter, times(0)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // clear cache while request is still on going, before it is processed
        test.clearCacheForNode(node1.getId());

        test.fireSimulationAndWait(node1.getId());
        assertThat(test.reroute.get(), equalTo(1));
        // counter 1 because fetch completed, even though cache was wiped
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // prepare next request
        test.addSimulation(node1.getId(), response1_2);

        // verify still no fetched data, request still on going
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(false));
        // counter unchanged because fetch ongoing
        verify(asyncFetchTotalCounter, times(1)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        test.fireSimulationAndWait(node1.getId());
        assertThat(test.reroute.get(), equalTo(2));
        // counter 2 because second fetch completed
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

        // verify we get new data back
        fetchData = test.fetchData(nodes, emptyMap());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(1));
        assertThat(fetchData.getData().get(node1), sameInstance(response1_2));
        // counter unchanged because fetchData does not trigger new async fetch
        verify(asyncFetchTotalCounter, times(2)).add(anyDouble());
        verify(asyncFetchFailureCounter, times(0)).add(anyDouble());

    }

    static class TestFetch extends AsyncShardFetch<Response> {

        static class Entry {
            public final Response response;
            public final Throwable failure;
            private final CountDownLatch executeLatch = new CountDownLatch(1);
            private final CountDownLatch waitLatch = new CountDownLatch(1);

            Entry(Response response, Throwable failure) {
                this.response = response;
                this.failure = failure;
            }
        }

        private final ThreadPool threadPool;
        private final Map<String, Entry> simulations = new ConcurrentHashMap<>();
        private AtomicInteger reroute = new AtomicInteger();

        TestFetch(ThreadPool threadPool, MetricsRegistry metricsRegistry) {
            super(
                LogManager.getLogger(TestFetch.class),
                "test",
                new ShardId("test", "_na_", 1),
                "",
                null,
                new ClusterManagerMetrics(metricsRegistry)
            );
            this.threadPool = threadPool;
        }

        public void addSimulation(String nodeId, Response response) {
            simulations.put(nodeId, new Entry(response, null));
        }

        public void addSimulation(String nodeId, Throwable t) {
            simulations.put(nodeId, new Entry(null, t));
        }

        public void fireSimulationAndWait(String nodeId) throws InterruptedException {
            simulations.get(nodeId).executeLatch.countDown();
            simulations.get(nodeId).waitLatch.await();
            simulations.remove(nodeId);
        }

        @Override
        protected void reroute(String shardId, String reason) {
            reroute.incrementAndGet();
        }

        @Override
        protected void asyncFetch(DiscoveryNode[] nodes, long fetchingRound) {
            for (final DiscoveryNode node : nodes) {
                final String nodeId = node.getId();
                threadPool.generic().execute(new Runnable() {
                    @Override
                    public void run() {
                        Entry entry = null;
                        try {
                            entry = simulations.get(nodeId);
                            if (entry == null) {
                                // we are simulating a cluster-manager node switch, wait for it to not be null
                                assertBusy(() -> assertTrue(simulations.containsKey(nodeId)));
                            }
                            assert entry != null;
                            entry.executeLatch.await();
                            if (entry.failure != null) {
                                processAsyncFetch(
                                    null,
                                    Collections.singletonList(new FailedNodeException(nodeId, "unexpected", entry.failure)),
                                    fetchingRound
                                );
                            } else {
                                processAsyncFetch(Collections.singletonList(entry.response), null, fetchingRound);
                            }
                        } catch (Exception e) {
                            logger.error("unexpected failure", e);
                        } finally {
                            if (entry != null) {
                                entry.waitLatch.countDown();
                            }
                        }
                    }
                });
            }
        }
    }

    static class Response extends BaseNodeResponse {

        Response(DiscoveryNode node) {
            super(node);
        }
    }
}
