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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.ConnectionProfile;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.TransportStats;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static java.util.Collections.emptySet;
import static org.opensearch.cluster.NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING;
import static org.opensearch.common.settings.Settings.builder;
import static org.opensearch.common.unit.TimeValue.timeValueMillis;
import static org.opensearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.opensearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class NodeConnectionsServiceTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private TransportService transportService;
    private Map<DiscoveryNode, CheckedRunnable<Exception>> nodeConnectionBlocks;

    private List<DiscoveryNode> generateNodes() {
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (int i = randomIntBetween(20, 50); i > 0; i--) {
            Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES));
            nodes.add(
                new DiscoveryNode("node_" + i, "" + i, buildNewFakeTransportAddress(), Collections.emptyMap(), roles, Version.CURRENT)
            );
        }
        return nodes;
    }

    private DiscoveryNodes discoveryNodesFromList(List<DiscoveryNode> discoveryNodes) {
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (final DiscoveryNode discoveryNode : discoveryNodes) {
            builder.add(discoveryNode);
        }
        return builder.build();
    }

    public void testConnectAndDisconnect() throws Exception {
        final NodeConnectionsService service = new NodeConnectionsService(Settings.EMPTY, threadPool, transportService);

        final AtomicBoolean stopReconnecting = new AtomicBoolean();
        final Thread reconnectionThread = new Thread(() -> {
            while (stopReconnecting.get() == false) {
                final PlainActionFuture<Void> future = new PlainActionFuture<>();
                service.ensureConnections(() -> future.onResponse(null));
                future.actionGet();
            }
        }, "reconnection thread");
        reconnectionThread.start();

        try {

            final List<DiscoveryNode> allNodes = generateNodes();
            for (int iteration = 0; iteration < 3; iteration++) {

                final boolean isDisrupting = randomBoolean();
                if (isDisrupting == false) {
                    // if the previous iteration was a disrupting one then there could still be some pending disconnections which would
                    // prevent us from asserting that all nodes are connected in this iteration without this call.
                    ensureConnections(service);
                }
                final AtomicBoolean stopDisrupting = new AtomicBoolean();
                final Thread disruptionThread = new Thread(() -> {
                    while (isDisrupting && stopDisrupting.get() == false) {
                        transportService.disconnectFromNode(randomFrom(allNodes));
                    }
                }, "disruption thread " + iteration);
                disruptionThread.start();

                final DiscoveryNodes nodes = discoveryNodesFromList(randomSubsetOf(allNodes));
                final PlainActionFuture<Void> future = new PlainActionFuture<>();
                service.connectToNodes(nodes, () -> future.onResponse(null));
                future.actionGet();
                if (isDisrupting == false) {
                    assertConnected(transportService, nodes);
                }
                service.disconnectFromNodesExcept(nodes);

                assertTrue(stopDisrupting.compareAndSet(false, true));
                disruptionThread.join();

                if (randomBoolean()) {
                    // sometimes do not wait for the disconnections to complete before starting the next connections
                    if (usually()) {
                        ensureConnections(service);
                        assertConnectedExactlyToNodes(nodes);
                    } else {
                        assertBusy(() -> assertConnectedExactlyToNodes(nodes));
                    }
                }
            }
        } finally {
            assertTrue(stopReconnecting.compareAndSet(false, true));
            reconnectionThread.join();
        }

        ensureConnections(service);
    }

    public void testPeriodicReconnection() {
        final Settings.Builder settings = Settings.builder();
        final long reconnectIntervalMillis;
        if (randomBoolean()) {
            reconnectIntervalMillis = CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.get(Settings.EMPTY).millis();
        } else {
            reconnectIntervalMillis = randomLongBetween(1, 100000);
            settings.put(CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), reconnectIntervalMillis + "ms");
        }

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            builder().put(NODE_NAME_SETTING.getKey(), "node").build(),
            random()
        );

        MockTransport transport = new MockTransport(deterministicTaskQueue.getThreadPool());
        TestTransportService transportService = new TestTransportService(transport, deterministicTaskQueue.getThreadPool());
        transportService.start();
        transportService.acceptIncomingRequests();

        final NodeConnectionsService service = new NodeConnectionsService(
            settings.build(),
            deterministicTaskQueue.getThreadPool(),
            transportService
        );
        service.start();

        final List<DiscoveryNode> allNodes = generateNodes();
        final DiscoveryNodes targetNodes = discoveryNodesFromList(randomSubsetOf(allNodes));

        transport.randomConnectionExceptions = true;

        final AtomicBoolean connectionCompleted = new AtomicBoolean();
        service.connectToNodes(targetNodes, () -> connectionCompleted.set(true));
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(connectionCompleted.get());

        long maxDisconnectionTime = 0;
        for (int iteration = 0; iteration < 3; iteration++) {
            // simulate disconnects
            for (DiscoveryNode node : allNodes) {
                if (randomBoolean()) {
                    final long disconnectionTime = randomLongBetween(0, 120000);
                    maxDisconnectionTime = Math.max(maxDisconnectionTime, disconnectionTime);
                    deterministicTaskQueue.scheduleAt(disconnectionTime, new Runnable() {
                        @Override
                        public void run() {
                            transportService.disconnectFromNode(node);
                        }

                        @Override
                        public String toString() {
                            return "scheduled disconnection of " + node;
                        }
                    });
                }
            }
        }

        runTasksUntil(deterministicTaskQueue, maxDisconnectionTime);

        // disable exceptions so things can be restored
        transport.randomConnectionExceptions = false;
        logger.info("renewing connections");
        runTasksUntil(deterministicTaskQueue, maxDisconnectionTime + reconnectIntervalMillis);
        assertConnectedExactlyToNodes(transportService, targetNodes);
    }

    public void testOnlyBlocksOnConnectionsToNewNodes() throws Exception {
        final NodeConnectionsService service = new NodeConnectionsService(Settings.EMPTY, threadPool, transportService);

        // connect to one node
        final DiscoveryNode node0 = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNodes nodes0 = DiscoveryNodes.builder().add(node0).build();
        final PlainActionFuture<Void> future0 = new PlainActionFuture<>();
        service.connectToNodes(nodes0, () -> future0.onResponse(null));
        future0.actionGet();
        assertConnectedExactlyToNodes(nodes0);

        // connection attempts to node0 block indefinitely
        final CyclicBarrier connectionBarrier = new CyclicBarrier(2);
        try {
            nodeConnectionBlocks.put(node0, connectionBarrier::await);
            transportService.disconnectFromNode(node0);

            // can still connect to another node without blocking
            final DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
            final DiscoveryNodes nodes1 = DiscoveryNodes.builder().add(node1).build();
            final DiscoveryNodes nodes01 = DiscoveryNodes.builder(nodes0).add(node1).build();
            final PlainActionFuture<Void> future1 = new PlainActionFuture<>();
            service.connectToNodes(nodes01, () -> future1.onResponse(null));
            future1.actionGet();
            assertConnectedExactlyToNodes(nodes1);

            // can also disconnect from node0 without blocking
            final PlainActionFuture<Void> future2 = new PlainActionFuture<>();
            service.connectToNodes(nodes1, () -> future2.onResponse(null));
            future2.actionGet();
            service.disconnectFromNodesExcept(nodes1);
            assertConnectedExactlyToNodes(nodes1);

            // however, now node0 is considered to be a new node so we will block on a subsequent attempt to connect to it
            final PlainActionFuture<Void> future3 = new PlainActionFuture<>();
            service.connectToNodes(nodes01, () -> future3.onResponse(null));
            expectThrows(OpenSearchTimeoutException.class, () -> future3.actionGet(timeValueMillis(scaledRandomIntBetween(1, 1000))));

            // once the connection is unblocked we successfully connect to it.
            connectionBarrier.await(10, TimeUnit.SECONDS);
            nodeConnectionBlocks.clear();
            future3.actionGet();
            assertConnectedExactlyToNodes(nodes01);

            // if we disconnect from a node while blocked trying to connect to it then we do eventually disconnect from it
            nodeConnectionBlocks.put(node0, connectionBarrier::await);
            transportService.disconnectFromNode(node0);
            final PlainActionFuture<Void> future4 = new PlainActionFuture<>();
            service.connectToNodes(nodes01, () -> future4.onResponse(null));
            future4.actionGet();
            assertConnectedExactlyToNodes(nodes1);

            service.disconnectFromNodesExcept(nodes1);
            connectionBarrier.await();
            if (randomBoolean()) {
                // assertBusy because the connection completes before disconnecting, so we might briefly observe a connection to node0
                assertBusy(() -> assertConnectedExactlyToNodes(nodes1));
            }

            // use ensureConnections() to wait until the service is idle
            ensureConnections(service);
            assertConnectedExactlyToNodes(nodes1);

            // if we disconnect from a node while blocked trying to connect to it then the listener is notified
            final PlainActionFuture<Void> future6 = new PlainActionFuture<>();
            service.connectToNodes(nodes01, () -> future6.onResponse(null));
            expectThrows(OpenSearchTimeoutException.class, () -> future6.actionGet(timeValueMillis(scaledRandomIntBetween(1, 1000))));

            service.disconnectFromNodesExcept(nodes1);
            future6.actionGet(); // completed even though the connection attempt is still blocked
            assertConnectedExactlyToNodes(nodes1);

            connectionBarrier.await(10, TimeUnit.SECONDS);
            nodeConnectionBlocks.clear();
            ensureConnections(service);
            assertConnectedExactlyToNodes(nodes1);
        } finally {
            nodeConnectionBlocks.clear();
            connectionBarrier.reset();
        }
    }

    @TestLogging(reason = "testing that DEBUG-level logging is reasonable", value = "org.opensearch.cluster.NodeConnectionsService:DEBUG")
    public void testDebugLogging() throws IllegalAccessException {
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            builder().put(NODE_NAME_SETTING.getKey(), "node").build(),
            random()
        );

        MockTransport transport = new MockTransport(deterministicTaskQueue.getThreadPool());
        TestTransportService transportService = new TestTransportService(transport, deterministicTaskQueue.getThreadPool());
        transportService.start();
        transportService.acceptIncomingRequests();

        final NodeConnectionsService service = new NodeConnectionsService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            transportService
        );
        service.start();

        final List<DiscoveryNode> allNodes = generateNodes();
        final DiscoveryNodes targetNodes = discoveryNodesFromList(randomSubsetOf(allNodes));
        service.connectToNodes(targetNodes, () -> {});
        deterministicTaskQueue.runAllRunnableTasks();

        // periodic reconnections to unexpectedly-disconnected nodes are logged
        final Set<DiscoveryNode> disconnectedNodes = new HashSet<>(randomSubsetOf(allNodes));
        for (DiscoveryNode disconnectedNode : disconnectedNodes) {
            transportService.disconnectFromNode(disconnectedNode);
        }
        final Logger logger = LogManager.getLogger("org.opensearch.cluster.NodeConnectionsService");
        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            for (DiscoveryNode targetNode : targetNodes) {
                if (disconnectedNodes.contains(targetNode)) {
                    appender.addExpectation(
                        new MockLogAppender.SeenEventExpectation(
                            "connecting to " + targetNode,
                            "org.opensearch.cluster.NodeConnectionsService",
                            Level.DEBUG,
                            "connecting to " + targetNode
                        )
                    );
                    appender.addExpectation(
                        new MockLogAppender.SeenEventExpectation(
                            "connected to " + targetNode,
                            "org.opensearch.cluster.NodeConnectionsService",
                            Level.DEBUG,
                            "connected to " + targetNode
                        )
                    );
                } else {
                    appender.addExpectation(
                        new MockLogAppender.UnseenEventExpectation(
                            "connecting to " + targetNode,
                            "org.opensearch.cluster.NodeConnectionsService",
                            Level.DEBUG,
                            "connecting to " + targetNode
                        )
                    );
                    appender.addExpectation(
                        new MockLogAppender.UnseenEventExpectation(
                            "connected to " + targetNode,
                            "org.opensearch.cluster.NodeConnectionsService",
                            Level.DEBUG,
                            "connected to " + targetNode
                        )
                    );
                }
            }

            runTasksUntil(deterministicTaskQueue, CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.get(Settings.EMPTY).millis());
            appender.assertAllExpectationsMatched();
        }
        for (DiscoveryNode disconnectedNode : disconnectedNodes) {
            transportService.disconnectFromNode(disconnectedNode);
        }

        // changes to the expected set of nodes are logged, including reconnections to any unexpectedly-disconnected nodes
        final DiscoveryNodes newTargetNodes = discoveryNodesFromList(randomSubsetOf(allNodes));
        for (DiscoveryNode disconnectedNode : disconnectedNodes) {
            transportService.disconnectFromNode(disconnectedNode);
        }

        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            for (DiscoveryNode targetNode : targetNodes) {
                if (disconnectedNodes.contains(targetNode) && newTargetNodes.get(targetNode.getId()) != null) {
                    appender.addExpectation(
                        new MockLogAppender.SeenEventExpectation(
                            "connecting to " + targetNode,
                            "org.opensearch.cluster.NodeConnectionsService",
                            Level.DEBUG,
                            "connecting to " + targetNode
                        )
                    );
                    appender.addExpectation(
                        new MockLogAppender.SeenEventExpectation(
                            "connected to " + targetNode,
                            "org.opensearch.cluster.NodeConnectionsService",
                            Level.DEBUG,
                            "connected to " + targetNode
                        )
                    );
                } else {
                    appender.addExpectation(
                        new MockLogAppender.UnseenEventExpectation(
                            "connecting to " + targetNode,
                            "org.opensearch.cluster.NodeConnectionsService",
                            Level.DEBUG,
                            "connecting to " + targetNode
                        )
                    );
                    appender.addExpectation(
                        new MockLogAppender.UnseenEventExpectation(
                            "connected to " + targetNode,
                            "org.opensearch.cluster.NodeConnectionsService",
                            Level.DEBUG,
                            "connected to " + targetNode
                        )
                    );
                }
                if (newTargetNodes.get(targetNode.getId()) == null) {
                    appender.addExpectation(
                        new MockLogAppender.SeenEventExpectation(
                            "disconnected from " + targetNode,
                            "org.opensearch.cluster.NodeConnectionsService",
                            Level.DEBUG,
                            "disconnected from " + targetNode
                        )
                    );
                }
            }
            for (DiscoveryNode targetNode : newTargetNodes) {
                appender.addExpectation(
                    new MockLogAppender.UnseenEventExpectation(
                        "disconnected from " + targetNode,
                        "org.opensearch.cluster.NodeConnectionsService",
                        Level.DEBUG,
                        "disconnected from " + targetNode
                    )
                );
                if (targetNodes.get(targetNode.getId()) == null) {
                    appender.addExpectation(
                        new MockLogAppender.SeenEventExpectation(
                            "connecting to " + targetNode,
                            "org.opensearch.cluster.NodeConnectionsService",
                            Level.DEBUG,
                            "connecting to " + targetNode
                        )
                    );
                    appender.addExpectation(
                        new MockLogAppender.SeenEventExpectation(
                            "connected to " + targetNode,
                            "org.opensearch.cluster.NodeConnectionsService",
                            Level.DEBUG,
                            "connected to " + targetNode
                        )
                    );
                }
            }

            service.disconnectFromNodesExcept(newTargetNodes);
            service.connectToNodes(newTargetNodes, () -> {});
            deterministicTaskQueue.runAllRunnableTasks();
            appender.assertAllExpectationsMatched();
        }
    }

    private void runTasksUntil(DeterministicTaskQueue deterministicTaskQueue, long endTimeMillis) {
        while (deterministicTaskQueue.getCurrentTimeMillis() < endTimeMillis) {
            if (deterministicTaskQueue.hasRunnableTasks() && randomBoolean()) {
                deterministicTaskQueue.runRandomTask();
            } else if (deterministicTaskQueue.hasDeferredTasks()) {
                deterministicTaskQueue.advanceTime();
            }
        }
        deterministicTaskQueue.runAllRunnableTasks();
    }

    private void ensureConnections(NodeConnectionsService service) {
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        service.ensureConnections(() -> future.onResponse(null));
        future.actionGet();
    }

    private void assertConnectedExactlyToNodes(DiscoveryNodes discoveryNodes) {
        assertConnectedExactlyToNodes(transportService, discoveryNodes);
    }

    private void assertConnectedExactlyToNodes(TransportService transportService, DiscoveryNodes discoveryNodes) {
        assertConnected(transportService, discoveryNodes);
        assertThat(transportService.getConnectionManager().size(), equalTo(discoveryNodes.getSize()));
    }

    private void assertConnected(TransportService transportService, Iterable<DiscoveryNode> nodes) {
        for (DiscoveryNode node : nodes) {
            assertTrue("not connected to " + node, transportService.nodeConnected(node));
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        this.threadPool = threadPool;
        nodeConnectionBlocks = newConcurrentMap();
        transportService = new TestTransportService(new MockTransport(threadPool), threadPool);
        transportService.start();
        transportService.acceptIncomingRequests();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        transportService.stop();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        super.tearDown();
    }

    private final class TestTransportService extends TransportService {

        private TestTransportService(Transport transport, ThreadPool threadPool) {
            super(
                Settings.EMPTY,
                transport,
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundAddress -> DiscoveryNode.createLocal(Settings.EMPTY, buildNewFakeTransportAddress(), UUIDs.randomBase64UUID()),
                null,
                emptySet()
            );
        }

        @Override
        public void handshake(
            Transport.Connection connection,
            long timeout,
            Predicate<ClusterName> clusterNamePredicate,
            ActionListener<HandshakeResponse> listener
        ) {
            listener.onResponse(new HandshakeResponse(connection.getNode(), new ClusterName(""), Version.CURRENT));
        }

        @Override
        public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
            throw new AssertionError("no blocking connect");
        }

        @Override
        public void connectToNode(DiscoveryNode node, ActionListener<Void> listener) throws ConnectTransportException {
            final CheckedRunnable<Exception> connectionBlock = nodeConnectionBlocks.get(node);
            if (connectionBlock != null) {
                getThreadPool().generic().execute(() -> {
                    try {
                        connectionBlock.run();
                        super.connectToNode(node, listener);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                });
            } else {
                super.connectToNode(node, listener);
            }
        }
    }

    private static final class MockTransport implements Transport {
        private final ResponseHandlers responseHandlers = new ResponseHandlers();
        private final RequestHandlers requestHandlers = new RequestHandlers();
        private volatile boolean randomConnectionExceptions = false;
        private final ThreadPool threadPool;

        MockTransport(ThreadPool threadPool) {
            this.threadPool = threadPool;
        }

        @Override
        public void setMessageListener(TransportMessageListener listener) {}

        @Override
        public BoundTransportAddress boundAddress() {
            return null;
        }

        @Override
        public Map<String, BoundTransportAddress> profileBoundAddresses() {
            return null;
        }

        @Override
        public TransportAddress[] addressesFromString(String address) {
            return new TransportAddress[0];
        }

        @Override
        public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Connection> listener) {
            if (profile == null && randomConnectionExceptions && randomBoolean()) {
                threadPool.generic().execute(() -> listener.onFailure(new ConnectTransportException(node, "simulated")));
            } else {
                threadPool.generic().execute(() -> listener.onResponse(new Connection() {
                    @Override
                    public DiscoveryNode getNode() {
                        return node;
                    }

                    @Override
                    public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                        throws TransportException {}

                    @Override
                    public void addCloseListener(ActionListener<Void> listener) {}

                    @Override
                    public void close() {}

                    @Override
                    public boolean isClosed() {
                        return false;
                    }
                }));
            }
        }

        @Override
        public List<String> getDefaultSeedAddresses() {
            return null;
        }

        @Override
        public Lifecycle.State lifecycleState() {
            return null;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {}

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {}

        @Override
        public void start() {}

        @Override
        public void stop() {}

        @Override
        public void close() {}

        @Override
        public TransportStats getStats() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ResponseHandlers getResponseHandlers() {
            return responseHandlers;
        }

        @Override
        public RequestHandlers getRequestHandlers() {
            return requestHandlers;
        }
    }
}
